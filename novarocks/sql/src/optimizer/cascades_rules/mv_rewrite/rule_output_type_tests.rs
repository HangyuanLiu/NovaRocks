// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Included in rule::tests to reuse its actual logical-plan and catalogue authors.
fn output_type_rollup_fixture(
    count: bool,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> (Memo, MExpr, MvRewriteCandidate, OutputColumn) {
    let mv_a = col(100, "a");
    let mut mv_value = col(110, "materialized_aggregate");
    let mut mv_call = if count {
        count_star(&mv_value)
    } else {
        sum_call(&mv_a, &mv_value)
    };
    mv_call.source = crate::binding::AggregateArgumentSource::uncertified(
        mv_call.source.arguments().to_vec(),
        mv_call.source.order_by().to_vec(),
        crate::binding::SqlFunctionBinding::new(
            mv_call.source.binding().resolved().clone(),
            policy,
        ),
    );
    let novarocks_functions::FunctionResultType::Scalar(selected) =
        &mv_call.source.binding().selected.result_type
    else {
        panic!("test aggregate must be scalar");
    };
    mv_value.value_type = selected.clone();
    let mv_plan = LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: vec![col_ref(&mv_a)],
            aggregates: vec![mv_call],
            output_columns: vec![mv_a.clone(), mv_value.clone()],
            already_pushed: false,
        }),
        vec![base_scan(std::slice::from_ref(&mv_a))],
        None,
    );
    let (mv, mv_scalars) = spjg_descriptor_for_test(&mv_plan);
    let mut target_table = iceberg_table(
        "cat",
        "ns",
        "exact_output_mv",
        &["a", "materialized_aggregate"],
    );
    target_table.columns[1].data_type = mv_value.value_type.data_type.clone();
    target_table.columns[1].nullable = mv_value.value_type.nullable;
    let candidate = MvRewriteCandidate {
        mv_name: "exact_output_mv".into(),
        mv,
        mv_scalars,
        target_database: "ns".into(),
        target_table,
        target_stats_ref: stats_ref_for_test(731),
        selection: Some(selection_facts()),
    };

    let a = col(1, "a");
    let mut published = col(3, "query_published_name");
    let mut call = if count {
        count_star(&published)
    } else {
        sum_call(&a, &published)
    };
    call.source = crate::binding::AggregateArgumentSource::uncertified(
        call.source.arguments().to_vec(),
        call.source.order_by().to_vec(),
        crate::binding::SqlFunctionBinding::new(call.source.binding().resolved().clone(), policy),
    );
    let novarocks_functions::FunctionResultType::Scalar(selected) =
        &call.source.binding().selected.result_type
    else {
        panic!("test aggregate must be scalar");
    };
    published.value_type = selected.clone();
    let query = LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: vec![],
            aggregates: vec![call],
            output_columns: vec![published.clone()],
            already_pushed: false,
        }),
        vec![base_scan(std::slice::from_ref(&a))],
        None,
    );
    let mut memo = test_memo();
    let root = logical_plan_to_memo_for_test(&query, &mut memo);
    advance_factory(&mut memo, 200);
    let expr = memo.groups[root].logical_exprs[0].clone();
    (memo, expr, candidate, published)
}

#[test]
fn count_rollup_internal_sum_uses_selected_nullable_type_and_exact_visible_count() {
    for policy in [
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
    ] {
        let (mut memo, expr, candidate, published) = output_type_rollup_fixture(true, policy);
        assert!(
            !published.value_type.nullable,
            "actual COUNT author is nonnullable"
        );
        let alts = MvRewriteRule::new(vec![candidate])
            .apply(&expr, &mut memo, crate::optimizer::test_optimizer_control())
            .unwrap();
        assert_eq!(alts.len(), 1);
        let Operator::LogicalProject(project) = &alts[0].op else {
            panic!("scalar COUNT requires the COALESCE projection");
        };
        let item = &project.items[0];
        assert_eq!(item.output_column_id, published.column_id);
        assert_eq!(memo.scalars.value_type(item.expr), &published.value_type);
        let ScalarNode::FunctionCall {
            name,
            args,
            binding,
            ..
        } = memo.scalars.node(item.expr)
        else {
            panic!("expected the real COALESCE binding");
        };
        assert_eq!(name, "coalesce");
        assert_eq!(binding.decimal_overflow_policy(), policy);
        let novarocks_functions::FunctionResultType::Scalar(coalesce_result) =
            &binding.selected.result_type
        else {
            panic!("COALESCE must be scalar")
        };
        assert_eq!(memo.scalars.value_type(item.expr), coalesce_result);
        assert!(!memo.scalars.value_type(args[1]).nullable);

        let Operator::LogicalAggregate(inner) =
            &memo.groups[alts[0].children[0]].logical_exprs[0].op
        else {
            panic!("expected the actual rollup aggregate")
        };
        let novarocks_functions::FunctionResultType::Scalar(sum_result) =
            &inner.aggregates[0].source.binding().selected.result_type
        else {
            panic!("SUM must be scalar")
        };
        assert!(
            sum_result.nullable,
            "SUM can return successful NULL on no rows"
        );
        assert_eq!(
            &inner.output_layout.aggregate_columns[0].value_type,
            sum_result
        );
        assert_eq!(&inner.output_columns[0].value_type, sum_result);
        assert_eq!(memo.scalars.value_type(args[0]), sum_result);
        assert_ne!(inner.output_columns[0].column_id, published.column_id);
        assert_eq!(inner.output_columns[0].name, published.name);
        assert_eq!(
            inner.aggregates[0]
                .source
                .binding()
                .decimal_overflow_policy(),
            policy
        );
    }
}

#[test]
fn sum_rollup_without_coalesce_keeps_exact_selected_result_and_original_id() {
    let (mut memo, expr, candidate, published) = output_type_rollup_fixture(
        false,
        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
    );
    let alts = MvRewriteRule::new(vec![candidate])
        .apply(&expr, &mut memo, crate::optimizer::test_optimizer_control())
        .unwrap();
    assert_eq!(alts.len(), 1);
    let Operator::LogicalAggregate(inner) = &alts[0].op else {
        panic!("SUM rollup does not require COALESCE");
    };
    let novarocks_functions::FunctionResultType::Scalar(selected) =
        &inner.aggregates[0].source.binding().selected.result_type
    else {
        panic!("SUM must be scalar")
    };
    assert_eq!(selected, &published.value_type);
    for output in [
        &inner.output_layout.aggregate_columns[0],
        &inner.output_columns[0],
    ] {
        assert_eq!(output.column_id, published.column_id);
        assert_eq!(output.name, published.name);
        assert_eq!(output.value_type, published.value_type);
        assert_eq!(output.is_internal, published.is_internal);
    }
}

#[test]
fn rollup_refuses_target_storage_and_query_published_type_drift() {
    for wrong_type in [DataType::UInt64, DataType::Float64] {
        let (mut memo, expr, mut candidate, _) = output_type_rollup_fixture(
            true,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        );
        candidate.target_table.columns[1].data_type = wrong_type;
        assert!(
            MvRewriteRule::new(vec![candidate])
                .apply(&expr, &mut memo, crate::optimizer::test_optimizer_control())
                .unwrap()
                .is_empty()
        );
    }
    let (mut memo, expr, mut candidate, _) = output_type_rollup_fixture(
        true,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    );
    candidate.target_table.columns[1].nullable = true;
    assert!(
        MvRewriteRule::new(vec![candidate])
            .apply(&expr, &mut memo, crate::optimizer::test_optimizer_control())
            .unwrap()
            .is_empty()
    );

    // Keep the real SUM binding and layout. Only the published visible column
    // lies about nullability; retaining its id must not authorize the rewrite.
    let (mut memo, mut expr, candidate, _) = output_type_rollup_fixture(
        false,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    );
    let Operator::LogicalAggregate(aggregate) = &mut expr.op else {
        panic!("aggregate fixture")
    };
    aggregate.output_columns[0].value_type.nullable = false;
    assert!(
        aggregate.output_layout.aggregate_columns[0]
            .value_type
            .nullable
    );
    assert!(
        MvRewriteRule::new(vec![candidate])
            .apply(&expr, &mut memo, crate::optimizer::test_optimizer_control())
            .unwrap()
            .is_empty()
    );
}

#[derive(Default)]
struct OutputTypeTrace {
    trace: Mutex<Vec<(novarocks_type_contract::CompilePhase, u32)>>,
    refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
}
impl novarocks_type_contract::PureCompileControl for OutputTypeTrace {
    fn checkpoint(
        &self,
        phase: novarocks_type_contract::CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if let Some((at, cause)) = self.refuse {
            if trace.len() == at + 1 {
                return Err(cause);
            }
        }
        Ok(())
    }
}

#[test]
fn count_rollup_every_original_control_callback_refuses_without_later_observation() {
    use crate::compiler::SqlCompileError;
    use novarocks_type_contract::CompileControlError;
    for wrong_target in [false, true] {
        let (mut memo, expr, mut candidate, _) = output_type_rollup_fixture(
            true,
            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        );
        candidate.target_table.columns[1].nullable = wrong_target;
        let baseline = OutputTypeTrace::default();
        let result = MvRewriteRule::new(vec![candidate])
            .apply(&expr, &mut memo, &baseline)
            .unwrap();
        assert_eq!(result.len(), usize::from(!wrong_target));
        let trace = baseline.trace.into_inner().unwrap();
        assert!(!trace.is_empty());
        assert!(trace.iter().any(|(_, units)| *units > 0));
        for at in 0..trace.len() {
            for (cause, expected) in [
                (CompileControlError::Cancelled, SqlCompileError::Cancelled),
                (
                    CompileControlError::DeadlineExceeded,
                    SqlCompileError::DeadlineExceeded,
                ),
                (
                    CompileControlError::ResourceExhausted,
                    SqlCompileError::ResourceExhausted,
                ),
            ] {
                let (mut memo, expr, mut candidate, _) = output_type_rollup_fixture(
                    true,
                    novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                );
                candidate.target_table.columns[1].nullable = wrong_target;
                let control = OutputTypeTrace {
                    trace: Mutex::default(),
                    refuse: Some((at, cause)),
                };
                let result = MvRewriteRule::new(vec![candidate]).apply(&expr, &mut memo, &control);
                assert_eq!(
                    result.err(),
                    Some(expected),
                    "callback={at} wrong_target={wrong_target}"
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
