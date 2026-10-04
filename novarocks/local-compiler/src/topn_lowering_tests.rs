// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::values_lowering_tests::{build, checked_pool, compile_options, providers};
use super::*;
use crate::FragmentCompileError;
use crate::{compile_fragment, topn::lower_topn};
use arrow_array::{ArrayRef, Int64Array, StringArray, StructArray};
use novarocks_local_program::{
    ProgramExpressionArena, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, SortTopNType,
};
use novarocks_physical_plan::NodeKind;
use novarocks_physical_plan::{
    NullOrdering, SortDirection, SortExpr, SortMode, TopNPhase, TopNSequenceId,
};
use novarocks_type_contract::{ControlShape, ValueLogicalType};

fn compile(
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    compile_fragment(providers(package), &functions(), compile_options(), control)
}
fn builder_from(base: &FragmentPackage) -> FragmentBuilder {
    let mut builder = FragmentBuilder::new(base.fragment().id());
    for value in base.fragment().values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for (_, expr) in base.fragment().expressions().iter() {
        builder.insert_expression(expr.clone()).unwrap();
    }
    for node in base.fragment().nodes().values() {
        builder.insert_node_unchecked(node.clone()).unwrap();
    }
    builder
}
fn complete(
    mut input: FragmentPackageInput,
    builder: FragmentBuilder,
    root: NodeId,
) -> Result<Arc<FragmentPackage>, Box<dyn std::error::Error>> {
    let fragment = builder.finish_definition(
        root,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
    )?;
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control::good())?;
    let domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (&site, source))| {
            let use_id = ExpressionUseId::new(match ordinal {
                0 => 0,
                1 => u32::MAX,
                n => u32::try_from(n - 1).unwrap(),
            });
            uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: source.demand,
                },
                definition: source.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::good(),
    )?;
    let expression_uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control::good())?;
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &Control::good())?;
    let output = fragment.nodes()[&root].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(index, &value)| ResultField {
            name: format!("result-{index}").into(),
            alias: Some(format!("original-{index}").into()),
            value,
            ty: fragment.values()[&value].ty.clone(),
        })
        .collect();
    input.result = Some(ResultPort {
        fragment: fragment.id(),
        output,
        fields,
    });
    input.fragment = fragment;
    input.calls = calls;
    input.expression_uses = expression_uses;
    Ok(Arc::new(FragmentPackage::try_new(
        input,
        package_admission(),
        &Control::good(),
    )?))
}
fn package(
    source: &ConstantPool,
    keys: usize,
    tail: bool,
    limit: u64,
    offset: u64,
) -> Arc<FragmentPackage> {
    let base = super::sort_lowering_tests::package(source, keys, tail, SortMode::Global, true);
    let mut input = (*base).clone().into_input();
    let root = input.fragment.root();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for (_, expr) in input.fragment.expressions().iter() {
        builder.insert_expression(expr.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        let mut node = node.clone();
        if node.id == NodeId::new(0) {
            let NodeKind::TopN {
                limit: old_limit,
                offset: old_offset,
                ..
            } = &mut node.kind
            else {
                panic!("actual ordinary TopN")
            };
            *old_limit = limit;
            *old_offset = offset;
        }
        builder.insert_node_unchecked(node).unwrap();
    }
    // All roots are re-authored against this actual fragment, never borrowed from a changed body.
    input.result = None;
    complete(input, builder, root).unwrap()
}
fn component(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
    missing: bool,
) -> Result<(ProgramNodeKind, novarocks_local_program::StaticLayout), FragmentCompileError> {
    let program = compile(Arc::new(package.clone()), &Control::good()).unwrap();
    let layout = program.graph().nodes()[0].output_layout();
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let site = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::SortOrder { key: 0 },
    };
    let use_id = snapshot.bindings()[&site];
    let definition = snapshot.flows()[&ProgramExpressionArena::Main].uses()[&use_id].definition;
    let mut ids = BTreeMap::from([(ExprId::new(1000), definition)]);
    if missing {
        ids.clear();
    }
    lower_topn(
        &package.fragment().nodes()[&NodeId::new(0)],
        ProgramNodeId::new(0),
        layout,
        &ids,
        control,
    )
}
fn is_control(error: &FragmentCompileError, cause: CompileControlError) -> bool {
    matches!(error,FragmentCompileError::Control(actual) if *actual==cause)
}

#[test]
fn single_topn_public_compile_preserves_ordered_keys_root_occurrences_and_exact_source_layout() {
    let package = package(&integer_pool(), 3, false, 2, 1);
    let program = compile(package.clone(), &Control::good()).unwrap();
    let ProgramNodeKind::Sort {
        input,
        use_top_n,
        order_by,
        limit,
        offset,
        topn_type,
        max_buffered_rows,
        max_buffered_bytes,
        partition_exprs,
        partition_limit,
    } = program.graph().nodes()[1].kind()
    else {
        panic!("actual local TopN sort")
    };
    assert_eq!(input.index(), 0);
    assert!(*use_top_n);
    assert_eq!(*limit, Some(2));
    assert_eq!(*offset, 1);
    assert_eq!(*topn_type, SortTopNType::RowNumber);
    assert_eq!(*max_buffered_rows, None);
    assert_eq!(*max_buffered_bytes, None);
    assert!(partition_exprs.is_empty());
    assert_eq!(*partition_limit, None);
    assert_eq!(
        order_by
            .iter()
            .map(|k| (k.asc, k.nulls_first))
            .collect::<Vec<_>>(),
        [(false, true), (true, false), (false, true)]
    );
    assert!(order_by.iter().all(|k| k.expr == order_by[0].expr));
    assert_eq!(
        program.graph().nodes()[0].output_layout().slots(),
        program.graph().nodes()[1].output_layout().slots()
    );
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let roles = snapshot
        .bindings()
        .keys()
        .filter_map(|site| match site {
            ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::SortOrder { key },
            } if node.index() == 1 => Some(*key),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(roles, [0, 1, 2]);
    let physical = package
        .expression_uses()
        .bindings()
        .iter()
        .filter(|(site, _)| site.node == NodeId::new(0))
        .count();
    assert_eq!(physical, 3);
    let ProgramNodeKind::Values { values } = program.graph().nodes()[0].kind() else {
        panic!("actual values")
    };
    let column = values
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(column.values().as_ref(), &[-7, 42]);
    assert_eq!(
        program.graph().nodes()[0].physical_sources()[0].get(),
        u32::MAX
    );
}

#[test]
fn single_topn_zero_and_host_representable_max_rows_preserve_values_without_buffer_defaults() {
    for (limit, offset) in [(0, 0), (0, 1), (2, 1), (u64::MAX, 0)] {
        let source = integer_pool();
        let package = package(&source, 1, false, limit, offset);
        let result = compile(package, &Control::good());
        if usize::try_from(limit).is_err() {
            assert!(matches!(
                result,
                Err(FragmentCompileError::Invalid(
                    "TopN limit exceeds host range"
                ))
            ));
            continue;
        }
        let program = result.unwrap();
        let ProgramNodeKind::Sort {
            limit: actual,
            offset: actual_offset,
            use_top_n,
            ..
        } = program.graph().nodes()[1].kind()
        else {
            panic!("TopN")
        };
        assert!(*use_top_n);
        assert_eq!(*actual, Some(usize::try_from(limit).unwrap()));
        assert_eq!(*actual_offset, usize::try_from(offset).unwrap());
    }
    // A legal host conversion never replaces the original u64 sum validator.
    let base =
        super::sort_lowering_tests::package(&integer_pool(), 1, false, SortMode::Global, true);
    let mut input = (*base).clone().into_input();
    let root = input.fragment.root();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for (_, expr) in input.fragment.expressions().iter() {
        builder.insert_expression(expr.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        let mut node = node.clone();
        if let NodeKind::TopN { limit, offset, .. } = &mut node.kind {
            *limit = u64::MAX;
            *offset = 1;
        }
        builder.insert_node_unchecked(node).unwrap();
    }
    input.result = None;
    let error = complete(input, builder, root).unwrap_err();
    assert!(error.to_string().contains("TopN limit and offset overflow"));
}

#[test]
fn topn_transparent_tail_preserves_original_ordering_and_passthrough_result_channels() {
    let package = package(&integer_pool(), 2, true, 2, 1);
    let program = compile(package, &Control::good()).unwrap();
    assert_eq!(program.graph().nodes().len(), 5);
    let source = program.graph().nodes()[1].output_layout().slots();
    assert_eq!(source, program.graph().nodes()[2].output_layout().slots());
    let ProgramNodeKind::Limit { limit, offset, .. } = program.graph().nodes()[4].kind() else {
        panic!("transparent limit")
    };
    assert_eq!(*limit, Some(1));
    assert_eq!(*offset, 0);
    assert!(matches!(
        program.graph().nodes()[3].kind(),
        ProgramNodeKind::Project { .. }
    ));
}

#[test]
fn topn_duplicate_passthrough_occurrences_keep_independent_slots_and_nested_field_metadata() {
    let scalar = integer_pool();
    let field = Arc::new(
        Field::new("nested-original", DataType::Utf8, true)
            .with_metadata([("provider.annotation".into(), "source-text".into())].into()),
    );
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(StringArray::from(vec![
            Some("ignored"),
            None,
            Some("selected"),
        ]))],
        None,
    ));
    let nested = checked_pool(nested, false, ValueLogicalType::Physical);
    let base = build(
        &[scalar, nested],
        &[vec![(0, 2), (1, 2)], vec![(0, 1), (1, 1)]],
        &[0, 1],
        false,
    );
    let mut builder = builder_from(&base);
    let input = NodeId::new(u32::MAX);
    let columns = &base.fragment().nodes()[&input].output.columns;
    let project = NodeId::new(7);
    let expression = builder
        .add_expression(
            project,
            base.fragment().values()[&columns[0]].ty.clone(),
            ExprKind::Value(columns[0]),
        )
        .unwrap();
    let nested_expr = builder
        .add_expression(
            project,
            base.fragment().values()[&columns[1]].ty.clone(),
            ExprKind::Value(columns[1]),
        )
        .unwrap();
    builder
        .add_project(
            project,
            input,
            Box::from([
                (expression, columns[0]),
                (expression, columns[0]),
                (nested_expr, columns[1]),
            ]),
            Box::from([columns[0], columns[0], columns[1]]),
        )
        .unwrap();
    let topn = NodeId::new(0);
    let key = builder
        .add_expression(
            topn,
            base.fragment().values()[&columns[0]].ty.clone(),
            ExprKind::Value(columns[0]),
        )
        .unwrap();
    builder
        .add_top_n(
            topn,
            project,
            Box::from([SortExpr {
                expr: key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            2,
            0,
            TopNPhase::Single,
        )
        .unwrap();
    let package = complete((*base).clone().into_input(), builder, topn).unwrap();
    let program = compile(package.clone(), &Control::good()).unwrap();
    let child = program.graph().nodes()[1].output_layout();
    let top = program.graph().nodes()[2].output_layout();
    assert_eq!(child.slots(), top.slots());
    assert_eq!(top.slots().len(), 3);
    assert_ne!(top.slots()[0], top.slots()[1]);
    let channel = program.checked().channels();
    // Complete nested types come from the existing channel owner, not a raw carrier retag.
    let output = channel
        .channel_type(novarocks_local_program::ProgramChannelSite::Layout {
            node: ProgramNodeId::new(2),
            role: novarocks_local_program::ProgramChannelLayoutRole::NodeOutput,
            ordinal: 2,
        })
        .unwrap();
    let observation = Control::good();
    assert!(
        output
            .exactly_equals_observed::<crate::expressions::ExpressionLoweringError>(
                &package.fragment().values()[&columns[1]].ty,
                || observation
                    .checkpoint(CompilePhase::Validate, 1)
                    .map_err(crate::expressions::ExpressionLoweringError::Control)
            )
            .unwrap()
    );
}

#[test]
fn actual_closed_partial_final_topn_package_stays_unsupported_and_keeps_original_control_prefixes()
{
    let base =
        super::sort_lowering_tests::package(&integer_pool(), 1, false, SortMode::Global, true);
    let mut input = (*base).clone().into_input();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for (_, expr) in input.fragment.expressions().iter() {
        builder.insert_expression(expr.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        let mut node = node.clone();
        if let NodeKind::TopN {
            phase,
            limit,
            offset,
            ..
        } = &mut node.kind
        {
            *phase = TopNPhase::Partial {
                sequence: TopNSequenceId::new(u32::MAX),
            };
            *limit = 3;
            *offset = 0;
        }
        builder.insert_node_unchecked(node).unwrap();
    }
    let final_node = NodeId::new(7);
    let column = input.fragment.nodes()[&NodeId::new(0)].output.columns[0];
    let expr = builder
        .add_expression(
            final_node,
            input.fragment.values()[&column].ty.clone(),
            ExprKind::Value(column),
        )
        .unwrap();
    builder
        .add_top_n(
            final_node,
            NodeId::new(0),
            Box::from([SortExpr {
                expr,
                direction: SortDirection::Descending,
                null_ordering: NullOrdering::First,
            }]),
            2,
            1,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(u32::MAX),
            },
        )
        .unwrap();
    input.result = None;
    let package = complete(input, builder, final_node).unwrap();
    let baseline = Control::good();
    assert!(
        matches!(compile(package.clone(),&baseline),Err(FragmentCompileError::Unsupported{node:Some(node),..}) if node==final_node)
    );
    let trace = baseline.trace();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause);
            assert!(is_control(
                &compile(package.clone(), &control).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}

#[test]
fn topn_actual_public_and_shared_key_component_preserve_all_small_control_prefixes_and_ordinary_tail()
 {
    let package = package(&integer_pool(), 2, false, 2, 1);
    for missing in [false, true] {
        let baseline = Control::good();
        let result = component(&package, &baseline, missing);
        assert_eq!(result.is_ok(), !missing);
        let trace = baseline.trace();
        assert!(trace.last().is_some_and(|(_, units)| *units > 0));
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control::refusing(at, cause);
                assert!(is_control(
                    &component(&package, &control, missing).unwrap_err(),
                    cause
                ));
                assert_eq!(control.trace(), trace[..at]);
            }
        }
    }
    let baseline = Control::good();
    compile(package.clone(), &baseline).unwrap();
    let trace = baseline.trace();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause);
            assert!(is_control(
                &compile(package.clone(), &control).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}

#[test]
fn topn_320_order_occurrences_cross_real_quantum_and_sample_entry_boundary_tail_causes() {
    let package = package(&integer_pool(), 320, false, 3, 1);
    let baseline = Control::good();
    let program = compile(package.clone(), &baseline).unwrap();
    let ProgramNodeKind::Sort { order_by, .. } = program.graph().nodes()[1].kind() else {
        panic!("TopN")
    };
    assert_eq!(order_by.len(), 320);
    assert!(order_by[319].asc);
    assert!(!order_by[319].nulls_first);
    let trace = baseline.trace();
    let quantum = trace
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("real completed work quantum")
        + 1;
    for at in [1, quantum, trace.len()] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause);
            assert!(is_control(
                &compile(package.clone(), &control).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}
