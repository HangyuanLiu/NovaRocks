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

//! Actual SQL source and permanent Frame witnesses. Before preparation is RED
//! at the original package effect author; raw scalar oracles remain independent.
use super::compiled_program::CompiledExpressionInstance;
use super::legacy_between_observed_baseline_tests::original;
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_scan_residuals;
use arrow::array::{Array, ArrayRef, BooleanArray, Decimal128Array, Int64Array, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind, ProgramRootInput,
    root_input_layout,
};
use novarocks_physical_plan::ExprKind;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BETWEEN never waits")
    }
}
const REQUIRED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/filter/sql/filter_decimal_binary_pred_mixed_precision.sql"
));
fn between_count(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .fragments()
        .values()
        .flat_map(|f| f.expressions().iter())
        .filter(|(_, e)| matches!(e.kind, ExprKind::Between { .. }))
        .count()
}
#[test]
fn between_sql_source_original_required_native_case_retains_actual_bound_decimal_domains() {
    let source = sql_source(
        REQUIRED,
        DataType::Int64,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    assert_eq!(between_count(&source), 1);
    for fragment in source.plan().fragments().values() {
        for (_, node) in fragment.expressions().iter() {
            if let ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } = &node.kind
            {
                assert!(!negated);
                assert_eq!(node.ty.data_type, DataType::Boolean);
                let operand = &fragment.expressions().get(*expr).unwrap().ty;
                assert!(matches!(operand.data_type, DataType::Decimal128(..)));
                for child in [low, high] {
                    assert!(
                        operand.same_value_domain(&fragment.expressions().get(*child).unwrap().ty)
                    );
                }
                assert_eq!(
                    node.ty.nullable,
                    operand.nullable
                        || fragment.expressions().get(*low).unwrap().ty.nullable
                        || fragment.expressions().get(*high).unwrap().ty.nullable
                );
            }
        }
    }
}
fn evaluate(sql: &str, input: ArrayRef, low: ArrayRef, high: ArrayRef, negated: bool, truth: bool) {
    let source = sql_source(
        sql,
        input.data_type().clone(),
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    assert_eq!(
        between_count(&source),
        1,
        "fixture must preserve actual BETWEEN, not SQL rewrite into an assumed closure"
    );
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    // This original author call is deliberately permanent RED before support.
    let programs = programs_with_catalogue_and_original_scan_residuals(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            let site = match node.kind() {
                ProgramNodeKind::Project { exprs, .. }
                    if !truth
                        && exprs.len() == 1
                        && node.output_layout().schema().field(0).data_type()
                            == &DataType::Boolean =>
                {
                    ProgramExpressionRootSite::Node {
                        node: node.local_id().unwrap(),
                        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
                    }
                }
                ProgramNodeKind::Filter { .. } if truth => ProgramExpressionRootSite::Node {
                    node: node.local_id().unwrap(),
                    role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
                },
                ProgramNodeKind::Scan { residuals, .. } if truth && residuals.len() == 1 => {
                    ProgramExpressionRootSite::Node {
                        node: node.local_id().unwrap(),
                        role: ProgramNodeExpressionRole::ScanResidual { predicate: 0 },
                    }
                }
                _ => continue,
            };
            let snapshot = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            if snapshot.bindings().contains_key(&site) {
                assert!(found.is_none(), "fixture owns one actual predicate root");
                found = Some((program.clone(), site));
            }
        }
    }
    let (program, site) = found.expect("actual SQL predicate producer");
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), site).unwrap()
    else {
        panic!("fixture owns actual scan input")
    };
    let schema = program
        .checked()
        .channels()
        .channel_layout(node, role)
        .unwrap()
        .schema()
        .clone();
    // The actual source preserves a one-column scan; do not retag or forge FVT.
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    let data = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    let old = original(vec![input.clone(), low, high], negated).unwrap();
    for rows in [(0..input.len()).collect::<Vec<_>>(), vec![0], Vec::new()] {
        let selection = Selection::try_sparse(input.len(), &rows).unwrap();
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let out = frame.evaluate(&data, selection, &Control).unwrap();
        assert!(out.errors().is_empty());
        let indices = UInt64Array::from(
            rows.iter()
                .map(|r| u64::try_from(*r).unwrap())
                .collect::<Vec<_>>(),
        );
        let expected = arrow::compute::take(old.as_ref(), &indices, None).unwrap();
        let wanted = if truth {
            Arc::new(BooleanArray::from(
                expected
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap_or(false))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        } else {
            expected
        };
        assert_eq!(out.values().to_data(), wanted.to_data());
    }
}
#[test]
fn between_actual_compiler_positive_value_and_truthonly_int64_source() {
    for (sql, truth) in [
        (
            "SELECT k BETWEEN 1 AND 5 AS original_range FROM fixture.source",
            false,
        ),
        ("SELECT k FROM fixture.source WHERE k BETWEEN 1 AND 5", true),
    ] {
        evaluate(
            sql,
            Arc::new(Int64Array::from(vec![0, 1, 3, 5, 9])),
            Arc::new(Int64Array::from(vec![1; 5])),
            Arc::new(Int64Array::from(vec![5; 5])),
            false,
            truth,
        );
    }
}
#[test]
fn between_actual_compiler_negated_value_source() {
    evaluate(
        "SELECT k NOT BETWEEN 1 AND 5 AS original_range FROM fixture.source",
        Arc::new(Int64Array::from(vec![0, 1, 3, 5, 9])),
        Arc::new(Int64Array::from(vec![1; 5])),
        Arc::new(Int64Array::from(vec![5; 5])),
        true,
        false,
    );
}
#[test]
fn between_actual_compiler_required_decimal_bound_source() {
    let make = |p, s, values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef
    };
    evaluate(
        "SELECT k BETWEEN CAST(100.0 AS DECIMAL(6,1)) AND CAST(150 AS DECIMAL(4,0)) AS original_range FROM fixture.source",
        make(7, 2, vec![12000, 9000]),
        make(6, 1, vec![1000; 2]),
        make(4, 0, vec![150; 2]),
        false,
        false,
    );
}

#[test]
fn between_actual_compiler_decimal_literals_without_integral_cast_source() {
    let make = |values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(7, 2)
                .unwrap(),
        ) as ArrayRef
    };
    // This separate fixture leaves the required mixed-bound test untouched.
    // Decimal literals avoid the independently missing Int64-to-Decimal128 cast.
    evaluate(
        "SELECT k BETWEEN 100.0 AND 150.0 AS original_range FROM fixture.source",
        make(vec![12000, 9000]),
        make(vec![10000; 2]),
        make(vec![15000; 2]),
        false,
        false,
    );
}
