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

//! Actual original SQL source receipts and permanent IN/NOT IN Frame witnesses.
//! Before preparation these are RED at UnsupportedExpression(InList).
use super::compiled_program::CompiledExpressionInstance;
use super::legacy_inlist_required_baseline_tests::{original, original_required_case_operand};
use super::numeric_unary_original_nonnull_sql_baseline_tests::{
    sql_source_with_single_field, sql_source_with_single_field_and_core_residuals,
};
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source;
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field};
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
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN never waits")
    }
}
fn inspect(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut n = 0;
    for (&id, fragment) in source.plan().fragments() {
        for (_, node) in fragment.expressions().iter() {
            if let ExprKind::InList {
                expr,
                list,
                negated,
            } = &node.kind
            {
                n += 1;
                assert_eq!(node.ty.data_type, DataType::Boolean);
                let input = fragment.expressions().get(*expr).unwrap();
                println!(
                    "actual original IN source fragment={id:?} input={:?} result={:?} negated={negated} candidates={}",
                    input.ty,
                    node.ty,
                    list.len()
                );
                for value in list.iter() {
                    println!(
                        "actual original IN candidate {:?}",
                        fragment.expressions().get(*value).unwrap().ty
                    );
                }
            }
        }
    }
    n
}
#[test]
fn inlist_sql_source_original_required_numeric_and_nullable_region_facts() {
    for (sql, field) in [
        (
            "SELECT ship_code IN (1,2) FROM t0",
            Field::new("ship_code", DataType::Int32, true),
        ),
        (
            "SELECT c_region IN ('ASIA','AMERICA') FROM t_filter_customer_metrics",
            Field::new("c_region", DataType::Utf8, true),
        ),
        (
            "SELECT c_region NOT IN ('ASIA','AMERICA',NULL) FROM t_filter_customer_metrics",
            Field::new("c_region", DataType::Utf8, true),
        ),
        (
            "SELECT (CASE WHEN ship_code >= 90 THEN 'A' WHEN ship_code >= 80 THEN 'B' WHEN ship_code >= 70 THEN 'C' WHEN ship_code >= 60 THEN 'D' ELSE 'E' END) IN ('A','B') FROM t0",
            Field::new("ship_code", DataType::Int32, true),
        ),
    ] {
        let source =
            sql_source_with_single_field(sql, field, SqlPhysicalEmissionMode::OriginalNativeV1);
        assert!(
            inspect(&source) > 0,
            "original source must retain real IN definition"
        );
    }
}
fn evaluate(
    sql: &str,
    field: Field,
    input: ArrayRef,
    candidates: Vec<ArrayRef>,
    negated: bool,
    truth: bool,
    original_operand: Option<ArrayRef>,
) {
    // Same original SQL's legitimate optimizer variant, not a claim that
    // the native default producer emitted this exact residual site.
    let source = sql_source_with_single_field_and_core_residuals(
        sql,
        field,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    assert!(inspect(&source) > 0);
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    // This actual package effects author is deliberately RED before IN support.
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
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
                assert!(found.is_none(), "fixture has one actual membership root");
                found = Some((program.clone(), site));
            }
        }
    }
    let (program, site) = found.expect("actual original membership root");
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), site).unwrap()
    else {
        panic!("actual source owns a scan input")
    };
    let schema = program
        .checked()
        .channels()
        .channel_layout(node, role)
        .unwrap()
        .schema()
        .clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    let data = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    let mut arrays = vec![original_operand.unwrap_or_else(|| input.clone())];
    arrays.extend(candidates);
    let old = original(arrays, negated).unwrap();
    for rows in [
        (0..input.len()).collect::<Vec<_>>(),
        (0..input.len()).filter(|r| r % 2 == 0).collect(),
        Vec::new(),
    ] {
        let selection = Selection::try_sparse(input.len(), &rows).unwrap();
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let out = frame.evaluate(&data, selection, &Control).unwrap();
        assert!(out.errors().is_empty());
        assert_eq!(out.selection(), selection);
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
fn ints() -> ArrayRef {
    Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        None,
        Some(9),
    ]))
}
fn texts() -> ArrayRef {
    Arc::new(StringArray::from(vec![
        Some("ASIA"),
        Some("AMERICA"),
        Some("AFRICA"),
        None,
        Some("é中"),
    ]))
}
#[test]
fn inlist_actual_compiler_i32_value_nullable_literal_list() {
    evaluate(
        "SELECT ship_code IN (1,2,NULL) FROM t0",
        Field::new("ship_code", DataType::Int32, true),
        ints(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1); 5])),
            Arc::new(Int32Array::from(vec![Some(2); 5])),
            Arc::new(Int32Array::from(vec![None; 5])),
        ],
        false,
        false,
        None,
    );
}
#[test]
fn inlist_actual_compiler_utf8_value_nullable_original_region_source() {
    evaluate(
        "SELECT c_region IN ('ASIA','AMERICA') FROM t_filter_customer_metrics",
        Field::new("c_region", DataType::Utf8, true),
        texts(),
        vec![
            Arc::new(StringArray::from(vec![Some("ASIA"); 5])),
            Arc::new(StringArray::from(vec![Some("AMERICA"); 5])),
        ],
        false,
        false,
        None,
    );
}
#[test]
fn inlist_actual_compiler_i32_notin_truthonly_original_scan_source() {
    evaluate(
        "SELECT ship_code FROM t0 WHERE ship_code NOT IN (1,2)",
        Field::new("ship_code", DataType::Int32, true),
        ints(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1); 5])),
            Arc::new(Int32Array::from(vec![Some(2); 5])),
        ],
        true,
        true,
        None,
    );
}
#[test]
fn inlist_actual_compiler_utf8_truthonly_original_required_predicate() {
    evaluate(
        "SELECT c_region FROM t_filter_customer_metrics WHERE c_region IN ('ASIA','AMERICA')",
        Field::new("c_region", DataType::Utf8, true),
        texts(),
        vec![
            Arc::new(StringArray::from(vec![Some("ASIA"); 5])),
            Arc::new(StringArray::from(vec![Some("AMERICA"); 5])),
        ],
        false,
        true,
        None,
    );
}

#[test]
fn inlist_actual_compiler_utf8_case_truthonly_original_required_source() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(100),
        Some(85),
        Some(70),
        None,
        Some(59),
    ]));
    let operand = original_required_case_operand(input.clone());
    evaluate(
        "SELECT ship_code FROM t0 WHERE (CASE WHEN ship_code >= 90 THEN 'A' WHEN ship_code >= 80 THEN 'B' WHEN ship_code >= 70 THEN 'C' WHEN ship_code >= 60 THEN 'D' ELSE 'E' END) IN ('A','B')",
        Field::new("ship_code", DataType::Int32, true),
        input,
        vec![
            Arc::new(StringArray::from(vec![Some("A"); 5])),
            Arc::new(StringArray::from(vec![Some("B"); 5])),
        ],
        false,
        true,
        Some(operand),
    );
}
