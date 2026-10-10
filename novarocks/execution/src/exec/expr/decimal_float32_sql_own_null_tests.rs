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
//! Actual non-NULL Connector SQL source -> owned transaction -> LocalCompiler -> Frame.
//! The original declaration is a public obligation, not the internal Cast admission.
use super::compiled_program::CompiledExpressionInstance;
use super::legacy_decimal_float32_cast_baseline_tests::{actual, assert_same, input};
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use super::numeric_unary_owned_transaction_tests::{batch, producer_root, programs};
use arrow::array::{Array, Float32Array, UInt32Array};
use arrow::datatypes::DataType;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_physical_plan::ExprKind;
use novarocks_query_application::api::QueryExecutionKind;
use novarocks_query_application::preparation::{CompletedPhysicalPlanCandidate, OutputContract};
use novarocks_sql::compiler::{SqlCompileControl, SqlPhysicalEmissionMode};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, PureCompileControl,
};
use std::time::Duration;

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
        panic!("Decimal Float32 SQL cast never waits")
    }
}

fn same_source_case(wide: bool, scale: i8, expected_null_values: bool) {
    let dtype = if wide {
        DataType::Decimal256(76, scale)
    } else {
        DataType::Decimal128(38, scale)
    };
    let sql = "SELECT CAST(k AS FLOAT) AS original_cast FROM fixture";
    // ColumnDef and Arrow Field are authored false by the existing real connector seam.
    let before = sql_source(
        sql,
        dtype.clone(),
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    let source = sql_source(
        sql,
        dtype.clone(),
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    let original = source
        .checked_original_result_declaration_observed(&Control)
        .unwrap()
        .unwrap();
    let computed = source.plan().result_port().unwrap();
    assert_eq!(
        original.fields(),
        before.plan().result_port().unwrap().fields.as_ref()
    );
    assert_eq!(original.fields().len(), 1);
    assert_eq!(original.fields()[0].ty.data_type, DataType::Float32);
    // Explicit SQL CAST is originally nullable=true, even from a non-NULL column.
    // Do not manufacture a false public obligation for this actual source shape.
    assert!(original.fields()[0].ty.nullable);
    assert!(computed.fields[0].ty.nullable);
    assert_eq!(computed.output, original.original_port().output);
    let mut casts = 0;
    for fragment in source.plan().fragments().values() {
        for (_, node) in fragment.expressions().iter() {
            if let ExprKind::Cast { expr, target, .. } = &node.kind {
                let operand = fragment.expressions().get(*expr).unwrap();
                if operand.ty.data_type == dtype && target == &DataType::Float32 {
                    casts += 1;
                    assert!(!operand.ty.nullable);
                    assert!(node.ty.nullable);
                    // Cast target is a DataType, not a second root-nullability declaration.
                    assert_eq!(&node.ty.data_type, target);
                }
            }
        }
    }
    assert!(
        casts > 0,
        "actual SQL must retain the real Decimal-to-FLOAT Cast"
    );
    let candidate =
        CompletedPhysicalPlanCandidate::for_sql_program(source.clone(), &Control).unwrap();
    let output =
        OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate).unwrap();
    assert_eq!(output.fields()[0].name(), "original_cast");
    assert_eq!(output.fields()[0].data_type(), &DataType::Float32);
    assert!(output.fields()[0].nullable());
    let programs = programs(&source);
    let (program, root) = producer_root(&programs);
    let values = input(
        wide,
        if wide { 76 } else { 38 },
        scale,
        vec![Some(0), Some(4), Some(-4), Some(1)],
    );
    let data = batch(&program, root, values.clone());
    let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
    let selected = frame.evaluate(&data, Selection::all(4), &Control).unwrap();
    assert!(selected.errors().is_empty());
    let old = actual(&values, DecimalOverflowPolicy::OutputNull, false).unwrap();
    assert_same(selected.values(), &old);
    let floats = selected
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(floats.value(0).to_bits(), 0f32.to_bits());
    if expected_null_values {
        assert!(floats.is_null(1));
        assert!(floats.is_null(2));
    } else if wide && scale == -38 {
        assert_eq!(floats.value(1), f32::INFINITY);
        assert_eq!(floats.value(2), f32::NEG_INFINITY);
        assert_eq!(floats.null_count(), 0);
    }
    let rows = [0, 1, 3];
    let sparse = frame
        .evaluate(&data, Selection::try_sparse(4, &rows).unwrap(), &Control)
        .unwrap();
    let taken = arrow::compute::take(
        old.as_ref(),
        &UInt32Array::from(rows.map(|r| r as u32).to_vec()),
        None,
    )
    .unwrap();
    assert_same(sparse.values(), &taken);
    assert_eq!(
        frame
            .evaluate(&data, Selection::try_sparse(4, &[]).unwrap(), &Control)
            .unwrap()
            .values()
            .len(),
        0
    );
}

#[test]
fn decimal_float32_sql_explicit_cast_nonnull_decimal128_negative_scale_preserves_original_public_true()
 {
    same_source_case(false, -38, true);
}
#[test]
fn decimal_float32_sql_explicit_cast_nonnegative_decimal128_and_decimal256_preserve_original_projection()
 {
    same_source_case(false, 0, false);
    // Wide Decimal has no genuine ConnectorValueType assignment. Its entire
    // Float32 recipe/Frame domain stays in the existing direct compiler oracles.
}

#[test]
fn decimal_float32_sql_explicit_cast_actual_decimal256_literal_and_unary_frame_preserve_infinity() {
    use arrow::datatypes::Schema;
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use novarocks_local_program::{
        ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
    };
    use std::sync::Arc;
    // The point makes this a genuine decimal token; >38 digits is authored
    // Decimal256 by the original analyzer, not a retagged scan or guessed CV.
    let literal = "4000000000000000000000000000000000000000.0";
    for negative in [false, true] {
        let sql = format!(
            "SELECT CAST({}{literal} AS FLOAT) AS wide_literal",
            if negative { "-" } else { "" }
        );
        let source = sql_source(
            &sql,
            DataType::Int8,
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        );
        let mut actual_wide = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                assert!(!matches!(
                    node.kind,
                    novarocks_physical_plan::NodeKind::Scan { .. }
                ));
            }
            for (_, expression) in fragment.expressions().iter() {
                if matches!(expression.ty.data_type, DataType::Decimal256(p, s) if p > 38 && s == 1)
                {
                    actual_wide += 1;
                }
            }
        }
        assert!(
            actual_wide > 0,
            "the actual SQL literal author must retain Decimal256"
        );
        let original = source.original_public_result_declaration().unwrap();
        assert_eq!(original.fields()[0].ty.data_type, DataType::Float32);
        assert!(original.fields()[0].ty.nullable);
        let lowered = programs(&source);
        // Borrow actual compiled root bindings. Project is preferred when the
        // original optimizer retained it; a dynamic Values cell has the same
        // documented empty one-row port. No alternate graph is constructed.
        let mut selected = None;
        for program in lowered.values() {
            let snapshot = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            for &site in snapshot.bindings().keys() {
                if matches!(
                    site,
                    ProgramExpressionRootSite::Node {
                        role: ProgramNodeExpressionRole::ProjectOutput { .. },
                        ..
                    }
                ) {
                    assert!(selected.is_none());
                    selected = Some((program.clone(), site));
                }
            }
        }
        if selected.is_none() {
            for program in lowered.values() {
                let snapshot = program
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                for &site in snapshot.bindings().keys() {
                    if matches!(
                        site,
                        ProgramExpressionRootSite::Node {
                            role: ProgramNodeExpressionRole::ValuesCell { .. },
                            ..
                        }
                    ) {
                        assert!(selected.is_none());
                        selected = Some((program.clone(), site));
                    }
                }
            }
        }
        let (program, root) =
            selected.expect("actual SQL literal Cast must retain a runtime projection/cell");
        let ProgramExpressionRootSite::Node { node, role } = root else {
            unreachable!()
        };
        let empty_one_row = |schema: Arc<Schema>| {
            assert!(
                schema.fields().is_empty(),
                "dynamic literal cell reads the exact empty port"
            );
            RecordBatch::try_new_with_options(
                schema,
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )
            .unwrap()
        };
        let port = match role {
            ProgramNodeExpressionRole::ProjectOutput { .. } => {
                let ProgramNodeKind::Project { input, .. } =
                    program.graph().nodes()[node.index()].kind()
                else {
                    unreachable!()
                };
                let parent = &program.graph().nodes()[input.index()];
                match parent.kind() {
                    // If the actual optimizer authored a materialized VALUES
                    // input, consume its original checked batch directly.
                    ProgramNodeKind::Values { values } => match values.batch() {
                        Some(batch) => batch.clone(),
                        None => empty_one_row(parent.output_layout().schema().clone()),
                    },
                    _ => empty_one_row(parent.output_layout().schema().clone()),
                }
            }
            ProgramNodeExpressionRole::ValuesCell { .. } => {
                empty_one_row(Arc::new(Schema::empty()))
            }
            _ => unreachable!(),
        };
        let mut frame = CompiledExpressionInstance::try_new(program, root, &Control).unwrap();
        let output = frame.evaluate(&port, Selection::all(1), &Control).unwrap();
        assert!(output.errors().is_empty());
        let value = output
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(value.null_count(), 0);
        assert_eq!(
            value.value(0),
            if negative {
                f32::NEG_INFINITY
            } else {
                f32::INFINITY
            }
        );
        assert_eq!(
            frame
                .evaluate(&port, Selection::try_sparse(1, &[]).unwrap(), &Control)
                .unwrap()
                .values()
                .len(),
            0
        );
    }
}
