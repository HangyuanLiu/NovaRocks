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

//! Signed IN source/ordered lifecycle probes. UNRUN until serialized root tests.
//! Formal host MEM grants and the Utf8 Variant environment remain separate gates.
use super::compiled_program::CompiledExpressionInstance;
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_single_field_and_core_residuals;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source;
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramRootInput, StaticExprKind, root_input_layout,
};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_type_contract::{CompileControlError, CompilePhase, ControlShape, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
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
        panic!("IN never waits")
    }
}
fn compiled(sql: &str, dtype: DataType) -> (Arc<LocalProgram>, ProgramExpressionRootSite) {
    let source = sql_source_with_single_field_and_core_residuals(
        sql,
        Field::new("k", dtype, true),
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        for (&site, &root) in snapshot.bindings() {
            let flow = &snapshot.flows()[&site.arena()];
            let occurrence = &flow.uses()[&root];
            let definition = snapshot.roots().arenas()[&site.arena()]
                .node(occurrence.definition)
                .unwrap();
            if matches!(definition.kind(), StaticExprKind::PreparedInList { .. }) {
                assert!(found.is_none(), "one actual IN root");
                found = Some((program.clone(), site));
            }
        }
    }
    found.expect("actual SQL IN root survived original lowering")
}
fn batch(program: &LocalProgram, site: ProgramExpressionRootSite, input: ArrayRef) -> RecordBatch {
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), site).unwrap()
    else {
        panic!("actual source input");
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
    RecordBatch::try_new(schema, vec![input]).unwrap()
}
fn values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn inlist_signed_actual_source_definitions_ordered_uses_guards_and_exact_recipe() {
    let (program, site) = compiled("SELECT k IN (k,-k,k) FROM t0", DataType::Int64);
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let flow = &snapshot.flows()[&site.arena()];
    let occurrence = &flow.uses()[&snapshot.bindings()[&site]];
    assert_eq!(
        occurrence.control,
        ControlShape::Membership { negated: false }
    );
    assert_eq!(occurrence.arguments.len(), 4);
    assert_ne!(occurrence.arguments[0], occurrence.arguments[1]);
    assert_ne!(occurrence.arguments[1], occurrence.arguments[3]);
    let definition = snapshot.roots().arenas()[&site.arena()]
        .node(occurrence.definition)
        .unwrap();
    let StaticExprKind::PreparedInList { child, values, .. } = definition.kind() else {
        panic!("actual original IN definition");
    };
    // SQL's original lowering owns each definition. Repeated lexical `k`
    // does not authorize this test to invent CSE or an aliasing receipt.
    assert_eq!(flow.uses()[&occurrence.arguments[0]].definition, *child);
    assert_eq!(values.len(), 3);
    for (ordinal, candidate) in values.iter().enumerate() {
        assert_eq!(
            flow.uses()[&occurrence.arguments[ordinal + 1]].definition,
            *candidate
        );
    }
    for ordinal in 1..4 {
        let child = &flow.uses()[&occurrence.arguments[ordinal]];
        assert_eq!(
            child.context.demand,
            novarocks_type_contract::EvaluationDemand::Value
        );
        assert_eq!(
            flow.domains()[&child.context.domain].guard.unwrap().kind,
            novarocks_type_contract::GuardKind::MembershipAfterSource {
                ordinal: (ordinal - 1) as u32
            }
        );
    }
    let recipe = program
        .native_inlist_recipe(novarocks_local_program::ProgramUseRef {
            arena: site.arena(),
            use_id: snapshot.bindings()[&site],
        })
        .unwrap();
    assert_eq!(recipe.source_type().data_type, DataType::Int64);
    assert_eq!(recipe.candidate_types().len(), 3);
    assert_eq!(recipe.result_type().data_type, DataType::Boolean);
    assert!(recipe.result_type().nullable);
}
#[test]
fn inlist_signed_actual_all_widths_value_truthonly_null_sparse_sliced_empty() {
    for (ty, input) in [
        (
            DataType::Int8,
            Arc::new(
                Int8Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
        (
            DataType::Int16,
            Arc::new(
                Int16Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
        (
            DataType::Int32,
            Arc::new(
                Int32Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
        (
            DataType::Int64,
            Arc::new(
                Int64Array::from(vec![Some(99), Some(1), Some(3), None, Some(2), Some(98)])
                    .slice(1, 4),
            ) as ArrayRef,
        ),
    ] {
        let name = match ty {
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INT",
            DataType::Int64 => "BIGINT",
            _ => unreachable!(),
        };
        let positive = format!(
            "SELECT k IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name})) FROM t0"
        );
        let negative = format!(
            "SELECT k NOT IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name})) FROM t0"
        );
        let positive_truth = format!(
            "SELECT k FROM t0 WHERE k IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name}))"
        );
        let negative_truth = format!(
            "SELECT k FROM t0 WHERE k NOT IN (CAST(1 AS {name}),CAST(2 AS {name}),CAST(NULL AS {name}))"
        );
        for (sql, expected) in [
            (positive.as_str(), vec![Some(true), None, None, Some(true)]),
            (
                negative.as_str(),
                vec![Some(false), None, None, Some(false)],
            ),
            (
                positive_truth.as_str(),
                vec![Some(true), Some(false), Some(false), Some(true)],
            ),
            (negative_truth.as_str(), vec![Some(false); 4]),
        ] {
            let (program, site) = compiled(sql, ty.clone());
            let snapshot = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            let use_id = snapshot.bindings()[&site];
            let recipe = program
                .native_inlist_recipe(novarocks_local_program::ProgramUseRef {
                    arena: site.arena(),
                    use_id,
                })
                .unwrap();
            assert_eq!(recipe.source_type().data_type, ty);
            assert!(
                recipe
                    .candidate_types()
                    .iter()
                    .all(|candidate| candidate.data_type == ty)
            );
            let data = batch(&program, site, input.clone());
            for rows in [vec![0, 1, 2, 3], vec![1, 3], vec![]] {
                let selection = Selection::try_sparse(4, &rows).unwrap();
                let mut frame =
                    CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
                let out = frame.evaluate(&data, selection, &Control).unwrap();
                assert!(out.errors().is_empty());
                assert_eq!(
                    values(&out),
                    rows.iter().map(|r| expected[*r]).collect::<Vec<_>>()
                );
            }
        }
    }
}
fn original_negate_after_match_error() -> String {
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use novarocks_types::SlotId;
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![i64::MIN, 7, 0]))],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(data.schema().as_ref(), &[SlotId::new(17)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(data, schema);
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int64);
    let zero = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(0)), DataType::Int64);
    let negative = arena.push_typed(
        ExprNode::Sub(
            zero,
            input,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        ),
        DataType::Int64,
    );
    let root = arena.push_typed(
        ExprNode::In {
            child: input,
            values: vec![input, negative],
            is_not_in: false,
        },
        DataType::Boolean,
    );
    arena.eval(root, &chunk).unwrap_err()
}
#[test]
fn inlist_signed_actual_match_never_masks_later_data_error_and_sparse_isolates_it() {
    let full = original_negate_after_match_error();
    for sql in [
        "SELECT k IN (k,-k) FROM t0",
        "SELECT k NOT IN (k,-k) FROM t0",
    ] {
        let negated = sql.contains("NOT IN");
        let (program, site) = compiled(sql, DataType::Int64);
        let data = batch(
            &program,
            site,
            Arc::new(Int64Array::from(vec![i64::MIN, 7, 0])),
        );
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let out = frame.evaluate(&data, Selection::all(3), &Control).unwrap();
        assert_eq!(values(&out), vec![None, Some(!negated), Some(!negated)]);
        assert_eq!(out.errors().len(), 1);
        assert_eq!(out.errors()[0].selected_ordinal(), 0);
        assert_eq!(out.errors()[0].message(), full);
        let rows = [1, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let out = frame.evaluate(&data, selection, &Control).unwrap();
        assert!(out.errors().is_empty());
        assert_eq!(values(&out), vec![Some(!negated); 2]);
    }
}
struct Callback {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
}
impl Callback {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            stop,
            cause,
        }
    }
}
impl KernelEvaluationControl for Callback {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        assert!(t.len() < self.stop, "no callback after primary refusal");
        t.push(n);
        if t.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("in-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("in-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("in-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn inlist_signed_actual_every_constructor_phase_callback_seven_causes_no_footer_or_replay() {
    let (program, site) = compiled("SELECT k IN (1,5,NULL) FROM t0", DataType::Int64);
    let data = batch(&program, site, Arc::new(Int64Array::from(vec![0, 3, 9])));
    let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
    CompiledExpressionInstance::try_new(program.clone(), site, &record).unwrap();
    let trace = record.trace.lock().unwrap().clone();
    for stop in 1..=trace.len() {
        for cause in causes() {
            let c = Callback::new(stop, cause.clone());
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.clone(),site,&c),Err(actual) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
        }
    }
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        frame.evaluate(&data, selection, &record).unwrap();
        let trace = record.trace.lock().unwrap().clone();
        for stop in 1..=trace.len() {
            for cause in causes() {
                let mut frame =
                    CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
                let c = Callback::new(stop, cause.clone());
                assert!(matches!(frame.evaluate(&data,selection,&c),Err(actual) if actual==cause));
                assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
                let after = Callback::new(usize::MAX, KernelFailure::Cancelled);
                assert!(matches!(
                    frame.evaluate(&data, selection, &after),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn inlist_signed_actual_original_control_codec_roundtrip_and_invalid_ordered_use() {
    for (sql, negated) in [
        ("SELECT k IN (1,5,NULL) FROM t0", false),
        ("SELECT k NOT IN (1,5,NULL) FROM t0", true),
    ] {
        let source = sql_source_with_single_field_and_core_residuals(
            sql,
            Field::new("k", DataType::Int64, true),
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        );
        let policy = novarocks_functions::ConstantPolicy {
            max_rows: 64,
            max_array_nodes: 1024,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        };
        let semantics =
            novarocks_sql::compiler::author_fragment_package_semantics(&source, policy, &Control)
                .unwrap();
        let mut seen = 0;
        for (id, semantic) in semantics {
            let fragment = &source.plan().fragments()[&id];
            let mut encoded = novarocks_plan_codec::physical_control_v2::encode_expression_control(
                &semantic.expression_uses,
                &Control,
            )
            .unwrap();
            let decoded = novarocks_plan_codec::physical_control_v2::decode_expression_control(
                fragment, &encoded, &Control,
            )
            .unwrap();
            assert_eq!(decoded.flow(), semantic.expression_uses.flow());
            assert_eq!(decoded.bindings(), semantic.expression_uses.bindings());
            let before = seen;
            for (&id, invocation) in semantic.expression_uses.flow().uses() {
                if invocation.control == (ControlShape::Membership { negated }) {
                    let emitted = encoded
                        .uses
                        .iter_mut()
                        .find(|entry| entry.id == id.get())
                        .unwrap();
                    assert_eq!(emitted.argument_use_ids.len(), 4);
                    // No invented expression ID: remove one original use to
                    // verify the existing codec/source correspondence author.
                    emitted.argument_use_ids.pop();
                    seen += 1;
                }
            }
            if seen != before {
                assert!(
                    novarocks_plan_codec::physical_control_v2::decode_expression_control(
                        fragment, &encoded, &Control,
                    )
                    .is_err()
                );
            }
        }
        assert_eq!(seen, 1);
    }
}
