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

//! Genuine shared-codec sources, decoded by the production Native namespace.
use super::*;
use crate::{
    fragment_original_requests::{OriginalOccurrence, decode_frozen_selection},
    fragment_type_decode::decode_fragment_type_owner,
    native_type_construction::NativeTypeConstruction,
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_functions::{ConstantPolicy, FunctionValueType};
use novarocks_physical_plan::{ConstantPool, ConstantPoolId};
use novarocks_plan_codec::{
    execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
    },
    physical_constant_v2::prepare_constant_record_write_pure_in,
    physical_type_v2::{TypeProjectionLimits, encode_source_field_table_pure_in},
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{common, expr};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::sync::Arc;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn zero_source_policy() -> call_request::SourceConstantPolicy {
    call_request::SourceConstantPolicy {
        max_rows: Some(0),
        max_array_nodes: Some(0),
        max_logical_elements: Some(0),
        max_retained_buffer_bytes: Some(0),
        max_type_depth: Some(0),
        max_type_nodes: Some(0),
        max_dictionary_depth: Some(0),
        max_metadata_bytes: Some(0),
        max_library_validation_work: Some(0),
        max_library_validation_bytes: Some(0),
    }
}
fn reference(pool: u32, row: u32) -> call_request::ConstantReference {
    call_request::ConstantReference {
        pool_id: Some(pool),
        row_ordinal: row,
    }
}
fn expression(pool: u32, row: u32) -> expr::Expr {
    expr::Expr {
        original_definition_id: None,
        r#type: Some(common::TypeDesc {
            value_binding_id: Some(0),
        }),
        nullable: false,
        kind: Some(expr::expr::Kind::ConstantReference(reference(pool, row))),
    }
}
fn source() -> (plan::PlanFragment, FunctionValueType) {
    // The fixture is an explicit scalar author, not a type inferred from IPC.
    let value = FunctionValueType::new(DataType::Int64, false);
    let field = Arc::new(Field::new("original constant", DataType::Int64, false));
    // ConstantPool's source metadata model includes Field names. This scalar
    // author has one named Field, no attributes, children or timezone spelling.
    let source_metadata_bytes = u64::try_from(field.name().len()).unwrap();
    let pool = ConstantPool::try_new(
        field.clone(),
        value.clone(),
        Int64Array::from(vec![7, 8]).to_data(),
        ConstantPolicy {
            max_rows: 2,
            max_array_nodes: 1,
            max_logical_elements: 2,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 1,
            max_metadata_bytes: source_metadata_bytes,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        },
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    assert_eq!(pool.resource_facts().metadata_bytes, source_metadata_bytes);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let roots = [ExecutionTypeRoot {
        id: 0,
        value: &value,
    }];
    let execution = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(1).unwrap(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap()
        .into_wire();
    let fields = encode_source_field_table_pure_in(
        &[(0, &field)],
        TypeProjectionLimits::for_validated_source_root_count(1).unwrap(),
        &mut work,
    )
    .unwrap();
    let mut constants = Vec::new();
    for id in [0, u32::MAX] {
        constants.push(
            prepare_constant_record_write_pure_in(ConstantPoolId::new(id), 0, 0, &pool, &mut work)
                .unwrap()
                .emit_in(&mut work)
                .unwrap(),
        );
    }
    work.finish().unwrap();
    let request = call_request::OriginalCallRequest {
        definition: Some(call_request::CallRequestDefinition {
            kind: Some(call_request::call_request_definition::Kind::ExpressionDefinitionId(17)),
        }),
        arguments: vec![call_request::OriginalFunctionArgument {
            kind: Some(call_request::original_function_argument::Kind::Value(
                call_request::OriginalValueArgument {
                    value_type_id: Some(0),
                    constant: Some(reference(0, 0)),
                },
            )),
        }],
        logical_argument_count: Some(1),
        expected_result_value_type_id: None,
        constant_policy: Some(zero_source_policy()),
        request_id: Some(0),
    };
    (
        plan::PlanFragment {
            execution_types: Some(execution),
            constant_source_fields: Some(fields),
            constants,
            original_call_requests: Some(call_request::FragmentCallRequests {
                entries: vec![request],
            }),
            ..Default::default()
        },
        value,
    )
}

#[test]
fn native_namespace_constant_references_keep_actual_rows_and_pool_identity() {
    let (source, value) = source();
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "native original source test",
    );
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
    let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
    let types = decode_fragment_type_owner(&source, &mut resources).unwrap();
    let requests = decode_original_requests(&source, &types, &mut resources).unwrap();
    let first = requests
        .constant(&reference(0, 0), &value, &mut resources)
        .unwrap();
    let second = requests
        .constant(&reference(0, 1), &value, &mut resources)
        .unwrap();
    let other_pool = requests
        .constant(&reference(u32::MAX, 0), &value, &mut resources)
        .unwrap();
    assert_eq!(
        first
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(first.ordinal() as usize),
        7
    );
    assert_eq!(
        second
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(second.ordinal() as usize),
        8
    );
    assert!(
        !first
            .equals_observed(&second, CompilePhase::Validate, &Control)
            .unwrap()
    );
    assert!(
        first
            .equals_observed(&other_pool, CompilePhase::Validate, &Control)
            .unwrap()
    );
    requests
        .validate_actual_arguments(0, [&expression(0, 0)].into_iter(), &mut resources)
        .unwrap();
    for wrong in [expression(0, 1), expression(u32::MAX, 0)] {
        assert!(
            requests
                .validate_actual_arguments(0, [&wrong].into_iter(), &mut resources)
                .unwrap_err()
                .contains("constant address")
        );
    }
    assert!(
        requests
            .constant(&reference(0, 2), &value, &mut resources)
            .is_err()
    );
    drop(second);
    drop(other_pool);
    resources.finish().unwrap();
    drop(requests);
    drop(types);
    drop(creation);
    // This currently retains the construction upper, not precise live bytes.
    assert!(tracker.current() > 0);
    drop(first);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_original_definition_is_exact_and_precedes_selected_identity_creation() {
    let (source, _) = source();
    let tracker =
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("native occurrence test");
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
    let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
    let types = decode_fragment_type_owner(&source, &mut resources).unwrap();
    let requests = decode_original_requests(&source, &types, &mut resources).unwrap();
    let correct = OriginalOccurrence::Window {
        expression: Some(17),
    }
    .definition()
    .unwrap();
    requests
        .validate_definition(0, correct, &mut resources)
        .unwrap();
    assert!(
        OriginalOccurrence::Window { expression: None }
            .definition()
            .is_err()
    );
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let signature = plan::ResolvedAggregateSignature {
        original_request_id: Some(0),
        function_id: "never construct this selection".into(),
        ..Default::default()
    };
    for wrong in [
        OriginalOccurrence::Window {
            expression: Some(18),
        }
        .definition()
        .unwrap(),
        OriginalOccurrence::Aggregate { node: 17, call: 0 }
            .definition()
            .unwrap(),
    ] {
        let held = tracker.current();
        let error = decode_frozen_selection(
            &signature,
            &types.execution,
            &requests,
            &catalog,
            FieldPath::root("aggregate_binding"),
            wrong,
            &mut resources,
        )
        .unwrap_err();
        assert!(error.contains("different definition"), "{error}");
        assert_eq!(
            tracker.current(),
            held,
            "no selected identity allocation before the definition gate"
        );
    }
    resources.finish().unwrap();
    drop(requests);
    drop(types);
    drop(creation);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_original_namespace_validates_unused_sources_and_sparse_request_ids() {
    for case in 0..3 {
        let (mut source, _) = source();
        match case {
            0 => source.constants[1].source_field_id = Some(9),
            1 => {
                let mut unused = source.original_call_requests.as_ref().unwrap().entries[0].clone();
                unused.definition.as_mut().unwrap().kind =
                    Some(call_request::call_request_definition::Kind::ExpressionDefinitionId(18));
                source
                    .original_call_requests
                    .as_mut()
                    .unwrap()
                    .entries
                    .push(unused);
            }
            _ => source.original_call_requests.as_mut().unwrap().entries[0].request_id = None,
        }
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "native unused source test",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let types = decode_fragment_type_owner(&source, &mut resources).unwrap();
        assert!(decode_original_requests(&source, &types, &mut resources).is_err());
        drop(resources);
        drop(types);
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn native_original_source_first_capacity_refusal_does_not_reach_later_invalid_source() {
    let (mut source, _) = source();
    source.constants[1].source_field_id = Some(9);
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "native source refusal test",
    );
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
    let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
    let types = decode_fragment_type_owner(&source, &mut resources).unwrap();
    let held = tracker.current();
    tracker.install_limit_once(held).unwrap();
    let error = decode_original_requests(&source, &types, &mut resources)
        .err()
        .unwrap();
    assert!(
        matches!(error.construction_cause(), Some(CreationError::Capacity(_))),
        "{error}"
    );
    assert_eq!(
        tracker.current(),
        held,
        "failed request rolls back without publishing another owner"
    );
    drop(resources);
    drop(types);
    drop(creation);
    assert_eq!(tracker.current(), 0);
}

fn late_constant_pool(
    unrelated_bytes: usize,
) -> (
    ConstantPool,
    Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
) {
    let (mut source, value) = source();
    let kept = Arc::new(Field::new("original constant", DataType::Int64, false));
    let unused = Arc::new(
        Field::new("unused source", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                "provider.unused".to_owned(),
                "u".repeat(unrelated_bytes),
            )]),
        ),
    );
    let mut encode = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    source.constant_source_fields = Some(
        encode_source_field_table_pure_in(
            &[(0, &kept), (17, &unused)],
            TypeProjectionLimits::for_validated_source_root_count(2).unwrap(),
            &mut encode,
        )
        .unwrap(),
    );
    encode.finish().unwrap();
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "late original constant Pool",
    );
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
    creation
        .request_layout(std::alloc::Layout::array::<u64>(unrelated_bytes).unwrap())
        .unwrap();
    let unrelated = Vec::<u64>::with_capacity(unrelated_bytes);
    let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
    let types = decode_fragment_type_owner(&source, &mut resources).unwrap();
    let requests = decode_original_requests(&source, &types, &mut resources).unwrap();
    let constant = requests
        .constant(&reference(0, 1), &value, &mut resources)
        .unwrap();
    let pool = constant.pool().clone();
    assert_eq!(
        pool.array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, 8]
    );
    assert!(!pool.array().is_null(constant.ordinal() as usize));
    assert!(pool.field().metadata().is_empty());
    drop(constant);
    drop(requests);
    drop(types);
    resources.finish().unwrap();
    drop(unrelated);
    drop(creation);
    (pool, tracker)
}

#[test]
fn late_native_constant_pool_excludes_unused_sources_main_and_preparation_scratch() {
    let (small, small_tracker) = late_constant_pool(1);
    let (late, tracker) = late_constant_pool(1024);
    assert!(tracker.current() > 0);
    assert_eq!(
        tracker.current(),
        small_tracker.current(),
        "only the selected Pool and its actual source/type loans survive"
    );
    assert_eq!(late.field(), small.field());
    assert_eq!(late.value_type(), small.value_type());
    drop(late);
    assert_eq!(tracker.current(), 0);
    drop(small);
    assert_eq!(small_tracker.current(), 0);
}

#[test]
fn late_native_constant_array_projection_releases_the_pool_holder_responsibility() {
    let (pool, tracker) = late_constant_pool(1024);
    let pool_current = tracker.current();
    let quoted = tracker.allocated();
    // Keep the real immutable complete declaration independently alive. The
    // original release oracle concerns the Pool holder/data table, while an
    // Array projection deliberately carries no semantic declaration.
    let declaration = pool.value_type().clone();
    assert_eq!(tracker.current(), pool_current);
    assert_eq!(tracker.allocated(), quoted);
    let array = pool.array_projection();
    drop(pool);
    // The independent projection keeps source receipts and Arrow result
    // backing, while the PoolBacking reservation has actually exited.
    assert_eq!(
        pool_current - tracker.current(),
        (crate::native_type_construction::NativeTypeScope::reservation_allocation_layout()
            .unwrap()
            .size()
            + ConstantPool::backing_allocation_layout().size()
            + std::alloc::Layout::array::<arrow::buffer::Buffer>(1)
                .unwrap()
                .size()) as i64
    );
    let array_and_declaration = tracker.current();
    assert_eq!(declaration.data_type(), &DataType::Int64);
    assert!(!declaration.nullable());
    drop(declaration);
    assert!(
        tracker.current() < array_and_declaration,
        "the independently held complete logical/layout declaration exits separately from Arrow/source backing"
    );
    assert_eq!(
        array
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, 8]
    );
    assert!(tracker.current() > 0);
    drop(array);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_pool_materialization_first_actual_scope_refusal_has_no_control_or_array_tail() {
    use novarocks_plan_codec::{
        host_projection_v2::ProjectionFailure, physical_constant_v2, physical_type_v2,
    };
    use novarocks_type_contract::ValueSchemaCreation;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Polls(AtomicUsize);
    impl PureCompileControl for Polls {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let control = Polls(AtomicUsize::new(0));
    let (raw, _) = source();
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "pool materialization refusal",
    );
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
    let mut resources = NativeDecodeResources::try_new(&actual).unwrap();
    let types = decode_fragment_type_owner(&raw, &mut resources).unwrap();
    let factory = crate::native_type_construction::NativeTypeFactory::new(&actual);
    let mut invoice = raw_source_bytes(&raw, &mut resources).unwrap();
    invoice = invoice
        .checked_add(types.execution.retained_source_bytes_upper_bound())
        .unwrap();
    let fields = {
        let mut exact = |layout| actual.request_layout(layout);
        let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
        physical_type_v2::decode_source_field_table_input_derived_with_factory(
            raw.constant_source_fields.as_ref().unwrap(),
            invoice,
            &mut creation,
            &factory,
            &mut resources.work,
        )
        .unwrap()
    };
    invoice = invoice
        .checked_add(fields.creation_request_bytes_upper_bound().unwrap())
        .unwrap();
    let field = fields
        .field_borrow_observed(0, &mut resources.work)
        .unwrap()
        .unwrap();
    let declared = types
        .execution
        .get_observed(0, &mut resources.work)
        .unwrap()
        .unwrap();
    let prepared =
        physical_constant_v2::prepare_constant_record_from_sources_input_derived_with_factory_in(
            &raw.constants[0],
            declared,
            field.field(),
            field.storage_lifetime(),
            fields.metadata_namespace().unwrap(),
            invoice,
            invoice,
            &factory,
            &mut |_| Ok(()),
            &mut resources.work,
        )
        .unwrap();
    let prior = tracker.current();
    let allocated = tracker.allocated();
    tracker.install_limit_once(prior).unwrap();
    let mut seen = 0;
    let mut last_poll = 0;
    let error = prepared
        .materialize_with_factory_in(
            &factory,
            &mut |_| {
                seen += 1;
                last_poll = control.0.load(Ordering::SeqCst);
                Ok(())
            },
            &mut resources.work,
        )
        .err()
        .unwrap();
    assert!(
        matches!(error, ProjectionFailure::Host(CreationError::Capacity(_))),
        "{error}"
    );
    assert_eq!(seen, 1, "only the first actual operation scope is observed");
    assert_eq!(
        control.0.load(Ordering::SeqCst),
        last_poll,
        "original capacity refusal has no checkpoint/footer tail"
    );
    assert_eq!(
        tracker.allocated() - allocated,
        crate::native_type_construction::NativeTypeScope::reservation_allocation_layout()
            .unwrap()
            .size() as i64,
        "the prospective scope request is counted once; no Arrow result was entered"
    );
    assert!(
        tracker.current() < prior,
        "consumed preparation/source receipt scopes exit on refusal"
    );
    drop(resources);
    drop(fields);
    drop(types);
    drop(factory);
    drop(actual);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_constant_receipt_namespace_clone_keeps_its_own_small_responsibility() {
    let (pool, tracker) = late_constant_pool(1024);
    let namespace = pool.source_metadata_namespace().unwrap().clone();
    assert_eq!(pool.field().metadata().capacity(), 0);
    // Positive source identity is retained for every actual Field, including
    // a fresh empty attribute map. A receipt row is not an attribute entry.
    assert_eq!(namespace.fields().len(), 1);
    let loan = &namespace.fields()[0];
    assert!(loan.lends(pool.field_ref()));
    let original_address = Arc::as_ptr(pool.field_ref()) as usize;
    assert_eq!(loan.original_address(), original_address);
    drop(pool);
    assert_eq!(namespace.fields()[0].original_address(), original_address);
    assert!(
        namespace.fields()[0].borrow_original_field().is_none(),
        "the independent receipt does not retain a removed Field payload"
    );
    assert!(
        tracker.current() > 0,
        "the independently owned receipt Arc and finite scope remain covered"
    );
    drop(namespace);
    assert_eq!(
        tracker.current(),
        0,
        "no Field table, main scope or controller remains"
    );
}
