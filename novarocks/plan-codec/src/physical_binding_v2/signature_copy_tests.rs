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

use super::*;
use crate::physical_binding_v2::finish;
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::LegacyBindingMetadata;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionArgumentEvaluation, FunctionFailureBehavior,
    FunctionIntrinsicRowError, FunctionVolatility, MAX_VALUE_TYPE_NODES, PureCompileControl,
    ValueLogicalType,
};
use std::{
    alloc::Layout,
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    trace: Vec<u32>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn rejecting(at: usize, cause: CompileControlError) -> Self {
        Self(Mutex::new(State {
            trace: Vec::new(),
            stop: Some((at, cause)),
        }))
    }
    fn trace(&self) -> Vec<u32> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut state = self.0.lock().unwrap();
        let at = state.trace.len();
        if let Some((stop, _)) = state.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        state.trace.push(units);
        match state.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 8,
        max_type_references: 4096,
        max_request_bytes: 4 * 1024 * 1024,
        max_allocation_requests: 8192,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 512 * 1024 * 1024,
    }
}
fn fixture(kind: FunctionKind) -> BoundFunction {
    let field = Arc::new(
        Field::new("original", DataType::Utf8, true)
            .with_metadata(HashMap::from([("unrecognized".into(), "preserved".into())])),
    );
    BoundFunction::from_exact_signature(
        FunctionId::try_new("test/f").unwrap(),
        FunctionOverloadId::try_new("test/o").unwrap(),
        kind,
        vec![
            FunctionArgumentType::Value(FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            )),
            FunctionArgumentType::Lambda {
                parameter_types: vec![
                    FunctionValueType::new(DataType::Int64, false),
                    FunctionValueType::new(DataType::Struct(vec![field].into()), true),
                ]
                .into_boxed_slice(),
                result_type: FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                )
                .unwrap(),
            },
        ]
        .into_boxed_slice(),
        FunctionValueType::new(DataType::Int64, true),
    )
}
fn run(source: &BoundFunction, control: &Control) -> Result<BoundFunction, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
        preflight_scalar_signature_copy(source, &mut model, limits(), &mut work)?;
        copy_scalar_signature_observed(source, &mut work)
    })();
    finish(work, result)
}
fn prefixes(source: &BoundFunction, expected_success: bool) {
    let control = Control::default();
    assert_eq!(run(source, &control).is_ok(), expected_success);
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::rejecting(at, cause);
            assert!(
                matches!(run(source, &control), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn exact_signature_copy_preserves_all_scalar_result_kinds_and_full_types() {
    for kind in [
        FunctionKind::Scalar,
        FunctionKind::Aggregate,
        FunctionKind::Window,
    ] {
        let source = fixture(kind);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
        preflight_scalar_signature_copy(&source, &mut model, limits(), &mut work).unwrap();
        // Two identity Boxes, args Vec+Box, Lambda params Vec+Box, two
        // Dictionary Boxes. Five owned roots; no mirrored datatype walker.
        let bytes = 12
            + 2 * Layout::array::<FunctionArgumentType>(2).unwrap().size()
            + 2 * Layout::array::<FunctionValueType>(2).unwrap().size()
            + 2 * Layout::new::<DataType>().size();
        let retained = 12
            + Layout::array::<FunctionArgumentType>(2).unwrap().size()
            + Layout::array::<FunctionValueType>(2).unwrap().size()
            + 2 * Layout::new::<DataType>().size();
        assert_eq!(model.facts.type_reference_count, 5);
        assert_eq!(model.facts.allocation_requests_upper_bound, 8);
        assert_eq!(model.facts.request_bytes_upper_bound, bytes);
        assert_eq!(model.retained, retained);
        assert_eq!(
            model.facts.coexisting_source_and_request_bytes_upper_bound,
            4096 + bytes
        );
        assert_eq!(
            model.facts.cumulative_work_upper_bound,
            128 + 32 * 5 + 5 * (16 + 8 * MAX_VALUE_TYPE_NODES) + 4 * bytes + 8
        );
        let copied = copy_scalar_signature_observed(&source, &mut work).unwrap();
        finish(work, Ok::<_, BindingCodecError>(())).unwrap();
        assert_eq!(copied.function_id.as_str(), "test/f");
        assert_eq!(copied.overload.as_str(), "test/o");
        assert_eq!(copied.kind, kind);
        assert!(copied.legacy_metadata.is_none());
        assert_eq!(
            copied.result_type,
            FunctionValueType::new(DataType::Int64, true)
        );
        let FunctionArgumentType::Value(dictionary) = &copied.argument_types[0] else {
            panic!("Value")
        };
        assert!(dictionary.nullable);
        assert_eq!(dictionary.logical_type, ValueLogicalType::Physical);
        let FunctionArgumentType::Value(original_dictionary) = &source.argument_types[0] else {
            panic!("source Value")
        };
        let DataType::Dictionary(original_key, original_value) = &original_dictionary.data_type
        else {
            panic!("source Dictionary")
        };
        let DataType::Dictionary(copied_key, copied_value) = &dictionary.data_type else {
            panic!("copied Dictionary")
        };
        assert!(!std::ptr::eq(original_key.as_ref(), copied_key.as_ref()));
        assert!(!std::ptr::eq(
            original_value.as_ref(),
            copied_value.as_ref()
        ));
        assert_eq!(
            dictionary.data_type,
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8))
        );
        let FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } = &copied.argument_types[1]
        else {
            panic!("Lambda")
        };
        assert_eq!(parameter_types.len(), 2);
        assert_eq!(
            parameter_types[0],
            FunctionValueType::new(DataType::Int64, false)
        );
        assert!(parameter_types[1].nullable);
        assert_eq!(result_type.logical_type, ValueLogicalType::LargeInt);
        assert_eq!(result_type.data_type, DataType::FixedSizeBinary(16));
        assert!(!result_type.nullable);
        let DataType::Struct(fields) = &parameter_types[1].data_type else {
            panic!("Struct")
        };
        assert_eq!(fields[0].name(), "original");
        assert_eq!(
            fields[0].metadata().get("unrecognized").unwrap(),
            "preserved"
        );
        let FunctionArgumentType::Lambda {
            parameter_types: original,
            ..
        } = &source.argument_types[1]
        else {
            panic!("source Lambda")
        };
        let DataType::Struct(original_fields) = &original[1].data_type else {
            panic!("source Struct")
        };
        assert!(Arc::ptr_eq(&fields[0], &original_fields[0]));
        assert!(!std::ptr::eq(
            source.argument_types.as_ptr(),
            copied.argument_types.as_ptr()
        ));
        assert!(!std::ptr::eq(original.as_ptr(), parameter_types.as_ptr()));
    }
}

#[test]
fn repeated_signature_occurrences_share_one_cumulative_model_and_numeric_first_cause() {
    let source = fixture(FunctionKind::Aggregate);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut model = MaterializationModel::for_composition(2, 0, 4096, 4096);
    preflight_scalar_signature_copy(&source, &mut model, limits(), &mut work).unwrap();
    let once_bytes = model.facts.request_bytes_upper_bound;
    let once_retained = model.retained;
    preflight_scalar_signature_copy(&source, &mut model, limits(), &mut work).unwrap();
    assert_eq!(model.facts.definition_count, 2);
    assert_eq!(model.facts.type_reference_count, 10);
    assert_eq!(model.facts.allocation_requests_upper_bound, 16);
    assert_eq!(model.facts.request_bytes_upper_bound, 2 * once_bytes);
    assert_eq!(model.retained, 2 * once_retained);
    assert_eq!(
        model.facts.coexisting_source_and_request_bytes_upper_bound,
        4096 + 2 * once_bytes
    );
    finish(work, Ok::<_, BindingCodecError>(())).unwrap();
    let facts = model.facts;
    let tight = BindingProjectionLimits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_request_bytes: facts.request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    model.check(tight).unwrap();
    for axis in 0..6 {
        let mut under = tight;
        match axis {
            0 => under.max_definitions -= 1,
            1 => under.max_type_references -= 1,
            2 => under.max_request_bytes -= 1,
            3 => under.max_allocation_requests -= 1,
            4 => under.max_coexisting_source_and_request_bytes -= 1,
            5 => under.max_work -= 1,
            _ => unreachable!(),
        }
        for late in CAUSES {
            let control = Control::rejecting(1, late);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let result = preflight_scalar_signature_copy(&source, &mut model, under, &mut work);
            assert!(matches!(
                finish(work, result),
                Err(BindingCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), [0]);
        }
    }
}

#[test]
fn signature_copy_actual_callbacks_preserve_success_ordinary_and_all_control_prefixes() {
    prefixes(&fixture(FunctionKind::Scalar), true);
    let mut legacy = fixture(FunctionKind::Aggregate);
    legacy.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: FunctionVolatility::Immutable,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        semantic_parameters: Box::new([]),
    });
    assert!(matches!(
        run(&legacy, &Control::default()),
        Err(BindingCodecError::InvalidShape(
            "signature copy requires an exact binding without legacy metadata"
        ))
    ));
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let result = copy_scalar_signature_observed(&legacy, &mut work);
    assert!(matches!(
        finish(work, result),
        Err(BindingCodecError::InvalidShape(_))
    ));
    assert_eq!(control.trace(), [0, 1]);
    for at in 0..2 {
        for cause in CAUSES {
            let control = Control::rejecting(at, cause);
            let result = (|| {
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
                let result = copy_scalar_signature_observed(&legacy, &mut work);
                finish(work, result)
            })();
            assert!(matches!(result, Err(BindingCodecError::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), [0, 1][..=at]);
        }
    }
    prefixes(&legacy, false);
    let wrong_kind = fixture(FunctionKind::Table);
    prefixes(&wrong_kind, false);
}

#[test]
fn wide_lambda_reference_gate_precedes_clone_and_keeps_ordered_full_parameters() {
    let source = BoundFunction::from_exact_signature(
        FunctionId::try_new("test/wide").unwrap(),
        FunctionOverloadId::try_new("test/wide/o").unwrap(),
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Lambda {
            parameter_types: (0..320)
                .map(|ordinal| FunctionValueType::new(DataType::Int64, ordinal % 2 == 1))
                .collect(),
            result_type: FunctionValueType::new(DataType::Utf8, true),
        }]
        .into_boxed_slice(),
        FunctionValueType::new(DataType::Utf8, false),
    );
    let mut under = limits();
    under.max_type_references = 321;
    for late in CAUSES {
        let control = Control::rejecting(1, late);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
        let result = preflight_scalar_signature_copy(&source, &mut model, under, &mut work);
        assert!(matches!(
            finish(work, result),
            Err(BindingCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [0]);
    }
    let output = run(&source, &Control::default()).unwrap();
    let FunctionArgumentType::Lambda {
        parameter_types,
        result_type,
    } = &output.argument_types[0]
    else {
        panic!("Lambda")
    };
    assert_eq!(parameter_types.len(), 320);
    assert_eq!(
        parameter_types[0],
        FunctionValueType::new(DataType::Int64, false)
    );
    assert_eq!(
        parameter_types[1],
        FunctionValueType::new(DataType::Int64, true)
    );
    assert_eq!(
        parameter_types[318],
        FunctionValueType::new(DataType::Int64, false)
    );
    assert_eq!(
        parameter_types[319],
        FunctionValueType::new(DataType::Int64, true)
    );
    assert_eq!(*result_type, FunctionValueType::new(DataType::Utf8, true));
    assert_eq!(
        output.result_type,
        FunctionValueType::new(DataType::Utf8, false)
    );
}

fn table_fixture() -> BoundTableFunction {
    let scalar = fixture(FunctionKind::Scalar);
    BoundTableFunction::from_exact_signature(
        scalar.function_id,
        scalar.overload,
        scalar.argument_types,
        vec![
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                true,
            ),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ]
        .into_boxed_slice(),
    )
}
fn run_table(
    source: &BoundTableFunction,
    control: &Control,
) -> Result<BoundTableFunction, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
        preflight_table_signature_copy(source, &mut model, limits(), &mut work)?;
        copy_table_signature_observed(source, &mut work)
    })();
    finish(work, result)
}

#[test]
fn table_signature_copy_preserves_relation_roots_with_independent_layout_invoice() {
    let source = table_fixture();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
    preflight_table_signature_copy(&source, &mut model, limits(), &mut work).unwrap();
    let bytes = 12
        + 2 * Layout::array::<FunctionArgumentType>(2).unwrap().size()
        + 4 * Layout::array::<FunctionValueType>(2).unwrap().size()
        + 4 * Layout::new::<DataType>().size();
    let retained = 12
        + Layout::array::<FunctionArgumentType>(2).unwrap().size()
        + 2 * Layout::array::<FunctionValueType>(2).unwrap().size()
        + 4 * Layout::new::<DataType>().size();
    assert_eq!(model.facts.type_reference_count, 6);
    assert_eq!(model.facts.allocation_requests_upper_bound, 12);
    assert_eq!(model.facts.request_bytes_upper_bound, bytes);
    assert_eq!(model.retained, retained);
    assert_eq!(
        model.facts.coexisting_source_and_request_bytes_upper_bound,
        4096 + bytes
    );
    assert_eq!(
        model.facts.cumulative_work_upper_bound,
        128 + 32 * 7 + 6 * (16 + 8 * MAX_VALUE_TYPE_NODES) + 4 * bytes + 12
    );
    let copied = copy_table_signature_observed(&source, &mut work).unwrap();
    finish(work, Ok::<_, BindingCodecError>(())).unwrap();
    assert_eq!(copied.function_id.as_str(), "test/f");
    assert_eq!(copied.overload.as_str(), "test/o");
    assert!(copied.legacy_metadata.is_none());
    assert_eq!(copied.result_types.len(), 2);
    assert_eq!(
        copied.result_types[0],
        FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true
        )
    );
    assert_eq!(
        copied.result_types[1].logical_type,
        ValueLogicalType::LargeInt
    );
    assert_eq!(
        copied.result_types[1].data_type,
        DataType::FixedSizeBinary(16)
    );
    assert!(!copied.result_types[1].nullable);
    assert_eq!(copied.argument_types, source.argument_types);
    assert!(!std::ptr::eq(
        copied.result_types.as_ptr(),
        source.result_types.as_ptr()
    ));
    let DataType::Dictionary(source_key, source_value) = &source.result_types[0].data_type else {
        panic!("source dictionary")
    };
    let DataType::Dictionary(copied_key, copied_value) = &copied.result_types[0].data_type else {
        panic!("copied dictionary")
    };
    assert!(!std::ptr::eq(source_key.as_ref(), copied_key.as_ref()));
    assert!(!std::ptr::eq(source_value.as_ref(), copied_value.as_ref()));
}

#[test]
fn table_signature_copy_empty_roots_and_all_actual_refusal_prefixes() {
    let full = table_fixture();
    let empty = BoundTableFunction::from_exact_signature(
        FunctionId::try_new("test/f").unwrap(),
        FunctionOverloadId::try_new("test/o").unwrap(),
        Box::new([]),
        Box::new([]),
    );
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
    preflight_table_signature_copy(&empty, &mut model, limits(), &mut work).unwrap();
    assert_eq!(model.facts.type_reference_count, 0);
    assert_eq!(model.facts.allocation_requests_upper_bound, 2);
    assert_eq!(model.facts.request_bytes_upper_bound, 12);
    assert_eq!(model.retained, 12);
    finish(work, Ok::<_, BindingCodecError>(())).unwrap();
    for source in [&full, &empty] {
        let control = Control::default();
        let actual = run_table(source, &control).unwrap();
        assert_eq!(&actual, source);
        let trace = control.trace();
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control::rejecting(at, cause);
                assert!(
                    matches!(run_table(source, &control), Err(BindingCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
    let mut legacy = full;
    legacy.legacy_metadata = Some(LegacyBindingMetadata {
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        volatility: FunctionVolatility::Immutable,
        intrinsic_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        semantic_parameters: Box::new([]),
    });
    let control = Control::default();
    assert!(matches!(
        run_table(&legacy, &control),
        Err(BindingCodecError::InvalidShape(_))
    ));
    let trace = control.trace();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::rejecting(at, cause);
            assert!(
                matches!(run_table(&legacy, &control), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn staged_scalar_counts_admit_all_roots_before_owned_dictionary_walk() {
    let source = fixture(FunctionKind::Aggregate);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
    preflight_scalar_signature_copy_counts(&source, &mut model, limits(), &mut work).unwrap();
    let prefix_bytes = 12
        + 2 * Layout::array::<FunctionArgumentType>(2).unwrap().size()
        + 2 * Layout::array::<FunctionValueType>(2).unwrap().size();
    assert_eq!(model.facts.type_reference_count, 5);
    assert_eq!(model.facts.allocation_requests_upper_bound, 6);
    assert_eq!(model.facts.request_bytes_upper_bound, prefix_bytes);
    assert_eq!(model.items, 4);
    preflight_scalar_signature_copy_types(&source, &mut model, limits(), &mut work).unwrap();
    assert_eq!(model.facts.allocation_requests_upper_bound, 8);
    assert_eq!(
        model.facts.request_bytes_upper_bound,
        prefix_bytes + 2 * Layout::new::<DataType>().size()
    );
    work.finish().unwrap();
    let full = Control::default();
    let mut work = CompileCheckpoints::try_new(&full, CompilePhase::Decode).unwrap();
    let mut original = MaterializationModel::for_composition(1, 0, 4096, 4096);
    preflight_scalar_signature_copy(&source, &mut original, limits(), &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(full.trace(), control.trace());
    assert_eq!(
        original.facts.request_bytes_upper_bound,
        model.facts.request_bytes_upper_bound
    );
}

#[test]
fn table_signature_parent_port_preserves_full_relation_and_independent_layouts() {
    use crate::physical_node_v2::{Model as NodeModel, NodeProjectionLimits};
    use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
    let source = table_fixture();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut snapshots = Vec::new();
    let mut parent = |facts: &crate::physical_node_v2::NodeProjectionFacts| {
        snapshots.push(*facts);
        Ok(())
    };
    let node_limits = NodeProjectionLimits {
        max_input_nodes: 8,
        max_value_references: 4096,
        max_list_items: 4096,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 << 20,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_work: 1 << 30,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 4096,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 4 << 20,
            max_coexisting_source_and_request_bytes: 8 << 20,
            max_work: 1 << 30,
        },
    };
    let base = NodeModel {
        inputs: 1,
        refs: 2,
        items: 4,
        requests: 1,
        requested: 9,
        delegated_work: 37,
    };
    let final_facts = {
        let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
        model
            .compose_in_node_in(base, 1, node_limits, limits(), &mut parent)
            .unwrap();
        preflight_table_signature_copy_counts_in(
            &source,
            &mut model,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        assert_eq!(model.facts.type_reference_count, 6);
        assert_eq!(model.facts.allocation_requests_upper_bound, 8);
        preflight_table_signature_copy_types_in(
            &source,
            &mut model,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let request_bytes = 12
            + 2 * Layout::array::<FunctionArgumentType>(2).unwrap().size()
            + 4 * Layout::array::<FunctionValueType>(2).unwrap().size()
            + 4 * Layout::new::<DataType>().size();
        assert_eq!(model.facts.allocation_requests_upper_bound, 12);
        assert_eq!(model.facts.request_bytes_upper_bound, request_bytes);
        let final_facts = model
            .node_facts(2 * model.facts.cumulative_work_upper_bound, &mut work)
            .unwrap();
        assert_eq!(final_facts.allocation_requests_upper_bound, 13);
        assert_eq!(
            final_facts.allocation_request_bytes_upper_bound,
            9 + request_bytes
        );
        assert_eq!(final_facts.list_item_count, 10);
        let copied = copy_table_signature_observed(&source, &mut work).unwrap();
        assert_eq!(copied, source);
        work.finish().unwrap();
        final_facts
    };
    assert!(snapshots.iter().all(|p| p.allocation_requests_upper_bound
        <= final_facts.allocation_requests_upper_bound
        && p.allocation_request_bytes_upper_bound
            <= final_facts.allocation_request_bytes_upper_bound
        && p.cumulative_work_upper_bound <= final_facts.cumulative_work_upper_bound));
}

#[test]
fn table_signature_parent_dictionary_known_boxes_precede_pending255_callback() {
    use crate::physical_node_v2::{Model as NodeModel, NodeProjectionFacts, NodeProjectionLimits};
    use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
    let source = table_fixture();
    let node_limits = NodeProjectionLimits {
        max_input_nodes: 8,
        max_value_references: 4096,
        max_list_items: 4096,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 << 20,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_work: 1 << 30,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 4096,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 4 << 20,
            max_coexisting_source_and_request_bytes: 8 << 20,
            max_work: 1 << 30,
        },
    };
    // Eight requests are the argument/result Vec-to-Box pairs, two identity
    // strings and the actual Lambda parameter pair. The first Dictionary adds
    // two DataType boxes through the sole clone preflight, before its flush.
    let run = |control: &Control, reject: bool| -> Result<usize, BindingCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let mut marker = None;
        let mut parent = |facts: &NodeProjectionFacts| {
            if facts.allocation_requests_upper_bound > 8 {
                marker.get_or_insert_with(|| control.trace().len());
                if reject {
                    return Err(CompileControlError::ResourceExhausted);
                }
            }
            Ok(())
        };
        {
            let mut model = MaterializationModel::for_composition(1, 0, 4096, 4096);
            model.compose_in_node_in(
                NodeModel::default(),
                1,
                node_limits,
                limits(),
                &mut parent,
            )?;
            preflight_table_signature_copy_counts_in(
                &source,
                &mut model,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )?;
            work.flush()?;
            for _ in 0..255 {
                work.step()?;
            }
            let result = preflight_table_signature_copy_types_in(
                &source,
                &mut model,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            );
            if matches!(result, Err(BindingCodecError::Control(_))) {
                return result.map(|_| 0);
            }
            result?;
            work.finish()?;
        }
        Ok(marker.unwrap())
    };
    let success = Control::default();
    let marker = run(&success, false).unwrap();
    let trace = success.trace();
    assert_eq!(trace[marker], 255);
    for cause in CAUSES {
        let control = Control::rejecting(marker, cause);
        assert!(matches!(
            run(&control, true),
            Err(BindingCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), trace[..marker]);
    }
}
