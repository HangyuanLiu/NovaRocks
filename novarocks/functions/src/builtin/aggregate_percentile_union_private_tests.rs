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

//! Private full-generic Union consumer tests; no public registration.
use super::*;
use arrow_array::{Array, LargeBinaryArray, LargeStringArray, StringArray, StructArray};

fn payload(count: usize) -> Vec<u8> {
    let mut state = digest::PercentileState::default();
    for n in 0..count {
        digest::add_value(&mut state, n as f64).unwrap();
    }
    digest::encode_state(&state)
}
fn packed(values: ArrayRef, rates: ArrayRef, extra: usize, root_null: bool) -> ArrayRef {
    let mut fields = vec![
        Field::new("value", values.data_type().clone(), true),
        Field::new("rate", rates.data_type().clone(), true),
    ];
    let mut arrays = vec![values, rates];
    if extra > 0 {
        fields.push(Field::new("compression", DataType::Int64, true));
        arrays.push(Arc::new(Int64Array::from(vec![None; arrays[0].len()])));
    }
    for n in 1..extra {
        fields.push(Field::new(format!("ignored{n}"), DataType::Null, true));
        arrays.push(Arc::new(arrow_array::NullArray::new(arrays[0].len())));
    }
    let nulls = root_null.then(|| arrow_buffer::NullBuffer::new_null(arrays[0].len()));
    Arc::new(StructArray::new(fields.into(), arrays, nulls))
}
fn assert_data(
    result: EvaluationFailure,
    expected: &str,
    phase: crate::aggregate_format::AggregateFailureStage,
) {
    let EvaluationFailure::InvocationData(data) = result else {
        panic!("original whole-call Data")
    };
    assert_eq!(data.message(), phase.message(expected).to_string());
}
#[test]
fn percentile_union_private_all_phases_binary_writer_and_empty_state() {
    let bytes = payload(2);
    let values = vec![Arc::new(BinaryArray::from(vec![
        Some(bytes.as_slice()),
        None,
        Some(bytes.as_slice()),
    ])) as ArrayRef];
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let k = kernel("percentile_union", types(&values), phase);
        let host = Arc::new(Host::default());
        let mut state = k
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        if phase.consumes_logical_arguments() {
            update_selected(
                &k,
                &mut state,
                host.clone(),
                &values,
                Selection::all(3),
                &[17; 3],
                &Control::default(),
            )
            .unwrap();
        } else {
            merge_selected(
                &k,
                &mut state,
                host.clone(),
                &values[0],
                Selection::all(3),
                &[17; 3],
                &Control::default(),
            )
            .unwrap();
        }
        assert_eq!(state.core.digest.count(), 4.);
        let host_dyn: Arc<dyn AggregateStateAllocator> = host.clone();
        let indices = [17];
        let context =
            AggregateEmissionContext::from_host(&k.contract, &indices, 32, Some(&host_dyn));
        let output = if phase.produces_final_result() {
            k.build_final_evaluation_with_context(
                std::iter::once(&state),
                &context,
                &Control::default(),
            )
        } else {
            k.build_intermediate_evaluation_with_context(
                std::iter::once(&state),
                &context,
                &Control::default(),
            )
        }
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            digest::encode_state(&state.core)
        );
        drop(output);
        drop(state);
        drop(host_dyn);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
    let empty = vec![Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())) as ArrayRef];
    let k = kernel(
        "percentile_union",
        types(&empty),
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = k
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &k,
        &mut state,
        host.clone(),
        &empty,
        Selection::all(0),
        &[],
        &Control::default(),
    )
    .unwrap();
    let output = final_output(&k, &state, host.clone()).unwrap();
    let binary = output.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert!(!binary.is_null(0));
    assert_eq!(binary.value(0), payload(0));
    drop(output);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn percentile_union_private_packed_full_fields_root_null_and_empty_validation() {
    for extra in [0, 1, 3] {
        let values = vec![packed(
            Arc::new(Float64Array::from(vec![2., 4.])),
            Arc::new(Float64Array::from(vec![0.5; 2])),
            extra,
            true,
        )];
        let k = kernel(
            "percentile_union",
            types(&values),
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = k
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        update_selected(
            &k,
            &mut state,
            host.clone(),
            &values,
            Selection::all(2),
            &[0; 2],
            &Control::default(),
        )
        .unwrap();
        assert_eq!(state.core.digest.count(), 2.);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
    for fields in [0usize, 1] {
        for len in [0usize, 1] {
            let array: ArrayRef = if fields == 0 {
                Arc::new(StructArray::new_empty_fields(len, None))
            } else {
                Arc::new(StructArray::new(
                    vec![Field::new("value", DataType::Float64, true)].into(),
                    vec![Arc::new(Float64Array::from(vec![None; len]))],
                    None,
                ))
            };
            let values = vec![array];
            let k = kernel(
                "percentile_union",
                types(&values),
                AggregateKernelPhase::Single,
            );
            assert!(k.requires_empty_update_preparation());
            let host = Arc::new(Host::default());
            let mut state = k
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let error = update_selected(
                &k,
                &mut state,
                host.clone(),
                &values,
                Selection::all(len),
                &vec![7; len],
                &Control::default(),
            )
            .unwrap_err();
            assert_data(
                error,
                "percentile_approx: percentile_approx expects STRUCT(value, quantile[, compression]) input",
                crate::aggregate_format::AggregateFailureStage::Update,
            );
            assert_eq!(state.core.digest.count(), 0.);
            assert!(!state.failed);
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn percentile_union_private_full_type_data_null_and_sparse_constant_addresses() {
    let long = DataType::List(Arc::new(Field::new(
        "unsupported".repeat(80),
        DataType::UInt32,
        true,
    )));
    for ty in [
        DataType::Null,
        DataType::Boolean,
        DataType::UInt32,
        DataType::Decimal256(60, 2),
        long,
    ] {
        let values = vec![arrow_array::new_null_array(&ty, 3)];
        let k = kernel(
            "percentile_union",
            types(&values),
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = k
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let expected = core::merge_row(
            &mut digest::PercentileState::default(),
            &values[0],
            2,
            Diagnostic::UnweightedUpdate,
        )
        .unwrap_err();
        let error = update_selected(
            &k,
            &mut state,
            host.clone(),
            &values,
            Selection::try_sparse(3, &[2]).unwrap(),
            &[19],
            &Control::default(),
        )
        .unwrap_err();
        assert_data(
            error,
            &expected,
            crate::aggregate_format::AggregateFailureStage::Update,
        );
        assert!(state.failed);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        let empty = vec![arrow_array::new_empty_array(&ty)];
        let mut state = k
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        update_selected(
            &k,
            &mut state,
            host.clone(),
            &empty,
            Selection::all(0),
            &[],
            &Control::default(),
        )
        .unwrap();
        assert_eq!(state.core.digest.count(), 0.);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
    let values = vec![packed(
        Arc::new(Float64Array::from(vec![None, Some(4.), None])),
        Arc::new(Float64Array::from(vec![2., 0.5, 2.])),
        1,
        true,
    )];
    let k = kernel(
        "percentile_union",
        types(&values),
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = k
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let ty = FunctionValueType::new(values[0].data_type().clone(), true);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("packed-original").unwrap()),
        ty,
        values[0].to_data(),
        ConstantPolicy {
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
        },
        CompilePhase::FunctionSpecialization,
        &Compile,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let arguments = [EvaluatedArgument::Constant(&value)];
    let control = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &k.contract,
        Selection::all(321),
        &arguments,
        &[],
        &control,
    )
    .unwrap();
    let prepared = k
        .prepare_update_evaluation(input, &vec![0; 321], Some(host.clone()), &control)
        .unwrap();
    for n in 0..321 {
        k.update_row_evaluation(&mut state, &prepared, n, &control)
            .unwrap();
    }
    assert_eq!(state.core.digest.count(), 321.);
    assert!(control.trace.lock().unwrap().iter().all(|n| *n <= 256));
    drop(prepared);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn percentile_union_private_original_payload_errors_and_state_latch() {
    for bytes in [vec![], vec![0xa2, 3], vec![0xa2, 4]] {
        let values = vec![Arc::new(BinaryArray::from(vec![bytes.as_slice()])) as ArrayRef];
        for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
            let k = kernel("percentile_union", types(&values), phase);
            let host = Arc::new(Host::default());
            let mut state = k
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let diagnostic = if phase.consumes_logical_arguments() {
                Diagnostic::UnweightedUpdate
            } else {
                Diagnostic::UnweightedMerge
            };
            let expected = core::merge_row(
                &mut digest::PercentileState::default(),
                &values[0],
                0,
                diagnostic,
            )
            .unwrap_err();
            let error = if phase.consumes_logical_arguments() {
                update_selected(
                    &k,
                    &mut state,
                    host.clone(),
                    &values,
                    Selection::all(1),
                    &[7],
                    &Control::default(),
                )
            } else {
                merge_selected(
                    &k,
                    &mut state,
                    host.clone(),
                    &values[0],
                    Selection::all(1),
                    &[7],
                    &Control::default(),
                )
            }
            .unwrap_err();
            assert_data(
                error,
                &expected,
                if phase.consumes_logical_arguments() {
                    crate::aggregate_format::AggregateFailureStage::Update
                } else {
                    crate::aggregate_format::AggregateFailureStage::Merge
                },
            );
            assert!(state.failed);
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn percentile_union_private_every_update_merge_emit_callback_seven_causes_no_tail() {
    for mode in [0usize, 1, 2, 3] {
        let bytes = payload(1);
        let values = if mode == 3 {
            vec![packed(
                Arc::new(Float64Array::from(vec![None])),
                Arc::new(Float64Array::from(vec![2.])),
                0,
                false,
            )]
        } else {
            vec![Arc::new(BinaryArray::from(vec![bytes.as_slice()])) as ArrayRef]
        };
        let k = kernel(
            "percentile_union",
            types(&values),
            if mode == 1 {
                AggregateKernelPhase::Final
            } else {
                AggregateKernelPhase::Single
            },
        );
        let run = |control: &Control| {
            let host = Arc::new(Host::default());
            let mut state = k
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let result = if mode == 2 {
                final_output_observed(&k, &state, host.clone(), control).map(|_| ())
            } else if mode == 1 {
                merge_selected(
                    &k,
                    &mut state,
                    host.clone(),
                    &values[0],
                    Selection::all(1),
                    &[7],
                    control,
                )
            } else {
                update_selected(
                    &k,
                    &mut state,
                    host.clone(),
                    &values,
                    Selection::all(1),
                    &[7],
                    control,
                )
            };
            drop(state);
            (result, host)
        };
        let observed = Control::default();
        let (result, host) = run(&observed);
        if mode == 3 {
            assert!(matches!(&result, Err(EvaluationFailure::InvocationData(_))));
            drop(result)
        } else {
            result.unwrap()
        };
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        let trace = observed.trace.into_inner().unwrap();
        assert!(!trace.is_empty());
        for n in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((n, cause.clone())),
                };
                let (result, host) = run(&control);
                assert_eq!(result.unwrap_err(), EvaluationFailure::Kernel(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=n]);
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}
fn final_output_observed(
    k: &ApproxPercentileKernel,
    state: &ApproxPercentileState,
    host: Arc<Host>,
    control: &Control,
) -> Result<ArrayRef, EvaluationFailure> {
    let host: Arc<dyn AggregateStateAllocator> = host;
    let indices = [7];
    let context = AggregateEmissionContext::from_host(&k.contract, &indices, 8, Some(&host));
    k.build_final_evaluation_with_context(std::iter::once(state), &context, control)
}
#[test]
fn percentile_union_private_every_actual_host_allocation_seven_causes_and_missing_host() {
    for bad in [false, true] {
        let values = if bad {
            vec![packed(
                Arc::new(Float64Array::from(vec![None])),
                Arc::new(Float64Array::from(vec![2.])),
                0,
                false,
            )]
        } else {
            vec![Arc::new(BinaryArray::from(vec![payload(3).as_slice()])) as ArrayRef]
        };
        let k = kernel(
            "percentile_union",
            types(&values),
            AggregateKernelPhase::Single,
        );
        assert!(matches!(
            k.create_state(&Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let run = |host: Arc<Host>| {
            let mut state = k
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .map_err(EvaluationFailure::Kernel)?;
            update_selected(
                &k,
                &mut state,
                host,
                &values,
                Selection::all(1),
                &[7],
                &Control::default(),
            )
        };
        let host = Arc::new(Host::default());
        let result = run(host.clone());
        if bad {
            assert!(matches!(&result, Err(EvaluationFailure::InvocationData(_))));
            drop(result)
        } else {
            result.unwrap()
        };
        let allocations = host.ledger.lock().unwrap().attempts;
        assert!(allocations > 0);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        for at in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                *host.refusal.lock().unwrap() = Some((at, cause.clone()));
                assert_eq!(
                    run(host.clone()).unwrap_err(),
                    EvaluationFailure::Kernel(cause)
                );
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}

#[test]
fn percentile_union_private_text_large_binary_and_original_magic_quirk() {
    let mut bytes = payload(0);
    bytes[0] = b'P';
    let text = std::str::from_utf8(&bytes).unwrap();
    let sources: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(vec![Some(bytes.as_slice()), None])),
        Arc::new(LargeBinaryArray::from(vec![Some(bytes.as_slice()), None])),
        Arc::new(StringArray::from(vec![Some(text), None])),
        Arc::new(LargeStringArray::from(vec![Some(text), None])),
    ];
    for array in sources {
        let values = vec![array];
        let k = kernel(
            "percentile_union",
            types(&values),
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = k
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        update_selected(
            &k,
            &mut state,
            host.clone(),
            &values,
            Selection::all(2),
            &[0; 2],
            &Control::default(),
        )
        .unwrap();
        assert_eq!(state.core.digest.count(), 0.);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[test]
fn percentile_union_private_compile_three_real_source_causes() {
    struct Refuse(CompileControlError);
    impl PureCompileControl for Refuse {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0)
        }
    }
    let catalog = super::super::super::catalogue::percentile_union_private_test_catalog();
    let args = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Binary, true),
        constant: None,
    }];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        assert_eq!(
            catalog
                .resolve_bound_user(
                    "percentile_union",
                    FunctionKind::Aggregate,
                    request,
                    &Refuse(cause)
                )
                .unwrap_err(),
            FunctionBindingError::Control(cause)
        );
    }
}

#[test]
fn percentile_union_private_original_oversized_field_guard_is_resource() {
    let ty = DataType::List(Arc::new(Field::new(
        "unsupported".repeat(180),
        DataType::UInt32,
        true,
    )));
    let DataType::List(field) = &ty else {
        unreachable!()
    };
    assert!(field.name().len() > novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES);
    let catalog = super::super::super::catalogue::percentile_union_private_test_catalog();
    let args = [FunctionArgument::Value {
        value_type: FunctionValueType::new(ty, true),
        constant: None,
    }];
    assert_eq!(
        catalog
            .resolve_bound_user(
                "percentile_union",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: 1,
                    expected_result_type: None
                },
                &Compile
            )
            .unwrap_err(),
        FunctionBindingError::Control(CompileControlError::ResourceExhausted)
    );
}
