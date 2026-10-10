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
//! Real prepared FIELD owners, exact address origins and every host refusal.
use crate::field_shared::FieldState;
use crate::{
    EvaluatedArgument, FunctionValueType, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    ScalarEvaluationInstance, SelectedValues, Selection,
};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionNullBehavior, ValueLogicalType};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("FIELD never waits")
    }
}
fn prepared(ty: FunctionValueType, arity: usize) -> Arc<dyn crate::PreparedScalarKernel> {
    super::string_field_owner::prepared_for_test_with_policy(
        "field",
        &vec![ty; arity],
        DecimalOverflowPolicy::ReportError,
    )
    .unwrap()
}
fn values(out: &SelectedValues<'_>) -> Vec<i32> {
    assert!(out.errors().is_empty());
    assert_eq!(out.values().null_count(), 0);
    out.values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}
#[test]
fn field_kernel_original_float_scalar_equality_is_not_arrow_total_order() {
    let first: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.),
        Some(-0.),
        Some(f64::NAN),
        None,
    ]));
    let candidate: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.),
        Some(0.),
        Some(f64::NAN),
        Some(9.),
    ]));
    let args = [
        EvaluatedArgument::Column(&first),
        EvaluatedArgument::Column(&candidate),
    ];
    let p = prepared(FunctionValueType::new(DataType::Float64, true), 2);
    assert_eq!(
        p.contract().effects().null_behavior,
        FunctionNullBehavior::CalledOnNull
    );
    assert!(!p.contract().result_type().nullable);
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(4), &args, &Control::default())
        .unwrap();
    assert_eq!(values(&out), vec![1, 1, 0, 0]);
}
#[test]
fn field_kernel_real_compact_scalar_nonzero_pool_origins_preserve_zero_and_first_match() {
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let p = prepared(ty.clone(), 3);
    let backing: ArrayRef = Arc::new(StringArray::from(vec![
        Some("ignored"),
        Some("九\0"),
        None,
        Some("x"),
    ]));
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact: ArrayRef = Arc::new(StringArray::from(vec![Some("九\0"), Some("x")]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Utf8, compact, Box::default()).unwrap();
    let scalar = backing.slice(1, 1);
    let arguments = [
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::Column(&backing),
    ];
    let out = ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_eq!(values(&out), vec![1, 2]);
    let pool = crate::ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-field-pool").unwrap()),
        ty,
        backing.to_data(),
        crate::ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
        crate::binding_test_control(),
    )
    .unwrap();
    for ordinal in [1, 2] {
        let value = pool.value(ordinal).unwrap();
        let arguments = [
            EvaluatedArgument::Constant(&value),
            EvaluatedArgument::Scalar(&scalar),
            EvaluatedArgument::Column(&backing),
        ];
        let out = ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(
            values(&out),
            if ordinal == 1 { vec![1, 1] } else { vec![0, 0] }
        );
    }
}
#[test]
fn field_kernel_every_actual_callback_seven_causes_first_prefix_and_failed_instance_no_replay() {
    let text = "x".repeat(777);
    let source: ArrayRef = Arc::new(StringArray::from(vec![Some(text.as_str()), None]));
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Column(&source),
    ];
    let p = prepared(FunctionValueType::new(DataType::Utf8, true), 3);
    let success = Control::default();
    ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(Selection::all(2), &args, &success)
        .unwrap();
    let trace = success.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational")),
            KernelFailure::InstanceFailed,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause.clone())),
            };
            let mut instance = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
            assert_eq!(
                instance
                    .evaluate(Selection::all(2), &args, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(2), &args, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
#[test]
fn field_kernel_generic_nominal_bytes_do_not_install_a_json_or_uuid_decoder() {
    for logical in [ValueLogicalType::Json, ValueLogicalType::Uuid] {
        let a: ArrayRef = if logical == ValueLogicalType::Json {
            Arc::new(StringArray::from(vec![Some("raw-not-json"), None]))
        } else {
            crate::largeint::array_from_i128(&[Some(i128::MIN), None]).unwrap()
        };
        let ty =
            FunctionValueType::try_with_logical_type(a.data_type().clone(), true, logical).unwrap();
        let p = prepared(ty, 2);
        let args = [EvaluatedArgument::Column(&a), EvaluatedArgument::Column(&a)];
        assert_eq!(
            values(
                &ScalarEvaluationInstance::instantiate(p)
                    .unwrap()
                    .evaluate(Selection::all(2), &args, &Control::default())
                    .unwrap()
            ),
            vec![1, 0]
        );
    }
}
#[test]
fn field_kernel_raw_step_keeps_complete_data_error_without_observer_footer() {
    let zone = "long-zone".repeat(120);
    let first: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![7]).with_timezone(zone));
    let candidate: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![7]));
    let expected = format!(
        "field frozen argument mismatch: {:?}/1 vs {:?}/1",
        first.data_type(),
        candidate.data_type()
    );
    assert!(expected.len() > 512);
    let mut state = FieldState::new(&first, 2).unwrap();
    let mut observer =
        |_| -> Result<(), KernelFailure> { panic!("original data guard precedes observation") };
    assert_eq!(
        state
            .step_observed(0, &candidate, Some(&mut observer))
            .unwrap()
            .unwrap_err(),
        expected
    );
    assert_eq!(
        crate::field_shared::validate_arity(1).unwrap_err(),
        "field requires a value and an INT-bounded candidate list"
    );
    if usize::BITS > 32 {
        assert_eq!(
            crate::field_shared::validate_arity(i32::MAX as usize + 2).unwrap_err(),
            "field requires a value and an INT-bounded candidate list"
        );
    }
}

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(novarocks_type_contract::CompilePhase, u32)>>,
    refusal: Option<(usize, novarocks_type_contract::CompileControlError)>,
}
impl novarocks_type_contract::PureCompileControl for CompileControl {
    fn checkpoint(
        &self,
        phase: novarocks_type_contract::CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[test]
fn field_kernel_actual_preparation_success_and_rejection_preserve_every_compile_cause() {
    use novarocks_type_contract::CompileControlError;
    // Success and a real ordinary binding rejection both have observed tails.
    // No new resolver or synthetic selected contract is used for either path.
    for ty in [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(DataType::Binary, true),
    ] {
        let good = CompileControl::default();
        let result = super::string_field_owner::prepared_for_test_with_control(
            "field",
            &[ty.clone(), ty.clone()],
            DecimalOverflowPolicy::ReportError,
            &good,
        );
        assert_eq!(result.is_ok(), ty.data_type == DataType::Utf8);
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((at, cause)),
                };
                let actual = super::string_field_owner::prepared_for_test_with_control(
                    "field",
                    &[ty.clone(), ty.clone()],
                    DecimalOverflowPolicy::ReportError,
                    &control,
                )
                .err()
                .and_then(|error| match error {
                    crate::FunctionSpecializationFailure::Control(actual) => Some(actual),
                    crate::FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    crate::FunctionSpecializationFailure::Kernel(
                        KernelFailure::DeadlineExceeded,
                    ) => Some(CompileControlError::DeadlineExceeded),
                    crate::FunctionSpecializationFailure::Kernel(
                        KernelFailure::ResourceExhausted,
                    ) => Some(CompileControlError::ResourceExhausted),
                    _ => None,
                });
                assert_eq!(actual, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn field_kernel_one_original_count_author_and_explicit_pure_shape_boundaries() {
    use super::string_field_owner::check_pure_arity;
    let max_total = i32::MAX as usize + 1;
    for count in [2, 3, max_total] {
        assert!(crate::field_shared::validate_arity(count).is_ok());
        assert!(check_pure_arity(count).is_ok());
    }
    for count in [0, 1, max_total + 1, usize::MAX] {
        assert_eq!(
            crate::field_shared::validate_arity(count).unwrap_err(),
            "field requires a value and an INT-bounded candidate list"
        );
        let crate::FunctionBindingError::InvalidBinding(message) =
            check_pure_arity(count).unwrap_err()
        else {
            panic!("explicit binding shape refusal")
        };
        assert_eq!(
            message.as_ref(),
            format!(
                "field unsupported argument shape ({count} total): field requires a value and an INT-bounded candidate list"
            )
        );
    }
}
#[test]
fn field_kernel_actual_owner_refuses_out_of_bounds_count_before_request_traversal() {
    use crate::FunctionBindingResolver;
    // Structural count projection is independent of allocating a billion
    // argument channels. This is an owner admission proof, not a claim that a
    // malformed count/empty vector was a valid Physical invocation.
    let owner = super::string_field_owner::owner_for_test("field");
    let count = i32::MAX as usize + 2;
    let request = crate::FunctionBindingRequest {
        expected_result_type: None,
        arguments: &[],
        logical_argument_count: count,
    };
    let control = CompileControl::default();
    let crate::FunctionBindingError::InvalidBinding(message) =
        owner.resolve(request, &control).unwrap_err()
    else {
        panic!("binding shape")
    };
    assert!(message.starts_with("field unsupported argument shape"));
    assert_eq!(
        control.trace.lock().unwrap().as_slice(),
        &[(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0
        )]
    );
    let refused = CompileControl {
        trace: Mutex::new(Vec::new()),
        refusal: Some((
            0,
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
        )),
    };
    assert!(matches!(
        owner.resolve(request, &refused),
        Err(crate::FunctionBindingError::Control(
            novarocks_type_contract::CompileControlError::DeadlineExceeded
        ))
    ));
    assert_eq!(refused.trace.lock().unwrap().len(), 1);
}

#[test]
fn field_kernel_real_public_compile_binding_refuses_unsupported_shape_before_cpu_preparation() {
    let actual = super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let arguments = [crate::FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Utf8, true),
        constant: None,
    }];
    for count in [0, 1] {
        let request = crate::FunctionBindingRequest {
            arguments: &arguments[..count],
            logical_argument_count: count,
            expected_result_type: None,
        };
        // This is the actual public catalog binding entry consumed by SQL,
        // not the metadata owner's private check alone. It never constructs
        // a ScalarCallInput/instance or runs a child expression.
        let error = actual
            .resolve_bound_user(
                "field",
                crate::FunctionKind::Scalar,
                request,
                crate::binding_test_control(),
            )
            .unwrap_err();
        let crate::FunctionBindingError::InvalidBinding(message) = error else {
            panic!("explicit FIELD compile shape refusal: {error}")
        };
        assert_eq!(
            message.as_ref(),
            format!(
                "field unsupported argument shape ({count} total): field requires a value and an INT-bounded candidate list"
            )
        );
        let error = super::string_field_owner::prepared_for_test_with_policy(
            "field",
            &[FunctionValueType::new(DataType::Utf8, true)][..count],
            DecimalOverflowPolicy::ReportError,
        )
        .err()
        .expect("no CPU preparation for an unsupported shape");
        assert!(matches!(
            error,
            crate::FunctionSpecializationFailure::Binding(
                crate::FunctionBindingError::InvalidBinding(_)
            )
        ));
    }
}
