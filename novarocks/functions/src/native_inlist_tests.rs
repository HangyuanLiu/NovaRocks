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
//! Shared original signed IN/state/control witnesses. Not formal MEM admission.
use super::{InError, InObservation, InRows, signed_equality_legacy, signed_equality_observed};
use crate::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, NullArray,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
fn values(array: &BooleanArray) -> Vec<Option<bool>> {
    array.iter().collect()
}
fn source(width: usize) -> ArrayRef {
    match width {
        8 => Arc::new(Int8Array::from(vec![Some(-1), None, Some(7)])),
        16 => Arc::new(Int16Array::from(vec![Some(-1), None, Some(7)])),
        32 => Arc::new(Int32Array::from(vec![Some(-1), None, Some(7)])),
        64 => Arc::new(Int64Array::from(vec![Some(-1), None, Some(7)])),
        _ => unreachable!(),
    }
}
#[test]
fn inlist_core_signed_full_pair_matrix_original_nulls_and_scalar() {
    for left in [8, 16, 32, 64] {
        for right in [8, 16, 32, 64] {
            let left = source(left);
            let column = source(right);
            assert_eq!(
                values(&signed_equality_legacy(&left, &column).unwrap().unwrap()),
                vec![Some(true), None, Some(true)]
            );
            let scalar = column.slice(0, 1);
            assert_eq!(
                values(&signed_equality_legacy(&left, &scalar).unwrap().unwrap()),
                vec![Some(true), None, Some(false)]
            );
        }
    }
}
#[test]
fn inlist_core_original_match_dominates_null_and_root_null() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
    let candidate: ArrayRef = Arc::new(Int32Array::from(vec![Some(1); 3]));
    for negated in [false, true] {
        let mut state = InRows::legacy(&input);
        state.candidate_nulls_legacy(&candidate).unwrap();
        state.equalities_legacy(&signed_equality_legacy(&input, &candidate).unwrap().unwrap());
        let null: ArrayRef = Arc::new(Int32Array::from(vec![None; 3]));
        state.candidate_nulls_legacy(&null).unwrap();
        state.equalities_legacy(&signed_equality_legacy(&input, &null).unwrap().unwrap());
        assert_eq!(
            values(&state.finish_legacy(&input, negated)),
            vec![Some(!negated), None, None]
        );
        let bare: ArrayRef = Arc::new(NullArray::new(3));
        assert_eq!(
            values(&InRows::legacy(&bare).finish_legacy(&bare, negated)),
            vec![None; 3]
        );
    }
}
#[test]
fn inlist_core_original_length_message_is_full_and_empty_assembly_exact() {
    let input = source(32);
    let mut state = InRows::legacy(&input);
    let empty = input.slice(0, 0);
    assert_eq!(
        state.candidate_nulls_legacy(&empty).unwrap_err(),
        "IN predicate value length mismatch: input has 3, value has 0"
    );
    assert_eq!(
        InRows::legacy(&empty)
            .finish_legacy(&empty, false)
            .to_data(),
        BooleanArray::from(Vec::<bool>::new()).to_data()
    );
}
struct Control {
    trace: Mutex<Vec<u32>>,
    fail: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = &self.fail {
            if trace.len() == *at {
                return Err(cause.clone());
            }
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN never waits")
    }
}
fn invoke(control: &dyn KernelEvaluationControl) -> Result<BooleanArray, InError<KernelFailure>> {
    let mut units = 0u32;
    let mut observe = |event| {
        match event {
            InObservation::Step => {
                units += 1;
                if units < 256 {
                    return Ok(());
                }
            }
            InObservation::OpaqueBoundary => {}
        }
        let due = units;
        units = 0;
        control.checkpoint(due)
    };
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(7); 513]));
    let candidate: ArrayRef = Arc::new(Int8Array::from(vec![Some(7)]));
    let mut state = InRows::begin_observed(&input, &mut observe)?;
    state.candidate_nulls_observed(&candidate, &mut observe)?;
    let eq = signed_equality_observed(&input, &candidate, &mut observe)?.unwrap();
    state.equalities_observed(&eq, &mut observe)?;
    state.finish_observed(&input, false, &mut observe)
}
#[test]
fn inlist_core_every_real_callback_preserves_all_seven_causes_without_footer() {
    let success = Control {
        trace: Mutex::new(Vec::new()),
        fail: None,
    };
    assert!(
        values(&invoke(&success).unwrap())
            .into_iter()
            .all(|v| v == Some(true))
    );
    let trace = success.trace.into_inner().unwrap();
    assert!(trace.iter().any(|v| *v == 256));
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid refusal")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational refusal")),
        KernelFailure::InstanceFailed,
    ];
    for at in 1..=trace.len() {
        for cause in &causes {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                fail: Some((at, cause.clone())),
            };
            assert!(matches!(invoke(&control),Err(InError::Host(found)) if found==*cause));
            assert_eq!(
                *control.trace.lock().unwrap(),
                trace[..at],
                "no callback after originating refusal"
            );
        }
    }
}

#[test]
fn inlist_recipe_exact_signed_widths_nullable_result_and_foreign_carriers() {
    use crate::{ComparisonPrepareError, PreparedNativeInListRecipe};
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    };
    struct Compile;
    impl PureCompileControl for Compile {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let input = FunctionValueType::new(data_type.clone(), false);
        let nullable = FunctionValueType::new(data_type, true);
        let result = FunctionValueType::new(DataType::Boolean, true);
        for negated in [false, true] {
            let recipe = PreparedNativeInListRecipe::try_new(
                negated,
                &input,
                &[&input, &nullable],
                &result,
                &Compile,
            )
            .unwrap();
            assert_eq!(recipe.source_type(), &input);
            assert_eq!(recipe.candidate_types(), &[input.clone(), nullable.clone()]);
            assert_eq!(recipe.result_type(), &result);
            assert_eq!(recipe.negated(), negated);
            assert_eq!(
                PreparedNativeInListRecipe::try_new(
                    negated,
                    &input,
                    &[&nullable],
                    &FunctionValueType::new(DataType::Boolean, false),
                    &Compile
                )
                .unwrap_err(),
                ComparisonPrepareError::TypeMismatch
            );
        }
    }
    for ty in [
        DataType::Utf8,
        DataType::UInt64,
        DataType::Null,
        DataType::Decimal128(4, 0),
    ] {
        let input = FunctionValueType::new(ty, true);
        assert_eq!(
            PreparedNativeInListRecipe::try_new(
                false,
                &input,
                &[],
                &FunctionValueType::new(DataType::Boolean, true),
                &Compile
            )
            .unwrap_err(),
            ComparisonPrepareError::Unsupported
        );
    }
}
#[test]
fn inlist_shared_selected_parent_mapping_truthonly_and_error_placeholder_are_distinct() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(9), Some(11)]));
    let candidate: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(9)]));
    for truth_only in [false, true] {
        let mut state = InRows::legacy(&input);
        state
            .candidate_nulls_selected_observed(&candidate, &[0, 2], &mut |_| Ok::<_, ()>(()))
            .unwrap();
        state
            .equalities_selected_observed(
                &BooleanArray::from(vec![None, Some(true)]),
                &[0, 2],
                &mut |_| Ok::<_, ()>(()),
            )
            .unwrap();
        let out = state
            .finish_selected_observed(&input, false, truth_only, &[3], &mut |_| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(
            values(&out),
            if truth_only {
                vec![Some(false), Some(false), Some(true), None]
            } else {
                vec![None, None, Some(true), None]
            }
        );
    }
}

#[test]
fn inlist_recipe_actual_compile_callbacks_preserve_three_causes_and_no_footer() {
    use crate::{ComparisonPrepareError, PreparedNativeInListRecipe};
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    };
    struct Compile {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        stop: usize,
        cause: CompileControlError,
    }
    impl PureCompileControl for Compile {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            assert!(trace.len() < self.stop, "no compile footer after refusal");
            trace.push((phase, units));
            if trace.len() == self.stop {
                Err(self.cause)
            } else {
                Ok(())
            }
        }
    }
    let source = FunctionValueType::new(arrow_schema::DataType::Int64, true);
    let result = FunctionValueType::new(arrow_schema::DataType::Boolean, true);
    let success = Compile {
        trace: Mutex::new(vec![]),
        stop: usize::MAX,
        cause: CompileControlError::Cancelled,
    };
    PreparedNativeInListRecipe::try_new(false, &source, &[&source, &source], &result, &success)
        .unwrap();
    let trace = success.trace.into_inner().unwrap();
    for stop in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Compile {
                trace: Mutex::new(vec![]),
                stop,
                cause,
            };
            let error = PreparedNativeInListRecipe::try_new(
                false,
                &source,
                &[&source, &source],
                &result,
                &control,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert!(matches!(
                error,
                ComparisonPrepareError::Control(_) | ComparisonPrepareError::Kernel(_)
            ));
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop]);
        }
    }
}
