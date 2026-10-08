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
use crate::KernelDiagnostic;
use arrow_array::{
    Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow_buffer::i256;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(
        &self,
        _: CompilePhase,
        n: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after originating refusal");
        }
        trace.push(n);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric NEGATE never waits")
    }
}
fn recipe(ty: DataType) -> PreparedNativeNegateRecipe {
    let t = if ty == DataType::FixedSizeBinary(16) {
        FunctionValueType::try_with_logical_type(ty, true, ValueLogicalType::LargeInt).unwrap()
    } else {
        FunctionValueType::new(ty, true)
    };
    PreparedNativeNegateRecipe::try_new(&t, &t, &Compile).unwrap()
}
#[test]
fn native_negate_all_original_accurate_profiles_call_same_array_core() {
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(-2), None])),
        Arc::new(Int16Array::from(vec![Some(-2), None])),
        Arc::new(Int32Array::from(vec![Some(-2), None])),
        Arc::new(Int64Array::from(vec![Some(-2), None])),
        Arc::new(Float32Array::from(vec![Some(-2.0), None])),
        Arc::new(Float64Array::from(vec![Some(-2.0), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(-2), None])
                .with_precision_and_scale(38, 6)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![Some(i256::from_i128(-2)), None])
                .with_precision_and_scale(76, 6)
                .unwrap(),
        ),
        crate::largeint::array_from_i128(&[Some(-2), None]).unwrap(),
    ];
    for input in inputs {
        let recipe = recipe(input.data_type().clone());
        for row in 0..2 {
            let outcome = recipe
                .evaluate_row(
                    EvaluatedArgument::Column(&input),
                    row,
                    row,
                    &Control::default(),
                )
                .unwrap();
            let NativeNegateRowResult::Value(out) = outcome else {
                panic!("original valid profile succeeds")
            };
            let zero = crate::legacy_literal::eval(&recipe.zero, 1).unwrap();
            let old = crate::legacy_arithmetic::eval_sub_arrays(
                zero,
                input.slice(row, 1),
                input.data_type().clone(),
                false,
                DecimalOverflowPolicy::OutputNull,
            )
            .unwrap();
            assert_eq!(out.to_data(), old.to_data());
            assert_eq!(out.is_null(0), row == 1);
        }
    }
}
#[test]
fn native_negate_original_int64_error_retains_current_ordinal_and_full_message() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(-2)]));
    let recipe = recipe(DataType::Int64);
    let NativeNegateRowResult::RowError(error) = recipe
        .evaluate_row(EvaluatedArgument::Column(&input), 5, 0, &Control::default())
        .unwrap()
    else {
        panic!("original i64 overflow must remain required data error")
    };
    assert_eq!(error.selected_ordinal(), 5);
    assert_eq!(
        error.message(),
        "Arithmetic overflow: Overflow happened on: 0 - -9223372036854775808"
    );
    assert!(matches!(
        recipe
            .evaluate_row(EvaluatedArgument::Column(&input), 0, 1, &Control::default())
            .unwrap(),
        NativeNegateRowResult::Value(_)
    ));
}
#[test]
fn native_negate_original_control_every_seven_causes_keep_exact_prefix() {
    let input: ArrayRef = Arc::new(Float64Array::from(vec![Some(-101.9)]));
    let recipe = recipe(DataType::Float64);
    let baseline = Control::default();
    recipe
        .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &baseline)
        .unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
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
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let error = recipe
                .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &control)
                .unwrap_err();
            assert_eq!(error, cause);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn native_negate_original_wider_numeric_zero_refusals_are_explicit() {
    for ty in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal32(7, 2),
        DataType::Decimal64(18, 2),
        DataType::Null,
    ] {
        let t = FunctionValueType::new(ty.clone(), true);
        let error = PreparedNativeNegateRecipe::try_new(&t, &t, &Compile).unwrap_err();
        assert!(
            matches!(error,ArithmeticPrepareError::Kernel(KernelFailure::InvalidProgram(message))if message.message()==format!("NEGATE is not supported for data type {ty:?}"))
        );
    }
}

#[test]
fn native_negate_selected_sparse_compact_scalar_empty_and_inherited_errors() {
    use crate::{SelectedValues, Selection};
    let input: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-10.0),
        Some(-20.0),
        None,
        Some(-40.0),
    ]));
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let recipe = recipe(DataType::Float64);
    let result = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            selection,
            &Control::default(),
        )
        .unwrap();
    let out = result
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(out.values().as_ref(), &[20.0, 40.0]);
    let compact: ArrayRef = Arc::new(Float64Array::from(vec![Some(-20.0), None]));
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Float64,
        compact,
        Box::from([RowDataError::new(1, "original child error")]),
    )
    .unwrap();
    let result = recipe
        .evaluate_selected(
            EvaluatedArgument::SelectedColumn(&compact),
            selection,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(result.errors()[0].message(), "original child error");
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
    assert!(result.values().is_null(1));
    let scalar = input.slice(0, 1);
    let result = recipe
        .evaluate_selected(
            EvaluatedArgument::Scalar(&scalar),
            selection,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10.0, 10.0]
    );
    let empty = [];
    let selection = Selection::try_sparse(4, &empty).unwrap();
    assert_eq!(
        recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                selection,
                &Control::default()
            )
            .unwrap()
            .values()
            .len(),
        0
    );
}

#[test]
fn native_negate_large_selected_ref_projection_adds_actual_quantum() {
    use crate::Selection;
    let input: ArrayRef = Arc::new(Float64Array::from(vec![Some(-1.0); 277]));
    let recipe = recipe(DataType::Float64);
    let control = Control::default();
    let result = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            Selection::all(277),
            &control,
        )
        .unwrap();
    assert_eq!(result.values().len(), 277);
    let trace = control.trace.lock().unwrap();
    assert_eq!(
        trace.iter().filter(|&&n| n == 256).count(),
        278,
        "277 bounded original scalar operations plus actual 277-reference projection"
    );
}

#[test]
fn native_negate_computed_domain_is_exact_for_every_original_full_carrier() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(1, 0),
        DataType::Decimal256(1, 0),
        DataType::FixedSizeBinary(16),
    ] {
        for nullable in [false, true] {
            let source = if dtype == DataType::FixedSizeBinary(16) {
                FunctionValueType::try_with_logical_type(
                    dtype.clone(),
                    nullable,
                    ValueLogicalType::LargeInt,
                )
                .unwrap()
            } else {
                FunctionValueType::new(dtype.clone(), nullable)
            };
            let computed = native_negate_computed_result_type(&source, &Compile).unwrap();
            assert_eq!(computed.logical_type, source.logical_type);
            assert_eq!(computed.data_type, source.data_type);
            assert_eq!(
                computed.nullable,
                nullable
                    || matches!(
                        dtype,
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Decimal128(_, _)
                            | DataType::Decimal256(_, _)
                    )
            );
            let recipe = PreparedNativeNegateRecipe::try_new(&source, &computed, &Compile).unwrap();
            assert_eq!(recipe.source_type(), &source);
            assert_eq!(recipe.result_type(), &computed);
        }
    }
}
#[test]
fn native_negate_computed_full_decimal_carriers_preserve_original_successful_null() {
    // Original independent raw full-carrier baseline passed before extraction.
    for input in [
        Arc::new(
            Decimal128Array::from(vec![Some(10), Some(-10), Some(1)])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(10)),
                Some(i256::from_i128(-10)),
                Some(i256::ONE),
            ])
            .with_precision_and_scale(1, 0)
            .unwrap(),
        ) as ArrayRef,
    ] {
        let source = FunctionValueType::new(input.data_type().clone(), false);
        let computed = native_negate_computed_result_type(&source, &Compile).unwrap();
        let recipe = PreparedNativeNegateRecipe::try_new(&source, &computed, &Compile).unwrap();
        let selected = recipe
            .evaluate_selected(
                EvaluatedArgument::Column(&input),
                crate::Selection::all(3),
                &Control::default(),
            )
            .unwrap();
        let zero = crate::legacy_literal::eval(&recipe.zero, 3).unwrap();
        let raw = crate::legacy_arithmetic::eval_sub_arrays(
            zero,
            input.clone(),
            input.data_type().clone(),
            false,
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        assert!(selected.errors().is_empty());
        assert_eq!(selected.values().to_data(), raw.to_data());
        assert_eq!(selected.values().null_count(), 2);
    }
}
#[test]
fn native_negate_computed_specialization_keeps_actual_compile_primary_without_footer() {
    struct Fail {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        at: usize,
        cause: novarocks_type_contract::CompileControlError,
    }
    impl PureCompileControl for Fail {
        fn checkpoint(
            &self,
            p: CompilePhase,
            n: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            assert!(n <= 256);
            let mut trace = self.trace.lock().unwrap();
            assert!(trace.len() <= self.at, "callback after originating failure");
            let at = trace.len();
            trace.push((p, n));
            if at == self.at {
                Err(self.cause)
            } else {
                Ok(())
            }
        }
    }
    struct Record(Mutex<Vec<(CompilePhase, u32)>>);
    impl PureCompileControl for Record {
        fn checkpoint(
            &self,
            p: CompilePhase,
            n: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            assert!(n <= 256);
            self.0.lock().unwrap().push((p, n));
            Ok(())
        }
    }
    let source = FunctionValueType::new(DataType::Decimal256(76, 6), false);
    let success = Record(Mutex::new(Vec::new()));
    native_negate_computed_result_type(&source, &success).unwrap();
    let trace = success.0.into_inner().unwrap();
    for at in 0..trace.len() {
        for cause in [
            novarocks_type_contract::CompileControlError::Cancelled,
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ] {
            let control = Fail {
                trace: Mutex::new(Vec::new()),
                at,
                cause,
            };
            assert_eq!(
                native_negate_computed_result_type(&source, &control)
                    .unwrap_err()
                    .control_error(),
                Some(cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
