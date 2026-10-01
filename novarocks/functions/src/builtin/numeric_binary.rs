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

//! Selected binary numeric computation for exact installed builtin owners.
//! Arrow builder allocation still requires formal host memory admission.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    builder::Float64Builder,
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

/// Chosen by the exact immutable preparation owner, never from runtime names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NumericBinaryOp {
    Atan2,
    Fmod,
    Pow,
}
impl NumericBinaryOp {
    fn apply(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Atan2 => left.atan2(right),
            Self::Fmod => left % right,
            Self::Pow => left.powf(right),
        }
    }
}

enum NumericInput<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
}
impl<'a> NumericInput<'a> {
    fn checked(array: &'a ArrayRef, data_type: &DataType) -> Result<Self, KernelFailure> {
        if array.data_type() != data_type {
            return Err(internal(
                "binary numeric carrier differs from its checked argument",
            ));
        }
        macro_rules! downcast {
            ($array:ty, $variant:ident) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| internal("binary numeric selected carrier cannot be downcast"))
            };
        }
        match data_type {
            DataType::Int8 => downcast!(Int8Array, Int8),
            DataType::Int16 => downcast!(Int16Array, Int16),
            DataType::Int32 => downcast!(Int32Array, Int32),
            DataType::Int64 => downcast!(Int64Array, Int64),
            DataType::Float32 => downcast!(Float32Array, Float32),
            DataType::Float64 => downcast!(Float64Array, Float64),
            _ => Err(invalid(
                "binary numeric input is not an installed numeric profile",
            )),
        }
    }
    fn value(&self, row: usize) -> f64 {
        match self {
            Self::Int8(array) => array.value(row) as f64,
            Self::Int16(array) => array.value(row) as f64,
            Self::Int32(array) => array.value(row) as f64,
            Self::Int64(array) => array.value(row) as f64,
            Self::Float32(array) => array.value(row) as f64,
            Self::Float64(array) => array.value(row),
        }
    }
}

pub(super) fn evaluate_numeric_binary<'a>(
    op: NumericBinaryOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [
        FunctionArgumentType::Value(left_type),
        FunctionArgumentType::Value(right_type),
    ] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("binary numeric requires two exact value arguments"));
    };
    let [left, right] = input.arguments() else {
        return Err(invalid("binary numeric requires two evaluated arguments"));
    };
    let target = input.contract().result_type();
    if left_type.logical_type != ValueLogicalType::Physical
        || right_type.logical_type != ValueLogicalType::Physical
        || target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::Float64
        || !target.nullable
    {
        return Err(invalid(
            "binary numeric logical or result type differs from its exact profile",
        ));
    }
    let left_array = left.array();
    let right_array = right.array();
    let left_view = NumericInput::checked(left_array, &left_type.data_type)?;
    let right_view = NumericInput::checked(right_array, &right_type.data_type)?;
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut builder = Float64Builder::with_capacity(selection.len());
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        // Each argument owns its mapping: scalar row zero, checked constant
        // pool ordinal, original column row or compact selected ordinal.
        let left_row = left.value_row(ordinal, batch_row);
        let right_row = right.value_row(ordinal, batch_row);
        if left_row >= left_array.len() || right_row >= right_array.len() {
            return Err(internal(
                "binary numeric selected row is outside its checked carrier",
            ));
        }
        let left_null = left_array.is_null(left_row);
        let right_null = right_array.is_null(right_row);
        if (left_null && !left_type.nullable) || (right_null && !right_type.nullable) {
            return Err(internal(
                "binary numeric non-null input contains selected SQL NULL",
            ));
        }
        if left_null || right_null {
            builder.append_null();
        } else {
            // Preserve raw-input formula order: NaN^0 and atan2(Inf, Inf)
            // produce finite answers; pre-sanitizing inputs changes behavior.
            let value = op.apply(left_view.value(left_row), right_view.value(right_row));
            if value.is_finite() {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
    }
    let values = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("binary numeric compact output violates its selected contract"))
}

/// Rust allocation representability only; this does not authorize memory.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType,
        ScalarEvaluationInstance, Selection,
    };
    use novarocks_type_contract::CompilePhase;
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(index: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((index, error)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = &self.refusal
                && *at == index
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("binary numeric kernels must not wait");
        }
    }
    fn instance(name: &str, types: &[FunctionValueType; 2]) -> ScalarEvaluationInstance {
        let prepared = super::super::numeric_binary_owner::prepared_for_test(name, types).unwrap();
        ScalarEvaluationInstance::instantiate(prepared).unwrap()
    }
    fn dense(name: &str, left: ArrayRef, right: ArrayRef) -> ArrayRef {
        let types = [
            FunctionValueType::new(left.data_type().clone(), true),
            FunctionValueType::new(right.data_type().clone(), true),
        ];
        let mut instance = instance(name, &types);
        let arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
        ];
        let output = instance
            .evaluate(Selection::all(left.len()), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn floats(name: &str, left: Vec<Option<f64>>, right: Vec<Option<f64>>) -> ArrayRef {
        dense(
            name,
            Arc::new(Float64Array::from(left)),
            Arc::new(Float64Array::from(right)),
        )
    }
    fn assert_values(array: &ArrayRef, expected: &[Option<f64>]) {
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(value) => {
                    assert!(!array.is_null(row));
                    let actual = array.value(row);
                    if *value == 0.0 {
                        assert_eq!(actual.to_bits(), value.to_bits(), "signed zero");
                    } else {
                        assert!(
                            (actual - value).abs() <= value.abs().max(1.0) * 1e-14,
                            "actual={actual:?} expected={value:?}"
                        );
                    }
                }
                None => assert!(array.is_null(row)),
            }
        }
    }

    #[test]
    fn three_actual_operations_have_independent_finite_oracles() {
        let atan = floats(
            "atan2",
            vec![Some(1.0), Some(-1.0), Some(0.0), Some(3.0)],
            vec![Some(1.0), Some(0.0), Some(-1.0), Some(4.0)],
        );
        assert_values(
            &atan,
            &[
                Some(std::f64::consts::FRAC_PI_4),
                Some(-std::f64::consts::FRAC_PI_2),
                Some(std::f64::consts::PI),
                Some(0.6435011087932844),
            ],
        );
        let fmod = floats(
            "fmod",
            vec![Some(5.5), Some(-5.5), Some(5.5)],
            vec![Some(2.0), Some(2.0), Some(-2.0)],
        );
        assert_values(&fmod, &[Some(1.5), Some(-1.5), Some(1.5)]);
        let pow = floats(
            "pow",
            vec![Some(2.0), Some(-2.0), Some(4.0), Some(2.0)],
            vec![Some(3.0), Some(3.0), Some(0.5), Some(-2.0)],
        );
        assert_values(&pow, &[Some(8.0), Some(-8.0), Some(2.0), Some(0.25)]);
    }

    #[test]
    fn atan2_nonfinite_quadrants_and_signed_zero_keep_operand_order() {
        let inf = f64::INFINITY;
        let result = floats(
            "atan2",
            vec![
                Some(0.0),
                Some(-0.0),
                Some(0.0),
                Some(-0.0),
                Some(inf),
                Some(-inf),
                Some(inf),
                Some(-inf),
                Some(-1.0),
                Some(f64::NAN),
            ],
            vec![
                Some(1.0),
                Some(1.0),
                Some(-1.0),
                Some(-1.0),
                Some(inf),
                Some(inf),
                Some(-inf),
                Some(-inf),
                Some(inf),
                Some(1.0),
            ],
        );
        use std::f64::consts::{FRAC_PI_4, PI};
        assert_values(
            &result,
            &[
                Some(0.0),
                Some(-0.0),
                Some(PI),
                Some(-PI),
                Some(FRAC_PI_4),
                Some(-FRAC_PI_4),
                Some(3.0 * FRAC_PI_4),
                Some(-3.0 * FRAC_PI_4),
                Some(-0.0),
                None,
            ],
        );
    }

    #[test]
    fn fmod_nonfinite_results_null_but_infinite_divisor_keeps_dividend() {
        let result = floats(
            "fmod",
            vec![
                Some(0.0),
                Some(-0.0),
                Some(5.5),
                Some(-5.5),
                Some(1.0),
                Some(f64::INFINITY),
                Some(f64::NAN),
            ],
            vec![
                Some(2.0),
                Some(2.0),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(2.0),
                Some(1.0),
            ],
        );
        assert_values(
            &result,
            &[
                Some(0.0),
                Some(-0.0),
                Some(5.5),
                Some(-5.5),
                None,
                None,
                None,
            ],
        );
    }

    #[test]
    fn pow_evaluates_raw_nan_and_zero_rules_before_finite_filtering() {
        let result = floats(
            "pow",
            vec![
                Some(f64::NAN),
                Some(1.0),
                Some(f64::INFINITY),
                Some(-0.0),
                Some(-0.0),
                Some(-0.0),
                Some(-2.0),
                Some(1e308),
                None,
            ],
            vec![
                Some(0.0),
                Some(f64::NAN),
                Some(0.0),
                Some(3.0),
                Some(2.0),
                Some(-1.0),
                Some(0.5),
                Some(2.0),
                Some(0.0),
            ],
        );
        assert_values(
            &result,
            &[
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(-0.0),
                Some(0.0),
                None,
                None,
                None,
                None,
            ],
        );
    }

    #[test]
    fn each_installed_alias_preserves_the_pow_body() {
        for name in ["fpow", "dpow", "power"] {
            let output = floats(
                name,
                vec![Some(2.0), Some(f64::NAN), None],
                vec![Some(3.0), Some(0.0), Some(0.0)],
            );
            assert_values(&output, &[Some(8.0), Some(1.0), None]);
        }
    }

    #[test]
    fn all_36_actual_input_profile_pairs_use_float64_computation() {
        fn profiles(value: i8) -> [ArrayRef; 6] {
            [
                Arc::new(Int8Array::from(vec![value])),
                Arc::new(Int16Array::from(vec![i16::from(value)])),
                Arc::new(Int32Array::from(vec![i32::from(value)])),
                Arc::new(Int64Array::from(vec![i64::from(value)])),
                Arc::new(Float32Array::from(vec![f32::from(value)])),
                Arc::new(Float64Array::from(vec![f64::from(value)])),
            ]
        }
        let mut pairs = 0;
        for left in profiles(2) {
            for right in profiles(3) {
                let result = dense("pow", left.clone(), right);
                assert_values(&result, &[Some(8.0)]);
                pairs += 1;
            }
        }
        assert_eq!(pairs, 36);
        let result = dense(
            "fmod",
            Arc::new(Int64Array::from(vec![9007199254740993])),
            Arc::new(Int64Array::from(vec![2])),
        );
        assert_values(&result, &[Some(0.0)]);
        let result = dense(
            "pow",
            Arc::new(Float32Array::from(vec![f32::MAX])),
            Arc::new(Int8Array::from(vec![2])),
        );
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            0x4fefffffc0000020
        );
    }

    #[test]
    fn selected_column_and_original_column_have_independent_maps() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let left: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            -5.5,
            f64::INFINITY,
            99.0,
            8.0,
            100.0,
        ]));
        let right: ArrayRef = Arc::new(Int16Array::from(vec![2, 3]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int16, right, Box::default()).unwrap();
        let arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let mut instance = instance(
            "fmod",
            &[
                FunctionValueType::new(DataType::Float64, false),
                FunctionValueType::new(DataType::Int16, false),
            ],
        );
        let result = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_values(result.values(), &[Some(-1.5), Some(2.0)]);
    }

    #[test]
    fn either_side_scalar_broadcast_is_explicit_and_null_is_strict() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let left: ArrayRef = Arc::new(Int8Array::from(vec![2]));
        let right: ArrayRef = Arc::new(Int8Array::from(vec![99, 3, 99, 99, 4, 99]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Column(&right),
        ];
        let mut broadcast = instance(
            "pow",
            &[
                FunctionValueType::new(DataType::Int8, false),
                FunctionValueType::new(DataType::Int8, false),
            ],
        );
        assert_values(
            broadcast
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(8.0), Some(16.0)],
        );

        let left: ArrayRef = Arc::new(Float64Array::from(vec![Some(4.0), None]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Float64, left, Box::default()).unwrap();
        let right: ArrayRef = Arc::new(Int32Array::from(vec![2]));
        let arguments = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Scalar(&right),
        ];
        let mut reverse = instance(
            "pow",
            &[
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(DataType::Int32, false),
            ],
        );
        assert_values(
            reverse
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(16.0), None],
        );

        let wrong = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Column(&right),
        ];
        let mut wrong_shape = instance(
            "pow",
            &[
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(DataType::Int32, false),
            ],
        );
        assert!(matches!(
            wrong_shape.evaluate(selection, &wrong, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }

    #[test]
    fn constant_arguments_use_two_independent_checked_pool_ordinals() {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(3), Some(10)]));
        // Explicit finite fixture bounds, including the owner's measured
        // opaque-validation scratch bound. This is not a production grant.
        let policy = ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 1,
            max_logical_elements: 3,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("binary-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let left = pool.value(2).unwrap();
        let right = pool.value(1).unwrap();
        let arguments = [
            EvaluatedArgument::Constant(&left),
            EvaluatedArgument::Constant(&right),
        ];
        let rows = [2, 7];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let mut instance = instance("fmod", &[ty.clone(), ty]);
        assert_values(
            instance
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(1.0), Some(1.0)],
        );
        assert!(Arc::ptr_eq(left.pool().array(), right.pool().array()));
    }

    #[test]
    fn empty_selection_preserves_exact_result_and_skips_binary_body() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        let mut instance = instance("pow", &[source.clone(), source]);
        let control = Control::default();
        let result = instance
            .evaluate(Selection::all(0), &arguments, &control)
            .unwrap();
        assert!(result.values().is_empty());
        assert_eq!(result.values().data_type(), &DataType::Float64);
        assert!(result.errors().is_empty());
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            3
        );
    }

    #[test]
    fn every_body_keeps_entry_256_tail_and_publication_errors_typed_and_latched() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![3.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        let types = [source.clone(), source];
        let selection = Selection::all(320);
        for name in ["atan2", "fmod", "pow"] {
            let mut baseline = instance(name, &types);
            let control = Control::default();
            baseline.evaluate(selection, &arguments, &control).unwrap();
            let calls = control.calls();
            let body_entry = calls
                .iter()
                .enumerate()
                .filter(|(_, units)| **units == 0)
                .nth(3)
                .unwrap()
                .0;
            let interior = calls.iter().position(|units| *units == 256).unwrap();
            assert!(body_entry < interior);
            assert_eq!(calls[interior + 1], 64);
            for error in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                for at in [0, body_entry, interior, interior + 1, calls.len() - 1] {
                    let mut instance = instance(name, &types);
                    let control = Control::refusing(at, error.clone());
                    assert_eq!(
                        instance
                            .evaluate(selection, &arguments, &control)
                            .unwrap_err(),
                        error
                    );
                    assert_eq!(control.calls().len(), at + 1);
                    assert_eq!(
                        instance
                            .evaluate(selection, &arguments, &Control::default())
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                }
            }
        }
    }

    #[test]
    fn capacity_overflow_fails_before_builder_allocation_or_row_formula() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        for name in ["atan2", "fmod", "pow"] {
            let mut instance = instance(name, &[source.clone(), source.clone()]);
            let control = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(usize::MAX), &arguments, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|units| *units < 256));
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                4
            );
        }
    }
}
