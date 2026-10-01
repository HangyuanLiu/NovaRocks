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

//! Selected integer remainder computation for exact installed builtin owners.
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
pub(super) enum NumericModOp {
    Mod,
    Pmod,
}
impl NumericModOp {
    fn apply(self, left: i64, right: i64) -> Result<Option<f64>, KernelFailure> {
        if right == 0 {
            return Ok(None);
        }
        // Widen before division and absolute value. MIN % -1 and abs(MIN)
        // overflow signed BIGINT, although every remainder fits its range.
        let mut remainder = i128::from(left) % i128::from(right);
        if self == Self::Pmod && remainder < 0 {
            remainder += i128::from(right).abs();
        }
        // |r| < |b| <= 2^63; positive correction is [0, |b|-1]. Failure
        // here is an implementation bug, never a legal row data error.
        let result = i64::try_from(remainder)
            .map_err(|_| internal("integer remainder exceeds its proven signed range"))?;
        Ok(Some(result as f64))
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
                "integer remainder carrier differs from its checked argument",
            ));
        }
        macro_rules! downcast {
            ($array:ty, $variant:ident) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| {
                        internal("integer remainder selected carrier cannot be downcast")
                    })
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
                "integer remainder input is not an installed numeric profile",
            )),
        }
    }
    fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Int8(array) => Some(i64::from(array.value(row))),
            Self::Int16(array) => Some(i64::from(array.value(row))),
            Self::Int32(array) => Some(i64::from(array.value(row))),
            Self::Int64(array) => Some(array.value(row)),
            Self::Float32(array) => {
                let value = f64::from(array.value(row));
                if value.is_finite() {
                    Some(value as i64)
                } else {
                    None
                }
            }
            Self::Float64(array) => {
                let value = array.value(row);
                if value.is_finite() {
                    Some(value as i64)
                } else {
                    None
                }
            }
        }
    }
}

pub(super) fn evaluate_numeric_mod<'a>(
    op: NumericModOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [
        FunctionArgumentType::Value(left_type),
        FunctionArgumentType::Value(right_type),
    ] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "integer remainder requires two exact value arguments",
        ));
    };
    let [left, right] = input.arguments() else {
        return Err(invalid(
            "integer remainder requires two evaluated arguments",
        ));
    };
    let target = input.contract().result_type();
    if left_type.logical_type != ValueLogicalType::Physical
        || right_type.logical_type != ValueLogicalType::Physical
        || target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::Float64
        || !target.nullable
    {
        return Err(invalid(
            "integer remainder logical or result type differs from its exact profile",
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
                "integer remainder selected row is outside its checked carrier",
            ));
        }
        let left_null = left_array.is_null(left_row);
        let right_null = right_array.is_null(right_row);
        if (left_null && !left_type.nullable) || (right_null && !right_type.nullable) {
            return Err(internal(
                "integer remainder non-null input contains selected SQL NULL",
            ));
        }
        if left_null || right_null {
            builder.append_null();
        } else {
            // Signed source precision is preserved before the remainder;
            // floats retain the old finite/truncating/saturating Rust cast.
            let value = match (left_view.value(left_row), right_view.value(right_row)) {
                (Some(left), Some(right)) => op.apply(left, right)?,
                _ => None,
            };
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
    }
    let values = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("integer remainder compact output violates its selected contract"))
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
        let prepared = super::super::numeric_mod_owner::prepared_for_test(name, types).unwrap();
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
    fn signed_minimum_maximum_and_negative_divisors_have_independent_oracles() {
        let left = [
            i64::MIN,
            -1,
            i64::MIN,
            i64::MAX,
            i64::MIN,
            i64::MAX,
            -7,
            -7,
            7,
            7,
            3,
        ];
        let right = [
            -1,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            i64::MAX,
            2,
            3,
            -3,
            3,
            -3,
            0,
        ];
        let expected_mod = [
            Some(0.0),
            Some(-1.0),
            Some(0.0),
            Some(9223372036854775808.0),
            Some(-1.0),
            Some(1.0),
            Some(-1.0),
            Some(-1.0),
            Some(1.0),
            Some(1.0),
            None,
        ];
        let expected_pmod = [
            Some(0.0),
            Some(9223372036854775808.0),
            Some(0.0),
            Some(9223372036854775808.0),
            Some(9223372036854775808.0),
            Some(1.0),
            Some(2.0),
            Some(2.0),
            Some(1.0),
            Some(1.0),
            None,
        ];
        for (name, expected) in [("mod", expected_mod), ("pmod", expected_pmod)] {
            let result = dense(
                name,
                Arc::new(Int64Array::from(left.to_vec())),
                Arc::new(Int64Array::from(right.to_vec())),
            );
            assert_values(&result, &expected);
            if name == "pmod" {
                assert_eq!(
                    result
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(1)
                        .to_bits(),
                    0x43e0000000000000
                );
            }
        }
    }

    #[test]
    fn signed_sources_keep_full_integer_precision_until_final_float_output() {
        for name in ["mod", "pmod"] {
            let result = dense(
                name,
                Arc::new(Int64Array::from(vec![9007199254740993, i64::MAX, i64::MIN])),
                Arc::new(Int64Array::from(vec![2, 2, -3])),
            );
            assert_values(
                &result,
                &[
                    Some(1.0),
                    Some(1.0),
                    Some(if name == "mod" { -2.0 } else { 1.0 }),
                ],
            );
        }
    }

    #[test]
    fn finite_float_inputs_keep_rust_truncating_saturation_not_arrow_safe_cast() {
        let left = vec![
            Some(-7.9),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(-1.0),
            Some(-1.0),
            Some(7.9),
        ];
        let right = vec![
            Some(3.9),
            Some(2.0),
            Some(-1.0),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(0.9),
        ];
        for (name, expected) in [
            (
                "mod",
                [
                    Some(-1.0),
                    Some(1.0),
                    Some(0.0),
                    Some(-1.0),
                    Some(-1.0),
                    None,
                ],
            ),
            (
                "pmod",
                [
                    Some(2.0),
                    Some(1.0),
                    Some(0.0),
                    Some(9223372036854775808.0),
                    Some(9223372036854775808.0),
                    None,
                ],
            ),
        ] {
            assert_values(&floats(name, left.clone(), right.clone()), &expected);
            let result = dense(
                name,
                Arc::new(Float32Array::from(vec![f32::MAX])),
                Arc::new(Int8Array::from(vec![2])),
            );
            assert_values(&result, &[Some(1.0)]);
        }
    }

    #[test]
    fn nonfinite_zero_divisor_and_null_are_successful_null_without_row_errors() {
        for name in ["mod", "pmod"] {
            let result = floats(
                name,
                vec![
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(3.0),
                    Some(3.0),
                    Some(3.0),
                    None,
                    Some(3.0),
                    Some(-0.0),
                ],
                vec![
                    Some(1.0),
                    Some(1.0),
                    Some(f64::NAN),
                    Some(f64::NEG_INFINITY),
                    Some(0.0),
                    Some(2.0),
                    None,
                    Some(3.0),
                ],
            );
            assert_values(
                &result,
                &[None, None, None, None, None, None, None, Some(0.0)],
            );
        }
    }

    #[test]
    fn all_36_profiles_for_each_real_owner_use_the_original_integer_reader() {
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
        for (name, expected) in [("mod", -1.0), ("pmod", 2.0)] {
            let mut pairs = 0;
            for left in profiles(-7) {
                for right in profiles(3) {
                    assert_values(&dense(name, left.clone(), right), &[Some(expected)]);
                    pairs += 1;
                }
            }
            assert_eq!(pairs, 36);
        }
    }

    #[test]
    fn sparse_original_and_compact_arguments_keep_independent_row_maps() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let left: ArrayRef = Arc::new(Int64Array::from(vec![999, -7, 999, 999, -8, 999]));
        let right: ArrayRef = Arc::new(Int16Array::from(vec![3, -3]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int16, right, Box::default()).unwrap();
        let arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let types = [
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int16, false),
        ];
        let mut instance = instance("pmod", &types);
        let result = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_values(result.values(), &[Some(2.0), Some(1.0)]);
    }

    #[test]
    fn each_side_scalar_broadcast_is_explicit_and_selected_null_is_strict() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let left: ArrayRef = Arc::new(Int8Array::from(vec![-7]));
        let right: ArrayRef = Arc::new(Int8Array::from(vec![99, 3, 99, 99, -3, 99]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Column(&right),
        ];
        let types = [
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int8, false),
        ];
        let mut broadcast = instance("pmod", &types);
        assert_values(
            broadcast
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(2.0), Some(2.0)],
        );

        let left: ArrayRef = Arc::new(Float64Array::from(vec![Some(-8.9), None]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Float64, left, Box::default()).unwrap();
        let right: ArrayRef = Arc::new(Int32Array::from(vec![3]));
        let arguments = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Scalar(&right),
        ];
        let types = [
            FunctionValueType::new(DataType::Float64, true),
            FunctionValueType::new(DataType::Int32, false),
        ];
        let mut reverse = instance("mod", &types);
        assert_values(
            reverse
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(-2.0), None],
        );
        let wrong = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Column(&right),
        ];
        let mut wrong_shape = instance("mod", &types);
        assert!(matches!(
            wrong_shape.evaluate(selection, &wrong, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }

    #[test]
    fn constant_values_keep_independent_nonzero_ordinals_in_one_shared_pool() {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(-3), Some(-7)]));
        // Explicit finite test policy; the 8192-byte validation bound includes
        // the real owner's scratch model and does not grant runtime memory.
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
            Arc::new(ty.try_to_field("mod-input").unwrap()),
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
        let mut instance = instance("pmod", &[ty.clone(), ty]);
        assert_values(
            instance
                .evaluate(Selection::all(3), &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(2.0); 3],
        );
        assert!(Arc::ptr_eq(left.pool().array(), right.pool().array()));
    }

    #[test]
    fn empty_selection_keeps_exact_result_and_skips_private_remainder_body() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        for name in ["mod", "pmod"] {
            let mut instance = instance(name, &[source.clone(), source.clone()]);
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
    }

    #[test]
    fn both_owners_keep_entry_256_tail_and_publication_failures_typed_and_latched() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![-7.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![3.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        let types = [source.clone(), source];
        let selection = Selection::all(320);
        for name in ["mod", "pmod"] {
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
    fn capacity_overflow_fails_before_builder_allocation_or_remainder_work() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        for name in ["mod", "pmod"] {
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
