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

//! Selected-row ABS computation for the exact builtin owner.
//! Arrow builder allocation still requires the host's formal memory admission.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, PrimitiveArray,
    builder::{FixedSizeBinaryBuilder, PrimitiveBuilder},
    types::{
        ArrowPrimitiveType, Decimal128Type, DecimalType, Float32Type, Float64Type, Int8Type,
        Int16Type, Int32Type, Int64Type, validate_decimal_precision_and_scale,
    },
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

/// The caller supplies the checked scalar contract and evaluated argument.
/// Output ordinals follow Selection; no unselected input value is inspected.
pub(super) fn evaluate_abs<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [FunctionArgumentType::Value(source)] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("ABS requires one exact value argument"));
    };
    let [argument] = input.arguments() else {
        return Err(invalid("ABS requires one evaluated argument"));
    };
    let target = input.contract().result_type();
    if source.nullable != target.nullable {
        return Err(invalid(
            "ABS result must preserve selected input nullability",
        ));
    }
    let selection = input.selection();
    let values = match (
        source.logical_type,
        &source.data_type,
        target.logical_type,
        &target.data_type,
    ) {
        (
            ValueLogicalType::Physical,
            DataType::Int8,
            ValueLogicalType::Physical,
            DataType::Int16,
        ) => primitive::<Int8Type, Int16Type>(input, *argument, control, |value| {
            Ok(i16::from(value).abs())
        })?,
        (
            ValueLogicalType::Physical,
            DataType::Int16,
            ValueLogicalType::Physical,
            DataType::Int32,
        ) => primitive::<Int16Type, Int32Type>(input, *argument, control, |value| {
            Ok(i32::from(value).abs())
        })?,
        (
            ValueLogicalType::Physical,
            DataType::Int32,
            ValueLogicalType::Physical,
            DataType::Int64,
        ) => primitive::<Int32Type, Int64Type>(input, *argument, control, |value| {
            Ok(i64::from(value).abs())
        })?,
        (
            ValueLogicalType::Physical,
            DataType::Int64,
            ValueLogicalType::LargeInt,
            DataType::FixedSizeBinary(16),
        ) => {
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .ok_or_else(|| internal("ABS selected Int64 carrier cannot be downcast"))?;
            largeint(input, *argument, control, |row| {
                Ok(i128::from(array.value(row)).abs())
            })?
        }
        (
            ValueLogicalType::LargeInt,
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
            DataType::FixedSizeBinary(16),
        ) => {
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| internal("ABS selected LargeInt carrier cannot be downcast"))?;
            if array.value_length() != 16 {
                return Err(internal("ABS LargeInt carrier has an incorrect byte width"));
            }
            largeint(input, *argument, control, |row| {
                let bytes = array
                    .value(row)
                    .try_into()
                    .map_err(|_| internal("ABS LargeInt value has an incorrect byte width"))?;
                // LARGEINT has no wider signed carrier. Preserve its existing
                // two's-complement minimum-value contract, without a row error.
                Ok(i128::from_be_bytes(bytes).wrapping_abs())
            })?
        }
        (
            ValueLogicalType::Physical,
            DataType::Float32,
            ValueLogicalType::Physical,
            DataType::Float32,
        ) => primitive::<Float32Type, Float32Type>(input, *argument, control, |value| {
            Ok(value.abs())
        })?,
        (
            ValueLogicalType::Physical,
            DataType::Float64,
            ValueLogicalType::Physical,
            DataType::Float64,
        ) => primitive::<Float64Type, Float64Type>(input, *argument, control, |value| {
            Ok(value.abs())
        })?,
        (
            ValueLogicalType::Physical,
            DataType::Decimal128(precision, scale),
            ValueLogicalType::Physical,
            DataType::Decimal128(output_precision, output_scale),
        ) if precision == output_precision && scale == output_scale => {
            validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale)
                .map_err(|_| invalid("ABS selected Decimal128 precision or scale is invalid"))?;
            primitive::<Decimal128Type, Decimal128Type>(input, *argument, control, |value| {
                Decimal128Type::validate_decimal_precision(value, *precision, *scale)
                    .map_err(|_| internal("ABS input exceeds selected Decimal128 precision"))?;
                value
                    .checked_abs()
                    .ok_or_else(|| internal("ABS admitted Decimal128 value cannot be represented"))
            })?
        }
        _ => {
            return Err(invalid(
                "ABS input and output differ from an exact installed overload",
            ));
        }
    };
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("ABS compact output violates its selected contract"))
}

/// Check Rust allocation representability, not an application capacity grant.
fn output_capacity(rows: usize, width: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(width)
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

fn selected_row(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    ordinal: usize,
    batch_row: usize,
    nullable: bool,
) -> Result<Option<usize>, KernelFailure> {
    // Row identity comes from the same checked Selection walk. Constant uses
    // its checked pool ordinal; Scalar and compact columns keep their own maps.
    let row = argument.value_row(ordinal, batch_row);
    if ordinal >= selection.len() || row >= argument.array().len() {
        return Err(internal(
            "ABS selected argument row is outside its checked carrier",
        ));
    }
    if argument.array().is_null(row) {
        if !nullable {
            return Err(internal("ABS non-null selected argument contains SQL NULL"));
        }
        Ok(None)
    } else {
        Ok(Some(row))
    }
}

fn primitive<I: ArrowPrimitiveType, O: ArrowPrimitiveType>(
    input: ScalarCallInput<'_, '_>,
    argument: EvaluatedArgument<'_>,
    control: &dyn KernelEvaluationControl,
    mut absolute: impl FnMut(I::Native) -> Result<O::Native, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let array = argument
        .array()
        .as_any()
        .downcast_ref::<PrimitiveArray<I>>()
        .ok_or_else(|| internal("ABS selected primitive carrier cannot be downcast"))?;
    let selection = input.selection();
    output_capacity(selection.len(), std::mem::size_of::<O::Native>())?;
    let mut builder = PrimitiveBuilder::<O>::with_capacity(selection.len())
        .with_data_type(input.contract().result_type().data_type.clone());
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        match selected_row(
            argument,
            selection,
            ordinal,
            batch_row,
            input.contract().result_type().nullable,
        )? {
            Some(row) => builder.append_value(absolute(array.value(row))?),
            None => builder.append_null(),
        }
    }
    let result = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    Ok(result)
}

fn largeint(
    input: ScalarCallInput<'_, '_>,
    argument: EvaluatedArgument<'_>,
    control: &dyn KernelEvaluationControl,
    mut absolute: impl FnMut(usize) -> Result<i128, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let selection = input.selection();
    output_capacity(selection.len(), 16)?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(selection.len(), 16);
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        match selected_row(
            argument,
            selection,
            ordinal,
            batch_row,
            input.contract().result_type().nullable,
        )? {
            Some(row) => builder
                .append_value(absolute(row)?.to_be_bytes())
                .map_err(|_| internal("ABS LargeInt output has an incorrect byte width"))?,
            None => builder.append_null(),
        }
    }
    let result = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConstantPolicy, ConstantPool, FunctionValueType, ScalarEvaluationInstance};
    use arrow_array::{
        Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
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
                && index == *at
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("ABS must never wait");
        }
    }

    fn instance(source: FunctionValueType) -> ScalarEvaluationInstance {
        ScalarEvaluationInstance::instantiate(super::super::abs_owner::prepared_for_test(source))
            .unwrap()
    }
    fn physical(data_type: DataType, nullable: bool) -> FunctionValueType {
        FunctionValueType::new(data_type, nullable)
    }
    fn column_result(array: ArrayRef, source: FunctionValueType) -> ArrayRef {
        let mut instance = instance(source);
        let arguments = [EvaluatedArgument::Column(&array)];
        let output = instance
            .evaluate(Selection::all(array.len()), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn largeints(values: &[Option<i128>]) -> ArrayRef {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
        for value in values {
            match value {
                Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }
    fn assert_largeints(array: &ArrayRef, expected: &[Option<i128>]) {
        assert_eq!(array.data_type(), &DataType::FixedSizeBinary(16));
        let array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(array.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(value) => {
                    assert!(!array.is_null(row));
                    assert_eq!(array.value(row), value.to_be_bytes());
                }
                None => assert!(array.is_null(row)),
            }
        }
    }

    #[test]
    fn int8_minimum_widens_to_int16_with_nulls() {
        let output = column_result(
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(-1),
                Some(0),
                Some(i8::MAX),
                None,
            ])),
            physical(DataType::Int8, true),
        );
        let values = output.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(128), Some(1), Some(0), Some(127), None]
        );
    }
    #[test]
    fn int16_minimum_widens_to_int32() {
        let output = column_result(
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, i16::MAX])),
            physical(DataType::Int16, false),
        );
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[32768, 1, 0, 32767]
        );
    }
    #[test]
    fn int32_minimum_widens_to_int64() {
        let output = column_result(
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, i32::MAX])),
            physical(DataType::Int32, false),
        );
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2147483648, 1, 0, 2147483647]
        );
    }
    #[test]
    fn int64_minimum_widens_to_big_endian_largeint() {
        let output = column_result(
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(-1),
                Some(0),
                Some(i64::MAX),
                None,
            ])),
            physical(DataType::Int64, true),
        );
        assert_largeints(
            &output,
            &[
                Some(9223372036854775808),
                Some(1),
                Some(0),
                Some(9223372036854775807),
                None,
            ],
        );
    }
    #[test]
    fn largeint_minimum_preserves_wrapping_contract_and_exact_bytes() {
        let output = column_result(
            largeints(&[Some(i128::MIN), Some(-123), Some(0), Some(i128::MAX), None]),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        );
        assert_largeints(
            &output,
            &[Some(i128::MIN), Some(123), Some(0), Some(i128::MAX), None],
        );
    }
    #[test]
    fn float32_abs_clears_only_sign_bits_including_nan_payloads() {
        let bits = [
            0x80000000u32,
            0,
            0xff800000,
            0x7f800000,
            0xffc12345,
            0x7fc54321,
            0xbf800000,
        ];
        let output = column_result(
            Arc::new(Float32Array::from(
                bits.iter()
                    .map(|bits| f32::from_bits(*bits))
                    .collect::<Vec<_>>(),
            )),
            physical(DataType::Float32, false),
        );
        let values = output.as_any().downcast_ref::<Float32Array>().unwrap();
        for (row, bits) in bits.iter().enumerate() {
            assert_eq!(values.value(row).to_bits(), bits & 0x7fffffff);
        }
    }
    #[test]
    fn float64_abs_clears_only_sign_bits_including_nan_payloads() {
        let bits = [
            0x8000000000000000u64,
            0,
            0xfff0000000000000,
            0x7ff0000000000000,
            0xfff8123456789abc,
            0x7ff8abcdef123456,
            0xbff0000000000000,
        ];
        let output = column_result(
            Arc::new(Float64Array::from(
                bits.iter()
                    .map(|bits| f64::from_bits(*bits))
                    .collect::<Vec<_>>(),
            )),
            physical(DataType::Float64, false),
        );
        let values = output.as_any().downcast_ref::<Float64Array>().unwrap();
        for (row, bits) in bits.iter().enumerate() {
            assert_eq!(values.value(row).to_bits(), bits & 0x7fffffffffffffff);
        }
    }
    #[test]
    fn decimal128_preserves_precision_scale_and_exact_unscaled_values() {
        for (precision, scale, negative, expected) in [
            (3, 1, -999i128, 999i128),
            (18, -2, -123456789012345678, 123456789012345678),
            (
                38,
                7,
                -99999999999999999999999999999999999999,
                99999999999999999999999999999999999999,
            ),
        ] {
            let array = Decimal128Array::from(vec![Some(negative), Some(0), None])
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let output = column_result(
                Arc::new(array),
                physical(DataType::Decimal128(precision, scale), true),
            );
            assert_eq!(output.data_type(), &DataType::Decimal128(precision, scale));
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(expected), Some(0), None]
            );
        }
    }

    #[test]
    fn sparse_decimal_does_not_inspect_unselected_invalid_precision_values() {
        let source = physical(DataType::Decimal128(3, 1), true);
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![9999, -123, 9999])
                .with_precision_and_scale(3, 1)
                .unwrap(),
        );
        let rows = [1];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let args = [EvaluatedArgument::Column(&array)];
        let mut sparse = instance(source.clone());
        let output = sparse
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            123
        );
        assert!(output.errors().is_empty());
        let mut dense = instance(source);
        assert!(matches!(
            dense.evaluate(Selection::all(3), &args, &Control::default()),
            Err(KernelFailure::Internal(_))
        ));
    }

    #[test]
    fn selected_column_uses_compact_ordinals_and_scalar_broadcast_is_explicit() {
        let rows = [2, 7, 9];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let array: ArrayRef = Arc::new(Int8Array::from(vec![Some(-4), None, Some(i8::MIN)]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int8, array, Box::default()).unwrap();
        let arguments = [EvaluatedArgument::SelectedColumn(&compact)];
        let mut compact_instance = instance(physical(DataType::Int8, true));
        let result = compact_instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(4), None, Some(128)]
        );

        let scalar: ArrayRef = Arc::new(Int8Array::from(vec![-7]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut scalar_instance = instance(physical(DataType::Int8, false));
        let result = scalar_instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[7, 7, 7]
        );
        let arguments = [EvaluatedArgument::Column(&scalar)];
        let mut wrong_shape = instance(physical(DataType::Int8, false));
        assert!(matches!(
            wrong_shape.evaluate(selection, &arguments, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }

    #[test]
    fn constant_nonzero_pool_ordinal_broadcasts_only_its_value() {
        let ty = physical(DataType::Int8, true);
        let array: ArrayRef = Arc::new(Int8Array::from(vec![None, Some(-3), Some(i8::MIN)]));
        // Explicit finite fixture limits, not a production policy or MEM grant.
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
            // The existing shared model includes bounded diagnostic/header
            // scratch in addition to this three-value Arrow backing.
            max_library_validation_bytes: 8192,
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("abs-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let value = pool.value(2).unwrap();
        let rows = [1, 6];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let arguments = [EvaluatedArgument::Constant(&value)];
        let mut instance = instance(ty);
        let result = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[128, 128]
        );
        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
    }

    #[test]
    fn empty_selection_returns_exact_empty_output_without_abs_body() {
        let rows = [];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut instance = instance(physical(DataType::Int64, true));
        let control = Control::default();
        let output = instance.evaluate(selection, &arguments, &control).unwrap();
        assert!(output.values().is_empty());
        assert_eq!(output.values().data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            instance.contract().result_type().logical_type,
            ValueLogicalType::LargeInt
        );
        assert!(output.errors().is_empty());
        // Wrapper entry and argument entry only; the private body has its own
        // zero-work entry, which must not run on an empty selected domain.
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            2
        );
    }

    #[test]
    fn actual_body_entry_256_and_tail_refusals_preserve_each_outer_category() {
        let scalar: ArrayRef = Arc::new(Int8Array::from(vec![-2]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let source = physical(DataType::Int8, true);
        let selection = Selection::all(320);
        let control = Control::default();
        let mut baseline = instance(source.clone());
        baseline.evaluate(selection, &arguments, &control).unwrap();
        let calls = control.calls();
        let body_entry = calls
            .iter()
            .enumerate()
            .filter(|(_, units)| **units == 0)
            .nth(2)
            .unwrap()
            .0;
        let body_interior = calls.iter().position(|units| *units == 256).unwrap();
        let body_tail = body_interior + 1;
        assert!(body_entry < body_interior);
        assert_eq!(calls[body_tail], 64);
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in [0, body_entry, body_interior, body_tail] {
                let control = Control::refusing(at, error.clone());
                let mut instance = instance(source.clone());
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &control)
                        .unwrap_err(),
                    error
                );
                assert_eq!(
                    control.calls().len(),
                    at + 1,
                    "failure must stop before publishing or later checkpoints"
                );
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &Control::default())
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
            }
        }
    }

    #[test]
    fn selected_output_capacity_overflow_is_refused_before_any_row_or_allocation() {
        for data_type in [DataType::Int8, DataType::Int64, DataType::Float64] {
            let scalar: ArrayRef = match data_type {
                DataType::Int8 => Arc::new(Int8Array::from(vec![-1])),
                DataType::Int64 => Arc::new(Int64Array::from(vec![-1])),
                _ => Arc::new(Float64Array::from(vec![-1.0])),
            };
            let arguments = [EvaluatedArgument::Scalar(&scalar)];
            // Nullable avoids a non-null selected-row scan before the actual
            // body. Selection itself retains no dense row storage.
            let mut instance = instance(physical(data_type, true));
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
                3
            );
        }
    }
}
