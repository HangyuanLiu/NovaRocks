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

//! Exact selected DROUND computation. Arrow allocation still requires host MEM admission.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
    builder::Float64Builder,
    types::{Decimal128Type, validate_decimal_precision_and_scale},
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

/// Frozen by the exact owner; binary DROUND deliberately truncates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DroundOp {
    Round,
    TruncateDigits,
}
impl DroundOp {
    pub(super) const fn arity(self) -> usize {
        match self {
            Self::Round => 1,
            Self::TruncateDigits => 2,
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
    Decimal128(&'a Decimal128Array, i8),
}
impl<'a> NumericInput<'a> {
    fn checked(array: &'a ArrayRef, data_type: &DataType) -> Result<Self, KernelFailure> {
        if array.data_type() != data_type {
            return Err(internal("dround carrier differs from its checked argument"));
        }
        macro_rules! downcast {
            ($array:ty, $variant:ident) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| internal("dround selected carrier cannot be downcast"))
            };
        }
        match data_type {
            DataType::Int8 => downcast!(Int8Array, Int8),
            DataType::Int16 => downcast!(Int16Array, Int16),
            DataType::Int32 => downcast!(Int32Array, Int32),
            DataType::Int64 => downcast!(Int64Array, Int64),
            DataType::Float32 => downcast!(Float32Array, Float32),
            DataType::Float64 => downcast!(Float64Array, Float64),
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale)
                    .map_err(|_| invalid("dround selected decimal parameters are invalid"))?;
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .map(|array| Self::Decimal128(array, *scale))
                    .ok_or_else(|| internal("dround selected decimal cannot be downcast"))
            }
            _ => Err(invalid("dround input is not an installed numeric profile")),
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
            Self::Decimal128(array, scale) => {
                // Keep the original i128->f64 conversion before scaling,
                // including its precision loss and negative-scale behavior.
                (array.value(row) as f64) / 10_f64.powi(*scale as i32)
            }
        }
    }
}

struct NumericValue<'a> {
    argument: EvaluatedArgument<'a>,
    view: NumericInput<'a>,
    nullable: bool,
}
impl<'a> NumericValue<'a> {
    fn checked(
        argument: EvaluatedArgument<'a>,
        value_type: &FunctionValueType,
    ) -> Result<Self, KernelFailure> {
        if value_type.logical_type != ValueLogicalType::Physical {
            return Err(invalid("dround input is not an exact installed profile"));
        }
        let view = NumericInput::checked(argument.array(), &value_type.data_type)?;
        Ok(Self {
            argument,
            view,
            nullable: value_type.nullable,
        })
    }
    fn value(&self, ordinal: usize, batch_row: usize) -> Result<Option<f64>, KernelFailure> {
        let row = self.argument.value_row(ordinal, batch_row);
        let array = self.argument.array();
        if row >= array.len() {
            return Err(internal(
                "dround selected row is outside its checked carrier",
            ));
        }
        if array.is_null(row) {
            if !self.nullable {
                return Err(internal("dround non-null input contains selected SQL NULL"));
            }
            Ok(None)
        } else {
            Ok(Some(self.view.value(row)))
        }
    }
}

struct DigitsInput<'a> {
    argument: EvaluatedArgument<'a>,
    array: &'a Int32Array,
    nullable: bool,
}
impl<'a> DigitsInput<'a> {
    fn checked(
        argument: EvaluatedArgument<'a>,
        source: &FunctionValueType,
    ) -> Result<Self, KernelFailure> {
        if source.logical_type != ValueLogicalType::Physical || source.data_type != DataType::Int32
        {
            return Err(invalid(
                "dround digits require the exact Physical Int32 profile",
            ));
        }
        let array = argument
            .array()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal("dround digits checked carrier cannot be downcast"))?;
        Ok(Self {
            argument,
            array,
            nullable: source.nullable,
        })
    }
    fn value(&self, ordinal: usize, batch_row: usize) -> Result<Option<i64>, KernelFailure> {
        let row = self.argument.value_row(ordinal, batch_row);
        if row >= self.array.len() {
            return Err(internal("dround digits row is outside its checked carrier"));
        }
        if self.array.is_null(row) {
            if !self.nullable {
                return Err(internal("dround non-null digits contain selected SQL NULL"));
            }
            Ok(None)
        } else {
            Ok(Some(i64::from(self.array.value(row))))
        }
    }
}

pub(super) fn evaluate_dround<'a>(
    op: DroundOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let target = input.contract().result_type();
    if target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::Float64
        || !target.nullable
    {
        return Err(invalid(
            "dround requires its exact nullable Physical Float64 result",
        ));
    }
    if input.arguments().len() != op.arity()
        || input.contract().selected().argument_types.len() != op.arity()
    {
        return Err(invalid(
            "dround arguments differ from their exact operation arity",
        ));
    }
    let (value, digits) = match (
        op,
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) {
        (DroundOp::Round, [FunctionArgumentType::Value(source)], [argument]) => {
            (NumericValue::checked(*argument, source)?, None)
        }
        (
            DroundOp::TruncateDigits,
            [
                FunctionArgumentType::Value(source),
                FunctionArgumentType::Value(digits),
            ],
            [left, right],
        ) if source.logical_type == ValueLogicalType::Physical
            && source.data_type == DataType::Float64 =>
        {
            (
                NumericValue::checked(*left, source)?,
                Some(DigitsInput::checked(*right, digits)?),
            )
        }
        _ => {
            return Err(invalid(
                "dround arguments differ from their exact installed profile",
            ));
        }
    };
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut builder = Float64Builder::with_capacity(selection.len());
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let x = value.value(ordinal, batch_row)?;
        let output = match &digits {
            None => x.map(f64::round),
            Some(digits) => match (x, digits.value(ordinal, batch_row)?) {
                (Some(x), Some(digits)) => {
                    // Int32->Int64 makes negation safe. Preserve the original
                    // magnitude->Int32 wrap, including the Int32::MIN case.
                    if digits >= 0 {
                        let factor = 10_f64.powi(digits as i32);
                        Some((x * factor).trunc() / factor)
                    } else {
                        let factor = 10_f64.powi((-digits) as i32);
                        Some((x / factor).trunc() * factor)
                    }
                }
                _ => None,
            },
        }
        .filter(|value| value.is_finite());
        match output {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    let values = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("dround compact output violates its selected contract"))
}
/// Check Rust allocation representability; this never authorizes memory.
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
            panic!("dround kernels must not wait");
        }
    }
    fn instance(types: &[FunctionValueType]) -> ScalarEvaluationInstance {
        ScalarEvaluationInstance::instantiate(
            super::super::dround_owner::prepared_for_test(types).unwrap(),
        )
        .unwrap()
    }
    fn dense(arrays: Vec<ArrayRef>, rows: usize) -> ArrayRef {
        let types: Vec<_> = arrays
            .iter()
            .map(|array| FunctionValueType::new(array.data_type().clone(), true))
            .collect();
        let mut instance = instance(&types);
        let arguments: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
        let output = instance
            .evaluate(Selection::all(rows), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn unary(values: Vec<Option<f64>>) -> ArrayRef {
        let rows = values.len();
        dense(vec![Arc::new(Float64Array::from(values))], rows)
    }
    fn binary(values: Vec<Option<f64>>, digits: Vec<Option<i32>>) -> ArrayRef {
        let rows = values.len();
        dense(
            vec![
                Arc::new(Float64Array::from(values)),
                Arc::new(Int32Array::from(digits)),
            ],
            rows,
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
    fn unary_round_uses_half_away_from_zero_and_preserves_signed_zero() {
        assert_values(
            &unary(vec![
                Some(0.5),
                Some(-0.5),
                Some(1.5),
                Some(-1.5),
                Some(2.49),
                Some(-2.49),
                Some(0.0),
                Some(-0.0),
                Some(-0.1),
                None,
            ]),
            &[
                Some(1.0),
                Some(-1.0),
                Some(2.0),
                Some(-2.0),
                Some(2.0),
                Some(-2.0),
                Some(0.0),
                Some(-0.0),
                Some(-0.0),
                None,
            ],
        );
    }
    #[test]
    fn unary_nonfinite_values_are_successful_null_without_row_errors() {
        assert_values(
            &unary(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ]),
            &[None; 4],
        );
    }
    #[test]
    fn binary_dround_truncates_instead_of_rounding_for_both_digit_signs() {
        assert_values(
            &binary(
                vec![
                    Some(1.99),
                    Some(-1.99),
                    Some(199.9),
                    Some(-199.9),
                    Some(0.0),
                    Some(-0.0),
                    Some(-0.1),
                    None,
                    Some(1.0),
                ],
                vec![
                    Some(1),
                    Some(1),
                    Some(-2),
                    Some(-2),
                    Some(3),
                    Some(-3),
                    Some(0),
                    Some(2),
                    None,
                ],
            ),
            &[
                Some(1.9),
                Some(-1.9),
                Some(100.0),
                Some(-100.0),
                Some(0.0),
                Some(-0.0),
                Some(-0.0),
                None,
                None,
            ],
        );
    }
    #[test]
    fn digits_extremes_keep_raw_pow_formula_and_int32_magnitude_wrap() {
        assert_values(
            &binary(
                vec![
                    Some(1.0),
                    Some(2.0),
                    Some(1.0),
                    Some(-1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(0.0),
                    Some(f64::INFINITY),
                    Some(f64::NAN),
                ],
                vec![
                    Some(308),
                    Some(308),
                    Some(-308),
                    Some(-308),
                    Some(309),
                    Some(-309),
                    Some(i32::MAX),
                    Some(i32::MIN),
                    Some(i32::MAX),
                    Some(-308),
                    Some(0),
                ],
            ),
            &[
                Some(1.0),
                None,
                Some(0.0),
                Some(-0.0),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        );
    }
    #[test]
    fn all_seven_unary_profiles_use_original_numeric_conversion() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(3), None])),
            Arc::new(Int16Array::from(vec![Some(3), None])),
            Arc::new(Int32Array::from(vec![Some(3), None])),
            Arc::new(Int64Array::from(vec![Some(3), None])),
            Arc::new(Float32Array::from(vec![Some(2.5), None])),
            Arc::new(Float64Array::from(vec![Some(2.5), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(250), None])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
        ];
        for array in arrays {
            assert_values(&dense(vec![array], 2), &[Some(3.0), None]);
        }
    }
    #[test]
    fn decimal_precision_negative_scale_and_float_precision_loss_are_preserved() {
        for (precision, scale, raw, expected) in [
            (18, 3, 1500_i128, 2.0_f64),
            (18, 3, -1500, -2.0),
            (18, -2, 15, 1500.0),
            (38, 0, 9_007_199_254_740_993, 9_007_199_254_740_992.0),
            (38, 0, 10_i128.pow(37), 1e37),
        ] {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![raw])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            assert_values(&dense(vec![array], 1), &[Some(expected)]);
        }
    }
    #[test]
    fn nonnullable_input_still_has_exact_nullable_result_profile() {
        for types in [
            vec![FunctionValueType::new(DataType::Int64, false)],
            vec![
                FunctionValueType::new(DataType::Float64, false),
                FunctionValueType::new(DataType::Int32, false),
            ],
        ] {
            let prepared = super::super::dround_owner::prepared_for_test(&types).unwrap();
            assert_eq!(
                prepared.contract().result_type(),
                &FunctionValueType::new(DataType::Float64, true)
            );
        }
    }
    #[test]
    fn sparse_original_compact_and_scalar_arguments_keep_independent_rows() {
        let selected_rows = [1, 4];
        let selection = Selection::try_sparse(6, &selected_rows).unwrap();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 1.99, 0.0, 0.0, 199.9, 0.0]));
        let digits: ArrayRef = Arc::new(Int32Array::from(vec![1, -2]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int32, digits, Box::default()).unwrap();
        let mut binary_instance = instance(&[
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::Int32, false),
        ]);
        let arguments = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let output = binary_instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_values(output.values(), &[Some(1.9), Some(100.0)]);
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![-1.5]));
        let mut unary = instance(&[FunctionValueType::new(DataType::Float64, false)]);
        assert_values(
            unary
                .evaluate(
                    selection,
                    &[EvaluatedArgument::Scalar(&scalar)],
                    &Control::default(),
                )
                .unwrap()
                .values(),
            &[Some(-2.0), Some(-2.0)],
        );
    }
    fn pool(ty: FunctionValueType, array: ArrayRef) -> ConstantPool {
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
        ConstantPool::try_new(
            Arc::new(ty.try_to_field("dround-input").unwrap()),
            ty,
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap()
    }
    #[test]
    fn constant_pool_nonzero_ordinals_and_typed_null_are_not_resliced() {
        let value_ty = FunctionValueType::new(DataType::Float64, true);
        let digit_ty = FunctionValueType::new(DataType::Int32, true);
        let values = pool(
            value_ty.clone(),
            Arc::new(Float64Array::from(vec![None, Some(1.99), Some(-1.5)])),
        );
        let digits = pool(
            digit_ty.clone(),
            Arc::new(Int32Array::from(vec![None, Some(8), Some(1)])),
        );
        let value = values.value(1).unwrap();
        let digit = digits.value(2).unwrap();
        assert!(Arc::ptr_eq(value.pool().array(), values.array()));
        let mut binary = instance(&[value_ty.clone(), digit_ty]);
        assert_values(
            binary
                .evaluate(
                    Selection::all(3),
                    &[
                        EvaluatedArgument::Constant(&value),
                        EvaluatedArgument::Constant(&digit),
                    ],
                    &Control::default(),
                )
                .unwrap()
                .values(),
            &[Some(1.9); 3],
        );
        let value = values.value(2).unwrap();
        let null = values.value(0).unwrap();
        let mut unary = instance(std::slice::from_ref(&value_ty));
        assert_values(
            unary
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Constant(&value)],
                    &Control::default(),
                )
                .unwrap()
                .values(),
            &[Some(-2.0); 2],
        );
        let mut unary = instance(&[value_ty]);
        assert_values(
            unary
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Constant(&null)],
                    &Control::default(),
                )
                .unwrap()
                .values(),
            &[None; 2],
        );
    }
    fn control_inputs(arity: usize) -> (Vec<FunctionValueType>, Vec<ArrayRef>) {
        let mut types = vec![FunctionValueType::new(DataType::Float64, true)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![1.99]))];
        if arity == 2 {
            types.push(FunctionValueType::new(DataType::Int32, true));
            arrays.push(Arc::new(Int32Array::from(vec![1])));
        }
        (types, arrays)
    }
    #[test]
    fn empty_selection_skips_both_private_recipes() {
        for arity in [1, 2] {
            let (types, arrays) = control_inputs(arity);
            let args: Vec<_> = arrays.iter().map(EvaluatedArgument::Scalar).collect();
            let mut instance = instance(&types);
            let control = Control::default();
            let output = instance
                .evaluate(Selection::all(0), &args, &control)
                .unwrap();
            assert_eq!(output.values().data_type(), &DataType::Float64);
            assert!(output.values().is_empty());
            assert!(output.errors().is_empty());
            assert_eq!(
                control.calls().iter().filter(|n| **n == 0).count(),
                arity + 1
            );
        }
    }
    #[test]
    fn both_recipes_keep_entry_256_tail_and_publication_typed_and_latched() {
        let selection = Selection::all(320);
        for arity in [1, 2] {
            let (types, arrays) = control_inputs(arity);
            let args: Vec<_> = arrays.iter().map(EvaluatedArgument::Scalar).collect();
            let mut baseline = instance(&types);
            let control = Control::default();
            baseline.evaluate(selection, &args, &control).unwrap();
            let calls = control.calls();
            let body = calls
                .iter()
                .enumerate()
                .filter(|(_, n)| **n == 0)
                .nth(arity + 1)
                .unwrap()
                .0;
            let interior = calls.iter().position(|n| *n == 256).unwrap();
            assert!(body < interior);
            assert_eq!(calls[interior + 1], 64);
            for error in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                for at in [0, body, interior, interior + 1, calls.len() - 1] {
                    let mut instance = instance(&types);
                    let control = Control::refusing(at, error.clone());
                    assert_eq!(
                        instance.evaluate(selection, &args, &control).unwrap_err(),
                        error
                    );
                    assert_eq!(control.calls().len(), at + 1);
                    assert_eq!(
                        instance
                            .evaluate(selection, &args, &Control::default())
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                }
            }
        }
    }
    #[test]
    fn unrepresentable_output_is_refused_before_builder_or_selected_loop() {
        for arity in [1, 2] {
            let (types, arrays) = control_inputs(arity);
            let args: Vec<_> = arrays.iter().map(EvaluatedArgument::Scalar).collect();
            let mut instance = instance(&types);
            let control = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(usize::MAX), &args, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|n| *n < 256));
            assert_eq!(
                control.calls().iter().filter(|n| **n == 0).count(),
                arity + 2
            );
        }
    }
}
