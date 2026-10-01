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

//! Selected log/sign/constants computation for exact installed builtin owners.
//! Arrow builder allocation still requires formal host memory admission.

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

/// Frozen by the exact owner, including the two distinct LOG arities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NumericElementaryOp {
    LogNatural,
    LogBase,
    Sign,
    E,
    Pi,
}
impl NumericElementaryOp {
    pub(super) const fn arity(self) -> usize {
        match self {
            Self::LogNatural | Self::Sign => 1,
            Self::LogBase => 2,
            Self::E | Self::Pi => 0,
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
            return Err(internal(
                "elementary numeric carrier differs from its checked argument",
            ));
        }
        macro_rules! downcast {
            ($array:ty, $variant:ident) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| {
                        internal("elementary numeric selected carrier cannot be downcast")
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
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale)
                    .map_err(|_| {
                        invalid("elementary numeric selected decimal parameters are invalid")
                    })?;
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .map(|array| Self::Decimal128(array, *scale))
                    .ok_or_else(|| {
                        internal("elementary numeric selected decimal cannot be downcast")
                    })
            }
            _ => Err(invalid(
                "elementary numeric input is not an installed numeric profile",
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
        decimal: bool,
    ) -> Result<Self, KernelFailure> {
        if value_type.logical_type != ValueLogicalType::Physical
            || (!decimal && matches!(value_type.data_type, DataType::Decimal128(_, _)))
        {
            return Err(invalid(
                "elementary numeric input is not an exact installed profile",
            ));
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
                "elementary numeric selected row is outside its checked carrier",
            ));
        }
        if array.is_null(row) {
            if !self.nullable {
                return Err(internal(
                    "elementary numeric non-null input contains selected SQL NULL",
                ));
            }
            Ok(None)
        } else {
            Ok(Some(self.view.value(row)))
        }
    }
}

enum ElementaryInput<'a> {
    Constant(f64),
    Unary(NumericValue<'a>),
    Binary(NumericValue<'a>, NumericValue<'a>),
}
impl ElementaryInput<'_> {
    fn compute(
        &self,
        op: NumericElementaryOp,
        ordinal: usize,
        batch_row: usize,
    ) -> Result<Option<f64>, KernelFailure> {
        let result = match (self, op) {
            (Self::Constant(value), NumericElementaryOp::E | NumericElementaryOp::Pi) => {
                Some(*value)
            }
            (Self::Unary(value), NumericElementaryOp::LogNatural) => {
                value.value(ordinal, batch_row)?.map(f64::ln)
            }
            (Self::Unary(value), NumericElementaryOp::Sign) => {
                value.value(ordinal, batch_row)?.map(|value| {
                    if value > 0.0 {
                        1.0
                    } else if value < 0.0 {
                        -1.0
                    } else {
                        0.0
                    }
                })
            }
            (Self::Binary(base, value), NumericElementaryOp::LogBase) => {
                match (
                    base.value(ordinal, batch_row)?,
                    value.value(ordinal, batch_row)?,
                ) {
                    (Some(base), Some(value)) if base > 0.0 && base != 1.0 && value > 0.0 => {
                        Some(value.log(base))
                    }
                    _ => None,
                }
            }
            _ => {
                return Err(internal(
                    "elementary numeric operation differs from its prepared inputs",
                ));
            }
        };
        // Filter only the result: LOG(Inf, finite) gives finite zero; SIGN
        // receives NaN and infinities and owns their comparison answers.
        Ok(result.filter(|value| value.is_finite()))
    }
}

pub(super) fn evaluate_numeric_elementary<'a>(
    op: NumericElementaryOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let target = input.contract().result_type();
    if target.logical_type != ValueLogicalType::Physical || target.data_type != DataType::Float64 {
        return Err(invalid(
            "elementary numeric requires its exact Physical Float64 result",
        ));
    }
    if input.arguments().len() != op.arity()
        || input.contract().selected().argument_types.len() != op.arity()
    {
        return Err(invalid(
            "elementary numeric arguments differ from their exact operation arity",
        ));
    }
    let inputs = match (
        op,
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) {
        (NumericElementaryOp::E, [], []) if target.nullable => {
            ElementaryInput::Constant(std::f64::consts::E)
        }
        (NumericElementaryOp::Pi, [], []) if target.nullable => {
            ElementaryInput::Constant(std::f64::consts::PI)
        }
        (
            NumericElementaryOp::LogNatural | NumericElementaryOp::Sign,
            [FunctionArgumentType::Value(source)],
            [argument],
        ) => {
            if (op == NumericElementaryOp::LogNatural && !target.nullable)
                || (op == NumericElementaryOp::Sign && target.nullable != source.nullable)
            {
                return Err(invalid(
                    "elementary numeric result nullability differs from its exact profile",
                ));
            }
            ElementaryInput::Unary(NumericValue::checked(*argument, source, true)?)
        }
        (
            NumericElementaryOp::LogBase,
            [
                FunctionArgumentType::Value(base),
                FunctionArgumentType::Value(value),
            ],
            [left, right],
        ) if target.nullable => ElementaryInput::Binary(
            NumericValue::checked(*left, base, false)?,
            NumericValue::checked(*right, value, false)?,
        ),
        _ => {
            return Err(invalid(
                "elementary numeric arguments differ from their exact arity or result profile",
            ));
        }
    };
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut builder = Float64Builder::with_capacity(selection.len());
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        match inputs.compute(op, ordinal, batch_row)? {
            Some(value) => builder.append_value(value),
            None if target.nullable => builder.append_null(),
            None => {
                return Err(internal(
                    "elementary numeric successful NULL contradicts its result type",
                ));
            }
        }
    }
    let values = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("elementary numeric compact output violates its selected contract"))
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
            panic!("binary numeric kernels must not wait");
        }
    }
    fn instance(name: &str, types: &[FunctionValueType]) -> ScalarEvaluationInstance {
        let prepared =
            super::super::numeric_elementary_owner::prepared_for_test(name, types).unwrap();
        ScalarEvaluationInstance::instantiate(prepared).unwrap()
    }
    fn dense(name: &str, arrays: Vec<ArrayRef>, rows: usize) -> ArrayRef {
        let types: Vec<_> = arrays
            .iter()
            .map(|array| FunctionValueType::new(array.data_type().clone(), true))
            .collect();
        let mut instance = instance(name, &types);
        let arguments: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
        let output = instance
            .evaluate(Selection::all(rows), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn unary(name: &str, values: Vec<Option<f64>>) -> ArrayRef {
        let len = values.len();
        dense(name, vec![Arc::new(Float64Array::from(values))], len)
    }
    fn binary_log(base: Vec<Option<f64>>, values: Vec<Option<f64>>) -> ArrayRef {
        let len = values.len();
        dense(
            "log",
            vec![
                Arc::new(Float64Array::from(base)),
                Arc::new(Float64Array::from(values)),
            ],
            len,
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
    fn natural_log_has_independent_finite_and_domain_oracles() {
        let output = unary(
            "log",
            vec![
                Some(1.0),
                Some(2.0),
                Some(0.0),
                Some(-1.0),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(f64::NAN),
                Some(-0.0),
                None,
            ],
        );
        assert_values(
            &output,
            &[
                Some(0.0),
                Some(f64::from_bits(0x3fe62e42fefa39ef)),
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
    fn base_log_keeps_base_value_order_and_raw_infinite_base_signed_zero() {
        let output = binary_log(
            vec![
                Some(2.0),
                Some(10.0),
                Some(0.5),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(1.0),
                Some(0.0),
                Some(-2.0),
                Some(f64::NAN),
                Some(2.0),
                Some(2.0),
                Some(f64::INFINITY),
                None,
            ],
            vec![
                Some(8.0),
                Some(100.0),
                Some(2.0),
                Some(2.0),
                Some(0.5),
                Some(8.0),
                Some(8.0),
                Some(8.0),
                Some(8.0),
                Some(0.0),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(8.0),
            ],
        );
        assert_values(
            &output,
            &[
                Some(3.0),
                Some(2.0),
                Some(-1.0),
                Some(0.0),
                Some(-0.0),
                None,
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
    fn sign_compares_raw_nan_infinity_and_signed_zero_without_sanitizing_inputs() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::from_bits(0xfff8123456789abc),
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0,
            0.0,
            2.0,
            -2.0,
        ]));
        let mut instance = instance("sign", &[FunctionValueType::new(DataType::Float64, false)]);
        assert!(!instance.contract().result_type().nullable);
        let arguments = [EvaluatedArgument::Column(&array)];
        let output = instance
            .evaluate(Selection::all(8), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        assert_values(
            output.values(),
            &[
                Some(0.0),
                Some(0.0),
                Some(1.0),
                Some(-1.0),
                Some(0.0),
                Some(0.0),
                Some(1.0),
                Some(-1.0),
            ],
        );
        assert_values(&unary("sign", vec![None, Some(-0.0)]), &[None, Some(0.0)]);
    }

    #[test]
    fn e_pi_are_zero_argument_exact_bits_for_dense_and_sparse_outputs() {
        for (name, expected) in [("e", 0x4005bf0a8b145769u64), ("pi", 0x400921fb54442d18)] {
            let mut instance = instance(name, &[]);
            assert!(instance.contract().result_type().nullable);
            let output = instance
                .evaluate(Selection::all(3), &[], &Control::default())
                .unwrap();
            assert!(output.errors().is_empty());
            let values = output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            assert_eq!(
                values
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                [expected; 3]
            );
            let rows = [1, 7];
            let selection = Selection::try_sparse(10, &rows).unwrap();
            let output = instance
                .evaluate(selection, &[], &Control::default())
                .unwrap();
            assert_eq!(output.selection(), selection);
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                [expected; 2]
            );
        }
    }

    #[test]
    fn unary_log_and_sign_preserve_all_seven_numeric_profiles() {
        let profiles: [ArrayRef; 7] = [
            Arc::new(Int8Array::from(vec![Some(1), None])),
            Arc::new(Int16Array::from(vec![Some(1), None])),
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(Float32Array::from(vec![Some(1.0), None])),
            Arc::new(Float64Array::from(vec![Some(1.0), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1000), None])
                    .with_precision_and_scale(18, 3)
                    .unwrap(),
            ),
        ];
        for array in profiles {
            assert_values(&dense("log", vec![array.clone()], 2), &[Some(0.0), None]);
            assert_values(&dense("sign", vec![array], 2), &[Some(1.0), None]);
        }
    }

    #[test]
    fn base_log_preserves_all_36_installed_numeric_profile_pairs() {
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
        for base in profiles(2) {
            for value in profiles(8) {
                assert_values(&dense("log", vec![base.clone(), value], 1), &[Some(3.0)]);
                pairs += 1;
            }
        }
        assert_eq!(pairs, 36);
    }

    #[test]
    fn decimal_reader_preserves_original_scale_precision_and_sign_rules() {
        for (precision, scale, raw, expected) in [
            (18, 3, 1000i128, 0.0_f64),
            (18, -2, 1, 4.605170185988092),
            (
                38,
                0,
                10000000000000000000000000000000000000,
                85.1956484407797,
            ),
        ] {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![raw])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            assert_values(&dense("log", vec![array], 1), &[Some(expected)]);
        }
        for scale in [-3, 0, 3] {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(-1), Some(0), Some(1), None])
                    .with_precision_and_scale(38, scale)
                    .unwrap(),
            );
            assert_values(
                &dense("sign", vec![array], 4),
                &[Some(-1.0), Some(0.0), Some(1.0), None],
            );
        }
    }

    #[test]
    fn selected_original_compact_and_scalar_arguments_have_independent_maps() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let base: ArrayRef = Arc::new(Int8Array::from(vec![0, 2, 0, 0, 10, 0]));
        let values: ArrayRef = Arc::new(Float64Array::from(vec![8.0, 100.0]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Float64, values, Box::default()).unwrap();
        let arguments = [
            EvaluatedArgument::Column(&base),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let mut binary = instance(
            "log",
            &[
                FunctionValueType::new(DataType::Int8, false),
                FunctionValueType::new(DataType::Float64, false),
            ],
        );
        let output = binary
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_values(output.values(), &[Some(3.0), Some(2.0)]);
        let scalar: ArrayRef = Arc::new(Int8Array::from(vec![-2]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut unary = instance("sign", &[FunctionValueType::new(DataType::Int8, false)]);
        assert_values(
            unary
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(-1.0), Some(-1.0)],
        );
    }

    #[test]
    fn constant_ordinal_broadcast_and_unary_null_keep_exact_pool_backing() {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(2), Some(8)]));
        // Explicit finite fixture bounds, never a production policy or grant.
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
            Arc::new(ty.try_to_field("elementary-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let base = pool.value(1).unwrap();
        let value = pool.value(2).unwrap();
        let arguments = [
            EvaluatedArgument::Constant(&base),
            EvaluatedArgument::Constant(&value),
        ];
        let mut binary = instance("log", &[ty.clone(), ty.clone()]);
        assert_values(
            binary
                .evaluate(Selection::all(3), &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(3.0); 3],
        );
        assert!(Arc::ptr_eq(base.pool().array(), value.pool().array()));
        let null = pool.value(0).unwrap();
        let arguments = [EvaluatedArgument::Constant(&null)];
        let mut unary = instance("sign", &[ty]);
        assert_values(
            unary
                .evaluate(Selection::all(2), &arguments, &Control::default())
                .unwrap()
                .values(),
            &[None, None],
        );
    }

    #[test]
    fn every_arity_empty_domain_skips_private_body_and_keeps_float_result() {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let source = FunctionValueType::new(DataType::Float64, true);
        for (name, arity) in [("log", 1), ("log", 2), ("sign", 1), ("e", 0), ("pi", 0)] {
            let types = vec![source.clone(); arity];
            let arguments = vec![EvaluatedArgument::Scalar(&scalar); arity];
            let mut instance = instance(name, &types);
            let control = Control::default();
            let output = instance
                .evaluate(Selection::all(0), &arguments, &control)
                .unwrap();
            assert!(output.values().is_empty());
            assert_eq!(output.values().data_type(), &DataType::Float64);
            assert!(output.errors().is_empty());
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                arity + 1
            );
        }
    }

    #[test]
    fn each_recipe_keeps_entry_256_tail_and_publication_refusals_typed_and_latched() {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let source = FunctionValueType::new(DataType::Float64, true);
        let selection = Selection::all(320);
        for (name, arity) in [("log", 1), ("log", 2), ("sign", 1), ("e", 0), ("pi", 0)] {
            let types = vec![source.clone(); arity];
            let arguments = vec![EvaluatedArgument::Scalar(&scalar); arity];
            let mut baseline = instance(name, &types);
            let control = Control::default();
            baseline.evaluate(selection, &arguments, &control).unwrap();
            let calls = control.calls();
            let body_entry = calls
                .iter()
                .enumerate()
                .filter(|(_, units)| **units == 0)
                .nth(arity + 1)
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
    fn capacity_overflow_refuses_before_allocation_or_actual_elementary_row_work() {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let source = FunctionValueType::new(DataType::Float64, true);
        for (name, arity) in [("log", 1), ("log", 2), ("sign", 1), ("e", 0), ("pi", 0)] {
            let types = vec![source.clone(); arity];
            let arguments = vec![EvaluatedArgument::Scalar(&scalar); arity];
            let mut instance = instance(name, &types);
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
                arity + 2
            );
        }
    }
}
