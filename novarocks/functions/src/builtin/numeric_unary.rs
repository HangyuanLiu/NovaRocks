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

//! Selected numeric unary computation for exact installed builtin owners.
//! Arrow builder allocation still requires formal host memory admission.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
    builder::{Float64Builder, Int64Builder},
    types::{Decimal128Type, validate_decimal_precision_and_scale},
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

/// Frozen by the exact preparation owner, never reselected from a SQL name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NumericUnaryOp {
    Acos,
    Asin,
    Atan,
    Cbrt,
    Ceil,
    Cos,
    Cot,
    Degrees,
    Dlog1,
    Exp,
    Floor,
    Ln,
    Log10,
    Log2,
    Radians,
    Positive,
    Sin,
    Sqrt,
    Square,
    Tan,
}
impl NumericUnaryOp {
    pub(super) const fn returns_integer(self) -> bool {
        matches!(self, Self::Ceil | Self::Floor)
    }
    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Acos => value.acos(),
            Self::Asin => value.asin(),
            Self::Atan => value.atan(),
            Self::Cbrt => value.cbrt(),
            Self::Ceil => value.ceil(),
            Self::Cos => value.cos(),
            Self::Cot => 1.0 / value.tan(),
            Self::Degrees => value.to_degrees(),
            // Preserve the original addition/rounding; ln_1p differs at zero
            // and for small inputs and is not the accepted implementation.
            Self::Dlog1 => (1.0 + value).ln(),
            Self::Exp => value.exp(),
            Self::Floor => value.floor(),
            Self::Ln => value.ln(),
            Self::Log10 => value.log10(),
            Self::Log2 => value.log2(),
            Self::Radians => value.to_radians(),
            Self::Positive => value,
            Self::Sin => value.sin(),
            Self::Sqrt => value.sqrt(),
            Self::Square => value * value,
            Self::Tan => value.tan(),
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
                "numeric unary carrier differs from its checked argument",
            ));
        }
        macro_rules! downcast {
            ($array:ty, $variant:ident) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(Self::$variant)
                    .ok_or_else(|| internal("numeric unary selected carrier cannot be downcast"))
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
                        invalid("numeric unary selected decimal parameters are invalid")
                    })?;
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .map(|array| Self::Decimal128(array, *scale))
                    .ok_or_else(|| internal("numeric unary selected decimal cannot be downcast"))
            }
            _ => Err(invalid(
                "numeric unary input is not an installed numeric profile",
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

pub(super) fn evaluate_numeric_unary<'a>(
    op: NumericUnaryOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [FunctionArgumentType::Value(source)] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("numeric unary requires one exact value argument"));
    };
    let [argument] = input.arguments() else {
        return Err(invalid("numeric unary requires one evaluated argument"));
    };
    let target = input.contract().result_type();
    let expected = if op.returns_integer() {
        DataType::Int64
    } else {
        DataType::Float64
    };
    if source.logical_type != ValueLogicalType::Physical
        || target.logical_type != ValueLogicalType::Physical
        || target.data_type != expected
    {
        return Err(invalid(
            "numeric unary logical or result type differs from its exact profile",
        ));
    }
    let array = argument.array();
    let view = NumericInput::checked(array, &source.data_type)?;
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut output = if op.returns_integer() {
        NumericOutput::Integer(Int64Builder::with_capacity(selection.len()))
    } else {
        NumericOutput::Float(Float64Builder::with_capacity(selection.len()))
    };
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= array.len() {
            return Err(internal(
                "numeric unary selected row is outside its checked carrier",
            ));
        }
        if array.is_null(row) {
            if !source.nullable {
                return Err(internal(
                    "numeric unary non-null input contains selected SQL NULL",
                ));
            }
            output.append(None, target.nullable)?;
        } else {
            // Filter after the operation: atan(Inf) and exp(-Inf) have finite
            // answers. Positive is equivalent to the old direct safe cast for
            // these seven exact profiles, with non-finite results sanitized.
            let value = op.apply(view.value(row));
            output.append(value.is_finite().then_some(value), target.nullable)?;
        }
    }
    let values = output.finish();
    work.finish()?;
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("numeric unary compact output violates its selected contract"))
}

enum NumericOutput {
    Integer(Int64Builder),
    Float(Float64Builder),
}
impl NumericOutput {
    fn append(&mut self, value: Option<f64>, nullable: bool) -> Result<(), KernelFailure> {
        match self {
            Self::Integer(builder) => {
                // The same scalar conversion used by the locked Arrow safe
                // Float64->Int64 cast; Rust saturating `as` is not equivalent.
                let value = value.and_then(arrow_cast::num_cast::<f64, i64>);
                match value {
                    Some(value) => builder.append_value(value),
                    None if nullable => builder.append_null(),
                    None => {
                        return Err(internal(
                            "numeric unary successful NULL contradicts its result type",
                        ));
                    }
                }
            }
            Self::Float(builder) => match value {
                Some(value) => builder.append_value(value),
                None if nullable => builder.append_null(),
                None => {
                    return Err(internal(
                        "numeric unary successful NULL contradicts its result type",
                    ));
                }
            },
        }
        Ok(())
    }
    fn finish(self) -> ArrayRef {
        match self {
            Self::Integer(mut builder) => Arc::new(builder.finish()),
            Self::Float(mut builder) => Arc::new(builder.finish()),
        }
    }
}

/// Check Rust allocation representability, never an application capacity grant.
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
            panic!("numeric unary kernels must never wait");
        }
    }
    fn instance(name: &str, source: FunctionValueType) -> ScalarEvaluationInstance {
        let prepared = super::super::numeric_unary_owner::prepared_for_test(name, source).unwrap();
        ScalarEvaluationInstance::instantiate(prepared).unwrap()
    }
    fn dense(name: &str, source: FunctionValueType, array: ArrayRef) -> ArrayRef {
        let mut instance = instance(name, source);
        let arguments = [EvaluatedArgument::Column(&array)];
        let output = instance
            .evaluate(Selection::all(array.len()), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn floats(name: &str, values: Vec<Option<f64>>) -> ArrayRef {
        dense(
            name,
            FunctionValueType::new(DataType::Float64, true),
            Arc::new(Float64Array::from(values)),
        )
    }
    fn assert_float(value: f64, expected: f64) {
        if expected == 0.0 {
            assert_eq!(
                value.to_bits(),
                expected.to_bits(),
                "signed zero must retain the original operation's bits"
            );
        } else {
            assert!(
                (value - expected).abs() <= expected.abs().max(1.0) * 1e-14,
                "actual={value:?} expected={expected:?}"
            );
        }
    }
    fn assert_floats(array: &ArrayRef, expected: &[Option<f64>]) {
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!array.is_null(row));
                    assert_float(array.value(row), *expected);
                }
                None => assert!(array.is_null(row)),
            }
        }
    }

    #[test]
    fn twenty_real_operations_have_independent_finite_oracles() {
        use std::f64::consts::{FRAC_PI_2, FRAC_PI_3, FRAC_PI_4, FRAC_PI_6, PI};
        // Expected answers are identities/constants, not calls to the same
        // operation used by the production enum. libm last-bit identity is
        // not promised; signed zero has a separate exact-bit assertion.
        let cases = [
            ("acos", 0.5, FRAC_PI_3),
            ("asin", 0.5, FRAC_PI_6),
            ("atan", 1.0, FRAC_PI_4),
            ("cbrt", -8.0, -2.0),
            ("ceil", -1.2, -1.0),
            ("cos", PI, -1.0),
            ("cot", FRAC_PI_4, 1.0),
            ("degress", PI, 180.0),
            ("dlog1", 0.0, 0.0),
            ("exp", 0.0, 1.0),
            ("floor", -1.2, -2.0),
            ("ln", 1.0, 0.0),
            ("log10", 100.0, 2.0),
            ("log2", 8.0, 3.0),
            ("radians", 180.0, PI),
            ("positive", -2.0, -2.0),
            ("sin", FRAC_PI_2, 1.0),
            ("sqrt", 4.0, 2.0),
            ("square", -3.0, 9.0),
            ("tan", FRAC_PI_4, 1.0),
        ];
        assert_eq!(cases.len(), 20);
        for (name, value, expected) in cases {
            let array = floats(name, vec![Some(value), None]);
            if ["ceil", "floor"].contains(&name) {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(array.value(0), if name == "ceil" { -1 } else { -2 });
                assert!(array.is_null(1));
            } else {
                assert_floats(&array, &[Some(expected), None]);
            }
        }
    }

    #[test]
    fn twenty_real_operations_keep_nonfinite_domain_and_signed_zero_rules() {
        use std::f64::consts::FRAC_PI_2;
        let nan = f64::from_bits(0xfff8123456789abc);
        let inf = f64::INFINITY;
        let cases = [
            (
                "acos",
                [2.0, nan, inf, -0.0],
                [None, None, None, Some(FRAC_PI_2)],
            ),
            (
                "asin",
                [-2.0, nan, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "atan",
                [nan, inf, -inf, -0.0],
                [None, Some(FRAC_PI_2), Some(-FRAC_PI_2), Some(-0.0)],
            ),
            (
                "cbrt",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "ceil",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(0.0)],
            ),
            ("cos", [nan, inf, -inf, -0.0], [None, None, None, Some(1.0)]),
            ("cot", [0.0, -0.0, nan, inf], [None, None, None, None]),
            (
                "degress",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "dlog1",
                [-2.0, -1.0, nan, -0.0],
                [None, None, None, Some(0.0)],
            ),
            (
                "exp",
                [nan, inf, -inf, -0.0],
                [None, None, Some(0.0), Some(1.0)],
            ),
            (
                "floor",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(0.0)],
            ),
            ("ln", [0.0, -1.0, nan, inf], [None, None, None, None]),
            ("log10", [0.0, -1.0, nan, inf], [None, None, None, None]),
            ("log2", [0.0, -1.0, nan, inf], [None, None, None, None]),
            (
                "radians",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "positive",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "sin",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "sqrt",
                [-1.0, nan, inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
            (
                "square",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(0.0)],
            ),
            (
                "tan",
                [nan, inf, -inf, -0.0],
                [None, None, None, Some(-0.0)],
            ),
        ];
        assert_eq!(cases.len(), 20);
        for (name, values, expected) in cases {
            let array = floats(name, values.into_iter().map(Some).collect());
            if ["ceil", "floor"].contains(&name) {
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, None, None, Some(0)]
                );
            } else {
                assert_floats(&array, &expected);
            }
        }
    }

    #[test]
    fn aliases_use_only_the_existing_supported_operations() {
        for (alias, canonical) in [
            ("ceiling", "ceil"),
            ("dceil", "ceil"),
            ("dfloor", "floor"),
            ("dexp", "exp"),
            ("dlog10", "log10"),
            ("dsqrt", "sqrt"),
        ] {
            let input = vec![Some(1.25), Some(-0.0), None];
            let left = floats(alias, input.clone());
            let right = floats(canonical, input);
            assert_eq!(left.to_data(), right.to_data(), "alias {alias}");
        }
    }

    #[test]
    fn all_seven_actual_numeric_profiles_preserve_input_conversion() {
        let profiles: [ArrayRef; 7] = [
            Arc::new(Int8Array::from(vec![Some(-2), None])),
            Arc::new(Int16Array::from(vec![Some(-2), None])),
            Arc::new(Int32Array::from(vec![Some(-2), None])),
            Arc::new(Int64Array::from(vec![Some(-2), None])),
            Arc::new(Float32Array::from(vec![Some(-2.0), None])),
            Arc::new(Float64Array::from(vec![Some(-2.0), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(-20), None])
                    .with_precision_and_scale(3, 1)
                    .unwrap(),
            ),
        ];
        for array in profiles {
            let source = FunctionValueType::new(array.data_type().clone(), true);
            let result = dense("atan", source, array);
            assert_floats(&result, &[Some(-1.1071487177940904), None]);
        }
    }

    #[test]
    fn decimal_scaling_large_precision_and_integer_rounding_remain_original() {
        for (precision, scale, raw, expected) in [
            (18, 3, 12345i128, 12.345_f64),
            (18, -2, -12345, -1234500.0),
            (38, 0, 99999999999999999999999999999999999999, 1e38),
            (38, 0, 9007199254740993, 9007199254740992.0),
        ] {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![raw])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            let result = dense(
                "positive",
                FunctionValueType::new(array.data_type().clone(), false),
                array,
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                expected.to_bits()
            );
        }
        let result = dense(
            "positive",
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![9007199254740993])),
        );
        assert_floats(&result, &[Some(9007199254740992.0)]);
    }

    #[test]
    fn dlog1_retains_add_then_log_rounding_instead_of_ln_1p() {
        let result = floats("dlog1", vec![Some(1e-20), Some(-1e-20), Some(-0.0)]);
        assert_floats(&result, &[Some(0.0), Some(0.0), Some(0.0)]);
    }

    #[test]
    fn ceil_floor_keep_arrow_safe_integer_boundary_instead_of_saturation() {
        for name in ["ceil", "floor"] {
            let result = dense(
                name,
                FunctionValueType::new(DataType::Int64, false),
                Arc::new(Int64Array::from(vec![i64::MAX, i64::MIN, 9007199254740993])),
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(i64::MIN), Some(9007199254740992)]
            );
            let result = floats(
                name,
                vec![
                    Some(9223372036854775808.0),
                    Some(-9223372036854775808.0),
                    Some(9223372036854774784.0),
                    Some(-9223372036854777856.0),
                ],
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(i64::MIN), Some(9223372036854774784), None]
            );
        }
    }

    #[test]
    fn positive_nonnullable_float_preserves_successful_null_and_minus_zero() {
        for array in [
            Arc::new(Float32Array::from(vec![f32::NAN, f32::INFINITY, -0.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![f64::NAN, f64::NEG_INFINITY, -0.0])) as ArrayRef,
        ] {
            let source = FunctionValueType::new(array.data_type().clone(), false);
            let mut instance = instance("positive", source);
            assert!(
                instance.contract().result_type().nullable,
                "the exact float owner must admit its existing sanitized NULL result"
            );
            let arguments = [EvaluatedArgument::Column(&array)];
            let result = instance
                .evaluate(Selection::all(3), &arguments, &Control::default())
                .unwrap();
            assert!(result.errors().is_empty());
            assert_floats(result.values(), &[None, None, Some(-0.0)]);
        }
    }

    #[test]
    fn sparse_column_compact_column_and_scalar_keep_explicit_row_mapping() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            4.0,
            -1.0,
            f64::INFINITY,
            9.0,
            -10.0,
        ]));
        let arguments = [EvaluatedArgument::Column(&array)];
        let mut column = instance("sqrt", FunctionValueType::new(DataType::Float64, false));
        let result = column
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_floats(result.values(), &[Some(2.0), Some(3.0)]);
        let compact_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(16.0), None]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Float64, compact_array, Box::default())
                .unwrap();
        let arguments = [EvaluatedArgument::SelectedColumn(&compact)];
        let mut compact_instance =
            instance("sqrt", FunctionValueType::new(DataType::Float64, true));
        assert_floats(
            compact_instance
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(4.0), None],
        );
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![25.0]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        let mut broadcast = instance("sqrt", FunctionValueType::new(DataType::Float64, false));
        assert_floats(
            broadcast
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(5.0), Some(5.0)],
        );
    }

    #[test]
    fn constant_nonzero_ordinal_broadcasts_without_copying_its_pool() {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(-1), Some(9)]));
        // Explicit finite fixture policy only, not a production profile/grant.
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
            Arc::new(ty.try_to_field("unary-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let constant = pool.value(2).unwrap();
        let arguments = [EvaluatedArgument::Constant(&constant)];
        let mut instance = instance("sqrt", ty);
        let result = instance
            .evaluate(Selection::all(3), &arguments, &Control::default())
            .unwrap();
        assert_floats(result.values(), &[Some(3.0); 3]);
        assert!(Arc::ptr_eq(constant.pool().array(), pool.array()));
    }

    #[test]
    fn empty_selection_keeps_result_profile_and_skips_private_body() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        for (name, expected) in [("sqrt", DataType::Float64), ("floor", DataType::Int64)] {
            let mut instance = instance(name, FunctionValueType::new(DataType::Float64, true));
            let control = Control::default();
            let result = instance
                .evaluate(Selection::all(0), &arguments, &control)
                .unwrap();
            assert!(result.values().is_empty());
            assert_eq!(result.values().data_type(), &expected);
            assert!(result.errors().is_empty());
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                2
            );
        }
    }

    #[test]
    fn entry_256_tail_and_output_publication_preserve_typed_refusal_without_replay() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![4.0]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        let source = FunctionValueType::new(DataType::Float64, true);
        let selection = Selection::all(320);
        let mut baseline = instance("sqrt", source.clone());
        let control = Control::default();
        baseline.evaluate(selection, &arguments, &control).unwrap();
        let calls = control.calls();
        let body_entry = calls
            .iter()
            .enumerate()
            .filter(|(_, units)| **units == 0)
            .nth(2)
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
                let mut instance = instance("sqrt", source.clone());
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

    #[test]
    fn capacity_overflow_refuses_before_builder_allocation_or_row_work() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        for name in ["sqrt", "ceil"] {
            let mut instance = instance(name, FunctionValueType::new(DataType::Float64, true));
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
