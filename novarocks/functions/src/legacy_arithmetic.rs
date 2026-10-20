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
//! Original already-evaluated v1 arithmetic author. No arena or runtime host.
use crate::largeint;
use crate::legacy_decimal::{div_round_i256, pow10_i128, pow10_i256};
use arrow_arith::numeric::{add, div, mul, rem, sub};
use arrow_array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array};
use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;

#[derive(Debug)]
pub struct ArithmeticFailure(Box<ArithmeticFailureCause>);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArrowOperation {
    Cast,
    Compute,
    Construct,
}
#[derive(Debug)]
enum ArithmeticFailureCause {
    Contract(novarocks_failure_contract::BoundedDetail),
    Numeric(&'static str),
    Math(crate::legacy_decimal::DecimalMathFailure),
    Slice(std::array::TryFromSliceError),
    Arrow {
        operation: ArrowOperation,
        source: arrow_schema::ArrowError,
    },
}
impl ArithmeticFailure {
    fn contract(detail: impl std::fmt::Display) -> Self {
        Self(Box::new(ArithmeticFailureCause::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        )))
    }
    pub fn data_reason(&self) -> Option<crate::DataFailureReason> {
        use arrow_schema::ArrowError;
        match self.0.as_ref() {
            ArithmeticFailureCause::Numeric(_) => Some(crate::DataFailureReason::NumericOutOfRange),
            ArithmeticFailureCause::Math(cause) => Some(cause.data_reason()),
            ArithmeticFailureCause::Arrow {
                operation: ArrowOperation::Cast | ArrowOperation::Compute,
                source: ArrowError::ArithmeticOverflow(_),
            } => Some(crate::DataFailureReason::NumericOutOfRange),
            ArithmeticFailureCause::Arrow {
                operation: ArrowOperation::Compute,
                source: ArrowError::DivideByZero,
            }
            | ArithmeticFailureCause::Arrow {
                operation: ArrowOperation::Cast,
                source: ArrowError::CastError(_),
            } => Some(crate::DataFailureReason::InvalidValue),
            _ => None,
        }
    }
}
impl From<crate::legacy_decimal::DecimalMathFailure> for ArithmeticFailure {
    fn from(cause: crate::legacy_decimal::DecimalMathFailure) -> Self {
        Self(Box::new(ArithmeticFailureCause::Math(cause)))
    }
}
impl std::fmt::Display for ArithmeticFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            ArithmeticFailureCause::Contract(detail) => f.write_str(detail.as_str()),
            ArithmeticFailureCause::Numeric(name) => write!(
                f,
                "Expr evaluate meet error: The '{name}' operation involving decimal values overflows"
            ),
            ArithmeticFailureCause::Math(cause) => cause.fmt(f),
            ArithmeticFailureCause::Slice(cause) => {
                write!(f, "arithmetic LARGEINT fixed width: {cause}")
            }
            ArithmeticFailureCause::Arrow { source, .. } => source.fmt(f),
        }
    }
}
impl std::error::Error for ArithmeticFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            ArithmeticFailureCause::Math(cause) => Some(cause),
            ArithmeticFailureCause::Slice(cause) => Some(cause),
            ArithmeticFailureCause::Arrow { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ArithmeticFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        if let Some(reason) = self.data_reason() {
            return reason.failure_fact();
        }
        let (class, code) = match self.0.as_ref() {
            ArithmeticFailureCause::Contract(_) => (FailureClass::Invariant, 231),
            ArithmeticFailureCause::Slice(_) => (FailureClass::Invariant, 232),
            ArithmeticFailureCause::Arrow {
                source: ArrowError::MemoryError(_),
                ..
            } => (FailureClass::Resource, 233),
            ArithmeticFailureCause::Arrow {
                source: ArrowError::OffsetOverflowError(_),
                ..
            } => (FailureClass::Representation, 234),
            ArithmeticFailureCause::Arrow {
                source: ArrowError::IoError(..),
                ..
            } => (FailureClass::Unexpected, 235),
            ArithmeticFailureCause::Arrow { .. } => (FailureClass::Invariant, 236),
            ArithmeticFailureCause::Numeric(_) | ArithmeticFailureCause::Math(_) => {
                unreachable!("data returned above")
            }
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(
            self.0.as_ref(),
            ArithmeticFailureCause::Arrow {
                source: ArrowError::OffsetOverflowError(_),
                ..
            }
        ) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(38).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
/// Value and outer witnesses can only be constructed by this domain's classifier.
#[derive(Debug)]
pub enum ClassifiedArithmeticFailure {
    Value(ArithmeticValueFailure),
    Outer(ArithmeticOuterFailure),
}
#[derive(Debug)]
/// A value failure cannot be promoted through the outer-only kernel constructor.
/// ```compile_fail
/// fn invalid(cause: novarocks_functions::legacy_arithmetic::ArithmeticValueFailure) {
///     let _ = novarocks_functions::KernelFailure::from_domain(cause);
/// }
/// ```
pub struct ArithmeticValueFailure {
    source: ArithmeticFailure,
    reason: crate::DataFailureReason,
}
#[derive(Debug)]
pub struct ArithmeticOuterFailure(ArithmeticFailure);
impl ArithmeticFailure {
    pub fn classify(self) -> ClassifiedArithmeticFailure {
        match self.data_reason() {
            Some(reason) => ClassifiedArithmeticFailure::Value(ArithmeticValueFailure {
                source: self,
                reason,
            }),
            None => ClassifiedArithmeticFailure::Outer(ArithmeticOuterFailure(self)),
        }
    }
}
impl ArithmeticValueFailure {
    pub fn reason(&self) -> crate::DataFailureReason {
        self.reason
    }
}
impl std::fmt::Display for ArithmeticValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for ArithmeticValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ArithmeticValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason)
    }
}
impl std::fmt::Display for ArithmeticOuterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ArithmeticOuterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ArithmeticOuterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.0)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ArithmeticOuterFailure {}

fn arithmetic_cast_failure(source: arrow_schema::ArrowError) -> ArithmeticFailure {
    ArithmeticFailure(Box::new(ArithmeticFailureCause::Arrow {
        operation: ArrowOperation::Cast,
        source,
    }))
}
fn arithmetic_compute_failure(source: arrow_schema::ArrowError) -> ArithmeticFailure {
    ArithmeticFailure(Box::new(ArithmeticFailureCause::Arrow {
        operation: ArrowOperation::Compute,
        source,
    }))
}
fn arithmetic_construct_failure(source: arrow_schema::ArrowError) -> ArithmeticFailure {
    ArithmeticFailure(Box::new(ArithmeticFailureCause::Arrow {
        operation: ArrowOperation::Construct,
        source,
    }))
}
fn arithmetic_largeint_view<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<&'a arrow_array::FixedSizeBinaryArray, ArithmeticFailure> {
    let array = array
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .ok_or_else(|| {
            ArithmeticFailure::contract(format_args!("{context}: expected FixedSizeBinaryArray"))
        })?;
    if array.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(ArithmeticFailure::contract(format_args!(
            "{context}: expected 16-byte LARGEINT carrier, got {}",
            array.value_length()
        )));
    }
    Ok(array)
}
fn arithmetic_largeint_value(
    array: &arrow_array::FixedSizeBinaryArray,
    row: usize,
) -> Result<i128, ArithmeticFailure> {
    let bytes: [u8; 16] = array
        .value(row)
        .try_into()
        .map_err(|source| ArithmeticFailure(Box::new(ArithmeticFailureCause::Slice(source))))?;
    Ok(i128::from_be_bytes(bytes))
}
fn arithmetic_largeint_array(values: &[Option<i128>]) -> ArrayRef {
    match largeint::array_from_i128_observed(
        values,
        &mut |_| Ok::<(), std::convert::Infallible>(()),
    ) {
        Ok(array) => array,
        Err(never) => match never {},
    }
}

// Helper to cast array to Int64Array for arithmetic
fn cast_to_i64(arr: &ArrayRef) -> Result<&Int64Array, ArithmeticFailure> {
    arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        ArithmeticFailure::contract(format_args!(
            "expected Int64Array, got {:?}",
            arr.data_type()
        ))
    })
}

// Helper to cast array to Float64Array for arithmetic
fn cast_to_f64(arr: &ArrayRef) -> Result<&Float64Array, ArithmeticFailure> {
    arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        ArithmeticFailure::contract(format_args!(
            "expected Float64Array, got {:?}",
            arr.data_type()
        ))
    })
}

fn to_largeint_values(
    arr: &ArrayRef,
    context: &str,
) -> Result<Vec<Option<i128>>, ArithmeticFailure> {
    match arr.data_type() {
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let fixed = arithmetic_largeint_view(arr, context)?;
            let mut values = Vec::with_capacity(fixed.len());
            for row in 0..fixed.len() {
                if fixed.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(arithmetic_largeint_value(fixed, row)?));
                }
            }
            Ok(values)
        }
        DataType::Int64 => {
            let int_arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ArithmeticFailure::contract(format_args!(
                    "{context}: failed to downcast Int64Array"
                ))
            })?;
            let mut values = Vec::with_capacity(int_arr.len());
            for row in 0..int_arr.len() {
                if int_arr.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(int_arr.value(row) as i128));
                }
            }
            Ok(values)
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            let casted =
                arrow_cast::cast(arr, &DataType::Int64).map_err(arithmetic_cast_failure)?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ArithmeticFailure::contract(format_args!(
                        "{context}: failed to downcast Int64Array"
                    ))
                })?;
            let mut values = Vec::with_capacity(int_arr.len());
            for row in 0..int_arr.len() {
                if int_arr.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(int_arr.value(row) as i128));
                }
            }
            Ok(values)
        }
        DataType::Null => Ok(vec![None; arr.len()]),
        other => Err(ArithmeticFailure::contract(format_args!(
            "{context}: unsupported LARGEINT operand type: {:?}",
            other
        ))),
    }
}

enum LargeIntOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn eval_largeint_binop(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    output_type: &DataType,
    op: LargeIntOp,
) -> Result<Option<ArrayRef>, ArithmeticFailure> {
    if !largeint::is_largeint_data_type(output_type) {
        return Ok(None);
    }
    let context = match op {
        LargeIntOp::Add => "add",
        LargeIntOp::Sub => "sub",
        LargeIntOp::Mul => "mul",
        LargeIntOp::Div => "div",
        LargeIntOp::Mod => "mod",
    };
    let lhs_values = to_largeint_values(lhs, context)?;
    let rhs_values = to_largeint_values(rhs, context)?;
    if lhs_values.len() != rhs_values.len() {
        return Err(ArithmeticFailure::contract(format_args!(
            "largeint {context} length mismatch"
        )));
    }

    let mut values = Vec::with_capacity(lhs_values.len());
    for row in 0..lhs_values.len() {
        let out = match (lhs_values[row], rhs_values[row]) {
            (Some(l), Some(r)) => match op {
                LargeIntOp::Add => Some(l.wrapping_add(r)),
                LargeIntOp::Sub => Some(l.wrapping_sub(r)),
                LargeIntOp::Mul => Some(l.wrapping_mul(r)),
                LargeIntOp::Div => {
                    if r == 0 {
                        None
                    } else if l == i128::MIN && r == -1 {
                        Some(i128::MIN)
                    } else {
                        Some(l / r)
                    }
                }
                LargeIntOp::Mod => {
                    if r == 0 {
                        None
                    } else if l == i128::MIN && r == -1 {
                        Some(0)
                    } else {
                        Some(l % r)
                    }
                }
            },
            _ => None,
        };
        values.push(out);
    }
    Ok(Some(arithmetic_largeint_array(&values)))
}

#[derive(Clone, Copy)]
pub enum DecimalOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn to_decimal128_values(
    arr: &ArrayRef,
    context: &str,
) -> Result<(Vec<Option<i128>>, i32), ArithmeticFailure> {
    match arr.data_type() {
        DataType::Decimal128(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    ArithmeticFailure::contract(format_args!(
                        "{context}: failed to downcast Decimal128Array"
                    ))
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(typed.value(row)));
                }
            }
            Ok((out, *scale as i32))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = if matches!(arr.data_type(), DataType::Int64) {
                arr.clone()
            } else {
                arrow_cast::cast(arr, &DataType::Int64).map_err(arithmetic_cast_failure)?
            };
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ArithmeticFailure::contract(format_args!(
                        "{context}: failed to downcast Int64Array"
                    ))
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(typed.value(row) as i128));
                }
            }
            Ok((out, 0))
        }
        DataType::Null => Ok((vec![None; arr.len()], 0)),
        other => Err(ArithmeticFailure::contract(format_args!(
            "{context}: unsupported Decimal128 operand type: {:?}",
            other
        ))),
    }
}

fn to_decimal256_values(
    arr: &ArrayRef,
    context: &str,
) -> Result<(Vec<Option<i256>>, i32), ArithmeticFailure> {
    match arr.data_type() {
        DataType::Decimal256(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    ArithmeticFailure::contract(format_args!(
                        "{context}: failed to downcast Decimal256Array"
                    ))
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(typed.value(row)));
                }
            }
            Ok((out, *scale as i32))
        }
        DataType::Decimal128(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    ArithmeticFailure::contract(format_args!(
                        "{context}: failed to downcast Decimal128Array"
                    ))
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(i256::from_i128(typed.value(row))));
                }
            }
            Ok((out, *scale as i32))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = if matches!(arr.data_type(), DataType::Int64) {
                arr.clone()
            } else {
                arrow_cast::cast(arr, &DataType::Int64).map_err(arithmetic_cast_failure)?
            };
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ArithmeticFailure::contract(format_args!(
                        "{context}: failed to downcast Int64Array"
                    ))
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(i256::from_i128(typed.value(row) as i128)));
                }
            }
            Ok((out, 0))
        }
        ty if largeint::is_largeint_data_type(ty) => {
            let typed = arithmetic_largeint_view(arr, context)?;
            let values = (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        Ok(None)
                    } else {
                        arithmetic_largeint_value(typed, row)
                            .map(|value| Some(i256::from_i128(value)))
                    }
                })
                .collect::<Result<Vec<_>, ArithmeticFailure>>()?;
            Ok((values, 0))
        }
        DataType::Null => Ok((vec![None; arr.len()], 0)),
        other => Err(ArithmeticFailure::contract(format_args!(
            "{context}: unsupported Decimal256 operand type: {:?}",
            other
        ))),
    }
}

fn eval_decimal256_div_value(
    lhs_val: i256,
    rhs_val: i256,
    lhs_scale_i32: i32,
    rhs_scale_i32: i32,
    out_scale_i32: i32,
) -> Result<Option<i256>, ArithmeticFailure> {
    if rhs_val == i256::ZERO {
        return Ok(None);
    }
    let exponent = out_scale_i32 + rhs_scale_i32 - lhs_scale_i32;
    let numerator = if exponent >= 0 {
        let factor = match pow10_i256(exponent as usize) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        match lhs_val.checked_mul(factor) {
            Some(v) => v,
            None => return Ok(None),
        }
    } else {
        let factor = match pow10_i256((-exponent) as usize) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        lhs_val.checked_div(factor).ok_or_else(|| {
            ArithmeticFailure::from(crate::legacy_decimal::DecimalMathFailure::Overflow)
        })?
    };
    Ok(Some(div_round_i256(numerator, rhs_val)?))
}

fn checked_decimal_divide_half_up(numerator: i128, denominator: i128) -> Option<i128> {
    let quotient = numerator.checked_div(denominator)?;
    let remainder = numerator.checked_rem(denominator)?;
    let magnitude = denominator.unsigned_abs();
    let threshold = (magnitude >> 1) + (magnitude & 1);
    if remainder.unsigned_abs() >= threshold {
        quotient.checked_add(if (numerator < 0) ^ (denominator < 0) {
            -1
        } else {
            1
        })
    } else {
        Some(quotient)
    }
}

pub fn decimal_overflow_error(op: DecimalOp) -> ArithmeticFailure {
    let name = match op {
        DecimalOp::Add => "add",
        DecimalOp::Sub => "sub",
        DecimalOp::Mul => "mul",
        DecimalOp::Div => "div",
        DecimalOp::Mod => "mod",
    };
    ArithmeticFailure(Box::new(ArithmeticFailureCause::Numeric(name)))
}

pub fn eval_decimal_binop(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    output_type: &DataType,
    op: DecimalOp,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<Option<ArrayRef>, ArithmeticFailure> {
    let is_decimal =
        |ty: &DataType| matches!(ty, DataType::Decimal128(_, _) | DataType::Decimal256(_, _));
    if (largeint::is_largeint_data_type(lhs.data_type()) && is_decimal(rhs.data_type()))
        || (largeint::is_largeint_data_type(rhs.data_type()) && is_decimal(lhs.data_type()))
    {
        let operation = match &op {
            DecimalOp::Add => novarocks_type_contract::ArithmeticOperator::Add,
            DecimalOp::Sub => novarocks_type_contract::ArithmeticOperator::Subtract,
            DecimalOp::Mul => novarocks_type_contract::ArithmeticOperator::Multiply,
            DecimalOp::Div => novarocks_type_contract::ArithmeticOperator::Divide,
            DecimalOp::Mod => novarocks_type_contract::ArithmeticOperator::Modulo,
        };
        if novarocks_type_contract::arithmetic_result_type_with_op(
            lhs.data_type(),
            rhs.data_type(),
            operation,
        )
        .as_ref()
            != Some(output_type)
        {
            return Err(ArithmeticFailure::contract(
                "Decimal/LARGEINT arithmetic differs from its frozen add/subtract rule",
            ));
        }
    }
    match output_type {
        DataType::Decimal128(out_precision, out_scale) => {
            if !matches!(
                lhs.data_type(),
                DataType::Decimal128(_, _)
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Null
            ) || !matches!(
                rhs.data_type(),
                DataType::Decimal128(_, _)
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Null
            ) {
                return Ok(None);
            }
            let (lhs_values, lhs_scale_i32) = to_decimal128_values(lhs, "decimal arithmetic lhs")?;
            let (rhs_values, rhs_scale_i32) = to_decimal128_values(rhs, "decimal arithmetic rhs")?;
            if lhs_values.len() != rhs_values.len() {
                return Err(ArithmeticFailure::contract(
                    "decimal arithmetic length mismatch",
                ));
            }
            let mut values = Vec::with_capacity(lhs_values.len());
            let ls = lhs_scale_i32;
            let rs = rhs_scale_i32;
            let os = i32::from(*out_scale);
            if matches!(op, DecimalOp::Add | DecimalOp::Sub | DecimalOp::Mod)
                && (os < ls || os < rs)
            {
                return Err(ArithmeticFailure::contract(
                    "frozen decimal add/sub/mod scale mismatch",
                ));
            }
            let precision_limit = 10_u128
                .checked_pow(u32::from(*out_precision))
                .filter(|_| (1..=38).contains(out_precision))
                .ok_or_else(|| {
                    ArithmeticFailure::contract("invalid frozen Decimal128 precision")
                })?;
            // All metadata-derived factors are computed once per batch.
            let factor = |exponent: i32| pow10_i128(exponent.unsigned_abs() as usize).ok();
            let left_factor = factor(os - ls);
            let right_factor = factor(os - rs);
            let product_diff = os - ls - rs;
            let product_factor = factor(product_diff);
            let division_diff = os + rs - ls;
            let division_factor = factor(division_diff);
            for row in 0..lhs_values.len() {
                let (Some(left), Some(right)) = (lhs_values[row], rhs_values[row]) else {
                    values.push(None);
                    continue;
                };
                if matches!(op, DecimalOp::Div | DecimalOp::Mod) && right == 0 {
                    values.push(None);
                    continue;
                }
                let checked = match op {
                    DecimalOp::Add | DecimalOp::Sub | DecimalOp::Mod => left_factor
                        .and_then(|factor| left.checked_mul(factor))
                        .zip(right_factor.and_then(|factor| right.checked_mul(factor)))
                        .and_then(|(left, right)| match op {
                            DecimalOp::Add => left.checked_add(right),
                            DecimalOp::Sub => left.checked_sub(right),
                            DecimalOp::Mod => left.checked_rem(right),
                            _ => unreachable!(),
                        }),
                    DecimalOp::Mul => left.checked_mul(right).and_then(|product| {
                        product_factor.and_then(|factor| {
                            if product_diff >= 0 {
                                product.checked_mul(factor)
                            } else {
                                product.checked_div(factor)
                            }
                        })
                    }),
                    DecimalOp::Div => division_factor
                        .and_then(|factor| {
                            if division_diff >= 0 {
                                left.checked_mul(factor)
                            } else {
                                left.checked_div(factor)
                            }
                        })
                        .and_then(|numerator| checked_decimal_divide_half_up(numerator, right)),
                }
                .filter(|value| value.unsigned_abs() < precision_limit);
                match checked {
                    Some(value) => values.push(Some(value)),
                    None if decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                        || (strict_overflow && matches!(op, DecimalOp::Mul)) =>
                    {
                        return Err(decimal_overflow_error(op));
                    }
                    None => values.push(None),
                }
            }
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(arithmetic_construct_failure)?;
            Ok(Some(Arc::new(array)))
        }
        DataType::Decimal256(out_precision, out_scale) => {
            let (lhs_values, lhs_scale_i32) = to_decimal256_values(lhs, "decimal arithmetic lhs")?;
            let (rhs_values, rhs_scale_i32) = to_decimal256_values(rhs, "decimal arithmetic rhs")?;
            if lhs_values.len() != rhs_values.len() {
                return Err(ArithmeticFailure::contract(
                    "decimal arithmetic length mismatch",
                ));
            }
            let out_scale_i32 = *out_scale as i32;
            let mut values: Vec<Option<i256>> = Vec::with_capacity(lhs_values.len());
            let mut had_mul_overflow = false;
            let mut had_numeric_overflow = false;
            let precision_limit = pow10_i256(*out_precision as usize)?;
            for row in 0..lhs_values.len() {
                let (Some(lhs_val), Some(rhs_val)) = (lhs_values[row], rhs_values[row]) else {
                    values.push(None);
                    continue;
                };
                let out_val = match op {
                    DecimalOp::Add | DecimalOp::Sub => {
                        if out_scale_i32 < lhs_scale_i32 || out_scale_i32 < rhs_scale_i32 {
                            return Err(ArithmeticFailure::contract(
                                "decimal add/sub scale mismatch",
                            ));
                        }
                        let lhs_factor = match pow10_i256((out_scale_i32 - lhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i256((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        let out = if matches!(op, DecimalOp::Add) {
                            lhs_scaled.checked_add(rhs_scaled)
                        } else {
                            lhs_scaled.checked_sub(rhs_scaled)
                        };
                        let Some(out) = out else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        out
                    }
                    DecimalOp::Mul => {
                        let scale_in = lhs_scale_i32 + rhs_scale_i32;
                        let diff = out_scale_i32 - scale_in;
                        let Some(product) = lhs_val.checked_mul(rhs_val) else {
                            had_mul_overflow = true;
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        if diff >= 0 {
                            let factor = match pow10_i256(diff as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    had_numeric_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            let Some(out) = product.checked_mul(factor) else {
                                had_mul_overflow = true;
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            };
                            out
                        } else {
                            let factor = match pow10_i256((-diff) as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    had_numeric_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            match product.checked_div(factor) {
                                Some(v) => v,
                                None => {
                                    had_mul_overflow = true;
                                    had_numeric_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            }
                        }
                    }
                    DecimalOp::Div => {
                        let Some(divided) = eval_decimal256_div_value(
                            lhs_val,
                            rhs_val,
                            lhs_scale_i32,
                            rhs_scale_i32,
                            out_scale_i32,
                        )?
                        else {
                            had_numeric_overflow |= rhs_val != i256::ZERO;
                            values.push(None);
                            continue;
                        };
                        divided
                    }
                    DecimalOp::Mod => {
                        if rhs_val == i256::ZERO {
                            values.push(None);
                            continue;
                        }
                        if out_scale_i32 < lhs_scale_i32 || out_scale_i32 < rhs_scale_i32 {
                            return Err(ArithmeticFailure::contract("decimal mod scale mismatch"));
                        }
                        let lhs_factor = match pow10_i256((out_scale_i32 - lhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i256((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        match lhs_scaled.checked_rem(rhs_scaled) {
                            Some(v) => v,
                            None => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        }
                    }
                };
                // Declared precision and carrier capacity both bound the result;
                // the frozen policy determines how numeric overflow is returned.
                if out_val <= -precision_limit || out_val >= precision_limit {
                    had_numeric_overflow = true;
                    had_mul_overflow |= matches!(op, DecimalOp::Mul);
                    values.push(None);
                    continue;
                }
                values.push(Some(out_val));
            }
            if (decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                && had_numeric_overflow)
                || (strict_overflow && matches!(op, DecimalOp::Mul) && had_mul_overflow)
            {
                return Err(decimal_overflow_error(op));
            }
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(arithmetic_construct_failure)?;
            Ok(Some(Arc::new(array)))
        }
        _ => Ok(None),
    }
}

// Generic Arrow arithmetic operation with type coercion
fn eval_numeric_binop_arrays<F1, F2>(
    lhs: ArrayRef,
    rhs: ArrayRef,
    int_op: F1,
    float_op: F2,
) -> Result<ArrayRef, ArithmeticFailure>
where
    F1: FnOnce(
        &Int64Array,
        &Int64Array,
    ) -> Result<Arc<dyn arrow_array::Array>, arrow_schema::ArrowError>,
    F2: FnOnce(
        &Float64Array,
        &Float64Array,
    ) -> Result<Arc<dyn arrow_array::Array>, arrow_schema::ArrowError>,
{
    use arrow_cast::cast;

    let is_float = |dt: &DataType| matches!(dt, DataType::Float32 | DataType::Float64);
    let is_lhs_float = is_float(lhs.data_type());
    let is_rhs_float = is_float(rhs.data_type());

    if is_lhs_float || is_rhs_float {
        let lhs_f64_arr = if matches!(lhs.data_type(), DataType::Float64) {
            lhs
        } else {
            cast(&lhs, &DataType::Float64).map_err(arithmetic_cast_failure)?
        };
        let rhs_f64_arr = if matches!(rhs.data_type(), DataType::Float64) {
            rhs
        } else {
            cast(&rhs, &DataType::Float64).map_err(arithmetic_cast_failure)?
        };
        let lhs_f64 = cast_to_f64(&lhs_f64_arr)?;
        let rhs_f64 = cast_to_f64(&rhs_f64_arr)?;
        float_op(lhs_f64, rhs_f64)
            .map_err(arithmetic_compute_failure)
            .map(|arc| arc as ArrayRef)
    } else {
        let lhs_i64_arr = if matches!(lhs.data_type(), DataType::Int64) {
            lhs
        } else {
            cast(&lhs, &DataType::Int64).map_err(arithmetic_cast_failure)?
        };
        let rhs_i64_arr = if matches!(rhs.data_type(), DataType::Int64) {
            rhs
        } else {
            cast(&rhs, &DataType::Int64).map_err(arithmetic_cast_failure)?
        };
        let lhs_i64 = cast_to_i64(&lhs_i64_arr)?;
        let rhs_i64 = cast_to_i64(&rhs_i64_arr)?;
        int_op(lhs_i64, rhs_i64)
            .map_err(arithmetic_compute_failure)
            .map(|arc| arc as ArrayRef)
    }
}

fn cast_numeric_output(
    result: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, ArithmeticFailure> {
    use arrow_cast::cast;
    if matches!(output_type, DataType::Null) || result.data_type() == output_type {
        return Ok(result);
    }
    match output_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => cast(&result, output_type).map_err(arithmetic_cast_failure),
        other => Err(ArithmeticFailure::contract(format_args!(
            "arithmetic output type mismatch: {:?}",
            other
        ))),
    }
}

pub fn eval_add_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, ArithmeticFailure> {
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Add)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Add,
        strict_overflow,
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(lhs, rhs, |x, y| add(x, y), |x, y| add(x, y))?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_sub_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, ArithmeticFailure> {
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Sub)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Sub,
        strict_overflow,
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(lhs, rhs, |x, y| sub(x, y), |x, y| sub(x, y))?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_mul_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, ArithmeticFailure> {
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Mul)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Mul,
        strict_overflow,
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(lhs, rhs, |x, y| mul(x, y), |x, y| mul(x, y))?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_div_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: &DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, ArithmeticFailure> {
    // Replace zeros in the divisor with NULLs so that division by zero
    // returns NULL instead of an error (matches StarRocks behavior).
    let rhs = nullify_zeros(&rhs);
    let output_type = output_type.clone();
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Div)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Div,
        strict_overflow,
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    // StarRocks: integer / integer → DOUBLE. Cast integer inputs to Float64
    // BEFORE dividing so that the result preserves fractional parts.
    let both_integral = is_integer_type(lhs.data_type()) && is_integer_type(rhs.data_type());
    if both_integral && matches!(output_type, DataType::Float64) {
        let lhs_f = arrow_cast::cast(&lhs, &DataType::Float64).map_err(arithmetic_cast_failure)?;
        let rhs_f = arrow_cast::cast(&rhs, &DataType::Float64).map_err(arithmetic_cast_failure)?;
        let result = eval_numeric_binop_arrays(lhs_f, rhs_f, |x, y| div(x, y), |x, y| div(x, y))?;
        return Ok(result);
    }
    let result = eval_numeric_binop_arrays(lhs, rhs, |x, y| div(x, y), |x, y| div(x, y))?;
    cast_numeric_output(result, &output_type)
}

fn is_integer_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Sole original division zero-mask carrier classification. The per-width
/// readers remain in nullify_zeros; F32/Decimal128 omissions are intentional.
pub(crate) fn division_masks_zeros(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float64
    )
}

/// Replace zero values in a numeric array with NULLs for safe division.
fn nullify_zeros(arr: &ArrayRef) -> ArrayRef {
    use arrow_array::BooleanArray;
    let len = arr.len();
    let mut is_zero_buf = vec![false; len];
    // Keep the original vector allocation before unsupported-type return.
    // The shared classification owns admission; the match below owns readers.
    if !division_masks_zeros(arr.data_type()) {
        return arr.clone();
    }
    match arr.data_type() {
        DataType::Int8 => {
            if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int8Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Int16 => {
            if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int16Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Int32 => {
            if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int32Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Int64 => {
            if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0.0 {
                        *is_zero = true;
                    }
                }
            }
        }
        _ => return arr.clone(),
    }
    let mask = BooleanArray::from(is_zero_buf);
    arrow_select::nullif::nullif(arr, &mask).unwrap_or_else(|_| arr.clone())
}

pub fn eval_mod_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, ArithmeticFailure> {
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Mod)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Mod,
        strict_overflow,
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(lhs, rhs, |x, y| rem(x, y), |x, y| rem(x, y))?;
    cast_numeric_output(result, &output_type)
}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn arithmetic_failure_preserves_real_arrow_and_operation_role() {
        let memory = arithmetic_compute_failure(arrow_schema::ArrowError::MemoryError(
            "decimal overflow cancelled".into(),
        ));
        assert_eq!(memory.failure_fact().class(), FailureClass::Resource);
        assert_eq!(memory.data_reason(), None);
        assert!(memory.source().unwrap().is::<arrow_schema::ArrowError>());
        let numeric = arithmetic_compute_failure(arrow_schema::ArrowError::ArithmeticOverflow(
            "actual numeric result".into(),
        ));
        assert_eq!(
            numeric.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        let metadata =
            arithmetic_construct_failure(arrow_schema::ArrowError::InvalidArgumentError(
                "numeric overflow text is not a numeric cause".into(),
            ));
        assert_eq!(metadata.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(metadata.data_reason(), None);
    }
}
