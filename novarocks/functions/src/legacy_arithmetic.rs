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
// Helper to cast array to Int64Array for arithmetic
fn cast_to_i64(arr: &ArrayRef) -> Result<&Int64Array, String> {
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("expected Int64Array, got {:?}", arr.data_type()))
}

// Helper to cast array to Float64Array for arithmetic
fn cast_to_f64(arr: &ArrayRef) -> Result<&Float64Array, String> {
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| format!("expected Float64Array, got {:?}", arr.data_type()))
}

fn to_largeint_values(arr: &ArrayRef, context: &str) -> Result<Vec<Option<i128>>, String> {
    match arr.data_type() {
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let fixed = largeint::as_fixed_size_binary_array(arr, context)?;
            let mut values = Vec::with_capacity(fixed.len());
            for row in 0..fixed.len() {
                if fixed.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(largeint::value_at(fixed, row)?));
                }
            }
            Ok(values)
        }
        DataType::Int64 => {
            let int_arr = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
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
            let casted = arrow_cast::cast(arr, &DataType::Int64)
                .map_err(|e| format!("{context}: failed to cast operand to Int64: {e}"))?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
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
        other => Err(format!(
            "{context}: unsupported LARGEINT operand type: {:?}",
            other
        )),
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
) -> Result<Option<ArrayRef>, String> {
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
        return Err(format!("largeint {context} length mismatch"));
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
    largeint::array_from_i128(&values).map(Some)
}

#[derive(Clone, Copy)]
pub enum DecimalOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn to_decimal128_values(arr: &ArrayRef, context: &str) -> Result<(Vec<Option<i128>>, i32), String> {
    match arr.data_type() {
        DataType::Decimal128(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
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
                arrow_cast::cast(arr, &DataType::Int64).map_err(|e| {
                    format!("{context}: failed to cast integer operand to Int64: {e}")
                })?
            };
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
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
        other => Err(format!(
            "{context}: unsupported Decimal128 operand type: {:?}",
            other
        )),
    }
}

fn to_decimal256_values(arr: &ArrayRef, context: &str) -> Result<(Vec<Option<i256>>, i32), String> {
    match arr.data_type() {
        DataType::Decimal256(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal256Array"))?;
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
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
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
                arrow_cast::cast(arr, &DataType::Int64).map_err(|e| {
                    format!("{context}: failed to cast integer operand to Int64: {e}")
                })?
            };
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
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
            let typed = largeint::as_fixed_size_binary_array(arr, context)?;
            let values = (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        Ok(None)
                    } else {
                        largeint::value_at(typed, row).map(|value| Some(i256::from_i128(value)))
                    }
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok((values, 0))
        }
        DataType::Null => Ok((vec![None; arr.len()], 0)),
        other => Err(format!(
            "{context}: unsupported Decimal256 operand type: {:?}",
            other
        )),
    }
}

fn eval_decimal256_div_value(
    lhs_val: i256,
    rhs_val: i256,
    lhs_scale_i32: i32,
    rhs_scale_i32: i32,
    out_scale_i32: i32,
) -> Result<Option<i256>, String> {
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
        lhs_val
            .checked_div(factor)
            .ok_or_else(|| "decimal overflow".to_string())?
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

pub fn decimal_overflow_error(op: DecimalOp) -> String {
    let name = match op {
        DecimalOp::Add => "add",
        DecimalOp::Sub => "sub",
        DecimalOp::Mul => "mul",
        DecimalOp::Div => "div",
        DecimalOp::Mod => "mod",
    };
    format!("Expr evaluate meet error: The '{name}' operation involving decimal values overflows")
}

pub fn eval_decimal_binop(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    output_type: &DataType,
    op: DecimalOp,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<Option<ArrayRef>, String> {
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
            return Err(
                "Decimal/LARGEINT arithmetic differs from its frozen add/subtract rule".to_string(),
            );
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
                return Err("decimal arithmetic length mismatch".to_string());
            }
            let mut values = Vec::with_capacity(lhs_values.len());
            let ls = lhs_scale_i32;
            let rs = rhs_scale_i32;
            let os = i32::from(*out_scale);
            if matches!(op, DecimalOp::Add | DecimalOp::Sub | DecimalOp::Mod)
                && (os < ls || os < rs)
            {
                return Err("frozen decimal add/sub/mod scale mismatch".to_string());
            }
            let precision_limit = 10_u128
                .checked_pow(u32::from(*out_precision))
                .filter(|_| (1..=38).contains(out_precision))
                .ok_or_else(|| "invalid frozen Decimal128 precision".to_string())?;
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
                .map_err(|e| e.to_string())?;
            Ok(Some(Arc::new(array)))
        }
        DataType::Decimal256(out_precision, out_scale) => {
            let (lhs_values, lhs_scale_i32) = to_decimal256_values(lhs, "decimal arithmetic lhs")?;
            let (rhs_values, rhs_scale_i32) = to_decimal256_values(rhs, "decimal arithmetic rhs")?;
            if lhs_values.len() != rhs_values.len() {
                return Err("decimal arithmetic length mismatch".to_string());
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
                            return Err("decimal add/sub scale mismatch".to_string());
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
                            return Err("decimal mod scale mismatch".to_string());
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
                .map_err(|e| e.to_string())?;
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
) -> Result<ArrayRef, String>
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
            cast(&lhs, &DataType::Float64).map_err(|e| e.to_string())?
        };
        let rhs_f64_arr = if matches!(rhs.data_type(), DataType::Float64) {
            rhs
        } else {
            cast(&rhs, &DataType::Float64).map_err(|e| e.to_string())?
        };
        let lhs_f64 = cast_to_f64(&lhs_f64_arr)?;
        let rhs_f64 = cast_to_f64(&rhs_f64_arr)?;
        float_op(lhs_f64, rhs_f64)
            .map_err(|e| e.to_string())
            .map(|arc| arc as ArrayRef)
    } else {
        let lhs_i64_arr = if matches!(lhs.data_type(), DataType::Int64) {
            lhs
        } else {
            cast(&lhs, &DataType::Int64).map_err(|e| e.to_string())?
        };
        let rhs_i64_arr = if matches!(rhs.data_type(), DataType::Int64) {
            rhs
        } else {
            cast(&rhs, &DataType::Int64).map_err(|e| e.to_string())?
        };
        let lhs_i64 = cast_to_i64(&lhs_i64_arr)?;
        let rhs_i64 = cast_to_i64(&rhs_i64_arr)?;
        int_op(lhs_i64, rhs_i64)
            .map_err(|e| e.to_string())
            .map(|arc| arc as ArrayRef)
    }
}

fn cast_numeric_output(result: ArrayRef, output_type: &DataType) -> Result<ArrayRef, String> {
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
        | DataType::Float64 => cast(&result, output_type).map_err(|e| e.to_string()),
        other => Err(format!("arithmetic output type mismatch: {:?}", other)),
    }
}

pub fn eval_add_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, String> {
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
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = add(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = add(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_sub_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, String> {
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
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = sub(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = sub(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_mul_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, String> {
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
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = mul(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = mul(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_div_arrays(
    lhs: ArrayRef,
    rhs: ArrayRef,
    output_type: &DataType,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<ArrayRef, String> {
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
        let lhs_f =
            arrow_cast::cast(&lhs, &DataType::Float64).map_err(|e| format!("div cast lhs: {e}"))?;
        let rhs_f =
            arrow_cast::cast(&rhs, &DataType::Float64).map_err(|e| format!("div cast rhs: {e}"))?;
        let result = eval_numeric_binop_arrays(
            lhs_f,
            rhs_f,
            |x, y| {
                let result = div(x, y)?;
                Ok(Arc::new(result))
            },
            |x, y| {
                let result = div(x, y)?;
                Ok(Arc::new(result))
            },
        )?;
        return Ok(result);
    }
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = div(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = div(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
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
) -> Result<ArrayRef, String> {
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
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = rem(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = rem(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}
