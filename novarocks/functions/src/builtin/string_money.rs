// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original MONEY_FORMAT rounding, grouping and floating negative-zero behavior.
use super::string_extended::Row;
use crate::{KernelFailure, kernel_control::internal, kernel_input::EvaluationCheckpoints};
use arrow_array::{
    Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array,
};
use arrow_schema::DataType;
pub(super) fn visit(
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8) -> Result<(), KernelFailure>,
) -> Result<Row, KernelFailure> {
    work.step()?;
    let (cents, omit, negative_zero) = match array.data_type() {
        DataType::Int8 => (
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| internal("money input is not Int8"))?
                .value(row) as i128
                * 100,
            false,
            false,
        ),
        DataType::Int16 => (
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| internal("money input is not Int16"))?
                .value(row) as i128
                * 100,
            false,
            false,
        ),
        DataType::Int32 => (
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| internal("money input is not Int32"))?
                .value(row) as i128
                * 100,
            false,
            false,
        ),
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| internal("money input is not Float32"))?
                .value(row) as f64;
            match round_half_away_from_zero(value * 100.0) {
                Ok(cents) => (cents, true, value.is_sign_negative() && cents == 0),
                Err(error) => return Ok(Row::Error(error)),
            }
        }
        DataType::Int64 => (
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| internal("money input is not Int64"))?
                .value(row) as i128
                * 100,
            false,
            false,
        ),
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| internal("money input is not Float64"))?
                .value(row);
            match round_half_away_from_zero(value * 100.0) {
                Ok(cents) => (cents, true, value.is_sign_negative() && cents == 0),
                Err(error) => return Ok(Row::Error(error)),
            }
        }
        DataType::Decimal128(_, scale) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| internal("money input is not Decimal128"))?
                .value(row);
            match decimal_to_cents(value, i32::from(*scale)) {
                Ok(cents) => (cents, false, false),
                Err(error) => return Ok(Row::Error(error)),
            }
        }
        _ => return Err(internal("money input differs from selected profile")),
    };
    // i128 currency formatting has fewer than 64 bytes. Its library formatting
    // is bounded independently of input length, and surrounded by checkpoints.
    work.flush()?;
    let text = format_currency(cents, omit, negative_zero);
    work.flush()?;
    for byte in text.bytes() {
        emit(byte)?;
        work.step()?;
    }
    Ok(Row::Value)
}
fn round_half_away_from_zero(value: f64) -> Result<i128, &'static str> {
    if !value.is_finite() {
        return Err("money_format input must be finite");
    }
    let rounded = if value >= 0.0 {
        (value + 0.5).floor()
    } else {
        (value - 0.5).ceil()
    };
    if rounded < i128::MIN as f64 || rounded > i128::MAX as f64 {
        return Err("money_format overflow");
    }
    Ok(rounded as i128)
}

fn pow10_i128(exp: u32) -> Result<i128, &'static str> {
    10_i128
        .checked_pow(exp)
        .ok_or_else(|| "money_format overflow")
}

fn decimal_to_cents(value: i128, scale: i32) -> Result<i128, &'static str> {
    if scale <= 2 {
        let factor = pow10_i128((2 - scale) as u32)?;
        return value
            .checked_mul(factor)
            .ok_or_else(|| "money_format overflow");
    }

    let divisor = pow10_i128((scale - 2) as u32)?;
    let mut cents = value / divisor;
    let remainder = value.unsigned_abs() % (divisor as u128);
    if remainder.saturating_mul(2) >= divisor as u128 {
        cents = cents
            .checked_add(if value >= 0 { 1 } else { -1 })
            .ok_or_else(|| "money_format overflow")?;
    }
    Ok(cents)
}

fn format_currency(cents: i128, omit_leading_zero: bool, preserve_negative_zero: bool) -> String {
    let sign = if cents < 0 || (preserve_negative_zero && cents == 0) {
        "-"
    } else {
        ""
    };
    let abs_cents = cents.unsigned_abs();
    let integer_part = abs_cents / 100;
    let fractional_part = abs_cents % 100;
    let integer = if omit_leading_zero && integer_part == 0 {
        String::new()
    } else {
        format_grouped_integer(integer_part)
    };
    if integer.is_empty() {
        format!("{sign}.{:02}", fractional_part)
    } else {
        format!("{sign}{integer}.{:02}", fractional_part)
    }
}

fn format_grouped_integer(v: u128) -> String {
    let digits = v.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (digits.len() - idx).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_money_original_rounding_and_negative_zero() {
        assert_eq!(decimal_to_cents(5, 3), Ok(1));
        assert_eq!(decimal_to_cents(-5, 3), Ok(-1));
        assert_eq!(decimal_to_cents(i128::MAX, 0), Err("money_format overflow"));
        assert_eq!(format_currency(-123456789, false, false), "-1,234,567.89");
        assert_eq!(format_currency(0, true, true), "-.00");
        assert_eq!(format_currency(0, false, false), "0.00");
        assert_eq!(
            round_half_away_from_zero(f64::INFINITY),
            Err("money_format input must be finite")
        );
        assert_eq!(round_half_away_from_zero(-0.5), Ok(-1));
    }
}
