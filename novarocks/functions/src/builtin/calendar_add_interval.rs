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

//! Original interval conversions retained for legacy date-shift shells.
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;

fn float_to_i64(v: f64) -> Option<i64> {
    if !v.is_finite() || v < i64::MIN as f64 || v > i64::MAX as f64 {
        return None;
    }
    Some(v.trunc() as i64)
}

fn pow10_i128(scale: u32) -> Option<i128> {
    let mut out: i128 = 1;
    for _ in 0..scale {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

fn decimal128_to_i64(value: i128, scale: i8) -> Option<i64> {
    let integral = if scale >= 0 {
        let divisor = pow10_i128(scale as u32)?;
        value / divisor
    } else {
        let factor = pow10_i128((-scale) as u32)?;
        value.checked_mul(factor)?
    };
    i64::try_from(integral).ok()
}

fn parse_i64_from_utf8(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = trimmed.parse::<i64>() {
        return Some(v);
    }
    let float_v = trimmed.parse::<f64>().ok()?;
    float_to_i64(float_v)
}

pub fn legacy_extract_calendar_intervals(
    array: &ArrayRef,
    func_name: &str,
) -> Result<Vec<Option<i64>>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect())
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "failed to downcast to UInt8Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "failed to downcast to UInt16Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast to UInt32Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast to UInt64Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        i64::try_from(arr.value(i)).ok()
                    }
                })
                .collect())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        float_to_i64(arr.value(i) as f64)
                    }
                })
                .collect())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        float_to_i64(arr.value(i))
                    }
                })
                .collect())
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        decimal128_to_i64(arr.value(i), *scale)
                    }
                })
                .collect())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        parse_i64_from_utf8(arr.value(i))
                    }
                })
                .collect())
        }
        DataType::Null => Ok(vec![None; array.len()]),
        _ => Err(format!("{func_name} expects int")),
    }
}

#[cfg(test)]
mod legacy_goldens {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn calendar_interval_conversion_keeps_original_trim_float_and_range_behavior() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some(" 12 "),
            Some(" -12.9 "),
            Some("NaN"),
            Some("9223372036854775808"),
            Some(""),
            None,
        ]));
        assert_eq!(
            legacy_extract_calendar_intervals(&values, "date_add").unwrap(),
            vec![Some(12), Some(-12), None, Some(i64::MAX), None, None]
        );
        let values: ArrayRef = Arc::new(UInt64Array::from(vec![0, i64::MAX as u64, u64::MAX]));
        assert_eq!(
            legacy_extract_calendar_intervals(&values, "date_add").unwrap(),
            vec![Some(0), Some(i64::MAX), None]
        );
    }
}
