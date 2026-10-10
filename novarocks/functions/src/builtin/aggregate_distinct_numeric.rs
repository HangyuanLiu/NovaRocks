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

//! Numeric distinct results retain the original arithmetic and decimal rules.
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelDiagnostic, KernelFailure};
use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::DataType;
use std::sync::Arc;
/// Computation/control errors stay typed; legacy callers retain full text.
#[derive(Debug)]
pub enum DistinctComputationError {
    Kernel(KernelFailure),
    Operational(String),
}
impl From<String> for DistinctComputationError {
    fn from(s: String) -> Self {
        Self::Operational(s)
    }
}
impl From<KernelFailure> for DistinctComputationError {
    fn from(e: KernelFailure) -> Self {
        Self::Kernel(e)
    }
}
impl DistinctComputationError {
    pub fn into_legacy_message(self) -> String {
        match self {
            Self::Operational(s) => s,
            Self::Kernel(
                KernelFailure::Internal(d)
                | KernelFailure::InvalidProgram(d)
                | KernelFailure::Operational(d),
            ) => d.message().to_owned(),
            Self::Kernel(e) => e.to_string(),
        }
    }
    pub fn into_kernel_failure(self) -> KernelFailure {
        match self {
            Self::Kernel(e) => e,
            Self::Operational(s) => KernelFailure::Operational(KernelDiagnostic::new(&s)),
        }
    }
}

/// Storage supplies its existing iteration order. This is part of legacy
/// floating accumulation and must not be replaced by insertion order or sorting.
pub trait NumericDistinctSet {
    fn len(&self) -> usize;
    fn keys(&self) -> impl Iterator<Item = &[u8]>;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// The caller owns actual allocation and its failure text. The pure codec only
/// requests the exact admitted extent and appends its frozen wire bytes.
pub trait NumericDistinctBuffer {
    fn reserve_exact(&mut self, size: usize) -> Result<(), String>;
    fn reserve_exact_typed(&mut self, size: usize) -> Result<(), DistinctComputationError> {
        self.reserve_exact(size)
            .map_err(DistinctComputationError::from)
    }
    fn append(&mut self, bytes: &[u8]);
}

pub fn sum_from_set(
    set: &impl NumericDistinctSet,
    input_type: &DataType,
    output_type: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, DistinctComputationError> {
    if set.is_empty() {
        // Return null
        return match output_type {
            DataType::Int64 => Ok(std::sync::Arc::new(Int64Array::from(vec![None]))),
            DataType::Float64 => Ok(std::sync::Arc::new(Float64Array::from(vec![None]))),
            DataType::Decimal128(precision, scale) => {
                let array = Decimal128Array::from(vec![None])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(std::sync::Arc::new(array))
            }
            DataType::Decimal256(precision, scale) => {
                let array = Decimal256Array::from(vec![None])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(std::sync::Arc::new(array))
            }
            other => {
                Err((format!("multi_distinct_sum output type unsupported: {:?}", other)).into())
            }
        };
    }

    match output_type {
        DataType::Int64 => {
            let mut sum: i128 = 0;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Int8 => i8::from_le_bytes(v[..1].try_into().unwrap()) as i128,
                    DataType::Int16 => i16::from_le_bytes(v[..2].try_into().unwrap()) as i128,
                    DataType::Int32 => i32::from_le_bytes(v[..4].try_into().unwrap()) as i128,
                    DataType::Int64 => i64::from_le_bytes(v[..8].try_into().unwrap()) as i128,
                    DataType::Boolean => i8::from_le_bytes(v[..1].try_into().unwrap()) as i128,
                    other => {
                        return Err((format!(
                            "multi_distinct_sum unsupported input type for int output: {:?}",
                            other
                        ))
                        .into());
                    }
                };
                sum += value;
            }
            let sum_i64 =
                i64::try_from(sum).map_err(|_| "multi_distinct_sum overflow".to_string())?;
            Ok(std::sync::Arc::new(Int64Array::from(vec![Some(sum_i64)])))
        }
        DataType::Float64 => {
            let mut sum = 0.0f64;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Float32 => f32::from_le_bytes(v[..4].try_into().unwrap()) as f64,
                    DataType::Float64 => f64::from_le_bytes(v[..8].try_into().unwrap()),
                    other => {
                        return Err((format!(
                            "multi_distinct_sum unsupported input type for float output: {:?}",
                            other
                        ))
                        .into());
                    }
                };
                sum += value;
            }
            Ok(std::sync::Arc::new(Float64Array::from(vec![Some(sum)])))
        }
        DataType::Decimal128(precision, scale) => {
            let mut sum: i128 = 0;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Decimal128(_, _) => i128::from_le_bytes(v[..16].try_into().unwrap()),
                    other => {
                        return Err((format!(
                            "multi_distinct_sum unsupported input type for decimal output: {:?}",
                            other
                        ))
                        .into());
                    }
                };
                sum += value;
            }
            let array = Decimal128Array::from(vec![Some(sum)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(std::sync::Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let mut sum = i256::ZERO;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Decimal256(_, _) => i256::from_le_bytes(
                        v[..32]
                            .try_into()
                            .map_err(|_| "invalid Decimal256 distinct value bytes".to_string())?,
                    ),
                    other => {
                        return Err((format!(
                            "multi_distinct_sum unsupported input type for decimal output: {:?}",
                            other
                        ))
                        .into());
                    }
                };
                sum = sum
                    .checked_add(value)
                    .ok_or_else(|| "multi_distinct_sum decimal overflow".to_string())?;
            }
            let array = Decimal256Array::from(vec![Some(sum)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(std::sync::Arc::new(array))
        }
        other => Err((format!("multi_distinct_sum output type unsupported: {:?}", other)).into()),
    }
}

pub fn avg_from_set(
    set: &impl NumericDistinctSet,
    input_type: &DataType,
    output_type: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, DistinctComputationError> {
    if set.is_empty() {
        return Ok(arrow_array::new_null_array(output_type, 1));
    }
    match output_type {
        DataType::Float64 => {
            let mut sum = 0.0f64;
            for value in set.keys() {
                work.step()?;
                sum += match input_type {
                    DataType::Int8 => i8::from_le_bytes(value[..1].try_into().unwrap()) as f64,
                    DataType::Int16 => i16::from_le_bytes(value[..2].try_into().unwrap()) as f64,
                    DataType::Int32 => i32::from_le_bytes(value[..4].try_into().unwrap()) as f64,
                    DataType::Int64 => i64::from_le_bytes(value[..8].try_into().unwrap()) as f64,
                    DataType::Float32 => f32::from_le_bytes(value[..4].try_into().unwrap()) as f64,
                    DataType::Float64 => f64::from_le_bytes(value[..8].try_into().unwrap()),
                    other => {
                        return Err(
                            (format!("distinct avg numeric input unsupported: {other:?}")).into(),
                        );
                    }
                };
            }
            Ok(Arc::new(Float64Array::from(vec![Some(
                sum / set.len() as f64,
            )])))
        }
        DataType::Decimal128(precision, scale) => {
            let DataType::Decimal128(_, input_scale) = input_type else {
                return Err(("distinct avg decimal input signature mismatch".to_string()).into());
            };
            let mut sum = i256::ZERO;
            for value in set.keys() {
                work.step()?;
                let coefficient = i128::from_le_bytes(value[..16].try_into().unwrap());
                sum = sum
                    .checked_add(i256::from_i128(coefficient))
                    .ok_or_else(|| "distinct avg decimal sum overflow".to_string())?;
            }
            let difference = i32::from(*scale) - i32::from(*input_scale);
            let factor = pow10(difference.unsigned_abs() as usize, work)?;
            if difference >= 0 {
                sum = sum
                    .checked_mul(factor)
                    .ok_or_else(|| "distinct avg decimal rescale overflow".to_string())?;
            } else {
                sum = sum / factor;
            }
            let coefficient = round(sum, i256::from_i128(set.len() as i128))?
                .to_i128()
                .ok_or_else(|| "distinct avg decimal output overflow".to_string())?;
            let result = Decimal128Array::from(vec![Some(coefficient)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| error.to_string())?;
            result
                .validate_decimal_precision(*precision)
                .map_err(|error| error.to_string())?;
            Ok(Arc::new(result))
        }
        DataType::Decimal256(precision, scale) => {
            if input_type != output_type {
                return Err(
                    ("distinct avg decimal256 bound precision/scale mismatch".to_string()).into(),
                );
            }
            // Divide each coefficient before accumulation. A valid DECIMAL256
            // mean can fit even when the sum of its inputs exceeds i256.
            let count = i256::from_i128(set.len() as i128);
            let mut quotient = i256::ZERO;
            let mut remainder = i256::ZERO;
            for value in set.keys() {
                work.step()?;
                let coefficient = i256::from_le_bytes(value[..32].try_into().unwrap());
                quotient = quotient
                    .checked_add(coefficient / count)
                    .ok_or_else(|| "distinct avg decimal256 quotient overflow".to_string())?;
                remainder = remainder
                    .checked_add(coefficient % count)
                    .ok_or_else(|| "distinct avg decimal256 remainder overflow".to_string())?;
            }
            quotient = quotient
                .checked_add(remainder / count)
                .ok_or_else(|| "distinct avg decimal256 quotient overflow".to_string())?;
            remainder = remainder % count;
            // Normalize to the truncated quotient of the complete sum so that
            // half-up rounding also handles opposite-sign coefficients.
            if quotient > i256::ZERO && remainder < i256::ZERO {
                quotient = quotient - i256::ONE;
                remainder = remainder + count;
            } else if quotient < i256::ZERO && remainder > i256::ZERO {
                quotient = quotient + i256::ONE;
                remainder = remainder - count;
            }
            let rounded = quotient
                .checked_add(round(remainder, count)?)
                .ok_or_else(|| "distinct avg decimal256 rounding overflow".to_string())?;
            let result = Decimal256Array::from(vec![Some(rounded)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| error.to_string())?;
            result
                .validate_decimal_precision(*precision)
                .map_err(|error| error.to_string())?;
            Ok(Arc::new(result))
        }
        other => Err((format!("distinct avg output unsupported: {other:?}")).into()),
    }
}

fn pow10(n: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<i256, DistinctComputationError> {
    let mut v = i256::ONE;
    for _ in 0..n {
        work.step()?;
        v = v
            .checked_mul(i256::from_i128(10))
            .ok_or_else(|| "decimal overflow".to_string())?;
    }
    Ok(v)
}
fn round(dividend: i256, divisor: i256) -> Result<i256, String> {
    let mut q = dividend
        .checked_div(divisor)
        .ok_or_else(|| "decimal overflow".to_string())?;
    let r = dividend
        .checked_rem(divisor)
        .ok_or_else(|| "decimal overflow".to_string())?;
    if r == i256::ZERO {
        return Ok(q);
    }
    let abs_b = if divisor.is_negative() {
        divisor
            .checked_neg()
            .ok_or_else(|| "decimal overflow".to_string())?
    } else {
        divisor
    };
    let abs_r = if r.is_negative() {
        r.checked_neg()
            .ok_or_else(|| "decimal overflow".to_string())?
    } else {
        r
    };
    let threshold = (abs_b >> 1)
        .checked_add(abs_b & i256::ONE)
        .ok_or_else(|| "decimal overflow".to_string())?;
    if abs_r >= threshold {
        let carry = if dividend.is_negative() ^ divisor.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ONE
        };
        q = q
            .checked_add(carry)
            .ok_or_else(|| "decimal overflow".to_string())?;
    }
    Ok(q)
}

pub struct NumericKey {
    bytes: [u8; 32],
    len: usize,
}
impl NumericKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
fn encode_le<const N: usize>(value: [u8; N]) -> NumericKey {
    let mut bytes = [0u8; 32];
    bytes[..N].copy_from_slice(&value);
    NumericKey { bytes, len: N }
}
fn canonical_f32_bits(v: f32) -> u32 {
    if v.is_nan() {
        f32::NAN.to_bits()
    } else if v == 0.0 {
        0.0f32.to_bits()
    } else {
        v.to_bits()
    }
}
fn canonical_f64_bits(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0.0f64.to_bits()
    } else {
        v.to_bits()
    }
}
pub fn encode_numeric_row(
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<NumericKey>, DistinctComputationError> {
    work.step()?;
    if array.is_null(row) {
        return Ok(None);
    }
    let encoded = match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            encode_le([u8::from(arr.value(row))])
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            encode_le(canonical_f32_bits(arr.value(row)).to_le_bytes())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            encode_le(canonical_f64_bits(arr.value(row)).to_le_bytes())
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
            encode_le(arr.value(row).to_le_bytes())
        }
        other => {
            return Err((format!("multi_distinct_sum unsupported input type: {:?}", other)).into());
        }
    };
    Ok(Some(encoded))
}

pub fn serialize_set_into(
    set: &impl NumericDistinctSet,
    out: &mut impl NumericDistinctBuffer,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), DistinctComputationError> {
    let count = u32::try_from(set.len()).map_err(|_| "distinct set count overflow".to_string())?;
    let size = set.keys().try_fold(
        4usize,
        |size, value| -> Result<usize, DistinctComputationError> {
            work.step()?;
            u32::try_from(value.len()).map_err(|_| "distinct key length overflow".to_string())?;
            size.checked_add(4)
                .and_then(|size| size.checked_add(value.len()))
                .ok_or_else(|| {
                    DistinctComputationError::Operational(
                        "distinct set payload overflow".to_string(),
                    )
                })
        },
    )?;
    if size > i32::MAX as usize {
        return Err(("distinct state payload exceeds the Binary offset domain".to_string()).into());
    }
    work.flush()?;
    out.reserve_exact_typed(size)?;
    work.flush()?;
    out.append(&count.to_le_bytes());
    for value in set.keys() {
        work.step()?;
        out.append(&(value.len() as u32).to_le_bytes());
        out.append(value);
    }
    Ok(())
}

pub fn numeric_key_width(data_type: &DataType) -> Result<usize, DistinctComputationError> {
    match data_type {
        DataType::Boolean | DataType::Int8 => Ok(1),
        DataType::Int16 => Ok(2),
        DataType::Int32 | DataType::Float32 => Ok(4),
        DataType::Int64 | DataType::Float64 => Ok(8),
        DataType::Decimal128(..) => Ok(16),
        DataType::Decimal256(..) => Ok(32),
        other => Err((format!("distinct numeric key type unsupported: {other:?}")).into()),
    }
}

// Validate the complete payload before mutation, then visit borrowed keys.
// Keep the existing SUM state-v1 bytes: count:u32, then (length:u32, bytes)*.
pub fn visit_serialized_keys(
    bytes: &[u8],
    width: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(&[u8]) -> Result<(), DistinctComputationError>,
) -> Result<(), DistinctComputationError> {
    visit_serialized_keys_with_work(bytes, width, work, |bytes, _| visit(bytes))
}

pub fn visit_serialized_keys_with_work(
    bytes: &[u8],
    width: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(&[u8], &mut EvaluationCheckpoints<'_>) -> Result<(), DistinctComputationError>,
) -> Result<(), DistinctComputationError> {
    let read = |at: usize| -> Result<u32, String> {
        let end = at
            .checked_add(4)
            .ok_or_else(|| "distinct set offset overflow".to_string())?;
        let word = bytes
            .get(at..end)
            .ok_or_else(|| "invalid distinct set encoding".to_string())?;
        Ok(u32::from_le_bytes(word.try_into().unwrap()))
    };
    let count = read(0)? as usize;
    let stride = 4usize
        .checked_add(width)
        .ok_or_else(|| "distinct key width overflow".to_string())?;
    let expected = count
        .checked_mul(stride)
        .and_then(|length| length.checked_add(4))
        .ok_or_else(|| "distinct set length overflow".to_string())?;
    if bytes.len() != expected {
        return Err(("invalid distinct set payload length".to_string()).into());
    }
    for index in 0..count {
        work.step()?;
        if read(4 + index * stride)? as usize != width {
            return Err(
                ("distinct set key width differs from selected input type".to_string()).into(),
            );
        }
    }
    for index in 0..count {
        work.step()?;
        let start = 8 + index * stride;
        visit(&bytes[start..start + width], work)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_distinct_numeric_tests.rs"]
mod tests;
