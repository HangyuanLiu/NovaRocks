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
use super::super::*;
use arrow::array::{
    BinaryArray, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, ListArray, MapArray, StringArray, StringBuilder, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::i256;
use chrono::{DateTime, NaiveDate};
use std::cmp::Ordering;

use crate::exec::expr::agg::{AggregateAllocator, AggregateVec, aggregate_bytes};
use novarocks_types::largeint;
const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
}

pub(in crate::exec::expr::agg) fn build_bool_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BooleanBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const BoolState) };
        if state.has_value {
            builder.append_value(state.value);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[derive(Debug)]
pub(super) struct TrackedUtf8State {
    pub(super) value: Option<AggregateVec<u8>>,
    pub(super) allocator: AggregateAllocator,
}

impl TrackedUtf8State {
    pub(super) fn new(allocator: AggregateAllocator) -> Self {
        Self {
            value: None,
            allocator,
        }
    }

    pub(super) fn replace(&mut self, value: &str) -> Result<(), String> {
        let replacement = aggregate_bytes(self.allocator.clone(), value.as_bytes())?;
        self.value = Some(replacement);
        Ok(())
    }
}

pub(in crate::exec::expr::agg) fn build_utf8_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const TrackedUtf8State) };
        match &state.value {
            Some(value) => {
                builder.append_value(std::str::from_utf8(value).map_err(|error| error.to_string())?)
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(in crate::exec::expr::agg) fn build_date32_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I32State) };
        values.push(state.has_value.then_some(state.value));
    }
    Ok(Arc::new(Date32Array::from(values)))
}

pub(in crate::exec::expr::agg) fn build_timestamp_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (unit, tz) = match output_type {
        DataType::Timestamp(unit, tz) => (*unit, tz.as_deref().map(|s| s.to_string())),
        other => return Err(format!("timestamp output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I64State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array: ArrayRef = match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
    };
    Ok(array)
}

pub(in crate::exec::expr::agg) fn build_decimal128_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I128State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

pub(in crate::exec::expr::agg) fn build_decimal256_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal256(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal256 output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I256State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

pub(in crate::exec::expr::agg) fn build_largeint_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I128State) };
        values.push(state.has_value.then_some(state.value));
    }
    largeint::array_from_i128(&values)
}

pub use novarocks_functions::aggregate_scalar::AggScalarValue;
pub(super) type TrackedAggScalarValue =
    novarocks_functions::aggregate_scalar::TrackedAggScalarValue<AggregateAllocator>;

pub(super) fn aggregate_vec_with_capacity<T>(
    allocator: &AggregateAllocator,
    capacity: usize,
    operation: &str,
) -> Result<AggregateVec<T>, String> {
    let mut values = AggregateVec::new_in(allocator.clone());
    values
        .try_reserve_exact(capacity)
        .map_err(|_| allocator.allocation_error(operation))?;
    Ok(values)
}

pub(super) fn tracked_scalar_from_array(
    array: &ArrayRef,
    row: usize,
    allocator: &AggregateAllocator,
) -> Result<Option<TrackedAggScalarValue>, String> {
    novarocks_functions::aggregate_scalar::tracked_scalar_from_array(
        array,
        row,
        allocator,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
pub(super) fn tracked_scalar_from_value(
    value: AggScalarValue,
    allocator: &AggregateAllocator,
) -> Result<TrackedAggScalarValue, String> {
    Ok(match value {
        AggScalarValue::Bool(value) => TrackedAggScalarValue::Bool(value),
        AggScalarValue::Int64(value) => TrackedAggScalarValue::Int64(value),
        AggScalarValue::Float64(value) => TrackedAggScalarValue::Float64(value),
        AggScalarValue::Utf8(value) => {
            TrackedAggScalarValue::Utf8(aggregate_bytes(allocator.clone(), value.as_bytes())?)
        }
        AggScalarValue::Date32(value) => TrackedAggScalarValue::Date32(value),
        AggScalarValue::Timestamp(value) => TrackedAggScalarValue::Timestamp(value),
        AggScalarValue::Decimal128(value) => TrackedAggScalarValue::Decimal128(value),
        AggScalarValue::Decimal256(value) => TrackedAggScalarValue::Decimal256(value),
        AggScalarValue::Binary(value) => {
            TrackedAggScalarValue::Binary(aggregate_bytes(allocator.clone(), &value)?)
        }
        AggScalarValue::Struct(values) => TrackedAggScalarValue::Struct(
            tracked_optional_values_from_values(values, allocator, "struct")?,
        ),
        AggScalarValue::List(values) => TrackedAggScalarValue::List(
            tracked_optional_values_from_values(values, allocator, "list")?,
        ),
        AggScalarValue::Map(entries) => {
            let mut tracked = aggregate_vec_with_capacity(
                allocator,
                entries.len(),
                "reserve aggregate map scalar",
            )?;
            for (key, value) in entries {
                tracked.push((
                    key.map(|value| tracked_scalar_from_value(value, allocator))
                        .transpose()?,
                    value
                        .map(|value| tracked_scalar_from_value(value, allocator))
                        .transpose()?,
                ));
            }
            TrackedAggScalarValue::Map(tracked)
        }
    })
}

#[cfg(test)]
fn tracked_optional_values_from_values(
    values: Vec<Option<AggScalarValue>>,
    allocator: &AggregateAllocator,
    kind: &str,
) -> Result<AggregateVec<Option<TrackedAggScalarValue>>, String> {
    let operation = match kind {
        "struct" => "reserve aggregate struct scalar",
        "list" => "reserve aggregate list scalar",
        _ => "reserve aggregate nested scalar",
    };
    let mut tracked = aggregate_vec_with_capacity(allocator, values.len(), operation)?;
    for value in values {
        tracked.push(
            value
                .map(|value| tracked_scalar_from_value(value, allocator))
                .transpose()?,
        );
    }
    Ok(tracked)
}

pub(super) fn tracked_scalar_to_output(
    value: &TrackedAggScalarValue,
) -> Result<AggScalarValue, String> {
    novarocks_functions::aggregate_scalar::tracked_scalar_to_output(
        value,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

pub(super) fn compare_tracked_scalar_values(
    left: &TrackedAggScalarValue,
    right: &TrackedAggScalarValue,
) -> Result<Ordering, String> {
    novarocks_functions::aggregate_scalar::compare_tracked_scalar_values(
        left,
        right,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

pub(super) fn tracked_key_fingerprint(
    key: &TrackedAggScalarValue,
    allocator: &AggregateAllocator,
) -> Result<AggregateVec<u8>, String> {
    let encoded_len = tracked_scalar_encoded_len(key)?;
    let mut output = aggregate_vec_with_capacity(
        allocator,
        encoded_len,
        "reserve aggregate scalar fingerprint",
    )?;
    encode_tracked_scalar(&mut output, key)?;
    debug_assert_eq!(output.len(), encoded_len);
    Ok(output)
}

pub(super) fn tracked_optional_key_fingerprint(
    value: &Option<TrackedAggScalarValue>,
    allocator: &AggregateAllocator,
) -> Result<AggregateVec<u8>, String> {
    let value_len = value
        .as_ref()
        .map(tracked_scalar_encoded_len)
        .transpose()?
        .unwrap_or(0);
    let encoded_len = 1usize
        .checked_add(value_len)
        .ok_or_else(|| "aggregate scalar fingerprint length overflow".to_string())?;
    let mut output = aggregate_vec_with_capacity(
        allocator,
        encoded_len,
        "reserve optional aggregate scalar fingerprint",
    )?;
    encode_tracked_optional_value(&mut output, value)?;
    debug_assert_eq!(output.len(), encoded_len);
    Ok(output)
}

fn checked_encoded_len_add(total: &mut usize, additional: usize) -> Result<(), String> {
    *total = total
        .checked_add(additional)
        .ok_or_else(|| "aggregate scalar fingerprint length overflow".to_string())?;
    Ok(())
}

fn checked_u32_len(len: usize) -> Result<u32, String> {
    u32::try_from(len).map_err(|_| "aggregate scalar fingerprint exceeds u32 length".to_string())
}

fn tracked_scalar_encoded_len(value: &TrackedAggScalarValue) -> Result<usize, String> {
    let mut len = 1usize;
    match value {
        TrackedAggScalarValue::Bool(_) => checked_encoded_len_add(&mut len, 1)?,
        TrackedAggScalarValue::Int64(_)
        | TrackedAggScalarValue::Float64(_)
        | TrackedAggScalarValue::Timestamp(_) => checked_encoded_len_add(&mut len, 8)?,
        TrackedAggScalarValue::Date32(_) => checked_encoded_len_add(&mut len, 4)?,
        TrackedAggScalarValue::Decimal128(_) => checked_encoded_len_add(&mut len, 16)?,
        TrackedAggScalarValue::Decimal256(_) => checked_encoded_len_add(&mut len, 32)?,
        TrackedAggScalarValue::Utf8(bytes) | TrackedAggScalarValue::Binary(bytes) => {
            let _ = checked_u32_len(bytes.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            checked_encoded_len_add(&mut len, bytes.len())?;
        }
        TrackedAggScalarValue::Struct(values) | TrackedAggScalarValue::List(values) => {
            let _ = checked_u32_len(values.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            for value in values {
                checked_encoded_len_add(&mut len, 1)?;
                if let Some(value) = value {
                    checked_encoded_len_add(&mut len, tracked_scalar_encoded_len(value)?)?;
                }
            }
        }
        TrackedAggScalarValue::Map(entries) => {
            let _ = checked_u32_len(entries.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            for (key, value) in entries {
                for value in [key, value] {
                    checked_encoded_len_add(&mut len, 1)?;
                    if let Some(value) = value {
                        checked_encoded_len_add(&mut len, tracked_scalar_encoded_len(value)?)?;
                    }
                }
            }
        }
    }
    Ok(len)
}

fn encode_tracked_scalar(
    output: &mut AggregateVec<u8>,
    value: &TrackedAggScalarValue,
) -> Result<(), String> {
    match value {
        TrackedAggScalarValue::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        TrackedAggScalarValue::Int64(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Float64(value) => {
            output.push(3);
            let bits = if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            };
            output.extend_from_slice(&bits.to_le_bytes());
        }
        TrackedAggScalarValue::Utf8(value) => {
            output.push(4);
            output.extend_from_slice(&checked_u32_len(value.len())?.to_le_bytes());
            output.extend_from_slice(value);
        }
        TrackedAggScalarValue::Date32(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Timestamp(value) => {
            output.push(6);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Decimal128(value) => {
            output.push(7);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Struct(values) => {
            output.push(8);
            encode_tracked_optional_values(output, values)?;
        }
        TrackedAggScalarValue::Map(entries) => {
            output.push(9);
            output.extend_from_slice(&checked_u32_len(entries.len())?.to_le_bytes());
            for (key, value) in entries {
                encode_tracked_optional_value(output, key)?;
                encode_tracked_optional_value(output, value)?;
            }
        }
        TrackedAggScalarValue::List(values) => {
            output.push(10);
            encode_tracked_optional_values(output, values)?;
        }
        TrackedAggScalarValue::Decimal256(value) => {
            output.push(11);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Binary(value) => {
            output.push(12);
            output.extend_from_slice(&checked_u32_len(value.len())?.to_le_bytes());
            output.extend_from_slice(value);
        }
    }
    Ok(())
}

fn encode_tracked_optional_values(
    output: &mut AggregateVec<u8>,
    values: &[Option<TrackedAggScalarValue>],
) -> Result<(), String> {
    output.extend_from_slice(&checked_u32_len(values.len())?.to_le_bytes());
    for value in values {
        encode_tracked_optional_value(output, value)?;
    }
    Ok(())
}

fn encode_tracked_optional_value(
    output: &mut AggregateVec<u8>,
    value: &Option<TrackedAggScalarValue>,
) -> Result<(), String> {
    if let Some(value) = value {
        output.push(1);
        encode_tracked_scalar(output, value)?;
    } else {
        output.push(0);
    }
    Ok(())
}

/// Heap bytes owned by a scalar value, excluding the inline enum body.
///
/// This walks a newly materialized value once. Aggregate states that can hold
/// an unbounded number of values cache the resulting sum so their
/// `retained_bytes` implementation remains O(1).
#[cfg(test)]
pub(super) fn scalar_heap_bytes(value: &AggScalarValue) -> usize {
    match value {
        AggScalarValue::Utf8(value) => value.capacity(),
        AggScalarValue::Binary(value) => value.capacity(),
        AggScalarValue::Struct(values) | AggScalarValue::List(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<AggScalarValue>>())
            .saturating_add(
                values
                    .iter()
                    .flatten()
                    .map(scalar_heap_bytes)
                    .sum::<usize>(),
            ),
        AggScalarValue::Map(entries) => entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(
                Option<AggScalarValue>,
                Option<AggScalarValue>,
            )>())
            .saturating_add(
                entries
                    .iter()
                    .flat_map(|(key, value)| [key.as_ref(), value.as_ref()])
                    .flatten()
                    .map(scalar_heap_bytes)
                    .sum::<usize>(),
            ),
        _ => 0,
    }
}

pub fn scalar_from_array(array: &ArrayRef, row: usize) -> Result<Option<AggScalarValue>, String> {
    match array.data_type() {
        DataType::Null => Ok(None),
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Bool(arr.value(row))))
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row))))
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Float64(arr.value(row) as f64)))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Float64(arr.value(row))))
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Utf8(arr.value(row).to_string())))
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Binary(arr.value(row).to_vec())))
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Binary(arr.value(row).to_vec())))
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Date32(arr.value(row))))
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Decimal128(arr.value(row))))
            }
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Decimal256(arr.value(row))))
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let v = largeint::value_at(arr, row)?;
                Ok(Some(AggScalarValue::Decimal128(v)))
            }
        }
        DataType::List(_item) => {
            let arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = arr.values();
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                out.push(scalar_from_array(values, idx)?);
            }
            Ok(Some(AggScalarValue::List(out)))
        }
        DataType::Struct(fields) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let mut out = Vec::with_capacity(fields.len());
            for col in arr.columns() {
                out.push(scalar_from_array(col, row)?);
            }
            Ok(Some(AggScalarValue::Struct(out)))
        }
        DataType::Map(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let keys = arr.keys();
            let values = arr.values();
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                out.push((
                    scalar_from_array(keys, idx)?,
                    scalar_from_array(values, idx)?,
                ));
            }
            Ok(Some(AggScalarValue::Map(out)))
        }
        other => Err(format!("unsupported scalar type: {:?}", other)),
    }
}

pub(in crate::exec::expr::agg) fn scalar_to_string(
    value: &AggScalarValue,
    data_type: &DataType,
) -> Result<String, String> {
    match value {
        AggScalarValue::Bool(v) => Ok(if *v { "1".to_string() } else { "0".to_string() }),
        AggScalarValue::Int64(v) => Ok(v.to_string()),
        AggScalarValue::Float64(v) => Ok(v.to_string()),
        AggScalarValue::Utf8(v) => Ok(v.clone()),
        AggScalarValue::Date32(v) => {
            let date = date32_to_naive(*v).ok_or_else(|| "invalid date32 value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        AggScalarValue::Timestamp(v) => match data_type {
            DataType::Timestamp(unit, tz) => Ok(format_timestamp(*unit, *v, tz.as_deref())),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Decimal128(v) => match data_type {
            DataType::Decimal128(_, scale) => Ok(format_decimal(*v, *scale)),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Decimal256(v) => match data_type {
            DataType::Decimal256(_, scale) => Ok(format_decimal256(*v, *scale)),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Binary(v) => Ok(hex::encode(v)),
        AggScalarValue::Struct(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Some(v) => rendered.push(scalar_to_string(v, data_type)?),
                    None => rendered.push("NULL".to_string()),
                }
            }
            Ok(format!("{{{}}}", rendered.join(",")))
        }
        AggScalarValue::Map(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for (k, v) in items {
                let key = match k {
                    Some(k) => scalar_to_string(k, data_type)?,
                    None => "NULL".to_string(),
                };
                let value = match v {
                    Some(v) => scalar_to_string(v, data_type)?,
                    None => "NULL".to_string(),
                };
                rendered.push(format!("{}:{}", key, value));
            }
            Ok(format!("{{{}}}", rendered.join(",")))
        }
        AggScalarValue::List(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Some(v) => rendered.push(scalar_to_string(v, data_type)?),
                    None => rendered.push("NULL".to_string()),
                }
            }
            Ok(format!("[{}]", rendered.join(",")))
        }
    }
}

fn format_timestamp(unit: TimeUnit, value: i64, tz: Option<&str>) -> String {
    // Align with StarRocks: omit fractional part when zero (e.g. "2020-01-01 00:10:00" not "2020-01-01 00:10:00.000000")
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value / 1_000;
            let millis = value.rem_euclid(1_000) as u32;
            let nanos = millis * 1_000_000;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if millis == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            }
        }
        TimeUnit::Microsecond => {
            let seconds = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            let nanos = micros * 1_000;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if micros == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
        TimeUnit::Nanosecond => {
            let seconds = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if nanos == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
            }
        }
    };
    if let Some(tz) = tz {
        format!("{} {}", timestamp_str, tz)
    } else {
        timestamp_str
    }
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    let scale = scale as i32;
    if scale <= 0 {
        return unscaled.to_string();
    }

    let unscaled_str = unscaled.abs().to_string();
    let scale_usize = scale as usize;

    if unscaled_str.len() <= scale_usize {
        let padded = format!("{:0>width$}", unscaled_str, width = scale_usize);
        if unscaled < 0 {
            format!("-0.{}", padded)
        } else {
            format!("0.{}", padded)
        }
    } else {
        let split_pos = unscaled_str.len() - scale_usize;
        let integer_part = &unscaled_str[..split_pos];
        let fractional_part = &unscaled_str[split_pos..];
        if unscaled < 0 {
            format!("-{}.{}", integer_part, fractional_part)
        } else {
            format!("{}.{}", integer_part, fractional_part)
        }
    }
}

fn format_decimal256(unscaled: i256, scale: i8) -> String {
    let scale = scale as i32;
    if scale <= 0 {
        return unscaled.to_string();
    }

    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let abs_str = abs.to_string();
    let scale_usize = scale as usize;

    if abs_str.len() <= scale_usize {
        let padded = format!("{:0>width$}", abs_str, width = scale_usize);
        if negative {
            format!("-0.{}", padded)
        } else {
            format!("0.{}", padded)
        }
    } else {
        let split_pos = abs_str.len() - scale_usize;
        let integer_part = &abs_str[..split_pos];
        let fractional_part = &abs_str[split_pos..];
        if negative {
            format!("-{}.{}", integer_part, fractional_part)
        } else {
            format!("{}.{}", integer_part, fractional_part)
        }
    }
}

pub fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
) -> Result<Ordering, String> {
    match (left, right) {
        (AggScalarValue::Bool(l), AggScalarValue::Bool(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Int64(l), AggScalarValue::Int64(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Float64(l), AggScalarValue::Float64(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| "float comparison is not ordered".to_string()),
        (AggScalarValue::Utf8(l), AggScalarValue::Utf8(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Date32(l), AggScalarValue::Date32(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Timestamp(l), AggScalarValue::Timestamp(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Decimal128(l), AggScalarValue::Decimal128(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Decimal256(l), AggScalarValue::Decimal256(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Binary(l), AggScalarValue::Binary(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Struct(l), AggScalarValue::Struct(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                let ord = compare_optional_scalar_values(&l[idx], &r[idx])?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        (AggScalarValue::Map(l), AggScalarValue::Map(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                let (lk, lv) = &l[idx];
                let (rk, rv) = &r[idx];
                let key_ord = compare_optional_scalar_values(lk, rk)?;
                if !key_ord.is_eq() {
                    return Ok(key_ord);
                }
                let value_ord = compare_optional_scalar_values(lv, rv)?;
                if !value_ord.is_eq() {
                    return Ok(value_ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        (AggScalarValue::List(l), AggScalarValue::List(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                let ord = compare_optional_scalar_values(&l[idx], &r[idx])?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        _ => Err("scalar comparison type mismatch".to_string()),
    }
}

fn compare_optional_scalar_values(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
) -> Result<Ordering, String> {
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(l), Some(r)) => compare_scalar_values(l, r),
    }
}

/// Stable byte fingerprint of an `AggScalarValue` suitable for use as a
/// `HashMap`/`HashSet` key when ordinary `PartialEq + Hash` is not available
/// (e.g. floats, decimals). Notable properties:
///
/// - `Utf8` is length-prefixed so `"ab"+"c"` and `"a"+"bc"` cannot collide.
/// - `Float32`/`Float64` NaN values are normalized to a single canonical bit
///   pattern so multiple NaN inputs collapse into one bucket.
/// - Composite types (`Struct`/`Map`/`List`) are encoded recursively for
///   safety, even though aggregate callers reject them upstream when needed.
///
/// Shared by aggregate functions that need stable scalar-key identity.
pub(in crate::exec::expr::agg) fn key_fingerprint(key: &AggScalarValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_scalar(&mut out, key);
    out
}

fn encode_scalar(out: &mut Vec<u8>, key: &AggScalarValue) {
    match key {
        AggScalarValue::Bool(v) => {
            out.push(1);
            out.push(if *v { 1 } else { 0 });
        }
        AggScalarValue::Int64(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Float64(v) => {
            out.push(3);
            // Normalize NaN: any NaN maps to the same fingerprint so
            // multiple NaN inputs collapse into one bucket.
            let bits = if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            };
            out.extend_from_slice(&bits.to_le_bytes());
        }
        AggScalarValue::Utf8(v) => {
            out.push(4);
            let len = v.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        AggScalarValue::Date32(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Timestamp(v) => {
            out.push(6);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal128(v) => {
            out.push(7);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal256(v) => {
            out.push(11);
            let text = v.to_string();
            let len = text.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        AggScalarValue::Binary(v) => {
            out.push(12);
            let len = v.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(v);
        }
        AggScalarValue::Struct(items) => {
            out.push(8);
            let len = items.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            for item in items {
                match item {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v);
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::Map(items) => {
            out.push(9);
            let len = items.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            for (k, v) in items {
                match k {
                    Some(k) => {
                        out.push(1);
                        encode_scalar(out, k);
                    }
                    None => out.push(0),
                }
                match v {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v);
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::List(items) => {
            out.push(10);
            let len = items.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            for item in items {
                match item {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v);
                    }
                    None => out.push(0),
                }
            }
        }
    }
}

pub fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
) -> Result<ArrayRef, String> {
    novarocks_functions::aggregate_scalar::build_scalar_array(
        output_type,
        values,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::datatypes::{Field, Fields};

    #[test]
    fn scalar_heap_bytes_counts_nested_capacities_once() {
        let mut text = String::with_capacity(64);
        text.push_str("value");
        let text_capacity = text.capacity();
        let mut binary = Vec::with_capacity(32);
        binary.extend_from_slice(b"bytes");
        let binary_capacity = binary.capacity();
        let mut items = Vec::with_capacity(8);
        items.push(Some(AggScalarValue::Utf8(text)));
        items.push(Some(AggScalarValue::Binary(binary)));
        let items_capacity = items.capacity();
        let value = AggScalarValue::List(items);

        assert_eq!(
            scalar_heap_bytes(&value),
            items_capacity * std::mem::size_of::<Option<AggScalarValue>>()
                + text_capacity
                + binary_capacity
        );
    }

    #[test]
    fn tracked_scalar_owns_nested_string_binary_list_struct_and_map_allocations() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let map_entry_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Binary, true),
        ]));
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", map_entry_type, false)),
            false,
        );
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("items", list_type.clone(), true),
            Field::new("attributes", map_type.clone(), true),
        ]));
        let input_value = AggScalarValue::Struct(vec![
            Some(AggScalarValue::Utf8("root".to_string())),
            Some(AggScalarValue::Binary(vec![1, 2, 3])),
            Some(AggScalarValue::List(vec![
                Some(AggScalarValue::Utf8("first".to_string())),
                None,
            ])),
            Some(AggScalarValue::Map(vec![(
                Some(AggScalarValue::Utf8("key".to_string())),
                Some(AggScalarValue::Binary(vec![4, 5])),
            )])),
        ]);
        let array = build_scalar_array(&struct_type, vec![Some(input_value)]).unwrap();
        let tracker = MemTracker::new_root("nested-tracked-scalar-test");
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));

        let tracked = tracked_scalar_from_array(&array, 0, &allocator)
            .unwrap()
            .unwrap();
        assert!(tracker.current() > 0);
        let output = tracked_scalar_to_output(&tracked).unwrap();
        let AggScalarValue::Struct(fields) = output else {
            panic!("expected struct output");
        };
        assert!(matches!(
            fields[0].as_ref(),
            Some(AggScalarValue::Utf8(value)) if value == "root"
        ));
        assert!(matches!(
            fields[1].as_ref(),
            Some(AggScalarValue::Binary(value)) if value == &[1, 2, 3]
        ));
        assert!(matches!(fields[2], Some(AggScalarValue::List(_))));
        assert!(matches!(fields[3], Some(AggScalarValue::Map(_))));

        drop(tracked);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn tracked_nested_scalar_oom_releases_partial_recursive_allocations() {
        let struct_type =
            DataType::Struct(Fields::from(vec![Field::new("text", DataType::Utf8, true)]));
        let array = build_scalar_array(
            &struct_type,
            vec![Some(AggScalarValue::Struct(vec![Some(
                AggScalarValue::Utf8("too-large".to_string()),
            )]))],
        )
        .unwrap();
        let tracker = MemTracker::new_root("nested-tracked-scalar-oom-test");
        tracker.install_limit_once(1).unwrap();
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));

        let error = tracked_scalar_from_array(&array, 0, &allocator).unwrap_err();
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn tracked_fingerprint_is_exactly_reserved_and_released() {
        let value = AggScalarValue::Struct(vec![
            Some(AggScalarValue::Int64(7)),
            Some(AggScalarValue::Utf8("tracked".to_string())),
        ]);
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("number", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let array = build_scalar_array(&data_type, vec![Some(value.clone())]).unwrap();
        let tracker = MemTracker::new_root("tracked-fingerprint-test");
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let tracked = tracked_scalar_from_array(&array, 0, &allocator)
            .unwrap()
            .unwrap();
        let state_bytes = tracker.current();

        let fingerprint = tracked_key_fingerprint(&tracked, &allocator).unwrap();
        assert_eq!(fingerprint.as_slice(), key_fingerprint(&value));
        assert_eq!(fingerprint.len(), fingerprint.capacity());
        assert!(tracker.current() > state_bytes);

        drop(fingerprint);
        drop(tracked);
        assert_eq!(tracker.current(), 0);
    }
}
