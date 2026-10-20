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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprId, ExprNode, LiteralValue, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};

const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

pub fn eval_iceberg_identity<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    child: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    arena.eval(child, chunk)
}

pub fn eval_iceberg_void<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _child: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let data_type = arena
        .data_type(_child)
        .ok_or_else(|| expression_contract("iceberg void transform input type was not prepared"))?;
    Ok(arrow::array::new_null_array(&data_type, chunk.len()))
}

pub fn eval_iceberg_year<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    child: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    expression_contract("iceberg_transform_year expects DATE for Date32 array")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = arr.value(i);
                let date = date_from_days(days)?;
                let years = i64::from(date.year() - 1970);
                builder.append_value(years);
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    expression_contract(
                        "iceberg_transform_year expects DATETIME for Timestamp array",
                    )
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let dt = micros_to_naive_datetime(micros)?;
                let years = i64::from(dt.year() - 1970);
                builder.append_value(years);
            }
        }
        other => {
            return Err(expression_contract(format_args!(
                "iceberg_transform_year unsupported type: {other:?}"
            )));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_month<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    child: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    expression_contract("iceberg_transform_month expects DATE for Date32 array")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = arr.value(i);
                let date = date_from_days(days)?;
                let years = date.year() - 1970;
                let months = years as i64 * 12 + i64::from(date.month() - 1);
                builder.append_value(months);
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    expression_contract(
                        "iceberg_transform_month expects DATETIME for Timestamp array",
                    )
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let dt = micros_to_naive_datetime(micros)?;
                let years = dt.year() - 1970;
                let months = years as i64 * 12 + i64::from(dt.month() - 1);
                builder.append_value(months);
            }
        }
        other => {
            return Err(expression_contract(format_args!(
                "iceberg_transform_month unsupported type: {other:?}"
            )));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_day<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    child: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    expression_contract("iceberg_transform_day expects DATE for Date32 array")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = arr.value(i);
                builder.append_value(i64::from(days));
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    expression_contract(
                        "iceberg_transform_day expects DATETIME for Timestamp array",
                    )
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let days = micros.div_euclid(MICROS_PER_DAY);
                builder.append_value(days);
            }
        }
        other => {
            return Err(expression_contract(format_args!(
                "iceberg_transform_day unsupported type: {other:?}"
            )));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_hour<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    child: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    expression_contract("iceberg_transform_hour expects DATE for Date32 array")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = i64::from(arr.value(i));
                builder.append_value(days.saturating_mul(24));
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    expression_contract(
                        "iceberg_transform_hour expects DATETIME for Timestamp array",
                    )
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let hours = micros.div_euclid(MICROS_PER_HOUR);
                builder.append_value(hours);
            }
        }
        other => {
            return Err(expression_contract(format_args!(
                "iceberg_transform_hour unsupported type: {other:?}"
            )));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_bucket<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    value: ExprId,
    buckets: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let width = extract_const_i64(arena, buckets, chunk)?;
    if width <= 0 {
        return Err(ExecutionFailure::from_domain(
            TransformValueFailure::Width {
                operation: "bucket",
                value: Some(width),
            },
        ));
    }
    let array = arena.eval(value, chunk)?;
    let mut builder = arrow::array::Int32Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects INT"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i) as i64;
                let h = murmur3_32(&v.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects BIGINT"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let h = murmur3_32(&v.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects VARCHAR"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i).as_bytes();
                let h = murmur3_32(v, 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects BINARY"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let h = murmur3_32(v, 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects DATE"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = i64::from(arr.value(i));
                let h = murmur3_32(&days.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects DATETIME"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let h = murmur3_32(&micros.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_bucket expects DECIMAL"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let bytes = decimal_to_bytes(v);
                let h = murmur3_32(&bytes, 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        other => {
            return Err(expression_contract(format_args!(
                "iceberg_transform_bucket unsupported type: {other:?}"
            )));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_truncate<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    value: ExprId,
    width: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let width = extract_const_i64(arena, width, chunk)?;
    if width <= 0 {
        return Err(ExecutionFailure::from_domain(
            TransformValueFailure::Width {
                operation: "truncate",
                value: Some(width),
            },
        ));
    }
    let array = arena.eval(value, chunk)?;
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_truncate expects INT"))?;
            let mut builder = arrow::array::Int32Builder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i) as i64;
                let truncated = truncate_i128(i128::from(v), i128::from(width), 32)?;
                let truncated = i32::try_from(truncated).map_err(|source| {
                    ExecutionFailure::from_domain(TransformValueFailure::Narrow {
                        source,
                        value: truncated,
                        bits: 32,
                    })
                })?;
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_truncate expects BIGINT"))?;
            let mut builder = arrow::array::Int64Builder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let truncated = truncate_i128(i128::from(v), i128::from(width), 64)?;
                let truncated = i64::try_from(truncated).map_err(|source| {
                    ExecutionFailure::from_domain(TransformValueFailure::Narrow {
                        source,
                        value: truncated,
                        bits: 64,
                    })
                })?;
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| expression_contract("iceberg_transform_truncate expects DECIMAL"))?;
            let mut builder = arrow::array::Decimal128Builder::with_capacity(arr.len())
                .with_data_type(DataType::Decimal128(*precision, *scale));
            let width = width as i128;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let truncated = truncate_i128(v, width, 128)?;
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| expression_contract("iceberg_transform_truncate expects VARCHAR"))?;
            let mut builder = arrow::array::StringBuilder::with_capacity(arr.len(), 0);
            let width = width as usize;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let truncated: String = v.chars().take(width).collect();
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| expression_contract("iceberg_transform_truncate expects BINARY"))?;
            let mut builder = arrow::array::BinaryBuilder::with_capacity(arr.len(), 0);
            let width = width as usize;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let end = v.len().min(width);
                builder.append_value(&v[..end]);
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(expression_contract(format_args!(
            "iceberg_transform_truncate unsupported type: {other:?}"
        ))),
    }
}

fn extract_const_i64<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<i64> {
    match arena.node(expr) {
        Some(ExprNode::Literal(LiteralValue::Int32(value))) => return Ok(i64::from(*value)),
        Some(ExprNode::Literal(LiteralValue::Int64(value))) => return Ok(*value),
        Some(ExprNode::Literal(LiteralValue::Null)) => {
            return Err(ExecutionFailure::from_domain(
                TransformValueFailure::Width {
                    operation: "constant",
                    value: None,
                },
            ));
        }
        _ => {}
    }
    let array = arena.eval(expr, chunk)?;
    if array.is_empty() {
        return Err(expression_contract(
            "iceberg transform constant argument has no value",
        ));
    }
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| expression_contract("expected INT for constant argument"))?;
            if arr.is_null(0) {
                return Err(ExecutionFailure::from_domain(
                    TransformValueFailure::Width {
                        operation: "constant",
                        value: None,
                    },
                ));
            }
            Ok(i64::from(arr.value(0)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| expression_contract("expected BIGINT for constant argument"))?;
            if arr.is_null(0) {
                return Err(ExecutionFailure::from_domain(
                    TransformValueFailure::Width {
                        operation: "constant",
                        value: None,
                    },
                ));
            }
            Ok(arr.value(0))
        }
        other => Err(expression_contract(format_args!(
            "constant argument must be INT/BIGINT, got {other:?}"
        ))),
    }
}

fn micros_to_naive_datetime(micros: i64) -> ExecutionResult<NaiveDateTime> {
    let secs = micros.div_euclid(MICROS_PER_SECOND);
    let rem = micros.rem_euclid(MICROS_PER_SECOND);
    let nanos = (rem as u32) * 1000;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| ExecutionFailure::from_domain(TransformValueFailure::Timestamp(micros)))?;
    Ok(dt.naive_utc())
}

fn decimal_to_bytes(value: i128) -> Vec<u8> {
    let mut v = value;
    let mut bytes = value.to_le_bytes().to_vec();
    if v < 0 {
        v = !v;
    }
    let mut bit_len = 0;
    while v > 0 {
        v >>= 1;
        bit_len += 1;
    }
    let new_len = (bit_len / 8 + 1).max(1);
    bytes.resize(new_len, 0);
    bytes.reverse();
    bytes
}

fn murmur3_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let rem = chunks.remainder();
    let mut k1 = 0u32;
    match rem.len() {
        3 => {
            k1 ^= (rem[2] as u32) << 16;
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        2 => {
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        1 => {
            k1 ^= rem[0] as u32;
        }
        _ => {}
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= data.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    hash
}

#[derive(Debug)]
enum TransformValueFailure {
    Date(i32),
    Timestamp(i64),
    Width {
        operation: &'static str,
        value: Option<i64>,
    },
    Truncate {
        value: i128,
        width: i128,
        bits: u16,
    },
    Narrow {
        source: std::num::TryFromIntError,
        value: i128,
        bits: u16,
    },
}
impl std::fmt::Display for TransformValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Date(days) => write!(f, "invalid Date32 value {days}"),
            Self::Timestamp(micros) => write!(f, "invalid datetime micros {micros}"),
            Self::Width {
                operation,
                value: Some(value),
            } => write!(
                f,
                "iceberg_transform_{operation} width must be > 0, got {value}"
            ),
            Self::Width { value: None, .. } => f.write_str("constant argument is NULL"),
            Self::Truncate { value, width, bits } => write!(
                f,
                "iceberg truncate({value}, {width}) exceeds {bits}-bit integer"
            ),
            Self::Narrow { value, bits, .. } => write!(
                f,
                "iceberg truncated value {value} exceeds {bits}-bit integer"
            ),
        }
    }
}
impl std::error::Error for TransformValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Narrow { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for TransformValueFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Width { .. } => FailureFact::new(
                FailureClass::Data,
                crate::runtime::fragment::ExecutionReason::IcebergTransformParameter.reason(),
            ),
            _ => novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact(),
        }
    }
}
fn date_from_days(days: i32) -> ExecutionResult<NaiveDate> {
    UNIX_EPOCH_DAY_OFFSET
        .checked_add(days)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .ok_or_else(|| ExecutionFailure::from_domain(TransformValueFailure::Date(days)))
}
fn truncate_i128(value: i128, width: i128, bits: u16) -> ExecutionResult<i128> {
    if width <= 0 {
        return Err(expression_contract(
            "validated truncate width must be positive",
        ));
    }
    value.checked_sub(value.rem_euclid(width)).ok_or_else(|| {
        ExecutionFailure::from_domain(TransformValueFailure::Truncate { value, width, bits })
    })
}

#[cfg(test)]
mod source_properties {
    use super::*;
    use std::error::Error;
    #[test]
    fn real_calendar_and_timestamp_range_refusals_keep_numeric_facts() {
        for failure in [
            date_from_days(i32::MAX).unwrap_err(),
            date_from_days(i32::MIN).unwrap_err(),
            micros_to_naive_datetime(i64::MAX).unwrap_err(),
        ] {
            assert_eq!(failure.failure_fact().class(), FailureClass::Data);
            assert_eq!(failure.failure_fact().reason().code(), 41);
            assert!(failure.failure_fact().resource().is_none());
        }
        assert_eq!(date_from_days(0).unwrap().to_string(), "1970-01-01");
    }
    #[test]
    fn truncate_preserves_exact_negative_math_and_rejects_only_unrepresentable_output() {
        assert_eq!(truncate_i128(-1, 10, 64).unwrap(), -10);
        assert_eq!(
            truncate_i128(i128::from(i64::MAX - 1), i128::from(i64::MAX), 64).unwrap(),
            0
        );
        assert_eq!(truncate_i128(i128::MAX - 1, i128::MAX, 128).unwrap(), 0);
        let failure = truncate_i128(i128::MIN, 3, 128).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 41);
        let truncated = truncate_i128(i128::from(i32::MIN), 3, 32).unwrap();
        let source = i32::try_from(truncated).unwrap_err();
        let failure = ExecutionFailure::from_domain(TransformValueFailure::Narrow {
            source,
            value: truncated,
            bits: 32,
        });
        assert_eq!(failure.failure_fact().reason().code(), 41);
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<std::num::TryFromIntError>()
        );
    }
}
