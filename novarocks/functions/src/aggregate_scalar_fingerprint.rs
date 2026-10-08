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
//! Original ARRAY aggregate scalar fingerprint and owned comparator bodies.
//! These original fingerprints differ from COUNT DISTINCT and are not interchangeable.
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork, TrackedAggScalarValue,
    aggregate_vec_with_capacity,
};
use allocator_api2::vec::Vec as ScalarVec;
use std::cmp::Ordering;
pub fn tracked_key_fingerprint<A: ScalarStateAllocator>(
    key: &TrackedAggScalarValue<A>,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, ScalarStateError> {
    work.step()?;
    let encoded_len = tracked_scalar_encoded_len(key, work)?;
    let mut output = aggregate_vec_with_capacity(
        allocator,
        encoded_len,
        "reserve aggregate scalar fingerprint",
        work,
    )?;
    encode_tracked_scalar(&mut output, key, work)?;
    debug_assert_eq!(output.len(), encoded_len);
    Ok(output)
}

pub fn tracked_optional_key_fingerprint<A: ScalarStateAllocator>(
    value: &Option<TrackedAggScalarValue<A>>,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, ScalarStateError> {
    work.step()?;
    let value_len = value
        .as_ref()
        .map(|value| tracked_scalar_encoded_len(value, work))
        .transpose()?
        .unwrap_or(0);
    let encoded_len = 1usize
        .checked_add(value_len)
        .ok_or_else(|| "aggregate scalar fingerprint length overflow".to_string())?;
    let mut output = aggregate_vec_with_capacity(
        allocator,
        encoded_len,
        "reserve optional aggregate scalar fingerprint",
        work,
    )?;
    encode_tracked_optional_value(&mut output, value, work)?;
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

fn tracked_scalar_encoded_len<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, ScalarStateError> {
    work.step()?;
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
                work.step()?;
                checked_encoded_len_add(&mut len, 1)?;
                if let Some(value) = value {
                    checked_encoded_len_add(&mut len, tracked_scalar_encoded_len(value, work)?)?;
                }
            }
        }
        TrackedAggScalarValue::Map(entries) => {
            let _ = checked_u32_len(entries.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            for (key, value) in entries {
                work.step()?;
                for value in [key, value] {
                    work.step()?;
                    checked_encoded_len_add(&mut len, 1)?;
                    if let Some(value) = value {
                        checked_encoded_len_add(
                            &mut len,
                            tracked_scalar_encoded_len(value, work)?,
                        )?;
                    }
                }
            }
        }
    }
    Ok(len)
}

fn encode_tracked_scalar<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
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
            for _ in value {
                work.step()?;
            }
            work.flush()?;
            output.extend_from_slice(value);
            work.flush()?;
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
            encode_tracked_optional_values(output, values, work)?;
        }
        TrackedAggScalarValue::Map(entries) => {
            output.push(9);
            output.extend_from_slice(&checked_u32_len(entries.len())?.to_le_bytes());
            for (key, value) in entries {
                work.step()?;
                encode_tracked_optional_value(output, key, work)?;
                encode_tracked_optional_value(output, value, work)?;
            }
        }
        TrackedAggScalarValue::List(values) => {
            output.push(10);
            encode_tracked_optional_values(output, values, work)?;
        }
        TrackedAggScalarValue::Decimal256(value) => {
            output.push(11);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Binary(value) => {
            output.push(12);
            output.extend_from_slice(&checked_u32_len(value.len())?.to_le_bytes());
            for _ in value {
                work.step()?;
            }
            work.flush()?;
            output.extend_from_slice(value);
            work.flush()?;
        }
    }
    Ok(())
}

fn encode_tracked_optional_values<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    values: &[Option<TrackedAggScalarValue<A>>],
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    output.extend_from_slice(&checked_u32_len(values.len())?.to_le_bytes());
    for value in values {
        work.step()?;
        encode_tracked_optional_value(output, value, work)?;
    }
    Ok(())
}

fn encode_tracked_optional_value<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    value: &Option<TrackedAggScalarValue<A>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    if let Some(value) = value {
        output.push(1);
        encode_tracked_scalar(output, value, work)?;
    } else {
        output.push(0);
    }
    Ok(())
}

pub fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, ScalarStateError> {
    work.step()?;
    match (left, right) {
        (AggScalarValue::Bool(l), AggScalarValue::Bool(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Int64(l), AggScalarValue::Int64(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Float64(l), AggScalarValue::Float64(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| "float comparison is not ordered".to_string().into()),
        (AggScalarValue::Utf8(l), AggScalarValue::Utf8(r)) => {
            work.flush()?;
            let ord = l.cmp(r);
            work.flush()?;
            Ok(ord)
        }
        (AggScalarValue::Date32(l), AggScalarValue::Date32(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Timestamp(l), AggScalarValue::Timestamp(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Decimal128(l), AggScalarValue::Decimal128(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Decimal256(l), AggScalarValue::Decimal256(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Binary(l), AggScalarValue::Binary(r)) => {
            work.flush()?;
            let ord = l.cmp(r);
            work.flush()?;
            Ok(ord)
        }
        (AggScalarValue::Struct(l), AggScalarValue::Struct(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                work.step()?;
                let ord = compare_optional_scalar_values(&l[idx], &r[idx], work)?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        (AggScalarValue::Map(l), AggScalarValue::Map(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                work.step()?;
                let (lk, lv) = &l[idx];
                let (rk, rv) = &r[idx];
                let key_ord = compare_optional_scalar_values(lk, rk, work)?;
                if !key_ord.is_eq() {
                    return Ok(key_ord);
                }
                let value_ord = compare_optional_scalar_values(lv, rv, work)?;
                if !value_ord.is_eq() {
                    return Ok(value_ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        (AggScalarValue::List(l), AggScalarValue::List(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                work.step()?;
                let ord = compare_optional_scalar_values(&l[idx], &r[idx], work)?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        _ => Err("scalar comparison type mismatch".to_string().into()),
    }
}

fn compare_optional_scalar_values(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, ScalarStateError> {
    work.step()?;
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(l), Some(r)) => compare_scalar_values(l, r, work),
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
pub fn key_fingerprint(
    key: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, ScalarStateError> {
    work.step()?;
    let mut out = Vec::new();
    encode_scalar(&mut out, key, work)?;
    Ok(out)
}

fn encode_scalar(
    out: &mut Vec<u8>,
    key: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
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
            for _ in v.as_bytes() {
                work.step()?;
            }
            work.flush()?;
            out.extend_from_slice(v.as_bytes());
            work.flush()?;
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
            for _ in text.as_bytes() {
                work.step()?;
            }
            work.flush()?;
            out.extend_from_slice(text.as_bytes());
            work.flush()?;
        }
        AggScalarValue::Binary(v) => {
            out.push(12);
            let len = v.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            for _ in v {
                work.step()?;
            }
            work.flush()?;
            out.extend_from_slice(v);
            work.flush()?;
        }
        AggScalarValue::Struct(items) => {
            out.push(8);
            let len = items.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            for item in items {
                work.step()?;
                match item {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v, work)?;
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
                work.step()?;
                match k {
                    Some(k) => {
                        out.push(1);
                        encode_scalar(out, k, work)?;
                    }
                    None => out.push(0),
                }
                match v {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v, work)?;
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
                work.step()?;
                match item {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v, work)?;
                    }
                    None => out.push(0),
                }
            }
        }
    }
    Ok(())
}
