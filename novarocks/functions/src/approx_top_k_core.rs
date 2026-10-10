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
//! Original tracked ApproxTopK state, codec, directional count update and output.
//! The allocator is supplied by its existing consumer; no runtime context is read.
use crate::aggregate_scalar::{
    self as scalar, AggScalarValue, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
};
use crate::aggregate_scalar_fingerprint;
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{Array, ArrayRef, ListArray, StructArray, new_null_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use hashbrown::{HashMap, hash_map::DefaultHashBuilder};
use std::{cmp::Ordering, sync::Arc};
pub const DEFAULT_K: usize = 5;
pub const MAX_COUNTER_NUM: usize = 100_000;

pub struct TopKEntry<A: ScalarStateAllocator> {
    pub value: Option<TrackedAggScalarValue<A>>,
    pub count: i64,
}

pub struct TrackedDecodedTopK<A: ScalarStateAllocator> {
    pub k: usize,
    pub counter_num: usize,
    pub entries: ScalarVec<TopKEntry<A>, A>,
}

pub struct ApproxTopKState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub initialized: bool,
    pub k: usize,
    pub counter_num: usize,
    pub counts: HashMap<ScalarVec<u8, A>, TopKEntry<A>, DefaultHashBuilder, A>,
}

impl<A: ScalarStateAllocator> ApproxTopKState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            counts: HashMap::with_hasher_in(DefaultHashBuilder::default(), allocator.clone()),
            allocator,
            initialized: false,
            k: DEFAULT_K,
            counter_num: default_counter_num(DEFAULT_K),
        }
    }
}

fn default_counter_num(k: usize) -> usize {
    (2 * k).clamp(100, MAX_COUNTER_NUM)
}

fn clamp_k(v: i64) -> Option<usize> {
    if v <= 0 {
        return None;
    }
    let v = usize::try_from(v).ok()?;
    if v == 0 || v > MAX_COUNTER_NUM {
        return None;
    }
    Some(v)
}

fn clamp_counter_num(v: i64, k: usize) -> Option<usize> {
    if v <= 0 {
        return None;
    }
    let v = usize::try_from(v).ok()?;
    if v == 0 || v > MAX_COUNTER_NUM {
        return None;
    }
    Some(v.max(k))
}

fn scalar_to_i64(value: &Option<AggScalarValue>) -> Option<i64> {
    match value {
        Some(AggScalarValue::Int64(v)) => Some(*v),
        Some(AggScalarValue::Float64(v)) => Some(*v as i64),
        Some(AggScalarValue::Decimal128(v)) => i64::try_from(*v).ok(),
        Some(AggScalarValue::Decimal256(v)) => v.to_string().parse::<i64>().ok(),
        _ => None,
    }
}

fn encode_optional_scalar(
    value: &Option<AggScalarValue>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, TopKFailure> {
    fn encode_into(
        buf: &mut Vec<u8>,
        value: &AggScalarValue,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), TopKFailure> {
        work.step()?;
        match value {
            AggScalarValue::Bool(v) => {
                buf.push(1);
                buf.push(*v as u8);
            }
            AggScalarValue::Int64(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Float64(v) => {
                buf.push(3);
                buf.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            AggScalarValue::Utf8(v) => {
                buf.push(4);
                let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            AggScalarValue::Date32(v) => {
                buf.push(5);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Timestamp(v) => {
                buf.push(6);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Decimal128(v) => {
                buf.push(7);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Decimal256(v) => {
                buf.push(11);
                let text = v.to_string();
                let len = u32::try_from(text.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            AggScalarValue::Binary(v) => {
                buf.push(12);
                let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(v);
            }
            AggScalarValue::Struct(items) => {
                buf.push(8);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v, work)?;
                        }
                        None => buf.push(0),
                    }
                }
            }
            AggScalarValue::Map(items) => {
                buf.push(9);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for (k, v) in items {
                    match k {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v, work)?;
                        }
                        None => buf.push(0),
                    }
                    match v {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v, work)?;
                        }
                        None => buf.push(0),
                    }
                }
            }
            AggScalarValue::List(items) => {
                buf.push(10);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v, work)?;
                        }
                        None => buf.push(0),
                    }
                }
            }
        }
        Ok(())
    }

    let mut out = Vec::new();
    match value {
        Some(v) => {
            out.push(1);
            encode_into(&mut out, v, work)?;
        }
        None => out.push(0),
    }
    Ok(out)
}

fn aggregate_vec_with_capacity<T, A: ScalarStateAllocator>(
    allocator: &A,
    capacity: usize,
    operation: &str,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<T, A>, TopKFailure> {
    scalar::aggregate_vec_with_capacity(allocator, capacity, operation, work)
        .map_err(TopKFailure::Scalar)
}
fn aggregate_bytes<A: ScalarStateAllocator>(
    allocator: A,
    bytes: &[u8],
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, TopKFailure> {
    scalar::scalar_bytes(allocator, bytes, work).map_err(TopKFailure::Scalar)
}
fn tracked_optional_key_fingerprint<A: ScalarStateAllocator>(
    value: &Option<TrackedAggScalarValue<A>>,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, TopKFailure> {
    aggregate_scalar_fingerprint::tracked_optional_key_fingerprint(value, allocator, work)
        .map_err(TopKFailure::Scalar)
}
fn tracked_scalar_from_array<A: ScalarStateAllocator>(
    array: &ArrayRef,
    row: usize,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<TrackedAggScalarValue<A>>, TopKFailure> {
    scalar::tracked_scalar_from_array(array, row, allocator, work).map_err(TopKFailure::Scalar)
}
fn tracked_scalar_to_output<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<AggScalarValue, TopKFailure> {
    scalar::tracked_scalar_to_output(value, work).map_err(TopKFailure::Scalar)
}
fn scalar_from_array(
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<AggScalarValue>, TopKFailure> {
    scalar::scalar_from_array(array, row, work).map_err(TopKFailure::Scalar)
}
fn build_scalar_array(
    ty: &DataType,
    values: Vec<Option<AggScalarValue>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, TopKFailure> {
    scalar::build_scalar_array(ty, values, work).map_err(TopKFailure::Scalar)
}
pub fn decode_optional_scalar_tracked_observed<A: ScalarStateAllocator>(
    bytes: &[u8],
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<TrackedAggScalarValue<A>>, TopKFailure> {
    fn need(bytes: &[u8], pos: usize, len: usize, label: &str) -> Result<(), TopKFailure> {
        if pos.checked_add(len).is_none_or(|end| end > bytes.len()) {
            Err(format!("approx_top_k decode {label}: buffer too short").into())
        } else {
            Ok(())
        }
    }
    fn read_u32(bytes: &[u8], pos: &mut usize, label: &str) -> Result<usize, TopKFailure> {
        need(bytes, *pos, 4, label)?;
        let value = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        Ok(value)
    }
    fn decode_optional<A: ScalarStateAllocator>(
        bytes: &[u8],
        pos: &mut usize,
        allocator: &A,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<TrackedAggScalarValue<A>>, TopKFailure> {
        need(bytes, *pos, 1, "optional marker")?;
        let present = bytes[*pos];
        *pos += 1;
        match present {
            0 => Ok(None),
            1 => decode_value(bytes, pos, allocator, work).map(Some),
            _ => Err("approx_top_k decode: invalid optional marker"
                .to_string()
                .into()),
        }
    }
    fn decode_values<A: ScalarStateAllocator>(
        bytes: &[u8],
        pos: &mut usize,
        allocator: &A,
        label: &str,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<ScalarVec<Option<TrackedAggScalarValue<A>>, A>, TopKFailure> {
        let len = read_u32(bytes, pos, label)?;
        let mut values = aggregate_vec_with_capacity(
            allocator,
            len,
            "reserve approx_top_k nested scalar",
            work,
        )?;
        for _ in 0..len {
            work.step()?;
            values.push(decode_optional(bytes, pos, allocator, work)?);
        }
        Ok(values)
    }
    fn decode_value<A: ScalarStateAllocator>(
        bytes: &[u8],
        pos: &mut usize,
        allocator: &A,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<TrackedAggScalarValue<A>, TopKFailure> {
        need(bytes, *pos, 1, "tag")?;
        let tag = bytes[*pos];
        *pos += 1;
        macro_rules! fixed {
            ($len:expr, $label:literal, $ctor:expr) => {{
                need(bytes, *pos, $len, $label)?;
                let raw: [u8; $len] = bytes[*pos..*pos + $len].try_into().unwrap();
                *pos += $len;
                $ctor(raw)
            }};
        }
        Ok(match tag {
            1 => {
                need(bytes, *pos, 1, "bool")?;
                let value = bytes[*pos] != 0;
                *pos += 1;
                TrackedAggScalarValue::Bool(value)
            }
            2 => fixed!(8, "int64", |raw| TrackedAggScalarValue::Int64(
                i64::from_le_bytes(raw)
            )),
            3 => fixed!(8, "float64", |raw| TrackedAggScalarValue::Float64(
                f64::from_bits(u64::from_le_bytes(raw))
            )),
            4 | 12 => {
                let len = read_u32(bytes, pos, "byte length")?;
                need(bytes, *pos, len, "byte payload")?;
                let value = aggregate_bytes(allocator.clone(), &bytes[*pos..*pos + len], work)?;
                *pos += len;
                if tag == 4 {
                    std::str::from_utf8(&value)
                        .map_err(|error| format!("approx_top_k decode utf8: {error}"))?;
                    TrackedAggScalarValue::Utf8(value)
                } else {
                    TrackedAggScalarValue::Binary(value)
                }
            }
            5 => fixed!(4, "date32", |raw| TrackedAggScalarValue::Date32(
                i32::from_le_bytes(raw)
            )),
            6 => fixed!(8, "timestamp", |raw| TrackedAggScalarValue::Timestamp(
                i64::from_le_bytes(raw)
            )),
            7 => fixed!(16, "decimal128", |raw| {
                TrackedAggScalarValue::Decimal128(i128::from_le_bytes(raw))
            }),
            8 => TrackedAggScalarValue::Struct(decode_values(
                bytes,
                pos,
                allocator,
                "struct length",
                work,
            )?),
            9 => {
                let len = read_u32(bytes, pos, "map length")?;
                let mut entries = aggregate_vec_with_capacity(
                    allocator,
                    len,
                    "reserve approx_top_k map scalar",
                    work,
                )?;
                for _ in 0..len {
                    work.step()?;
                    entries.push((
                        decode_optional(bytes, pos, allocator, work)?,
                        decode_optional(bytes, pos, allocator, work)?,
                    ));
                }
                TrackedAggScalarValue::Map(entries)
            }
            10 => TrackedAggScalarValue::List(decode_values(
                bytes,
                pos,
                allocator,
                "list length",
                work,
            )?),
            11 => {
                let len = read_u32(bytes, pos, "decimal256 length")?;
                need(bytes, *pos, len, "decimal256 payload")?;
                let text = std::str::from_utf8(&bytes[*pos..*pos + len])
                    .map_err(|error| format!("approx_top_k decode decimal256: {error}"))?;
                *pos += len;
                TrackedAggScalarValue::Decimal256(
                    text.parse()
                        .map_err(|error| format!("approx_top_k decode decimal256: {error}"))?,
                )
            }
            other => return Err(format!("approx_top_k decode: unknown tag {other}").into()),
        })
    }

    let mut pos = 0;
    let value = decode_optional(bytes, &mut pos, allocator, work)?;
    if pos != bytes.len() {
        return Err("approx_top_k decode: trailing bytes".to_string().into());
    }
    Ok(value)
}

pub fn ensure_initialized<A: ScalarStateAllocator>(state: &mut ApproxTopKState<A>) {
    if !state.initialized {
        state.initialized = true;
        state.k = DEFAULT_K;
        state.counter_num = default_counter_num(DEFAULT_K);
    }
}

pub fn evict_one_counter_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<i64, TopKFailure> {
    work.flush()?;
    let victim = state
        .counts
        .iter()
        .min_by(|(left_key, left), (right_key, right)| {
            left.count
                .cmp(&right.count)
                .then_with(|| left_key.cmp(right_key))
        })
        .map(|(key, entry)| (key as *const ScalarVec<u8, A> as usize, entry.count));
    let Some((key_address, count)) = victim else {
        return Ok(0);
    };
    state
        .counts
        .retain(|key, _| key as *const ScalarVec<u8, A> as usize != key_address);
    work.flush()?;
    Ok(count)
}

pub fn enforce_counter_limit_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), TopKFailure> {
    while state.counts.len() > state.counter_num {
        evict_one_counter_observed(state, work)?;
    }
    Ok(())
}

pub fn update_one_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    value: Option<TrackedAggScalarValue<A>>,
    count: i64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), TopKFailure> {
    if count <= 0 {
        return Ok(());
    }
    ensure_initialized(state);
    let key = tracked_optional_key_fingerprint(&value, &state.allocator, work)?;
    if let Some(entry) = state.counts.get_mut(&key) {
        entry.count = entry.count.saturating_add(count);
        return Ok(());
    }
    let inherited = if state.counts.len() >= state.counter_num {
        evict_one_counter_observed(state, work)?
    } else {
        0
    };
    state.counts.try_reserve(1).map_err(|_| {
        state
            .allocator
            .scalar_allocation_error("reserve approx_top_k counter")
    })?;
    state.counts.insert(
        key,
        TopKEntry {
            value,
            count: inherited.saturating_add(count),
        },
    );
    Ok(())
}

pub fn serialize_state_observed<A: ScalarStateAllocator>(
    state: &ApproxTopKState<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, TopKFailure> {
    let mut out = Vec::new();
    let k = u32::try_from(state.k).unwrap_or(DEFAULT_K as u32);
    let counter_num = u32::try_from(state.counter_num).unwrap_or(MAX_COUNTER_NUM as u32);
    out.extend_from_slice(&k.to_le_bytes());
    out.extend_from_slice(&counter_num.to_le_bytes());
    let count = u32::try_from(state.counts.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for entry in state.counts.values() {
        work.step()?;
        let value = entry
            .value
            .as_ref()
            .map(|value| tracked_scalar_to_output(value, work))
            .transpose()?;
        let value_bytes = encode_optional_scalar(&value, work)?;
        let len = u32::try_from(value_bytes.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&value_bytes);
        out.extend_from_slice(&entry.count.to_le_bytes());
    }
    Ok(out)
}

pub fn deserialize_state_tracked_observed<A: ScalarStateAllocator>(
    bytes: &[u8],
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<TrackedDecodedTopK<A>, TopKFailure> {
    if bytes.len() < 12 {
        return Err("approx_top_k merge payload too short".to_string().into());
    }
    let k = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let counter_num = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let entry_num = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if entry_num > MAX_COUNTER_NUM {
        return Err(format!(
            "approx_top_k merge entry count {entry_num} exceeds {MAX_COUNTER_NUM}"
        )
        .into());
    }
    let mut entries = aggregate_vec_with_capacity(
        allocator,
        entry_num,
        "reserve approx_top_k decoded entries",
        work,
    )?;
    let mut pos = 12usize;
    for _ in 0..entry_num {
        work.step()?;
        let value_len_end = pos
            .checked_add(4)
            .ok_or_else(|| "approx_top_k decode entry length overflow".to_string())?;
        let value_len_bytes = bytes
            .get(pos..value_len_end)
            .ok_or_else(|| "approx_top_k decode entry len failed".to_string())?;
        let value_len = u32::from_le_bytes(value_len_bytes.try_into().unwrap()) as usize;
        pos = value_len_end;
        let value_end = pos
            .checked_add(value_len)
            .ok_or_else(|| "approx_top_k decode value length overflow".to_string())?;
        let count_end = value_end
            .checked_add(8)
            .ok_or_else(|| "approx_top_k decode count offset overflow".to_string())?;
        let value_bytes = bytes
            .get(pos..value_end)
            .ok_or_else(|| "approx_top_k decode value out of bounds".to_string())?;
        let count_bytes = bytes
            .get(value_end..count_end)
            .ok_or_else(|| "approx_top_k decode count out of bounds".to_string())?;
        entries.push(TopKEntry {
            value: decode_optional_scalar_tracked_observed(value_bytes, allocator, work)?,
            count: i64::from_le_bytes(count_bytes.try_into().unwrap()),
        });
        pos = count_end;
    }
    if pos != bytes.len() {
        return Err("approx_top_k decode trailing bytes".to_string().into());
    }
    Ok(TrackedDecodedTopK {
        k,
        counter_num,
        entries,
    })
}

pub fn sorted_top_entries_observed<'a, A: ScalarStateAllocator>(
    state: &'a ApproxTopKState<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<(&'a ScalarVec<u8, A>, &'a TopKEntry<A>)>, TopKFailure> {
    work.flush()?;
    let mut entries = state.counts.iter().collect::<Vec<_>>();
    entries.sort_by(|(lk, lv), (rk, rv)| match rv.count.cmp(&lv.count) {
        Ordering::Equal => lk.cmp(rk),
        other => other,
    });
    if entries.len() > state.k {
        entries.truncate(state.k);
    }
    work.flush()?;
    Ok(entries)
}

pub fn output_topk_array_observed<'a, A: ScalarStateAllocator>(
    output_type: &DataType,
    group_states: impl ExactSizeIterator<Item = &'a ApproxTopKState<A>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, TopKFailure> {
    let DataType::List(list_field) = output_type else {
        return Err(format!(
            "approx_top_k output type must be LIST<STRUCT>, got {:?}",
            output_type
        )
        .into());
    };
    let DataType::Struct(struct_fields) = list_field.data_type() else {
        return Err(format!(
            "approx_top_k list element type must be STRUCT, got {:?}",
            list_field.data_type()
        )
        .into());
    };
    if struct_fields.len() < 2 {
        return Err("approx_top_k output struct must have at least 2 fields"
            .to_string()
            .into());
    }

    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut items = Vec::new();
    let mut counts = Vec::new();
    let mut extras: Vec<Vec<Option<AggScalarValue>>> =
        (2..struct_fields.len()).map(|_| Vec::new()).collect();

    for state in group_states {
        work.step()?;
        let top_entries = sorted_top_entries_observed(state, work)?;
        current += top_entries.len() as i64;
        if current > i32::MAX as i64 {
            return Err("approx_top_k output offset overflow".to_string().into());
        }
        offsets.push(current as i32);
        for (_key, entry) in top_entries {
            work.step()?;
            items.push(
                entry
                    .value
                    .as_ref()
                    .map(|value| tracked_scalar_to_output(value, work))
                    .transpose()?,
            );
            counts.push(Some(AggScalarValue::Int64(entry.count)));
            for extra in &mut extras {
                extra.push(None);
            }
        }
    }

    let item_array = build_scalar_array(struct_fields[0].data_type(), items, work)?;
    let count_array = build_scalar_array(struct_fields[1].data_type(), counts, work)?;
    let mut struct_columns = vec![item_array, count_array];
    for (idx, field) in struct_fields.iter().enumerate().skip(2) {
        let values = std::mem::take(&mut extras[idx - 2]);
        if values.is_empty() {
            struct_columns.push(new_null_array(field.data_type(), 0));
        } else {
            struct_columns.push(build_scalar_array(field.data_type(), values, work)?);
        }
    }
    let struct_array = StructArray::new(struct_fields.clone(), struct_columns, None);
    let list_array = ListArray::new(
        list_field.clone(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        None,
    );
    Ok(Arc::new(list_array))
}

/// The original pre-row setup check, shared even for zero selected rows.
#[derive(Clone, Copy, Debug)]
pub enum TopKInputFailure {
    EmptyStruct,
}
impl std::fmt::Display for TopKInputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyStruct => {
                f.write_str("approx_top_k struct input must have at least 1 field")
            }
        }
    }
}
pub fn update_input_failure(array: &ArrayRef) -> Option<TopKInputFailure> {
    if let Some(array) = array.as_any().downcast_ref::<StructArray>() {
        if array.columns().is_empty() {
            return Some(TopKInputFailure::EmptyStruct);
        }
    }
    None
}
pub fn check_update_array(array: &ArrayRef) -> Result<(), String> {
    match update_input_failure(array) {
        Some(failure) => Err(failure.to_string()),
        None => Ok(()),
    }
}
#[derive(Clone, Copy)]
pub struct TopKArgument<'a> {
    pub array: &'a ArrayRef,
    pub row: usize,
}
fn update_packed_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    value: TopKArgument<'_>,
    k_argument: Option<TopKArgument<'_>>,
    counter_argument: Option<TopKArgument<'_>>,
    root_is_null: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), TopKFailure> {
    ensure_initialized(state);
    if let Some(argument) = k_argument {
        let k = scalar_to_i64(&scalar_from_array(argument.array, argument.row, work)?)
            .and_then(clamp_k);
        if let Some(k) = k {
            state.k = k;
            state.counter_num = state.counter_num.max(k);
        }
    }
    if let Some(argument) = counter_argument {
        let counter = scalar_to_i64(&scalar_from_array(argument.array, argument.row, work)?)
            .and_then(|v| clamp_counter_num(v, state.k));
        if let Some(counter) = counter {
            state.counter_num = counter;
            enforce_counter_limit_observed(state, work)?;
        }
    } else {
        state.counter_num = state.counter_num.max(default_counter_num(state.k));
    }
    let value = if root_is_null {
        None
    } else {
        tracked_scalar_from_array(value.array, value.row, &state.allocator, work)?
    };
    update_one_observed(state, value, 1, work)?;
    Ok(())
}
pub fn update_row_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), TopKFailure> {
    if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
        let cols = struct_arr.columns();
        check_update_array(array)?;
        return update_packed_observed(
            state,
            TopKArgument {
                array: &cols[0],
                row,
            },
            cols.get(1).map(|array| TopKArgument { array, row }),
            cols.get(2).map(|array| TopKArgument { array, row }),
            struct_arr.is_null(row),
            work,
        );
    }
    ensure_initialized(state);
    state.counter_num = state.counter_num.max(default_counter_num(state.k));
    let value = tracked_scalar_from_array(array, row, &state.allocator, work)?;
    update_one_observed(state, value, 1, work)
}
pub fn update_arguments_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    arguments: &[TopKArgument<'_>],
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), TopKFailure> {
    if arguments.len() == 1 {
        return update_row_observed(state, arguments[0].array, arguments[0].row, work);
    }
    update_packed_observed(
        state,
        arguments[0],
        arguments.get(1).copied(),
        arguments.get(2).copied(),
        false,
        work,
    )
}
pub fn merge_payload_observed<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    payload: &[u8],
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), TopKFailure> {
    let decoded = deserialize_state_tracked_observed(payload, &state.allocator, work)?;
    ensure_initialized(state);
    let has_entries = !decoded.entries.is_empty();
    if has_entries {
        if decoded.k > 0 {
            state.k = decoded.k.min(MAX_COUNTER_NUM);
        }
        if decoded.counter_num > 0 {
            state.counter_num = decoded.counter_num.max(state.k).min(MAX_COUNTER_NUM);
            enforce_counter_limit_observed(state, work)?;
        }
    }
    for entry in decoded.entries {
        work.step()?;
        update_one_observed(state, entry.value, entry.count, work)?;
    }
    enforce_counter_limit_observed(state, work)?;
    Ok(())
}

#[derive(Debug)]
pub enum TopKFailure {
    Original(String),
    Scalar(scalar::ScalarStateError),
}
impl From<String> for TopKFailure {
    fn from(v: String) -> Self {
        Self::Original(v)
    }
}
impl From<scalar::ScalarStateError> for TopKFailure {
    fn from(v: scalar::ScalarStateError) -> Self {
        Self::Scalar(v)
    }
}
impl std::fmt::Display for TopKFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Original(v) => v.fmt(f),
            Self::Scalar(v) => v.fmt(f),
        }
    }
}
impl std::error::Error for TopKFailure {}
pub fn decode_optional_scalar_tracked<A: ScalarStateAllocator>(
    bytes: &[u8],
    allocator: &A,
) -> Result<Option<TrackedAggScalarValue<A>>, String> {
    decode_optional_scalar_tracked_observed(bytes, allocator, &mut ScalarWork::new(None))
        .map_err(|e| e.to_string())
}
pub fn evict_one_counter<A: ScalarStateAllocator>(state: &mut ApproxTopKState<A>) -> i64 {
    evict_one_counter_observed(state, &mut ScalarWork::new(None))
        .expect("tracked approx_top_k value must materialize")
}
pub fn enforce_counter_limit<A: ScalarStateAllocator>(state: &mut ApproxTopKState<A>) -> () {
    enforce_counter_limit_observed(state, &mut ScalarWork::new(None))
        .expect("tracked approx_top_k value must materialize")
}
pub fn update_one<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    value: Option<TrackedAggScalarValue<A>>,
    count: i64,
) -> Result<(), String> {
    update_one_observed(state, value, count, &mut ScalarWork::new(None)).map_err(|e| e.to_string())
}
pub fn serialize_state<A: ScalarStateAllocator>(state: &ApproxTopKState<A>) -> Vec<u8> {
    serialize_state_observed(state, &mut ScalarWork::new(None))
        .map_err(|error| error.to_string())
        .expect("tracked approx_top_k value must materialize")
}
pub fn deserialize_state_tracked<A: ScalarStateAllocator>(
    bytes: &[u8],
    allocator: &A,
) -> Result<TrackedDecodedTopK<A>, String> {
    deserialize_state_tracked_observed(bytes, allocator, &mut ScalarWork::new(None))
        .map_err(|e| e.to_string())
}
pub fn sorted_top_entries<A: ScalarStateAllocator>(
    state: &ApproxTopKState<A>,
) -> Vec<(&ScalarVec<u8, A>, &TopKEntry<A>)> {
    sorted_top_entries_observed(state, &mut ScalarWork::new(None))
        .expect("tracked approx_top_k value must materialize")
}
pub fn output_topk_array<'a, A: ScalarStateAllocator>(
    output_type: &DataType,
    group_states: impl ExactSizeIterator<Item = &'a ApproxTopKState<A>>,
) -> Result<ArrayRef, String> {
    output_topk_array_observed(output_type, group_states, &mut ScalarWork::new(None))
        .map_err(|e| e.to_string())
}
pub fn update_row<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    array: &ArrayRef,
    row: usize,
) -> Result<(), String> {
    update_row_observed(state, array, row, &mut ScalarWork::new(None)).map_err(|e| e.to_string())
}
pub fn merge_payload<A: ScalarStateAllocator>(
    state: &mut ApproxTopKState<A>,
    payload: &[u8],
) -> Result<(), String> {
    merge_payload_observed(state, payload, &mut ScalarWork::new(None)).map_err(|e| e.to_string())
}
