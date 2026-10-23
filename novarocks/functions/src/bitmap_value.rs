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

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

use roaring::RoaringBitmap;
#[path = "bitmap_failure.rs"]
mod failure;
pub(crate) use failure::TypedBitmapPort;
use failure::bitmap_wire_count;
pub use failure::{
    BitmapDataFailure, BitmapOuterFailure, BitmapOutputTally, BitmapValueFailure,
    ClassifiedBitmapFailure,
};

pub const BITMAP_TYPE_EMPTY: u8 = 0;
pub const BITMAP_TYPE_SINGLE32: u8 = 1;
pub const BITMAP_TYPE_BITMAP32: u8 = 2;
pub const BITMAP_TYPE_SINGLE64: u8 = 3;
pub const BITMAP_TYPE_BITMAP64: u8 = 4;
pub const BITMAP_TYPE_SET: u8 = 10;
pub const BITMAP_TYPE_BITMAP32_SERIV2: u8 = 12;
pub const BITMAP_TYPE_BITMAP64_SERIV2: u8 = 13;

const ROARING_COOKIE_NO_RUNCONTAINER: u32 = 12_346; // 0x303A
const ROARING_COOKIE_RUNCONTAINER: u16 = 12_347; // 0x303B

pub fn encode_varint_u64(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub trait BitmapDecodePort {
    type Error: std::fmt::Display;
    fn failure(&mut self, source: BitmapValueFailure) -> Self::Error;
    fn data(&mut self, message: std::fmt::Arguments<'_>) -> Self::Error {
        self.failure(BitmapValueFailure::InvalidEncoding(
            novarocks_failure_contract::BoundedDetail::truncating_display(message),
        ))
    }
    fn is_data(error: &Self::Error) -> bool;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn boundary(&mut self) -> Result<(), Self::Error>;
    fn before_render(&mut self, entries: usize) -> Result<(), Self::Error>;
    fn before_tree_insert(&mut self, existing: usize) -> Result<(), Self::Error>;
    fn before_tree_collection(&mut self, entries: usize) -> Result<(), Self::Error>;
    fn before_roaring(&mut self, payload_bytes: usize) -> Result<(), Self::Error>;
    fn before_u32_collection(&mut self, entries: u64) -> Result<(), Self::Error>;
}
pub fn decode_varint_u64(bytes: &[u8]) -> Result<(u64, usize), BitmapValueFailure> {
    decode_varint_u64_with_port(bytes, &mut TypedBitmapPort)
}
pub fn parse_bitmap_string(text: &str) -> Result<BTreeSet<u64>, BitmapValueFailure> {
    parse_bitmap_string_with_port(text, &mut TypedBitmapPort)
}
pub fn decode_internal_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, BitmapValueFailure> {
    decode_internal_bitmap_with_port(bytes, &mut TypedBitmapPort)
}
pub fn decode_external_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, BitmapValueFailure> {
    decode_external_bitmap_with_port(bytes, &mut TypedBitmapPort)
}
pub fn decode_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, BitmapValueFailure> {
    decode_bitmap_with_port(bytes, &mut TypedBitmapPort)
}
pub fn decode_varint_u64_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<(u64, usize), P::Error> {
    let mut out = 0u64;
    let mut shift = 0u32;
    for (idx, byte) in bytes.iter().enumerate() {
        port.step()?;
        out |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok((out, idx + 1));
        }
        shift += 7;
        if shift > 63 {
            return Err(port.data(format_args!("bitmap decode varint overflow")));
        }
    }
    Err(port.data(format_args!("bitmap decode varint reached end of payload")))
}

pub fn parse_bitmap_string_with_port<P: BitmapDecodePort>(
    text: &str,
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    port.boundary()?;
    let trimmed = text.trim();
    port.boundary()?;
    if trimmed.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut out = BTreeSet::new();
    for part in trimmed.split(',') {
        port.step()?;
        port.boundary()?;
        let token = part.trim();
        port.boundary()?;
        if token.is_empty() {
            continue;
        }
        port.boundary()?;
        let value = token.parse::<u64>().map_err(|_| {
            port.data(format_args!(
                "bitmap string contains invalid value: {}",
                token
            ))
        })?;
        port.boundary()?;
        port.before_tree_insert(out.len())?;
        out.insert(value);
    }
    Ok(out)
}

pub fn parse_bitmap_text_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    port.boundary()?;
    let text = std::str::from_utf8(bytes)
        .map_err(|_| port.data(format_args!("bitmap payload is not utf8 text")))?;
    port.boundary()?;
    parse_bitmap_string_with_port(text, port)
}

pub fn decode_internal_bitmap_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    match bytes[0] {
        BITMAP_TYPE_EMPTY => {
            if bytes.len() != 1 {
                return Err(port.data(format_args!(
                    "bitmap internal EMPTY payload length mismatch: expected=1 actual={}",
                    bytes.len()
                )));
            }
            Ok(BTreeSet::new())
        }
        BITMAP_TYPE_SINGLE32 => {
            if bytes.len() != 5 {
                return Err(port.data(format_args!(
                    "bitmap internal SINGLE32 payload length mismatch: expected=5 actual={}",
                    bytes.len()
                )));
            }
            let value = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
            );
            {
                port.before_tree_insert(0)?;
                Ok(BTreeSet::from([u64::from(value)]))
            }
        }
        BITMAP_TYPE_SINGLE64 => {
            if bytes.len() != 9 {
                return Err(port.data(format_args!(
                    "bitmap internal SINGLE64 payload length mismatch: expected=9 actual={}",
                    bytes.len()
                )));
            }
            let value = u64::from_le_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
            );
            {
                port.before_tree_insert(0)?;
                Ok(BTreeSet::from([value]))
            }
        }
        BITMAP_TYPE_SET => {
            if bytes.len() < 5 {
                return Err(port.data(format_args!(
                    "bitmap internal SET payload too short: actual={}",
                    bytes.len()
                )));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
            ) as usize;
            let mut offset = 5usize;
            let mut values = BTreeSet::new();
            for idx in 0..count {
                port.step()?;
                let (value, consumed) = decode_varint_u64_with_port(&bytes[offset..], port)?;
                if consumed == 0 {
                    return Err(port.data(format_args!(
                        "bitmap internal decode SET consumed zero bytes at entry {}",
                        idx
                    )));
                }
                offset = offset.saturating_add(consumed);
                if offset > bytes.len() {
                    return Err(port.data(format_args!(
                        "bitmap internal SET payload overflow: offset={} len={}",
                        offset,
                        bytes.len()
                    )));
                }
                port.before_tree_insert(values.len())?;
                values.insert(value);
            }
            if offset != bytes.len() {
                return Err(port.data(format_args!(
                    "bitmap internal SET payload has trailing bytes: offset={} len={}",
                    offset,
                    bytes.len()
                )));
            }
            Ok(values)
        }
        _ => Err(port.data(format_args!(
            "bitmap internal unsupported payload type code: {}",
            bytes[0]
        ))),
    }
}

pub fn decode_roaring32_payload_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<(Vec<u32>, usize), P::Error> {
    if bytes.is_empty() {
        return Err(port.data(format_args!("bitmap roaring32 payload is empty")));
    }
    let mut cursor = Cursor::new(bytes);
    port.before_roaring(bytes.len())?;
    let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
        .map_err(|source| port.failure(BitmapValueFailure::DecodeIo(source)))?;
    port.boundary()?;
    let consumed = usize::try_from(cursor.position())
        .map_err(|source| port.failure(BitmapValueFailure::Position(source)))?;
    if consumed == 0 || consumed > bytes.len() {
        return Err(port.data(format_args!(
            "bitmap decode roaring32 payload consumed invalid size: consumed={} len={}",
            consumed,
            bytes.len()
        )));
    }
    port.before_u32_collection(bitmap.len())?;
    let values = bitmap.iter().collect();
    port.boundary()?;
    Ok((values, consumed))
}

pub fn decode_external_bitmap32_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    if bytes.len() <= 1 {
        return Err(port.data(format_args!("bitmap external BITMAP32 payload is empty")));
    }
    let (values, _) = decode_roaring32_payload_with_port(&bytes[1..], port)?;
    port.before_tree_collection(values.len())?;
    let tree = values.into_iter().map(u64::from).collect();
    port.boundary()?;
    Ok(tree)
}

pub fn decode_external_bitmap64_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    if bytes.len() <= 1 {
        return Err(port.data(format_args!("bitmap external BITMAP64 payload is empty")));
    }
    let (map_size, consumed) = decode_varint_u64_with_port(&bytes[1..], port)?;
    let mut offset = 1usize.saturating_add(consumed);
    let mut out = BTreeSet::new();
    for idx in 0..map_size {
        port.step()?;
        if offset.saturating_add(4) > bytes.len() {
            return Err(port.data(format_args!(
                "bitmap external BITMAP64 map key overflow at entry {}: offset={} len={}",
                idx,
                offset,
                bytes.len()
            )));
        }
        let high = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
        );
        offset += 4;
        let (values, used) = decode_roaring32_payload_with_port(&bytes[offset..], port)?;
        offset = offset.saturating_add(used);
        if offset > bytes.len() {
            return Err(port.data(format_args!(
                "bitmap external BITMAP64 payload overflow at entry {}: offset={} len={}",
                idx,
                offset,
                bytes.len()
            )));
        }
        for low in values {
            port.step()?;
            port.before_tree_insert(out.len())?;
            out.insert((u64::from(high) << 32) | u64::from(low));
        }
    }
    Ok(out)
}

pub fn decode_external_bitmap_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    if bytes.is_empty() {
        return Err(port.data(format_args!("bitmap external payload is empty")));
    }
    match bytes[0] {
        BITMAP_TYPE_EMPTY => Ok(BTreeSet::new()),
        BITMAP_TYPE_SINGLE32 => {
            if bytes.len() < 5 {
                return Err(port.data(format_args!(
                    "bitmap external SINGLE32 payload too short: actual={}",
                    bytes.len()
                )));
            }
            let value = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
            );
            {
                port.before_tree_insert(0)?;
                Ok(BTreeSet::from([u64::from(value)]))
            }
        }
        BITMAP_TYPE_SINGLE64 => {
            if bytes.len() < 9 {
                return Err(port.data(format_args!(
                    "bitmap external SINGLE64 payload too short: actual={}",
                    bytes.len()
                )));
            }
            let value = u64::from_le_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
            );
            {
                port.before_tree_insert(0)?;
                Ok(BTreeSet::from([value]))
            }
        }
        BITMAP_TYPE_SET => {
            if bytes.len() < 5 {
                return Err(port.data(format_args!(
                    "bitmap external SET payload too short: actual={}",
                    bytes.len()
                )));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
            ) as usize;
            let required = 5usize.saturating_add(count.saturating_mul(8));
            if bytes.len() < required {
                return Err(port.data(format_args!(
                    "bitmap external SET payload too short: required={} actual={}",
                    required,
                    bytes.len()
                )));
            }
            let mut out = BTreeSet::new();
            let mut offset = 5usize;
            for _ in 0..count {
                port.step()?;
                let value = u64::from_le_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .map_err(|source| port.failure(BitmapValueFailure::Width(source)))?,
                );
                offset += 8;
                port.before_tree_insert(out.len())?;
                out.insert(value);
            }
            Ok(out)
        }
        BITMAP_TYPE_BITMAP32 | BITMAP_TYPE_BITMAP32_SERIV2 => {
            decode_external_bitmap32_with_port(bytes, port)
        }
        BITMAP_TYPE_BITMAP64 | BITMAP_TYPE_BITMAP64_SERIV2 => {
            decode_external_bitmap64_with_port(bytes, port)
        }
        _ => Err(port.data(format_args!(
            "bitmap external unsupported payload type code: {}",
            bytes[0]
        ))),
    }
}

pub fn decode_bitmap_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<BTreeSet<u64>, P::Error> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    match decode_internal_bitmap_with_port(bytes, port) {
        Ok(values) => return Ok(values),
        Err(error) if P::is_data(&error) => drop(error),
        Err(error) => return Err(error),
    }
    match decode_external_bitmap_with_port(bytes, port) {
        Ok(values) => return Ok(values),
        Err(error) if P::is_data(&error) => drop(error),
        Err(error) => return Err(error),
    }
    parse_bitmap_text_with_port(bytes, port)
}

pub fn encode_internal_bitmap(values: &BTreeSet<u64>) -> Result<Vec<u8>, BitmapValueFailure> {
    if values.is_empty() {
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let value = values
            .first()
            .copied()
            .expect("a set of length one has a first element");
        if let Ok(v32) = u32::try_from(value) {
            let mut out = Vec::with_capacity(5);
            out.push(BITMAP_TYPE_SINGLE32);
            out.extend_from_slice(&v32.to_le_bytes());
            return Ok(out);
        }
        let mut out = Vec::with_capacity(9);
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&value.to_le_bytes());
        return Ok(out);
    }

    let count = bitmap_wire_count(values.len())?;
    let mut out = Vec::new();
    out.push(BITMAP_TYPE_SET);
    out.extend_from_slice(&count.to_le_bytes());
    for value in values {
        encode_varint_u64(*value, &mut out);
    }
    Ok(out)
}

/// Encode one unsigned value using the canonical internal bitmap representation.
pub fn encode_bitmap_single(value: u64) -> Vec<u8> {
    encode_internal_bitmap(&BTreeSet::from([value]))
        .expect("a single bitmap value is always representable")
}

/// Encode a bitmap aggregate intermediate in its historical fixed-width SET
/// layout. The general decoder accepts this alongside the compact SQL form.
pub trait BitmapAggregateEncodePort: BitmapDecodePort {
    fn before_aggregate_buffer(&mut self, bytes: usize) -> Result<(), Self::Error>;
    fn before_aggregate_singleton(&mut self, value: u64) -> Result<(), Self::Error>;
}
/// Pure encoding geometry, before materializing an ordered copy or payload.
/// The optional singleton is read only when `entries == 1`.
pub fn aggregate_bitmap_byte_len(
    entries: usize,
    singleton: Option<u64>,
) -> Result<usize, BitmapValueFailure> {
    failure::bitmap_wire_count(entries)?;
    let bytes = match entries {
        0 => 1,
        1 => {
            let singleton = singleton.ok_or(BitmapValueFailure::MissingSingleton)?;
            if singleton <= u32::MAX as u64 { 5 } else { 9 }
        }
        _ => 5 + entries as u128 * 8,
    };
    if bytes > isize::MAX as u128 {
        return Err(BitmapValueFailure::StorageExtent { required: bytes });
    }
    Ok(bytes as usize)
}
pub fn encode_bitmap_aggregate(values: &BTreeSet<u64>) -> Result<Vec<u8>, BitmapValueFailure> {
    encode_bitmap_aggregate_with_port(values, &mut TypedBitmapPort)
}
pub fn encode_bitmap_aggregate_with_port<P: BitmapAggregateEncodePort>(
    values: &BTreeSet<u64>,
    port: &mut P,
) -> Result<Vec<u8>, P::Error> {
    let capacity = aggregate_bitmap_byte_len(values.len(), values.first().copied())
        .map_err(|source| port.failure(source))?;
    if values.is_empty() {
        port.before_aggregate_buffer(1)?;
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let value = values
            .first()
            .copied()
            .ok_or_else(|| port.data(format_args!("bitmap aggregate missing singleton value")))?;
        port.before_aggregate_singleton(value)?;
        return Ok(encode_bitmap_single(value));
    }

    let count = bitmap_wire_count(values.len()).map_err(|source| port.failure(source))?;
    port.before_aggregate_buffer(capacity)?;
    let mut out = Vec::with_capacity(capacity);
    out.push(BITMAP_TYPE_SET);
    out.extend_from_slice(&count.to_le_bytes());
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
        port.step()?;
    }
    Ok(out)
}

fn collect_runs_u16(values: &[u32]) -> Option<(u16, Vec<(u16, u16)>)> {
    let first = *values.first()?;
    let key = (first >> 16) as u16;
    let mut runs = Vec::new();
    let mut start = (first & 0xffff) as u16;
    let mut prev = start;

    for &value in values.iter().skip(1) {
        if ((value >> 16) as u16) != key {
            return None;
        }
        let low = (value & 0xffff) as u16;
        let is_next = prev != u16::MAX && low == prev + 1;
        if is_next {
            prev = low;
            continue;
        }
        runs.push((start, prev.wrapping_sub(start)));
        start = low;
        prev = low;
    }
    runs.push((start, prev.wrapping_sub(start)));
    Some((key, runs))
}

fn should_encode_run_container(values: &[u32]) -> bool {
    if values.len() <= 32 {
        return false;
    }
    let Some((_, runs)) = collect_runs_u16(values) else {
        return false;
    };
    runs.len() <= values.len() / 2
}

fn encode_roaring32_no_run(values: &[u32]) -> Result<Vec<u8>, BitmapValueFailure> {
    let mut bitmap = RoaringBitmap::new();
    for &value in values {
        bitmap.insert(value);
    }
    let mut out = Vec::new();
    bitmap
        .serialize_into(&mut out)
        .map_err(BitmapValueFailure::EncodeIo)?;
    Ok(out)
}

fn encode_roaring32_run_single_container(values: &[u32]) -> Option<Vec<u8>> {
    let (key, runs) = collect_runs_u16(values)?;
    if runs.len() > u16::MAX as usize {
        return None;
    }
    let cardinality_minus_one = u16::try_from(values.len().checked_sub(1)?).ok()?;
    let runs_count = u16::try_from(runs.len()).ok()?;

    let mut out = Vec::new();
    let cookie = u32::from(ROARING_COOKIE_RUNCONTAINER);
    out.extend_from_slice(&cookie.to_le_bytes());
    out.push(0x01); // run-container bitmap for one container
    out.extend_from_slice(&key.to_le_bytes());
    out.extend_from_slice(&cardinality_minus_one.to_le_bytes());
    out.extend_from_slice(&runs_count.to_le_bytes());
    for (start, len_minus_one) in runs {
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&len_minus_one.to_le_bytes());
    }
    Some(out)
}

fn encode_roaring32_payload(values: &[u32]) -> Result<Vec<u8>, BitmapValueFailure> {
    if should_encode_run_container(values)
        && let Some(out) = encode_roaring32_run_single_container(values)
    {
        return Ok(out);
    }
    encode_roaring32_no_run(values)
}

pub fn encode_external_bitmap(values: &BTreeSet<u64>) -> Result<Vec<u8>, BitmapValueFailure> {
    if values.is_empty() {
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let value = values
            .first()
            .copied()
            .ok_or(BitmapValueFailure::MissingSingleton)?;
        if let Ok(v32) = u32::try_from(value) {
            let mut out = Vec::with_capacity(5);
            out.push(BITMAP_TYPE_SINGLE32);
            out.extend_from_slice(&v32.to_le_bytes());
            return Ok(out);
        }
        let mut out = Vec::with_capacity(9);
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&value.to_le_bytes());
        return Ok(out);
    }
    if values.len() <= 32 {
        let count = bitmap_wire_count(values.len())?;
        let mut out = Vec::with_capacity(1 + 4 + values.len() * 8);
        out.push(BITMAP_TYPE_SET);
        out.extend_from_slice(&count.to_le_bytes());
        for &value in values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        return Ok(out);
    }

    let mut buckets: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for &value in values {
        let high = (value >> 32) as u32;
        let low = value as u32;
        buckets.entry(high).or_default().push(low);
    }
    if buckets.len() == 1 && buckets.contains_key(&0) {
        let payload =
            encode_roaring32_payload(buckets.get(&0).ok_or(BitmapValueFailure::MissingBucket)?)?;
        let mut out = Vec::with_capacity(1 + payload.len());
        out.push(BITMAP_TYPE_BITMAP32);
        out.extend_from_slice(&payload);
        return Ok(out);
    }

    let mut out = Vec::new();
    out.push(BITMAP_TYPE_BITMAP64);
    encode_varint_u64(
        u64::try_from(buckets.len()).map_err(BitmapValueFailure::Position)?,
        &mut out,
    );
    for (high, lows) in buckets {
        out.extend_from_slice(&high.to_le_bytes());
        let payload = encode_roaring32_payload(&lows)?;
        out.extend_from_slice(&payload);
    }
    Ok(out)
}

pub fn roaring_cookie_no_run() -> u32 {
    ROARING_COOKIE_NO_RUNCONTAINER
}
