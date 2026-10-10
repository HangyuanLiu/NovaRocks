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
//! The original recursive BY codec and winner transition, generic over host storage.
use crate::aggregate_scalar::{
    self as scalar, AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork,
    TrackedAggScalarValue,
};
use arrow_array::ArrayRef;
use arrow_buffer::i256;
use std::cmp::Ordering;

/// The v1 shell keeps its original std Vec buffer; selected execution supplies
/// a fallible host-allocated buffer. Both use this one byte-writing algorithm.
pub trait ByEncodeBuffer {
    fn push(&mut self, byte: u8) -> Result<(), ScalarStateError>;
    fn append(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError>;
}

#[derive(Clone, Copy, Debug)]
pub enum ByDirection {
    Maximum,
    Minimum,
}
#[derive(Debug)]
pub struct ByState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub key: Option<TrackedAggScalarValue<A>>,
    pub value: Option<TrackedAggScalarValue<A>>,
    pub failed: bool,
    retained: usize,
}
impl<A: ScalarStateAllocator> ByState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            allocator,
            key: None,
            value: None,
            failed: false,
            retained: 0,
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained
    }
    pub fn latch_failure(&mut self) {
        self.failed = true;
        self.key = None;
        self.value = None;
        self.retained = 0;
    }
    fn admit_candidate(
        &mut self,
        direction: ByDirection,
        key: TrackedAggScalarValue<A>,
        value: Option<TrackedAggScalarValue<A>>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let should_update = match self.key.as_ref() {
            None => true,
            Some(current) => {
                let ordering = scalar::compare_tracked_scalar_values(&key, current, work)?;
                match direction {
                    ByDirection::Maximum => ordering == Ordering::Greater,
                    ByDirection::Minimum => ordering == Ordering::Less,
                }
            }
        };
        if should_update {
            let retained = scalar::tracked_scalar_heap_capacity(&key, work)?
                .checked_add(match &value {
                    Some(value) => scalar::tracked_scalar_heap_capacity(value, work)?,
                    None => 0,
                })
                .ok_or(crate::KernelFailure::ResourceExhausted)?;
            self.key = Some(key);
            self.value = value;
            self.retained = retained;
        }
        Ok(())
    }
    pub fn update_from_arrays(
        &mut self,
        direction: ByDirection,
        values: &ArrayRef,
        value_row: usize,
        keys: &ArrayRef,
        key_row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let key = scalar::tracked_scalar_from_array(keys, key_row, &self.allocator, work)?;
        let Some(key) = key else {
            return Ok(());
        };
        // Preserve the original candidate allocation even for a losing key.
        let value = scalar::tracked_scalar_from_array(values, value_row, &self.allocator, work)?;
        self.admit_candidate(direction, key, value, work)
    }
    pub fn merge_bytes(
        &mut self,
        direction: ByDirection,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let mut slice = bytes;
        let key = decode_tracked_scalar(&mut slice, &self.allocator, work)?
            .ok_or_else(|| "max_by/min_by merge missing key".to_string())?;
        let value = decode_tracked_scalar(&mut slice, &self.allocator, work)?;
        if !slice.is_empty() {
            return Err("max_by/min_by merge input has trailing bytes"
                .to_string()
                .into());
        }
        self.admit_candidate(direction, key, value, work)
    }
    /// Empty states remain NULL Binary rows; no alternate empty byte encoding.
    pub fn serialize<B: ByEncodeBuffer>(
        &self,
        output: &mut B,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<bool, ScalarStateError> {
        if self.key.is_none() {
            return Ok(false);
        }
        encode_tracked_scalar(&self.key, output, work)?;
        encode_tracked_scalar(&self.value, output, work)?;
        Ok(true)
    }
    pub fn output(
        &self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<AggScalarValue>, ScalarStateError> {
        if self.key.is_some() {
            self.value
                .as_ref()
                .map(|value| scalar::tracked_scalar_to_output(value, work))
                .transpose()
        } else {
            Ok(None)
        }
    }
}
fn need_len(input: &[u8], need: usize, label: &str) -> Result<(), ScalarStateError> {
    if input.len() < need {
        Err(format!("max_by/min_by {} decode failed", label).into())
    } else {
        Ok(())
    }
}

fn read_u32(input: &mut &[u8], label: &str) -> Result<u32, ScalarStateError> {
    need_len(input, 4, label)?;
    let value = u32::from_le_bytes(input[..4].try_into().unwrap());
    *input = &input[4..];
    Ok(value)
}

fn encode_tracked_scalar_value<A: ScalarStateAllocator, B: ByEncodeBuffer>(
    value: &TrackedAggScalarValue<A>,
    output: &mut B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    match value {
        TrackedAggScalarValue::Bool(value) => {
            output.push(1)?;
            output.push(*value as u8)?;
        }
        TrackedAggScalarValue::Int64(value) => {
            output.push(2)?;
            output.append(&value.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Float64(value) => {
            output.push(3)?;
            output.append(&value.to_bits().to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Utf8(value) => {
            output.push(4)?;
            let len = u32::try_from(value.len())
                .map_err(|_| "max_by/min_by tracked UTF-8 value too large".to_string())?;
            output.append(&len.to_le_bytes(), work)?;
            output.append(value, work)?;
        }
        TrackedAggScalarValue::Date32(value) => {
            output.push(5)?;
            output.append(&value.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Timestamp(value) => {
            output.push(6)?;
            output.append(&value.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Decimal128(value) => {
            output.push(7)?;
            output.append(&value.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Struct(values) => {
            output.push(8)?;
            let len = u32::try_from(values.len())
                .map_err(|_| "max_by/min_by tracked struct too large".to_string())?;
            output.append(&len.to_le_bytes(), work)?;
            for value in values {
                encode_tracked_scalar(value, output, work)?;
            }
        }
        TrackedAggScalarValue::Map(entries) => {
            output.push(9)?;
            let len = u32::try_from(entries.len())
                .map_err(|_| "max_by/min_by tracked map too large".to_string())?;
            output.append(&len.to_le_bytes(), work)?;
            for (key, value) in entries {
                encode_tracked_scalar(key, output, work)?;
                encode_tracked_scalar(value, output, work)?;
            }
        }
        TrackedAggScalarValue::List(values) => {
            output.push(10)?;
            let len = u32::try_from(values.len())
                .map_err(|_| "max_by/min_by tracked list too large".to_string())?;
            output.append(&len.to_le_bytes(), work)?;
            for value in values {
                encode_tracked_scalar(value, output, work)?;
            }
        }
        TrackedAggScalarValue::Decimal256(value) => {
            output.push(11)?;
            let text = value.to_string();
            let len = u32::try_from(text.len())
                .map_err(|_| "max_by/min_by decimal256 too large".to_string())?;
            output.append(&len.to_le_bytes(), work)?;
            output.append(text.as_bytes(), work)?;
        }
        TrackedAggScalarValue::Binary(value) => {
            output.push(12)?;
            let len = u32::try_from(value.len())
                .map_err(|_| "max_by/min_by tracked binary too large".to_string())?;
            output.append(&len.to_le_bytes(), work)?;
            output.append(value, work)?;
        }
    }
    Ok(())
}

fn encode_tracked_scalar<A: ScalarStateAllocator, B: ByEncodeBuffer>(
    value: &Option<TrackedAggScalarValue<A>>,
    output: &mut B,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    match value {
        Some(value) => {
            output.push(1)?;
            encode_tracked_scalar_value(value, output, work)
        }
        None => {
            output.push(0)?;
            Ok(())
        }
    }
}

fn decode_tracked_scalar_value<A: ScalarStateAllocator>(
    input: &mut &[u8],
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<TrackedAggScalarValue<A>, ScalarStateError> {
    work.step()?;
    need_len(input, 1, "tracked scalar")?;
    let tag = input[0];
    *input = &input[1..];
    match tag {
        1 => {
            need_len(input, 1, "tracked bool")?;
            let value = input[0] != 0;
            *input = &input[1..];
            Ok(TrackedAggScalarValue::Bool(value))
        }
        2 => {
            need_len(input, 8, "tracked int64")?;
            let value = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(TrackedAggScalarValue::Int64(value))
        }
        3 => {
            need_len(input, 8, "tracked float64")?;
            let value = f64::from_bits(u64::from_le_bytes(input[..8].try_into().unwrap()));
            *input = &input[8..];
            Ok(TrackedAggScalarValue::Float64(value))
        }
        4 | 12 => {
            let len = read_u32(input, "tracked bytes")? as usize;
            need_len(input, len, "tracked bytes")?;
            let value = scalar::scalar_bytes(allocator.clone(), &input[..len], work)?;
            *input = &input[len..];
            if tag == 4 {
                for _ in &value {
                    work.step()?;
                }
                work.flush()?;
                std::str::from_utf8(&value).map_err(|error| error.to_string())?;
                Ok(TrackedAggScalarValue::Utf8(value))
            } else {
                Ok(TrackedAggScalarValue::Binary(value))
            }
        }
        5 => {
            need_len(input, 4, "tracked date32")?;
            let value = i32::from_le_bytes(input[..4].try_into().unwrap());
            *input = &input[4..];
            Ok(TrackedAggScalarValue::Date32(value))
        }
        6 => {
            need_len(input, 8, "tracked timestamp")?;
            let value = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(TrackedAggScalarValue::Timestamp(value))
        }
        7 => {
            need_len(input, 16, "tracked decimal128")?;
            let value = i128::from_le_bytes(input[..16].try_into().unwrap());
            *input = &input[16..];
            Ok(TrackedAggScalarValue::Decimal128(value))
        }
        8 | 10 => {
            let len = read_u32(input, "tracked sequence")? as usize;
            let operation = if tag == 8 {
                "reserve tracked aggregate struct decode"
            } else {
                "reserve tracked aggregate list decode"
            };
            let mut values = scalar::aggregate_vec_with_capacity(allocator, len, operation, work)?;
            for _ in 0..len {
                work.step()?;
                values.push(decode_tracked_scalar(input, allocator, work)?);
            }
            if tag == 8 {
                Ok(TrackedAggScalarValue::Struct(values))
            } else {
                Ok(TrackedAggScalarValue::List(values))
            }
        }
        9 => {
            let len = read_u32(input, "tracked map")? as usize;
            let mut entries = scalar::aggregate_vec_with_capacity(
                allocator,
                len,
                "reserve tracked aggregate map decode",
                work,
            )?;
            for _ in 0..len {
                work.step()?;
                entries.push((
                    decode_tracked_scalar(input, allocator, work)?,
                    decode_tracked_scalar(input, allocator, work)?,
                ));
            }
            Ok(TrackedAggScalarValue::Map(entries))
        }
        11 => {
            let len = read_u32(input, "tracked decimal256")? as usize;
            need_len(input, len, "tracked decimal256")?;
            for _ in &input[..len] {
                work.step()?;
            }
            work.flush()?;
            let text = std::str::from_utf8(&input[..len]).map_err(|error| error.to_string())?;
            *input = &input[len..];
            Ok(TrackedAggScalarValue::Decimal256(
                text.parse::<i256>()
                    .map_err(|_| "max_by/min_by decimal256 decode failed".to_string())?,
            ))
        }
        _ => Err("max_by/min_by tracked scalar decode failed: unknown tag"
            .to_string()
            .into()),
    }
}

fn decode_tracked_scalar<A: ScalarStateAllocator>(
    input: &mut &[u8],
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<TrackedAggScalarValue<A>>, ScalarStateError> {
    work.step()?;
    need_len(input, 1, "tracked scalar")?;
    let has_value = input[0];
    *input = &input[1..];
    match has_value {
        0 => Ok(None),
        1 => decode_tracked_scalar_value(input, allocator, work).map(Some),
        _ => Err(
            "max_by/min_by tracked scalar decode failed: invalid null flag"
                .to_string()
                .into(),
        ),
    }
}
