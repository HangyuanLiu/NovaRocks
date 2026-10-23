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
//! Network stream sink for distributed exchange output.
//!
//! Canonical native tuple hashing and partition materialization shared by
//! network exchange and local driver routing. Both layers consume the same
//! base hash before applying their explicit partition domain.

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamStepContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for StreamStepContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for StreamStepContract {}
impl novarocks_failure_contract::FailureProjection for StreamStepContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::ExchangeStreamContract.reason(),
        )
    }
}
pub(crate) fn stream_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StreamStepContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

#[derive(Debug)]
struct StreamPartitionArrowFailure(arrow::error::ArrowError);
impl std::fmt::Display for StreamPartitionArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for StreamPartitionArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for StreamPartitionArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, reason) = match &self.0 {
            ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_) => (
                FailureClass::Resource,
                ExecutionReason::ExchangePartitionAllocation,
            ),
            ArrowError::OffsetOverflowError(_) => (
                FailureClass::Representation,
                ExecutionReason::ExchangePartitionOffset,
            ),
            ArrowError::IoError(..) | ArrowError::ExternalError(_) => (
                FailureClass::Unexpected,
                ExecutionReason::ExchangePartitionUnexpected,
            ),
            _ => (
                FailureClass::Invariant,
                ExecutionReason::ExchangePartitionArrow,
            ),
        };
        let fact = FailureFact::new(class, reason.reason());
        if matches!(self.0, ArrowError::OffsetOverflowError(_)) {
            // The selected Arrow take/cast boundary knows neither the complete
            // required extent nor a safe independent output prefix.
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(15).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
fn partition_arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(StreamPartitionArrowFailure(source))
}
#[derive(Debug)]
struct StreamDictionaryKeyFailure {
    row: usize,
    key: i32,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for StreamDictionaryKeyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hash_partition: negative dictionary key {} at row {}",
            self.key, self.row
        )
    }
}
impl std::error::Error for StreamDictionaryKeyFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for StreamDictionaryKeyFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::ExchangeDictionaryKey.reason(),
        )
    }
}
#[derive(Clone, Copy, Debug)]
struct PartitionRowExtent {
    last_index: u64,
}
impl std::fmt::Display for PartitionRowExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "exchange partition row index {} exceeds UInt32",
            self.last_index
        )
    }
}
impl std::error::Error for PartitionRowExtent {}
impl novarocks_failure_contract::FailureProjection for PartitionRowExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        FailureFact::new(
            FailureClass::Representation,
            crate::runtime::fragment::ExecutionReason::ExchangePartitionRowExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(16).unwrap(),
            },
            required: Some(self.last_index),
            extent_limit: Some(u64::from(u32::MAX)),
            representable_prefix: None,
        })
    }
}
pub(crate) fn check_partition_row_extent(rows: usize) -> ExecutionResult<()> {
    let last_index = rows.saturating_sub(1);
    if last_index as u128 > u128::from(u32::MAX) {
        return Err(ExecutionFailure::from_domain(PartitionRowExtent {
            last_index: last_index as u64,
        }));
    }
    Ok(())
}

use crate::exec::chunk::Chunk;
use crate::exec::hash_table::hash::{canonical_f32_bits, canonical_f64_bits};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::compute::take;
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use std::sync::Arc;

use crate::exec::hash_table::key_builder::{
    canonical_group_key_crc32_hash, canonical_group_key_fnv_hash,
};
use novarocks_types::largeint;

// FNV hash constants (from StarRocks BE)
const FNV_SEED: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

// FNV hash for a single value
fn fnv_hash_value(value: &[u8]) -> u64 {
    let mut hash = FNV_SEED;
    for &byte in value {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn crc32_update(mut crc: u32, value: &[u8]) -> u32 {
    for &byte in value {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb88320
            } else {
                crc >> 1
            };
        }
    }
    crc
}

fn crc32_hash_value(value: &[u8]) -> u32 {
    crc32_update(0xffff_ffff, value) ^ 0xffff_ffff
}

fn fnv_hash_list_utf8_row(list: &ListArray, values: &StringArray, row: usize) -> u64 {
    let offsets = list.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut hash = FNV_SEED;
    let count_bytes = (end.saturating_sub(start) as u64).to_le_bytes();
    hash ^= fnv_hash_value(&count_bytes);
    hash = hash.wrapping_mul(FNV_PRIME);
    for idx in start..end {
        if values.is_null(idx) {
            hash = hash.wrapping_mul(FNV_PRIME);
        } else {
            hash ^= fnv_hash_value(values.value(idx).as_bytes());
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

fn fnv_hash_list_int32_row(list: &ListArray, values: &Int32Array, row: usize) -> u64 {
    let offsets = list.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut hash = FNV_SEED;
    let count_bytes = (end.saturating_sub(start) as u64).to_le_bytes();
    hash ^= fnv_hash_value(&count_bytes);
    hash = hash.wrapping_mul(FNV_PRIME);
    for idx in start..end {
        if values.is_null(idx) {
            hash = hash.wrapping_mul(FNV_PRIME);
        } else {
            let bytes = values.value(idx).to_le_bytes();
            hash ^= fnv_hash_value(&bytes);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

fn dictionary_int32_indices(array: &ArrayRef) -> ExecutionResult<&DictionaryArray<Int32Type>> {
    array
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| {
            stream_contract(format_args!(
                "hash_partition: failed to downcast dictionary carrier {:?} to DictionaryArray<Int32Type>",
                array.data_type()
            ))
        })
}

fn dictionary_key_index(key: i32, values_len: usize, row: usize) -> ExecutionResult<usize> {
    let idx = usize::try_from(key).map_err(|source| {
        ExecutionFailure::from_domain(StreamDictionaryKeyFailure { row, key, source })
    })?;
    if idx >= values_len {
        return Err(stream_contract(format_args!(
            "hash_partition: dictionary key {key} at row {row} exceeds values len {values_len}"
        )));
    }
    Ok(idx)
}

fn compute_fnv_hash_dictionary_array(
    array: &ArrayRef,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<u64>> {
    let dict = dictionary_int32_indices(array)?;
    let values = dict.values();
    let value_hashes = compute_fnv_hash_array_with(values, checkpoint)?;
    let keys = dict.keys();
    let mut out = vec![FNV_SEED; dict.len()];
    for (row, hash_value) in out.iter_mut().enumerate().take(dict.len()) {
        if row & 1023 == 0 {
            checkpoint()?;
        }
        if dict.is_null(row) {
            *hash_value = hash_value.wrapping_mul(FNV_PRIME);
        } else {
            let idx = dictionary_key_index(keys.value(row), value_hashes.len(), row)?;
            *hash_value = value_hashes[idx];
        }
    }
    Ok(out)
}

fn compute_crc32_hash_dictionary_array(array: &ArrayRef) -> ExecutionResult<Vec<u32>> {
    let dict = dictionary_int32_indices(array)?;
    let values = dict.values();
    let value_hashes = compute_crc32_hash_array(values)?;
    let keys = dict.keys();
    let mut out = vec![0u32; dict.len()];
    for (row, hash_value) in out.iter_mut().enumerate().take(dict.len()) {
        if row & 1023 == 0 {
            checkpoint()?;
        }
        if dict.is_null(row) {
            *hash_value = 0;
        } else {
            let idx = dictionary_key_index(keys.value(row), value_hashes.len(), row)?;
            *hash_value = value_hashes[idx];
        }
    }
    Ok(out)
}

fn compute_fnv_hash_array(array: &ArrayRef) -> ExecutionResult<Vec<u64>> {
    compute_fnv_hash_array_with(array, &|| Ok(()))
}

// Compute FNV hash for each row in an array
fn compute_fnv_hash_array_with(
    array: &ArrayRef,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<u64>> {
    let len = array.len();
    let mut hash_values = vec![FNV_SEED; len];

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| stream_contract("failed to downcast to BooleanArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = if arr.value(i) { 1u8 } else { 0u8 };
                    *hash_value ^= val as u64;
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Int64Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Int32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Int16Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Float64Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = canonical_f64_bits(val).to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Float32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = canonical_f32_bits(val).to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| stream_contract("failed to downcast to StringArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    *hash_value ^= fnv_hash_value(val.as_bytes());
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| stream_contract("failed to downcast to LargeStringArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    *hash_value ^= fnv_hash_value(val.as_bytes());
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| stream_contract("failed to downcast to BinaryArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    *hash_value ^= fnv_hash_value(arr.value(i));
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| stream_contract("failed to downcast to LargeBinaryArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    *hash_value ^= fnv_hash_value(arr.value(i));
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Date32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Timestamp(unit, _tz) => {
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if array.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = match unit {
                        TimeUnit::Second => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .ok_or_else(|| {
                                    stream_contract("failed to downcast to TimestampSecondArray")
                                })?;
                            arr.value(i)
                        }
                        TimeUnit::Millisecond => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .ok_or_else(|| {
                                    stream_contract(
                                        "failed to downcast to TimestampMillisecondArray",
                                    )
                                })?;
                            arr.value(i)
                        }
                        TimeUnit::Microsecond => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .ok_or_else(|| {
                                    stream_contract(
                                        "failed to downcast to TimestampMicrosecondArray",
                                    )
                                })?;
                            arr.value(i)
                        }
                        TimeUnit::Nanosecond => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .ok_or_else(|| {
                                    stream_contract(
                                        "failed to downcast to TimestampNanosecondArray",
                                    )
                                })?;
                            arr.value(i)
                        }
                    };
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Decimal128Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Decimal256Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| stream_contract("failed to downcast to FixedSizeBinaryArray"))?;
            if arr.value_length() != 16 {
                return Err(stream_contract(
                    "hash_partition LARGEINT physical width differs from 16",
                ));
            }
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if arr.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    // FixedSizeBinaryArray owns this exact fixed-width slice.
                    let fixed: [u8; 16] = arr
                        .value(i)
                        .try_into()
                        .expect("checked LARGEINT physical width");
                    let value = i128::from_be_bytes(fixed);
                    let bytes = value.to_le_bytes();
                    *hash_value ^= fnv_hash_value(&bytes);
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| stream_contract("failed to downcast list values to StringArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if list.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let row_hash = fnv_hash_list_utf8_row(list, values, i);
                    *hash_value ^= row_hash;
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| stream_contract("failed to downcast list values to Int32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if i & 1023 == 0 {
                    checkpoint()?;
                }
                if list.is_null(i) {
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                } else {
                    let row_hash = fnv_hash_list_int32_row(list, values, i);
                    *hash_value ^= row_hash;
                    *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                }
            }
        }
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            for (row, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if row & 1023 == 0 {
                    checkpoint()?;
                }
                match canonical_group_key_fnv_hash(array, row)? {
                    Some(row_hash) => {
                        *hash_value ^= row_hash;
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                    None => {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
        }
        dict_type if is_low_cardinality_exchange_dictionary(dict_type) => {
            return compute_fnv_hash_dictionary_array(array, checkpoint);
        }
        _ => {
            return Err(stream_contract(format_args!(
                "hash_partition: unsupported array type for FNV hash: {:?}",
                array.data_type()
            )));
        }
    }

    Ok(hash_values)
}

// Compute CRC32 hash for each row in an array (for BUCKET_SHUFFLE_HASH_PARTITIONED)
fn compute_crc32_hash_array(array: &ArrayRef) -> ExecutionResult<Vec<u32>> {
    let len = array.len();
    let mut hash_values = vec![0u32; len];

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| stream_contract("failed to downcast to BooleanArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = if arr.value(i) { 1u8 } else { 0u8 };
                    *hash_value = crc32_hash_value(&[val]);
                }
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Int64Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Int32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Int16Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Float64Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = canonical_f64_bits(val).to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Float32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = canonical_f32_bits(val).to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| stream_contract("failed to downcast to StringArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    *hash_value = crc32_hash_value(val.as_bytes());
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| stream_contract("failed to downcast to LargeStringArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    *hash_value = crc32_hash_value(val.as_bytes());
                }
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| stream_contract("failed to downcast to BinaryArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    *hash_value = crc32_hash_value(arr.value(i));
                }
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| stream_contract("failed to downcast to LargeBinaryArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    *hash_value = crc32_hash_value(arr.value(i));
                }
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Date32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Timestamp(unit, _tz) => {
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !array.is_null(i) {
                    let val = match unit {
                        TimeUnit::Second => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .ok_or_else(|| {
                                    stream_contract("failed to downcast to TimestampSecondArray")
                                })?;
                            arr.value(i)
                        }
                        TimeUnit::Millisecond => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .ok_or_else(|| {
                                    stream_contract(
                                        "failed to downcast to TimestampMillisecondArray",
                                    )
                                })?;
                            arr.value(i)
                        }
                        TimeUnit::Microsecond => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .ok_or_else(|| {
                                    stream_contract(
                                        "failed to downcast to TimestampMicrosecondArray",
                                    )
                                })?;
                            arr.value(i)
                        }
                        TimeUnit::Nanosecond => {
                            let arr = array
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .ok_or_else(|| {
                                    stream_contract(
                                        "failed to downcast to TimestampNanosecondArray",
                                    )
                                })?;
                            arr.value(i)
                        }
                    };
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Decimal128Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| stream_contract("failed to downcast to Decimal256Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    let bytes = val.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| stream_contract("failed to downcast to FixedSizeBinaryArray"))?;
            if arr.value_length() != 16 {
                return Err(stream_contract(
                    "hash_partition LARGEINT physical width differs from 16",
                ));
            }
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if !arr.is_null(i) {
                    // FixedSizeBinaryArray owns this exact fixed-width slice.
                    let fixed: [u8; 16] = arr
                        .value(i)
                        .try_into()
                        .expect("checked LARGEINT physical width");
                    let value = i128::from_be_bytes(fixed);
                    let bytes = value.to_le_bytes();
                    *hash_value = crc32_hash_value(&bytes);
                }
            }
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| stream_contract("failed to downcast list values to StringArray"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if list.is_null(i) {
                    continue;
                }
                let offsets = list.value_offsets();
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let mut crc = crc32_update(
                    0xffff_ffff,
                    &(end.saturating_sub(start) as u64).to_le_bytes(),
                );
                for idx in start..end {
                    if values.is_null(idx) {
                        crc = crc32_update(crc, &[0]);
                    } else {
                        crc = crc32_update(crc, &[1]);
                        crc = crc32_update(crc, values.value(idx).as_bytes());
                    }
                }
                *hash_value = crc ^ 0xffff_ffff;
            }
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| stream_contract("failed to downcast list values to Int32Array"))?;
            for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if list.is_null(i) {
                    continue;
                }
                let offsets = list.value_offsets();
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let mut crc = crc32_update(
                    0xffff_ffff,
                    &(end.saturating_sub(start) as u64).to_le_bytes(),
                );
                for idx in start..end {
                    if values.is_null(idx) {
                        crc = crc32_update(crc, &[0]);
                    } else {
                        crc = crc32_update(crc, &[1]);
                        crc = crc32_update(crc, &values.value(idx).to_le_bytes());
                    }
                }
                *hash_value = crc ^ 0xffff_ffff;
            }
        }
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            for (row, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                if let Some(row_hash) = canonical_group_key_crc32_hash(array, row)? {
                    *hash_value = row_hash;
                }
            }
        }
        dict_type if is_low_cardinality_exchange_dictionary(dict_type) => {
            return compute_crc32_hash_dictionary_array(array);
        }
        _ => {
            return Err(stream_contract(format_args!(
                "hash_partition: unsupported array type for CRC32 hash: {:?}",
                array.data_type()
            )));
        }
    }

    Ok(hash_values)
}

fn canonicalize_hash_array(array: &ArrayRef) -> ExecutionResult<ArrayRef> {
    match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            cast(array, &DataType::Int64).map_err(partition_arrow)
        }
        DataType::Float32 => cast(array, &DataType::Float64).map_err(partition_arrow),
        _ => Ok(array.clone()),
    }
}

// Partition a chunk based on hash values without converting to rows
fn take_partition_rows_with<E: From<ExecutionFailure>>(
    source: &Chunk,
    rows: &[u32],
    publish: impl FnOnce(&Chunk, Vec<ArrayRef>, usize) -> Result<Chunk, E>,
) -> Result<Chunk, E> {
    let indices = arrow::array::UInt32Array::from(rows.to_vec());
    let indices_ref = Arc::new(indices) as ArrayRef;
    let mut columns = Vec::with_capacity(source.record_batch().num_columns());
    for column in source.record_batch().columns() {
        columns.push(
            take(column.as_ref(), &indices_ref, None)
                .map_err(partition_arrow)
                .map_err(E::from)?,
        );
    }
    publish(source, columns, rows.len())
}

pub(crate) fn take_partition_rows_created(
    source: &Chunk,
    rows: &[u32],
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> crate::runtime::fragment::ExecutionResult<Chunk> {
    checkpoint()?;
    let owner = factory.begin_input_construction(source)?;
    take_partition_rows_in(source, rows, owner.as_ref(), checkpoint)
}

pub(crate) fn take_partition_rows_created_bounded(
    source: &Chunk,
    rows: &[u32],
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    byte_limit: usize,
) -> ExecutionResult<Chunk> {
    let owner = factory.begin_typed_construction_bounded(source.chunk_schema(), byte_limit)?;
    take_partition_rows_in(source, rows, owner.as_ref(), checkpoint)
}

fn take_partition_rows_in(
    source: &Chunk,
    rows: &[u32],
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Chunk> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    checkpoint()?;
    for layout in [
        std::alloc::Layout::array::<u32>(rows.len()),
        std::alloc::Layout::array::<ArrayRef>(source.columns().len()),
    ] {
        owner.request(layout.map_err(|_| {
            ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
    }
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: rows.len() },
    )?;
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut opaque,
    );
    let mut work = |_| checkpoint().map_err(ConstructionFailure::from);
    novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction_observed(
        &mut creation,
        &mut work,
    )
    .map_err(copy_construction_failure)?;
    let indices = arrow::array::UInt32Array::from(rows.to_vec());
    let mut columns = Vec::with_capacity(source.columns().len());
    for column in source.columns() {
        novarocks_functions::selected_copy::preflight_take_construction_unified_observed(
            column.as_ref(),
            novarocks_functions::selected_copy::CopyIndexView::UInt32(&indices),
            &mut creation,
            &mut work,
        )
        .map_err(copy_construction_failure)?;
        columns.push(take(column.as_ref(), &indices, None).map_err(partition_arrow)?);
        checkpoint()?;
    }
    let storage = source.source_storage_lifetime_observed::<ConstructionFailure>(
        owner.storage_lifetime(),
        |layout| owner.request(layout),
    )?;
    let receipt_storage = storage.clone();
    let mut output = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        source.chunk_schema_ref(),
        columns,
        rows.len(),
        storage,
        |step| owner.observe(0, &[], step),
    )?;
    if let Some(source_receipt) = source.source_backing_receipt() {
        let constructed = owner.successful_request_extent_upper().ok_or_else(|| {
            stream_contract("partition creator has no complete successful request extent")
        })?;
        let complete = source_receipt
            .retained_bytes()
            .checked_add(constructed)
            .ok_or_else(|| {
                ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
            })?;
        output.attach_source_backing_receipt(
            crate::exec::chunk::ChunkSourceBackingReceipt::from_source(complete, receipt_storage),
        )?;
    }
    checkpoint()?;
    Ok(output)
}

#[cfg(test)]
pub fn partition_chunk_by_hash_arrays(
    chunk: &Chunk,
    keys: &[ArrayRef],
    partitions: usize,
    crc: bool,
) -> ExecutionResult<Vec<Chunk>> {
    partition_chunk_by_hash_arrays_with(chunk, keys, partitions, crc, |source, rows| {
        take_partition_rows_with(source, rows, |source, columns, _| {
            Chunk::try_new_with_columns(source.chunk_schema_ref(), columns).map_err(Into::into)
        })
    })
}
pub(crate) fn partition_chunk_by_hash_arrays_created(
    chunk: &Chunk,
    keys: &[ArrayRef],
    partitions: usize,
    crc: bool,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<Chunk>> {
    checkpoint()?;
    let output =
        partition_chunk_by_hash_arrays_with(chunk, keys, partitions, crc, |source, rows| {
            take_partition_rows_created(source, rows, factory, checkpoint)
        })?;
    checkpoint()?;
    Ok(output)
}
fn partition_chunk_by_hash_arrays_with(
    chunk: &Chunk,
    partition_arrays: &[ArrayRef],
    num_partitions: usize,
    use_crc32: bool,
    mut take_rows: impl FnMut(&Chunk, &[u32]) -> ExecutionResult<Chunk>,
) -> ExecutionResult<Vec<Chunk>> {
    if chunk.is_empty() {
        return Ok(vec![Chunk::default(); num_partitions]);
    }

    check_partition_row_extent(chunk.len())?;
    if num_partitions == 0 {
        return Err(stream_contract(
            "nonempty hash partition requires a destination",
        ));
    }

    let num_rows = chunk.len();
    let hash_values = if use_crc32 {
        let arrays = canonical_tuple_arrays(partition_arrays, num_rows)?;
        let mut hashes = vec![0u32; num_rows];
        for array in &arrays {
            let values = compute_crc32_hash_array(array)?;
            for (hash, value) in hashes.iter_mut().zip(values) {
                *hash = hash.wrapping_add(value);
            }
        }
        hashes.into_iter().map(u64::from).collect()
    } else {
        native_tuple_hashes(partition_arrays, num_rows)?
    };

    // Compute partition index for each row
    let partition_indices: Vec<usize> = hash_values
        .iter()
        .map(|&h| (h as usize) % num_partitions)
        .collect();

    // Build row index arrays for each partition (similar to StarRocks BE)
    let mut partition_row_indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for (row_idx, &part_idx) in partition_indices.iter().enumerate() {
        partition_row_indices[part_idx].push(row_idx as u32);
    }

    let mut partition_chunks = Vec::with_capacity(num_partitions);
    for rows in &partition_row_indices {
        if rows.is_empty() {
            partition_chunks.push(Chunk::default());
        } else {
            partition_chunks.push(take_rows(chunk, rows)?);
        }
    }

    Ok(partition_chunks)
}
pub(crate) fn is_low_cardinality_exchange_dictionary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key_type, value_type)
            if key_type.as_ref() == &DataType::Int32
                && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
    )
}

fn canonical_tuple_arrays(keys: &[ArrayRef], rows: usize) -> ExecutionResult<Vec<ArrayRef>> {
    keys.iter()
        .map(|array| {
            if array.len() != rows {
                return Err(stream_contract(format_args!(
                    "hash partition key rows {} differ from input rows {}",
                    array.len(),
                    rows
                )));
            }
            canonicalize_hash_array(array)
        })
        .collect()
}

/// The canonical native FNV tuple base, before any instance/driver domain.
/// Key order, NULL handling and native scalar canonicalization are shared by
/// every consumer; aggregate hash-table hashing is a different contract.
pub(crate) fn native_tuple_hashes(keys: &[ArrayRef], rows: usize) -> ExecutionResult<Vec<u64>> {
    let arrays = canonical_tuple_arrays(keys, rows)?;
    let mut hashes = vec![FNV_SEED; rows];
    for array in &arrays {
        let values = compute_fnv_hash_array(array)?;
        for (hash, value) in hashes.iter_mut().zip(values) {
            *hash = (*hash ^ value).wrapping_mul(FNV_PRIME);
        }
    }
    Ok(hashes)
}

fn copy_construction_failure(
    error: novarocks_functions::selected_copy::CopyConstructionError<
        crate::exec::pipeline::operator::ConstructionFailure,
    >,
) -> ExecutionFailure {
    use novarocks_functions::selected_copy::CopyConstructionError;
    match error {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => {
            crate::exec::pipeline::operator::ConstructionFailure::Resource(cause).into()
        }
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}

/// The same tuple base under the caller's finite construction scope. Widened
/// native scalar hashing reads the original scalar directly, avoiding temporary
/// cast arrays while preserving the existing Int64/Float64 canonical bytes.
pub(crate) fn native_tuple_hashes_created(
    keys: &[ArrayRef],
    rows: usize,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<u64>> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let request_hashes = |count: usize| -> ExecutionResult<()> {
        let layout = std::alloc::Layout::array::<u64>(count).map_err(|_| {
            ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        owner.request(layout)?;
        Ok(())
    };
    checkpoint()?;
    for key in keys {
        if key.len() != rows {
            return Err(stream_contract(
                "native tuple key rows differ from input rows",
            ));
        }
    }
    request_hashes(rows)?;
    let mut hashes = vec![FNV_SEED; rows];
    for key in keys {
        checkpoint()?;
        request_hashes(rows)?;
        let values = match key.data_type() {
            DataType::Int8 => {
                let array = key
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| stream_contract("native tuple Int8 carrier differs"))?;
                widened_hash_rows(
                    key,
                    |row| i64::from(array.value(row)).to_le_bytes(),
                    checkpoint,
                )?
            }
            DataType::Int16 => {
                let array = key
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| stream_contract("native tuple Int16 carrier differs"))?;
                widened_hash_rows(
                    key,
                    |row| i64::from(array.value(row)).to_le_bytes(),
                    checkpoint,
                )?
            }
            DataType::Int32 => {
                let array = key
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| stream_contract("native tuple Int32 carrier differs"))?;
                widened_hash_rows(
                    key,
                    |row| i64::from(array.value(row)).to_le_bytes(),
                    checkpoint,
                )?
            }
            DataType::Float32 => {
                let array = key
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| stream_contract("native tuple Float32 carrier differs"))?;
                widened_hash_rows(
                    key,
                    |row| canonical_f64_bits(f64::from(array.value(row))).to_le_bytes(),
                    checkpoint,
                )?
            }
            ty if is_low_cardinality_exchange_dictionary(ty) => {
                let array = dictionary_int32_indices(key)?;
                // The existing dictionary author allocates an initial row hash
                // vector, its dictionary-value hashes and the final row vector.
                request_hashes(array.values().len())?;
                request_hashes(rows)?;
                compute_fnv_hash_array_with(key, checkpoint)?
            }
            _ => compute_fnv_hash_array_with(key, checkpoint)?,
        };
        for (row, (hash, value)) in hashes.iter_mut().zip(values).enumerate() {
            if row & 1023 == 0 {
                checkpoint()?;
            }
            *hash = (*hash ^ value).wrapping_mul(FNV_PRIME);
        }
    }
    checkpoint()?;
    Ok(hashes)
}

fn widened_hash_rows(
    array: &ArrayRef,
    bytes: impl Fn(usize) -> [u8; 8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<u64>> {
    let mut hashes = vec![FNV_SEED; array.len()];
    for (row, hash) in hashes.iter_mut().enumerate() {
        if row & 1023 == 0 {
            checkpoint()?;
        }
        if !array.is_null(row) {
            *hash ^= fnv_hash_value(&bytes(row));
        }
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
    Ok(hashes)
}

#[cfg(test)]
mod created_tests {
    use super::*;
    use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use novarocks_type_contract::FunctionValueType;

    #[test]
    fn created_tuple_hashes_preserve_native_canonical_bytes_without_cast_arrays() {
        let tracker = MemTracker::new_root("created canonical tuple");
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let source = crate::exec::chunk::typed_fixture::columns(
            &["key"],
            &[novarocks_types::SlotId::new(1)],
            &[FunctionValueType::new(DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(-2)]))],
            3,
        );
        let owner = factory
            .begin_typed_construction_bounded(source.chunk_schema(), 65536)
            .unwrap();
        let expected = vec![0x5d0f667dfabf7962, 0x06312a7ba27624ce, 0xb95fafe2fa09c39a];
        for key in [
            Arc::new(Int8Array::from(vec![Some(1), None, Some(-2)])) as ArrayRef,
            Arc::new(Int16Array::from(vec![Some(1), None, Some(-2)])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(-2)])),
            source.columns()[0].clone(),
        ] {
            assert_eq!(
                native_tuple_hashes_created(&[key.clone()], 3, owner.as_ref(), &|| Ok(())).unwrap(),
                expected
            );
            assert_eq!(native_tuple_hashes(&[key], 3).unwrap(), expected);
        }
        let float: ArrayRef = Arc::new(Float32Array::from(vec![Some(-0.0), None, Some(f32::NAN)]));
        assert_eq!(
            native_tuple_hashes_created(&[float.clone()], 3, owner.as_ref(), &|| Ok(())).unwrap(),
            native_tuple_hashes(&[float], 3).unwrap()
        );
        drop(owner);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn finite_tuple_workspace_refuses_before_hash_vector_allocation() {
        let tracker = MemTracker::new_root("finite canonical tuple");
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let source = crate::exec::chunk::typed_fixture::columns(
            &["key"],
            &[novarocks_types::SlotId::new(1)],
            &[FunctionValueType::new(DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1; 1024]))],
            1024,
        );
        let owner = factory
            .begin_typed_construction_bounded(source.chunk_schema(), 1024)
            .unwrap();
        let before = tracker.current();
        let failure =
            native_tuple_hashes_created(source.columns(), source.len(), owner.as_ref(), &|| Ok(()))
                .unwrap_err();
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            failure.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        assert_eq!(tracker.current(), before);
    }
}
