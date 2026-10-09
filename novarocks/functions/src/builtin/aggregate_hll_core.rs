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
//! Original HLL registers, value hash and state interpretation shared by both shells.
use crate::KernelFailure;
use crate::hll::{HLL_DATA_EMPTY, HLL_DATA_EXPLICIT, MURMUR_SEED, encode_hll_empty};
use crate::kernel_input::EvaluationCheckpoints;
use arrow_array::*;
use arrow_schema::{DataType, TimeUnit};
use std::ops::{Deref, DerefMut};
pub const HLL_DATA_SPARSE: u8 = 2;
pub const HLL_DATA_FULL: u8 = 3;
const HLL_COLUMN_PRECISION: usize = 14;
pub const HLL_REGISTERS_COUNT: usize = 16 * 1024;
const HLL_SPARSE_THRESHOLD: usize = 4096;
#[derive(Debug)]
pub enum HllError {
    Legacy(String),
    Kernel(KernelFailure),
}
impl From<String> for HllError {
    fn from(message: String) -> Self {
        Self::Legacy(message)
    }
}
impl From<KernelFailure> for HllError {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl std::fmt::Display for HllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(message) => f.write_str(message),
            Self::Kernel(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for HllError {}

pub struct HllWork<'control, 'scope> {
    work: Option<&'scope mut EvaluationCheckpoints<'control>>,
}
impl<'control, 'scope> HllWork<'control, 'scope> {
    pub fn new(work: Option<&'scope mut EvaluationCheckpoints<'control>>) -> Self {
        Self { work }
    }
    pub fn step(&mut self) -> Result<(), HllError> {
        if let Some(work) = &mut self.work {
            work.step()?;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<(), HllError> {
        if let Some(work) = &mut self.work {
            work.flush()?;
        }
        Ok(())
    }
}
/// Storage only: the original fixed register math is implemented once below.
pub trait HllRegisterAllocator: Clone {
    type Registers: Deref<Target = [u8; HLL_REGISTERS_COUNT]> + DerefMut;
    fn allocate_registers(&self) -> Result<Self::Registers, HllError>;
}
#[derive(Clone, Copy, Default)]
pub struct LegacyHllRegisterAllocator;
impl HllRegisterAllocator for LegacyHllRegisterAllocator {
    type Registers = Box<[u8; HLL_REGISTERS_COUNT]>;
    fn allocate_registers(&self) -> Result<Self::Registers, HllError> {
        Ok(Box::new([0u8; HLL_REGISTERS_COUNT]))
    }
}
impl HllRegisterAllocator for crate::aggregate_host_allocator::HostAggregateAllocator {
    type Registers = allocator_api2::boxed::Box<[u8; HLL_REGISTERS_COUNT], Self>;
    fn allocate_registers(&self) -> Result<Self::Registers, HllError> {
        allocator_api2::boxed::Box::try_new_in([0u8; HLL_REGISTERS_COUNT], self.clone())
            .map_err(|_| HllError::Kernel(self.take_failure()))
    }
}
pub struct HllRawState<A: HllRegisterAllocator = LegacyHllRegisterAllocator> {
    pub has_value: bool,
    pub registers: Option<A::Registers>,
    pub allocator: A,
}
impl<A: HllRegisterAllocator> HllRawState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            has_value: false,
            registers: None,
            allocator,
        }
    }
    pub fn clear(&mut self) {
        self.registers = None;
        self.has_value = false;
    }
}
impl Default for HllRawState {
    fn default() -> Self {
        Self::new(LegacyHllRegisterAllocator)
    }
}
pub fn ensure_registers<'a, A: HllRegisterAllocator>(
    state: &'a mut HllRawState<A>,
    work: &mut HllWork<'_, '_>,
) -> Result<&'a mut [u8; HLL_REGISTERS_COUNT], HllError> {
    if state.registers.is_none() {
        work.flush()?;
        state.registers = Some(state.allocator.allocate_registers()?);
        work.flush()?;
    }
    Ok(state.registers.as_mut().unwrap().deref_mut())
}
pub fn update_state_register_from_hash<A: HllRegisterAllocator>(
    state: &mut HllRawState<A>,
    hash_value: u64,
    work: &mut HllWork<'_, '_>,
) -> Result<(), HllError> {
    if hash_value == 0 {
        return Ok(());
    }
    state.has_value = true;
    let registers = ensure_registers(state, work)?;
    update_register_from_hash(registers, hash_value);
    work.step()?;
    Ok(())
}
pub fn hash_bytes_for_hll_observed(
    bytes: &[u8],
    work: &mut HllWork<'_, '_>,
) -> Result<u64, HllError> {
    crate::hll::murmur_hash64a_observed(bytes, MURMUR_SEED, &mut || work.step())
}
pub fn estimate_cardinality<A: HllRegisterAllocator>(
    state: &HllRawState<A>,
    work: &mut HllWork<'_, '_>,
) -> Result<i64, HllError> {
    if !state.has_value {
        return Ok(0);
    }
    let Some(registers) = state.registers.as_ref() else {
        return Ok(0);
    };
    estimate_cardinality_from_registers_observed(registers, work)
}
pub fn cardinality_from_serialized_hll(
    bytes: &[u8],
    work: &mut HllWork<'_, '_>,
) -> Result<i64, HllError> {
    let mut state = HllRawState {
        has_value: true,
        ..HllRawState::default()
    };
    merge_hll_bytes(&mut state, bytes, work)?;
    estimate_cardinality(&state, work)
}

pub fn update_register_from_hash(registers: &mut [u8; HLL_REGISTERS_COUNT], hash_value: u64) {
    if hash_value == 0 {
        return;
    }
    let idx = (hash_value % HLL_REGISTERS_COUNT as u64) as usize;
    let mut shifted = hash_value >> HLL_COLUMN_PRECISION;
    shifted |= 1_u64 << (64 - HLL_COLUMN_PRECISION);
    let rank = shifted.trailing_zeros() as u8 + 1;
    if registers[idx] < rank {
        registers[idx] = rank;
    }
}

fn merge_as_opaque_payload<A: HllRegisterAllocator>(
    state: &mut HllRawState<A>,
    bytes: &[u8],
    work: &mut HllWork<'_, '_>,
) -> Result<(), HllError> {
    let hash = hash_bytes_for_hll_observed(bytes, work)?;
    update_state_register_from_hash(state, hash, work)?;
    Ok(())
}

pub fn merge_hll_bytes<A: HllRegisterAllocator>(
    state: &mut HllRawState<A>,
    bytes: &[u8],
    work: &mut HllWork<'_, '_>,
) -> Result<(), HllError> {
    if bytes.is_empty() {
        return Err("hll_raw merge payload is empty".to_string().into());
    }
    match bytes[0] {
        HLL_DATA_EMPTY => Ok(()),
        HLL_DATA_EXPLICIT => {
            if bytes.len() < 2 {
                return Err("hll_raw EXPLICIT payload is malformed".to_string().into());
            }
            let count = bytes[1] as usize;
            let expected = 2 + count * 8;
            if bytes.len() != expected {
                // Keep query running for non-standard HLL payloads (for example ds_hll states)
                // by folding unknown bytes into a deterministic hash bucket.
                merge_as_opaque_payload(state, bytes, work)?;
                return Ok(());
            }
            let mut pos = 2usize;
            for _ in 0..count {
                let hash = u64::from_le_bytes(
                    bytes[pos..pos + 8]
                        .try_into()
                        .map_err(|_| "hll_raw decode EXPLICIT hash failed".to_string())?,
                );
                pos += 8;
                update_state_register_from_hash(state, hash, work)?;
                work.step()?;
            }
            Ok(())
        }
        HLL_DATA_SPARSE => {
            if bytes.len() < 5 {
                return Err("hll_raw SPARSE payload is malformed".to_string().into());
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "hll_raw decode SPARSE count failed".to_string())?,
            ) as usize;
            let expected = 5 + count * 3;
            if bytes.len() != expected {
                merge_as_opaque_payload(state, bytes, work)?;
                return Ok(());
            }
            let mut pos = 5usize;
            let mut has_non_zero = false;
            for _ in 0..count {
                let idx = u16::from_le_bytes(
                    bytes[pos..pos + 2]
                        .try_into()
                        .map_err(|_| "hll_raw decode SPARSE index failed".to_string())?,
                ) as usize;
                pos += 2;
                if idx >= HLL_REGISTERS_COUNT {
                    merge_as_opaque_payload(state, bytes, work)?;
                    return Ok(());
                }
                let value = bytes[pos];
                pos += 1;
                work.step()?;
                if value > 0 {
                    has_non_zero = true;
                    let registers = ensure_registers(state, work)?;
                    if registers[idx] < value {
                        registers[idx] = value;
                    }
                }
            }
            if has_non_zero {
                state.has_value = true;
            }
            Ok(())
        }
        HLL_DATA_FULL => {
            let expected = 1 + HLL_REGISTERS_COUNT;
            if bytes.len() != expected {
                merge_as_opaque_payload(state, bytes, work)?;
                return Ok(());
            }
            let mut has_non_zero = false;
            for (idx, value) in bytes[1..].iter().enumerate() {
                work.step()?;
                if *value > 0 {
                    has_non_zero = true;
                    let registers = ensure_registers(state, work)?;
                    if registers[idx] < *value {
                        registers[idx] = *value;
                    }
                }
            }
            if has_non_zero {
                state.has_value = true;
            }
            Ok(())
        }
        _ => {
            merge_as_opaque_payload(state, bytes, work)?;
            Ok(())
        }
    }
}

pub fn serialize_hll_state<A: HllRegisterAllocator>(
    state: &HllRawState<A>,
    work: &mut HllWork<'_, '_>,
) -> Result<Option<Vec<u8>>, HllError> {
    if !state.has_value {
        return Ok(None);
    }
    // A non-null input that contributed no register updates (e.g. merging an
    // HLL_DATA_EMPTY payload) is still a non-null observation, so emit a
    // valid empty HLL payload rather than NULL.
    let Some(registers) = state.registers.as_ref() else {
        return Ok(Some(encode_hll_empty()));
    };
    let mut non_zero = 0;
    for value in registers.iter() {
        if *value > 0 {
            non_zero += 1;
        }
        work.step()?;
    }
    if non_zero == 0 {
        return Ok(Some(encode_hll_empty()));
    }

    if non_zero > HLL_SPARSE_THRESHOLD {
        work.flush()?;
        let mut out = Vec::with_capacity(1 + HLL_REGISTERS_COUNT);
        out.push(HLL_DATA_FULL);
        out.extend_from_slice(&registers[..]);
        work.flush()?;
        return Ok(Some(out));
    }

    work.flush()?;
    let mut out = Vec::with_capacity(5 + non_zero * 3);
    work.flush()?;
    out.push(HLL_DATA_SPARSE);
    out.extend_from_slice(&(non_zero as u32).to_le_bytes());
    for (idx, value) in registers.iter().enumerate() {
        work.step()?;
        if *value > 0 {
            out.extend_from_slice(&(idx as u16).to_le_bytes());
            out.push(*value);
        }
    }
    Ok(Some(out))
}

pub fn estimate_cardinality_from_registers_observed(
    registers: &[u8; HLL_REGISTERS_COUNT],
    work: &mut HllWork<'_, '_>,
) -> Result<i64, HllError> {
    let num_streams = HLL_REGISTERS_COUNT as f64;
    let alpha = match HLL_REGISTERS_COUNT {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / num_streams),
    };

    let mut harmonic_mean = 0.0f64;
    let mut zero_registers = 0usize;
    for register in registers.iter() {
        harmonic_mean += 2_f64.powi(-(*register as i32));
        work.step()?;
        if *register == 0 {
            zero_registers += 1;
        }
    }

    if harmonic_mean == 0.0 {
        return Ok(0);
    }

    let mut estimate = alpha * num_streams * num_streams / harmonic_mean;
    if estimate <= num_streams * 2.5 && zero_registers != 0 {
        estimate = num_streams * (num_streams / zero_registers as f64).ln();
    } else if HLL_REGISTERS_COUNT == 16 * 1024 && estimate < 72_000.0 {
        // Keep parity with StarRocks' correction in be/src/types/hll.cpp.
        let bias = 5.9119e-18 * estimate.powi(4) - 1.4253e-12 * estimate.powi(3)
            + 1.2940e-7 * estimate.powi(2)
            - 5.2921e-3 * estimate
            + 83.3216;
        estimate -= estimate * (bias / 100.0);
    }

    Ok(estimate.max(0.0).round() as i64)
}

pub fn hash_array_value_for_hll_observed(
    array: &ArrayRef,
    row: usize,
    work: &mut HllWork<'_, '_>,
) -> Result<Option<u64>, HllError> {
    let mut hash = |bytes: &[u8]| hash_bytes_for_hll_observed(bytes, work);
    if row >= array.len() {
        return Err(format!("hll_raw row {row} out of bounds for len {}", array.len()).into());
    }
    if array.is_null(row) {
        return Ok(None);
    }

    macro_rules! hash_primitive_value {
        ($array_ty:ty, $name:literal) => {{
            let arr = array
                .as_any()
                .downcast_ref::<$array_ty>()
                .ok_or_else(|| format!("failed to downcast to {}", $name))?;
            Ok(Some(hash(&arr.value(row).to_le_bytes())?))
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            let value = if arr.value(row) { 1u8 } else { 0u8 };
            Ok(Some(hash(&[value])?))
        }
        DataType::Int8 => hash_primitive_value!(Int8Array, "Int8Array"),
        DataType::Int16 => hash_primitive_value!(Int16Array, "Int16Array"),
        DataType::Int32 => hash_primitive_value!(Int32Array, "Int32Array"),
        DataType::Int64 => hash_primitive_value!(Int64Array, "Int64Array"),
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            Ok(Some(hash(
                &canonical_f32_bits_for_hll(arr.value(row)).to_le_bytes(),
            )?))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            Ok(Some(hash(
                &canonical_f64_bits_for_hll(arr.value(row)).to_le_bytes(),
            )?))
        }
        DataType::Date32 => hash_primitive_value!(Date32Array, "Date32Array"),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => hash_primitive_value!(TimestampSecondArray, "TimestampSecondArray"),
            TimeUnit::Millisecond => {
                hash_primitive_value!(TimestampMillisecondArray, "TimestampMillisecondArray")
            }
            TimeUnit::Microsecond => {
                hash_primitive_value!(TimestampMicrosecondArray, "TimestampMicrosecondArray")
            }
            TimeUnit::Nanosecond => {
                hash_primitive_value!(TimestampNanosecondArray, "TimestampNanosecondArray")
            }
        },
        DataType::Decimal128(_, _) => hash_primitive_value!(Decimal128Array, "Decimal128Array"),
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            Ok(Some(hash(arr.value(row).as_bytes())?))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "failed to downcast to LargeStringArray".to_string())?;
            Ok(Some(hash(arr.value(row).as_bytes())?))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
            Ok(Some(hash(arr.value(row))?))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            Ok(Some(hash(arr.value(row))?))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
            Ok(Some(hash(arr.value(row))?))
        }
        other => Err(format!("hll_raw does not support input type {:?}", other).into()),
    }
}

fn canonical_f32_bits_for_hll(value: f32) -> u32 {
    if value.is_nan() {
        0x7FC0_0000
    } else if value == 0.0 {
        0.0f32.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_f64_bits_for_hll(value: f64) -> u64 {
    if value.is_nan() {
        0x7FF8_0000_0000_0000
    } else if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

#[cfg(test)]
#[path = "aggregate_hll_core_tests.rs"]
mod tests;
