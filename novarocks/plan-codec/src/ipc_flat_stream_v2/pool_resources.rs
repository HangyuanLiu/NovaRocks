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

//! The raw-to-post ConstantPolicy bridge for a checked flat stream. Its body
//! recipe is public read_record_batch with Buffer::from_slice_ref: body and
//! at most one full typed-buffer alignment copy, then zero-row empty offsets.
//! This is not total reader/container/error allocation or work admission.

use super::FlatConstantStream;
use crate::ipc_flat_batch_v2::{Layout as FlatLayout, layout};
use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::DataType;
use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, FlatConstantResourceBounds, FlatConstantResourceInput,
    preflight_flat_pool_resources,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{alloc::Layout, fmt};

#[derive(Debug)]
pub enum FlatPoolResourceError {
    Shape(TypeCodecError),
    Constant(ConstantError),
    Control(CompileControlError),
}
impl From<TypeCodecError> for FlatPoolResourceError {
    fn from(error: TypeCodecError) -> Self {
        match error {
            TypeCodecError::Control(error) => Self::Control(error),
            error => Self::Shape(error),
        }
    }
}
impl From<ConstantError> for FlatPoolResourceError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(error) => Self::Control(error),
            error => Self::Constant(error),
        }
    }
}
impl From<CompileControlError> for FlatPoolResourceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for FlatPoolResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Control(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for FlatPoolResourceError {}

/// Requested payload capacities for this specific safe reader recipe, and the
/// original constant owner's validation bound. Original input remains live.
/// Structural owners, transient diagnostics and reader work are not included.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatPoolResourceProjection {
    pub owned_body_capacity_bytes: usize,
    /// Conservative: a native width may exceed its Rust alignment. Charging
    /// a repair in that case is safe, without inventing an alignment matrix.
    pub alignment_repair_possible: bool,
    pub alignment_repair_capacity_bytes_upper_bound: usize,
    pub empty_offset_capacity_bytes: usize,
    pub constant: FlatConstantResourceBounds,
}
fn shape(message: &'static str) -> FlatPoolResourceError {
    TypeCodecError::InvalidShape(message).into()
}
fn add(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_add(b)
        .ok_or_else(|| shape("constant payload bound overflow"))
}
fn payload_layout(bytes: usize) -> Result<(), FlatPoolResourceError> {
    Layout::from_size_align(bytes, arrow_buffer::alloc::ALIGNMENT)
        .map(|_| ())
        .map_err(|_| shape("constant payload allocation layout is not representable"))
}
fn rounded_capacity(bytes: usize) -> Result<usize, FlatPoolResourceError> {
    let rounded = add(bytes, 63)? & !63;
    payload_layout(rounded)?;
    Ok(rounded)
}
fn u64_extent(bytes: usize) -> Result<u64, FlatPoolResourceError> {
    u64::try_from(bytes).map_err(|_| shape("constant payload bound exceeds u64"))
}

impl FlatConstantStream<'_, '_> {
    /// Checks the original complete Field/FVT and ConstantPolicy before any
    /// reader output allocation. The result only covers this pool bridge;
    /// a complete reader allocation/work model must precede materialization.
    pub fn preflight_pool_resources(
        &self,
        value_type: &FunctionValueType,
        policy: ConstantPolicy,
        control: &dyn PureCompileControl,
    ) -> Result<FlatPoolResourceProjection, FlatPoolResourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = self.pool_resources(value_type, policy, &mut work);
        if matches!(&result, Err(FlatPoolResourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn pool_resources(
        &self,
        value_type: &FunctionValueType,
        policy: ConstantPolicy,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FlatPoolResourceProjection, FlatPoolResourceError> {
        let geometry = self.geometry();
        let flat = layout(self.field().data_type())?;
        work.step()?;
        let body_capacity = rounded_capacity(geometry.body_bytes)?;
        work.step()?;
        let descriptors = self
            .record_batch()
            .buffers()
            .ok_or_else(|| shape("constant batch lacks buffers"))?;
        let (typed_width, offsets) = match flat {
            FlatLayout::Offsets(width) => (Some(width), Some(width)),
            FlatLayout::Views => (Some(std::mem::size_of::<u128>()), None),
            FlatLayout::Fixed(width)
                if !matches!(self.field().data_type(), DataType::FixedSizeBinary(_))
                    && width > 1 =>
            {
                (Some(width), None)
            }
            _ => (None, None),
        };
        let mut repair_possible = false;
        let mut repair_capacity = 0;
        let mut empty_offsets = 0;
        if let Some(width) = typed_width {
            let descriptor = descriptors.get(1); // the checked layout author proves this index
            let offset = usize::try_from(descriptor.offset())
                .map_err(|_| shape("constant buffer offset is not representable"))?;
            let length = usize::try_from(descriptor.length())
                .map_err(|_| shape("constant buffer length is not representable"))?;
            // Rust's native size is a multiple of its alignment. If the owned
            // body cannot guarantee width alignment, conservatively charge a
            // repair, including an empty descriptor's possible Bytes Arc.
            repair_possible = !arrow_buffer::alloc::ALIGNMENT.is_multiple_of(width)
                || !offset.is_multiple_of(width);
            if repair_possible {
                repair_capacity = rounded_capacity(length)?;
            }
            if let Some(width) = offsets
                && geometry.rows == 0
                && length == 0
            {
                empty_offsets = width;
                payload_layout(width)?; // from_len_zeroed uses exact width, not round64
            }
            work.step()?;
        }
        let mut visits = add(geometry.described_buffer_bytes, empty_offsets)?;
        if matches!(
            self.field().data_type(),
            DataType::Utf8 | DataType::LargeUtf8
        ) {
            let values = usize::try_from(descriptors.get(2).length())
                .map_err(|_| shape("constant UTF8 extent is not representable"))?;
            visits = add(visits, values)?; // successful whole-buffer/fallback scan
            work.step()?;
        }
        let retained = if matches!(flat, FlatLayout::Null) {
            0
        } else {
            add(add(body_capacity, repair_capacity)?, empty_offsets)?
        };
        let input = FlatConstantResourceInput {
            rows: u64_extent(geometry.rows)?,
            buffer_count_upper_bound: u64_extent(geometry.buffer_descriptors)?,
            buffer_visits_bytes_upper_bound: u64_extent(visits)?,
            retained_buffer_capacity_bytes_upper_bound: u64_extent(retained)?,
            view_validation_bytes_upper_bound: u64_extent(geometry.view_validation_bytes)?,
        };
        work.flush()?;
        let constant = preflight_flat_pool_resources(
            self.field(),
            value_type,
            input,
            policy,
            CompilePhase::Decode,
            work.control(),
        )?;
        Ok(FlatPoolResourceProjection {
            owned_body_capacity_bytes: body_capacity,
            alignment_repair_possible: repair_possible,
            alignment_repair_capacity_bytes_upper_bound: repair_capacity,
            empty_offset_capacity_bytes: empty_offsets,
            constant,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn payload_layout_and_rounding_refuse_before_opaque_allocation() {
        assert_eq!(rounded_capacity(0).unwrap(), 0);
        assert_eq!(rounded_capacity(1).unwrap(), 64);
        assert_eq!(rounded_capacity(64).unwrap(), 64);
        assert_eq!(rounded_capacity(65).unwrap(), 128);
        let largest = (isize::MAX as usize) & !(arrow_buffer::alloc::ALIGNMENT - 1);
        assert_eq!(rounded_capacity(largest).unwrap(), largest);
        for bytes in [
            largest + 1,
            isize::MAX as usize,
            usize::MAX - 62,
            usize::MAX,
        ] {
            assert!(matches!(
                rounded_capacity(bytes),
                Err(FlatPoolResourceError::Shape(_))
            ));
        }
        assert!(matches!(
            add(usize::MAX, 1),
            Err(FlatPoolResourceError::Shape(_))
        ));
        assert!(payload_layout(4).is_ok());
        assert!(payload_layout(8).is_ok());
    }
}
