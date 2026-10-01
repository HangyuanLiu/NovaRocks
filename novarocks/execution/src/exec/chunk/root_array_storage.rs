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
//! Borrowed storage inspection for the closed standard Arrow carrier set.
//!
//! This is deliberately only the array/Buffer portion of the root input
//! proof. Field/DataType/schema metadata and source ownership receipts are
//! separate prerequisites; this value must never stand in for a Chunk proof.

use std::mem::{size_of, size_of_val};

use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_result_contract::RootProfileV1;

/// No String allocation is needed on the inspection/failure path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootArrayStorageError {
    UnsupportedCarrier,
    UnknownBufferOwner,
    CapacityExceeded,
    WorkExceeded,
}

/// Finite structural work. This never scans rows, offsets, dictionary keys,
/// values, or computes logical nulls, even for CountOnly.
#[derive(Clone, Copy, Debug)]
pub struct RootArrayStorageLimits {
    pub bytes: usize,
    pub nodes: usize,
    pub depth: usize,
}

/// Sum full standard allocation capacities, including sliced-away backing and
/// complete dictionary values. Aliases may be counted more than once, making
/// the result a conservative upper bound. No growable identity table, Arrow
/// to_data(), hydration, or allocation is used. Variable DataType/Field/schema
/// allocations are excluded and require a separate construction-origin proof.
pub fn borrowed_root_array_storage(
    array: &dyn Array,
    limits: RootArrayStorageLimits,
) -> Result<usize, RootArrayStorageError> {
    // Callers may tighten these limits; they cannot enlarge the frozen
    // profile's structural work or original input backing allowance.
    let limits = RootArrayStorageLimits {
        bytes: limits.bytes.min(96 * 1024 * 1024),
        nodes: limits.nodes.min(2 * RootProfileV1::SCHEMA_TYPE_NODES),
        depth: limits.depth.min(RootProfileV1::MAX_DEPTH),
    };
    let mut state = Inspection {
        limits,
        bytes: 0,
        nodes: 0,
    };
    state.array(array, 0)?;
    Ok(state.bytes)
}

/// Inspect one batch's complete column-vector backing and all standard array
/// storage under a single shared work/byte budget. Schema and DataType metadata
/// remain separate source-origin obligations, exactly as for one array above.
pub fn borrowed_root_batch_storage(
    batch: &RecordBatch,
    limits: RootArrayStorageLimits,
) -> Result<usize, RootArrayStorageError> {
    if batch.num_columns() > RootProfileV1::MAX_COLUMNS {
        return Err(RootArrayStorageError::WorkExceeded);
    }
    let limits = RootArrayStorageLimits {
        bytes: limits.bytes.min(96 * 1024 * 1024),
        nodes: limits.nodes.min(2 * RootProfileV1::SCHEMA_TYPE_NODES),
        depth: limits.depth.min(RootProfileV1::MAX_DEPTH),
    };
    let mut state = Inspection {
        limits,
        bytes: 0,
        nodes: 0,
    };
    state.charge(size_of::<RecordBatch>())?;
    state.charge(
        batch
            .columns_capacity()
            .checked_mul(size_of::<ArrayRef>())
            .ok_or(RootArrayStorageError::CapacityExceeded)?,
    )?;
    for array in batch.columns() {
        state.array(array.as_ref(), 0)?;
    }
    Ok(state.bytes)
}

struct Inspection {
    limits: RootArrayStorageLimits,
    bytes: usize,
    nodes: usize,
}

impl Inspection {
    fn charge(&mut self, bytes: usize) -> Result<(), RootArrayStorageError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.limits.bytes)
            .ok_or(RootArrayStorageError::CapacityExceeded)?;
        Ok(())
    }

    fn buffer(&mut self, buffer: &Buffer) -> Result<(), RootArrayStorageError> {
        let capacity = buffer
            .standard_allocation_capacity()
            .ok_or(RootArrayStorageError::UnknownBufferOwner)?;
        let metadata = buffer
            .standard_owner_metadata_size()
            .ok_or(RootArrayStorageError::UnknownBufferOwner)?;
        self.charge(capacity)?;
        self.charge(metadata)
    }

    fn array(&mut self, array: &dyn Array, depth: usize) -> Result<(), RootArrayStorageError> {
        self.enter_node(depth)?;
        // The standard carriers keep physical nulls directly. Do not call
        // logical_nulls(), which can allocate/scan complete dictionaries.
        // Only inspect nulls after the concrete standard downcast below.
        macro_rules! primitive {
            ($ty:ty) => {{
                let a = exact::<$ty>(array)?;
                self.object(a)?;
                self.buffer(a.values().inner())?;
                self.nulls(a)?;
            }};
        }
        macro_rules! bytes {
            ($ty:ty) => {{
                let a = exact::<$ty>(array)?;
                self.object(a)?;
                self.buffer(a.offsets().inner().inner())?;
                self.buffer(a.values())?;
                self.nulls(a)?;
            }};
        }
        macro_rules! list {
            ($ty:ty) => {{
                let a = exact::<$ty>(array)?;
                self.object(a)?;
                self.buffer(a.offsets().inner().inner())?;
                self.nulls(a)?;
                self.array(a.values().as_ref(), depth + 1)?;
            }};
        }
        match array.data_type() {
            DataType::Null => {
                self.object(exact::<NullArray>(array)?)?;
            }
            DataType::Boolean => {
                let a = exact::<BooleanArray>(array)?;
                self.object(a)?;
                self.buffer(a.values().inner())?;
                self.nulls(a)?;
            }
            DataType::Int8 => primitive!(Int8Array),
            DataType::Int16 => primitive!(Int16Array),
            DataType::Int32 => primitive!(Int32Array),
            DataType::Int64 => primitive!(Int64Array),
            DataType::UInt8 => primitive!(UInt8Array),
            DataType::UInt16 => primitive!(UInt16Array),
            DataType::UInt32 => primitive!(UInt32Array),
            DataType::UInt64 => primitive!(UInt64Array),
            DataType::Float32 => primitive!(Float32Array),
            DataType::Float64 => primitive!(Float64Array),
            DataType::Decimal128(_, _) => primitive!(Decimal128Array),
            DataType::Decimal256(_, _) => primitive!(Decimal256Array),
            DataType::Date32 => primitive!(Date32Array),
            DataType::Date64 => primitive!(Date64Array),
            DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray),
            DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray),
            DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray),
            DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray),
            DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray),
            DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondArray),
            DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondArray),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondArray),
            DataType::Utf8 => bytes!(StringArray),
            DataType::LargeUtf8 => bytes!(LargeStringArray),
            DataType::Binary => bytes!(BinaryArray),
            DataType::LargeBinary => bytes!(LargeBinaryArray),
            DataType::FixedSizeBinary(_) => {
                let a = exact::<FixedSizeBinaryArray>(array)?;
                self.object(a)?;
                self.buffer(a.values())?;
                self.nulls(a)?;
            }
            DataType::List(_) => list!(ListArray),
            DataType::LargeList(_) => list!(LargeListArray),
            DataType::Struct(_) => self.structure(exact::<StructArray>(array)?, depth)?,
            DataType::Map(_, _) => {
                let a = exact::<MapArray>(array)?;
                self.object(a)?;
                self.buffer(a.offsets().inner().inner())?;
                self.nulls(a)?;
                // Map entries is an inline physical Struct, not an extra
                // semantic nesting level. Count its work but let key/value
                // children consume the one Map semantic level.
                self.enter_node(depth)?;
                self.structure(a.entries(), depth)?;
            }
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32
                    && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
            {
                let a = exact::<DictionaryArray<arrow::datatypes::Int32Type>>(array)?;
                self.object(a)?;
                self.array(a.keys(), depth)?;
                // All values retain their original backing, irrespective of
                // which keys the visible root rows happen to reference.
                self.array(a.values().as_ref(), depth)?;
            }
            _ => return Err(RootArrayStorageError::UnsupportedCarrier),
        }
        Ok(())
    }

    fn enter_node(&mut self, depth: usize) -> Result<(), RootArrayStorageError> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= self.limits.nodes)
            .ok_or(RootArrayStorageError::WorkExceeded)?;
        if depth > self.limits.depth {
            return Err(RootArrayStorageError::WorkExceeded);
        }
        Ok(())
    }

    fn object<T>(&mut self, value: &T) -> Result<(), RootArrayStorageError> {
        // Includes Arc control words/alignment even for inline keys/entries;
        // overcount is intentional and does not infer an allocation identity.
        self.charge(size_of_val(value))?;
        self.charge(4 * size_of::<usize>())
    }

    fn nulls(&mut self, array: &dyn Array) -> Result<(), RootArrayStorageError> {
        if let Some(nulls) = array.nulls() {
            self.buffer(nulls.buffer())?;
        }
        Ok(())
    }

    fn structure(
        &mut self,
        array: &StructArray,
        depth: usize,
    ) -> Result<(), RootArrayStorageError> {
        self.object(array)?;
        self.charge(
            array
                .columns_capacity()
                .checked_mul(size_of::<ArrayRef>())
                .ok_or(RootArrayStorageError::CapacityExceeded)?,
        )?;
        self.nulls(array)?;
        for child in array.columns() {
            self.array(child.as_ref(), depth + 1)?;
        }
        Ok(())
    }
}

fn exact<T: Array + 'static>(array: &dyn Array) -> Result<&T, RootArrayStorageError> {
    let value = array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(RootArrayStorageError::UnsupportedCarrier)?;
    // Do not accept a wrapper exposing a different embedded standard array
    // through as_any(): its outer owner would not be represented by this proof.
    if size_of_val(array) != size_of::<T>()
        || std::ptr::from_ref(array).cast::<()>() != std::ptr::from_ref(value).cast::<()>()
    {
        return Err(RootArrayStorageError::UnsupportedCarrier);
    }
    Ok(value)
}
