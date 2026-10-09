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

//! Borrow actual immutable Arrow structure before its allocating to_data route.
//! These are table facts, not a value decoder, retained payload grant, SQL type
//! coercion, or a row-dependent string estimate.
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::KernelFailure;
use arrow_array::{
    Array, BinaryViewArray, DictionaryArray, FixedSizeListArray, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, MapArray, RunArray, StringViewArray, StructArray,
    UnionArray,
};
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_schema::{DataType, UnionMode};

fn concrete<T: Array + 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        invalid("source backing geometry requires its actual built-in Arrow carrier")
    })
}
fn plus(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}

/// A borrowed node from the SAME source traversal. No ArrayData, payload,
/// metadata clone, or authorization is created by this loan.
pub(crate) struct BorrowedSourceNode<'a> {
    pub(crate) array: &'a dyn Array,
    pub(crate) data_buffers: usize,
    pub(crate) child_count: usize,
    pub(crate) copy_nulls: bool,
}
pub(crate) trait BorrowedSourceObservation<'source> {
    /// The observer owns any work or allocation it elects to perform. The
    /// source author adds neither a scope nor an extra success checkpoint.
    fn node(
        &mut self,
        node: BorrowedSourceNode<'source>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure>;
}

/// Use ONE custody metadata arithmetic with borrowed actual table counts. The
/// returned bytes cover metadata construction only; existing input lease owners
/// retain the original payload independently and are never replaced by this sum.
fn source_metadata_graph<'source>(
    array: &'source dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
    copy_nulls: bool,
    observer: &mut Option<&mut dyn BorrowedSourceObservation<'source>>,
) -> Result<usize, KernelFailure> {
    work.step()?;
    let mut child_bytes = 0;
    let buffers;
    let children;
    macro_rules! child {
        ($array:expr) => {{
            child_bytes = plus(
                child_bytes,
                source_metadata_graph($array.as_ref(), work, copy_nulls, observer)?,
            )?;
        }};
    }
    macro_rules! dictionary {
        ($key:ty) => {{
            child!(concrete::<DictionaryArray<$key>>(array)?.values());
        }};
    }
    match array.data_type() {
        DataType::Null => {
            buffers = 0;
            children = 0;
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            buffers = 2;
            children = 0;
        }
        DataType::Utf8View => {
            buffers = plus(1, concrete::<StringViewArray>(array)?.data_buffers().len())?;
            children = 0;
        }
        DataType::BinaryView => {
            buffers = plus(1, concrete::<BinaryViewArray>(array)?.data_buffers().len())?;
            children = 0;
        }
        DataType::List(_) => {
            buffers = 1;
            children = 1;
            child!(concrete::<ListArray>(array)?.values());
        }
        DataType::LargeList(_) => {
            buffers = 1;
            children = 1;
            child!(concrete::<LargeListArray>(array)?.values());
        }
        DataType::ListView(_) => {
            buffers = 2;
            children = 1;
            child!(concrete::<ListViewArray>(array)?.values());
        }
        DataType::LargeListView(_) => {
            buffers = 2;
            children = 1;
            child!(concrete::<LargeListViewArray>(array)?.values());
        }
        DataType::FixedSizeList(_, _) => {
            buffers = 0;
            children = 1;
            child!(concrete::<FixedSizeListArray>(array)?.values());
        }
        DataType::Struct(_) => {
            let columns = concrete::<StructArray>(array)?.columns();
            buffers = 0;
            children = columns.len();
            for column in columns {
                child!(column);
            }
        }
        DataType::Map(_, _) => {
            buffers = 1;
            children = 1;
            child_bytes = source_metadata_graph(
                concrete::<MapArray>(array)?.entries(),
                work,
                copy_nulls,
                observer,
            )?;
        }
        DataType::Dictionary(key, _) => {
            buffers = 1;
            children = 1;
            match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => return Err(invalid("source dictionary has no actual Arrow key carrier")),
            }
        }
        DataType::Union(fields, mode) => {
            let array = concrete::<UnionArray>(array)?;
            buffers = if *mode == UnionMode::Dense { 2 } else { 1 };
            children = fields.len();
            for (id, _) in fields.iter() {
                child!(array.child(id));
            }
        }
        DataType::RunEndEncoded(ends, _) => {
            buffers = 0;
            children = 2;
            // Arrow's run-end child is a primitive array with one data buffer
            // and no null bitmap. Values use their own actual dynamic structure.
            child_bytes =
                crate::arrow_result_custody::custody_node_metadata_upper_bound(1, false, 0)?;
            match ends.data_type() {
                DataType::Int16 => child!(concrete::<RunArray<Int16Type>>(array)?.values()),
                DataType::Int32 => child!(concrete::<RunArray<Int32Type>>(array)?.values()),
                DataType::Int64 => child!(concrete::<RunArray<Int64Type>>(array)?.values()),
                _ => return Err(invalid("source run-end has no actual Arrow index carrier")),
            }
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(..)
        | DataType::Time64(..)
        | DataType::Duration(..)
        | DataType::Interval(..)
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => {
            buffers = 1;
            children = 0;
        }
    }
    if let Some(observer) = observer.as_deref_mut() {
        observer.node(
            BorrowedSourceNode {
                array,
                data_buffers: buffers,
                child_count: children,
                copy_nulls,
            },
            work,
        )?;
    }
    let own = crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
        array.data_type(),
        buffers,
        copy_nulls || array.nulls().is_some(),
        children,
    )?;
    plus(own, child_bytes)
}

pub(crate) fn source_metadata_bytes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    // Four envelopes cover pinned Arrow to_data transient table growth,
    // clone/into_builder/make_array coexistence, and custom-buffer metadata.
    // Apply it ONCE to the complete actual graph, not exponentially by depth.
    source_metadata_graph(array, work, false, &mut None)?
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)
}

/// The SAME actual source graph bounds copy metadata. Any copied node may gain
/// validity from actual nullable indices even when its source has no bitmap.
/// Dictionary/View children remain source-owned; counting their metadata does
/// not admit or charge their already-owned payload.
pub(crate) fn copy_metadata_bytes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    source_metadata_graph(array, work, true, &mut None)?
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)
}

/// Explicit resource observation borrows the existing parent work. Legacy
/// entrypoints keep their original None path and original checkpoint trace.
pub(crate) fn observe_source_metadata<'source>(
    array: &'source dyn Array,
    copy_nulls: bool,
    work: &mut EvaluationCheckpoints<'_>,
    observer: &mut dyn BorrowedSourceObservation<'source>,
) -> Result<usize, KernelFailure> {
    source_metadata_graph(array, work, copy_nulls, &mut Some(observer))?
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)
}

#[cfg(test)]
#[path = "array_backing_geometry_observation_tests.rs"]
mod observation_tests;
