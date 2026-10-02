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

//! Shared format/extent author for Arrow constant broadcasts and selected copies.
//! Representability checks do not authorize allocations or mint memory grants.

use arrow::array::{
    Array, ArrayData, ArrayRef, FixedSizeListArray, GenericByteArray, GenericListArray,
    GenericListViewArray, MapArray, OffsetSizeTrait, RunArray, StructArray, UInt32Array,
    UnionArray,
};
use arrow::datatypes::{
    ByteArrayType, DataType, Int16Type, Int32Type, Int64Type, RunEndIndexType, UnionMode,
};
use novarocks_functions::{ConstantValue, KernelFailure};
use std::ops::Range;

#[derive(Debug)]
pub(super) enum CopyError {
    Extent,
    Invalid(&'static str),
    Unsupported(DataType),
    Control(KernelFailure),
    Arrow(arrow::error::ArrowError),
}
impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Extent => {
                f.write_str("constant broadcast output extent exceeds its Arrow format")
            }
            Self::Invalid(message) => f.write_str(message),
            Self::Unsupported(ty) => write!(
                f,
                "constant broadcast does not support Arrow carrier {ty:?}"
            ),
            Self::Control(error) => write!(f, "{error:?}"),
            Self::Arrow(error) => write!(f, "constant pool broadcast failed: {error}"),
        }
    }
}
struct CopyObservation<'a>(&'a mut dyn FnMut(bool) -> Result<(), KernelFailure>);
impl CopyObservation<'_> {
    fn step(&mut self) -> Result<(), CopyError> {
        (self.0)(false).map_err(CopyError::Control)
    }
    fn boundary(&mut self) -> Result<(), CopyError> {
        (self.0)(true).map_err(CopyError::Control)
    }
}

fn add(a: usize, b: usize) -> Result<usize, CopyError> {
    a.checked_add(b).ok_or(CopyError::Extent)
}
fn mul(a: usize, b: usize) -> Result<usize, CopyError> {
    a.checked_mul(b).ok_or(CopyError::Extent)
}
fn limit(value: usize, maximum: usize) -> Result<(), CopyError> {
    if value > maximum {
        Err(CopyError::Extent)
    } else {
        Ok(())
    }
}
fn buffer_extent(elements: usize, width: usize) -> Result<(), CopyError> {
    // Rust allocations cannot be larger than isize::MAX even if usize fits.
    limit(mul(elements, width)?, isize::MAX as usize)
}
fn offset_max(large: bool) -> usize {
    if large {
        usize::try_from(i64::MAX).unwrap_or(usize::MAX)
    } else {
        i32::MAX as usize
    }
}

#[derive(Clone)]
struct Block {
    ranges: Vec<Range<usize>>,
    repeats: usize,
}
#[derive(Clone)]
struct Selection {
    blocks: Vec<Block>,
    nulls: usize,
}
impl Selection {
    fn len(&self, work: &mut CopyObservation<'_>) -> Result<usize, CopyError> {
        self.blocks.iter().try_fold(self.nulls, |total, block| {
            work.step()?;
            let one = block.ranges.iter().try_fold(0, |len, range| {
                work.step()?;
                add(len, range.len())
            })?;
            add(total, mul(one, block.repeats)?)
        })
    }
    fn check(&self, source_len: usize, work: &mut CopyObservation<'_>) -> Result<(), CopyError> {
        for block in &self.blocks {
            work.step()?;
            for range in &block.ranges {
                work.step()?;
                if range.start > range.end || range.end > source_len {
                    return Err(CopyError::Invalid(
                        "constant broadcast selected range is outside its source",
                    ));
                }
            }
        }
        Ok(())
    }
    fn map_ranges(
        &self,
        work: &mut CopyObservation<'_>,
        mut map: impl FnMut(
            &Range<usize>,
            &mut Vec<Range<usize>>,
            &mut CopyObservation<'_>,
        ) -> Result<(), CopyError>,
    ) -> Result<Self, CopyError> {
        self.blocks
            .iter()
            .map(|block| {
                work.step()?;
                let mut ranges = Vec::new();
                for range in &block.ranges {
                    work.step()?;
                    map(range, &mut ranges, work)?;
                }
                Ok(Block {
                    ranges,
                    repeats: block.repeats,
                })
            })
            .collect::<Result<Vec<_>, CopyError>>()
            .map(|blocks| Self { blocks, nulls: 0 })
    }
}

#[derive(Clone, Copy, PartialEq)]
enum CopyMode {
    Take { index_maximum: usize },
    Extend,
}

impl CopyMode {
    fn is_take(self) -> bool {
        matches!(self, Self::Take { .. })
    }
}

pub(super) fn broadcast(value: &ConstantValue, rows: usize) -> Result<ArrayRef, String> {
    broadcast_checked(value, rows).map_err(|error| error.to_string())
}
fn broadcast_checked(value: &ConstantValue, rows: usize) -> Result<ArrayRef, CopyError> {
    let source = value.pool().array();
    if rows == 0 {
        return Ok(source.slice(value.ordinal() as usize, 0));
    }
    let start = value.ordinal() as usize;
    let selection = Selection {
        blocks: vec![Block {
            ranges: std::iter::once(start..add(start, 1)?).collect(),
            repeats: rows,
        }],
        nulls: 0,
    };
    let mut observe = |_| Ok(());
    let mut work = CopyObservation(&mut observe);
    preflight(
        source.as_ref(),
        &selection,
        CopyMode::Take {
            index_maximum: u32::MAX as usize,
        },
        &mut work,
    )?;
    let indices = UInt32Array::from(vec![value.ordinal(); rows]);
    let output = arrow::compute::take(source.as_ref(), &indices, None).map_err(CopyError::Arrow)?;
    if !novarocks_type_contract::arrow_data_types_exact(output.data_type(), source.data_type()) {
        return Err(CopyError::Invalid(
            "constant broadcast changed the exact Arrow carrier",
        ));
    }
    Ok(output)
}

// MutableArrayData constructors reserve by the library's capacity hint before
// extending selected children. The hint follows a separate path from logical
// output, particularly for FixedSizeList; both arithmetic paths need checking.
fn mutable_capacity(
    data: &ArrayData,
    capacity: usize,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    work.step()?;
    // This models actual constructor children even when no source row will be
    // extended. Dictionary constructors validate the whole retained dictionary
    // length against the key carrier, including their off-by-one library limit.
    let ty = data.data_type();
    let child = |index: usize| {
        data.child_data().get(index).ok_or_else(|| {
            CopyError::Invalid("constant mutable-copy source has missing child data")
        })
    };
    buffer_extent(add(capacity, 1)?, 8)?;
    if let Some(width) = ty.primitive_width() {
        return buffer_extent(capacity, width);
    }
    match ty {
        DataType::FixedSizeBinary(width) => buffer_extent(
            capacity,
            usize::try_from(*width).map_err(|_| CopyError::Extent)?,
        ),
        DataType::FixedSizeList(_, width) => mutable_capacity(
            child(0)?,
            mul(
                capacity,
                usize::try_from(*width).map_err(|_| CopyError::Extent)?,
            )?,
            work,
        ),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _) => mutable_capacity(child(0)?, capacity, work),
        DataType::Struct(_) | DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => {
            for child in data.child_data() {
                mutable_capacity(child, capacity, work)?;
            }
            Ok(())
        }
        DataType::Dictionary(key, _) => {
            let maximum = match key.as_ref() {
                DataType::Int8 => i8::MAX as usize,
                DataType::Int16 => i16::MAX as usize,
                DataType::Int32 => i32::MAX as usize,
                DataType::Int64 => offset_max(true),
                DataType::UInt8 => u8::MAX as usize,
                DataType::UInt16 => u16::MAX as usize,
                DataType::UInt32 => usize::try_from(u32::MAX).unwrap_or(usize::MAX),
                DataType::UInt64 => usize::MAX,
                _ => {
                    return Err(CopyError::Invalid(
                        "constant dictionary key carrier is invalid",
                    ));
                }
            };
            limit(child(0)?.len(), maximum)?;
            // Single-source MutableArrayData retains values without constructing
            // a mutable values array. Only the dictionary key buffer expands.
            buffer_extent(capacity, key.primitive_width().ok_or(CopyError::Extent)?)
        }
        DataType::Utf8View | DataType::BinaryView => {
            // The constructor stores the variadic buffer count in u32 even if
            // this copy is empty; a single source never adds a buffer offset.
            limit(data.buffers().len().saturating_sub(1), u32::MAX as usize)?;
            buffer_extent(capacity, 16)
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Null
        | DataType::Boolean => Ok(()),
        other => Err(CopyError::Unsupported(other.clone())),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, CopyError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| CopyError::Invalid("constant source array differs from its checked carrier"))
}

fn preflight(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    work.step()?;
    selection.check(array.len(), work)?;
    let rows = selection.len(work)?;
    if mode.is_take()
        && matches!(array.data_type(), DataType::RunEndEncoded(..))
        && selection.nulls != 0
    {
        return Err(CopyError::Invalid(
            "Arrow run-end take requires non-null indices",
        ));
    }
    if mode.is_take() && matches!(array.data_type(), DataType::Union(..)) && selection.nulls != 0 {
        return Err(CopyError::Invalid(
            "Arrow union take requires non-null indices",
        ));
    }
    if rows == 0 {
        return Ok(());
    }
    // The outer take has UInt32 or UInt64 indices; recursive kernels may use other
    // widths, so eight bytes conservatively bounds their index buffers.
    buffer_extent(rows, 8)?;
    buffer_extent(add(rows, 1)?, 8)?;
    if let Some(width) = array.data_type().primitive_width() {
        return buffer_extent(rows, width);
    }
    match array.data_type() {
        DataType::Null | DataType::Boolean => Ok(()),
        DataType::FixedSizeBinary(width) => buffer_extent(
            rows,
            usize::try_from(*width).map_err(|_| CopyError::Extent)?,
        ),
        DataType::Utf8 => bytes::<arrow::datatypes::Utf8Type>(array, selection, mode, false, work),
        DataType::LargeUtf8 => {
            bytes::<arrow::datatypes::LargeUtf8Type>(array, selection, mode, true, work)
        }
        DataType::Binary => {
            bytes::<arrow::datatypes::BinaryType>(array, selection, mode, false, work)
        }
        DataType::LargeBinary => {
            bytes::<arrow::datatypes::LargeBinaryType>(array, selection, mode, true, work)
        }
        DataType::Utf8View | DataType::BinaryView => buffer_extent(rows, 16),
        // take_dict and single-source MutableArrayData retain the dictionary
        // values unchanged; only keys expand, never its encoded value domain.
        DataType::Dictionary(key, _) => {
            buffer_extent(rows, key.primitive_width().ok_or(CopyError::Extent)?)
        }
        DataType::Struct(_) => {
            let array: &StructArray = downcast(array)?;
            for child in array.columns() {
                preflight(child.as_ref(), selection, mode, work)?;
            }
            Ok(())
        }
        DataType::FixedSizeList(_, width) => {
            let array: &FixedSizeListArray = downcast(array)?;
            let width = usize::try_from(*width).map_err(|_| CopyError::Extent)?;
            let mut child = selection.map_ranges(work, |range, output, _work| {
                let range = mul(range.start, width)?..mul(range.end, width)?;
                if mode.is_take() {
                    limit(range.end, u32::MAX as usize)?;
                }
                output.push(range);
                Ok(())
            })?;
            child.nulls = mul(selection.nulls, width)?;
            // Fixed-list take copies children even under a NULL parent.
            let child_mode = if mode.is_take() {
                CopyMode::Take {
                    index_maximum: u32::MAX as usize,
                }
            } else {
                mode
            };
            preflight(array.values().as_ref(), &child, child_mode, work)
        }
        DataType::List(_) => list::<i32>(array, selection, mode, false, work),
        DataType::LargeList(_) => list::<i64>(array, selection, mode, true, work),
        DataType::Map(_, _) => {
            let array: &MapArray = downcast(array)?;
            let child =
                offset_selection(array, array.value_offsets(), selection, mode, false, work)?;
            if mode.is_take() {
                let average = array.entries().len().checked_div(array.len()).unwrap_or(0);
                work.boundary()?;
                let data = array.entries().to_data();
                work.boundary()?;
                mutable_capacity(&data, mul(average, rows)?, work)?;
            }
            preflight(array.entries(), &child, CopyMode::Extend, work)
        }
        DataType::ListView(_) => list_view::<i32>(array, selection, mode, false, work),
        DataType::LargeListView(_) => list_view::<i64>(array, selection, mode, true, work),
        DataType::Union(fields, union_mode) => {
            let array: &UnionArray = downcast(array)?;
            if *union_mode == UnionMode::Sparse {
                for (id, _) in fields.iter() {
                    preflight(array.child(id).as_ref(), selection, mode, work)?;
                }
            } else {
                for (id, _) in fields.iter() {
                    let child = selection.map_ranges(work, |range, output, work| {
                        for row in range.clone() {
                            work.step()?;
                            if array.type_id(row) == id {
                                let offset = array.value_offset(row);
                                output.push(offset..add(offset, 1)?);
                            }
                        }
                        Ok(())
                    })?;
                    // Both take and MutableArrayData write signed i32 offsets.
                    limit(child.len(work)?, i32::MAX as usize)?;
                    let child_mode = if mode.is_take() {
                        CopyMode::Take {
                            index_maximum: i32::MAX as usize,
                        }
                    } else {
                        mode
                    };
                    preflight(array.child(id).as_ref(), &child, child_mode, work)?;
                }
            }
            Ok(())
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run::<Int16Type>(array, selection, mode, i16::MAX as usize, work),
            DataType::Int32 => run::<Int32Type>(array, selection, mode, i32::MAX as usize, work),
            DataType::Int64 => run::<Int64Type>(array, selection, mode, offset_max(true), work),
            _ => Err(CopyError::Invalid(
                "constant run-end index carrier is invalid",
            )),
        },
        other => Err(CopyError::Unsupported(other.clone())),
    }
}

fn bytes<T: ByteArrayType>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let array: &GenericByteArray<T> = downcast(array)?;
    let mut total = 0;
    for block in &selection.blocks {
        work.step()?;
        let mut one = 0;
        for range in &block.ranges {
            work.step()?;
            for row in range.clone() {
                work.step()?;
                if mode == CopyMode::Extend || array.is_valid(row) {
                    let start = offset_value(array.value_offsets()[row])?;
                    let end = offset_value(array.value_offsets()[row + 1])?;
                    one = add(one, end.checked_sub(start).ok_or(CopyError::Extent)?)?;
                }
            }
        }
        total = add(total, mul(one, block.repeats)?)?;
    }
    limit(total, offset_max(large))?;
    buffer_extent(total, 1)
}

fn offset_value<O: OffsetSizeTrait>(value: O) -> Result<usize, CopyError> {
    value.to_usize().ok_or(CopyError::Extent)
}
fn offset_selection<O: OffsetSizeTrait>(
    array: &dyn Array,
    offsets: &[O],
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<Selection, CopyError> {
    let child = selection.map_ranges(work, |range, output, work| {
        if mode == CopyMode::Extend {
            output.push(offset_value(offsets[range.start])?..offset_value(offsets[range.end])?);
        } else {
            for row in range.clone() {
                work.step()?;
                // take_list omits NULL parent ranges; MutableArrayData::extend
                // copies offsets/payload even when a copied parent is NULL.
                if array.is_valid(row) {
                    output.push(offset_value(offsets[row])?..offset_value(offsets[row + 1])?);
                }
            }
        }
        Ok(())
    })?;
    limit(child.len(work)?, offset_max(large))?;
    Ok(child)
}
fn list<O: OffsetSizeTrait>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let array: &GenericListArray<O> = downcast(array)?;
    let child = offset_selection(array, array.value_offsets(), selection, mode, large, work)?;
    if mode.is_take() {
        // The real take_list reserves a source-average capacity before extending
        // selected ranges. Its integer multiplication must also be checked.
        let average = array.values().len().checked_div(array.len()).unwrap_or(0);
        work.boundary()?;
        let data = array.values().to_data();
        work.boundary()?;
        let capacity = mul(average, selection.len(work)?)?;
        mutable_capacity(&data, capacity, work)?;
    }
    preflight(array.values().as_ref(), &child, CopyMode::Extend, work)
}
fn list_view<O: OffsetSizeTrait>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let array: &GenericListViewArray<O> = downcast(array)?;
    if mode.is_take() {
        // take_list_view retains the original child backing and copies views.
        return buffer_extent(selection.len(work)?, if large { 16 } else { 8 });
    }
    let child = selection.map_ranges(work, |range, output, work| {
        for row in range.clone() {
            work.step()?;
            let start = offset_value(array.value_offsets()[row])?;
            let size = offset_value(array.value_sizes()[row])?;
            output.push(start..add(start, size)?);
        }
        Ok(())
    })?;
    limit(child.len(work)?, offset_max(large))?;
    preflight(array.values().as_ref(), &child, CopyMode::Extend, work)
}

fn remove_first(ranges: &mut Vec<Range<usize>>) {
    if let Some(first) = ranges.first_mut() {
        first.start += 1;
        if first.start == first.end {
            ranges.remove(0);
        }
    }
}
fn run<R: RunEndIndexType>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    maximum: usize,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let array: &RunArray<R> = downcast(array)?;
    limit(selection.len(work)?, maximum)?;
    let mut output = Vec::new();
    let mut previous = None;
    for block in &selection.blocks {
        work.step()?;
        if block.repeats == 0 {
            continue;
        }
        let mut ranges: Vec<Range<usize>> = Vec::new();
        for range in &block.ranges {
            work.step()?;
            if range.is_empty() {
                continue;
            }
            work.boundary()?;
            let start = array.get_physical_index(range.start);
            let end = add(array.get_physical_index(range.end - 1), 1)?;
            work.boundary()?;
            // take_run casts physical value indices back to its input index
            // carrier (UInt32 normally; Int32 under a dense Union).
            if let CopyMode::Take { index_maximum } = mode {
                limit(end - 1, index_maximum)?;
            }
            let start = if mode.is_take() && ranges.last().is_some_and(|last| last.end - 1 == start)
            {
                add(start, 1)?
            } else {
                start
            };
            if start < end {
                ranges.push(start..end);
            }
        }
        if ranges.is_empty() {
            continue;
        }
        if mode == CopyMode::Extend {
            output.push(Block {
                ranges,
                repeats: block.repeats,
            });
            continue;
        }
        let first = ranges[0].start;
        let last = ranges.last().expect("nonempty physical range").end - 1;
        let mut initial = Vec::with_capacity(ranges.len());
        for range in &ranges {
            initial.push(range.clone());
            work.step()?;
        }
        if previous == Some(first) {
            remove_first(&mut initial);
        }
        output.push(Block {
            ranges: initial,
            repeats: 1,
        });
        if block.repeats > 1 {
            if first == last {
                remove_first(&mut ranges);
            }
            output.push(Block {
                ranges,
                repeats: block.repeats - 1,
            });
        }
        previous = Some(last);
    }
    preflight(
        array.values().as_ref(),
        &Selection {
            blocks: output,
            nulls: 0,
        },
        mode,
        work,
    )
}

/// Validate the actual UInt64 nullable take plan before the opaque Arrow copy.
/// NULL indices count toward output reservation, never toward source payload.
pub(super) fn preflight_take(
    array: &dyn Array,
    indices: &[Option<u64>],
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(&mut observe);
    let mut ranges = Vec::with_capacity(indices.len());
    let mut nulls = 0;
    for index in indices {
        work.step()?;
        if let Some(index) = index {
            let index = usize::try_from(*index).map_err(|_| CopyError::Extent)?;
            ranges.push(index..add(index, 1)?);
        } else {
            nulls = add(nulls, 1)?;
        }
    }
    preflight(
        array,
        &Selection {
            blocks: vec![Block { ranges, repeats: 1 }],
            nulls,
        },
        CopyMode::Take {
            index_maximum: usize::MAX,
        },
        &mut work,
    )
}
