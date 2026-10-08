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

#[path = "selected_copy/zip.rs"]
mod zip;
pub use zip::preflight_zip;

use crate::KernelFailure;
use arrow_array::types::{ByteArrayType, Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericByteArray, GenericListArray, GenericListViewArray,
    MapArray, OffsetSizeTrait, RunArray, StructArray, UnionArray,
};
use arrow_data::ArrayData;
use arrow_schema::{DataType, UnionMode};
use std::ops::Range;

#[derive(Debug)]
pub enum CopyError {
    Extent,
    Invalid(&'static str),
    Unsupported(DataType),
    Control(KernelFailure),
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
        }
    }
}
impl std::error::Error for CopyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            _ => None,
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

/// Preflight a UInt32-index broadcast of one actual source row. The callback
/// receives false for owned work and true for an observation boundary. It must
/// use the caller's original control; no allocation or memory grant is issued.
/// Ordinary errors and success flush the tail. A callback refusal is primary
/// and is returned without another callback.
pub fn preflight_broadcast(
    source: &dyn Array,
    ordinal: u32,
    rows: usize,
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(&mut observe);
    work.boundary()?;
    let result = (|| {
        let start = ordinal as usize;
        // Even an empty output must refer to an actual source value.
        if start >= source.len() {
            return Err(CopyError::Invalid(
                "constant broadcast selected range is outside its source",
            ));
        }
        work.step()?;
        let selection = Selection {
            blocks: vec![Block {
                ranges: std::iter::once(start..add(start, 1)?).collect(),
                repeats: rows,
            }],
            nulls: 0,
        };
        preflight(
            source,
            &selection,
            CopyMode::Take {
                index_maximum: u32::MAX as usize,
            },
            &mut work,
        )
    })();
    if matches!(&result, Err(CopyError::Control(_))) {
        return result;
    }
    work.boundary()?;
    result
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
        data.child_data().get(index).ok_or(CopyError::Invalid(
            "constant mutable-copy source has missing child data",
        ))
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
    array.as_any().downcast_ref().ok_or(CopyError::Invalid(
        "constant source array differs from its checked carrier",
    ))
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
        DataType::Utf8 => {
            bytes::<arrow_array::types::Utf8Type>(array, selection, mode, false, work)
        }
        DataType::LargeUtf8 => {
            bytes::<arrow_array::types::LargeUtf8Type>(array, selection, mode, true, work)
        }
        DataType::Binary => {
            bytes::<arrow_array::types::BinaryType>(array, selection, mode, false, work)
        }
        DataType::LargeBinary => {
            bytes::<arrow_array::types::LargeBinaryType>(array, selection, mode, true, work)
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
                for (child_index, (id, _)) in fields.iter().enumerate() {
                    let mut child = selection.map_ranges(work, |range, output, work| {
                        for row in range.clone() {
                            work.step()?;
                            if array.type_id(row) == id {
                                let offset = array.value_offset(row);
                                output.push(offset..add(offset, 1)?);
                            }
                        }
                        Ok(())
                    })?;
                    // MutableArrayData appends NULLs to the first declared child
                    // and casts its end offset to i32. Include that padding in
                    // both the offset bound and the child's recursive extent.
                    if mode == CopyMode::Extend && child_index == 0 {
                        child.nulls = selection.nulls;
                    }
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
/// The caller flushes the same original work at entry and on success or ordinary
/// failure; observer refusals must return immediately without another callback.
pub fn preflight_take(
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

/// Check one contiguous MutableArrayData copy and its trailing NULL padding.
/// Constructor capacity and selected payload use the existing separate extent
/// authors. The caller owns source ArrayData, scratch and copy memory scopes;
/// this operation only describes representability and observes actual work.
pub fn preflight_extend(
    source: &dyn Array,
    start: usize,
    len: usize,
    nulls: usize,
    capacity: usize,
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(&mut observe);
    work.boundary()?;
    let result = (|| {
        let end = add(start, len)?;
        let output = add(len, nulls)?;
        work.step()?;
        if start > source.len() || end > source.len() || output > capacity {
            return Err(CopyError::Invalid(
                "mutable copy range or padding exceeds its source or capacity",
            ));
        }
        work.boundary()?;
        let data = source.to_data();
        work.boundary()?;
        mutable_capacity(&data, capacity, &mut work)?;
        let selection = Selection {
            blocks: vec![Block {
                ranges: std::iter::once(start..end).collect(),
                repeats: 1,
            }],
            nulls,
        };
        preflight(source, &selection, CopyMode::Extend, &mut work)
    })();
    if matches!(&result, Err(CopyError::Control(_))) {
        return result;
    }
    work.boundary()?;
    result
}

// Arrow's MutableBuffer rounds bitmap reservations to 64-byte alignment.
// This is a format/layout check, not an allocation grant or byte invoice.
pub fn fixed_interleave_extent(ty: &DataType, rows: usize) -> Result<(), CopyError> {
    if let Some(width) = ty.primitive_width() {
        buffer_extent(rows, width)?;
    } else if let DataType::FixedSizeBinary(width) = ty {
        buffer_extent(
            rows,
            usize::try_from(*width).map_err(|_| CopyError::Extent)?,
        )?;
    } else if *ty != DataType::Boolean {
        return Err(CopyError::Unsupported(ty.clone()));
    }
    interleave_bitmap_extent(rows)
}

fn interleave_bitmap_extent(rows: usize) -> Result<(), CopyError> {
    let bitmap_bytes = add(rows / 8, usize::from(!rows.is_multiple_of(8)))?;
    let bitmap_blocks = add(
        bitmap_bytes / 64,
        usize::from(!bitmap_bytes.is_multiple_of(64)),
    )?;
    buffer_extent(bitmap_blocks, 64)
}

/// Static offset/validity extent, before entering any guarded child. Actual
/// byte payload is checked after the exact source choices are known.
pub fn guarded_interleave_extent(ty: &DataType, rows: usize) -> Result<(), CopyError> {
    let width = match ty {
        DataType::Utf8 | DataType::Binary => 4,
        DataType::LargeUtf8 | DataType::LargeBinary => 8,
        _ => return fixed_interleave_extent(ty, rows),
    };
    buffer_extent(add(rows, 1)?, width)?;
    interleave_bitmap_extent(rows)
}

pub(crate) fn byte_interleave_payload_extent(total: usize, large: bool) -> Result<(), CopyError> {
    limit(total, offset_max(large))?;
    buffer_extent(total, 1)
}

fn interleave_byte_payload<T: ByteArrayType>(
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let mut total = 0;
    for &(source, row) in choices {
        let array: &GenericByteArray<T> = downcast(sources[source].as_ref())?;
        let start = offset_value(array.value_offsets()[row])?;
        let end = offset_value(array.value_offsets()[row + 1])?;
        // Arrow interleave_bytes copies the offset span even for NULL values.
        // Single-source take's validity-sensitive payload count is insufficient.
        total = add(total, end.checked_sub(start).ok_or(CopyError::Extent)?)?;
        work.step()?;
    }
    byte_interleave_payload_extent(total, large)
}

fn validate_interleave_plan(
    ty: &DataType,
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    if sources.is_empty() {
        return Err(CopyError::Invalid(
            "Arrow interleave requires a source array",
        ));
    }
    for source in sources {
        let equal = novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            ty,
            source.data_type(),
            || (work.0)(false),
        )
        .map_err(CopyError::Control)?;
        work.step()?;
        if !equal {
            return Err(CopyError::Invalid(
                "interleave source differs from its exact frozen carrier",
            ));
        }
    }
    for &(source, row) in choices {
        let valid = sources.get(source).is_some_and(|array| row < array.len());
        work.step()?;
        if !valid {
            return Err(CopyError::Invalid(
                "interleave choice has an invalid source or row",
            ));
        }
    }
    Ok(())
}

/// Exact multi-source byte plan. Views and encoded/nested carriers retain
/// their explicit separate admission boundary; this does not mint a grant.
pub fn preflight_guarded_interleave(
    ty: &DataType,
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    if !matches!(
        ty,
        DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary
    ) {
        return preflight_fixed_interleave(ty, sources, choices, observe);
    }
    let mut work = CopyObservation(&mut observe);
    work.boundary()?;
    let result = (|| {
        guarded_interleave_extent(ty, choices.len())?;
        work.step()?;
        validate_interleave_plan(ty, sources, choices, &mut work)?;
        match ty {
            DataType::Utf8 => interleave_byte_payload::<arrow_array::types::Utf8Type>(
                sources, choices, false, &mut work,
            ),
            DataType::Binary => interleave_byte_payload::<arrow_array::types::BinaryType>(
                sources, choices, false, &mut work,
            ),
            DataType::LargeUtf8 => interleave_byte_payload::<arrow_array::types::LargeUtf8Type>(
                sources, choices, true, &mut work,
            ),
            DataType::LargeBinary => {
                interleave_byte_payload::<arrow_array::types::LargeBinaryType>(
                    sources, choices, true, &mut work,
                )
            }
            _ => unreachable!("checked offset byte carrier"),
        }
    })();
    if matches!(&result, Err(CopyError::Control(_))) {
        return result;
    }
    work.boundary()?;
    result
}

/// Check the actual fixed-width source-choice plan before opaque Arrow interleave.
/// Choices address compact source array rows, independently of the outer batch.
/// The caller has already admitted the frozen type and owns both allocations and
/// the control checkpoints immediately before and after the library copy.
pub fn preflight_fixed_interleave(
    ty: &DataType,
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(&mut observe);
    work.boundary()?;
    let result = (|| {
        fixed_interleave_extent(ty, choices.len())?;
        work.step()?;
        validate_interleave_plan(ty, sources, choices, &mut work)
    })();
    // An observer refusal is primary and must not trigger another callback.
    if matches!(&result, Err(CopyError::Control(_))) {
        return result;
    }
    work.boundary()?;
    result
}

#[cfg(test)]
#[path = "selected_copy/preflight_interleave_tests.rs"]
mod preflight_interleave_tests;

#[cfg(test)]
#[path = "selected_copy/preflight_byte_interleave_tests.rs"]
mod preflight_byte_interleave_tests;

#[cfg(test)]
mod broadcast_tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::Field;
    use std::sync::Arc;

    pub(super) fn failures() -> [KernelFailure; 7] {
        use crate::KernelDiagnostic;
        [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational")),
            KernelFailure::InstanceFailed,
        ]
    }

    #[test]
    fn broadcast_preflight_uses_actual_nonzero_source_ordinal_and_format_extent() {
        let input = StringArray::from(vec![Some("unused"), None, Some("selected")]);
        for rows in [0, 1, 320] {
            preflight_broadcast(&input, 2, rows, |_| Ok(())).unwrap();
            preflight_broadcast(&input, 1, rows, |_| Ok(())).unwrap();
        }
        assert!(matches!(
            preflight_broadcast(&input, 3, 0, |_| Ok(())),
            Err(CopyError::Invalid(_))
        ));
        let input = Int64Array::from(vec![Some(8)]);
        assert!(matches!(
            preflight_broadcast(&input, 0, usize::MAX, |_| Ok(())),
            Err(CopyError::Extent)
        ));
    }

    #[test]
    fn broadcast_preflight_every_original_control_refusal_keeps_actual_child_graph_prefix() {
        let fields = (0..320)
            .map(|i| Arc::new(Field::new(format!("child-{i}"), DataType::Int64, false)))
            .collect::<Vec<_>>();
        let arrays = (0..320)
            .map(|i| Arc::new(Int64Array::from(vec![i as i64, -(i as i64)])) as ArrayRef)
            .collect();
        let source = StructArray::try_new(fields.into(), arrays, None).unwrap();
        let mut trace = Vec::new();
        preflight_broadcast(&source, 1, 320, |boundary| {
            trace.push(boundary);
            Ok(())
        })
        .unwrap();
        assert_eq!(trace.first(), Some(&true));
        assert_eq!(trace.last(), Some(&true));
        assert!(trace.iter().filter(|boundary| !**boundary).count() > 256);
        for at in 0..trace.len() {
            for cause in failures() {
                let mut actual = Vec::new();
                let result = preflight_broadcast(&source, 1, 320, |boundary| {
                    actual.push(boundary);
                    if actual.len() - 1 == at {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                assert!(matches!(result, Err(CopyError::Control(ref error)) if error == &cause));
                assert_eq!(actual, trace[..=at]);
            }
        }
    }

    #[test]
    fn broadcast_preflight_ordinary_error_tail_can_refuse_without_losing_primary_control() {
        let source = Int64Array::from(vec![Some(4)]);
        let mut baseline = Vec::new();
        assert!(matches!(
            preflight_broadcast(&source, 2, 3, |boundary| {
                baseline.push(boundary);
                Ok(())
            }),
            Err(CopyError::Invalid(_))
        ));
        assert_eq!(baseline, vec![true, true]);
        for at in 0..baseline.len() {
            for cause in failures() {
                let mut actual = Vec::new();
                let result = preflight_broadcast(&source, 2, 3, |boundary| {
                    actual.push(boundary);
                    if actual.len() - 1 == at {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                assert!(matches!(result, Err(CopyError::Control(ref error)) if error == &cause));
                assert_eq!(actual, baseline[..=at]);
            }
        }
    }
}

#[cfg(test)]
#[path = "selected_copy/preflight_take_tests.rs"]
mod preflight_take_tests;

#[cfg(test)]
mod extend_tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{Field, UnionFields};
    use std::sync::Arc;

    #[test]
    fn dense_union_extend_bounds_first_child_null_padding_and_nested_fixed_list() {
        let fields = UnionFields::try_new(
            [7, 3],
            [
                Arc::new(Field::new("first", DataType::Int64, true)),
                Arc::new(Field::new("second", DataType::Int64, true)),
            ],
        )
        .unwrap();
        let source = UnionArray::try_new(
            fields,
            vec![7_i8, 3].into(),
            Some(vec![0_i32, 0].into()),
            vec![
                Arc::new(Int64Array::from(vec![Some(11)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(22)])) as ArrayRef,
            ],
        )
        .unwrap();
        let maximum = i32::MAX as usize;
        // No large output is allocated: these exercise the numeric preflight
        // against two actual source rows and the library's declared child order.
        preflight_extend(&source, 0, 1, maximum - 1, maximum, |_| Ok(())).unwrap();
        assert!(matches!(
            preflight_extend(&source, 0, 1, maximum, maximum + 1, |_| Ok(())),
            Err(CopyError::Extent)
        ));
        preflight_extend(&source, 1, 1, maximum, maximum + 1, |_| Ok(())).unwrap();
        let nested = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", source.data_type().clone(), true)),
            2,
            Arc::new(source),
            None,
        )
        .unwrap();
        let exact = (maximum - 1) / 2;
        preflight_extend(&nested, 0, 1, exact, exact + 1, |_| Ok(())).unwrap();
        assert!(matches!(
            preflight_extend(&nested, 0, 1, exact + 1, exact + 2, |_| Ok(())),
            Err(CopyError::Extent)
        ));
        for cause in super::broadcast_tests::failures() {
            let mut calls = 0;
            assert!(matches!(
                preflight_extend(&nested, 0, 1, exact + 1, exact + 2, |_| {
                    calls += 1;
                    Err(cause.clone())
                }),
                Err(CopyError::Control(actual)) if actual == cause
            ));
            assert_eq!(calls, 1);
        }
    }
}
