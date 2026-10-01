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

//! Format/extent preflight for the legacy Arrow constant broadcast. This is
//! neither allocation authorization nor a cooperative kernel control ABI.

use arrow::array::{
    Array, ArrayData, ArrayRef, FixedSizeListArray, GenericByteArray, GenericListArray,
    GenericListViewArray, MapArray, OffsetSizeTrait, RunArray, StructArray, UInt32Array,
    UnionArray,
};
use arrow::datatypes::{
    ByteArrayType, DataType, Int16Type, Int32Type, Int64Type, RunEndIndexType, UnionMode,
};
use novarocks_functions::ConstantValue;
use std::ops::Range;

const EXTENT_ERROR: &str = "constant broadcast output extent exceeds its Arrow format";

fn add(a: usize, b: usize) -> Result<usize, String> {
    a.checked_add(b).ok_or_else(|| EXTENT_ERROR.into())
}
fn mul(a: usize, b: usize) -> Result<usize, String> {
    a.checked_mul(b).ok_or_else(|| EXTENT_ERROR.into())
}
fn limit(value: usize, maximum: usize) -> Result<(), String> {
    if value > maximum {
        Err(EXTENT_ERROR.into())
    } else {
        Ok(())
    }
}
fn buffer_extent(elements: usize, width: usize) -> Result<(), String> {
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
struct Selection(Vec<Block>);
impl Selection {
    fn len(&self) -> Result<usize, String> {
        self.0.iter().try_fold(0, |total, block| {
            let one = block
                .ranges
                .iter()
                .try_fold(0, |len, range| add(len, range.len()))?;
            add(total, mul(one, block.repeats)?)
        })
    }
    fn check(&self, source_len: usize) -> Result<(), String> {
        for block in &self.0 {
            for range in &block.ranges {
                if range.start > range.end || range.end > source_len {
                    return Err("constant broadcast selected range is outside its source".into());
                }
            }
        }
        Ok(())
    }
    fn map_ranges(
        &self,
        mut map: impl FnMut(&Range<usize>, &mut Vec<Range<usize>>) -> Result<(), String>,
    ) -> Result<Self, String> {
        self.0
            .iter()
            .map(|block| {
                let mut ranges = Vec::new();
                for range in &block.ranges {
                    map(range, &mut ranges)?;
                }
                Ok(Block {
                    ranges,
                    repeats: block.repeats,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map(Self)
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
    let source = value.pool().array();
    if rows == 0 {
        return Ok(source.slice(value.ordinal() as usize, 0));
    }
    let start = value.ordinal() as usize;
    let selection = Selection(vec![Block {
        ranges: std::iter::once(start..add(start, 1)?).collect(),
        repeats: rows,
    }]);
    preflight(
        source.as_ref(),
        &selection,
        CopyMode::Take {
            index_maximum: u32::MAX as usize,
        },
    )?;
    let indices = UInt32Array::from(vec![value.ordinal(); rows]);
    let output = arrow::compute::take(source.as_ref(), &indices, None)
        .map_err(|error| format!("constant pool broadcast failed: {error}"))?;
    if !novarocks_type_contract::arrow_data_types_exact(output.data_type(), source.data_type()) {
        return Err("constant broadcast changed the exact Arrow carrier".into());
    }
    Ok(output)
}

// MutableArrayData constructors reserve by the library's capacity hint before
// extending selected children. The hint follows a separate path from logical
// output, particularly for FixedSizeList; both arithmetic paths need checking.
fn mutable_capacity(data: &ArrayData, capacity: usize) -> Result<(), String> {
    // This models actual constructor children even when no source row will be
    // extended. Dictionary constructors validate the whole retained dictionary
    // length against the key carrier, including their off-by-one library limit.
    let ty = data.data_type();
    let child = |index: usize| {
        data.child_data()
            .get(index)
            .ok_or_else(|| "constant mutable-copy source has missing child data".to_owned())
    };
    buffer_extent(add(capacity, 1)?, 8)?;
    if let Some(width) = ty.primitive_width() {
        return buffer_extent(capacity, width);
    }
    match ty {
        DataType::FixedSizeBinary(width) => {
            buffer_extent(capacity, usize::try_from(*width).map_err(|_| EXTENT_ERROR)?)
        }
        DataType::FixedSizeList(_, width) => mutable_capacity(
            child(0)?,
            mul(capacity, usize::try_from(*width).map_err(|_| EXTENT_ERROR)?)?,
        ),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _) => mutable_capacity(child(0)?, capacity),
        DataType::Struct(_) | DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => {
            for child in data.child_data() {
                mutable_capacity(child, capacity)?;
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
                _ => return Err("constant dictionary key carrier is invalid".into()),
            };
            limit(child(0)?.len(), maximum)?;
            // Single-source MutableArrayData retains values without constructing
            // a mutable values array. Only the dictionary key buffer expands.
            buffer_extent(capacity, key.primitive_width().ok_or(EXTENT_ERROR)?)
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
        other => Err(format!(
            "constant broadcast cannot reserve Arrow carrier {other:?}"
        )),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, String> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| "constant source array differs from its checked carrier".into())
}

fn preflight(array: &dyn Array, selection: &Selection, mode: CopyMode) -> Result<(), String> {
    selection.check(array.len())?;
    let rows = selection.len()?;
    if rows == 0 {
        return Ok(());
    }
    // The outer take has UInt32 indices; recursive kernels may use other
    // widths, so eight bytes conservatively bounds their index buffers.
    buffer_extent(rows, 8)?;
    buffer_extent(add(rows, 1)?, 8)?;
    if let Some(width) = array.data_type().primitive_width() {
        return buffer_extent(rows, width);
    }
    match array.data_type() {
        DataType::Null | DataType::Boolean => Ok(()),
        DataType::FixedSizeBinary(width) => {
            buffer_extent(rows, usize::try_from(*width).map_err(|_| EXTENT_ERROR)?)
        }
        DataType::Utf8 => bytes::<arrow::datatypes::Utf8Type>(array, selection, mode, false),
        DataType::LargeUtf8 => {
            bytes::<arrow::datatypes::LargeUtf8Type>(array, selection, mode, true)
        }
        DataType::Binary => bytes::<arrow::datatypes::BinaryType>(array, selection, mode, false),
        DataType::LargeBinary => {
            bytes::<arrow::datatypes::LargeBinaryType>(array, selection, mode, true)
        }
        DataType::Utf8View | DataType::BinaryView => buffer_extent(rows, 16),
        // take_dict and single-source MutableArrayData retain the dictionary
        // values unchanged; only keys expand, never its encoded value domain.
        DataType::Dictionary(key, _) => {
            buffer_extent(rows, key.primitive_width().ok_or(EXTENT_ERROR)?)
        }
        DataType::Struct(_) => {
            let array: &StructArray = downcast(array)?;
            for child in array.columns() {
                preflight(child.as_ref(), selection, mode)?;
            }
            Ok(())
        }
        DataType::FixedSizeList(_, width) => {
            let array: &FixedSizeListArray = downcast(array)?;
            let width = usize::try_from(*width).map_err(|_| EXTENT_ERROR)?;
            let child = selection.map_ranges(|range, output| {
                let range = mul(range.start, width)?..mul(range.end, width)?;
                if mode.is_take() {
                    limit(range.end, u32::MAX as usize)?;
                }
                output.push(range);
                Ok(())
            })?;
            // Fixed-list take copies children even under a NULL parent.
            let child_mode = if mode.is_take() {
                CopyMode::Take {
                    index_maximum: u32::MAX as usize,
                }
            } else {
                mode
            };
            preflight(array.values().as_ref(), &child, child_mode)
        }
        DataType::List(_) => list::<i32>(array, selection, mode, false),
        DataType::LargeList(_) => list::<i64>(array, selection, mode, true),
        DataType::Map(_, _) => {
            let array: &MapArray = downcast(array)?;
            let child = offset_selection(array, array.value_offsets(), selection, mode, false)?;
            if mode.is_take() {
                let average = array.entries().len().checked_div(array.len()).unwrap_or(0);
                mutable_capacity(&array.entries().to_data(), mul(average, rows)?)?;
            }
            preflight(array.entries(), &child, CopyMode::Extend)
        }
        DataType::ListView(_) => list_view::<i32>(array, selection, mode, false),
        DataType::LargeListView(_) => list_view::<i64>(array, selection, mode, true),
        DataType::Union(fields, union_mode) => {
            let array: &UnionArray = downcast(array)?;
            if *union_mode == UnionMode::Sparse {
                for (id, _) in fields.iter() {
                    preflight(array.child(id).as_ref(), selection, mode)?;
                }
            } else {
                for (id, _) in fields.iter() {
                    let child = selection.map_ranges(|range, output| {
                        for row in range.clone() {
                            if array.type_id(row) == id {
                                let offset = array.value_offset(row);
                                output.push(offset..add(offset, 1)?);
                            }
                        }
                        Ok(())
                    })?;
                    // Both take and MutableArrayData write signed i32 offsets.
                    limit(child.len()?, i32::MAX as usize)?;
                    let child_mode = if mode.is_take() {
                        CopyMode::Take {
                            index_maximum: i32::MAX as usize,
                        }
                    } else {
                        mode
                    };
                    preflight(array.child(id).as_ref(), &child, child_mode)?;
                }
            }
            Ok(())
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run::<Int16Type>(array, selection, mode, i16::MAX as usize),
            DataType::Int32 => run::<Int32Type>(array, selection, mode, i32::MAX as usize),
            DataType::Int64 => run::<Int64Type>(array, selection, mode, offset_max(true)),
            _ => Err("constant run-end index carrier is invalid".into()),
        },
        other => Err(format!(
            "constant broadcast does not support Arrow carrier {other:?}"
        )),
    }
}

fn bytes<T: ByteArrayType>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    large: bool,
) -> Result<(), String> {
    let array: &GenericByteArray<T> = downcast(array)?;
    let mut total = 0;
    for block in &selection.0 {
        let mut one = 0;
        for range in &block.ranges {
            for row in range.clone() {
                if mode == CopyMode::Extend || array.is_valid(row) {
                    let start = offset_value(array.value_offsets()[row])?;
                    let end = offset_value(array.value_offsets()[row + 1])?;
                    one = add(one, end.checked_sub(start).ok_or(EXTENT_ERROR)?)?;
                }
            }
        }
        total = add(total, mul(one, block.repeats)?)?;
    }
    limit(total, offset_max(large))?;
    buffer_extent(total, 1)
}

fn offset_value<O: OffsetSizeTrait>(value: O) -> Result<usize, String> {
    value.to_usize().ok_or_else(|| EXTENT_ERROR.into())
}
fn offset_selection<O: OffsetSizeTrait>(
    array: &dyn Array,
    offsets: &[O],
    selection: &Selection,
    mode: CopyMode,
    large: bool,
) -> Result<Selection, String> {
    let child = selection.map_ranges(|range, output| {
        if mode == CopyMode::Extend {
            output.push(offset_value(offsets[range.start])?..offset_value(offsets[range.end])?);
        } else {
            for row in range.clone() {
                // take_list omits NULL parent ranges; MutableArrayData::extend
                // copies offsets/payload even when a copied parent is NULL.
                if array.is_valid(row) {
                    output.push(offset_value(offsets[row])?..offset_value(offsets[row + 1])?);
                }
            }
        }
        Ok(())
    })?;
    limit(child.len()?, offset_max(large))?;
    Ok(child)
}
fn list<O: OffsetSizeTrait>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    large: bool,
) -> Result<(), String> {
    let array: &GenericListArray<O> = downcast(array)?;
    let child = offset_selection(array, array.value_offsets(), selection, mode, large)?;
    if mode.is_take() {
        // The real take_list reserves a source-average capacity before extending
        // selected ranges. Its integer multiplication must also be checked.
        let average = array.values().len().checked_div(array.len()).unwrap_or(0);
        mutable_capacity(&array.values().to_data(), mul(average, selection.len()?)?)?;
    }
    preflight(array.values().as_ref(), &child, CopyMode::Extend)
}
fn list_view<O: OffsetSizeTrait>(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    large: bool,
) -> Result<(), String> {
    let array: &GenericListViewArray<O> = downcast(array)?;
    if mode.is_take() {
        // take_list_view retains the original child backing and copies views.
        return buffer_extent(selection.len()?, if large { 16 } else { 8 });
    }
    let child = selection.map_ranges(|range, output| {
        for row in range.clone() {
            let start = offset_value(array.value_offsets()[row])?;
            let size = offset_value(array.value_sizes()[row])?;
            output.push(start..add(start, size)?);
        }
        Ok(())
    })?;
    limit(child.len()?, offset_max(large))?;
    preflight(array.values().as_ref(), &child, CopyMode::Extend)
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
) -> Result<(), String> {
    let array: &RunArray<R> = downcast(array)?;
    limit(selection.len()?, maximum)?;
    let mut output = Vec::new();
    let mut previous = None;
    for block in &selection.0 {
        if block.repeats == 0 {
            continue;
        }
        let mut ranges: Vec<Range<usize>> = Vec::new();
        for range in &block.ranges {
            if range.is_empty() {
                continue;
            }
            let start = array.get_physical_index(range.start);
            let end = add(array.get_physical_index(range.end - 1), 1)?;
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
        let mut initial = ranges.clone();
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
    preflight(array.values().as_ref(), &Selection(output), mode)
}
