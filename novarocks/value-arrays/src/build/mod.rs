// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frozen-layout byte construction. A row is published only by commit; every
//! refused operation rolls back that entire row. Scheduling belongs to callers.
use crate::arrow_raw::{self, ByteSpan};
use crate::control::{Growth, Work};
use crate::error::field_representation;
use crate::{ByteLayout, ValueError, ValueFailure, ValueViolation};
use allocator_api2::alloc::{Allocator, Global};
use allocator_api2::vec::Vec as AllocVec;
use arrow_array::{Array, ArrayRef};
use arrow_buffer::Buffer;
use novarocks_failure_contract::RepresentationBoundary;
use std::alloc::Layout;
use std::ops::Range;

const BLOCK_TARGET: usize = 64 * 1024;
const COPY_CHECKPOINT: usize = 64 * 1024;

#[derive(Debug)]
pub enum WriteFailure<E> {
    /// The current row fits an empty output, but not this output's real fields.
    Full,
    Failure(ValueFailure<E>),
}
impl<E> From<ValueError> for WriteFailure<E> {
    fn from(error: ValueError) -> Self {
        Self::Failure(error.into())
    }
}
impl<E> From<ValueFailure<E>> for WriteFailure<E> {
    fn from(error: ValueFailure<E>) -> Self {
        Self::Failure(error)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BuildFacts {
    pub rows: usize,
    pub logical_bytes: u64,
    /// Actual bytes copied by writes, excluding rolled-back rows.
    pub copied_bytes: u64,
    pub adopted_regions: usize,
    pub declared_payload_capacity: usize,
}

pub struct BuiltArray {
    pub array: ArrayRef,
    pub facts: BuildFacts,
}

enum Region<A: Allocator> {
    Owned(AllocVec<u8, A>),
    Shared(Buffer),
}

/// Its actual mutable allocations use A. Publication transfers Global buffers
/// to standard Arrow storage; a foreign A is released after a standard copy.
pub struct ValueBuilder<A: Allocator + Clone + 'static = Global> {
    layout: ByteLayout,
    allocator: A,
    payload: AllocVec<u8, A>,
    offsets32: AllocVec<i32, A>,
    offsets64: AllocVec<i64, A>,
    views: AllocVec<u128, A>,
    validity: AllocVec<u8, A>,
    regions: AllocVec<Region<A>, A>,
    current: Option<usize>,
    rows: usize,
    null_count: usize,
    logical_bytes: u64,
    copied_bytes: u64,
}

impl ValueBuilder<Global> {
    pub fn new(layout: ByteLayout) -> Self {
        Self::new_in(layout, Global)
    }
}

impl<A: Allocator + Clone + 'static> ValueBuilder<A> {
    pub fn new_in(layout: ByteLayout, allocator: A) -> Self {
        Self {
            layout,
            payload: AllocVec::new_in(allocator.clone()),
            offsets32: AllocVec::new_in(allocator.clone()),
            offsets64: AllocVec::new_in(allocator.clone()),
            views: AllocVec::new_in(allocator.clone()),
            validity: AllocVec::new_in(allocator.clone()),
            regions: AllocVec::new_in(allocator.clone()),
            allocator,
            current: None,
            rows: 0,
            null_count: 0,
            logical_bytes: 0,
            copied_bytes: 0,
        }
    }

    pub fn layout(&self) -> ByteLayout {
        self.layout
    }
    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn begin_row(&mut self) -> RowWriter<'_, A> {
        let current_len = self.current.map(|i| match &self.regions[i] {
            Region::Owned(bytes) => bytes.len(),
            Region::Shared(_) => unreachable!("current block is owned"),
        });
        let mark = RowMark {
            payload: self.payload.len(),
            region_count: self.regions.len(),
            current: self.current,
            current_len,
            copied_bytes: self.copied_bytes,
        };
        RowWriter {
            builder: self,
            mark,
            data: Pending::Empty,
            done: false,
        }
    }

    pub fn append_str<E>(
        &mut self,
        value: &str,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let mut row = self.begin_row();
        row.write_str(value, growth, work)?;
        row.commit(growth, work)
    }

    pub fn append_bytes<E>(
        &mut self,
        value: &[u8],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let mut row = self.begin_row();
        row.write_bytes(value, growth, work)?;
        row.commit(growth, work)
    }

    pub fn append_null<E>(
        &mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.begin_row().commit_null(growth, work)
    }

    /// Adopt a value from a valid Arrow string/binary source without UTF8
    /// revalidation. A NULL source appends a logical NULL, not an empty value.
    pub fn append_array_value<E>(
        &mut self,
        source: &dyn Array,
        index: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let mut row = self.begin_row();
        row.write_array_value(source, index, growth, work)?;
        row.commit(growth, work)
    }

    pub fn finish(self) -> Result<BuiltArray, ValueError> {
        let facts = self.facts();
        if self.rows == 0 {
            return Ok(BuiltArray {
                array: arrow_array::new_empty_array(&self.layout.data_type()),
                facts,
            });
        }
        let nulls = arrow_raw::make_nulls(
            arrow_raw::into_arrow_buffer(self.validity),
            self.rows,
            self.null_count,
        );
        let array = if self.layout.is_view() {
            let buffers = self
                .regions
                .into_iter()
                .map(|r| match r {
                    Region::Owned(bytes) => arrow_raw::into_arrow_buffer(bytes),
                    Region::Shared(buffer) => buffer,
                })
                .collect();
            let views = arrow_raw::into_arrow_buffer(self.views).into();
            // SAFETY: commits create only checked descriptors into retained
            // regions; strings originate from str or admitted Arrow strings.
            // NULL slots use a valid empty inline descriptor, and bitmap length
            // equals the committed descriptor count.
            unsafe { arrow_raw::make_view_unchecked(self.layout, views, buffers, nulls)? }
        } else {
            let offsets = if matches!(self.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
                arrow_raw::into_arrow_buffer(self.offsets32)
            } else {
                arrow_raw::into_arrow_buffer(self.offsets64)
            };
            arrow_raw::make_offsets(
                self.layout,
                offsets,
                arrow_raw::into_arrow_buffer(self.payload),
                nulls,
            )?
        };
        debug_assert!(array.to_data().validate_full().is_ok());
        Ok(BuiltArray { array, facts })
    }

    pub fn facts(&self) -> BuildFacts {
        let mut facts = BuildFacts {
            rows: self.rows,
            logical_bytes: self.logical_bytes,
            copied_bytes: self.copied_bytes,
            adopted_regions: 0,
            declared_payload_capacity: self.payload.capacity(),
        };
        for region in &self.regions {
            let capacity = match region {
                Region::Owned(bytes) => bytes.capacity(),
                Region::Shared(bytes) => {
                    facts.adopted_regions += 1;
                    bytes.capacity()
                }
            };
            facts.declared_payload_capacity =
                facts.declared_payload_capacity.saturating_add(capacity);
        }
        facts
    }
}

struct RowMark {
    payload: usize,
    region_count: usize,
    current: Option<usize>,
    current_len: Option<usize>,
    copied_bytes: u64,
}

enum Pending {
    Empty,
    Null,
    Owned {
        region: Option<usize>,
        start: usize,
        len: usize,
    },
    Shared {
        buffer: Buffer,
        offset: usize,
        len: usize,
    },
}
impl Pending {
    fn len(&self) -> usize {
        match self {
            Self::Owned { len, .. } | Self::Shared { len, .. } => *len,
            _ => 0,
        }
    }
}

/// An uncommitted row. Dropping it is an abort. A failed operation aborts
/// immediately and closes this writer so subsequent calls cannot publish it.
pub struct RowWriter<'a, A: Allocator + Clone + 'static = Global> {
    builder: &'a mut ValueBuilder<A>,
    mark: RowMark,
    data: Pending,
    done: bool,
}

impl<A: Allocator + Clone + 'static> Drop for RowWriter<'_, A> {
    fn drop(&mut self) {
        self.rollback();
    }
}

impl<A: Allocator + Clone + 'static> RowWriter<'_, A> {
    fn rollback(&mut self) {
        if self.done {
            return;
        }
        self.builder.payload.truncate(self.mark.payload);
        self.builder.regions.truncate(self.mark.region_count);
        self.builder.current = self.mark.current;
        if let (Some(index), Some(len)) = (self.mark.current, self.mark.current_len) {
            match &mut self.builder.regions[index] {
                Region::Owned(bytes) => bytes.truncate(len),
                Region::Shared(_) => unreachable!("current block is owned"),
            }
        }
        self.builder.copied_bytes = self.mark.copied_bytes;
        self.data = Pending::Empty;
        self.done = true;
    }

    pub fn abort(mut self) {
        self.rollback();
    }

    fn checked<E>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<(), WriteFailure<E>>,
    ) -> Result<(), WriteFailure<E>> {
        if self.done {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
        }
        let result = operation(self);
        if result.is_err() {
            self.rollback();
        }
        result
    }

    pub fn write_str<E>(
        &mut self,
        value: &str,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if !this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            this.write_admitted(value.as_bytes(), growth, work)
        })
    }

    pub fn write_bytes<E>(
        &mut self,
        value: &[u8],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            this.write_admitted(value, growth, work)
        })
    }

    pub fn write_array_value<E>(
        &mut self,
        source: &dyn Array,
        index: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            let source_layout = ByteLayout::from_data_type(source.data_type())
                .ok_or(ValueError::Invariant(ValueViolation::Carrier))?;
            if source_layout.is_utf8() != this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            match arrow_raw::byte_span(source, index)? {
                None => {
                    if !matches!(this.data, Pending::Empty) {
                        return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
                    }
                    this.data = Pending::Null;
                    Ok(())
                }
                Some(ByteSpan::Inline(bytes)) => this.write_admitted(bytes, growth, work),
                Some(ByteSpan::Region {
                    buffer,
                    offset,
                    len,
                }) => this.adopt_admitted(buffer, offset..offset + len, growth, work),
            }
        })
    }

    pub fn write_binary_region<E>(
        &mut self,
        buffer: &Buffer,
        range: Range<usize>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            this.adopt_admitted(buffer, range, growth, work)
        })
    }

    /// The str's pointer and length must describe exactly the selected region.
    /// That proof avoids rescanning a previously validated string's bytes.
    pub fn write_string_region<E>(
        &mut self,
        buffer: &Buffer,
        range: Range<usize>,
        value: &str,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if !this.builder.layout.is_utf8() || range.start > range.end || range.end > buffer.len()
            {
                return Err(ValueError::Invariant(ValueViolation::Bounds).into());
            }
            let bytes = &buffer.as_slice()[range.clone()];
            if bytes.as_ptr() != value.as_ptr() || bytes.len() != value.len() {
                return Err(ValueError::Invariant(ValueViolation::Utf8Boundary).into());
            }
            this.adopt_admitted(buffer, range, growth, work)
        })
    }

    fn adopt_admitted<E>(
        &mut self,
        buffer: &Buffer,
        range: Range<usize>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        if range.start > range.end || range.end > buffer.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds).into());
        }
        if self.builder.layout.is_view() && matches!(self.data, Pending::Empty) && range.len() > 12
        {
            check_value_extent(
                self.builder.layout,
                0,
                range.len() as u64,
                self.builder.rows,
            )?;
            work(1).map_err(ValueFailure::Caller)?;
            self.data = Pending::Shared {
                buffer: buffer.clone(),
                offset: range.start,
                len: range.len(),
            };
            Ok(())
        } else {
            self.write_admitted(&buffer.as_slice()[range], growth, work)
        }
    }

    fn write_admitted<E>(
        &mut self,
        value: &[u8],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        if matches!(self.data, Pending::Null) {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
        }
        let old_len = self.data.len();
        let total = old_len
            .checked_add(value.len())
            .ok_or_else(|| allocation_extent(self.builder.rows, None))?;
        check_value_extent(
            self.builder.layout,
            self.mark.payload as u64,
            total as u64,
            self.builder.rows,
        )?;
        if matches!(self.data, Pending::Shared { .. }) {
            let previous = std::mem::replace(&mut self.data, Pending::Empty);
            if let Pending::Shared {
                buffer,
                offset,
                len,
            } = previous
            {
                self.write_admitted(&buffer.as_slice()[offset..offset + len], growth, work)?;
            }
        }
        if matches!(self.data, Pending::Empty) {
            if self.builder.layout.is_view() {
                let reusable = self
                    .builder
                    .current
                    .filter(|&i| match &self.builder.regions[i] {
                        Region::Owned(bytes) => {
                            bytes.len() < BLOCK_TARGET && bytes.len() <= i32::MAX as usize
                        }
                        Region::Shared(_) => false,
                    });
                let index = if let Some(index) = reusable {
                    index
                } else {
                    if self.builder.regions.len() > i32::MAX as usize {
                        return Err(WriteFailure::Full);
                    }
                    reserve(&mut self.builder.regions, 1, self.builder.rows, growth)?;
                    let index = self.builder.regions.len();
                    self.builder.regions.push(Region::Owned(AllocVec::new_in(
                        self.builder.allocator.clone(),
                    )));
                    self.builder.current = Some(index);
                    index
                };
                let start = match &self.builder.regions[index] {
                    Region::Owned(bytes) => bytes.len(),
                    _ => unreachable!(),
                };
                self.data = Pending::Owned {
                    region: Some(index),
                    start,
                    len: 0,
                };
            } else {
                self.data = Pending::Owned {
                    region: None,
                    start: self.mark.payload,
                    len: 0,
                };
            }
        }
        let Pending::Owned { region, len, .. } = &mut self.data else {
            unreachable!()
        };
        let bytes = match region {
            Some(index) => match &mut self.builder.regions[*index] {
                Region::Owned(bytes) => bytes,
                _ => unreachable!(),
            },
            None => &mut self.builder.payload,
        };
        reserve(bytes, value.len(), self.builder.rows, growth)?;
        for part in value.chunks(COPY_CHECKPOINT) {
            work(part.len()).map_err(ValueFailure::Caller)?;
            bytes.extend_from_slice(part);
        }
        *len += value.len();
        self.builder.copied_bytes = self
            .builder
            .copied_bytes
            .checked_add(value.len() as u64)
            .ok_or_else(|| allocation_extent(self.builder.rows, None))?;
        Ok(())
    }

    pub fn commit_null<E>(
        mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if !matches!(this.data, Pending::Empty | Pending::Null) {
                return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
            }
            this.data = Pending::Null;
            this.commit_inner(growth, work)
        })
    }

    pub fn commit<E>(
        mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| this.commit_inner(growth, work))
    }

    fn commit_inner<E>(
        &mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let rows = self.builder.rows;
        let next_rows = rows
            .checked_add(1)
            .ok_or_else(|| allocation_extent(rows, None))?;
        let valid = !matches!(self.data, Pending::Null);
        let len = self.data.len();
        let next_logical = self
            .builder
            .logical_bytes
            .checked_add(len as u64)
            .ok_or_else(|| allocation_extent(rows, None))?;
        work(1).map_err(ValueFailure::Caller)?;
        if rows % 8 == 0 {
            reserve(&mut self.builder.validity, 1, rows, growth)?;
        }
        if self.builder.layout.is_view() {
            reserve(&mut self.builder.views, 1, rows, growth)?;
        } else if matches!(self.builder.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            reserve(
                &mut self.builder.offsets32,
                if rows == 0 { 2 } else { 1 },
                rows,
                growth,
            )?;
        } else {
            reserve(
                &mut self.builder.offsets64,
                if rows == 0 { 2 } else { 1 },
                rows,
                growth,
            )?;
        }
        let descriptor = if self.builder.layout.is_view() {
            match &self.data {
                Pending::Empty | Pending::Null => arrow_raw::inline_view(&[])?,
                Pending::Owned {
                    region: Some(index),
                    start,
                    len,
                } => {
                    let Region::Owned(bytes) = &self.builder.regions[*index] else {
                        unreachable!()
                    };
                    arrow_raw::external_view(
                        self.builder.layout,
                        &bytes[*start..*start + *len],
                        *index,
                        *start,
                    )?
                }
                Pending::Shared {
                    buffer,
                    offset,
                    len,
                } => {
                    // Match only the same accessible region. A shared allocation
                    // with a different slice is registered separately.
                    let same = self.builder.regions.iter().position(|region| matches!(region, Region::Shared(other)
                        if other.data_ptr() == buffer.data_ptr() && other.ptr_offset() == buffer.ptr_offset() && other.len() == buffer.len()));
                    let (index, offset) =
                        if let Some(index) = same.filter(|_| *offset <= i32::MAX as usize) {
                            (index, *offset)
                        } else {
                            if self.builder.regions.len() > i32::MAX as usize {
                                return Err(WriteFailure::Full);
                            }
                            reserve(&mut self.builder.regions, 1, rows, growth)?;
                            let index = self.builder.regions.len();
                            let (region, relative) = if *offset > i32::MAX as usize {
                                (buffer.slice_with_length(*offset, *len), 0)
                            } else {
                                (buffer.clone(), *offset)
                            };
                            self.builder.regions.push(Region::Shared(region));
                            (index, relative)
                        };
                    arrow_raw::external_view(
                        self.builder.layout,
                        &buffer.as_slice()[*self.data_offset()..*self.data_offset() + *len],
                        index,
                        offset,
                    )?
                }
                Pending::Owned { region: None, .. } => unreachable!(),
            }
        } else {
            0
        };
        // All fallible work is complete. Publish the row and bitmap together.
        if rows % 8 == 0 {
            self.builder.validity.push(0);
        }
        if valid {
            self.builder.validity[rows / 8] |= 1 << (rows % 8);
        } else {
            self.builder.null_count += 1;
        }
        if self.builder.layout.is_view() {
            self.builder.views.push(descriptor);
        } else if matches!(self.builder.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            if rows == 0 {
                self.builder.offsets32.push(0);
            }
            self.builder
                .offsets32
                .push(self.builder.payload.len() as i32);
        } else {
            if rows == 0 {
                self.builder.offsets64.push(0);
            }
            self.builder
                .offsets64
                .push(self.builder.payload.len() as i64);
        }
        self.builder.rows = next_rows;
        self.builder.logical_bytes = next_logical;
        self.done = true;
        Ok(())
    }

    fn data_offset(&self) -> &usize {
        match &self.data {
            Pending::Shared { offset, .. } => offset,
            _ => unreachable!(),
        }
    }
}

/// Pure arithmetic, also used by planners to test real limits without allocating
/// gigabytes. For offsets, used_payload is the committed prefix's payload.
pub fn check_value_extent<E>(
    layout: ByteLayout,
    used_payload: u64,
    value_bytes: u64,
    committed_rows: usize,
) -> Result<(), WriteFailure<E>> {
    let limit = layout.extent_limit();
    if value_bytes > limit {
        return Err(ValueError::Representation(layout.representation(
            RepresentationBoundary::MinimumUnit,
            Some(value_bytes),
            Some(limit),
            None,
        ))
        .into());
    }
    if !layout.is_view()
        && used_payload
            .checked_add(value_bytes)
            .is_none_or(|total| total > limit)
    {
        return Err(WriteFailure::Full);
    }
    let _ = committed_rows;
    Ok(())
}

fn allocation_extent(rows: usize, required: Option<u64>) -> ValueError {
    ValueError::Representation(field_representation(
        11,
        RepresentationBoundary::SingleOutput,
        required,
        Some(isize::MAX as u64),
        u64::try_from(rows).ok(),
    ))
}

fn reserve<T, A: Allocator, E>(
    values: &mut AllocVec<T, A>,
    additional: usize,
    rows: usize,
    growth: &mut Growth<'_, E>,
) -> Result<(), WriteFailure<E>> {
    let overflow = |required| {
        if rows > 0 {
            WriteFailure::Full
        } else {
            ValueError::Representation(field_representation(
                11,
                RepresentationBoundary::MinimumUnit,
                required,
                Some(isize::MAX as u64),
                None,
            ))
            .into()
        }
    };
    let needed = values
        .len()
        .checked_add(additional)
        .ok_or_else(|| overflow(None))?;
    if needed <= values.capacity() {
        return Ok(());
    }
    // Doubling affects efficiency only. Near the real Layout limit, fall back
    // to the exact required extent rather than rejecting a representable row.
    let preferred = values
        .capacity()
        .checked_mul(2)
        .unwrap_or(needed)
        .max(needed);
    let (capacity, layout) = match Layout::array::<T>(preferred) {
        Ok(layout) => (preferred, layout),
        Err(_) => (
            needed,
            Layout::array::<T>(needed).map_err(|_| {
                overflow(
                    needed
                        .checked_mul(std::mem::size_of::<T>())
                        .and_then(|bytes| u64::try_from(bytes).ok()),
                )
            })?,
        ),
    };
    growth(layout).map_err(ValueFailure::Caller)?;
    values
        .try_reserve_exact(capacity - values.len())
        .map_err(|source| ValueError::Allocator {
            requested: layout.size(),
            source,
        })?;
    Ok(())
}

#[cfg(test)]
mod tests;
