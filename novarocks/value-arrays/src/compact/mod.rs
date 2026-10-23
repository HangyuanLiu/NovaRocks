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

//! Plan shared interval copies before allocating output. Input aliases keep
//! their own custody; this operation reports reference changes, never releases.
use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation, arrow_raw,
    control::{Growth, Work},
    error::field_representation,
};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::Buffer;
use novarocks_failure_contract::RepresentationBoundary;
use std::{alloc::Layout, convert::Infallible};

const SORT_RUN: usize = 1024;
const COPY_TURN: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactFacts {
    pub rows: usize,
    pub copied_bytes: u128,
    pub copied_ranges: usize,
    pub retained_regions: usize,
    pub output_regions: usize,
}
#[derive(Clone, Copy, Default)]
struct Row {
    region: usize,
    offset: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Reference {
    base: usize,
    start: usize,
    end: usize,
    row: usize,
}
#[derive(Clone, Copy)]
struct Group {
    base: usize,
    start: usize,
    end: usize,
    first: usize,
    past: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RegionKey {
    copied: bool,
    source: usize,
    offset: usize,
    len: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RegionUse {
    key: RegionKey,
    row: usize,
}

pub struct CompactPlan {
    source: ArrayRef,
    layout: ByteLayout,
    rows: Vec<Row>,
    references: Vec<Reference>,
    groups: Vec<Group>,
    regions: Vec<RegionKey>,
    output_layouts: Vec<Layout>,
    planning_scratch_bytes: usize,
    retained_scratch_bytes: usize,
    facts: CompactFacts,
}
pub struct CompactedArray {
    pub array: ArrayRef,
    pub facts: CompactFacts,
}

fn extent(required: Option<u64>) -> ValueError {
    ValueError::Representation(field_representation(
        11,
        RepresentationBoundary::SingleOutput,
        required,
        Some(isize::MAX as u64),
        None,
    ))
}
fn validate_region_count(count: usize) -> Result<(), ValueError> {
    if count > i32::MAX as usize + 1 {
        return Err(ValueError::Representation(field_representation(
            7,
            RepresentationBoundary::SingleOutput,
            u64::try_from(count).ok(),
            Some(i32::MAX as u64 + 1),
            None,
        )));
    }
    Ok(())
}
fn validate_address(layout: ByteLayout, len: usize, offset: usize) -> Result<(), ValueError> {
    if len > i32::MAX as usize {
        return Err(ValueError::Representation(layout.representation(
            RepresentationBoundary::MinimumUnit,
            u64::try_from(len).ok(),
            Some(i32::MAX as u64),
            None,
        )));
    }
    if offset > i32::MAX as usize {
        return Err(ValueError::Representation(field_representation(
            8,
            RepresentationBoundary::SingleOutput,
            u64::try_from(offset).ok(),
            Some(i32::MAX as u64),
            None,
        )));
    }
    Ok(())
}
fn array_layout<T>(count: usize) -> Result<Layout, ValueError> {
    Layout::array::<T>(count).map_err(|_| {
        extent(
            count
                .checked_mul(std::mem::size_of::<T>())
                .and_then(|n| u64::try_from(n).ok()),
        )
    })
}
fn reserve<T, E>(
    out: &mut Vec<T>,
    count: usize,
    growth: &mut Growth<'_, E>,
) -> Result<(), ValueFailure<E>> {
    if out.capacity() >= count {
        return Ok(());
    }
    let layout = array_layout::<T>(count)?;
    if layout.size() != 0 {
        growth(layout).map_err(ValueFailure::Caller)?;
    }
    allocate(out, count, layout)
}
fn allocate<T, E>(out: &mut Vec<T>, count: usize, layout: Layout) -> Result<(), ValueFailure<E>> {
    out.try_reserve_exact(count - out.len())
        .map_err(|source| ValueError::Allocation {
            requested: layout.size(),
            source,
        })?;
    Ok(())
}
/// Bounded runs use the standard in-place sorter. Fallible merge turns observe
/// control throughout larger sorts without retaining resumable sort state.
fn sort<T: Ord + Copy, E>(
    values: &mut [T],
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<usize, ValueFailure<E>> {
    for run in values.chunks_mut(SORT_RUN) {
        work(run.len()).map_err(ValueFailure::Caller)?;
        run.sort_unstable();
    }
    if values.len() <= SORT_RUN {
        return Ok(0);
    }
    let mut scratch = Vec::new();
    reserve(&mut scratch, values.len(), growth)?;
    for chunk in values.chunks(SORT_RUN) {
        work(chunk.len()).map_err(ValueFailure::Caller)?;
        scratch.extend_from_slice(chunk);
    }
    let mut width = SORT_RUN;
    while width < values.len() {
        for start in (0..values.len()).step_by(width.saturating_mul(2)) {
            let mid = (start + width).min(values.len());
            let end = start
                .saturating_add(width.saturating_mul(2))
                .min(values.len());
            let (mut a, mut b) = (start, mid);
            for (target, slot) in scratch.iter_mut().enumerate().take(end).skip(start) {
                if (target - start).is_multiple_of(SORT_RUN) {
                    work((end - target).min(SORT_RUN)).map_err(ValueFailure::Caller)?;
                }
                if a < mid && (b >= end || values[a] <= values[b]) {
                    *slot = values[a];
                    a += 1;
                } else {
                    *slot = values[b];
                    b += 1;
                }
            }
        }
        for (output, input) in values.chunks_mut(SORT_RUN).zip(scratch.chunks(SORT_RUN)) {
            work(input.len()).map_err(ValueFailure::Caller)?;
            output.copy_from_slice(input);
        }
        width = width.saturating_mul(2);
    }
    Ok(scratch.capacity() * std::mem::size_of::<T>())
}

impl CompactPlan {
    /// Select buffer-table regions within this source. None selects all; an
    /// empty slice selects none. Selection never changes the visible values.
    pub fn plan(source: &ArrayRef, selected_regions: Option<&[usize]>) -> Result<Self, ValueError> {
        Self::plan_observed(
            source,
            selected_regions,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok::<_, Infallible>(()),
        )
        .map_err(ValueFailure::into_error)
    }
    pub fn plan_observed<E>(
        source: &ArrayRef,
        selected_regions: Option<&[usize]>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        let layout = ByteLayout::from_data_type(source.data_type())
            .filter(|l| l.is_view())
            .ok_or(ValueError::Invariant(ValueViolation::Layout))?;
        let (_, buffers) = arrow_raw::view_parts(source.as_ref())?;
        let selected = if let Some(indices) = selected_regions {
            let mut selected =
                crate::reachability::SelectionSet::try_new_observed(buffers.len(), growth)?;
            for &index in indices {
                work(1).map_err(ValueFailure::Caller)?;
                selected.insert(index)?;
            }
            Some(selected)
        } else {
            None
        };
        let mut rows = Vec::new();
        reserve(&mut rows, source.len(), growth)?;
        while rows.len() < source.len() {
            let end = rows.len().saturating_add(SORT_RUN).min(source.len());
            work(end - rows.len()).map_err(ValueFailure::Caller)?;
            rows.resize(end, Row::default());
        }
        let mut references = Vec::new();
        reserve(&mut references, source.len(), growth)?;
        let mut uses = Vec::new();
        reserve(&mut uses, source.len(), growth)?;
        for (row, planned) in rows.iter_mut().enumerate() {
            work(1).map_err(ValueFailure::Caller)?;
            let Some((index, offset, len)) =
                arrow_raw::view_external_location(source.as_ref(), row)?
            else {
                continue;
            };
            let buffer = &buffers[index];
            if selected.as_ref().is_none_or(|ids| ids.contains(index)) {
                let start = buffer
                    .ptr_offset()
                    .checked_add(offset)
                    .ok_or_else(|| extent(None))?;
                references.push(Reference {
                    base: buffer.data_ptr().as_ptr() as usize,
                    start,
                    end: start.checked_add(len).ok_or_else(|| extent(None))?,
                    row,
                });
            } else {
                uses.push(RegionUse {
                    key: RegionKey {
                        copied: false,
                        source: index,
                        offset: 0,
                        len: buffer.len(),
                    },
                    row,
                });
                planned.offset = offset;
            }
        }
        let reference_sort_scratch = sort(&mut references, growth, work)?;
        let mut groups: Vec<Group> = Vec::new();
        reserve(&mut groups, references.len(), growth)?;
        for (index, reference) in references.iter().enumerate() {
            work(1).map_err(ValueFailure::Caller)?;
            if let Some(group) = groups
                .last_mut()
                .filter(|g| g.base == reference.base && reference.start <= g.end)
            {
                group.end = group.end.max(reference.end);
                group.past = index + 1;
            } else {
                groups.push(Group {
                    base: reference.base,
                    start: reference.start,
                    end: reference.end,
                    first: index,
                    past: index + 1,
                });
            }
        }
        let mut copied_bytes = 0u128;
        for (index, group) in groups.iter().enumerate() {
            copied_bytes += (group.end - group.start) as u128;
            for reference in &references[group.first..group.past] {
                work(1).map_err(ValueFailure::Caller)?;
                let (original_index, _, _) =
                    arrow_raw::view_external_location(source.as_ref(), reference.row)?
                        .ok_or(ValueError::Invariant(ValueViolation::Structure))?;
                let original = &buffers[original_index];
                let window_start = original.ptr_offset().max(group.start);
                let window_end = (original.ptr_offset() + original.len()).min(group.end);
                let offset = window_start - group.start;
                let len = window_end - window_start;
                let relative = reference.start - window_start;
                uses.push(RegionUse {
                    key: RegionKey {
                        copied: true,
                        source: index,
                        offset,
                        len,
                    },
                    row: reference.row,
                });
                rows[reference.row].offset = relative;
            }
        }
        let region_sort_scratch = sort(&mut uses, growth, work)?;
        let mut regions = Vec::new();
        reserve(&mut regions, uses.len(), growth)?;
        for usage in &uses {
            work(1).map_err(ValueFailure::Caller)?;
            if regions.last() != Some(&usage.key) {
                regions.push(usage.key);
            }
            rows[usage.row].region = regions.len() - 1;
        }
        // Check every output address before obtaining any output storage.
        // Compaction may split one source region into several copied groups.
        validate_region_count(regions.len())?;
        for (row, planned) in rows.iter().enumerate() {
            work(1).map_err(ValueFailure::Caller)?;
            if let Some((_, _, len)) = arrow_raw::view_external_location(source.as_ref(), row)? {
                validate_address(layout, len, planned.offset)?;
            }
        }
        let mut output_layouts = Vec::new();
        reserve(&mut output_layouts, groups.len().saturating_add(3), growth)?;
        output_layouts.push(array_layout::<u128>(rows.len())?);
        output_layouts.push(array_layout::<Buffer>(groups.len())?);
        output_layouts.push(array_layout::<Buffer>(regions.len())?);
        for group in &groups {
            work(1).map_err(ValueFailure::Caller)?;
            output_layouts.push(array_layout::<u8>(group.end - group.start)?);
        }
        let retained_scratch_bytes = rows.capacity() * std::mem::size_of::<Row>()
            + references.capacity() * std::mem::size_of::<Reference>()
            + groups.capacity() * std::mem::size_of::<Group>()
            + regions.capacity() * std::mem::size_of::<RegionKey>()
            + output_layouts.capacity() * std::mem::size_of::<Layout>();
        let planning_scratch_bytes = retained_scratch_bytes
            .saturating_add(uses.capacity() * std::mem::size_of::<RegionUse>())
            .saturating_add(reference_sort_scratch.max(region_sort_scratch))
            .saturating_add(selected.as_ref().map_or(0, |set| set.descriptor_bytes()));
        let facts = CompactFacts {
            rows: source.len(),
            copied_bytes,
            copied_ranges: groups.len(),
            retained_regions: regions.iter().filter(|r| !r.copied).count(),
            output_regions: regions.len(),
        };
        Ok(Self {
            source: source.clone(),
            layout,
            rows,
            references,
            groups,
            regions,
            output_layouts,
            planning_scratch_bytes,
            retained_scratch_bytes,
            facts,
        })
    }
    pub fn facts(&self) -> CompactFacts {
        self.facts
    }
    pub fn output_layouts(&self) -> &[Layout] {
        &self.output_layouts
    }
    pub fn retained_scratch_bytes(&self) -> usize {
        self.retained_scratch_bytes
    }
    pub fn planning_scratch_bytes(&self) -> usize {
        self.planning_scratch_bytes
    }
    pub fn execute(&self) -> Result<CompactedArray, ValueError> {
        self.execute_observed(&mut |_| Ok::<_, Infallible>(()), &mut |_| {
            Ok::<_, Infallible>(())
        })
        .map_err(ValueFailure::into_error)
    }
    pub fn execute_observed<E>(
        &self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<CompactedArray, ValueFailure<E>> {
        // Refusal is resolved before any output payload exists. The plan's
        // retained scratch was already admitted in planning and is not charged
        // again as execution growth.
        for layout in &self.output_layouts {
            work(1).map_err(ValueFailure::Caller)?;
            if layout.size() != 0 {
                growth(*layout).map_err(ValueFailure::Caller)?;
            }
        }
        let mut copied = Vec::new();
        allocate(
            &mut copied,
            self.groups.len(),
            array_layout::<Buffer>(self.groups.len())?,
        )?;
        for group in &self.groups {
            let len = group.end - group.start;
            let mut bytes = Vec::new();
            allocate(&mut bytes, len, array_layout::<u8>(len)?)?;
            let mut cursor = group.start;
            for reference in &self.references[group.first..group.past] {
                work(1).map_err(ValueFailure::Caller)?;
                if reference.end <= cursor {
                    continue;
                }
                if reference.start > cursor {
                    return Err(ValueError::Invariant(ValueViolation::Bounds).into());
                }
                let span = arrow_raw::byte_span(self.source.as_ref(), reference.row)?
                    .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
                let selected = &span.bytes()[cursor - reference.start..];
                for chunk in selected.chunks(COPY_TURN) {
                    work(chunk.len()).map_err(ValueFailure::Caller)?;
                    bytes.extend_from_slice(chunk);
                }
                cursor = reference.end;
            }
            if cursor != group.end {
                return Err(ValueError::Invariant(ValueViolation::Bounds).into());
            }
            copied.push(Buffer::from_vec(bytes));
        }
        let (_, original) = arrow_raw::view_parts(self.source.as_ref())?;
        let mut regions = Vec::new();
        allocate(
            &mut regions,
            self.regions.len(),
            array_layout::<Buffer>(self.regions.len())?,
        )?;
        for region in &self.regions {
            work(1).map_err(ValueFailure::Caller)?;
            let buffer = if region.copied {
                &copied[region.source]
            } else {
                &original[region.source]
            };
            regions.push(buffer.slice_with_length(region.offset, region.len));
        }
        let mut views = Vec::new();
        allocate(
            &mut views,
            self.rows.len(),
            array_layout::<u128>(self.rows.len())?,
        )?;
        for (row, plan) in self.rows.iter().enumerate() {
            work(1).map_err(ValueFailure::Caller)?;
            let descriptor = match arrow_raw::byte_span(self.source.as_ref(), row)? {
                None => arrow_raw::inline_view(&[])?,
                Some(arrow_raw::ByteSpan::Inline(bytes)) => arrow_raw::inline_view(bytes)?,
                Some(span) => {
                    arrow_raw::external_view(self.layout, span.bytes(), plan.region, plan.offset)?
                }
            };
            views.push(descriptor);
        }
        // SAFETY: each descriptor has exactly the bytes of the corresponding
        // admitted source value. Copy ranges are covered by retained source
        // slices, rebased offsets stay in signed fields, and NULL slots are
        // legal empty descriptors under the original exact validity bitmap.
        let array = unsafe {
            arrow_raw::make_view_unchecked(
                self.layout,
                views.into(),
                regions,
                self.source.nulls().cloned(),
            )
        }?;
        Ok(CompactedArray {
            array,
            facts: self.facts,
        })
    }
}

#[cfg(test)]
mod tests;
