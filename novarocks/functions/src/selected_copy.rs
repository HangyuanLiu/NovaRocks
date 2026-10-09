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

#[path = "selected_copy/take_host.rs"]
mod take_host;
pub use take_host::{
    CopyIndices, CopyOperationError, CopyOperationFacts, RetainedTakeResult, take_copy_in,
};
#[path = "selected_copy/copy_buffer_peak.rs"]
mod copy_buffer_peak;
#[path = "selected_copy/slice_host.rs"]
mod slice_host;
pub use slice_host::{RetainedSliceResult, slice_copy_in};
#[path = "selected_copy/concat_host.rs"]
mod concat_host;
pub use concat_host::{RetainedConcatResult, concat_copy_in};
pub use zip::preflight_zip;

#[path = "selected_copy/root_scratch_host.rs"]
mod root_scratch_host;
pub use root_scratch_host::{
    TakeRootScratchFacts, preflight_take_child_tables_in, preflight_take_root_scratch_in,
};

#[path = "selected_copy/child_scratch.rs"]
mod child_scratch;
use child_scratch::ChildScratchVec;

#[path = "selected_copy/copy_diagnostic.rs"]
mod copy_diagnostic;
#[path = "selected_copy/concat_diagnostic.rs"]
mod concat_diagnostic;
pub use copy_diagnostic::OriginalCopyData;

use crate::KernelFailure;
use arrow_array::types::{ByteArrayType, Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericByteArray, GenericListArray, GenericListViewArray,
    MapArray, OffsetSizeTrait, RunArray, StructArray, UnionArray, make_array,
};
use arrow_array::cast::AsArray;
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
#[derive(Clone, Copy)]
enum ScratchCoverage {
    ChildTables,
    RecursiveSelections,
}
struct CopyObservation<'a>(
    &'a mut dyn FnMut(bool) -> Result<(), KernelFailure>,
    Option<&'a crate::aggregate_host_allocator::HostAggregateAllocator>,
    ScratchCoverage,
    Option<&'a mut take_host::CopyInvoiceTotals>,
);
impl<'a> CopyObservation<'a> {
    fn selection_allocator(
        &self,
    ) -> Option<&'a crate::aggregate_host_allocator::HostAggregateAllocator> {
        match self.2 {
            ScratchCoverage::ChildTables => None,
            ScratchCoverage::RecursiveSelections => self.1,
        }
    }

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
fn mutable_buffer_extent(elements: usize, width: usize) -> Result<(), CopyError> {
    zip::rounded_capacity(mul(elements, width)?)?;
    Ok(())
}

fn offset_max(large: bool) -> usize {
    if large {
        usize::try_from(i64::MAX).unwrap_or(usize::MAX)
    } else {
        i32::MAX as usize
    }
}

struct Block {
    source: usize,
    ranges: ChildScratchVec<Range<usize>>,
    repeats: usize,
    // Only the raw-index entry can create an invalid block. Its numeric source
    // indices remain ordered for Arrow Run/Union semantics.
    raw_null: bool,
    raw_outside: usize,
}
struct Selection {
    blocks: ChildScratchVec<Block>,
    nulls: usize,
    null_ops: usize,
}
impl Selection {
    fn len(&self, work: &mut CopyObservation<'_>) -> Result<usize, CopyError> {
        self.blocks.iter().try_fold(self.nulls, |total, block| {
            work.step()?;
            let one = block.ranges.iter().try_fold(0, |len, range| {
                work.step()?;
                add(len, range.len())
            })?;
            add(total, mul(add(one, block.raw_outside)?, block.repeats)?)
        })
    }
    fn check(
        &self,
        sources: &[&dyn Array],
        work: &mut CopyObservation<'_>,
    ) -> Result<(), CopyError> {
        for block in &self.blocks {
            work.step()?;
            for range in &block.ranges {
                work.step()?;
                let source = sources.get(block.source).ok_or(CopyError::Invalid(
                    "mutable copy source index is outside its constructor sources",
                ))?;
                if block.raw_null && block.repeats == 0 {
                    continue;
                }
                if block.raw_null
                    && !matches!(
                        source.data_type(),
                        DataType::RunEndEncoded(..) | DataType::Union(..)
                    )
                {
                    continue;
                }
                if range.start > range.end || range.end > source.len() {
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
            usize,
            &Range<usize>,
            bool,
            &mut ChildScratchVec<Range<usize>>,
            &mut CopyObservation<'_>,
        ) -> Result<(), CopyError>,
    ) -> Result<Self, CopyError> {
        let allocator = work.selection_allocator();
        let mapped = self.blocks.iter().map(|block| {
            work.step()?;
            let mut ranges = ChildScratchVec::new(allocator);
            for range in &block.ranges {
                work.step()?;
                map(block.source, range, block.raw_null, &mut ranges, work)?;
            }
            Ok(Block {
                source: block.source,
                ranges,
                repeats: block.repeats,
                raw_null: block.raw_null,
                raw_outside: block.raw_outside,
            })
        });
        ChildScratchVec::try_collect(mapped, allocator).map(|blocks| Self {
            blocks,
            nulls: 0,
            null_ops: 0,
        })
    }
}

#[derive(Clone, Copy, PartialEq)]
enum CopyMode {
    Take { index_maximum: usize, raw: bool },
    Extend,
    OriginalConcat,
}

impl CopyMode {
    fn is_original_operation(self) -> bool {
        self.is_raw_take() || self == Self::OriginalConcat
    }
    fn is_take(self) -> bool {
        matches!(self, Self::Take { .. })
    }
    fn is_raw_take(self) -> bool {
        matches!(self, Self::Take { raw: true, .. })
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
    let mut work = CopyObservation(&mut observe, None, ScratchCoverage::ChildTables, None);
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
                source: 0,
                ranges: std::iter::once(start..add(start, 1)?).collect(),
                repeats: rows,
                raw_null: false,
                raw_outside: 0,
            }]
            .into(),
            nulls: 0,
            null_ops: 0,
        };
        preflight(
            source,
            &selection,
            CopyMode::Take {
                index_maximum: u32::MAX as usize,
                raw: false,
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
    mutable_capacity_many(&[data], capacity, work)
}
fn mutable_capacity_many(
    sources: &[&ArrayData],
    capacity: usize,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    mutable_constructor_geometry(sources, capacity, CopyMode::Extend, work)
}
fn mutable_constructor_geometry(
    sources: &[&ArrayData],
    capacity: usize,
    mode: CopyMode,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    work.step()?;
    // This models actual constructor children even when no source row will be
    // extended. Dictionary constructors validate the whole retained dictionary
    // length against the key carrier, including their off-by-one library limit.
    let data = *sources.first().ok_or(CopyError::Invalid(
        "mutable copy requires constructor sources",
    ))?;
    let ty = data.data_type();
    if let Some(invoice) = work.3.as_deref_mut() {
        invoice.constructor(data, sources.len(), capacity)?;
    }
    mutable_buffer_extent(add(capacity, 1)?, 8)?;
    if let Some(width) = ty.primitive_width() {
        return mutable_buffer_extent(capacity, width);
    }
    match ty {
        DataType::FixedSizeBinary(width) => mutable_buffer_extent(
            capacity,
            usize::try_from(*width).map_err(|_| CopyError::Extent)?,
        ),
        DataType::FixedSizeList(_, width) => mutable_constructor_geometry(
            &data_children(sources, 0, work)?,
            mul(
                capacity,
                usize::try_from(*width).map_err(|_| CopyError::Extent)?,
            )?,
            mode,
            work,
        ),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _) => {
            mutable_constructor_geometry(&data_children(sources, 0, work)?, capacity, mode, work)
        }
        DataType::Struct(_) | DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => {
            for index in 0..data.child_data().len() {
                mutable_constructor_geometry(
                    &data_children(sources, index, work)?,
                    capacity,
                    mode,
                    work,
                )?;
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
            let dictionaries = data_children(sources, 0, work)?;
            let mut concat = false;
            for pair in dictionaries.windows(2) {
                work.step()?;
                work.boundary()?;
                let same = pair[0].ptr_eq(pair[1]);
                work.boundary()?;
                concat |= !same;
            }
            let mut cumulative = 0;
            for dictionary in &dictionaries {
                work.step()?;
                let end = add(if concat { cumulative } else { 0 }, dictionary.len())?;
                // Arrow validates offset + len, including an unused dictionary.
                if mode != CopyMode::OriginalConcat {
                    limit(end, maximum)?;
                }
                if concat {
                    cumulative = end;
                }
            }
            if concat {
                mutable_constructor_geometry(&dictionaries, cumulative, mode, work)?;
                // The constructor copies complete domains before any selected extend.
                // Use the same visitor, not a dictionary value decoder.
                let mut arrays = Vec::new();
                let mut blocks = Vec::new();
                for (source, dictionary) in dictionaries.iter().enumerate() {
                    work.step()?;
                    work.boundary()?;
                    arrays.push(make_array((*dictionary).clone()));
                    work.boundary()?;
                    blocks.push(Block {
                        source,
                        ranges: vec![0..dictionary.len()].into(),
                        repeats: 1,
                        raw_null: false,
                        raw_outside: 0,
                    });
                }
                let mut refs = Vec::new();
                for array in &arrays {
                    work.step()?;
                    refs.push(array.as_ref());
                }
                visit(
                    &refs,
                    &Selection {
                        blocks: blocks.into(),
                        nulls: 0,
                        null_ops: 0,
                    },
                    mode,
                    work,
                )?;
            }
            // Same backing retains the original domain; different backing was
            // checked above along the actual full-domain concatenation path.
            mutable_buffer_extent(capacity, key.primitive_width().ok_or(CopyError::Extent)?)
        }
        DataType::Utf8View | DataType::BinaryView => {
            // The actual constructor appends every source variadic buffer,
            // even for an empty selected copy, and uses cumulative u32 offsets.
            let mut buffers = 0;
            for data in sources {
                work.step()?;
                buffers = add(
                    buffers,
                    data.buffers()
                        .len()
                        .checked_sub(1)
                        .ok_or(CopyError::Invalid("view source has no view record buffer"))?,
                )?;
                if mode != CopyMode::OriginalConcat {
                    limit(buffers, u32::MAX as usize)?;
                }
            }
            mutable_buffer_extent(buffers, std::mem::size_of::<arrow_buffer::Buffer>())?;
            mutable_buffer_extent(capacity, 16)
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

fn data_children<'a>(
    sources: &[&'a ArrayData],
    index: usize,
    work: &mut CopyObservation<'_>,
) -> Result<ChildScratchVec<&'a ArrayData>, CopyError> {
    buffer_extent(sources.len(), std::mem::size_of::<&ArrayData>())?;
    let mut children = ChildScratchVec::new(work.1);
    for data in sources {
        work.step()?;
        children.try_push(data.child_data().get(index).ok_or(CopyError::Invalid(
            "constant mutable-copy source has missing child data",
        ))?)?;
    }
    Ok(children)
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, CopyError> {
    array.as_any().downcast_ref().ok_or(CopyError::Invalid(
        "constant source array differs from its checked carrier",
    ))
}

// Existing entry points project a single actual source into the same visitor.
fn preflight(
    array: &dyn Array,
    selection: &Selection,
    mode: CopyMode,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    visit(&[array], selection, mode, work)
}
fn child_sources<'a>(
    sources: &[&'a dyn Array],
    work: &mut CopyObservation<'_>,
    mut child: impl FnMut(&'a dyn Array) -> Result<&'a dyn Array, CopyError>,
) -> Result<ChildScratchVec<&'a dyn Array>, CopyError> {
    buffer_extent(sources.len(), std::mem::size_of::<&dyn Array>())?;
    let mut children = ChildScratchVec::new(work.1);
    for source in sources {
        work.step()?;
        children.try_push(child(*source)?)?;
    }
    Ok(children)
}
fn full_child_selection(
    sources: &[&dyn Array],
    work: &mut CopyObservation<'_>,
) -> Result<Selection, CopyError> {
    let mut blocks = ChildScratchVec::try_with_capacity(sources.len(), work.1)?;
    for (source, array) in sources.iter().enumerate() {
        work.step()?;
        let mut ranges = ChildScratchVec::new(work.1);
        if !array.is_empty() {
            ranges.try_push(0..array.len())?;
        }
        blocks.try_push(Block {
            source,
            ranges,
            repeats: 1,
            raw_null: false,
            raw_outside: 0,
        })?;
    }
    Ok(Selection {
        blocks,
        nulls: 0,
        null_ops: 0,
    })
}

fn visit(
    sources: &[&dyn Array],
    selection: &Selection,
    mode: CopyMode,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    work.step()?;
    let array = *sources.first().ok_or(CopyError::Invalid(
        "mutable copy requires constructor sources",
    ))?;
    selection.check(sources, work)?;
    let rows = selection.len(work)?;
    if mode.is_take()
        && !mode.is_raw_take()
        && matches!(array.data_type(), DataType::RunEndEncoded(..))
        && selection.nulls != 0
    {
        return Err(CopyError::Invalid(
            "Arrow run-end take requires non-null indices",
        ));
    }
    if mode.is_take()
        && !mode.is_raw_take()
        && matches!(array.data_type(), DataType::Union(..))
        && selection.nulls != 0
    {
        return Err(CopyError::Invalid(
            "Arrow union take requires non-null indices",
        ));
    }
    if mode == CopyMode::OriginalConcat {
        if let Some(invoice) = work.3.as_deref_mut() {
            invoice.original_concat_node(array, sources.len(), rows)?;
        }
    } else if let Some(invoice) = work.3.as_deref_mut() {
        invoice.selected_node(array, rows, mode)?;
    }
    if rows == 0 && work.3.is_none() {
        return Ok(());
    }
    // Original take(empty) invokes new_empty_array at the root, including all
    // nested offset constructors. Only the optional operation invoice must
    // visit those children; legacy preflight keeps its original early return.
    if mode == CopyMode::OriginalConcat && matches!(array.data_type(), DataType::Null) {
        return Ok(());
    }
    if mode == CopyMode::OriginalConcat
        && matches!(
            array.data_type(),
            DataType::FixedSizeBinary(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Map(_, _)
                | DataType::Union(_, _)
        )
    {
        // The exact original fallback recursively constructs Mutable children
        // BEFORE extending selected rows. Reuse the ONE constructor geometry
        // with all actual sources, without imposing its old semantic key gate
        // on specialized concat. Original fallback errors/panics still occur
        // at the ONE Arrow call, not in this resource projection.
        let mut data = ChildScratchVec::try_with_capacity(sources.len(), work.1)?;
        for source in sources {
            work.step()?;
            work.boundary()?;
            data.try_push(source.to_data())?;
            work.boundary()?;
        }
        let mut refs = ChildScratchVec::try_with_capacity(data.len(), work.1)?;
        for source in data.iter() {
            work.step()?;
            refs.try_push(source)?;
        }
        mutable_constructor_geometry(&refs, rows, mode, work)?;
    }
    let buffer_rows = if mode == CopyMode::OriginalConcat
        && matches!(array.data_type(), DataType::RunEndEncoded(..))
    {
        0
    } else {
        rows
    };
    // The outer take has UInt32 or UInt64 indices; recursive kernels may use other
    // widths, so eight bytes conservatively bounds their index buffers.
    buffer_extent(buffer_rows, 8)?;
    buffer_extent(add(buffer_rows, 1)?, 8)?;
    if mode == CopyMode::Extend {
        interleave_bitmap_extent(rows)?;
    }
    if let Some(width) = array.data_type().primitive_width() {
        return if mode == CopyMode::Extend {
            mutable_buffer_extent(rows, width)
        } else {
            buffer_extent(rows, width)
        };
    }
    match array.data_type() {
        DataType::Null | DataType::Boolean => Ok(()),
        DataType::FixedSizeBinary(width) => mutable_buffer_extent(
            rows,
            usize::try_from(*width).map_err(|_| CopyError::Extent)?,
        ),
        DataType::Utf8 => {
            bytes::<arrow_array::types::Utf8Type>(sources, selection, mode, false, work)
        }
        DataType::LargeUtf8 => {
            bytes::<arrow_array::types::LargeUtf8Type>(sources, selection, mode, true, work)
        }
        DataType::Binary => {
            bytes::<arrow_array::types::BinaryType>(sources, selection, mode, false, work)
        }
        DataType::LargeBinary => {
            bytes::<arrow_array::types::LargeBinaryType>(sources, selection, mode, true, work)
        }
        DataType::Utf8View | DataType::BinaryView => mutable_buffer_extent(rows, 16),
        // take_dict and single-source MutableArrayData retain the dictionary
        // values unchanged; only keys expand, never its encoded value domain.
        DataType::Dictionary(key, _) => {
            mutable_buffer_extent(rows, key.primitive_width().ok_or(CopyError::Extent)?)?;
            if mode == CopyMode::OriginalConcat {
                let children = child_sources(sources, work, |source| {
                    source
                        .as_any_dictionary_opt()
                        .map(|a| a.values().as_ref())
                        .ok_or(CopyError::Invalid(
                            "original concat requires its exact dictionary carrier",
                        ))
                })?;
                let selection = full_child_selection(&children, work)?;
                if let Some(invoice) = work.3.as_deref_mut() {
                    invoice.original_dictionary_merge_scratch(&children, key, sources.len())?;
                }
                // Original merge/interleave produces at most the full values
                // domain; original fallback concatenates that domain. No key or
                // equality decoder and no Mutable dictionary cardinality gate.
                visit(&children, &selection, mode, work)?;
            } else if rows == 0 && mode.is_take() && work.3.is_some() {
                // ArrayData::new_empty creates an empty values child, instead
                // of retaining the source dictionary domain as nonempty take.
                let children = child_sources(sources, work, |source| {
                    source
                        .as_any_dictionary_opt()
                        .map(|source| source.values().as_ref())
                        .ok_or(CopyError::Invalid(
                            "mutable copy requires its exact Arrow carrier",
                        ))
                })?;
                visit(&children, selection, mode, work)?;
            }
            Ok(())
        }
        DataType::Struct(_) => {
            let first: &StructArray = downcast(array)?;
            for field in 0..first.num_columns() {
                let children = child_sources(sources, work, |source| {
                    let source: &StructArray = downcast(source)?;
                    Ok(source.column(field).as_ref())
                })?;
                visit(&children, selection, mode, work)?;
            }
            Ok(())
        }
        DataType::FixedSizeList(_, width) => {
            let _array: &FixedSizeListArray = downcast(array)?;
            let width = usize::try_from(*width).map_err(|_| CopyError::Extent)?;
            let mut child =
                selection.map_ranges(work, |source, range, raw_null, output, _work| {
                    let range = if mode.is_raw_take() && raw_null {
                        0..1
                    } else if mode.is_raw_take() && !range.is_empty() {
                        let array: &FixedSizeListArray = downcast(sources[source])?;
                        for row in range.clone() {
                            _work.step()?;
                            // Original Arrow take generates UInt32 child indices:
                            // value_offset returns i32, then is cast to u32. The
                            // signed bit pattern is not a representability gate.
                            let start = array.value_offset(row) as u32;
                            if let Some(end) = start.checked_add(width as u32) {
                                output.try_push(start as usize..end as usize)?;
                            }
                            // Overflow remains the original start+length panic
                            // (or original release-mode empty range) under the
                            // granted operation. No child copy occurs beforehand.
                        }
                        return Ok(());
                    } else {
                        mul(range.start, width)?..mul(range.end, width)?
                    };
                    if mode.is_take() && !mode.is_raw_take() {
                        limit(range.end, u32::MAX as usize)?;
                    }
                    output.try_push(range)?;
                    Ok(())
                })?;
            if mode.is_raw_take() {
                for (original, mapped) in selection.blocks.iter().zip(child.blocks.iter_mut()) {
                    work.step()?;
                    if original.raw_null {
                        // Arrow UInt32Builder::append_nulls stores zero for EVERY
                        // child index, rather than a contiguous 0..width range.
                        while !mapped.ranges.is_empty() {
                            mapped.ranges.remove(0);
                        }
                        let array: &FixedSizeListArray = downcast(sources[original.source])?;
                        if array.values().is_empty() {
                            mapped.raw_outside = 1;
                        } else {
                            mapped.ranges.try_push(0..1)?;
                            mapped.raw_outside = 0;
                        }
                        let count = original.ranges.iter().try_fold(
                            original.raw_outside,
                            |sum, range| {
                                work.step()?;
                                add(sum, range.len())
                            },
                        )?;
                        mapped.repeats = mul(mul(count, original.repeats)?, width)?;
                    }
                }
            }
            if mode.is_raw_take() {
                for (original, mapped) in selection.blocks.iter().zip(child.blocks.iter_mut()) {
                    if !original.raw_null {
                        mapped.raw_outside = mul(original.raw_outside, width)?;
                    }
                }
            }
            child.nulls = mul(selection.nulls, width)?;
            child.null_ops = if width != 0 { selection.null_ops } else { 0 };
            // Fixed-list take copies children even under a NULL parent.
            let child_mode = if mode.is_take() {
                CopyMode::Take {
                    index_maximum: u32::MAX as usize,
                    raw: mode.is_raw_take(),
                }
            } else {
                mode
            };
            let children = child_sources(sources, work, |source| {
                let source: &FixedSizeListArray = downcast(source)?;
                Ok(source.values().as_ref())
            })?;
            visit(&children, &child, child_mode, work)
        }
        DataType::List(_) => list::<i32>(sources, selection, mode, false, work),
        DataType::LargeList(_) => list::<i64>(sources, selection, mode, true, work),
        DataType::Map(_, _) => {
            let array: &MapArray = downcast(array)?;
            let child = offset_selection::<i32>(sources, selection, mode, false, work, |source| {
                let source: &MapArray = downcast(source)?;
                Ok(source.value_offsets())
            })?;
            if mode.is_take() {
                let average = array.entries().len().checked_div(array.len()).unwrap_or(0);
                work.boundary()?;
                let data = array.entries().to_data();
                work.boundary()?;
                mutable_capacity(&data, mul(average, rows)?, work)?;
            }
            let children = child_sources(sources, work, |source| {
                let source: &MapArray = downcast(source)?;
                Ok(source.entries() as &dyn Array)
            })?;
            visit(
                &children,
                &child,
                if mode == CopyMode::OriginalConcat {
                    mode
                } else {
                    CopyMode::Extend
                },
                work,
            )
        }
        DataType::ListView(_) => list_view::<i32>(sources, selection, mode, false, work),
        DataType::LargeListView(_) => list_view::<i64>(sources, selection, mode, true, work),
        DataType::Union(fields, union_mode) => {
            let _array: &UnionArray = downcast(array)?;
            if *union_mode == UnionMode::Sparse {
                for (id, _) in fields.iter() {
                    let children = child_sources(sources, work, |source| {
                        let source: &UnionArray = downcast(source)?;
                        Ok(source.child(id).as_ref())
                    })?;
                    visit(&children, selection, mode, work)?;
                }
            } else {
                for (child_index, (id, _)) in fields.iter().enumerate() {
                    let mut child =
                        selection.map_ranges(work, |source, range, _raw_null, output, work| {
                            let array: &UnionArray = downcast(sources[source])?;
                            for row in range.clone() {
                                work.step()?;
                                if array.type_id(row) == id {
                                    let offset = array.value_offset(row);
                                    output.try_push(offset..add(offset, 1)?)?;
                                }
                            }
                            Ok(())
                        })?;
                    if mode.is_raw_take() {
                        for (original, block) in
                            selection.blocks.iter().zip(child.blocks.iter_mut())
                        {
                            block.raw_null = false;
                            block.raw_outside = 0;
                            if original.raw_null && original.raw_outside != 0 && id == 0 {
                                let array: &UnionArray = downcast(sources[original.source])?;
                                // Original take_native supplies type-id zero and
                                // offset zero, independently of source row zero.
                                if array.child(id).is_empty() {
                                    block.raw_outside = original.raw_outside;
                                } else {
                                    block.ranges.try_push(0..1)?;
                                    block.repeats = mul(original.repeats, original.raw_outside)?;
                                }
                            }
                            work.step()?;
                        }
                    }
                    // MutableArrayData appends NULLs to the first declared child
                    // and casts its end offset to i32. Include that padding in
                    // both the offset bound and the child's recursive extent.
                    if mode == CopyMode::Extend && child_index == 0 {
                        child.nulls = selection.nulls;
                        child.null_ops = selection.null_ops;
                    }
                    // Both take and MutableArrayData write signed i32 offsets.
                    if !mode.is_original_operation() {
                        limit(child.len(work)?, i32::MAX as usize)?;
                    }
                    let child_mode = if mode.is_take() {
                        CopyMode::Take {
                            index_maximum: i32::MAX as usize,
                            raw: mode.is_raw_take(),
                        }
                    } else {
                        mode
                    };
                    let children = child_sources(sources, work, |source| {
                        let source: &UnionArray = downcast(source)?;
                        Ok(source.child(id).as_ref())
                    })?;
                    visit(&children, &child, child_mode, work)?;
                }
            }
            Ok(())
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run::<Int16Type>(sources, selection, mode, i16::MAX as usize, work),
            DataType::Int32 => run::<Int32Type>(sources, selection, mode, i32::MAX as usize, work),
            DataType::Int64 => run::<Int64Type>(sources, selection, mode, offset_max(true), work),
            _ => Err(CopyError::Invalid(
                "constant run-end index carrier is invalid",
            )),
        },
        other => Err(CopyError::Unsupported(other.clone())),
    }
}

fn bytes<T: ByteArrayType>(
    sources: &[&dyn Array],
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let mut total = 0;
    for block in &selection.blocks {
        work.step()?;
        if mode.is_take() && block.raw_null {
            continue;
        }
        let array: &GenericByteArray<T> = downcast(sources[block.source])?;
        let mut one = 0;
        for range in &block.ranges {
            work.step()?;
            for row in range.clone() {
                work.step()?;
                if mode == CopyMode::Extend
                    || mode == CopyMode::OriginalConcat
                    || array.is_valid(row)
                {
                    let start = offset_value(array.value_offsets()[row])?;
                    let end = offset_value(array.value_offsets()[row + 1])?;
                    one = add(one, end.checked_sub(start).ok_or(CopyError::Extent)?)?;
                }
            }
        }
        total = add(total, mul(one, block.repeats)?)?;
    }
    if !mode.is_original_operation() {
        limit(total, offset_max(large))?;
    }
    if let Some(invoice) = work.3.as_deref_mut() {
        invoice.selected_payload(total, mode)?;
    }
    if mode == CopyMode::Extend {
        mutable_buffer_extent(total, 1)
    } else {
        buffer_extent(total, 1)
    }
}

fn offset_value<O: OffsetSizeTrait>(value: O) -> Result<usize, CopyError> {
    value.to_usize().ok_or(CopyError::Extent)
}
fn offset_selection<O: OffsetSizeTrait>(
    sources: &[&dyn Array],
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
    mut offsets: impl for<'a> FnMut(&'a dyn Array) -> Result<&'a [O], CopyError>,
) -> Result<Selection, CopyError> {
    let child = selection.map_ranges(work, |source, range, raw_null, output, work| {
        if mode.is_raw_take() && raw_null {
            return Ok(());
        }
        let array = sources[source];
        let offsets = offsets(array)?;
        if mode == CopyMode::Extend || mode == CopyMode::OriginalConcat {
            output
                .try_push(offset_value(offsets[range.start])?..offset_value(offsets[range.end])?)?;
        } else {
            for row in range.clone() {
                work.step()?;
                // take_list omits NULL parent ranges; MutableArrayData::extend
                // copies offsets/payload even when a copied parent is NULL.
                if array.is_valid(row) {
                    output
                        .try_push(offset_value(offsets[row])?..offset_value(offsets[row + 1])?)?;
                }
            }
        }
        Ok(())
    })?;
    let mut child = child;
    if mode.is_raw_take() {
        for block in child.blocks.iter_mut() {
            block.raw_outside = 0;
        }
    }
    if !mode.is_original_operation() {
        limit(child.len(work)?, offset_max(large))?;
    }
    Ok(child)
}
fn list<O: OffsetSizeTrait>(
    sources: &[&dyn Array],
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let array: &GenericListArray<O> = downcast(sources[0])?;
    let child = offset_selection::<O>(sources, selection, mode, large, work, |source| {
        let source: &GenericListArray<O> = downcast(source)?;
        Ok(source.value_offsets())
    })?;
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
    let children = child_sources(sources, work, |source| {
        let source: &GenericListArray<O> = downcast(source)?;
        Ok(source.values().as_ref())
    })?;
    visit(
        &children,
        &child,
        if mode == CopyMode::OriginalConcat {
            mode
        } else {
            CopyMode::Extend
        },
        work,
    )
}
fn list_view<O: OffsetSizeTrait>(
    sources: &[&dyn Array],
    selection: &Selection,
    mode: CopyMode,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let _array: &GenericListViewArray<O> = downcast(sources[0])?;
    if mode == CopyMode::OriginalConcat {
        let children = child_sources(sources, work, |source| {
            let source: &GenericListViewArray<O> = downcast(source)?;
            Ok(source.values().as_ref())
        })?;
        let selection = full_child_selection(&children, work)?;
        return visit(&children, &selection, mode, work);
    }
    if mode.is_take() {
        // Nonempty take_list_view retains its original child. Empty take uses
        // new_empty_array and recursively creates empty child offset buffers.
        let rows = selection.len(work)?;
        buffer_extent(rows, if large { 16 } else { 8 })?;
        if rows == 0 && work.3.is_some() {
            let children = child_sources(sources, work, |source| {
                let source: &GenericListViewArray<O> = downcast(source)?;
                Ok(source.values().as_ref())
            })?;
            visit(&children, selection, mode, work)?;
        }
        return Ok(());
    }
    let child = selection.map_ranges(work, |source, range, _raw_null, output, work| {
        let array: &GenericListViewArray<O> = downcast(sources[source])?;
        for row in range.clone() {
            work.step()?;
            let start = offset_value(array.value_offsets()[row])?;
            let size = offset_value(array.value_sizes()[row])?;
            output.try_push(start..add(start, size)?)?;
        }
        Ok(())
    })?;
    if !mode.is_original_operation() {
        limit(child.len(work)?, offset_max(large))?;
    }
    let children = child_sources(sources, work, |source| {
        let source: &GenericListViewArray<O> = downcast(source)?;
        Ok(source.values().as_ref())
    })?;
    visit(&children, &child, CopyMode::Extend, work)
}

fn remove_first(ranges: &mut ChildScratchVec<Range<usize>>) {
    if let Some(first) = ranges.first_mut() {
        first.start += 1;
        if first.start == first.end {
            ranges.remove(0);
        }
    }
}
fn run<R: RunEndIndexType>(
    sources: &[&dyn Array],
    selection: &Selection,
    mode: CopyMode,
    maximum: usize,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    if !mode.is_original_operation() {
        limit(selection.len(work)?, maximum)?;
    }
    let mut output = ChildScratchVec::new(work.selection_allocator());
    let mut previous = None;
    for block in &selection.blocks {
        work.step()?;
        let array: &RunArray<R> = downcast(sources[block.source])?;
        if block.repeats == 0 {
            continue;
        }
        let mut ranges: ChildScratchVec<Range<usize>> =
            ChildScratchVec::new(work.selection_allocator());
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
            if let CopyMode::Take {
                index_maximum,
                raw: false,
            } = mode
            {
                limit(end - 1, index_maximum)?;
            }
            let start = if mode.is_take() && ranges.last().is_some_and(|last| last.end - 1 == start)
            {
                add(start, 1)?
            } else {
                start
            };
            if start < end {
                ranges.try_push(start..end)?;
            }
        }
        if ranges.is_empty() {
            continue;
        }
        if mode == CopyMode::Extend || mode == CopyMode::OriginalConcat {
            output.try_push(Block {
                source: block.source,
                ranges,
                repeats: block.repeats,
                raw_null: false,
                raw_outside: 0,
            })?;
            continue;
        }
        let first = ranges[0].start;
        let last = ranges.last().expect("nonempty physical range").end - 1;
        let mut initial =
            ChildScratchVec::try_with_capacity(ranges.len(), work.selection_allocator())?;
        for range in &ranges {
            initial.try_push(range.clone())?;
            work.step()?;
        }
        if previous == Some((block.source, first)) {
            remove_first(&mut initial);
        }
        output.try_push(Block {
            source: block.source,
            ranges: initial,
            repeats: 1,
            raw_null: false,
            raw_outside: 0,
        })?;
        if block.repeats > 1 {
            if first == last {
                remove_first(&mut ranges);
            }
            output.try_push(Block {
                source: block.source,
                ranges,
                repeats: block.repeats - 1,
                raw_null: false,
                raw_outside: 0,
            })?;
        }
        previous = Some((block.source, last));
    }
    let children = child_sources(sources, work, |source| {
        let source: &RunArray<R> = downcast(source)?;
        Ok(source.values().as_ref())
    })?;
    let selected = Selection {
        blocks: output,
        nulls: if mode == CopyMode::Extend {
            selection.null_ops
        } else {
            0
        },
        null_ops: if mode == CopyMode::Extend {
            selection.null_ops
        } else {
            0
        },
    };
    if mode == CopyMode::OriginalConcat {
        let physical_rows = selected.len(work)?;
        if let Some(invoice) = work.3.as_deref_mut() {
            invoice.original_run_concat_scratch(
                physical_rows,
                sources.len(),
                size_of::<R::Native>(),
            )?;
        }
    }
    visit(&children, &selected, mode, work)
}

// Raw numeric carrier source for the standalone original Arrow operation.
// The same child/extent visitor receives ordered NULL and outside occurrences;
// old Option-based entry points retain their exact original shape and gates.
// Resource-only projection of the original concat input occurrences. Every
// child extent is visited by the SAME selected-copy author; no value/key
// comparison, serializer, or replacement concat executes here.
fn preflight_original_concat_with_invoice(
    sources: &[&dyn Array],
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
    allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
    invoice: &mut take_host::CopyInvoiceTotals,
) -> Result<(), CopyError> {
    if sources.len() <= 1 {
        // Original concat(empty) errors; concat(one) slices its exact source.
        // Neither route constructs a new payload. The caller admits their
        // original diagnostic or borrowed-slice metadata separately.
        return Ok(());
    }
    let mut work = CopyObservation(
        &mut observe,
        Some(allocator),
        ScratchCoverage::RecursiveSelections,
        Some(invoice),
    );
    let selection = full_child_selection(sources, &mut work)?;
    visit(sources, &selection, CopyMode::OriginalConcat, &mut work)
}

fn preflight_original_take_with_invoice(
    array: &dyn Array,
    indices: &take_host::CopyIndices,
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
    allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
    invoice: &mut take_host::CopyInvoiceTotals,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(
        &mut observe,
        Some(allocator),
        ScratchCoverage::RecursiveSelections,
        Some(invoice),
    );
    let mut blocks = ChildScratchVec::try_with_capacity(indices.len(), Some(allocator))?;
    for ordinal in 0..indices.len() {
        work.step()?;
        let index = indices.raw_value(ordinal);
        let mut ranges = ChildScratchVec::new(Some(allocator));
        let outside = index >= array.len();
        if !outside {
            let start = index;
            ranges.try_push(start..add(start, 1)?)?;
        }
        blocks.try_push(Block {
            source: 0,
            ranges,
            repeats: 1,
            raw_null: !indices.as_array().is_valid(ordinal),
            raw_outside: usize::from(outside),
        })?;
    }
    let selection = Selection {
        blocks,
        nulls: 0,
        null_ops: 0,
    };
    visit(
        &[array],
        &selection,
        CopyMode::Take {
            index_maximum: indices.index_maximum(),
            raw: true,
        },
        &mut work,
    )
}

/// Validate the actual UInt64 nullable take plan before the opaque Arrow copy.
/// NULL indices count toward output reservation, never toward source payload.
/// The caller flushes the same original work at entry and on success or ordinary
/// failure; observer refusals must return immediately without another callback.
pub fn preflight_take(
    array: &dyn Array,
    indices: &[Option<u64>],
    observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    preflight_take_with_root_scope(array, indices, observe, |_| Ok(()))
}

// ONE original take-plan construction and recursive extent/child author. The
// legacy entry has no host or new observation; optional root admission covers
// only the two exact original root vectors, not recursive scratch or payload.
fn preflight_take_with_root_scope<S>(
    array: &dyn Array,
    indices: &[Option<u64>],
    observe: impl FnMut(bool) -> Result<(), KernelFailure>,
    admit_root: impl FnOnce(usize) -> Result<S, CopyError>,
) -> Result<(), CopyError> {
    preflight_take_with_child_tables(
        array,
        indices,
        observe,
        admit_root,
        None,
        ScratchCoverage::ChildTables,
    )
}

fn preflight_take_with_child_tables<S>(
    array: &dyn Array,
    indices: &[Option<u64>],
    observe: impl FnMut(bool) -> Result<(), KernelFailure>,
    admit_root: impl FnOnce(usize) -> Result<S, CopyError>,
    child_allocator: Option<&crate::aggregate_host_allocator::HostAggregateAllocator>,
    coverage: ScratchCoverage,
) -> Result<(), CopyError> {
    preflight_take_with_invoice(
        array,
        indices,
        observe,
        admit_root,
        child_allocator,
        coverage,
        None,
    )
}

fn preflight_take_with_invoice<S>(
    array: &dyn Array,
    indices: &[Option<u64>],
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
    admit_root: impl FnOnce(usize) -> Result<S, CopyError>,
    child_allocator: Option<&crate::aggregate_host_allocator::HostAggregateAllocator>,
    coverage: ScratchCoverage,
    invoice: Option<&mut take_host::CopyInvoiceTotals>,
) -> Result<(), CopyError> {
    let root_scope = admit_root(indices.len())?;
    let mut work = CopyObservation(&mut observe, child_allocator, coverage, invoice);
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
    let selection = Selection {
        blocks: vec![Block {
            source: 0,
            ranges: ranges.into(),
            repeats: 1,
            raw_null: false,
            raw_outside: 0,
        }]
        .into(),
        nulls,
        null_ops: usize::from(nulls != 0),
    };
    let result = preflight(
        array,
        &selection,
        CopyMode::Take {
            index_maximum: usize::MAX,
            raw: false,
        },
        &mut work,
    );
    // Destroy the actual root plan before releasing its admitted scratch.
    drop(selection);
    drop(root_scope);
    result
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
    let mut work = CopyObservation(&mut observe, None, ScratchCoverage::ChildTables, None);
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
                source: 0,
                ranges: std::iter::once(start..end).collect(),
                repeats: 1,
                raw_null: false,
                raw_outside: 0,
            }]
            .into(),
            nulls,
            null_ops: usize::from(nulls != 0),
        };
        preflight(source, &selection, CopyMode::Extend, &mut work)
    })();
    if matches!(&result, Err(CopyError::Control(_))) {
        return result;
    }
    work.boundary()?;
    result
}

/// One exact source extension, optionally repeated, followed by one NULL padding
/// operation per repetition. Order is the actual MutableArrayData call order.
#[derive(Clone, Copy, Debug)]
pub struct ExtendSegment {
    pub source: usize,
    pub start: usize,
    pub len: usize,
    pub repeats: usize,
    pub nulls: usize,
}
/// Preflight the actual multi-source constructor and its ordered extension plan.
/// Constructor capacity is the real hint, not a bound or allocation grant.
/// All sources participate in dictionary/view setup even for an empty plan.
/// The caller owns formal scopes for these temporary headers and the library
/// copy; representability/control checks do not authorize physical allocations.
pub fn preflight_extend_multi(
    sources: &[&dyn Array],
    segments: &[ExtendSegment],
    constructor_capacity: usize,
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(&mut observe, None, ScratchCoverage::ChildTables, None);
    work.boundary()?;
    let result = (|| {
        let first = *sources.first().ok_or(CopyError::Invalid(
            "mutable copy requires constructor sources",
        ))?;
        buffer_extent(sources.len(), std::mem::size_of::<ArrayData>())?;
        buffer_extent(segments.len(), std::mem::size_of::<Block>())?;
        let mut data = Vec::new();
        for source in sources {
            work.step()?;
            let equal = novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                first.data_type(),
                source.data_type(),
                || (work.0)(false),
            )
            .map_err(CopyError::Control)?;
            if !equal {
                return Err(CopyError::Invalid(
                    "mutable copy sources differ from their complete carrier",
                ));
            }
            work.boundary()?;
            data.push(source.to_data());
            work.boundary()?;
        }
        let mut refs = Vec::new();
        for item in &data {
            work.step()?;
            refs.push(item);
        }
        mutable_capacity_many(&refs, constructor_capacity, &mut work)?;
        let mut blocks = Vec::new();
        let mut nulls = 0;
        let mut null_ops = 0;
        for segment in segments {
            work.step()?;
            let source = sources.get(segment.source).ok_or(CopyError::Invalid(
                "mutable copy source index is outside its constructor sources",
            ))?;
            let end = add(segment.start, segment.len)?;
            if segment.start > source.len() || end > source.len() {
                return Err(CopyError::Invalid(
                    "mutable copy range or padding exceeds its source or capacity",
                ));
            }
            nulls = add(nulls, mul(segment.nulls, segment.repeats)?)?;
            if segment.nulls != 0 {
                null_ops = add(null_ops, segment.repeats)?;
            }
            blocks.push(Block {
                source: segment.source,
                ranges: vec![segment.start..end].into(),
                repeats: segment.repeats,
                raw_null: false,
                raw_outside: 0,
            });
        }
        visit(
            sources,
            &Selection {
                blocks: blocks.into(),
                nulls,
                null_ops,
            },
            CopyMode::Extend,
            &mut work,
        )
    })();
    if matches!(&result, Err(CopyError::Control(_))) {
        return result;
    }
    work.boundary()?;
    result
}
#[cfg(test)]
#[path = "selected_copy/preflight_extend_multi_tests.rs"]
mod preflight_extend_multi_tests;

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
    let mut work = CopyObservation(&mut observe, None, ScratchCoverage::ChildTables, None);
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
    let mut work = CopyObservation(&mut observe, None, ScratchCoverage::ChildTables, None);
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
