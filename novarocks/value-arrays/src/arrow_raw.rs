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

//! Locked Arrow representation adapter. No SQL semantics or capacity authority.
#![allow(dead_code)] // Shared operations are wired by subsequent value modules.
use crate::{ByteLayout, ValueError, ValueViolation};
use allocator_api2::{
    alloc::{Allocator, Global},
    vec::Vec as AllocVec,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray,
};
use arrow_buffer::{ArrowNativeType, Buffer, NullBuffer, ScalarBuffer};
use arrow_data::ByteView;
use novarocks_failure_contract::RepresentationBoundary;
use std::{any::TypeId, sync::Arc};

#[derive(Clone, Copy)]
pub(crate) enum ByteSpan<'a> {
    Inline(&'a [u8]),
    Region {
        buffer: &'a Buffer,
        offset: usize,
        len: usize,
    },
}
impl<'a> ByteSpan<'a> {
    pub(crate) fn bytes(self) -> &'a [u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::Region {
                buffer,
                offset,
                len,
            } => &buffer.as_slice()[offset..offset + len],
        }
    }
}
fn concrete<T: Array + 'static>(array: &dyn Array) -> Result<&T, ValueError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or(ValueError::Invariant(ValueViolation::Carrier))
}
pub(crate) fn byte_span(array: &dyn Array, row: usize) -> Result<Option<ByteSpan<'_>>, ValueError> {
    if row >= array.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! offsets {
        ($ty:ty) => {{
            let a = concrete::<$ty>(array)?;
            let start = usize::try_from(a.value_offsets()[row])
                .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
            let end = usize::try_from(a.value_offsets()[row + 1])
                .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
            if end < start || end > a.values().len() {
                return Err(ValueError::Invariant(ValueViolation::Bounds));
            }
            ByteSpan::Region {
                buffer: a.values(),
                offset: start,
                len: end - start,
            }
        }};
    }
    macro_rules! views {
        ($ty:ty) => {{
            let a = concrete::<$ty>(array)?;
            let raw = a.views()[row];
            let v = ByteView::from(raw);
            if v.length <= 12 {
                ByteSpan::Inline(a.value(row).as_ref())
            } else {
                let buffer = a
                    .data_buffers()
                    .get(v.buffer_index as usize)
                    .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
                let offset = v.offset as usize;
                let len = v.length as usize;
                if offset.checked_add(len).is_none_or(|end| end > buffer.len()) {
                    return Err(ValueError::Invariant(ValueViolation::Bounds));
                }
                ByteSpan::Region {
                    buffer,
                    offset,
                    len,
                }
            }
        }};
    }
    Ok(Some(match array.data_type() {
        arrow_schema::DataType::Utf8 => offsets!(StringArray),
        arrow_schema::DataType::LargeUtf8 => offsets!(LargeStringArray),
        arrow_schema::DataType::Binary => offsets!(BinaryArray),
        arrow_schema::DataType::LargeBinary => offsets!(LargeBinaryArray),
        arrow_schema::DataType::Utf8View => views!(StringViewArray),
        arrow_schema::DataType::BinaryView => views!(BinaryViewArray),
        _ => return Err(ValueError::Invariant(ValueViolation::Carrier)),
    }))
}

pub(crate) fn inline_view(bytes: &[u8]) -> Result<u128, ValueError> {
    if bytes.len() > 12 {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    let mut raw = [0; 16];
    raw[..4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
    raw[4..4 + bytes.len()].copy_from_slice(bytes);
    Ok(u128::from_le_bytes(raw))
}
/// Signed field bounds come from the Arrow format, independently of Rust u32.
/// The sum offset+length is checked against the region, not limited to i32.
pub(crate) fn external_view(
    layout: ByteLayout,
    bytes: &[u8],
    buffer_index: usize,
    offset: usize,
) -> Result<u128, ValueError> {
    let invalid = |required| {
        ValueError::Representation(layout.representation(
            RepresentationBoundary::MinimumUnit,
            Some(required),
            Some(i32::MAX as u64),
            None,
        ))
    };
    if bytes.len() <= 12 {
        return inline_view(bytes);
    }
    if bytes.len() > i32::MAX as usize {
        return Err(invalid(bytes.len() as u64));
    }
    // Exhausting one output's region table is not an unrepresentable value.
    // Callers can flush an output or rebase a source slice before this adapter.
    if buffer_index > i32::MAX as usize {
        return Err(ValueError::Representation(
            crate::error::field_representation(
                7,
                RepresentationBoundary::SingleOutput,
                u64::try_from(buffer_index)
                    .ok()
                    .and_then(|v| v.checked_add(1)),
                Some(i32::MAX as u64 + 1),
                None,
            ),
        ));
    }
    if offset > i32::MAX as usize {
        return Err(ValueError::Representation(
            crate::error::field_representation(
                8,
                RepresentationBoundary::SingleOutput,
                u64::try_from(offset).ok(),
                Some(i32::MAX as u64),
                None,
            ),
        ));
    }
    Ok(ByteView {
        length: bytes.len() as u32,
        prefix: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        buffer_index: buffer_index as u32,
        offset: offset as u32,
    }
    .into())
}

/// Build only after the caller has checked every descriptor against retained
/// regions and proved UTF8 from str or an admitted string source.
///
/// # Safety
/// Every descriptor is valid for buffers, all string ranges are UTF8 and nulls
/// (when present) have exactly views.len() bits. Null slots also have legal views.
pub(crate) unsafe fn make_view_unchecked(
    layout: ByteLayout,
    views: ScalarBuffer<u128>,
    buffers: Vec<Buffer>,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ValueError> {
    let array: ArrayRef = match layout {
        ByteLayout::Utf8View => {
            Arc::new(unsafe { StringViewArray::new_unchecked(views, buffers, nulls) })
        }
        ByteLayout::BinaryView => {
            Arc::new(unsafe { BinaryViewArray::new_unchecked(views, buffers, nulls) })
        }
        _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
    };
    debug_assert!(array.to_data().validate_full().is_ok());
    Ok(array)
}

pub(crate) fn view_parts(
    array: &dyn Array,
) -> Result<(&ScalarBuffer<u128>, &[Buffer]), ValueError> {
    match array.data_type() {
        arrow_schema::DataType::Utf8View => {
            let a = concrete::<StringViewArray>(array)?;
            Ok((a.views(), a.data_buffers()))
        }
        arrow_schema::DataType::BinaryView => {
            let a = concrete::<BinaryViewArray>(array)?;
            Ok((a.views(), a.data_buffers()))
        }
        _ => Err(ValueError::Invariant(ValueViolation::Layout)),
    }
}

/// Global storage becomes ordinary Arrow backing. Foreign allocators release
/// their original storage after copying into standard backing; no deallocator
/// is guessed and no custom-buffer penalty is imposed on ordinary output.
pub(crate) fn into_arrow_buffer<T: ArrowNativeType, A: Allocator + 'static>(
    values: AllocVec<T, A>,
) -> Buffer {
    if TypeId::of::<A>() == TypeId::of::<Global>() {
        let (ptr, len, capacity, allocator) = values.into_raw_parts_with_alloc();
        drop(allocator);
        // SAFETY: allocator-api2 Global delegates to std::alloc with the same
        // Layout<T>; the TypeId check proves the exact allocator, not a proxy.
        // Ownership transfers once, with the original element count/capacity.
        Buffer::from_vec(unsafe { Vec::from_raw_parts(ptr, len, capacity) })
    } else {
        Buffer::from_vec(values.as_slice().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inline_external_and_sliced_source_ranges() {
        let source = StringArray::from(vec![Some("short"), None, Some("a much longer value")]);
        let sliced = source.slice(1, 2);
        assert!(byte_span(&sliced, 0).unwrap().is_none());
        let span = byte_span(&sliced, 1).unwrap().unwrap();
        assert_eq!(span.bytes(), b"a much longer value");
        let ByteSpan::Region {
            buffer,
            offset,
            len,
        } = span
        else {
            panic!("expected region")
        };
        let raw = external_view(ByteLayout::Utf8View, span.bytes(), 0, offset).unwrap();
        // SAFETY: raw was checked against exactly this retained source region;
        // the value comes from an admitted StringArray and the one slot is valid.
        let output = unsafe {
            make_view_unchecked(
                ByteLayout::Utf8View,
                vec![raw].into(),
                vec![buffer.clone()],
                None,
            )
        }
        .unwrap();
        assert_eq!(
            byte_span(output.as_ref(), 0).unwrap().unwrap().bytes(),
            &buffer.as_slice()[offset..offset + len]
        );
        assert_eq!(inline_view(b"abc").unwrap() as u32, 3);
    }
    #[test]
    fn address_fields_are_not_minimum_value_length_facts() {
        let bytes = b"a longer byte value";
        for (index, offset, code) in [(i32::MAX as usize + 1, 0, 7), (0, i32::MAX as usize + 1, 8)]
        {
            let error = external_view(ByteLayout::Utf8View, bytes, index, offset).unwrap_err();
            let facts = error.representation().unwrap();
            assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
            assert_eq!(facts.layout.code.get(), code);
            assert_eq!(facts.representable_prefix, None);
        }
    }
    #[test]
    fn global_buffer_retains_standard_mutability() {
        let mut values = AllocVec::new_in(Global);
        values.extend_from_slice(b"test");
        let buffer = into_arrow_buffer(values);
        assert!(buffer.into_mutable().is_ok());
    }
}

/// Identity of the visible offsets, payload and validity storage. Entries keep
/// the source array alive; these addresses never outlive that custody.
/// Slots: offsets(base,offset,len), values(base,offset,len), nulls(base,offset,
/// bit-offset,bit-length), array length, frozen physical byte-layout id.
pub(crate) fn byte_storage_identity(array: &dyn Array) -> Result<[usize; 12], ValueError> {
    let layout = ByteLayout::from_data_type(array.data_type())
        .ok_or(ValueError::Invariant(ValueViolation::Carrier))?;
    macro_rules! buffers {
        ($ty:ty) => {{
            let a = concrete::<$ty>(array)?;
            (a.offsets().inner().inner(), a.values())
        }};
    }
    let (offsets, values) = match layout {
        ByteLayout::Utf8 => buffers!(StringArray),
        ByteLayout::LargeUtf8 => buffers!(LargeStringArray),
        ByteLayout::Binary => buffers!(BinaryArray),
        ByteLayout::LargeBinary => buffers!(LargeBinaryArray),
        _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
    };
    let mut key = [0; 12];
    key[..6].copy_from_slice(&[
        offsets.data_ptr().as_ptr() as usize,
        offsets.ptr_offset(),
        offsets.len(),
        values.data_ptr().as_ptr() as usize,
        values.ptr_offset(),
        values.len(),
    ]);
    if let Some(nulls) = array.nulls() {
        let b = nulls.inner().inner();
        key[6..10].copy_from_slice(&[
            b.data_ptr().as_ptr() as usize,
            b.ptr_offset(),
            nulls.offset(),
            nulls.len(),
        ]);
    }
    key[10] = array.len();
    key[11] = layout as usize;
    Ok(key)
}

pub(crate) fn make_offsets(
    layout: ByteLayout,
    offsets: Buffer,
    payload: Buffer,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ValueError> {
    use arrow_buffer::OffsetBuffer;
    macro_rules! array {
        ($ty:ty,$offset:ty) => {{
            let offsets = OffsetBuffer::new(ScalarBuffer::<$offset>::from(offsets));
            Arc::new(<$ty>::try_new(offsets, payload, nulls)?) as ArrayRef
        }};
    }
    Ok(match layout {
        ByteLayout::Utf8 => array!(StringArray, i32),
        ByteLayout::LargeUtf8 => array!(LargeStringArray, i64),
        ByteLayout::Binary => array!(BinaryArray, i32),
        ByteLayout::LargeBinary => array!(LargeBinaryArray, i64),
        _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
    })
}

pub(crate) fn make_nulls(validity: Buffer, len: usize, null_count: usize) -> Option<NullBuffer> {
    if null_count == 0 {
        None
    } else {
        // The caller tracks the packed validity bits transactionally. Using
        // the safe constructor also verifies the exact null count in debug.
        let nulls = NullBuffer::new(arrow_buffer::BooleanBuffer::new(validity, 0, len));
        debug_assert_eq!(nulls.null_count(), null_count);
        Some(nulls)
    }
}

pub(crate) struct BorrowedNode {
    pub data_buffers: usize,
    pub child_count: usize,
}
pub(crate) fn borrowed_node(array: &dyn Array) -> Result<BorrowedNode, ValueError> {
    use arrow_schema::{DataType, UnionMode};
    let (data_buffers, child_count) = match array.data_type() {
        DataType::Null => (0, 0),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => (2, 0),
        DataType::Utf8View | DataType::BinaryView => (1 + view_parts(array)?.1.len(), 0),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => (1, 1),
        DataType::ListView(_) | DataType::LargeListView(_) => (2, 1),
        DataType::FixedSizeList(_, _) => (0, 1),
        DataType::Struct(fields) => (0, fields.len()),
        DataType::Union(fields, mode) => {
            (if *mode == UnionMode::Dense { 2 } else { 1 }, fields.len())
        }
        DataType::Dictionary(..) => (1, 1),
        DataType::RunEndEncoded(..) => (0, 2),
        _ => (1, 0),
    };
    Ok(BorrowedNode {
        data_buffers,
        child_count,
    })
}
pub(crate) enum BorrowedChild<'a> {
    Array(&'a dyn Array),
    RunEnds {
        data_type: &'a arrow_schema::DataType,
        buffer: &'a Buffer,
    },
}
pub(crate) fn borrowed_child(
    array: &dyn Array,
    index: usize,
) -> Result<BorrowedChild<'_>, ValueError> {
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_schema::DataType;
    let bounds = || ValueError::Invariant(ValueViolation::Bounds);
    macro_rules! one {
        ($ty:ty,$method:ident) => {{
            if index != 0 {
                return Err(bounds());
            }
            concrete::<$ty>(array)?.$method().as_ref()
        }};
    }
    macro_rules! dict {
        ($ty:ty) => {
            one!(DictionaryArray<$ty>, values)
        };
    }
    macro_rules! run {
        ($ty:ty,$ends:expr) => {{
            let a = concrete::<RunArray<$ty>>(array)?;
            match index {
                0 => {
                    return Ok(BorrowedChild::RunEnds {
                        data_type: $ends.data_type(),
                        buffer: a.run_ends().inner().inner(),
                    });
                }
                1 => a.values().as_ref(),
                _ => return Err(bounds()),
            }
        }};
    }
    Ok(BorrowedChild::Array(match array.data_type() {
        DataType::List(_) => one!(ListArray, values),
        DataType::LargeList(_) => one!(LargeListArray, values),
        DataType::ListView(_) => one!(ListViewArray, values),
        DataType::LargeListView(_) => one!(LargeListViewArray, values),
        DataType::FixedSizeList(_, _) => one!(FixedSizeListArray, values),
        DataType::Map(_, _) => {
            if index != 0 {
                return Err(bounds());
            }
            concrete::<MapArray>(array)?.entries()
        }
        DataType::Struct(_) => concrete::<StructArray>(array)?
            .columns()
            .get(index)
            .ok_or_else(bounds)?
            .as_ref(),
        DataType::Union(fields, _) => {
            let (id, _) = fields.iter().nth(index).ok_or_else(bounds)?;
            concrete::<UnionArray>(array)?.child(id).as_ref()
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dict!(Int8Type),
            DataType::Int16 => dict!(Int16Type),
            DataType::Int32 => dict!(Int32Type),
            DataType::Int64 => dict!(Int64Type),
            DataType::UInt8 => dict!(UInt8Type),
            DataType::UInt16 => dict!(UInt16Type),
            DataType::UInt32 => dict!(UInt32Type),
            DataType::UInt64 => dict!(UInt64Type),
            _ => return Err(bounds()),
        },
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => run!(Int16Type, ends),
            DataType::Int32 => run!(Int32Type, ends),
            DataType::Int64 => run!(Int64Type, ends),
            _ => return Err(bounds()),
        },
        _ => return Err(bounds()),
    }))
}

/// Adopt one visible source region without copying payload. Offset rebasing
/// exposes only a slice already owned by the source; no hidden bytes are read.
pub(crate) fn append_span_view(
    layout: ByteLayout,
    span: ByteSpan<'_>,
    regions: &mut Vec<Buffer>,
) -> Result<u128, ValueError> {
    let bytes = span.bytes();
    if bytes.len() <= 12 {
        return inline_view(bytes);
    }
    let ByteSpan::Region {
        buffer,
        offset,
        len,
    } = span
    else {
        return Err(ValueError::Invariant(ValueViolation::Carrier));
    };
    let (region, offset) = if offset > i32::MAX as usize {
        (buffer.slice_with_length(offset, len), 0)
    } else {
        (buffer.clone(), offset)
    };
    let identity = |b: &Buffer| (b.data_ptr(), b.ptr_offset(), b.len());
    let index = if let Some(index) = regions
        .iter()
        .position(|b| identity(b) == identity(&region))
    {
        index
    } else {
        regions
            .try_reserve(1)
            .map_err(|source| ValueError::Allocation {
                requested: std::mem::size_of::<Buffer>(),
                source,
            })?;
        let index = regions.len();
        regions.push(region);
        index
    };
    external_view(layout, bytes, index, offset)
}

#[cfg(test)]
mod structural_tests {
    use super::*;
    use arrow_array::{Int16Array, RunArray, types::Int16Type};
    #[test]
    fn run_ends_are_borrowed_as_the_actual_implicit_primitive_child() {
        let ends = Int16Array::from(vec![2, 4]);
        let values = StringArray::from(vec!["a", "b"]);
        let run = RunArray::<Int16Type>::try_new(&ends, &values).unwrap();
        let node = borrowed_node(&run).unwrap();
        assert_eq!((node.data_buffers, node.child_count), (0, 2));
        let BorrowedChild::RunEnds { data_type, buffer } = borrowed_child(&run, 0).unwrap() else {
            panic!("run ends")
        };
        assert_eq!(data_type, &arrow_schema::DataType::Int16);
        assert_eq!(buffer.as_ptr(), run.run_ends().inner().inner().as_ptr());
        let BorrowedChild::Array(child) = borrowed_child(&run, 1).unwrap() else {
            panic!("values")
        };
        assert_eq!(child.len(), 2);
    }
    #[test]
    fn offset_and_validity_slice_geometry_are_part_of_cache_identity() {
        let source = StringArray::from(vec![Some("a"), None, Some("c"), None, Some("e")]);
        let a = source.slice(0, 3);
        let b = source.slice(1, 3);
        let ka = byte_storage_identity(&a).unwrap();
        let kb = byte_storage_identity(&b).unwrap();
        assert_ne!(ka, kb);
        assert_eq!(ka[0], kb[0]);
        assert_eq!(ka[3], kb[3]);
        assert_ne!(ka[1], kb[1]);
        assert_ne!(ka[8], kb[8]);
    }
}
