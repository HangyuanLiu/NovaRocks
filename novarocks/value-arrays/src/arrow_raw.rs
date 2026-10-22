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
