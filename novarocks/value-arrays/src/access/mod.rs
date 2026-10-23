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

//! Borrowed native byte sources for converters, boundaries and domain codecs.
//! Ordinary kernels receive the decoded view, not this storage representation.
use crate::{
    ByteLayout, ValueError, ValueViolation,
    arrow_raw::{self, ByteSpan},
};
use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_buffer::Buffer;

#[derive(Clone, Copy)]
enum SourceMapping<'a> {
    Identity,
    Constant(usize),
    Dictionary(&'a Int32Array),
}

/// Parsing borrows the admitted carrier and its explicit frozen byte layout;
/// no metadata is consulted and no buffers, masks or normalized trees are built.
#[derive(Clone, Copy)]
pub struct ByteSource<'a> {
    input: &'a ArrayRef,
    values: &'a ArrayRef,
    layout: ByteLayout,
    mapping: SourceMapping<'a>,
    len: usize,
    storage: Option<&'a novarocks_type_contract::ValueTypeStorageLifetime>,
}
impl<'a> ByteSource<'a> {
    /// Dictionary-value layout is an explicit primitive parameter. Engine
    /// callers pass D(frozen), while D1 tests can pass a view-valued dictionary.
    pub fn parse(
        input: &'a ArrayRef,
        frozen: ByteLayout,
        dictionary_values: Option<ByteLayout>,
    ) -> Result<Self, ValueError> {
        if let arrow_schema::DataType::Dictionary(keys, _) = input.data_type() {
            if !frozen.is_utf8() || keys.as_ref() != &arrow_schema::DataType::Int32 {
                return Err(ValueError::Invariant(ValueViolation::Dictionary));
            }
            let (keys, values) = arrow_raw::source_dictionary(input)?;
            let expected = dictionary_values
                .map(ByteLayout::data_type)
                .or_else(|| frozen.dictionary_value_layout())
                .ok_or(ValueError::Invariant(ValueViolation::Dictionary))?;
            if values.data_type() != &expected
                || !ByteLayout::from_data_type(&expected).is_some_and(ByteLayout::is_utf8)
            {
                return Err(ValueError::Invariant(ValueViolation::Layout));
            }
            Ok(Self {
                input,
                values,
                layout: frozen,
                mapping: SourceMapping::Dictionary(keys),
                len: input.len(),
                storage: None,
            })
        } else {
            if input.data_type() != &frozen.data_type() {
                return Err(ValueError::Invariant(ValueViolation::Layout));
            }
            Ok(Self {
                input,
                values: input,
                layout: frozen,
                mapping: SourceMapping::Identity,
                len: input.len(),
                storage: None,
            })
        }
    }
    /// Carry real source and creator custody without retaining a runtime owner.
    pub fn with_storage(
        mut self,
        storage: &'a novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        self.storage = Some(storage);
        self
    }
    pub(crate) fn storage(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage
            .cloned()
            .unwrap_or_else(novarocks_type_contract::ValueTypeStorageLifetime::pure)
    }
    pub fn constant(
        value: &'a ArrayRef,
        frozen: ByteLayout,
        rows: usize,
    ) -> Result<Self, ValueError> {
        let mut source = Self::parse(value, frozen, None)?;
        if value.len() != 1 || source.is_dictionary() {
            return Err(ValueError::Invariant(ValueViolation::Bounds));
        }
        source.mapping = SourceMapping::Constant(0);
        source.len = rows;
        Ok(source)
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub const fn layout(&self) -> ByteLayout {
        self.layout
    }
    pub fn value_layout(&self) -> ByteLayout {
        ByteLayout::from_data_type(self.values.data_type()).expect("parsed byte layout")
    }
    pub const fn input(&self) -> &'a ArrayRef {
        self.input
    }
    pub const fn values(&self) -> &'a ArrayRef {
        self.values
    }
    pub fn is_dictionary(&self) -> bool {
        matches!(self.mapping, SourceMapping::Dictionary(_))
    }
    pub fn is_constant(&self) -> bool {
        matches!(self.mapping, SourceMapping::Constant(_))
    }
    pub fn dictionary_keys(&self) -> Option<&'a Int32Array> {
        match self.mapping {
            SourceMapping::Dictionary(keys) => Some(keys),
            _ => None,
        }
    }
    /// A valid key pointing at a NULL value stays Some(index); NULL key is None.
    pub fn mapped_row(&self, row: usize) -> Result<Option<usize>, ValueError> {
        if row >= self.len {
            return Err(ValueError::Invariant(ValueViolation::Bounds));
        }
        let mapped = match self.mapping {
            SourceMapping::Identity => row,
            SourceMapping::Constant(index) => index,
            SourceMapping::Dictionary(keys) => {
                if keys.is_null(row) {
                    return Ok(None);
                }
                usize::try_from(keys.value(row))
                    .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?
            }
        };
        if mapped >= self.values.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds));
        }
        Ok(Some(mapped))
    }
    pub fn value(&self, row: usize) -> Result<Option<&'a [u8]>, ValueError> {
        let Some(row) = self.mapped_row(row)? else {
            return Ok(None);
        };
        Ok(arrow_raw::byte_span(self.values.as_ref(), row)?.map(ByteSpan::bytes))
    }
    pub fn value_len(&self, row: usize) -> Result<Option<usize>, ValueError> {
        let Some(row) = self.mapped_row(row)? else {
            return Ok(None);
        };
        Ok(
            arrow_raw::byte_span(self.values.as_ref(), row)?.map(|span| match span {
                ByteSpan::Inline(bytes) => bytes.len(),
                ByteSpan::Region { len, .. } => len,
            }),
        )
    }
    pub(crate) fn span(&self, row: usize) -> Result<Option<ByteSpan<'a>>, ValueError> {
        let Some(row) = self.mapped_row(row)? else {
            return Ok(None);
        };
        arrow_raw::byte_span(self.values.as_ref(), row)
    }
}

/// One accessible region, borrowed under the source's actual Arrow ownership.
/// The region's declared Buffer capacity does not identify the full owner size.
#[derive(Clone, Copy, Debug)]
pub struct ByteRegion<'a> {
    pub buffer: &'a Buffer,
    pub offset: usize,
    pub len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{DictionaryArray, StringArray, StringViewArray, types::Int32Type};
    use std::sync::Arc;
    #[test]
    fn parse_preserves_key_null_and_value_null_as_distinct_mapping_facts() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("x")]));
        let input: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1)]),
                values,
            )
            .unwrap(),
        );
        let source = ByteSource::parse(&input, ByteLayout::Utf8, None).unwrap();
        assert_eq!(source.mapped_row(0).unwrap(), Some(0));
        assert_eq!(source.mapped_row(1).unwrap(), None);
        assert_eq!(source.value(0).unwrap(), None);
        assert_eq!(source.value(1).unwrap(), None);
        assert_eq!(source.value(2).unwrap(), Some(b"x".as_slice()));
    }
    #[test]
    fn primitive_view_dictionary_requires_the_explicit_value_layout() {
        let values: ArrayRef = Arc::new(StringViewArray::from(vec![Some("a longer string"), None]));
        let input: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![Some(0), Some(1)]), values)
                .unwrap(),
        );
        assert!(ByteSource::parse(&input, ByteLayout::Utf8View, None).is_err());
        let source =
            ByteSource::parse(&input, ByteLayout::Utf8View, Some(ByteLayout::Utf8View)).unwrap();
        assert_eq!(
            source.value(0).unwrap(),
            Some(b"a longer string".as_slice())
        );
        assert_eq!(source.value(1).unwrap(), None);
    }
    #[test]
    fn frozen_plain_layout_and_constant_mapping_are_exact() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("twelve bytes")]));
        assert!(ByteSource::parse(&input, ByteLayout::Utf8View, None).is_err());
        let source = ByteSource::constant(&input, ByteLayout::Utf8, 4).unwrap();
        assert_eq!(source.mapped_row(3).unwrap(), Some(0));
        assert_eq!(source.value_len(3).unwrap(), Some(12));
    }
}
