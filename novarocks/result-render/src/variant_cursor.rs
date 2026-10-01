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

//! Checked, zero-copy descriptors for the engine's serialized variant form.

use crate::{RenderError, RenderErrorKind};
use std::ops::Range;

fn invalid() -> RenderError {
    RenderError {
        kind: RenderErrorKind::UnsupportedPresentation,
        output_ordinal: None,
    }
}
pub(crate) fn read(data: &[u8], at: usize, width: usize) -> Result<u32, RenderError> {
    if !(1..=4).contains(&width) {
        return Err(invalid());
    }
    let bytes = data
        .get(at..at.checked_add(width).ok_or_else(invalid)?)
        .ok_or_else(invalid)?;
    Ok(bytes
        .iter()
        .enumerate()
        .fold(0, |v, (i, b)| v | (u32::from(*b) << (8 * i))))
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct VariantView {
    pub metadata_end: usize,
    pub dictionary_size: usize,
    pub offset_width: usize,
    pub strings_start: usize,
}
impl VariantView {
    pub fn parse(data: &[u8]) -> Result<Self, RenderError> {
        let size = read(data, 0, 4)? as usize;
        if size > 16 * 1024 * 1024 || size.checked_add(4) != Some(data.len()) {
            return Err(invalid());
        }
        let header = *data.get(4).ok_or_else(invalid)?;
        if header & 15 != 1 {
            return Err(invalid());
        }
        let offset_width = 1 + usize::from(header >> 6);
        let dictionary_size = read(data, 5, offset_width)? as usize;
        let offsets_start = 5 + offset_width;
        let strings_start = offsets_start
            .checked_add(
                (dictionary_size + 1)
                    .checked_mul(offset_width)
                    .ok_or_else(invalid)?,
            )
            .ok_or_else(invalid)?;
        let last = read(
            data,
            offsets_start
                .checked_add(
                    dictionary_size
                        .checked_mul(offset_width)
                        .ok_or_else(invalid)?,
                )
                .ok_or_else(invalid)?,
            offset_width,
        )? as usize;
        let metadata_end = strings_start.checked_add(last).ok_or_else(invalid)?;
        if metadata_end >= data.len() || read(data, offsets_start, offset_width)? != 0 {
            return Err(invalid());
        }
        Ok(Self {
            metadata_end,
            dictionary_size,
            offset_width,
            strings_start,
        })
    }
    pub fn key(self, data: &[u8], id: usize) -> Result<Range<usize>, RenderError> {
        if id >= self.dictionary_size {
            return Err(invalid());
        }
        let at = 5 + self.offset_width + id * self.offset_width;
        let start = read(data, at, self.offset_width)? as usize;
        let end = read(data, at + self.offset_width, self.offset_width)? as usize;
        if start > end {
            return Err(invalid());
        }
        let start = self.strings_start.checked_add(start).ok_or_else(invalid)?;
        let end = self.strings_start.checked_add(end).ok_or_else(invalid)?;
        if end > self.metadata_end {
            return Err(invalid());
        }
        Ok(start..end)
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Collection {
    pub count: usize,
    pub id_width: usize,
    pub offset_width: usize,
    pub ids: usize,
    pub offsets: usize,
    pub data: usize,
}
impl Collection {
    pub fn parse(data: &[u8], range: Range<usize>, object: bool) -> Result<Self, RenderError> {
        let bytes = data.get(range.clone()).ok_or_else(invalid)?;
        let header = *bytes.first().ok_or_else(invalid)? >> 2;
        let offset_width = 1 + usize::from(header & 3);
        let id_width = if object {
            1 + usize::from((header >> 2) & 3)
        } else {
            0
        };
        let large = if object {
            header & 0x10 != 0
        } else {
            header & 4 != 0
        };
        if (object && header & !0x1f != 0) || (!object && header & !7 != 0) {
            return Err(invalid());
        }
        let count_width = if large { 4 } else { 1 };
        let count = read(bytes, 1, count_width)? as usize;
        let ids = 1 + count_width;
        let offsets = ids
            .checked_add(count.checked_mul(id_width).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let payload = offsets
            .checked_add((count + 1).checked_mul(offset_width).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let last = read(bytes, offsets + count * offset_width, offset_width)? as usize;
        if payload.checked_add(last) != Some(bytes.len())
            || read(bytes, offsets, offset_width)? != 0
        {
            return Err(invalid());
        }
        Ok(Self {
            count,
            id_width,
            offset_width,
            ids: range.start + ids,
            offsets: range.start + offsets,
            data: range.start + payload,
        })
    }
    pub fn child(
        self,
        bytes: &[u8],
        i: usize,
        parent_end: usize,
    ) -> Result<Range<usize>, RenderError> {
        if i >= self.count {
            return Err(invalid());
        }
        let at = self.offsets + i * self.offset_width;
        let start = read(bytes, at, self.offset_width)? as usize;
        let end = read(bytes, at + self.offset_width, self.offset_width)? as usize;
        if start >= end {
            return Err(invalid());
        }
        let start = self.data.checked_add(start).ok_or_else(invalid)?;
        let end = self.data.checked_add(end).ok_or_else(invalid)?;
        if end > parent_end {
            return Err(invalid());
        }
        Ok(start..end)
    }
}
