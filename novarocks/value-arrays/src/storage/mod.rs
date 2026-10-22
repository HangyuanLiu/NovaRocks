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

//! Borrowed and retained storage facts, not an allocation or capacity ledger.
use crate::{ValueError, ValueViolation};
use arrow_buffer::Buffer;
use std::ops::Range;

/// Additional knowledge supplied by the actual source owner. Neither field is
/// inferred from an Arrow pointer or its declared capacity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OwnerFacts {
    pub source: Option<&'static str>,
    pub full_capacity: Option<usize>,
}

/// One accessible region. A clone keeps the real source alive; its address is
/// useful only while that source is retained by this operation.
#[derive(Clone, Debug)]
pub struct BufferRegion {
    buffer: Buffer,
    owner: OwnerFacts,
}

impl BufferRegion {
    pub fn new(buffer: Buffer) -> Self {
        Self { buffer, owner: OwnerFacts::default() }
    }

    pub fn with_owner(buffer: Buffer, owner: OwnerFacts) -> Self {
        Self { buffer, owner }
    }

    pub fn buffer(&self) -> &Buffer { &self.buffer }
    pub fn bytes(&self) -> &[u8] { self.buffer.as_slice() }
    pub fn owner(&self) -> OwnerFacts { self.owner }
    pub fn len(&self) -> usize { self.buffer.len() }
    pub fn is_empty(&self) -> bool { self.buffer.is_empty() }

    pub fn slice(&self, range: Range<usize>) -> Result<Self, ValueError> {
        if range.start > range.end || range.end > self.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds));
        }
        Ok(Self::with_owner(
            self.buffer.slice_with_length(range.start, range.end - range.start),
            self.owner,
        ))
    }

    pub fn facts(&self) -> RegionFacts {
        RegionFacts {
            base_address: self.buffer.data_ptr().as_ptr() as usize,
            offset: self.buffer.ptr_offset(),
            accessible_bytes: self.buffer.len(),
            declared_capacity: self.buffer.capacity(),
            owner: self.owner,
        }
    }
}

/// Values in one operation's coordinates. base_address is not a persistent
/// identity and never licenses reading before or after the accessible range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionFacts {
    pub base_address: usize,
    pub offset: usize,
    pub accessible_bytes: usize,
    pub declared_capacity: usize,
    pub owner: OwnerFacts,
}

impl RegionFacts {
    pub fn accessible_range(self) -> Range<usize> {
        // The range is from a valid Buffer, whose allocation cannot wrap usize.
        self.offset..self.offset + self.accessible_bytes
    }
}

/// Logical volume and reference volume are deliberately separate. The caller
/// supplies logical_bytes from its reachable row domain, including duplicates.
/// regions describe all buffers it still retains, including unreachable values.
#[derive(Clone, Debug, Default)]
pub struct BackingFacts {
    pub logical_bytes: u64,
    pub referenced_bytes: u64,
    pub regions: Vec<RegionFacts>,
}

#[cfg(test)]
mod tests;
