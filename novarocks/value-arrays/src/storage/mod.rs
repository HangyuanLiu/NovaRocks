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
use crate::{ValueError, ValueFailure, ValueViolation};
use arrow_array::Array;
use arrow_buffer::Buffer;
use arrow_schema::DataType;
use std::ops::Range;

/// General Arrow construction fact, shared with existing metadata observers.
/// This does not admit a carrier or relax active-ancestor value validation.
pub fn requires_default_validated_container(target: &DataType) -> bool {
    crate::arrow_raw::requires_default_validated_container(target)
}

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
        Self {
            buffer,
            owner: OwnerFacts::default(),
        }
    }

    pub fn with_owner(buffer: Buffer, owner: OwnerFacts) -> Self {
        Self { buffer, owner }
    }

    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }
    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }
    pub fn owner(&self) -> OwnerFacts {
        self.owner
    }
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn slice(&self, range: Range<usize>) -> Result<Self, ValueError> {
        if range.start > range.end || range.end > self.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds));
        }
        Ok(Self::with_owner(
            self.buffer
                .slice_with_length(range.start, range.end - range.start),
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

/// A loan from the actual Arrow structure. Counts describe physical tables,
/// including dictionary values and children outside a parent slice or NULL
/// domain. They do not estimate live payload or admit an engine carrier.
pub enum BorrowedNode<'source> {
    Array {
        array: &'source dyn Array,
        data_buffers: usize,
        child_count: usize,
    },
    /// Arrow stores run ends as a RunEndBuffer rather than an Array. Borrow
    /// that existing buffer instead of allocating a temporary primitive array.
    RunEnds {
        data_type: &'source DataType,
        buffer: &'source Buffer,
    },
}

impl<'source> BorrowedNode<'source> {
    pub fn data_type(&self) -> &'source DataType {
        match self {
            Self::Array { array, .. } => array.data_type(),
            Self::RunEnds { data_type, .. } => data_type,
        }
    }

    pub fn data_buffers(&self) -> usize {
        match self {
            Self::Array { data_buffers, .. } => *data_buffers,
            Self::RunEnds { .. } => 1,
        }
    }

    pub fn child_count(&self) -> usize {
        match self {
            Self::Array { child_count, .. } => *child_count,
            Self::RunEnds { .. } => 0,
        }
    }

    /// The physical buffer at this node. The logical row range does not
    /// shorten an owned child table or hide a still-mounted payload buffer.
    pub fn buffer(&self, index: usize) -> Result<&'source Buffer, ValueError> {
        match self {
            Self::Array { array, .. } => crate::arrow_raw::borrowed_buffer(*array, index),
            Self::RunEnds { buffer, .. } if index == 0 => Ok(buffer),
            Self::RunEnds { .. } => Err(ValueError::Invariant(ValueViolation::Bounds)),
        }
    }

    pub fn nulls(&self) -> Option<&'source arrow_buffer::NullBuffer> {
        match self {
            Self::Array { array, .. } => array.nulls(),
            Self::RunEnds { .. } => None,
        }
    }
}

/// Inspect only this node, without allocating ArrayData or retaining a clone.
/// This is general Arrow geometry: REE, Union, ListView and all Arrow dictionary
/// key types remain visible even when the engine's value carrier set excludes
/// them. The visitor's consumer owns any narrower semantic admission.
pub fn borrowed_node(array: &dyn Array) -> Result<BorrowedNode<'_>, ValueError> {
    let facts = crate::arrow_raw::borrowed_node(array)?;
    // Count facts alone must not turn a third-party wrapper advertising a
    // built-in data type into proof of an actual Arrow buffer table.
    for index in 0..facts.data_buffers {
        crate::arrow_raw::borrowed_buffer(array, index)?;
    }
    Ok(BorrowedNode::Array {
        array,
        data_buffers: facts.data_buffers,
        child_count: facts.child_count,
    })
}

/// Borrow a physical child by its table position, without applying logical
/// reachability. A RunEnds loan includes its original buffer and field type.
pub fn borrowed_child(array: &dyn Array, index: usize) -> Result<BorrowedNode<'_>, ValueError> {
    match crate::arrow_raw::borrowed_child(array, index)? {
        crate::arrow_raw::BorrowedChild::Array(child) => borrowed_node(child),
        crate::arrow_raw::BorrowedChild::RunEnds { data_type, buffer } => {
            Ok(BorrowedNode::RunEnds { data_type, buffer })
        }
    }
}

/// Visit the physical tree in child-before-parent order. This traversal creates
/// no metadata tables, clones, custody or control policy; an observer can refuse
/// at any node with its original cause. Shared subtrees are visited for each
/// table edge, allowing the consumer to choose its own deduplication policy.
pub fn visit_borrowed<'source, E>(
    array: &'source dyn Array,
    observe: &mut impl FnMut(BorrowedNode<'source>) -> Result<(), E>,
) -> Result<(), ValueFailure<E>> {
    fn walk<'source, E>(
        node: BorrowedNode<'source>,
        observe: &mut impl FnMut(BorrowedNode<'source>) -> Result<(), E>,
    ) -> Result<(), ValueFailure<E>> {
        if let BorrowedNode::Array {
            array, child_count, ..
        } = &node
        {
            for index in 0..*child_count {
                walk(borrowed_child(*array, index)?, observe)?;
            }
        }
        observe(node).map_err(ValueFailure::Caller)
    }
    walk(borrowed_node(array)?, observe)
}

/// Visit every physically mounted data/validity buffer, retaining the original
/// source loan. NULL parents, slice-external child rows and unreferenced
/// dictionary values remain visible. No pointer-based lifetime identity or
/// full-owner capacity is inferred from these accessible buffers.
pub fn visit_buffers<'source, E>(
    array: &'source dyn Array,
    observe: &mut impl FnMut(&'source Buffer) -> Result<(), E>,
) -> Result<(), ValueFailure<E>> {
    visit_borrowed(array, &mut |node| {
        for index in 0..node.data_buffers() {
            observe(node.buffer(index)?).map_err(ValueFailure::Caller)?;
        }
        if let Some(nulls) = node.nulls() {
            observe(nulls.inner().inner()).map_err(ValueFailure::Caller)?;
        }
        Ok::<_, ValueFailure<E>>(())
    })
    .map_err(|failure| match failure {
        ValueFailure::Value(error) => ValueFailure::Value(error),
        ValueFailure::Caller(inner) => inner,
    })
}

#[cfg(test)]
mod tests;
