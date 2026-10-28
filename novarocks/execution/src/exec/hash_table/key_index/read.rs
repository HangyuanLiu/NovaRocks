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

//! Short immutable page loans. Each page carries its actual typed binding and
//! ordered output coordinates. Encoded complex values remain explicitly encoded;
//! their Binary carrier is never presented as a List/Struct/Map Arrow array.

use super::{checkpoint_at, invariant, prepared::sort_by_key, reserve_push, vector};
use crate::{exec::pipeline::operator::InputConstructionOwner, runtime::fragment::ExecutionResult};
use arrow::array::{ArrayRef, BinaryViewArray};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{ValueViolation, batch::OwnedBytesPage};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyReadRow {
    /// Position in the caller's requested ID sequence, including duplicates.
    pub(crate) ordinal: usize,
    /// Physical row in this exact immutable page.
    pub(crate) row: usize,
}

pub(crate) enum KeyReadValues<'a> {
    Fixed {
        array: &'a ArrayRef,
        binding: &'a FunctionValueType,
    },
    Bytes {
        array: &'a ArrayRef,
        binding: &'a FunctionValueType,
        page: &'a OwnedBytesPage,
    },
    Canonical {
        encoded: &'a BinaryViewArray,
        logical_binding: &'a FunctionValueType,
    },
}

pub(crate) struct KeyReadPage<'a> {
    values: KeyReadValues<'a>,
    rows: Vec<KeyReadRow>,
}
impl<'a> KeyReadPage<'a> {
    pub(crate) fn values(&self) -> &KeyReadValues<'a> {
        &self.values
    }
    pub(crate) fn rows(&self) -> &[KeyReadRow] {
        &self.rows
    }
}
pub(crate) struct KeyReadColumn<'a> {
    pub(super) pages: Vec<KeyReadPage<'a>>,
}
impl<'a> KeyReadColumn<'a> {
    pub(crate) fn pages(&self) -> &[KeyReadPage<'a>] {
        &self.pages
    }
}
pub(crate) struct OwnedKeyReadBatch<'a> {
    pub(super) columns: Vec<KeyReadColumn<'a>>,
    pub(super) len: usize,
    // Protect all request/selection vectors until their borrowed slices exit.
    pub(super) _storage: ValueTypeStorageLifetime,
}
impl<'a> OwnedKeyReadBatch<'a> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn columns(&self) -> &[KeyReadColumn<'a>] {
        &self.columns
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReadRequest {
    pub(super) ordinal: usize,
    pub(super) batch: usize,
    pub(super) row: usize,
}
#[derive(Clone, Copy)]
pub(super) struct PositionedRow<P> {
    pub(super) page: P,
    pub(super) selected: KeyReadRow,
}

/// Both closures are monomorphized after the column strategy has been chosen.
/// The temporary contains only metadata, never copied key values.
pub(super) fn append_pages<'a>(
    requests: &[ReadRequest],
    position: impl Fn(usize) -> ExecutionResult<(usize, usize)>,
    page: impl Fn(usize) -> ExecutionResult<KeyReadValues<'a>>,
    output: &mut Vec<KeyReadPage<'a>>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let mut ordered = vector(owner, requests.len())?;
    for (i, request) in requests.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        let (page, row) = position(request.row)?;
        ordered.push(PositionedRow {
            page,
            selected: KeyReadRow {
                ordinal: request.ordinal,
                row,
            },
        });
    }
    append_positioned_pages(ordered, page, output, owner, checkpoint)
}

/// Group already-resolved real page coordinates. Hash and direct stores share
/// this author; the coordinate type does not grant access or fabricate a batch.
/// The caller funded the moved coordinate Vec through the same actual owner.
pub(super) fn append_positioned_pages<'a, P: Copy + Ord>(
    mut ordered: Vec<PositionedRow<P>>,
    page: impl Fn(P) -> ExecutionResult<KeyReadValues<'a>>,
    output: &mut Vec<KeyReadPage<'a>>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    sort_by_key(&mut ordered, owner, checkpoint, |r| {
        (r.page, r.selected.ordinal)
    })?;
    let mut begin = 0;
    while begin < ordered.len() {
        checkpoint()?;
        let mut end = begin + 1;
        while end < ordered.len() && ordered[end].page == ordered[begin].page {
            checkpoint_at(end - begin, checkpoint)?;
            end += 1;
        }
        let values = page(ordered[begin].page)?;
        let len = match &values {
            KeyReadValues::Fixed { array, .. } | KeyReadValues::Bytes { array, .. } => array.len(),
            KeyReadValues::Canonical { encoded, .. } => {
                use arrow::array::Array;
                encoded.len()
            }
        };
        let mut rows = vector(owner, end - begin)?;
        for (i, request) in ordered[begin..end].iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            if request.selected.row >= len {
                return Err(invariant(ValueViolation::Bounds));
            }
            rows.push(request.selected);
        }
        reserve_push(output, owner, checkpoint)?;
        output.push(KeyReadPage { values, rows });
        begin = end;
    }
    checkpoint()
}
