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

//! Pinned Arrow buffer geometry facts, not allocation authority.
//! The whole copy invoice must combine these with the ONE selected-copy
//! traversal and actual metadata before a caller enters the opaque operation.
use super::{CopyError, add, mul, buffer_extent, zip};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CopyBufferPeak {
    initial: usize,
    maximum_required: usize,
    retained_upper: usize,
    transient_upper: usize,
}
impl CopyBufferPeak {
    /// Arrow 58.2 MutableBuffer::reserve grows to
    /// max(round64(required), 2 * previous_capacity). For any append sequence
    /// bounded by maximum_required, every grown capacity is <= 2 * max(initial,
    /// round64(maximum_required)); its previous buffer is <= that same maximum.
    /// Reserving both covers realloc without assuming in-place growth.
    pub(super) fn mutable(initial: usize, maximum_required: usize) -> Result<Self, CopyError> {
        let initial = zip::rounded_capacity(initial)?;
        let required = zip::rounded_capacity(maximum_required)?;
        let high = initial.max(required);
        let (retained_upper, transient_upper) = if required > initial {
            let retained = mul(high, 2)?;
            (retained, add(high, retained)?)
        } else {
            (initial, initial)
        };
        buffer_extent(transient_upper, 1)?;
        Ok(Self {
            initial,
            maximum_required,
            retained_upper,
            transient_upper,
        })
    }
    /// The pinned take_bytes author sums actual selected offsets first, then
    /// creates offsets Vec(rows+1) and values Vec(payload) with exact capacity.
    /// Exact Vec capacity comes from that author, not a UTF8-per-row estimate.
    pub(super) fn exact(bytes: usize) -> Result<Self, CopyError> {
        buffer_extent(bytes, 1)?;
        Ok(Self {
            initial: bytes,
            maximum_required: bytes,
            retained_upper: bytes,
            transient_upper: bytes,
        })
    }
    pub(super) fn retained_upper(self) -> usize {
        self.retained_upper
    }
    pub(super) fn transient_upper(self) -> usize {
        self.transient_upper
    }
}
