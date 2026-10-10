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
//! ONE existing bitmap decoder/renderer opaque request proof, borrowed by exact consumers.
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{AggregateStateAllocator, KernelFailure};
use std::{alloc::Layout, sync::Arc};
pub(crate) struct BitmapDecodeResources {
    copies: OpaqueRetainedCharge,
    refused: bool,
}
impl BitmapDecodeResources {
    pub(crate) fn try_new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        Ok(Self {
            copies: OpaqueRetainedCharge::try_new(host)?,
            refused: false,
        })
    }
    pub(crate) fn refused(&self) -> bool {
        self.refused
    }
    fn boundary(&mut self, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
        work.flush().map_err(|cause| {
            self.refused = true;
            cause
        })
    }
    fn reserve(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let retained = self
            .copies
            .bytes()
            .checked_add(bytes)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Layout::from_size_align(bytes, 1).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut reservation = self.copies.reserve_operation(bytes).map_err(|cause| {
            self.refused = true;
            cause
        })?;
        self.copies
            .reconcile_under_reservation(retained, &mut reservation)
            .map_err(|cause| {
                self.refused = true;
                cause
            })
    }
    fn resource() -> KernelFailure {
        KernelFailure::ResourceExhausted
    }
    pub(crate) fn before_tree_insert(
        &mut self,
        _existing: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        // Holding every insertion's cumulative node ceiling until all original
        // row temporaries die also covers split requests consuming earlier slack.
        let layout =
            novarocks_type_contract::owned_resources::btree::node_layout_typed::<u64, ()>()
                .map_err(|_| internal("bitmap tree source request model drift"))?;
        self.boundary(work)?;
        self.reserve(layout.size())
    }
    pub(crate) fn before_tree_collection(
        &mut self,
        entries: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let facts =
            novarocks_type_contract::owned_resources::btree::insertion_only::<u64, ()>(entries)
                .map_err(|_| internal("bitmap tree source request model drift"))?;
        self.boundary(work)?;
        // The original BTreeSet::from_iter also collects and stable-sorts
        // a Vec<u64> before bulk construction. Its input/sort scratch coexist.
        let scratch = entries.max(4).checked_mul(24).ok_or_else(Self::resource)?;
        let bound = facts
            .request_bytes_upper_bound
            .checked_add(scratch)
            .ok_or_else(Self::resource)?;
        self.reserve(bound)
    }
    pub(crate) fn before_roaring(
        &mut self,
        bytes: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        // REVIEW GATE: pinned roaring 0.10.12 source request proof must be
        // reviewed before this private draft can be registered. No header
        // interpreter is added. Input extent bounds successful descriptions/
        // stores; the fixed term includes malformed pre-read allocations.
        let bound = bytes
            .checked_mul(32768)
            .and_then(|n| n.checked_add(1 << 20))
            .ok_or_else(Self::resource)?;
        self.boundary(work)?;
        self.reserve(bound)
    }
    pub(crate) fn before_u32_collection(
        &mut self,
        entries: u64,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let entries = usize::try_from(entries).map_err(|_| Self::resource())?;
        if entries == 0 {
            return Ok(());
        }
        let layout = Layout::array::<u32>(entries.max(4)).map_err(|_| Self::resource())?;
        self.boundary(work)?;
        self.reserve(layout.size())
    }
    pub(crate) fn before_aggregate_buffer(
        &mut self,
        bytes: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        self.boundary(work)?;
        self.reserve(bytes)
    }
    pub(crate) fn before_aggregate_singleton(
        &mut self,
        value: u64,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        // Original encode_bitmap_single constructs one additional one-element tree.
        self.before_tree_insert(0, work)?;
        self.before_aggregate_buffer(if u32::try_from(value).is_ok() { 5 } else { 9 }, work)
    }
    pub(crate) fn before_render(
        &mut self,
        entries: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        // Original Vec<String>, original integer ToString capacity ceiling,
        // and original join output coexist. No alternate renderer is installed.
        if entries == 0 {
            return Ok(());
        }
        let strings = Layout::array::<String>(entries.max(4))
            .map_err(|_| Self::resource())?
            .size();
        let payload = entries
            .checked_mul(32 + 21)
            .and_then(|n| n.checked_add(strings))
            .ok_or_else(Self::resource)?;
        self.boundary(work)?;
        self.reserve(payload)
    }
}
