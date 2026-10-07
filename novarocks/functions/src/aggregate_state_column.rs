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

//! Owned, growable aggregate state storage for one prepared call.
//!
//! A long-lived aggregate operator keeps one column per call and grows it as
//! new groups appear. The column owns both the typed states and their backing
//! blocks, so the operator never holds a slot that borrows its own storage.
//! Blocks come from a host allocator: allocation authority and memory
//! accounting stay with the host, and a block is returned only after every
//! typed state inside it was destroyed.

use crate::kernel_control::invalid;
use crate::{
    AggregateBatchInvocation, AggregateStateSlot, KernelEvaluationControl, KernelFailure,
    PreparedAggregateHandle, SelectedAggregateMergeInput, SelectedAggregateUpdateInput,
};
use arrow_array::ArrayRef;
use std::{alloc::Layout, fmt, mem::MaybeUninit, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// Host authority over aggregate state backing memory.
pub trait AggregateStateAllocator: Send + Sync {
    /// One block of exactly `layout`, which has a nonzero size. The host
    /// accounts for it until it is released.
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure>;

    /// Return one block.
    ///
    /// # Safety
    /// `block` came from `allocate(layout)` on this allocator, with this exact
    /// `layout`, and is never used again.
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout);
}

/// The process heap with no admission of its own. A host that admits or
/// accounts aggregate memory supplies its own allocator instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnaccountedAggregateStateAllocator;

impl AggregateStateAllocator for UnaccountedAggregateStateAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if layout.size() == 0 {
            return Err(invalid("aggregate state block has no size"));
        }
        // SAFETY: the layout has a nonzero size.
        NonNull::new(unsafe { std::alloc::alloc(layout) }).ok_or(KernelFailure::ResourceExhausted)
    }

    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller returns a block this allocator produced with
        // exactly this layout.
        unsafe { std::alloc::dealloc(block.as_ptr(), layout) }
    }
}

/// Every group's typed state for one prepared aggregate call, in group index
/// order, over host-allocated blocks that never move.
pub struct AggregateStateColumn {
    /// Declared first and cleared before any block is released.
    states: Vec<AggregateStateSlot<'static>>,
    blocks: Vec<NonNull<u8>>,
    handle: PreparedAggregateHandle,
    allocator: Arc<dyn AggregateStateAllocator>,
    state_layout: Layout,
    /// Bytes between consecutive states; zero for a zero-sized state.
    stride: usize,
    states_per_block: usize,
    block_layout: Layout,
}

// SAFETY: the column uniquely owns its blocks and the states inside them; the
// raw block pointers are never shared. Every slot is Send, and the handle and
// allocator are Send + Sync. The column is not Sync: its slots are not.
unsafe impl Send for AggregateStateColumn {}

impl fmt::Debug for AggregateStateColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateStateColumn")
            .field("states", &self.states.len())
            .field("blocks", &self.blocks.len())
            .field("state_layout", &self.state_layout)
            .finish_non_exhaustive()
    }
}

impl AggregateStateColumn {
    pub fn try_new(
        handle: PreparedAggregateHandle,
        allocator: Arc<dyn AggregateStateAllocator>,
        states_per_block: NonZeroUsize,
    ) -> Result<Self, KernelFailure> {
        let state_layout = handle.state_layout();
        let stride = state_layout.pad_to_align().size();
        let block_layout = Layout::from_size_align(
            stride
                .checked_mul(states_per_block.get())
                .ok_or(KernelFailure::ResourceExhausted)?,
            state_layout.align(),
        )
        .map_err(|_| KernelFailure::ResourceExhausted)?;
        Ok(Self {
            states: Vec::new(),
            blocks: Vec::new(),
            handle,
            allocator,
            state_layout,
            stride,
            states_per_block: states_per_block.get(),
            block_layout,
        })
    }

    pub fn handle(&self) -> &PreparedAggregateHandle {
        &self.handle
    }

    /// Number of initialized group states.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Bytes of host-allocated backing currently held.
    pub fn backing_bytes(&self) -> usize {
        self.blocks.len() * self.block_layout.size()
    }

    /// Owner-reported heap retained by every state, for reconciliation. This
    /// fact does not authorize allocation.
    pub fn retained_heap_bytes(&self) -> usize {
        self.states
            .iter()
            .map(AggregateStateSlot::retained_heap_bytes)
            .sum()
    }

    /// Initialize the next group's state and return its index.
    pub fn push(&mut self, control: &dyn KernelEvaluationControl) -> Result<usize, KernelFailure> {
        let index = self.states.len();
        self.states
            .try_reserve(1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let pointer = if self.stride == 0 {
            // A zero-sized state needs no backing, only its alignment.
            NonNull::new(self.state_layout.align() as *mut u8)
                .ok_or_else(|| invalid("aggregate state alignment is zero"))?
        } else {
            let within = index % self.states_per_block;
            if within == 0 {
                self.blocks
                    .try_reserve(1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let block = self.allocator.allocate(self.block_layout)?;
                self.blocks.push(block);
            }
            let block = *self
                .blocks
                .last()
                .ok_or_else(|| invalid("aggregate state block is absent"))?;
            // SAFETY: `within * stride` lies inside the block, which holds
            // `states_per_block` strides.
            unsafe { block.add(within * self.stride) }
        };
        // SAFETY: the bytes belong to a block this column owns and never moves
        // or releases while any state lives, and no other slot covers them.
        // The 'static borrow never leaves the column: its slots are destroyed
        // in Drop before the block is returned to the host.
        let storage: &'static mut [MaybeUninit<u8>] = unsafe {
            std::slice::from_raw_parts_mut(pointer.as_ptr().cast::<MaybeUninit<u8>>(), self.stride)
        };
        let slot = self.handle.initialize_in(storage, control)?;
        self.states.push(slot);
        Ok(index)
    }

    /// One update frame over these states; `mapping[i]` is the group of the
    /// input's selected row `i`.
    pub fn prepare_update_batch<'frame>(
        &'frame mut self,
        mapping: &'frame [usize],
        input: SelectedAggregateUpdateInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<AggregateBatchInvocation<'frame>, KernelFailure> {
        self.handle
            .prepare_update_batch(&mut self.states, mapping, input, control)
    }

    /// One merge frame over these states.
    pub fn prepare_merge_batch<'frame>(
        &'frame mut self,
        mapping: &'frame [usize],
        input: SelectedAggregateMergeInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<AggregateBatchInvocation<'frame>, KernelFailure> {
        self.handle
            .prepare_merge_batch(&mut self.states, mapping, input, control)
    }

    /// Emit the states at `indices`, in that order.
    pub fn emit(
        &self,
        indices: &[usize],
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        self.handle
            .emit(&self.states, indices, row_capacity, control)
    }
}

impl Drop for AggregateStateColumn {
    fn drop(&mut self) {
        // Every typed state is destroyed before its backing goes back.
        self.states.clear();
        for block in self.blocks.drain(..) {
            // SAFETY: each block came from this allocator with this layout,
            // and no state inside it is alive any more.
            unsafe { self.allocator.release(block, self.block_layout) };
        }
    }
}
