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

//! Actual host admission for the original take-plan ROOT scratch only.
//! The facts are Layouts, not grants. Recursive scratch and Arrow output need
//! their separate exact invoice and admission; this entry does not run take.
use super::{
    Block, CopyError, ScratchCoverage, add, buffer_extent, mul, preflight_take_with_root_scope,
    preflight_take_with_child_tables,
};
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelControlObservation,
    KernelEvaluationControl,
};
use arrow_array::Array;
use std::{alloc::Layout, ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TakeRootScratchFacts {
    ranges: Layout,
    blocks: Layout,
    bytes: usize,
}
impl TakeRootScratchFacts {
    /// Exact layouts of Vec<Range<usize>>::with_capacity(indices.len()) and
    /// the original vec![Block { ... }]. Even NULL indices retain root capacity.
    /// Existing ONE extent arithmetic remains the layout/representability author.
    pub fn try_new(indices: usize) -> Result<Self, CopyError> {
        buffer_extent(indices, std::mem::size_of::<Range<usize>>())?;
        buffer_extent(1, std::mem::size_of::<Block>())?;
        let ranges = Layout::array::<Range<usize>>(indices).map_err(|_| CopyError::Extent)?;
        let blocks = Layout::array::<Block>(1).map_err(|_| CopyError::Extent)?;
        let bytes = add(
            mul(indices, std::mem::size_of::<Range<usize>>())?,
            blocks.size(),
        )?;
        Ok(Self {
            ranges,
            blocks,
            bytes,
        })
    }
    pub fn ranges(&self) -> Layout {
        self.ranges
    }
    pub fn blocks(&self) -> Layout {
        self.blocks
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Borrow the existing real opaque host for ROOT scratch before the original
/// vector allocations. This is not an Arrow output grant, recursive scratch
/// grant, or completed kernel invocation memory capability. No stock is used.
/// The legacy entry's arithmetic, child traversal and errors remain ONE.
pub fn preflight_take_root_scratch_in(
    array: &dyn Array,
    indices: &[Option<u64>],
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), CopyError> {
    let observed = KernelControlObservation::new(control);
    observed.checkpoint(0).map_err(CopyError::Control)?;
    let charge = OpaqueRetainedCharge::try_new(host).map_err(CopyError::Control)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    let result = preflight_take_with_root_scope(
        array,
        indices,
        |boundary| if boundary { work.flush() } else { work.step() },
        |count| {
            let facts = TakeRootScratchFacts::try_new(count)?;
            observed.checkpoint(0).map_err(CopyError::Control)?;
            // Only this actual host call grants capacity. The facts alone do not.
            let reservation = charge
                .reserve_operation(facts.bytes())
                .map_err(CopyError::Control)?;
            observed.checkpoint(0).map_err(CopyError::Control)?;
            Ok(reservation)
        },
    );
    // On ordinary Data or first control refusal there is no fallible footer.
    // Root scratch has already dropped after the original root plan Drop.
    match result {
        Ok(()) => work.finish().map_err(CopyError::Control),
        Err(error) => Err(error),
    }
}

/// Extends ROOT scratch admission with actual allocations for the SAME
/// original data_children/child_sources reference tables at every recursive
/// invocation. This does not grant other ranges/blocks/ArrayData temporaries
/// or Arrow output, and never invokes Arrow take.
pub fn preflight_take_child_tables_in(
    array: &dyn Array,
    indices: &[Option<u64>],
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), CopyError> {
    let observed = KernelControlObservation::new(control);
    observed.checkpoint(0).map_err(CopyError::Control)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host)).map_err(CopyError::Control)?;
    observed.checkpoint(0).map_err(CopyError::Control)?;
    let allocator = crate::aggregate_host_allocator::HostAggregateAllocator::try_new(host)
        .map_err(CopyError::Control)?;
    observed.checkpoint(0).map_err(CopyError::Control)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    let result = preflight_take_with_child_tables(
        array,
        indices,
        |boundary| if boundary { work.flush() } else { work.step() },
        |count| {
            let facts = TakeRootScratchFacts::try_new(count)?;
            observed.checkpoint(0).map_err(CopyError::Control)?;
            let reservation = charge
                .reserve_operation(facts.bytes())
                .map_err(CopyError::Control)?;
            observed.checkpoint(0).map_err(CopyError::Control)?;
            Ok(reservation)
        },
        Some(&allocator),
        ScratchCoverage::ChildTables,
    );
    // All recursive reference tables and root plan have already dropped. The
    // original metadata block survives until this final allocator handle Drop.
    drop(allocator);
    match result {
        Ok(()) => work.finish().map_err(CopyError::Control),
        Err(error) => Err(error),
    }
}
