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

//! Request geometry of the original type-validation pending storage.
//! Neither the fixed scratch nor the dynamic request bound grants memory.
//! Callers admit initialization and the complete original operation separately.

use arrow_schema::DataType;
use std::alloc::Layout;

/// The original borrowed traversal stack, including every slot's occupancy.
pub type TypeValidationScratch<'a> = [Option<(&'a DataType, usize)>; crate::MAX_VALUE_TYPE_NODES];

/// Actual backing layout; this function does not initialize or allocate it.
pub fn scratch_layout() -> Layout {
    Layout::new::<TypeValidationScratch<'_>>()
}

/// Byte work for one opaque initialization of the complete fixed backing.
/// Slot count is not the byte cost, and no internal cooperation is claimed.
pub fn scratch_work_upper_bound() -> usize {
    scratch_layout().size()
}

/// Cumulative heap-request upper bound for the original dynamic validator.
/// Its initial `vec![(root, 1)]` requests one element. The sole walk checks
/// visited + pending before every child push, so pending length never exceeds
/// MAX_VALUE_TYPE_NODES, including on a type or observer error. Pops cannot
/// increase capacity. The locked growth sequence after the exact one-element
/// backing is a subsequence of the original fresh-push sequence starting at
/// four elements. Include both, without replacing or pre-running validation.
///
/// This deliberately bounds every original shape; it does not inspect a
/// foreign Vec's capacity or claim that the maximum is actually allocated.
/// Element payloads are borrowed. Each emitted Layout is one possible original
/// request contribution, including the initial request; no tail is included.
pub fn original_dynamic_pending_allocation_requests_observed<
    E: From<crate::ControlResourceError>,
>(
    observe: &mut impl FnMut() -> Result<(), E>,
    allocations: &mut Option<super::metadata_materialization::MetadataAllocationLoan<'_, E>>,
) -> Result<usize, E> {
    if !super::profile::LOCKED_TOOLCHAIN {
        return Err(crate::ControlResourceError::SourceModel(
            "Type validation pending source model drift",
        )
        .into());
    }
    let initial = Layout::new::<(&DataType, usize)>();
    if let Some(allocations) = allocations.as_deref_mut() {
        allocations(initial, 1)?;
    }
    observe()?;
    let growth = super::vec::original_fresh_push_allocation_requests_observed::<
        (&DataType, usize),
        E,
    >(crate::MAX_VALUE_TYPE_NODES, observe, allocations)?;
    initial.size().checked_add(growth).ok_or_else(|| {
        E::from(crate::ControlResourceError::from(
            crate::CompileControlError::ResourceExhausted,
        ))
    })
}

#[cfg(test)]
mod tests;
