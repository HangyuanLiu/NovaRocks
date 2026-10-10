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

//! Exact original Array::slice under actual metadata admission. Slicing does
//! not mint source payload capacity; the original source lease is transferred.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{AggregateStateAllocator, KernelControlObservation, KernelEvaluationControl};
use arrow_array::{Array, ArrayRef};
use std::sync::Arc;
use super::take_host::{CopyOperationError, CopyOperationFacts};

struct SliceSource<C> {
    source: ArrayRef,
    original_owner: C,
}
impl<C> crate::arrow_result_custody::CopyInputBacking for SliceSource<C> {
    fn source_count(&self) -> usize {
        1
    }
    fn source_array_at(&self, ordinal: usize) -> Option<&dyn Array> {
        (ordinal == 0).then_some(self.source.as_ref())
    }
    fn index_array(&self) -> Option<&dyn Array> {
        // This exact original operation has no index carrier. This is a
        // nominal operation fact, not guessed missing source metadata.
        None
    }
}

pub struct RetainedSliceResult {
    values: ArrayRef,
    metadata_envelope: usize,
}
impl RetainedSliceResult {
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
    pub fn metadata_envelope(&self) -> usize {
        self.metadata_envelope
    }
    pub fn into_values(self) -> ArrayRef {
        self.values
    }
}

/// Invoke ONE original slice exactly once. Its original out-of-bounds/invalid
/// carrier panic propagates; neither row addresses nor carrier metadata are
/// repaired. The caller supplies the actual source backing owner.
pub fn slice_copy_in<C: Send + Sync + 'static>(
    source: ArrayRef,
    offset: usize,
    len: usize,
    original_owner: C,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<RetainedSliceResult, CopyOperationError> {
    let source = SliceSource {
        source,
        original_owner,
    };
    let observed = KernelControlObservation::new(control);
    observed.checkpoint(0)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
    let allocator = HostAggregateAllocator::try_new(host)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    let metadata =
        crate::array_backing_geometry::copy_metadata_bytes(source.source.as_ref(), &mut work)?;
    let mut facts = CopyOperationFacts::borrowed_slice(metadata)?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let sliced = source.source.slice(offset, len);
    facts.bind_original_copy(&sliced);
    work.flush()?;
    let retained = crate::arrow_result_custody::retain_copied_result_backing(
        sliced,
        source,
        charge,
        &mut reservation,
        allocator,
        &facts,
        &mut work,
    )?;
    work.finish()?;
    Ok(RetainedSliceResult {
        values: retained.values,
        metadata_envelope: retained.new_backing_envelope,
    })
}
