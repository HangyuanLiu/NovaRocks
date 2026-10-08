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

//! Explicit host allocation for state-owned fallible containers.
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::alloc::{AllocError, Allocator};
use std::{
    alloc::Layout,
    fmt,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

pub(super) struct HostAggregateAllocator {
    host: Arc<dyn AggregateStateAllocator>,
    // The actual container's allocator records its own rejected operation.
    failure: Mutex<Option<KernelFailure>>,
}
impl HostAggregateAllocator {
    pub(super) fn new(host: Arc<dyn AggregateStateAllocator>) -> Self {
        Self {
            host,
            failure: Mutex::new(None),
        }
    }
    pub(super) fn take_failure(&self) -> KernelFailure {
        self.failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .unwrap_or(KernelFailure::ResourceExhausted)
    }
    fn refuse(&self, error: KernelFailure) -> AllocError {
        let mut slot = self
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if slot.is_none() {
            *slot = Some(error);
        }
        AllocError
    }
}
impl Clone for HostAggregateAllocator {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.host))
    }
}
impl fmt::Debug for HostAggregateAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostAggregateAllocator")
            .finish_non_exhaustive()
    }
}
// SAFETY: every nonzero block is delegated to the same clone-stable host,
// which owns exact allocation/release. Containers preserve the original Layout.
// The default resize methods reserve the replacement before releasing the old.
unsafe impl Allocator for HostAggregateAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() == 0 {
            let pointer = NonNull::new(layout.align() as *mut u8).expect("nonzero alignment");
            return Ok(NonNull::slice_from_raw_parts(pointer, 0));
        }
        if self
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_some()
        {
            return Err(AllocError);
        }
        let pointer = self
            .host
            .allocate(layout)
            .map_err(|error| self.refuse(error))?;
        Ok(NonNull::slice_from_raw_parts(pointer, layout.size()))
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            // SAFETY: exact block and Layout forwarded from this host.
            unsafe { self.host.release(pointer, layout) };
        }
    }
}

impl crate::aggregate_scalar::ScalarStateAllocator for HostAggregateAllocator {
    fn scalar_allocation_error(
        &self,
        _operation: &str,
    ) -> crate::aggregate_scalar::ScalarStateError {
        crate::aggregate_scalar::ScalarStateError::Kernel(self.take_failure())
    }
}
