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

//! Stable allocation origins. Hooks touch only this fixed record.
//! The registry pins a record while a scope can publish or an allocation can
//! release. A final free's count decrement is its last access; reclamation is
//! exclusively performed outside allocator hooks.
use crate::ids::AccountId;
use crate::sync::{AtomicU64, Ordering};

#[derive(Debug)]
pub(crate) struct OwnerRecord {
    pub origin: AccountId,
    pub live: AtomicU64,
    pub allocations: AtomicU64,
    pub scopes: AtomicU64,
    pub sequence: AtomicU64,
    pub covered_sequence: AtomicU64,
    pub covered_epoch: AtomicU64,
}
impl OwnerRecord {
    pub fn new(origin: AccountId) -> Self {
        Self {
            origin,
            live: AtomicU64::new(0),
            allocations: AtomicU64::new(0),
            scopes: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            covered_sequence: AtomicU64::new(0),
            covered_epoch: AtomicU64::new(0),
        }
    }
    pub fn publish(&self, bytes: u64) {
        // The active scope already pins this address, including zero-byte
        // allocations. Facts are published before the allocation can escape.
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.live.fetch_add(bytes, Ordering::AcqRel);
        self.sequence.fetch_add(1, Ordering::Release);
    }
    pub fn live(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }
    pub fn reclaimable(&self) -> bool {
        self.scopes.load(Ordering::Acquire) == 0 && self.allocations.load(Ordering::Acquire) == 0
    }
}

/// An opaque origin to store in an allocation header. It owns no account or
/// query reference. Copying a value does not mint an allocation obligation.
#[derive(Debug, Clone, Copy)]
// Design: ADR-0166 (docs/adr/ADR-0166-hierarchical-funding-and-stable-allocation-origins.md)
pub struct AllocationOrigin(*const OwnerRecord);
// Origin publication is protected by a scope; outstanding allocations pin it
// through their allocation count. Actual use remains an unsafe linear contract.
unsafe impl Send for AllocationOrigin {}
unsafe impl Sync for AllocationOrigin {}
impl AllocationOrigin {
    pub(crate) fn new(owner: &OwnerRecord) -> Self {
        Self(owner)
    }
    /// Releases one successful allocation after its underlying storage is freed.
    ///
    /// # Safety
    /// This exact origin/length must have been issued by record_allocation for
    /// this allocation. Call exactly once, after underlying free. Do not use a
    /// copied origin after that free, or release before allocation publication.
    /// The process authority must outlive this outstanding origin; shutdown
    /// never destroys records that still have raw allocation access rights.
    pub unsafe fn record_deallocation(self, bytes: u64) {
        // SAFETY: the outstanding allocation pins the record until the final
        // allocation-count decrement, which is the last record access below.
        let owner = unsafe { &*self.0 };
        owner.live.fetch_sub(bytes, Ordering::AcqRel);
        owner.sequence.fetch_add(1, Ordering::Release);
        owner.allocations.fetch_sub(1, Ordering::Release);
    }
    /// Diagnostic identity; valid while the allocation remains outstanding.
    ///
    /// # Safety
    /// The allocation this origin describes must still be outstanding.
    pub unsafe fn account_id(self) -> AccountId {
        unsafe { (*self.0).origin }
    }
}
