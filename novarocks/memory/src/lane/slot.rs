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

//! One destructor-free TLS slot; byte deltas never substitute for lifetime pins.
use super::{record::*, store::RecordStore, token::RecordRef};
use crate::sync::Ordering;

pub const SLOT_QUANTUM_BYTES: u64 = 1024 * 1024;
/// The slot is copied into/out of a `Cell`; copying it does not mint a pin.
/// Its low-level operations are unsafe because an identity is not an access right.
#[derive(Clone, Copy, Debug)]
pub struct SlotCore {
    bound: RecordRef,
    record: *const LaneRecord,
    tagged: i64,
    r1_small: i64,
    count: i64,
}
impl Default for SlotCore {
    fn default() -> Self {
        Self::new()
    }
}
impl SlotCore {
    pub const fn new() -> Self {
        Self {
            bound: RecordRef::NONE,
            record: std::ptr::null(),
            tagged: 0,
            r1_small: 0,
            count: 0,
        }
    }
    pub const fn is_empty(&self) -> bool {
        self.bound.is_none()
    }
    pub fn pending_bytes(&self) -> u64 {
        self.tagged
            .unsigned_abs()
            .saturating_add(self.r1_small.unsigned_abs())
    }
    pub const fn bound(&self) -> RecordRef {
        self.bound
    }
    /// # Safety
    /// Positive counts require a live owner; negative counts require distinct
    /// still-published allocations until this operation pins their record.
    /// A count-neutral resize requires its allocation to remain live. The store
    /// must be the same instance for all operations on this slot and must
    /// outlive every allocation/slot access. Every pinned slot must be flushed exactly once.
    /// Deltas must preserve the representable signed 40-bit allocation count.
    pub unsafe fn add(
        &mut self,
        store: &RecordStore,
        reference: RecordRef,
        tagged: i64,
        r1_small: i64,
        count: i64,
        buffered: bool,
    ) {
        if !buffered
            || tagged
                .unsigned_abs()
                .saturating_add(r1_small.unsigned_abs())
                >= SLOT_QUANTUM_BYTES
        {
            // Pending facts may have pins even if a direct event releases the
            // last already-published allocation. Flush ordering preserves both.
            unsafe { Self::direct(store, reference, tagged, r1_small, count) };
            return;
        }
        if self.bound != reference {
            // SAFETY: the existing slot owns its pin; caller protects the new record.
            unsafe { self.flush(store) };
            let Some(record) = store.resolve_hook(reference) else {
                return;
            };
            record.state.fetch_add(SLOT_PIN, Ordering::AcqRel);
            self.bound = reference;
            self.record = std::ptr::from_ref(record);
            store.faults.pin_added();
        }
        // A slot pin prevents generation change. Still verify before every
        // cached-pointer dereference so a malformed identity never gets routed.
        let record = unsafe { &*self.record };
        if record.generation.load(Ordering::Acquire) != reference.generation {
            store.faults.orphan();
            return;
        }
        self.tagged = self.tagged.wrapping_add(tagged);
        self.r1_small = self.r1_small.wrapping_add(r1_small);
        self.count = self.count.wrapping_add(count);
        if self.pending_bytes() >= SLOT_QUANTUM_BYTES {
            // SAFETY: this slot owns the pin until the combined final update.
            unsafe { self.flush(store) };
        }
    }
    /// # Safety
    /// The store is the one used to bind this slot and remains alive. Flush each
    /// pin once, including on unwind; do not flush independent copies of a slot.
    pub unsafe fn flush(&mut self, store: &RecordStore) {
        if self.is_empty() {
            return;
        }
        let reference = self.bound;
        let tagged = self.tagged;
        let small = self.r1_small;
        let count = self.count;
        let pointer = self.record;
        *self = Self::new();
        // SAFETY: the slot owns a lifetime pin until its final state update.
        let record = unsafe { &*pointer };
        if record.generation.load(Ordering::Acquire) != reference.generation {
            store.faults.orphan();
            store.faults.pin_removed();
            return;
        }
        let residual_growth = (tagged > 0 || small > 0)
            && record.responsibility_class() == ResponsibilityClass::Residual;
        record.tagged.fetch_add(tagged, Ordering::AcqRel);
        record.r1_small.fetch_add(small, Ordering::AcqRel);
        record.sequence.fetch_add(1, Ordering::Release);
        // LAST record access. The count delta and unpin are one modification;
        // after it a reclaimer may immediately reuse this stable address.
        record
            .state
            .fetch_add(LifetimeState::delta(count, -1), Ordering::Release);
        if residual_growth {
            store.faults.residual_growth();
        }
        store.faults.pin_removed();
    }
    /// # Safety
    /// A live owner, genuine outstanding allocation, or independent slot pin
    /// protects this record through the final state access. A matching token
    /// alone is insufficient. Stale identities may be probed, but a resolving
    /// identity requires the access right above. The same store must remain
    /// alive until all allocation and slot accesses finish. Count and byte deltas describe real facts exactly
    /// once and preserve the representable signed 40-bit lifetime count.
    pub unsafe fn direct(
        store: &RecordStore,
        reference: RecordRef,
        tagged: i64,
        r1_small: i64,
        count: i64,
    ) {
        let Some(record) = store.resolve_hook(reference) else {
            return;
        };
        let residual_growth = (tagged > 0 || r1_small > 0)
            && record.responsibility_class() == ResponsibilityClass::Residual;
        record.tagged.fetch_add(tagged, Ordering::AcqRel);
        record.r1_small.fetch_add(r1_small, Ordering::AcqRel);
        record.sequence.fetch_add(1, Ordering::Release);
        // LAST record access, including when count is zero during resize.
        record
            .state
            .fetch_add(LifetimeState::delta(count, 0), Ordering::Release);
        if residual_growth {
            store.faults.residual_growth();
        }
    }
}
