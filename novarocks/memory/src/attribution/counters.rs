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

//! Two counter bands share CountingAllocator's event implementation.
use crate::observe::allocator::{AllocatorSnapshot, ProcessCounters, SHARD_COUNT, shard_index};
use std::sync::atomic::{AtomicU64, Ordering};
#[repr(align(64))]
struct MigrationOverlap(AtomicU64);
impl MigrationOverlap {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
}
#[repr(align(64))]
struct DeallocationShard {
    small: AtomicU64,
    tagged: AtomicU64,
}
impl DeallocationShard {
    const fn new() -> Self {
        Self {
            small: AtomicU64::new(0),
            tagged: AtomicU64::new(0),
        }
    }
}
/// Independent atomic samples; exact at a quiescent, flushed point.
/// Per-band cumulative flows include whole-block migration; total flows remove
/// their shared overlap and preserve CountingAllocator's delta-only semantics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BandSnapshot {
    pub small: AllocatorSnapshot,
    pub tagged: AllocatorSnapshot,
    pub total: AllocatorSnapshot,
    /// Actual calls to the wrapper's dealloc route. Successful reallocations,
    /// including cross-band migration, never manufacture a deallocation event.
    pub small_deallocations: u64,
    pub tagged_deallocations: u64,
    pub total_deallocations: u64,
}
pub(crate) struct BandCounters {
    pub(crate) small: ProcessCounters,
    pub(crate) tagged: ProcessCounters,
    overlap: [MigrationOverlap; SHARD_COUNT],
    deallocations: [DeallocationShard; SHARD_COUNT],
}
impl BandCounters {
    pub(crate) const fn new() -> Self {
        Self {
            small: ProcessCounters::new(),
            tagged: ProcessCounters::new(),
            overlap: [const { MigrationOverlap::new() }; SHARD_COUNT],
            deallocations: [const { DeallocationShard::new() }; SHARD_COUNT],
        }
    }
    #[inline]
    pub(crate) fn band(&self, tagged: bool) -> &ProcessCounters {
        if tagged { &self.tagged } else { &self.small }
    }
    /// Adds only a real public dealloc event, after the inner release.
    #[inline]
    pub(crate) fn record_deallocation(&self, tagged: bool) {
        let shard = &self.deallocations[shard_index()];
        let counter = if tagged { &shard.tagged } else { &shard.small };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn migrate(&self, old_tagged: bool, old: usize, new: usize) {
        self.band(old_tagged).record_release(old);
        self.band(!old_tagged).record_migration_in(new);
        self.overlap[shard_index()]
            .0
            .fetch_add(old.min(new) as u64, Ordering::Relaxed);
    }
    pub(crate) fn snapshot(&self) -> BandSnapshot {
        let small = self.small.snapshot();
        let tagged = self.tagged.snapshot();
        let overlap = self
            .overlap
            .iter()
            .fold(0u64, |n, s| n.wrapping_add(s.0.load(Ordering::Relaxed)));
        let allocated = small
            .allocated_total_bytes
            .wrapping_add(tagged.allocated_total_bytes)
            .saturating_sub(overlap);
        let deallocated = small
            .deallocated_total_bytes
            .wrapping_add(tagged.deallocated_total_bytes)
            .saturating_sub(overlap);
        let mut small_deallocations = 0u64;
        let mut tagged_deallocations = 0u64;
        for shard in &self.deallocations {
            small_deallocations =
                small_deallocations.wrapping_add(shard.small.load(Ordering::Relaxed));
            tagged_deallocations =
                tagged_deallocations.wrapping_add(shard.tagged.load(Ordering::Relaxed));
        }
        BandSnapshot {
            small_deallocations,
            tagged_deallocations,
            total_deallocations: small_deallocations.wrapping_add(tagged_deallocations),
            small,
            tagged,
            total: AllocatorSnapshot {
                live_bytes: allocated.saturating_sub(deallocated),
                allocated_total_bytes: allocated,
                deallocated_total_bytes: deallocated,
                allocations: small.allocations.wrapping_add(tagged.allocations),
                reallocations: small.reallocations.wrapping_add(tagged.reallocations),
                failures: small.failures.wrapping_add(tagged.failures),
            },
        }
    }
}
