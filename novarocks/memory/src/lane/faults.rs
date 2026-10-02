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

//! Bounded diagnostics. Hooks never allocate, log, format or acquire a lock.
use std::sync::atomic::{AtomicU64, Ordering};

pub const FAULT_SHARDS: usize = 16;
#[repr(align(64))]
#[derive(Debug)]
struct FaultShard {
    orphan: AtomicU64,
    exhausted: AtomicU64,
    residual_growth: AtomicU64,
    reclaim_nonzero: AtomicU64,
    generation_exhausted: AtomicU64,
    scope_refused: AtomicU64,
    binding_failed: AtomicU64,
    pins_added: AtomicU64,
    pins_removed: AtomicU64,
}
impl FaultShard {
    const fn new() -> Self {
        Self {
            orphan: AtomicU64::new(0),
            exhausted: AtomicU64::new(0),
            residual_growth: AtomicU64::new(0),
            reclaim_nonzero: AtomicU64::new(0),
            generation_exhausted: AtomicU64::new(0),
            scope_refused: AtomicU64::new(0),
            binding_failed: AtomicU64::new(0),
            pins_added: AtomicU64::new(0),
            pins_removed: AtomicU64::new(0),
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultSnapshot {
    pub orphan_events: u64,
    pub record_exhaustions: u64,
    pub residual_growth_events: u64,
    pub reclaim_nonzero_events: u64,
    pub generation_exhaustions: u64,
    pub scope_refusals: u64,
    pub binding_failures: u64,
    /// Approximate cross-shard sample, exact after writers quiesce. It is not
    /// an instantaneous global bound during concurrent sampling.
    pub pinned_slots: u64,
}
#[derive(Debug)]
pub struct FaultCounters {
    shards: [FaultShard; FAULT_SHARDS],
}
impl Default for FaultCounters {
    fn default() -> Self {
        Self::new()
    }
}
impl FaultCounters {
    pub const fn new() -> Self {
        Self {
            shards: [const { FaultShard::new() }; FAULT_SHARDS],
        }
    }
    fn shard(&self) -> &FaultShard {
        &self.shards[stack_shard()]
    }
    pub(crate) fn orphan(&self) {
        self.shard().orphan.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn exhausted(&self) {
        self.shard().exhausted.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn residual_growth(&self) {
        self.shard().residual_growth.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn reclaim_nonzero(&self) {
        self.shard().reclaim_nonzero.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn generation_exhausted(&self) {
        self.shard()
            .generation_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn scope_refused(&self) {
        self.shard().scope_refused.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn binding_failed(&self) {
        self.shard().binding_failed.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn pin_added(&self) {
        self.shard().pins_added.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn pin_removed(&self) {
        self.shard().pins_removed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> FaultSnapshot {
        let mut s = FaultSnapshot::default();
        let mut added = 0u64;
        let mut removed = 0u64;
        for shard in &self.shards {
            s.orphan_events = s
                .orphan_events
                .wrapping_add(shard.orphan.load(Ordering::Relaxed));
            s.record_exhaustions = s
                .record_exhaustions
                .wrapping_add(shard.exhausted.load(Ordering::Relaxed));
            s.residual_growth_events = s
                .residual_growth_events
                .wrapping_add(shard.residual_growth.load(Ordering::Relaxed));
            s.reclaim_nonzero_events = s
                .reclaim_nonzero_events
                .wrapping_add(shard.reclaim_nonzero.load(Ordering::Relaxed));
            s.generation_exhaustions = s
                .generation_exhaustions
                .wrapping_add(shard.generation_exhausted.load(Ordering::Relaxed));
            s.scope_refusals = s
                .scope_refusals
                .wrapping_add(shard.scope_refused.load(Ordering::Relaxed));
            s.binding_failures = s
                .binding_failures
                .wrapping_add(shard.binding_failed.load(Ordering::Relaxed));
            added = added.wrapping_add(shard.pins_added.load(Ordering::Relaxed));
            removed = removed.wrapping_add(shard.pins_removed.load(Ordering::Relaxed));
        }
        // Independent shards and sampling instants can transiently invert totals.
        s.pinned_slots = added.saturating_sub(removed);
        s
    }
}
/// Shares the process counter's stack-region hash; this does not establish ownership.
#[inline]
pub(crate) fn stack_shard() -> usize {
    let probe = 0u8;
    let region = (std::ptr::from_ref(&probe).addr() >> 16) as u64;
    ((region.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 60) as usize) & (FAULT_SHARDS - 1)
}
