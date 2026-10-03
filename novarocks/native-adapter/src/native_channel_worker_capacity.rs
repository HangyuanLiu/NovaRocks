// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this
// file except in compliance with the License. You may obtain a copy at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Fixed logical Channel Worker positions, embedded in the original StockCore.
//! Tokens describe transitions only: the caller retains the exact issuer and
//! reports final physical owner exit. This kernel owns no task or funding graph.

use std::alloc::Layout;
use std::io;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

const FREE: usize = 0;
const INITIALIZING: usize = 1;
const LIVE: usize = 2;
const DETACHED: usize = 3;
const PHASE_MASK: usize = 3;
const WORKER_UNIT: usize = 4;

const VACANT: u8 = 0;
const CLAIMING: u8 = 1;
const OCCUPIED: u8 = 2;
const ATTACHED: u8 = 4;
const OWNER_EXITED: u8 = 8;
const FINAL_EXIT: u8 = OCCUPIED | OWNER_EXITED;

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}

/// An exact cache shell generation in its issuing original StockCore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CacheEpoch(u64);

/// An independent worker index/generation, never a reusable cache row index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NativeChannelWorkerToken {
    index: usize,
    generation: u64,
    cache_epoch: u64,
    cache_row_generation: u64,
}

struct Record {
    phase: AtomicU8,
    generation: AtomicU64,
    cache_epoch: AtomicU64,
    cache_row_generation: AtomicU64,
}
impl Record {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(VACANT),
            generation: AtomicU64::new(0),
            cache_epoch: AtomicU64::new(0),
            cache_row_generation: AtomicU64::new(0),
        }
    }
    fn matches(&self, token: NativeChannelWorkerToken) -> bool {
        self.generation.load(Ordering::Relaxed) == token.generation
            && self.cache_epoch.load(Ordering::Relaxed) == token.cache_epoch
            && self.cache_row_generation.load(Ordering::Relaxed) == token.cache_row_generation
    }
}

struct RecordGate<'a> {
    record: &'a Record,
    restore: u8,
    armed: bool,
}
impl Drop for RecordGate<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.record.phase.store(self.restore, Ordering::Release);
        }
    }
}

/// A fixed pure state kernel. Inline atomics and the Vec descriptor belong to
/// the enclosing StockCore Layout; only the typed Record array is additional.
pub(crate) struct NativeChannelWorkerCapacity {
    records: Vec<Record>,
    // Low bits are the singleton shell phase; each counted owner contributes 4.
    shell: AtomicUsize,
    epoch: AtomicU64,
}

impl NativeChannelWorkerCapacity {
    pub(crate) fn additional_backing_bytes(count: usize) -> io::Result<usize> {
        if count == 0 || count.checked_mul(WORKER_UNIT).is_none() {
            return Err(invalid());
        }
        Layout::array::<Record>(count)
            .map(|layout| layout.size())
            .map_err(|_| invalid())
    }

    pub(crate) fn new(count: usize) -> io::Result<Self> {
        Self::additional_backing_bytes(count)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(count)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        records.resize_with(count, Record::new);
        Ok(Self {
            records,
            shell: AtomicUsize::new(FREE),
            epoch: AtomicU64::new(0),
        })
    }

    pub(crate) fn claim_cache(&self) -> io::Result<CacheEpoch> {
        self.shell
            .compare_exchange(FREE, INITIALIZING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        let Some(epoch) = self.epoch.load(Ordering::Relaxed).checked_add(1) else {
            self.shell.store(FREE, Ordering::Release);
            return Err(invalid());
        };
        self.epoch.store(epoch, Ordering::Relaxed);
        self.shell.store(LIVE, Ordering::Release);
        Ok(CacheEpoch(epoch))
    }

    /// The cache backing has actually exited. Its original singleton remains
    /// occupied until every worker owner exits, including detached old rows.
    pub(crate) fn cache_backing_exited(&self, epoch: CacheEpoch) -> io::Result<()> {
        // Lock only the shell's phase, keeping concurrent final-owner decrements
        // enabled. Epoch validation under this gate prevents a stale Live-word
        // ABA from retiring a newly claimed cache. No callbacks run under it.
        loop {
            let word = self.shell.load(Ordering::Acquire);
            if word & PHASE_MASK != LIVE {
                return Err(invalid());
            }
            let locked = (word & !PHASE_MASK) | INITIALIZING;
            if self
                .shell
                .compare_exchange(word, locked, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            if epoch.0 == 0 || self.epoch.load(Ordering::Relaxed) != epoch.0 {
                self.finish_shell_phase(LIVE);
                return Err(invalid());
            }
            self.finish_shell_phase(DETACHED);
            return Ok(());
        }
    }

    // The caller owns the Initializing phase. Count decrements may race; using
    // CAS preserves them and never performs a delayed store over a new shell.
    fn finish_shell_phase(&self, phase: usize) {
        loop {
            let word = self.shell.load(Ordering::Acquire);
            debug_assert_eq!(word & PHASE_MASK, INITIALIZING);
            let count = word & !PHASE_MASK;
            let next = if phase == DETACHED && count == 0 {
                FREE
            } else {
                count | phase
            };
            if self
                .shell
                .compare_exchange(word, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    pub(crate) fn claim_worker(
        &self,
        epoch: CacheEpoch,
        cache_row_generation: u64,
    ) -> io::Result<NativeChannelWorkerToken> {
        if epoch.0 == 0 || cache_row_generation == 0 {
            return Err(invalid());
        }
        self.claim_worker_record(epoch, cache_row_generation)
    }

    /// Zero row identifies a transient constructor, rather than a fabricated
    /// cache row. The issuer retires its provisional attachment on handoff.
    pub(crate) fn claim_transient_worker(
        &self,
        epoch: CacheEpoch,
    ) -> io::Result<NativeChannelWorkerToken> {
        if epoch.0 == 0 {
            return Err(invalid());
        }
        self.claim_worker_record(epoch, 0)
    }

    fn claim_worker_record(
        &self,
        epoch: CacheEpoch,
        cache_row_generation: u64,
    ) -> io::Result<NativeChannelWorkerToken> {
        for (index, record) in self.records.iter().enumerate() {
            if record
                .phase
                .compare_exchange(VACANT, CLAIMING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let Some(generation) = record.generation.load(Ordering::Relaxed).checked_add(1) else {
                // This record stays permanently unavailable at generation MAX.
                record.phase.store(VACANT, Ordering::Release);
                continue;
            };
            record.generation.store(generation, Ordering::Relaxed);
            if let Err(error) = self.count_worker(epoch) {
                record.phase.store(VACANT, Ordering::Release);
                return Err(error);
            }
            record.cache_epoch.store(epoch.0, Ordering::Relaxed);
            record
                .cache_row_generation
                .store(cache_row_generation, Ordering::Relaxed);
            record.phase.store(OCCUPIED | ATTACHED, Ordering::Release);
            return Ok(NativeChannelWorkerToken {
                index,
                generation,
                cache_epoch: epoch.0,
                cache_row_generation,
            });
        }
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn count_worker(&self, epoch: CacheEpoch) -> io::Result<()> {
        loop {
            let word = self.shell.load(Ordering::Acquire);
            if word & PHASE_MASK != LIVE || self.epoch.load(Ordering::Acquire) != epoch.0 {
                return Err(io::ErrorKind::ConnectionAborted.into());
            }
            let next = word.checked_add(WORKER_UNIT).ok_or_else(invalid)?;
            if self
                .shell
                .compare_exchange(word, next, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            // The Live word could ABA across two cache epochs before the CAS.
            // Our count now prevents any further shell reuse. If necessary,
            // roll back exactly that count before publishing a worker record.
            if self.epoch.load(Ordering::Acquire) != epoch.0 {
                self.uncount_worker();
                return Err(io::ErrorKind::ConnectionAborted.into());
            }
            return Ok(());
        }
    }

    pub(crate) fn detach_worker(&self, token: NativeChannelWorkerToken) -> io::Result<()> {
        self.transition_worker(token, false)
    }

    /// Called only by the caller's final physical owner exit, never by future
    /// completion, timeout, channel eviction or JoinHandle observation.
    pub(crate) fn worker_owner_exited(&self, token: NativeChannelWorkerToken) -> io::Result<()> {
        self.transition_worker(token, true)
    }

    fn transition_worker(
        &self,
        token: NativeChannelWorkerToken,
        owner_exit: bool,
    ) -> io::Result<()> {
        let record = self.records.get(token.index).ok_or_else(invalid)?;
        loop {
            let phase = record.phase.load(Ordering::Acquire);
            if phase == CLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if phase & OCCUPIED == 0 || phase == FINAL_EXIT {
                return Err(invalid());
            }
            if record
                .phase
                .compare_exchange(phase, CLAIMING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let mut gate = RecordGate {
                record,
                restore: phase,
                armed: true,
            };
            // Validate under the short scalar gate: checking before CAS alone
            // permits an old token to mutate an equal phase in a replacement.
            let valid_event = if owner_exit {
                phase & OWNER_EXITED == 0
            } else {
                phase & ATTACHED != 0
            };
            if !record.matches(token) || !valid_event {
                return Err(invalid());
            }
            let next = if owner_exit {
                phase | OWNER_EXITED
            } else {
                phase & !ATTACHED
            };
            record.phase.store(next, Ordering::Release);
            gate.armed = false;
            if next == FINAL_EXIT {
                // Other events refuse FINAL_EXIT. Only this operation retires
                // its counted owner before publishing the record as vacant.
                // A new shell can be claimed after the last detached count
                // exits; this old operation performs no later shell mutation.
                self.uncount_worker();
                record
                    .phase
                    .compare_exchange(FINAL_EXIT, VACANT, Ordering::AcqRel, Ordering::Acquire)
                    .expect("exact final Channel Worker transition");
            }
            return Ok(());
        }
    }

    fn uncount_worker(&self) {
        loop {
            let word = self.shell.load(Ordering::Acquire);
            assert!(word >= WORKER_UNIT, "counted Channel Worker exits once");
            let reduced = word - WORKER_UNIT;
            let next = if reduced == DETACHED { FREE } else { reduced };
            if self
                .shell
                .compare_exchange(word, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// A snapshot including claims counted before their record publication.
    #[cfg(test)]
    pub(crate) fn occupied_workers(&self) -> usize {
        self.shell.load(Ordering::Acquire) / WORKER_UNIT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn detached_full_stock_stays_full_until_actual_owner_exits() {
        let capacity = NativeChannelWorkerCapacity::new(2).unwrap();
        let epoch = capacity.claim_cache().unwrap();
        let a = capacity.claim_worker(epoch, 1).unwrap();
        let b = capacity.claim_worker(epoch, 2).unwrap();
        capacity.detach_worker(a).unwrap();
        capacity.detach_worker(b).unwrap();
        assert_eq!(capacity.occupied_workers(), 2);
        assert_eq!(
            capacity.claim_worker(epoch, 3).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        capacity.worker_owner_exited(a).unwrap();
        let replacement = capacity.claim_worker(epoch, 3).unwrap();
        assert_eq!(replacement.index, a.index);
        assert!(replacement.generation > a.generation);
        capacity.detach_worker(replacement).unwrap();
        capacity.worker_owner_exited(replacement).unwrap();
        capacity.worker_owner_exited(b).unwrap();
        assert_eq!(capacity.occupied_workers(), 0);
    }

    #[test]
    fn both_exit_orders_require_detachment_and_physical_owner_exit() {
        let capacity = NativeChannelWorkerCapacity::new(1).unwrap();
        let epoch = capacity.claim_cache().unwrap();
        let first = capacity.claim_worker(epoch, 17).unwrap();
        capacity.worker_owner_exited(first).unwrap();
        assert_eq!(capacity.occupied_workers(), 1);
        assert!(capacity.claim_worker(epoch, 18).is_err());
        capacity.detach_worker(first).unwrap();
        let second = capacity.claim_worker(epoch, 18).unwrap();
        capacity.detach_worker(second).unwrap();
        assert_eq!(capacity.occupied_workers(), 1);
        capacity.worker_owner_exited(second).unwrap();
        assert_eq!(capacity.occupied_workers(), 0);
    }

    #[test]
    fn singleton_waits_for_detached_workers_and_cache_backing_exit() {
        let capacity = NativeChannelWorkerCapacity::new(2).unwrap();
        let epoch = capacity.claim_cache().unwrap();
        let token = capacity.claim_worker(epoch, 1).unwrap();
        capacity.detach_worker(token).unwrap();
        capacity.cache_backing_exited(epoch).unwrap();
        assert_eq!(
            capacity.claim_cache().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            capacity.claim_worker(epoch, 2).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        capacity.worker_owner_exited(token).unwrap();
        let next = capacity.claim_cache().unwrap();
        assert_ne!(next, epoch);
        let token = capacity.claim_worker(next, 1).unwrap();
        capacity.detach_worker(token).unwrap();
        capacity.worker_owner_exited(token).unwrap();
        assert!(capacity.claim_cache().is_err());
        capacity.cache_backing_exited(next).unwrap();
        assert!(capacity.claim_cache().is_ok());
    }

    #[test]
    fn stale_tokens_and_repeated_events_cannot_mutate_replacements() {
        let capacity = NativeChannelWorkerCapacity::new(1).unwrap();
        let epoch = capacity.claim_cache().unwrap();
        let old = capacity.claim_worker(epoch, 1).unwrap();
        capacity.detach_worker(old).unwrap();
        assert!(capacity.detach_worker(old).is_err());
        capacity.worker_owner_exited(old).unwrap();
        assert!(capacity.worker_owner_exited(old).is_err());
        let current = capacity.claim_worker(epoch, 2).unwrap();
        assert!(capacity.detach_worker(old).is_err());
        assert!(capacity.worker_owner_exited(old).is_err());
        assert_eq!(capacity.occupied_workers(), 1);
        capacity.worker_owner_exited(current).unwrap();
        assert!(capacity.worker_owner_exited(current).is_err());
        capacity.detach_worker(current).unwrap();
        capacity.cache_backing_exited(epoch).unwrap();
        let next = capacity.claim_cache().unwrap();
        assert!(capacity.cache_backing_exited(epoch).is_err());
        let current = capacity.claim_worker(next, 1).unwrap();
        assert!(capacity.detach_worker(old).is_err());
        capacity.detach_worker(current).unwrap();
        capacity.worker_owner_exited(current).unwrap();
    }

    #[test]
    fn zero_geometry_zero_row_and_exhausted_generations_refuse() {
        assert!(NativeChannelWorkerCapacity::new(0).is_err());
        assert!(NativeChannelWorkerCapacity::additional_backing_bytes(usize::MAX).is_err());
        let capacity = NativeChannelWorkerCapacity::new(1).unwrap();
        let epoch = capacity.claim_cache().unwrap();
        assert!(capacity.claim_worker(epoch, 0).is_err());
        capacity.records[0]
            .generation
            .store(u64::MAX, Ordering::Relaxed);
        assert_eq!(
            capacity.claim_worker(epoch, 1).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(capacity.occupied_workers(), 0);
        capacity.cache_backing_exited(epoch).unwrap();
        capacity.epoch.store(u64::MAX, Ordering::Relaxed);
        assert!(capacity.claim_cache().is_err());
        assert_eq!(capacity.shell.load(Ordering::Acquire), FREE);
    }

    #[test]
    fn an_attached_worker_survives_multiple_physical_connection_observations() {
        let capacity = NativeChannelWorkerCapacity::new(1).unwrap();
        let epoch = capacity.claim_cache().unwrap();
        let token = capacity.claim_worker(epoch, 9).unwrap();
        // Physical connection generations have no transition in this kernel.
        // Keeping one Channel Worker across reconnects does not claim anew.
        for _ in 0..8 {
            assert_eq!(capacity.occupied_workers(), 1);
            assert!(capacity.claim_worker(epoch, 9).is_err());
        }
        capacity.detach_worker(token).unwrap();
        assert_eq!(capacity.occupied_workers(), 1);
        capacity.worker_owner_exited(token).unwrap();
    }

    #[test]
    fn concurrent_shell_exit_and_last_owner_never_clobber_the_next_cache() {
        for _ in 0..32 {
            let capacity = Arc::new(NativeChannelWorkerCapacity::new(1).unwrap());
            let epoch = capacity.claim_cache().unwrap();
            let token = capacity.claim_worker(epoch, 1).unwrap();
            capacity.detach_worker(token).unwrap();
            let start = Arc::new(Barrier::new(3));
            let a = capacity.clone();
            let a_start = start.clone();
            let shell = std::thread::spawn(move || {
                a_start.wait();
                a.cache_backing_exited(epoch).unwrap();
            });
            let b = capacity.clone();
            let b_start = start.clone();
            let owner = std::thread::spawn(move || {
                b_start.wait();
                b.worker_owner_exited(token).unwrap();
            });
            start.wait();
            shell.join().unwrap();
            owner.join().unwrap();
            let next = capacity.claim_cache().unwrap();
            assert_ne!(next, epoch);
            assert!(capacity.cache_backing_exited(epoch).is_err());
            assert!(capacity.worker_owner_exited(token).is_err());
            assert!(capacity.claim_cache().is_err());
            capacity.cache_backing_exited(next).unwrap();
        }
    }

    #[test]
    fn concurrent_detach_and_owner_exit_return_exactly_one_position() {
        for _ in 0..32 {
            let capacity = Arc::new(NativeChannelWorkerCapacity::new(1).unwrap());
            let epoch = capacity.claim_cache().unwrap();
            let token = capacity.claim_worker(epoch, 1).unwrap();
            let a = capacity.clone();
            let b = capacity.clone();
            let detach = std::thread::spawn(move || a.detach_worker(token).unwrap());
            let owner = std::thread::spawn(move || b.worker_owner_exited(token).unwrap());
            detach.join().unwrap();
            owner.join().unwrap();
            assert_eq!(capacity.occupied_workers(), 0);
            let next = capacity.claim_worker(epoch, 2).unwrap();
            assert!(next.generation > token.generation);
            assert!(capacity.worker_owner_exited(token).is_err());
            capacity.detach_worker(next).unwrap();
            capacity.worker_owner_exited(next).unwrap();
        }
    }
}
