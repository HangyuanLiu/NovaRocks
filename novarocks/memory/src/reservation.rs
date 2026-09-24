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

//! A dedicated leaf for already allocated, governed retention.
//!
//! The leaf's counters are the account tree's own counters. F is the only
//! arbiter for fast growth and idle return. C changes under `slow`; the
//! version brackets those changes for leaf snapshots. A successful slow
//! growth commits its own delta directly to L before surplus F is published.

#[cfg(all(test, loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(all(test, loom))]
use loom::sync::{Arc, Mutex, MutexGuard};
use std::sync::TryLockError;
#[cfg(not(all(test, loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(all(test, loom)))]
use std::sync::{Arc, Mutex, MutexGuard};

use crate::account::{AccountHandle, ShrinkOutcome};
use crate::error::CapacityError;
use crate::ids::{AccountId, AccountKind, ExternalRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservationSnapshot {
    pub account: AccountId,
    pub live_bytes: u64,
    pub free_bytes: u64,
    pub committed_bytes: u64,
    pub closed: bool,
}

#[derive(Debug)]
struct ReservationState {
    account: AccountHandle,
    slow: Mutex<()>,
    version: AtomicU64,
    closed: AtomicBool,
    pending_return: AtomicBool,
    target: AtomicU64,
}

/// One private accounting leaf under a stable query or service sponsor.
/// Clones share the exact same leaf and can outlive the domain that created it.
#[derive(Debug, Clone)]
pub struct Reservation {
    state: Arc<ReservationState>,
}

struct VersionWrite<'a>(&'a AtomicU64);

impl VersionWrite<'_> {
    fn begin(version: &AtomicU64) -> VersionWrite<'_> {
        let previous = version.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(previous & 1, 0, "reservation writer must hold slow lock");
        VersionWrite(version)
    }
}

impl Drop for VersionWrite<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

impl Reservation {
    pub fn new(sponsor: &AccountHandle, external: ExternalRef) -> Result<Self, CapacityError> {
        let account = sponsor.create_child(AccountKind::Owner, external)?;
        let target = account.account().reservation_target();
        Ok(Self {
            state: Arc::new(ReservationState {
                account,
                slow: Mutex::new(()),
                version: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                pending_return: AtomicBool::new(false),
                target: AtomicU64::new(target),
            }),
        })
    }

    pub fn account_id(&self) -> AccountId {
        self.state.account.id()
    }

    pub fn try_grow(&self, bytes: u64) -> Result<(), CapacityError> {
        if bytes == 0 {
            return Ok(());
        }
        if self.state.closed.load(Ordering::Acquire) {
            return Err(self.cancelled());
        }
        let account = self.state.account.account();
        if account.reservation_take_free(bytes) {
            if self.state.closed.load(Ordering::Acquire) {
                account.reservation_restore_free(bytes);
                self.trim();
                return Err(self.cancelled());
            }
            account.reservation_commit_live(bytes);
            return Ok(());
        }
        let _slow = self.lock_slow();
        if self.state.closed.load(Ordering::Acquire) {
            return Err(self.cancelled());
        }
        if account.reservation_take_free(bytes) {
            account.reservation_commit_live(bytes);
            return Ok(());
        }
        let result = {
            let _version = VersionWrite::begin(&self.state.version);
            account.reservation_grow_slow(bytes)
        };
        self.state
            .target
            .store(account.reservation_target(), Ordering::Release);
        if self.state.pending_return.swap(false, Ordering::AcqRel) {
            self.return_idle_locked();
        }
        result
    }

    /// Retires live retention without waiting on the slow lock or a parent.
    /// If a return is already in progress, that return or a later shrink/trim
    /// will reconcile the newly available slack.
    pub fn shrink(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let account = self.state.account.account();
        account.reservation_shrink_live(bytes);
        let target = self.state.target.load(Ordering::Acquire);
        let idle = account.local_free_bytes();
        if self.state.closed.load(Ordering::Acquire) || idle > target.saturating_mul(2) {
            match self.state.slow.try_lock() {
                Ok(_slow) => self.return_idle_locked(),
                Err(TryLockError::Poisoned(_slow)) => self.return_idle_locked(),
                Err(TryLockError::WouldBlock) => {
                    self.state.pending_return.store(true, Ordering::Release);
                }
            }
        }
    }

    /// Returns all non-floor idle commitment, regardless of the adaptive
    /// target. This does not retire a live holder or revoke issued capacity.
    pub fn trim(&self) -> ShrinkOutcome {
        let _slow = self.lock_slow();
        let _version = VersionWrite::begin(&self.state.version);
        self.state.account.account().reservation_trim(u64::MAX)
    }

    /// Closes new retention while existing holders remain billable.
    pub fn close(&self) -> ShrinkOutcome {
        self.state.closed.store(true, Ordering::Release);
        let _slow = self.lock_slow();
        self.state.account.account().reservation_close();
        let _version = VersionWrite::begin(&self.state.version);
        self.state.account.account().reservation_trim(u64::MAX)
    }

    pub fn revoke(&self) -> ShrinkOutcome {
        self.close()
    }

    pub fn snapshot(&self) -> ReservationSnapshot {
        let account = self.state.account.account();
        loop {
            let before = self.state.version.load(Ordering::Acquire);
            if before & 1 != 0 {
                #[cfg(all(test, loom))]
                loom::thread::yield_now();
                #[cfg(not(all(test, loom)))]
                std::hint::spin_loop();
                continue;
            }
            let committed = account.committed_bytes();
            let live = account.own_live_bytes();
            let after = self.state.version.load(Ordering::Acquire);
            if before == after && committed >= live {
                return ReservationSnapshot {
                    account: account.id(),
                    live_bytes: live,
                    free_bytes: committed - live,
                    committed_bytes: committed,
                    closed: self.state.closed.load(Ordering::Acquire),
                };
            }
        }
    }

    fn return_idle_locked(&self) {
        let account = self.state.account.account();
        let closed = self.state.closed.load(Ordering::Acquire);
        let idle = account.local_free_bytes();
        let target = if closed {
            0
        } else {
            account.reservation_target()
        };
        if !closed && idle <= target.saturating_mul(2) {
            return;
        }
        let _version = VersionWrite::begin(&self.state.version);
        let outcome = account.reservation_trim(idle.saturating_sub(target));
        if outcome.reclaimed_bytes > 0 {
            account.reservation_reset_demand();
            self.state
                .target
                .store(account.reservation_target(), Ordering::Release);
        }
    }

    fn lock_slow(&self) -> MutexGuard<'_, ()> {
        self.state
            .slow
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cancelled(&self) -> CapacityError {
        CapacityError::Cancelled {
            scope: self.state.account.id(),
        }
    }
}

impl Drop for ReservationState {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.account.account().reservation_close();
        self.account.account().reservation_trim(u64::MAX);
    }
}
