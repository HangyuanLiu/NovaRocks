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

//! Feature-only child owner of the listener's same original disconnect tasks.
//! Fixed reservations include completed handles until their actual join is observed.

use crate::ClientDisconnectWatcher;
use crate::connection_registry::RegisteredConnectionLifetime;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use tokio::task::{JoinError, JoinHandle};

const WATCHER_SLOTS: usize = 544;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WatcherFacts {
    pub reserved: usize,
    pub joined: u64,
    pub succeeded: u64,
    pub expected_cancelled: u64,
    pub panicked: u64,
    pub unexpected_cancelled: u64,
    pub capacity_failure: bool,
    pub generation_overflow: bool,
    pub counter_overflow: bool,
    pub poisoned: bool,
    pub missing_original_task: bool,
}
impl WatcherFacts {
    pub(crate) fn failed(self) -> bool {
        self.panicked != 0
            || self.unexpected_cancelled != 0
            || self.capacity_failure
            || self.generation_overflow
            || self.counter_overflow
            || self.poisoned
            || self.missing_original_task
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WatcherExitKind {
    Succeeded,
    ExpectedCancelled,
    Panicked,
    UnexpectedCancelled,
}

#[derive(Default)]
struct Slot {
    generation: u64,
    occupied: bool,
    handle: Option<JoinHandle<()>>,
    // This alias keeps the original connection admission position through join.
    owner: Option<Arc<RegisteredConnectionLifetime>>,
    abort_requested: bool,
}
struct State {
    slots: [Slot; WATCHER_SLOTS],
    facts: WatcherFacts,
    cursor: usize,
    waiter: Option<Waker>,
    first_failure: Option<JoinError>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| Slot::default()),
            facts: WatcherFacts::default(),
            cursor: 0,
            waiter: None,
            first_failure: None,
        }
    }
}
#[derive(Default)]
pub(crate) struct OriginalWatcherOwner {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}
impl OriginalWatcherOwner {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Installation must not drop an already spawned original handle even if
        // an earlier scope panic poisoned this fixed owner. Poison stays fatal.
        match self.state.lock() {
            Ok(state) => state,
            Err(poison) => {
                let mut state = poison.into_inner();
                state.facts.poisoned = true;
                state
            }
        }
    }
    fn wake(&self, waiter: Option<Waker>) {
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        self.changed.notify_waiters();
    }
    pub(crate) fn reserve(
        self: &Arc<Self>,
        owner: Arc<RegisteredConnectionLifetime>,
    ) -> io::Result<WatcherPermit> {
        let mut state = self.lock();
        if state.facts.failed() {
            return Err(io::Error::other("original watcher owner already failed"));
        }
        let Some(index) = state.slots.iter().position(|slot| !slot.occupied) else {
            state.facts.capacity_failure = true;
            let waiter = state.waiter.take();
            drop(state);
            self.wake(waiter);
            return Err(io::Error::other(
                "fixed original watcher positions exhausted before spawn",
            ));
        };
        let Some(generation) = state.slots[index].generation.checked_add(1) else {
            state.facts.generation_overflow = true;
            let waiter = state.waiter.take();
            drop(state);
            self.wake(waiter);
            return Err(io::Error::other(
                "original watcher slot generation exhausted before spawn",
            ));
        };
        let slot = &mut state.slots[index];
        slot.generation = generation;
        slot.occupied = true;
        slot.owner = Some(owner);
        slot.abort_requested = false;
        state.facts.reserved += 1; // Fixed 544 slots; cannot overflow usize.
        Ok(WatcherPermit {
            owner: Arc::clone(self),
            index,
            generation,
            installed: false,
        })
    }
    fn release_reservation(&self, index: usize, generation: u64) {
        let mut state = self.lock();
        let slot = &mut state.slots[index];
        if slot.generation != generation || !slot.occupied || slot.handle.is_some() {
            return;
        }
        let owner = slot.owner.take();
        slot.occupied = false;
        state.facts.reserved -= 1;
        let waiter = state.waiter.take();
        drop(state);
        drop(owner);
        self.wake(waiter);
    }
    fn abort_exact(&self, index: usize, generation: u64) {
        let mut state = self.lock();
        let slot = &mut state.slots[index];
        if slot.generation != generation || !slot.occupied {
            return;
        }
        slot.abort_requested = true;
        if let Some(handle) = &slot.handle {
            handle.abort();
        }
        let waiter = state.waiter.take();
        drop(state);
        self.wake(waiter);
    }
    pub(crate) fn abort_remaining(&self) {
        let mut state = self.lock();
        for slot in &mut state.slots {
            if slot.occupied {
                slot.abort_requested = true;
                if let Some(handle) = &slot.handle {
                    handle.abort();
                }
            }
        }
        let waiter = state.waiter.take();
        drop(state);
        self.wake(waiter);
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.lock().facts.reserved == 0
    }
    pub(crate) fn snapshot(&self) -> WatcherFacts {
        self.lock().facts
    }
    pub(crate) fn take_failure_after_join(&self) -> Option<JoinError> {
        self.lock().first_failure.take()
    }
    pub(crate) async fn wait_for_failure(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.snapshot().failed() {
                return;
            }
            changed.await;
        }
    }
    /// Borrow the same handles; no wrapper task or replacement JoinHandle exists.
    /// Return at most one actual join per poll after at most 544 slot checks.
    pub(crate) fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<WatcherExitKind>> {
        let mut state = self.lock();
        state.waiter = Some(cx.waker().clone());
        if state.facts.reserved == 0 {
            return Poll::Ready(None);
        }
        for turn in 0..WATCHER_SLOTS {
            let index = (state.cursor + turn) % WATCHER_SLOTS;
            let slot = &mut state.slots[index];
            let Some(handle) = slot.handle.as_mut() else {
                continue;
            };
            let Poll::Ready(result) = Pin::new(handle).poll(cx) else {
                continue;
            };
            let expected_abort = slot.abort_requested;
            // Ready was obtained from the same original handle before releasing it.
            let joined_handle = slot.handle.take();
            let connection_owner = slot.owner.take();
            slot.occupied = false;
            state.facts.reserved -= 1;
            state.cursor = (index + 1) % WATCHER_SLOTS;
            fn increment(value: &mut u64) -> bool {
                match value.checked_add(1) {
                    Some(next) => {
                        *value = next;
                        false
                    }
                    None => true,
                }
            }
            let mut overflow = increment(&mut state.facts.joined);
            let kind = match result {
                Ok(()) => {
                    overflow |= increment(&mut state.facts.succeeded);
                    WatcherExitKind::Succeeded
                }
                Err(error) if error.is_cancelled() && expected_abort => {
                    overflow |= increment(&mut state.facts.expected_cancelled);
                    WatcherExitKind::ExpectedCancelled
                }
                Err(error) => {
                    let kind = if error.is_panic() {
                        overflow |= increment(&mut state.facts.panicked);
                        WatcherExitKind::Panicked
                    } else {
                        overflow |= increment(&mut state.facts.unexpected_cancelled);
                        WatcherExitKind::UnexpectedCancelled
                    };
                    if state.first_failure.is_none() {
                        state.first_failure = Some(error);
                    }
                    kind
                }
            };
            state.facts.counter_overflow |= overflow;
            if state.facts.failed() {
                for slot in &mut state.slots {
                    if slot.occupied {
                        slot.abort_requested = true;
                        if let Some(handle) = &slot.handle {
                            handle.abort();
                        }
                    }
                }
            }
            let waiter = state.waiter.take();
            drop(state);
            drop(joined_handle);
            drop(connection_owner);
            self.wake(waiter);
            return Poll::Ready(Some(kind));
        }
        Poll::Pending
    }
    pub(crate) async fn next(&self) -> Option<WatcherExitKind> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }
}

pub(crate) struct WatcherPermit {
    owner: Arc<OriginalWatcherOwner>,
    index: usize,
    generation: u64,
    installed: bool,
}
impl WatcherPermit {
    /// Caller reserves before its one existing spawn and immediately attaches,
    /// with no await or other fallible operation between spawn and this move.
    pub(crate) fn attach(
        mut self,
        mut watcher: ClientDisconnectWatcher,
    ) -> ClientDisconnectWatcher {
        let Some(handle) = watcher.take_original_handle_for_fixture() else {
            let mut state = self.owner.lock();
            state.facts.missing_original_task = true;
            let waiter = state.waiter.take();
            drop(state);
            self.owner.wake(waiter);
            return watcher;
        };
        let mut state = self.owner.lock();
        let failed = state.facts.failed();
        let slot = &mut state.slots[self.index];
        // Only this non-cloneable permit can release or install this reservation.
        // No allocation, user callback, fallible operation or await follows the move.
        slot.handle = Some(handle);
        if failed {
            slot.abort_requested = true;
        }
        if slot.abort_requested {
            slot.handle.as_ref().unwrap().abort();
        }
        self.installed = true;
        let waiter = state.waiter.take();
        drop(state);
        watcher.install_fixture_abort_guard(WatcherAbortGuard {
            owner: Arc::downgrade(&self.owner),
            index: self.index,
            generation: self.generation,
        });
        self.owner.wake(waiter);
        watcher
    }
}
impl Drop for WatcherPermit {
    fn drop(&mut self) {
        if !self.installed {
            self.owner.release_reservation(self.index, self.generation);
        }
    }
}
pub(crate) struct WatcherAbortGuard {
    owner: Weak<OriginalWatcherOwner>,
    index: usize,
    generation: u64,
}
impl Drop for WatcherAbortGuard {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.abort_exact(self.index, self.generation);
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{MysqlClientConnectionRegistry, spawn_disconnect_watcher};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn actual_tcp_callback_panic_is_retained_after_same_original_join() {
        let registry = MysqlClientConnectionRegistry::new();
        let registration = registry.register().unwrap();
        let owner = Arc::new(OriginalWatcherOwner::default());
        let permit = owner.reserve(registration.retain_owner()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let watcher = permit.attach(spawn_disconnect_watcher(&server, || {
            panic!("actual watcher callback panic")
        }));
        drop(client);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), owner.next())
                .await
                .unwrap(),
            Some(WatcherExitKind::Panicked)
        );
        assert!(owner.is_empty());
        let error = owner.take_failure_after_join().unwrap();
        assert!(error.is_panic());
        assert_eq!(
            *error.into_panic().downcast::<&str>().unwrap(),
            "actual watcher callback panic"
        );
        drop(watcher);
        drop(server);
        drop(registration);
        registry.wait_drained().await;
    }

    #[tokio::test]
    async fn full_positions_include_completed_unreaped_and_stale_guard_cannot_abort_reuse() {
        let registry = MysqlClientConnectionRegistry::new();
        let registration = registry.register().unwrap();
        let owner = Arc::new(OriginalWatcherOwner::default());
        let mut guards = Vec::with_capacity(WATCHER_SLOTS); // Fixed test workload only.
        for _ in 0..WATCHER_SLOTS {
            let permit = owner.reserve(registration.retain_owner()).unwrap();
            let handle = tokio::spawn(async {});
            guards
                .push(permit.attach(
                    ClientDisconnectWatcher::from_original_handle_for_fixture_test(handle),
                ));
        }
        tokio::task::yield_now().await;
        assert!(owner.reserve(registration.retain_owner()).is_err());
        assert_eq!(owner.snapshot().reserved, WATCHER_SLOTS);
        while owner.next().await.is_some() {}
        assert!(owner.is_empty());
        // Capacity refusal is sticky, so reuse is tested on a separate owner.
        let reuse = Arc::new(OriginalWatcherOwner::default());
        let old = reuse.reserve(registration.retain_owner()).unwrap().attach(
            ClientDisconnectWatcher::from_original_handle_for_fixture_test(tokio::spawn(async {})),
        );
        assert_eq!(reuse.next().await, Some(WatcherExitKind::Succeeded));
        let permit = reuse.reserve(registration.retain_owner()).unwrap();
        let (complete, receive) = tokio::sync::oneshot::channel::<()>();
        let current = permit.attach(
            ClientDisconnectWatcher::from_original_handle_for_fixture_test(tokio::spawn(
                async move {
                    let _ = receive.await;
                },
            )),
        );
        drop(old);
        complete.send(()).unwrap();
        assert_eq!(reuse.next().await, Some(WatcherExitKind::Succeeded));
        drop(current);
        drop(guards);
    }

    #[tokio::test]
    async fn permit_unwind_before_install_frees_only_its_reservation_and_abort_joins_original() {
        let registry = MysqlClientConnectionRegistry::new();
        let registration = registry.register().unwrap();
        let owner = Arc::new(OriginalWatcherOwner::default());
        let permit = owner.reserve(registration.retain_owner()).unwrap();
        drop(permit);
        assert!(owner.is_empty());
        let watcher = owner.reserve(registration.retain_owner()).unwrap().attach(
            ClientDisconnectWatcher::from_original_handle_for_fixture_test(tokio::spawn(
                std::future::pending(),
            )),
        );
        drop(watcher);
        assert_eq!(owner.next().await, Some(WatcherExitKind::ExpectedCancelled));
        assert_eq!(owner.snapshot().expected_cancelled, 1);
        assert!(!owner.snapshot().failed());
        assert!(owner.is_empty());
    }
}
