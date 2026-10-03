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

//! Original fixed channel-cache metadata and bounded single-flight waiters.
//! Channels, executor tasks and their physical connection families keep their
//! separate original owners. StockCore never retains this cache or a Channel.

use std::alloc::Layout;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use tonic::transport::Channel;

use crate::native_channel_identity::InlineNativeChannelIdentity;
use crate::native_channel_worker_capacity::{CacheEpoch, NativeChannelWorkerToken};
use crate::native_client::NativeChannelKey;
use crate::native_transport_capacity::NativeTransportCapacityFactory;

const WAITERS: usize =
    NativeResultSupportGeometry::V1.transport_tonic_pending_per_connection as usize;

pub(crate) fn entry_positions() -> io::Result<usize> {
    let g = NativeResultSupportGeometry::V1;
    let backends = usize::try_from(g.transport_maximum_live_backends).map_err(|_| invalid())?;
    let frontends = usize::try_from(g.transport_authenticated_live_frontends_per_backend)
        .map_err(|_| invalid())?;
    let per_backend = g
        .transport_exchange_connections_per_peer
        .checked_add(g.transport_exchange_connecting_positions_per_peer)
        .and_then(|n| n.checked_add(g.transport_exchange_closing_positions_per_peer))
        .and_then(|n| n.checked_add(g.transport_runtime_filter_connections_per_peer))
        .and_then(|n| n.checked_add(g.transport_runtime_filter_connecting_positions_per_peer))
        .and_then(|n| n.checked_add(g.transport_runtime_filter_closing_positions_per_peer))
        .ok_or_else(invalid)?;
    let per_frontend = 1u64
        .checked_add(g.transport_connecting_positions_per_lane)
        .and_then(|n| n.checked_add(g.transport_closing_positions_per_lane))
        .ok_or_else(invalid)?;
    backends
        .checked_mul(usize::try_from(per_backend).map_err(|_| invalid())?)
        .and_then(|n| {
            frontends
                .checked_mul(usize::try_from(per_frontend).ok()?)
                .and_then(|fe| n.checked_add(fe))
        })
        .ok_or_else(invalid)
}

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}

struct Waiter {
    generation: u64,
    occupied: bool,
    waker: Option<Waker>,
}

enum Phase {
    Vacant,
    Connecting,
    Ready(Channel),
    Failed,
}

struct Entry {
    identity: Option<InlineNativeChannelIdentity>,
    generation: u64,
    phase: Phase,
    waiters: [Waiter; WAITERS],
    // Channel and waiter owners exit before the row's attachment is retired.
    worker: Option<EntryWorker>,
}

impl Entry {
    fn vacant() -> Self {
        Self {
            identity: None,
            generation: 0,
            phase: Phase::Vacant,
            waiters: std::array::from_fn(|_| Waiter {
                generation: 0,
                occupied: false,
                waker: None,
            }),
            worker: None,
        }
    }
    fn reusable(&self) -> bool {
        matches!(self.phase, Phase::Vacant | Phase::Failed)
            && self.generation != u64::MAX
            && self.waiters.iter().all(|w| !w.occupied)
    }
    fn notifications(&mut self) -> [Option<Waker>; WAITERS] {
        std::array::from_fn(|index| self.waiters[index].waker.take())
    }
}

enum State {
    Legacy(HashMap<NativeChannelKey, Channel>),
    Bounded(Vec<Entry>),
}

struct EntryWorker {
    factory: NativeTransportCapacityFactory,
    token: NativeChannelWorkerToken,
}
impl Drop for EntryWorker {
    fn drop(&mut self) {
        self.factory.detach_channel_worker(self.token);
    }
}

struct CacheExit {
    factory: NativeTransportCapacityFactory,
    epoch: CacheEpoch,
}
impl Drop for CacheExit {
    fn drop(&mut self) {
        self.factory.release_channel_cache(self.epoch);
    }
}

struct Core {
    state: Mutex<State>,
    eviction_cursor: AtomicUsize,
    // Mutex/PAL, fixed Vec, every Channel and waiter exit before this owner.
    original: Option<CacheExit>,
}

pub(crate) struct NativeChannelCache {
    core: Option<Arc<Core>>,
}

impl Clone for NativeChannelCache {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core())),
        }
    }
}
impl Drop for NativeChannelCache {
    fn drop(&mut self) {
        drop(Arc::into_inner(
            self.core.take().expect("live channel-cache handle"),
        ));
    }
}

impl NativeChannelCache {
    fn core(&self) -> &Arc<Core> {
        self.core.as_ref().expect("live channel-cache handle")
    }

    pub(crate) fn allocation_capacity_bound() -> io::Result<usize> {
        let arc = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core>())
            .map_err(|_| invalid())?
            .0
            .pad_to_align()
            .size();
        let entries = Layout::array::<Entry>(entry_positions()?)
            .map_err(|_| invalid())?
            .size();
        // Pinned std uses one lazy pthread Box on Darwin, and inline futex
        // state on the supported Linux targets. Construction prewarms before
        // publication, so concurrent first lockers cannot allocate extra Boxes.
        #[cfg(target_os = "macos")]
        let pal = Layout::new::<libc::pthread_mutex_t>().size();
        #[cfg(all(target_os = "linux", target_has_atomic = "32"))]
        let pal = 0;
        #[cfg(not(any(
            target_os = "macos",
            all(target_os = "linux", target_has_atomic = "32")
        )))]
        return Err(io::ErrorKind::Unsupported.into());
        #[cfg(any(
            target_os = "macos",
            all(target_os = "linux", target_has_atomic = "32")
        ))]
        arc.checked_add(entries)
            .and_then(|n| n.checked_add(pal))
            .ok_or_else(invalid)
    }

    pub(crate) fn legacy() -> Self {
        Self {
            core: Some(Arc::new(Core {
                state: Mutex::new(State::Legacy(HashMap::new())),
                eviction_cursor: AtomicUsize::new(0),
                original: None,
            })),
        }
    }

    pub(crate) fn bounded(factory: NativeTransportCapacityFactory) -> io::Result<Self> {
        let epoch = factory.claim_channel_cache()?;
        let original = CacheExit { factory, epoch };
        let count = entry_positions()?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        entries.resize_with(count, Entry::vacant);
        let core = Core {
            state: Mutex::new(State::Bounded(entries)),
            eviction_cursor: AtomicUsize::new(0),
            original: Some(original),
        };
        drop(core.state.lock().expect("unpublished channel-cache mutex"));
        Ok(Self {
            core: Some(Arc::new(core)),
        })
    }

    pub(crate) fn is_bounded(&self) -> bool {
        self.core().original.is_some()
    }

    pub(crate) fn transient_original_channel_worker(
        &self,
    ) -> io::Result<Option<tonic::transport::OriginalChannelWorker>> {
        self.core()
            .original
            .as_ref()
            .map(|original| {
                original
                    .factory
                    .try_transient_channel_worker(original.epoch)
            })
            .transpose()
    }

    #[cfg(test)]
    pub(crate) fn same_cache(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.core(), other.core())
    }

    pub(crate) fn legacy_get(&self, key: &NativeChannelKey) -> Option<Channel> {
        match &*self.core().state.lock().expect("channel-cache lock") {
            State::Legacy(entries) => entries.get(key).cloned(),
            State::Bounded(_) => None,
        }
    }

    pub(crate) fn legacy_insert(&self, key: NativeChannelKey, channel: Channel) {
        let retired = match &mut *self.core().state.lock().expect("channel-cache lock") {
            State::Legacy(entries) => entries.insert(key, channel),
            State::Bounded(_) => panic!("bounded cache requires its exact election token"),
        };
        drop(retired);
    }

    #[cfg(test)]
    pub(crate) fn remove(&self, key: &NativeChannelKey) -> Option<Channel> {
        let mut retired_worker = None;
        let mut state = self.core().state.lock().expect("channel-cache lock");
        let channel = match &mut *state {
            State::Legacy(entries) => entries.remove(key),
            State::Bounded(entries) => {
                let identity = key.inline_identity().ok()?;
                let entry = entries.iter_mut().find(|e| e.identity == Some(identity))?;
                if !matches!(entry.phase, Phase::Ready(_)) {
                    return None;
                }
                retired_worker = entry.worker.take();
                match std::mem::replace(&mut entry.phase, Phase::Vacant) {
                    Phase::Ready(channel) => Some(channel),
                    _ => unreachable!(),
                }
            }
        };
        drop(state);
        drop(retired_worker);
        channel
    }

    pub(crate) fn acquire(&self, identity: InlineNativeChannelIdentity) -> Acquire {
        Acquire {
            cache: self.clone(),
            identity,
            registration: None,
        }
    }
}

#[derive(Clone, Copy)]
struct Registration {
    entry: usize,
    entry_generation: u64,
    waiter: usize,
    waiter_generation: u64,
}

pub(crate) enum Election {
    Ready(Channel),
    Leader(Leader),
}

pub(crate) struct Acquire {
    cache: NativeChannelCache,
    identity: InlineNativeChannelIdentity,
    registration: Option<Registration>,
}

impl Acquire {
    fn unregister(&mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };
        let retired = {
            let mut state = self.cache.core().state.lock().expect("channel-cache lock");
            let State::Bounded(entries) = &mut *state else {
                return;
            };
            let waiter = &mut entries[registration.entry].waiters[registration.waiter];
            if waiter.occupied && waiter.generation == registration.waiter_generation {
                waiter.occupied = false;
                waiter.waker.take()
            } else {
                None
            }
        };
        drop(retired);
    }
}
impl Drop for Acquire {
    fn drop(&mut self) {
        self.unregister();
    }
}

impl Future for Acquire {
    type Output = io::Result<Election>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        // Waker clone/drop and wake are outside the cache mutex, including
        // arbitrary reentrant RawWaker implementations in ownership probes.
        let mut incoming = Some(cx.waker().clone());
        let mut retired = None;
        let mut retired_channel = None;
        let mut retired_worker = None;
        let outcome = {
            let mut state = me.cache.core().state.lock().expect("channel-cache lock");
            let State::Bounded(entries) = &mut *state else {
                return Poll::Ready(Err(invalid()));
            };
            if let Some(registration) = me.registration {
                let entry = &mut entries[registration.entry];
                if entry.generation != registration.entry_generation {
                    Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()))
                } else {
                    match &entry.phase {
                        Phase::Ready(channel) => Poll::Ready(Ok(Election::Ready(channel.clone()))),
                        Phase::Connecting => {
                            retired = std::mem::replace(
                                &mut entry.waiters[registration.waiter].waker,
                                incoming.take(),
                            );
                            Poll::Pending
                        }
                        Phase::Vacant | Phase::Failed => {
                            Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()))
                        }
                    }
                }
            } else if let Some(index) = entries.iter().position(|e| {
                e.identity == Some(me.identity)
                    && (matches!(e.phase, Phase::Connecting | Phase::Ready(_))
                        || e.waiters.iter().any(|w| w.occupied))
            }) {
                let entry = &mut entries[index];
                match &entry.phase {
                    Phase::Ready(channel) => Poll::Ready(Ok(Election::Ready(channel.clone()))),
                    Phase::Connecting => {
                        match entry
                            .waiters
                            .iter()
                            .position(|w| !w.occupied && w.generation != u64::MAX)
                        {
                            Some(waiter_index) => {
                                let waiter = &mut entry.waiters[waiter_index];
                                waiter.generation += 1;
                                waiter.occupied = true;
                                waiter.waker = incoming.take();
                                me.registration = Some(Registration {
                                    entry: index,
                                    entry_generation: entry.generation,
                                    waiter: waiter_index,
                                    waiter_generation: waiter.generation,
                                });
                                Poll::Pending
                            }
                            None => Poll::Ready(Err(io::ErrorKind::WouldBlock.into())),
                        }
                    }
                    Phase::Vacant | Phase::Failed => {
                        Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()))
                    }
                }
            } else {
                let vacancy = entries.iter().position(Entry::reusable);
                let index = vacancy.or_else(|| {
                    let start = me.cache.core().eviction_cursor.load(Ordering::Relaxed);
                    (0..entries.len())
                        .map(|offset| (start + offset) % entries.len())
                        .find(|&index| {
                            let entry = &entries[index];
                            matches!(entry.phase, Phase::Ready(_))
                                && entry.generation != u64::MAX
                                && entry.waiters.iter().all(|w| !w.occupied)
                        })
                });
                match index {
                    Some(index) => {
                        let next_cursor = (index + 1) % entries.len();
                        let entry = &mut entries[index];
                        // Evict only the cache alias. Actual IO, escaped Channels
                        // and their original physical positions remain owned.
                        if let Phase::Ready(channel) =
                            std::mem::replace(&mut entry.phase, Phase::Connecting)
                        {
                            retired_channel = Some(channel);
                        }
                        retired_worker = entry.worker.take();
                        entry.generation += 1;
                        entry.identity = Some(me.identity);
                        me.cache
                            .core()
                            .eviction_cursor
                            .store(next_cursor, Ordering::Relaxed);
                        Poll::Ready(Ok(Election::Leader(Leader {
                            cache: me.cache.clone(),
                            entry: index,
                            generation: entry.generation,
                            completed: false,
                        })))
                    }
                    None => Poll::Ready(Err(io::ErrorKind::WouldBlock.into())),
                }
            }
        };
        drop(retired);
        drop(incoming);
        drop(retired_channel);
        drop(retired_worker);
        if outcome.is_ready() {
            me.unregister();
        }
        outcome
    }
}

#[cfg(test)]
#[path = "native_channel_cache_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "native_channel_cache_worker_tests.rs"]
mod worker_tests;

pub(crate) struct Leader {
    cache: NativeChannelCache,
    entry: usize,
    generation: u64,
    completed: bool,
}

impl Leader {
    /// Claim a logical Worker before connector construction. Reconnects on the
    /// resulting Channel keep this position rather than claiming new workers.
    pub(crate) fn original_channel_worker(
        &mut self,
    ) -> io::Result<tonic::transport::OriginalChannelWorker> {
        {
            let state = self.cache.core().state.lock().expect("channel-cache lock");
            let State::Bounded(entries) = &*state else {
                return Err(invalid());
            };
            let entry = &entries[self.entry];
            if entry.generation != self.generation
                || !matches!(entry.phase, Phase::Connecting)
                || entry.worker.is_some()
            {
                return Err(invalid());
            }
        }
        let original = self.cache.core().original.as_ref().ok_or_else(invalid)?;
        let (worker, token) = original
            .factory
            .try_channel_worker(original.epoch, self.generation)?;
        let attachment = EntryWorker {
            factory: original.factory.clone(),
            token,
        };
        {
            let mut state = self.cache.core().state.lock().expect("channel-cache lock");
            let State::Bounded(entries) = &mut *state else {
                return Err(invalid());
            };
            let entry = &mut entries[self.entry];
            if entry.generation != self.generation
                || !matches!(entry.phase, Phase::Connecting)
                || entry.worker.is_some()
            {
                return Err(invalid());
            }
            entry.worker = Some(attachment);
        }
        Ok(worker)
    }

    pub(crate) fn publish(mut self, channel: Channel) -> io::Result<()> {
        let notifications = {
            let mut state = self.cache.core().state.lock().expect("channel-cache lock");
            let State::Bounded(entries) = &mut *state else {
                return Err(invalid());
            };
            let entry = &mut entries[self.entry];
            if entry.generation != self.generation || !matches!(entry.phase, Phase::Connecting) {
                return Err(invalid());
            }
            entry.phase = Phase::Ready(channel);
            self.completed = true;
            entry.notifications()
        };
        notify(notifications);
        Ok(())
    }
}

impl Drop for Leader {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let (notifications, retired_worker) = {
            let mut state = self.cache.core().state.lock().expect("channel-cache lock");
            let State::Bounded(entries) = &mut *state else {
                return;
            };
            let entry = &mut entries[self.entry];
            if entry.generation != self.generation || !matches!(entry.phase, Phase::Connecting) {
                return;
            }
            entry.phase = Phase::Failed;
            (entry.notifications(), entry.worker.take())
        };
        drop(retired_worker);
        notify(notifications);
    }
}

fn notify(notifications: [Option<Waker>; WAITERS]) {
    let unwinding = std::thread::panicking();
    let mut first_panic = None;
    for waker in notifications.into_iter().flatten() {
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake()))
            && first_panic.is_none()
        {
            first_panic = Some(payload);
        }
    }
    if !unwinding && let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}
