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

//! Bounded single-flight cache of outgoing Native channels.
//!
//! One row per peer process, endpoint and method lane. The first caller for a
//! key becomes the leader that dials; at most `WAITERS` other callers wait for
//! it, and the next one is refused rather than queued. A Ready row may be
//! evicted only when nobody waits on it; eviction drops the cache's Channel
//! alias, while escaped Channels and their connections keep their own owners.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use tonic::transport::Channel;

use crate::native_channel_identity::InlineNativeChannelIdentity;

const WAITERS: usize =
    NativeResultSupportGeometry::V1.transport_tonic_pending_per_connection as usize;

/// Rows cover every legal peer lane with its connecting and closing headroom.
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

struct Core {
    state: Mutex<Vec<Entry>>,
    eviction_cursor: AtomicUsize,
}

#[derive(Clone)]
pub(crate) struct NativeChannelCache {
    core: Arc<Core>,
}

impl NativeChannelCache {
    fn core(&self) -> &Arc<Core> {
        &self.core
    }

    pub(crate) fn bounded() -> io::Result<Self> {
        let count = entry_positions()?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        entries.resize_with(count, Entry::vacant);
        Ok(Self {
            core: Arc::new(Core {
                state: Mutex::new(entries),
                eviction_cursor: AtomicUsize::new(0),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn same_cache(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.core(), other.core())
    }

    #[cfg(test)]
    pub(crate) fn remove(&self, identity: InlineNativeChannelIdentity) -> Option<Channel> {
        let mut state = self.core().state.lock().expect("channel-cache lock");
        let entry = state.iter_mut().find(|e| e.identity == Some(identity))?;
        if !matches!(entry.phase, Phase::Ready(_)) {
            return None;
        }
        match std::mem::replace(&mut entry.phase, Phase::Vacant) {
            Phase::Ready(channel) => Some(channel),
            _ => unreachable!(),
        }
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
            let entries = &mut *state;
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
        let outcome = {
            let mut state = me.cache.core().state.lock().expect("channel-cache lock");
            let entries = &mut *state;
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
        if outcome.is_ready() {
            me.unregister();
        }
        outcome
    }
}

#[cfg(test)]
#[path = "native_channel_cache_tests.rs"]
mod tests;

pub(crate) struct Leader {
    cache: NativeChannelCache,
    entry: usize,
    generation: u64,
    completed: bool,
}

impl Leader {
    pub(crate) fn publish(mut self, channel: Channel) -> io::Result<()> {
        let notifications = {
            let mut state = self.cache.core().state.lock().expect("channel-cache lock");
            let entries = &mut *state;
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
        let notifications = {
            let mut state = self.cache.core().state.lock().expect("channel-cache lock");
            let entries = &mut *state;
            let entry = &mut entries[self.entry];
            if entry.generation != self.generation || !matches!(entry.phase, Phase::Connecting) {
                return;
            }
            entry.phase = Phase::Failed;
            entry.notifications()
        };
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
