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

//! Fixed poll backing within the original control inventory.
//!
//! This private substrate quotes its frame/container storage only. It does not
//! provide a complete role profile, reserve input pointees, or activate Server.

mod ready_queue;
use ready_queue::ReadyQueue;

use crate::{
    WorkError, WorkId, WorkOwner, WorkloadControl,
    control_storage::{SlotId, SlotPhase, arc_bytes},
    scope::Inner,
};
use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    mem::size_of,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::sync::{Notify, futures::OwnedNotified};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PollRoleGeometry {
    per_slot: usize,
    shared: usize,
}
impl PollRoleGeometry {
    pub(crate) fn for_factory<I, F: Future, M: Fn(WorkOwner, I) -> F>(
        _: &M,
    ) -> Result<Self, WorkError> {
        let per_slot = size_of::<Position<I, F>>()
            .checked_add(size_of::<Option<F>>())
            .and_then(|n| n.checked_add(size_of::<PollBacking>()))
            .and_then(|n| n.checked_add(size_of::<Option<Arc<PollWake>>>()))
            .and_then(|n| n.checked_add(arc_bytes::<PollWake>().ok()?))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let shared = size_of::<RolePollBank<I, F, M>>()
            .checked_add(arc_bytes::<Notify>()?)
            .and_then(|n| n.checked_add(arc_bytes::<ReadyQueue>().ok()?))
            .and_then(|n| n.checked_add(size_of::<OwnedNotified>()))
            // Pinned macOS mutex backing: ready ring and Notify wait list.
            .and_then(|n| n.checked_add(if cfg!(target_os = "macos") { 128 } else { 0 }))
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(Self { per_slot, shared })
    }
    pub(crate) fn bytes(self, slots: usize) -> Result<usize, WorkError> {
        self.per_slot
            .checked_mul(slots)
            .and_then(|n| n.checked_add(self.shared))
            .ok_or(WorkError::ArithmeticOverflow)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PollPhase {
    Free,
    Mounted,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrationPhase {
    Absent,
    Installed,
    Stopping,
}

pub(crate) struct PollBacking {
    geometry: PollRoleGeometry,
    wake: Arc<PollWake>,
    pub(crate) phase: PollPhase,
    registration: RegistrationPhase,
}
impl PollBacking {
    pub(crate) fn is_installed(&self) -> bool {
        self.registration == RegistrationPhase::Installed
    }
    pub(crate) fn try_reap(&mut self) -> bool {
        if self.phase != PollPhase::Retired {
            return false;
        }
        let Some(wake) = Arc::get_mut(&mut self.wake) else {
            return false;
        };
        wake.queued.store(false, Ordering::Relaxed);
        if self.registration != RegistrationPhase::Installed {
            wake.changed = Weak::new();
        }
        self.phase = PollPhase::Free;
        true
    }
    pub(crate) fn new(geometry: PollRoleGeometry, slot: SlotId) -> Self {
        Self {
            geometry,
            wake: Arc::new(PollWake {
                queued: AtomicBool::new(false),
                slot,
                changed: Weak::new(),
            }),
            phase: PollPhase::Free,
            registration: RegistrationPhase::Absent,
        }
    }
}
struct PollWake {
    queued: AtomicBool,
    slot: SlotId,
    changed: Weak<ReadyQueue>,
}
impl Wake for PollWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(queue) = self.changed.upgrade()
            && queue.publish(self)
        {
            queue.notify();
        }
    }
}

// Input, frame and result are destroyed before the actual mount footer.
struct Position<I, F: Future> {
    input: Option<(WorkOwner, I)>,
    frame: Pin<Box<Option<F>>>,
    output: Option<F::Output>,
    footer: Option<Footer>,
}
struct Footer {
    wake: Arc<PollWake>,
    _pin: MountPin,
}
struct MountPin {
    inner: Arc<Inner>,
    slot: SlotId,
    generation: u64,
    work: WorkId,
    role: usize,
}
impl Drop for MountPin {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            let slot = &mut state.cancellations.slots[self.slot.0];
            assert_eq!(slot.generation, self.generation);
            assert_eq!(slot.phase, SlotPhase::Node(self.work));
            let role = &mut slot.polls[self.role];
            assert_eq!(role.phase, PollPhase::Mounted);
            role.phase = PollPhase::Retired;
            state.cancellations.retired_polls += 1;
            state.advance_progress_revision();
            state.collect(self.work);
        }
        self.inner.notify_reclaim_progress();
    }
}

/// A completed value keeps its original footer until the value actually exits.
/// There is no consuming callback or raw take API.
pub(crate) struct PollOutput<O> {
    value: O,
    footer: Footer,
}
impl<O> PollOutput<O> {
    pub(crate) fn value(&self) -> &O {
        &self.value
    }
}
/// The two real catch sites retain their original payloads. If frame cleanup
/// fails after Ready, the original output remains under this same footer too.
/// Borrowing reveals facts; it cannot detach an owned value from its custody.
pub(crate) struct PollFailure<O> {
    primary: Box<dyn Any + Send>,
    cleanup: Option<Box<dyn Any + Send>>,
    output: Option<O>,
}
impl<O> PollFailure<O> {
    pub(crate) fn primary(&self) -> &(dyn Any + Send) {
        self.primary.as_ref()
    }
    pub(crate) fn cleanup(&self) -> Option<&(dyn Any + Send)> {
        self.cleanup.as_deref()
    }
    pub(crate) fn output(&self) -> Option<&O> {
        self.output.as_ref()
    }
}
pub(crate) enum PollCompletion<O> {
    Ready(PollOutput<O>),
    Panicked(PollOutput<PollFailure<O>>),
}
pub(crate) struct MountRejected<I> {
    pub(crate) owner: WorkOwner,
    pub(crate) input: I,
    pub(crate) error: WorkError,
}

/// Only original fixed-profile composition can construct this bank. It has no
/// queue, free list, public mount guard, or independent capacity decision.
pub(crate) struct RolePollBank<I, F: Future, M> {
    inner: Arc<Inner>,
    role: usize,
    make: M,
    changed: Arc<Notify>,
    ready: Arc<ReadyQueue>,
    waiter: Pin<Box<OwnedNotified>>,
    positions: Vec<Position<I, F>>,
    marker: PhantomData<fn(I)>,
    // Registration leaves after make, positions and all other actual bank fields.
    _registration: BankRegistration,
}
impl<I, F: Future, M: Fn(WorkOwner, I) -> F> RolePollBank<I, F, M> {
    pub(crate) fn install(
        control: &mut WorkloadControl,
        role: usize,
        make: M,
    ) -> Result<Self, WorkError> {
        // Only the original unique startup owner can install. No shared Inner
        // entry point can allocate competing banks against the same paid base.
        let inner = &control.inner;
        let geometry = PollRoleGeometry::for_factory(&make)?;
        let validate = |state: &mut crate::scope::State| -> Result<usize, WorkError> {
            if state.ready || state.closed || !state.cancellations.fixed || !state.nodes.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            for slot in &mut state.cancellations.slots {
                let backing = slot.polls.get_mut(role).ok_or(WorkError::NotReady)?;
                if slot.phase != SlotPhase::Free
                    || backing.registration != RegistrationPhase::Absent
                    || backing.phase != PollPhase::Free
                    || backing.geometry != geometry
                    || Arc::get_mut(&mut backing.wake).is_none()
                {
                    return Err(WorkError::Conflict);
                }
            }
            Ok(state.cancellations.slots.len())
        };
        let count = validate(&mut inner.state.lock().unwrap())?;
        let changed = Arc::new(Notify::new());
        {
            let mut notified = std::pin::pin!(changed.notified());
            notified.as_mut().enable();
        }
        let ready = ReadyQueue::new(count, Arc::clone(&changed))?;
        let mut waiter = Box::pin(Arc::clone(&changed).notified_owned());
        waiter.as_mut().enable();
        let mut positions = Vec::new();
        positions
            .try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("fixed role positions"))?;
        for _ in 0..count {
            positions.push(Position {
                input: None,
                frame: Box::pin(None),
                output: None,
                footer: None,
            });
        }
        // This is startup registration, with all backing already funded by
        // the original fixed base. It executes no runtime notification callback.
        inner.update_facts_silent(|state| {
            validate(state)?;
            for slot in &mut state.cancellations.slots {
                let backing = &mut slot.polls[role];
                Arc::get_mut(&mut backing.wake).unwrap().changed = Arc::downgrade(&ready);
                backing.registration = RegistrationPhase::Installed;
            }
            Ok::<(), WorkError>(())
        })?;
        Ok(Self {
            inner: Arc::clone(inner),
            role,
            make,
            changed,
            ready,
            waiter,
            positions,
            marker: PhantomData,
            _registration: BankRegistration {
                inner: Arc::clone(inner),
                role,
            },
        })
    }

    pub(crate) fn mount_owned(
        &mut self,
        owner: WorkOwner,
        input: I,
    ) -> Result<(), MountRejected<I>> {
        let scope = owner.scope();
        if !Arc::ptr_eq(&scope.inner, &self.inner) {
            return Err(MountRejected {
                owner,
                input,
                error: WorkError::Conflict,
            });
        }
        let work = scope.id();
        let selected = {
            let state = self.inner.state.lock().unwrap();
            state.nodes.get(&work).map(|node| {
                (
                    node.cancellation,
                    state.cancellations.slots[node.cancellation.0].generation,
                )
            })
        };
        let Some((id, generation)) = selected else {
            return Err(MountRejected {
                owner,
                input,
                error: WorkError::Released,
            });
        };
        // The real owner and input are already in their stable receiving cell
        // before any transaction callback; rejected values are returned intact.
        let position = &mut self.positions[id.0];
        if position.footer.is_some()
            || position.input.is_some()
            || position.frame.as_ref().get_ref().is_some()
            || position.output.is_some()
        {
            return Err(MountRejected {
                owner,
                input,
                error: WorkError::Conflict,
            });
        }
        position.input = Some((owner, input));
        let result = self.inner.update_facts_silent(|state| {
            if state.closed {
                return Err(WorkError::Closed);
            }
            if !state.ready {
                return Err(WorkError::NotReady);
            }
            let slot = &mut state.cancellations.slots[id.0];
            if slot.phase != SlotPhase::Node(work) || slot.generation != generation {
                return Err(WorkError::Conflict);
            }
            let backing = &mut slot.polls[self.role];
            if !backing.is_installed() {
                return Err(WorkError::NotReady);
            }
            if backing.phase != PollPhase::Free || Arc::get_mut(&mut backing.wake).is_none() {
                return Err(WorkError::Capacity("role backing has not actually exited"));
            }
            backing.phase = PollPhase::Mounted;
            position.footer = Some(Footer {
                wake: Arc::clone(&backing.wake),
                _pin: MountPin {
                    inner: Arc::clone(&self.inner),
                    slot: id,
                    generation,
                    work,
                    role: self.role,
                },
            });
            assert!(
                self.ready.publish(&backing.wake),
                "unique mount has no old ready hint"
            );
            Ok(())
        });
        if let Err(error) = result {
            let (owner, input) = position.input.take().unwrap();
            return Err(MountRejected {
                owner,
                input,
                error,
            });
        }
        // Unpublished refusals return their original values before callbacks.
        // Successful facts precede both notifications; queue notification comes
        // first, so a WC waiter panic cannot hide the actual mounted input.
        self.ready.notify();
        self.inner.changed.notify_waiters();
        Ok(())
    }

    /// Level-triggered readiness. Register before reading the real queue;
    /// a consumed Notify permit never stands in for the queue's contents.
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.waiter.as_mut().enable();
        if !self.ready.is_empty() {
            return Poll::Ready(());
        }
        match self.waiter.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                self.waiter
                    .as_mut()
                    .set(Arc::clone(&self.changed).notified_owned());
                // A stale signal can cause one finite empty service turn.
                Poll::Ready(())
            }
        }
    }

    /// Poll at most the given number of actual ready hints. None means no
    /// completion in this turn; poll_ready still checks any remaining items.
    pub(crate) fn progress_turn(
        &mut self,
        budget: NonZeroUsize,
    ) -> Option<PollCompletion<F::Output>> {
        for _ in 0..budget.get() {
            let Some(wake) = self.ready.pop() else {
                break;
            };
            let position = &mut self.positions[wake.slot.0];
            let Some(footer) = position.footer.as_ref() else {
                // A completed role can still have a queued stale wake. Its
                // actual Arc exits here, outside the queue and State locks.
                continue;
            };
            assert!(
                Arc::ptr_eq(&footer.wake, &wake),
                "ready hint retains its original anchor"
            );
            let construction = if let Some((owner, input)) = position.input.take() {
                catch_unwind(AssertUnwindSafe(|| {
                    position.frame.as_mut().set(Some((self.make)(owner, input)))
                }))
            } else {
                Ok(())
            };
            let mut primary = construction.err();
            if primary.is_none() {
                let waker = Waker::from(Arc::clone(&position.footer.as_ref().unwrap().wake));
                let mut context = Context::from_waker(&waker);
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    position
                        .frame
                        .as_mut()
                        .as_pin_mut()
                        .unwrap()
                        .poll(&mut context)
                }));
                match polled {
                    Ok(Poll::Pending) => continue,
                    Ok(Poll::Ready(value)) => position.output = Some(value),
                    Err(payload) => primary = Some(payload),
                }
            }
            // All frame/input temporaries and callbacks leave before the footer.
            let cleared = catch_unwind(AssertUnwindSafe(|| position.frame.as_mut().set(None)));
            let mut cleanup = cleared.err();
            if primary.is_none() {
                primary = cleanup.take();
            }
            let footer = position.footer.take().unwrap();
            if let Some(primary) = primary {
                return Some(PollCompletion::Panicked(PollOutput {
                    value: PollFailure {
                        primary,
                        cleanup,
                        output: position.output.take(),
                    },
                    footer,
                }));
            }
            return Some(PollCompletion::Ready(PollOutput {
                value: position.output.take().unwrap(),
                footer,
            }));
        }
        None
    }
}

impl<I, F: Future, M> Drop for RolePollBank<I, F, M> {
    fn drop(&mut self) {
        // A ready process loses a required service. Stop new admission before
        // destroying any frame; do not manufacture terminal facts for its Work.
        let mut state = self.inner.state.lock().unwrap();
        for slot in &mut state.cancellations.slots {
            let role = &mut slot.polls[self.role];
            assert_eq!(role.registration, RegistrationPhase::Installed);
            role.registration = RegistrationPhase::Stopping;
        }
        if state.ready {
            state.ready = false;
            state.closed = true;
        }
        state.advance_progress_revision();
    }
}

struct BankRegistration {
    inner: Arc<Inner>,
    role: usize,
}
impl Drop for BankRegistration {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            for slot in &mut state.cancellations.slots {
                let role = &mut slot.polls[self.role];
                assert_eq!(role.registration, RegistrationPhase::Stopping);
                if let Some(wake) = Arc::get_mut(&mut role.wake) {
                    // A Weak retains the old Notify allocation after its value
                    // exits. Destroy it before a replacement bank may allocate.
                    wake.changed = Weak::new();
                }
                role.registration = RegistrationPhase::Absent;
            }
            state.advance_progress_revision();
        }
        // Original inventory/driver survives this bank, including external
        // PollOutput and silent stale Wakers that still retain its old slot.
        self.inner.notify_reclaim_progress();
    }
}

#[cfg(test)]
mod tests;
