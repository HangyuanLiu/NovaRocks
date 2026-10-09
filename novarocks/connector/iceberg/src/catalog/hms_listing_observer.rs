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

//! Feature-only observations of one original HMS admission allocation.
//!
//! This is not a provider authority or proof of RPC/connection/alias retirement.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(super) const JOURNAL_CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HmsListingOperation {
    Namespaces,
    Tables,
    Views,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExitSelection {
    OwnerDropped,
    InitialCheck,
    StopWaiting,
    DeadlineWaiting,
    AdmissionClosed,
    StopAdmitted,
    DeadlineAdmitted,
    ReadyOk,
    ReadyErr,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct InvocationRecord {
    pub ordinal: u64,
    pub operation: HmsListingOperation,
    pub target_sha256: Option<[u8; 32]>,
    pub original_deadline: Instant,
    pub started: u64,
    pub acquired: u64,
    pub sdk_created: u64,
    pub sdk_first_poll: u64,
    pub sdk_ready: u64,
    pub sdk_dropped: u64,
    pub wrapper_dropped: u64,
    pub permit_returned: u64,
    pub settled: u64,
    pub selection: ExitSelection,
    pub stop_at_selection: bool,
    pub deadline_at_selection: bool,
}

#[derive(Debug)]
pub(crate) struct Snapshot {
    // The token identifies this observer allocation, not a catalog/native identity.
    pub domain: uuid::Uuid,
    pub phase: u64,
    pub sequence: u64,
    pub invocations_in_flight: u64,
    pub admitted_wrappers_live: u64,
    pub peak_admitted_wrappers_live: u64,
    pub available_positions_sample: Option<usize>,
    pub sdk_objects_live: u64,
    pub peak_sdk_objects_live: u64,
    // Each snapshot owns an independent, exactly bounded heap journal. Large
    // by-value arrays would multiply stack usage in async callers.
    pub records: Box<[Option<InvocationRecord>]>,
    pub used: usize,
}

#[derive(Debug)]
struct State {
    phase: u64,
    sequence: u64,
    next_ordinal: u64,
    in_flight: u64,
    held: u64,
    peak: u64,
    sdk_live: u64,
    sdk_peak: u64,
    records: Box<[Option<InvocationRecord>]>,
    used: usize,
    invalid: bool,
}

#[derive(Debug)]
pub(super) struct Observer {
    domain: uuid::Uuid,
    state: Mutex<State>,
}

#[derive(Clone, Debug)]
pub(super) struct Invocation {
    observer: Arc<Observer>,
    phase: u64,
    ordinal: u64,
    slot: Option<usize>,
}

#[derive(Clone, Copy)]
enum Event {
    Acquire,
    SdkCreate,
    SdkPoll,
    SdkReady,
    SdkDrop,
    WrapperDrop,
    Return,
    Settle,
}

impl Default for Observer {
    fn default() -> Self {
        Self {
            domain: uuid::Uuid::now_v7(),
            state: Mutex::new(State {
                phase: 1,
                sequence: 0,
                next_ordinal: 0,
                in_flight: 0,
                held: 0,
                peak: 0,
                sdk_live: 0,
                sdk_peak: 0,
                records: vec![None; JOURNAL_CAPACITY].into_boxed_slice(),
                used: 0,
                invalid: false,
            }),
        }
    }
}

impl Observer {
    // Observation errors never replace the original SDK result or relax admission.
    fn with_state<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poison) => {
                let mut state = poison.into_inner();
                state.invalid = true;
                state
            }
        };
        f(&mut state)
    }

    pub(super) fn begin(
        self: &Arc<Self>,
        operation: HmsListingOperation,
        target_sha256: Option<[u8; 32]>,
        deadline: Instant,
    ) -> Invocation {
        self.with_state(|state| {
            let ordinal = increment(&mut state.next_ordinal, &mut state.invalid);
            let started = increment(&mut state.sequence, &mut state.invalid);
            increment(&mut state.in_flight, &mut state.invalid);
            let slot = if state.used < JOURNAL_CAPACITY {
                let slot = state.used;
                state.used += 1;
                state.records[slot] = Some(InvocationRecord {
                    ordinal,
                    operation,
                    target_sha256,
                    original_deadline: deadline,
                    started,
                    acquired: 0,
                    sdk_created: 0,
                    sdk_first_poll: 0,
                    sdk_ready: 0,
                    sdk_dropped: 0,
                    wrapper_dropped: 0,
                    permit_returned: 0,
                    settled: 0,
                    selection: ExitSelection::OwnerDropped,
                    stop_at_selection: false,
                    deadline_at_selection: false,
                });
                Some(slot)
            } else {
                state.invalid = true;
                None
            };
            Invocation {
                observer: Arc::clone(self),
                phase: state.phase,
                ordinal,
                slot,
            }
        })
    }

    pub(super) fn snapshot(&self) -> Result<Snapshot, &'static str> {
        self.with_state(|state| {
            if state.invalid {
                return Err("HMS listing observation is invalid");
            }
            Ok(Snapshot {
                domain: self.domain,
                phase: state.phase,
                sequence: state.sequence,
                invocations_in_flight: state.in_flight,
                admitted_wrappers_live: state.held,
                peak_admitted_wrappers_live: state.peak,
                available_positions_sample: None,
                sdk_objects_live: state.sdk_live,
                peak_sdk_objects_live: state.sdk_peak,
                records: state.records.clone(),
                used: state.used,
            })
        })
    }

    // A reset cannot erase failure or an outstanding invocation. History must be
    // independently saved before reset; ordinals and event sequence never reset.
    pub(super) fn reset_idle(
        &self,
        expected_domain: uuid::Uuid,
        expected_phase: u64,
        expected_sequence: u64,
    ) -> Result<u64, &'static str> {
        self.with_state(|state| {
            if state.invalid
                || expected_domain != self.domain
                || expected_phase != state.phase
                || expected_sequence != state.sequence
                || state.in_flight != 0
                || state.held != 0
                || state.sdk_live != 0
            {
                return Err("HMS listing observation reset refused");
            }
            let Some(phase) = state.phase.checked_add(1) else {
                state.invalid = true;
                return Err("HMS listing observation phase overflow");
            };
            state.phase = phase;
            state.records.fill(None);
            state.used = 0;
            state.peak = 0;
            state.sdk_peak = 0;
            Ok(phase)
        })
    }
}

fn increment(value: &mut u64, invalid: &mut bool) -> u64 {
    match value.checked_add(1) {
        Some(next) => {
            *value = next;
            next
        }
        None => {
            *invalid = true;
            0
        }
    }
}
fn decrement(value: &mut u64, invalid: &mut bool) {
    match value.checked_sub(1) {
        Some(next) => *value = next,
        None => *invalid = true,
    }
}

impl Invocation {
    fn event(&self, event: Event) {
        self.observer
            .with_state(|state| self.event_locked(state, event));
    }

    fn event_locked(&self, state: &mut State, event: Event) {
        if state.phase != self.phase {
            state.invalid = true;
            return;
        }
        let sequence = increment(&mut state.sequence, &mut state.invalid);
        match event {
            Event::Acquire => {
                increment(&mut state.held, &mut state.invalid);
                state.peak = state.peak.max(state.held);
                if state.held > super::listing_admission::LISTING_CONCURRENCY as u64 {
                    state.invalid = true;
                }
            }
            Event::SdkCreate => {
                increment(&mut state.sdk_live, &mut state.invalid);
                state.sdk_peak = state.sdk_peak.max(state.sdk_live);
                if state.sdk_live > super::listing_admission::LISTING_CONCURRENCY as u64 {
                    state.invalid = true;
                }
            }
            Event::SdkDrop => decrement(&mut state.sdk_live, &mut state.invalid),
            Event::Settle => decrement(&mut state.in_flight, &mut state.invalid),
            _ => (),
        }
        let Some(slot) = self.slot else {
            state.invalid = true;
            return;
        };
        let Some(record) = &mut state.records[slot] else {
            state.invalid = true;
            return;
        };
        if record.ordinal != self.ordinal {
            state.invalid = true;
            return;
        }
        if matches!(event, Event::WrapperDrop) && record.acquired != 0 {
            decrement(&mut state.held, &mut state.invalid);
        }
        let valid = match event {
            Event::Acquire => record.acquired == 0 && record.wrapper_dropped == 0,
            Event::SdkCreate => {
                record.acquired != 0 && record.sdk_created == 0 && record.wrapper_dropped == 0
            }
            Event::SdkPoll => record.sdk_created != 0 && record.sdk_dropped == 0,
            Event::SdkReady => {
                record.sdk_first_poll != 0 && record.sdk_ready == 0 && record.sdk_dropped == 0
            }
            Event::SdkDrop => {
                record.sdk_created != 0 && record.sdk_dropped == 0 && record.wrapper_dropped == 0
            }
            Event::WrapperDrop => {
                record.wrapper_dropped == 0 && (record.sdk_created == 0 || record.sdk_dropped != 0)
            }
            Event::Return => {
                record.acquired != 0
                    && record.wrapper_dropped != 0
                    && record.permit_returned == 0
                    && (record.sdk_created == 0
                        || (record.sdk_dropped > record.acquired && record.sdk_dropped < sequence))
            }
            Event::Settle => {
                record.wrapper_dropped != 0
                    && record.settled == 0
                    && (record.acquired == 0 || record.permit_returned != 0)
            }
        };
        if !valid {
            state.invalid = true;
            return;
        }
        match event {
            Event::Acquire => record.acquired = sequence,
            Event::SdkCreate => record.sdk_created = sequence,
            Event::SdkPoll => {
                if record.sdk_first_poll == 0 {
                    record.sdk_first_poll = sequence
                }
            }
            Event::SdkReady => record.sdk_ready = sequence,
            Event::SdkDrop => record.sdk_dropped = sequence,
            Event::WrapperDrop => record.wrapper_dropped = sequence,
            Event::Return => record.permit_returned = sequence,
            Event::Settle => record.settled = sequence,
        }
    }

    pub(super) fn select(
        &self,
        selection: ExitSelection,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) {
        self.observer.with_state(|state| {
            if state.phase != self.phase {
                state.invalid = true;
                return;
            }
            let Some(record) = self.slot.and_then(|slot| state.records[slot].as_mut()) else {
                state.invalid = true;
                return;
            };
            if record.ordinal != self.ordinal || record.original_deadline != context.deadline() {
                state.invalid = true;
                return;
            }
            record.selection = selection;
            // These are samples at selection, not atomic proof of simultaneous readiness.
            record.stop_at_selection = context.is_cancelled();
            record.deadline_at_selection = Instant::now() >= context.deadline();
        });
    }
    fn invalidate_unwind(&self) {
        self.observer.with_state(|state| state.invalid = true);
    }
}

// Declaration/drop order is intentional: the actual SDK future lives in the
// inner scope, so its destructor runs before this witness records SDK drop.
struct SdkExit(Invocation);
impl Drop for SdkExit {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.invalidate_unwind();
        } else {
            self.0.event(Event::SdkDrop);
        }
    }
}

pub(super) async fn sdk_call<T, F: Future<Output = T>>(
    invocation: Option<&Invocation>,
    call: F,
) -> T {
    let Some(invocation) = invocation else {
        return call.await;
    };
    invocation.event(Event::SdkCreate);
    let _exit = SdkExit(invocation.clone());
    {
        tokio::pin!(call);
        let mut first_poll = true;
        std::future::poll_fn(|cx| {
            if first_poll {
                invocation.event(Event::SdkPoll);
                first_poll = false;
            }
            let outcome = call.as_mut().poll(cx);
            if outcome.is_ready() {
                invocation.event(Event::SdkReady);
            }
            outcome
        })
        .await
    }
}

pub(super) struct WrapperExit(pub(super) Invocation);
impl Drop for WrapperExit {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.invalidate_unwind();
        } else {
            self.0.event(Event::WrapperDrop);
        }
    }
}
pub(super) struct PermitOwner {
    pub(super) invocation: Invocation,
    pub(super) permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl PermitOwner {
    pub(super) fn acquired(&mut self, permit: tokio::sync::OwnedSemaphorePermit) {
        self.permit = Some(permit);
        self.invocation.event(Event::Acquire);
    }
}
impl Drop for PermitOwner {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            // Do not hold the observer lock while the real permit wakes waiters.
            // Another acquire may occur between physical return and this event;
            // the journal never claims an atomic semaphore census.
            drop(permit);
            self.invocation.event(Event::Return);
        }
        self.invocation.event(Event::Settle);
    }
}

#[cfg(test)]
mod internal_tests {
    use super::*;

    #[test]
    fn overflow_never_reuses_a_historical_slot_and_cannot_clear_failure() {
        let observer = Arc::new(Observer::default());
        let first = observer.begin(HmsListingOperation::Tables, None, Instant::now());
        observer.with_state(|state| {
            state.used = JOURNAL_CAPACITY;
            state.in_flight = 0;
        });
        let extra = observer.begin(HmsListingOperation::Views, None, Instant::now());
        assert!(extra.slot.is_none());
        observer.with_state(|state| {
            assert_eq!(state.records[0].unwrap().ordinal, first.ordinal);
            assert_eq!(
                state.records[0].unwrap().operation,
                HmsListingOperation::Tables
            );
            assert!(state.invalid);
        });
        assert!(observer.snapshot().is_err());
    }

    #[test]
    fn counter_overflow_and_stale_invocation_cannot_produce_valid_evidence() {
        let observer = Arc::new(Observer::default());
        observer.with_state(|state| state.next_ordinal = u64::MAX);
        let invoke = observer.begin(HmsListingOperation::Tables, None, Instant::now());
        assert!(observer.snapshot().is_err());
        let other = Arc::new(Observer::default());
        let invoke = Invocation {
            observer: other.clone(),
            phase: invoke.phase + 1,
            ordinal: 1,
            slot: Some(0),
        };
        invoke.event(Event::SdkDrop);
        assert!(other.snapshot().is_err());
    }

    #[test]
    fn idle_phase_overflow_permanently_invalidates_observation() {
        let observer = Observer::default();
        observer.with_state(|state| state.phase = u64::MAX);
        let before = observer.snapshot().unwrap();
        assert_eq!(before.invocations_in_flight, 0);
        assert_eq!(before.admitted_wrappers_live, 0);
        assert_eq!(before.sdk_objects_live, 0);
        assert_eq!(
            observer.reset_idle(before.domain, before.phase, before.sequence),
            Err("HMS listing observation phase overflow")
        );
        assert!(observer.snapshot().is_err());
        assert_eq!(
            observer.reset_idle(before.domain, before.phase, before.sequence),
            Err("HMS listing observation reset refused")
        );
        observer.with_state(|state| {
            assert_eq!(state.phase, u64::MAX);
            assert!(state.invalid);
        });
    }
}
