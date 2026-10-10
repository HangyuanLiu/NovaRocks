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

//! Original Native subscription handles and their caller-owned responsibility.
//! Storage equals the SAME FE WorkloadConfig scope-record limit, not a new quota.

use super::original_retirement::OriginalRetirement;
use super::task_transport::SubscriptionState;
use crate::task_execution::status_intake::ObservationPublisher;
use novarocks_workload_control::{WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Instant,
};
use tokio::{
    runtime::Handle,
    sync::Notify,
    task::{Id, JoinError, JoinHandle},
};

type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type RawValue = Box<dyn Any + Send>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubscriptionJoinFacts {
    pub(crate) id: Id,
    pub(crate) panic: bool,
    pub(crate) cancelled: bool,
}
impl SubscriptionJoinFacts {
    fn of(error: &JoinError) -> Self {
        Self {
            id: error.id(),
            panic: error.is_panic(),
            cancelled: error.is_cancelled(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationError {
    Closed,
    Storage,
    Generation,
    Work(WorkError),
    SpawnUnjoinable,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FailureFacts {
    pub(crate) original: Option<SubscriptionJoinFacts>,
    pub(crate) reaper: Option<SubscriptionJoinFacts>,
    pub(crate) spawn_unjoinable: bool,
    pub(crate) reaper_exited_early: bool,
    pub(crate) cleanup_failures: u64,
    pub(crate) counter_overflow: bool,
}
impl FailureFacts {
    fn failed(self) -> bool {
        self.original.is_some()
            || self.reaper.is_some()
            || self.spawn_unjoinable
            || self.reaper_exited_early
            || self.cleanup_failures != 0
            || self.counter_overflow
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainObservation {
    Joined,
    Failed(FailureFacts),
    Deadline,
}

/// No arbitrary payload is destroyed by this backing. Its final alias follows
/// original future/cause retirement; otherwise dropping the owner records an orphan.
struct Backing {
    owner: Mutex<Option<WorkOwner>>,
    publisher: Option<Arc<ObservationPublisher>>,
    retired: std::sync::atomic::AtomicBool,
}
impl Drop for Backing {
    fn drop(&mut self) {
        let owner = self
            .owner
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("one original subscription child");
        // Release the original publisher only after actual F/cause destruction.
        self.publisher.take();
        if self.retired.load(std::sync::atomic::Ordering::Acquire) {
            owner.complete_after_terminal_cancel_settled();
        } else {
            drop(owner);
        }
    }
}
struct OriginalInput {
    future: Mutex<Option<OriginalFuture>>,
    cause: Mutex<Option<RawValue>>,
}
struct OriginalTask {
    input: Arc<OriginalInput>,
}
impl Future for OriginalTask {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let mut original = self.input.future.lock().unwrap_or_else(|e| e.into_inner());
        original
            .as_mut()
            .expect("one retained original subscription future")
            .as_mut()
            .poll(cx)
    }
}
enum OriginalState {
    Reserved,
    Running(JoinHandle<()>),
    Retiring(SubscriptionRetirement),
    // The public spawn API did not return a handle. Keep original F and raw
    // spawn cause in this SAME record, with no new spawn or guessed exit.
    SpawnUnjoinable,
}
// Retire F before the raw JoinError, so a destructor panic cannot unwind
// through a second arbitrary raw destructor. Both phases reuse ONE record.
struct SubscriptionRetirement {
    future: Option<OriginalRetirement<Backing>>,
    cause: Option<RawValue>,
    cause_retirement: Option<OriginalRetirement<Backing>>,
    backing: Arc<Backing>,
    cleanup_failures: u64,
}
impl SubscriptionRetirement {
    fn new(input: &OriginalInput, backing: Arc<Backing>) -> Self {
        let future = input
            .future
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("one original joined F");
        let cause = input.cause.lock().unwrap_or_else(|e| e.into_inner()).take();
        Self {
            future: Some(OriginalRetirement::new(future, Arc::clone(&backing))),
            cause,
            cause_retirement: None,
            backing,
            cleanup_failures: 0,
        }
    }
    fn failures_so_far(&self) -> u64 {
        self.cleanup_failures
            .saturating_add(
                self.future
                    .as_ref()
                    .map_or(0, |future| future.cleanup_failures),
            )
            .saturating_add(
                self.cause_retirement
                    .as_ref()
                    .map_or(0, |cause| cause.cleanup_failures),
            )
    }
    fn poll(&mut self, handle: &Handle, cx: &mut std::task::Context<'_>) -> Poll<()> {
        if let Some(future) = self.future.as_mut() {
            if future.poll(handle, cx).is_pending() {
                return Poll::Pending;
            }
            self.cleanup_failures = self
                .cleanup_failures
                .saturating_add(future.cleanup_failures);
            self.future.take();
        }
        if let Some(cause) = self.cause.take() {
            self.cause_retirement = Some(OriginalRetirement::new(cause, Arc::clone(&self.backing)));
        }
        if let Some(cause) = self.cause_retirement.as_mut() {
            if cause.poll(handle, cx).is_pending() {
                return Poll::Pending;
            }
            self.cleanup_failures = self.cleanup_failures.saturating_add(cause.cleanup_failures);
            self.cause_retirement.take();
        }
        Poll::Ready(())
    }
}
struct Record {
    generation: u64,
    failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    input: Arc<OriginalInput>,
    backing: Arc<Backing>,
    stop_requested: bool,
    state: OriginalState,
}
struct State {
    started: bool,
    closed: bool,
    next_generation: u64,
    records: Box<[Option<Record>]>,
    failures: FailureFacts,
}
struct Core {
    handle: Handle,
    state: Mutex<State>,
    changed: Notify,
}

/// One process-owned continuous reaper, never one task per observer.
pub(crate) struct NativeSubscriptionOwner {
    core: Arc<Core>,
    reaper: Option<JoinHandle<()>>,
    reaper_retirement: Option<OriginalRetirement<Core>>,
    children_joined: bool,
    joined: bool,
    unjoinable_reaper: bool,
}
#[derive(Clone)]
pub(crate) struct NativeSubscriptionPort {
    core: Arc<Core>,
}
pub(crate) struct SubscriptionLease {
    core: Arc<Core>,
    index: usize,
    generation: u64,
}
impl SubscriptionLease {
    pub(crate) fn stop(&self) {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(record) = state.records[self.index]
            .as_mut()
            .filter(|record| record.generation == self.generation)
        {
            record.stop_requested = true;
            if let OriginalState::Running(handle) = &record.state {
                handle.abort();
            }
        }
        drop(state);
        self.core.changed.notify_waiters();
    }
}
impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        self.stop();
    }
}

impl NativeSubscriptionOwner {
    /// `scope_records_limit` is copied from the SAME configured FE Workload;
    /// production composition must not substitute a default or another owner.
    pub(crate) fn new(
        handle: Handle,
        scope_records_limit: usize,
    ) -> Result<(Self, NativeSubscriptionPort), std::collections::TryReserveError> {
        let mut records = Vec::new();
        records.try_reserve_exact(scope_records_limit)?;
        records.resize_with(scope_records_limit, || None);
        let core = Arc::new(Core {
            handle,
            state: Mutex::new(State {
                started: false,
                closed: false,
                next_generation: 0,
                records: records.into_boxed_slice(),
                failures: FailureFacts::default(),
            }),
            changed: Notify::new(),
        });
        let port = NativeSubscriptionPort {
            core: Arc::clone(&core),
        };
        Ok((
            Self {
                core,
                reaper: None,
                reaper_retirement: None,
                children_joined: false,
                joined: false,
                unjoinable_reaper: false,
            },
            port,
        ))
    }
    /// Host installs this owner BEFORE opening the port. No early task is minted.
    pub(crate) fn start_reaper(&mut self) -> Result<(), RegistrationError> {
        {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.started || state.closed {
                return Err(RegistrationError::Closed);
            }
            state.started = true;
        }
        let core = Arc::clone(&self.core);
        let guard = ReaperExitGuard {
            core: Arc::clone(&self.core),
        };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.core.handle.spawn(async move {
                let _guard = guard;
                reap_originals(core).await;
            })
        })) {
            Ok(original) => {
                self.reaper = Some(original);
                Ok(())
            }
            Err(original) => {
                self.unjoinable_reaper = true;
                self.reaper_retirement =
                    Some(OriginalRetirement::new(original, Arc::clone(&self.core)));
                self.request_abort();
                self.core
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failures
                    .spawn_unjoinable = true;
                Err(RegistrationError::SpawnUnjoinable)
            }
        }
    }
    /// Called only after original logical/query/Abort/Release owners converge.
    pub(crate) fn request_abort(&self) {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        for record in state.records.iter_mut().flatten() {
            record.stop_requested = true;
            if let OriginalState::Running(original) = &record.state {
                original.abort();
            }
        }
        drop(state);
        self.core.changed.notify_waiters();
    }
    pub(crate) fn failure_facts(&self) -> FailureFacts {
        self.core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failures
    }
    pub(crate) fn is_joined(&self) -> bool {
        self.joined
    }

    /// Host borrows this observer alongside its existing execution drain.
    /// It never closes a healthy status stream or starts a replacement task.
    pub(crate) async fn wait_for_failed_reaper(&self) {
        loop {
            let changed = self.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let facts = self.failure_facts();
            if self.unjoinable_reaper || facts.reaper_exited_early || facts.reaper.is_some() {
                return;
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn abort_original_reaper(&self) {
        self.reaper
            .as_ref()
            .expect("started original reaper")
            .abort();
    }
    /// Every timeout borrows the SAME handle/state. Cancellation and retry retain
    /// all original handles; neither a timeout nor F Drop is an exit receipt.
    pub(crate) async fn drain_until(&mut self, deadline: Instant) -> DrainObservation {
        self.request_abort();
        if Instant::now() >= deadline {
            return DrainObservation::Deadline;
        }
        if self.joined {
            return if self.failure_facts().failed() {
                DrainObservation::Failed(self.failure_facts())
            } else {
                DrainObservation::Joined
            };
        }
        if !self
            .core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .started
        {
            self.children_joined = true;
        }
        if !self.children_joined {
            if let Some(original) = self.reaper.as_mut() {
                match tokio::time::timeout_at(deadline.into(), original).await {
                    Err(_) => return DrainObservation::Deadline,
                    Ok(Ok(())) => self.children_joined = true,
                    Ok(Err(actual)) => {
                        self.core
                            .state
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .failures
                            .reaper
                            .get_or_insert(SubscriptionJoinFacts::of(&actual));
                        self.reaper_retirement =
                            Some(OriginalRetirement::new(actual, Arc::clone(&self.core)));
                    }
                }
                self.reaper.take();
            }
            if !self.children_joined {
                // Reaper failure does not detach children: the SAME core/handles
                // are now polled by this original owner, without another task.
                if tokio::time::timeout_at(deadline.into(), reap_originals(Arc::clone(&self.core)))
                    .await
                    .is_err()
                {
                    return DrainObservation::Deadline;
                }
                self.children_joined = true;
            }
        }
        if let Some(retirement) = self.reaper_retirement.as_mut() {
            if tokio::time::timeout_at(
                deadline.into(),
                std::future::poll_fn(|cx| retirement.poll(&self.core.handle, cx)),
            )
            .await
            .is_err()
            {
                return DrainObservation::Deadline;
            }
            let failures = retirement.cleanup_failures;
            add_cleanup_failures(
                &mut self
                    .core
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failures,
                failures,
            );
            self.reaper_retirement.take();
        }
        // A spawn with no returned handle still has no physical exit proof.
        if self.unjoinable_reaper || Instant::now() >= deadline {
            return DrainObservation::Deadline;
        }
        self.joined = true;
        let facts = self.failure_facts();
        if facts.failed() {
            DrainObservation::Failed(facts)
        } else {
            DrainObservation::Joined
        }
    }
}

pub(crate) struct SubscriptionReservation {
    core: Arc<Core>,
    index: usize,
    generation: u64,
    active: bool,
}
impl Drop for NativeSubscriptionOwner {
    fn drop(&mut self) {
        // Fail closed on abandoned ownership. This only closes admission and
        // requests abort; it NEVER marks original joins/destructors complete.
        // Production Host must retain this owner through actual drain/retry.
        self.request_abort();
    }
}

impl NativeSubscriptionPort {
    /// Reserve the original child and fixed role position BEFORE constructing F.
    /// Publisher is Some for every production Covered subscriber; legacy is test-only.
    pub(crate) fn reserve(
        &self,
        scope: &WorkScope,
        publisher: Option<Arc<ObservationPublisher>>,
        failure_state: Option<Arc<Mutex<SubscriptionState>>>,
    ) -> Result<SubscriptionReservation, RegistrationError> {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.started || state.closed {
            return Err(RegistrationError::Closed);
        }
        let index = state
            .records
            .iter()
            .position(Option::is_none)
            .ok_or(RegistrationError::Storage)?;
        let generation = match state.next_generation.checked_add(1) {
            Some(value) => value,
            None => {
                state.failures.counter_overflow = true;
                return Err(RegistrationError::Generation);
            }
        };
        let owner = scope
            .child(WorkRequest::new(WorkClass::Query))
            .map_err(RegistrationError::Work)?;
        let backing = Arc::new(Backing {
            owner: Mutex::new(Some(owner)),
            publisher,
            retired: std::sync::atomic::AtomicBool::new(false),
        });
        let input = Arc::new(OriginalInput {
            future: Mutex::new(None),
            cause: Mutex::new(None),
        });
        state.next_generation = generation;
        state.records[index] = Some(Record {
            generation,
            failure_state,
            input,
            backing,
            stop_requested: false,
            state: OriginalState::Reserved,
        });
        Ok(SubscriptionReservation {
            core: Arc::clone(&self.core),
            index,
            generation,
            active: true,
        })
    }
}
impl SubscriptionReservation {
    pub(crate) fn start<F>(mut self, future: F) -> Result<SubscriptionLease, RegistrationError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        let record = state.records[self.index]
            .as_mut()
            .filter(|r| r.generation == self.generation)
            .expect("same original reservation");
        assert!(
            matches!(record.state, OriginalState::Reserved),
            "one original subscription start"
        );
        let input = Arc::clone(&record.input);
        *input.future.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::pin(future));
        self.active = false;
        if state.closed {
            let record = state.records[self.index]
                .as_mut()
                .expect("same original reservation");
            record.stop_requested = true;
            record.state = OriginalState::Retiring(SubscriptionRetirement::new(
                &record.input,
                Arc::clone(&record.backing),
            ));
            drop(state);
            self.core.changed.notify_waiters();
            return Err(RegistrationError::Closed);
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.core.handle.spawn(OriginalTask {
                input: Arc::clone(&input),
            })
        })) {
            Ok(original) => {
                state.records[self.index]
                    .as_mut()
                    .expect("committed original record")
                    .state = OriginalState::Running(original)
            }
            Err(actual) => {
                *input.cause.lock().unwrap_or_else(|e| e.into_inner()) = Some(actual);
                state.records[self.index]
                    .as_mut()
                    .expect("committed original record")
                    .state = OriginalState::SpawnUnjoinable;
                state.failures.spawn_unjoinable = true;
                reject_record(
                    state.records[self.index]
                        .as_ref()
                        .expect("same original failed spawn"),
                );
                drop(state);
                self.core.changed.notify_waiters();
                return Err(RegistrationError::SpawnUnjoinable);
            }
        }
        drop(state);
        self.core.changed.notify_waiters();
        Ok(SubscriptionLease {
            core: Arc::clone(&self.core),
            index: self.index,
            generation: self.generation,
        })
    }
}
impl Drop for SubscriptionReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        let record = state.records[self.index]
            .as_ref()
            .filter(|r| r.generation == self.generation)
            .expect("same original reservation");
        assert!(
            matches!(record.state, OriginalState::Reserved),
            "only empty pre-spawn reservation Drop can release"
        );
        let record = state.records[self.index]
            .take()
            .expect("same original reservation");
        record
            .backing
            .retired
            .store(true, std::sync::atomic::Ordering::Release);
        drop(state);
        drop(record);
        self.core.changed.notify_waiters();
    }
}

struct ReaperExitGuard {
    core: Arc<Core>,
}
impl Drop for ReaperExitGuard {
    fn drop(&mut self) {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.closed || state.records.iter().any(Option::is_some) {
            state.failures.reaper_exited_early = true;
            state.closed = true;
            for record in state.records.iter_mut().flatten() {
                reject_record(record);
                record.stop_requested = true;
                if let OriginalState::Running(handle) = &record.state {
                    handle.abort();
                }
            }
        }
        drop(state);
        self.core.changed.notify_waiters();
    }
}
fn reject_record(record: &Record) {
    if let Some(state) = &record.failure_state {
        *state.lock().unwrap_or_else(|e| e.into_inner()) = SubscriptionState::Rejected;
    }
}
fn add_cleanup_failures(facts: &mut FailureFacts, delta: u64) {
    match facts.cleanup_failures.checked_add(delta) {
        Some(value) => facts.cleanup_failures = value,
        None => facts.counter_overflow = true,
    }
}
async fn reap_originals(core: Arc<Core>) {
    loop {
        let notified = core.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let settled = std::future::poll_fn(|cx| {
            let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
            for index in 0..state.records.len() {
                let Some(record) = state.records[index].as_mut() else {
                    continue;
                };
                let mut actual = None;
                let mut failures = 0;
                let done = match &mut record.state {
                    OriginalState::Running(original) => {
                        if let Poll::Ready(joined) = Pin::new(original).poll(cx) {
                            actual = Some(joined);
                        }
                        false
                    }
                    OriginalState::Retiring(retirement) => {
                        let before = retirement.failures_so_far();
                        let done = retirement.poll(&core.handle, cx).is_ready();
                        failures = retirement.failures_so_far().saturating_sub(before);
                        done
                    }
                    OriginalState::Reserved | OriginalState::SpawnUnjoinable => false,
                };
                add_cleanup_failures(&mut state.failures, failures);
                if failures != 0 {
                    reject_record(
                        state.records[index]
                            .as_ref()
                            .expect("same original failed retirement"),
                    );
                }
                if let Some(joined) = actual {
                    let record = state.records[index]
                        .as_mut()
                        .expect("original joined record");
                    let failure = match joined {
                        Ok(()) => None,
                        Err(original) => {
                            let facts = SubscriptionJoinFacts::of(&original);
                            let unexpected =
                                facts.panic || !record.stop_requested || !facts.cancelled;
                            // Keep even cancellation-only original JoinError in
                            // the same protected destructor retirement.
                            *record.input.cause.lock().unwrap_or_else(|e| e.into_inner()) =
                                Some(Box::new(original));
                            unexpected.then_some(facts)
                        }
                    };
                    record.state = OriginalState::Retiring(SubscriptionRetirement::new(
                        &record.input,
                        Arc::clone(&record.backing),
                    ));
                    if let Some(failure) = failure {
                        reject_record(
                            state.records[index]
                                .as_ref()
                                .expect("same original failed join"),
                        );
                        state.failures.original.get_or_insert(failure);
                    }
                    cx.waker().wake_by_ref();
                }
                if done {
                    let original = state.records[index]
                        .take()
                        .expect("retired original record");
                    original
                        .backing
                        .retired
                        .store(true, std::sync::atomic::Ordering::Release);
                    drop(state);
                    return Poll::Ready((Some(original), false));
                }
            }
            let done = state.closed && state.records.iter().all(Option::is_none);
            drop(state);
            if done {
                Poll::Ready((None, true))
            } else {
                Poll::Pending
            }
        });
        tokio::pin!(settled);
        tokio::select! {
            outcome = &mut settled => { let (original, done) = outcome; drop(original); if done { return; } tokio::task::yield_now().await; },
            _ = &mut notified => {},
        }
    }
}

#[cfg(test)]
#[path = "subscription_owner_tests.rs"]
mod tests;
