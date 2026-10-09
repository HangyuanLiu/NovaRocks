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

//! Supervision for blocking Connector calls made by the frontend.
//!
//! Connector implementations may expose synchronous split enumeration and
//! credential vending. A submitted call keeps running on Tokio's blocking pool
//! when its query waiter is dropped. The worker publishes its actual outcome
//! only after the synchronous call returns. Query admission, rather than a
//! second Connector permit, controls whether the call may be submitted.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_workload_control::{
    ResultWindowAlias, WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope,
};

use tokio::runtime::Handle;

/// Why a submitted blocking call produced no Connector outcome.
#[derive(Clone, Debug)]
pub(crate) struct ConnectorBlockingIoError {
    detail: String,
    original: Arc<OriginalFailure>,
}

struct OriginalFailure {
    cause: OriginalFailureCause,
    // The original panic payload is destroyed before its backing responsibility.
    _backing: Box<[ConnectorBlockingIoBacking]>,
}

enum OriginalFailureCause {
    WorkerJoin(tokio::task::JoinError),
    OutcomeDrop(#[allow(dead_code)] Mutex<Box<dyn std::any::Any + Send>>),
}

impl fmt::Debug for OriginalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OriginalConnectorBlockingFailure")
    }
}

impl fmt::Display for ConnectorBlockingIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ConnectorBlockingIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.original.cause {
            OriginalFailureCause::WorkerJoin(error) => Some(error),
            OriginalFailureCause::OutcomeDrop(_) => None,
        }
    }
}

/// One source/call responsibility, derived before dispatch from the original
/// admitted scope. The existing scope-record bound limits these cells; this
/// does not acquire a second business, execution or Connector permit.
pub(crate) struct ConnectorBlockingIoResponsibility(Arc<ResponsibilityCell>);

struct ResponsibilityCell {
    owner: Option<WorkOwner>,
    pending_joins: AtomicUsize,
    _window: ResultWindowAlias,
}

impl Drop for ResponsibilityCell {
    fn drop(&mut self) {
        let owner = self
            .owner
            .take()
            .expect("one Connector responsibility owner");
        if *self.pending_joins.get_mut() == 0 {
            // This private child owns no receiver or remote cancellation
            // delivery. Its last backing alias and every original join have
            // exited, so its own pending Cancel is now obsolete.
            owner.complete_after_terminal_cancel_settled();
        } else {
            // A publisher aborted before awaiting its original blocking job
            // has no exit proof. WorkOwner::drop records an orphan, keeping
            // Host drain fail-closed instead of inventing a successful join.
            drop(owner);
        }
    }
}

impl ConnectorBlockingIoResponsibility {
    pub(crate) fn admit(scope: &WorkScope, window: &ResultWindowAlias) -> Result<Self, WorkError> {
        if !window.is_for_scope(scope) {
            return Err(WorkError::Conflict);
        }
        let owner = scope.child(WorkRequest::new(WorkClass::Query))?;
        let delegated = match window.for_child(&owner.scope()) {
            Ok(window) => window,
            Err(error) => {
                // No call, source or receiver was installed for this child.
                owner.complete_after_terminal_cancel_settled();
                return Err(error);
            }
        };
        Ok(Self(Arc::new(ResponsibilityCell {
            owner: Some(owner),
            pending_joins: AtomicUsize::new(0),
            _window: delegated,
        })))
    }

    /// Fixed backing alias, retained after all other fields of an enclosing
    /// source/result have been destroyed. It cannot authorize another job.
    pub(crate) fn retain_backing(&self) -> ConnectorBlockingIoBacking {
        ConnectorBlockingIoBacking(Arc::clone(&self.0))
    }

    pub(crate) fn join_pin(&self) -> ConnectorBlockingIoJoinPin {
        ConnectorBlockingIoJoinPin::new(&self.0)
    }
}

pub(crate) struct ConnectorBlockingIoBacking(#[allow(dead_code)] Arc<ResponsibilityCell>);

/// Remains outside the panic-able synchronous closure. Only the actual await
/// of that original JoinHandle may mark it joined, including a JoinError.
pub(crate) struct ConnectorBlockingIoJoinPin {
    cell: Arc<ResponsibilityCell>,
    joined: AtomicBool,
}

impl ConnectorBlockingIoJoinPin {
    fn new(cell: &Arc<ResponsibilityCell>) -> Self {
        cell.pending_joins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .expect("finite original Connector joins cannot overflow");
        Self {
            cell: Arc::clone(cell),
            joined: AtomicBool::new(false),
        }
    }
}

impl Drop for ConnectorBlockingIoJoinPin {
    fn drop(&mut self) {
        if self.joined.load(Ordering::Acquire) {
            let previous = self.cell.pending_joins.fetch_sub(1, Ordering::AcqRel);
            assert!(previous != 0, "original Connector join pin underflow");
        }
    }
}

struct JobOutcome<T: Send + 'static> {
    value: Mutex<Option<Result<T, ConnectorBlockingIoError>>>,
    // Unclaimed output retirement receives the same cells before these pins
    // retire. Its own original join keeps the backing through actual cleanup.
    pins: Box<[ConnectorBlockingIoJoinPin]>,
    retirement: ConnectorBlockingIoSupervisor,
}

impl<T: Send + 'static> Drop for JobOutcome<T> {
    fn drop(&mut self) {
        let outcome = self
            .value
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        match outcome {
            Some(Err(error)) => self.retirement.publish_original_failure(error),
            Some(Ok(value)) if std::mem::needs_drop::<T>() => {
                let pins = self
                    .pins
                    .iter()
                    .map(|pin| ConnectorBlockingIoJoinPin::new(&pin.cell))
                    .collect();
                self.retirement.spawn_cleanup(
                    pins,
                    move || drop(value),
                    "connector blocking-I/O unclaimed outcome panicked during cleanup",
                );
            }
            Some(Ok(value)) => drop(value),
            None => {}
        }
    }
}

/// A submitted call whose result can be polled by an existing serial owner.
pub(crate) struct ConnectorBlockingIoJob<T: Send + 'static> {
    outcome: Arc<JobOutcome<T>>,
    ready: Arc<tokio::sync::Notify>,
}

impl<T: Send + 'static> ConnectorBlockingIoJob<T> {
    pub(crate) fn try_take(&self) -> Option<Result<T, ConnectorBlockingIoError>> {
        self.outcome
            .value
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    /// Waits asynchronously for the submitted call to publish its outcome.
    ///
    /// The polling accessor remains useful to serial owners such as credential
    /// rotation. Split assignment uses this form to wake its serial round only
    /// after the blocking worker has really returned.
    pub(crate) async fn finish(self) -> Result<T, ConnectorBlockingIoError> {
        loop {
            let notified = self.ready.notified();
            if let Some(outcome) = self.try_take() {
                return outcome;
            }
            notified.await;
        }
    }
}

/// The one process owner that supervises frontend Connector blocking calls.
#[derive(Clone)]
pub(crate) struct ConnectorBlockingIoSupervisor {
    runtime: Handle,
    failure: Arc<Mutex<Option<ConnectorBlockingIoError>>>,
    failure_ready: Arc<tokio::sync::Notify>,
}

impl ConnectorBlockingIoSupervisor {
    /// Derive the job's responsibility from the caller's exact admission
    /// before submitting any synchronous provider code.
    pub(crate) fn spawn_admitted<T, F>(
        &self,
        scope: &WorkScope,
        window: &ResultWindowAlias,
        call: F,
    ) -> Result<ConnectorBlockingIoJob<T>, WorkError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let responsibility = ConnectorBlockingIoResponsibility::admit(scope, window)?;
        let pin = responsibility.join_pin();
        Ok(self.spawn_pinned(vec![pin], move || {
            let _responsibility = responsibility;
            call()
        }))
    }

    /// A claimed lane error may cross a String-only application port. Move its
    /// original payload into protected retirement before returning that finite
    /// presentation, so caller cancellation cannot destroy it on the async lane.
    pub(crate) fn present_and_retire_failure(&self, error: ConnectorBlockingIoError) -> String {
        let detail = error.to_string();
        self.retire_original_failure(error);
        detail
    }

    pub(crate) fn new(runtime: Handle) -> Self {
        Self {
            runtime,
            failure: Arc::new(Mutex::new(None)),
            failure_ready: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// The runtime this lane's work is admitted onto.
    ///
    /// Statement preparation runs on its own threads, not on this runtime, so
    /// it needs the handle to await anything that reaches the lane.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }

    /// Submit credential or lifecycle work and retain its actual completion.
    #[cfg(test)]
    pub(crate) fn spawn_protected<T, F>(&self, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_pinned(Vec::new(), call)
    }

    /// Submit ordinary split-source work.
    ///
    /// Only the synchronous Connector call belongs inside `call`; transport
    /// acknowledgement and retry waits must run after this job has finished so
    /// the blocking worker owns only the actual Connector call.
    #[cfg(test)]
    pub(crate) fn spawn_ordinary<T, F>(&self, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_pinned(Vec::new(), call)
    }

    pub(crate) fn spawn_pinned<T, F>(
        &self,
        pins: Vec<ConnectorBlockingIoJoinPin>,
        call: F,
    ) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let runtime = self.runtime.clone();
        let outcome = Arc::new(JobOutcome {
            value: Mutex::new(None),
            pins: pins.into_boxed_slice(),
            retirement: self.clone(),
        });
        let published = Arc::clone(&outcome);
        let ready = Arc::new(tokio::sync::Notify::new());
        let publish_ready = Arc::clone(&ready);
        self.runtime.spawn(async move {
            let joined = runtime.spawn_blocking(call).await;
            for pin in &published.pins {
                pin.joined.store(true, Ordering::Release);
            }
            let completed = joined.map_err(|error| ConnectorBlockingIoError {
                detail: if error.is_panic() {
                    "connector blocking-I/O worker panicked"
                } else {
                    "connector blocking-I/O worker was cancelled"
                }
                .to_owned(),
                original: Arc::new(OriginalFailure {
                    cause: OriginalFailureCause::WorkerJoin(error),
                    _backing: published
                        .pins
                        .iter()
                        .map(|pin| ConnectorBlockingIoBacking(Arc::clone(&pin.cell)))
                        .collect(),
                }),
            });
            *published
                .value
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(completed);
            // Each job has exactly one consuming waiter. A stored single
            // notification token also covers completion before `finish` registers, while
            // `notify_waiters` would lose that notification.
            publish_ready.notify_one();
        });
        ConnectorBlockingIoJob { outcome, ready }
    }

    /// Move the original unclaimed failure out of the fixed process slot.
    /// Host teardown records its finite verdict, destroys the actual payload
    /// and its backing aliases, and then drains the same workload authority.
    pub(crate) fn take_original_failure(&self) -> Option<ConnectorBlockingIoError> {
        self.failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    /// Payload destructors are provider code too. Retire the original failure
    /// on the same protected blocking lane, preserving each original scope
    /// through that destructor's actual JoinHandle without new admission.
    pub(crate) fn retire_original_failure(&self, error: ConnectorBlockingIoError) {
        let pins = error
            .original
            ._backing
            .iter()
            .map(|backing| ConnectorBlockingIoJoinPin::new(&backing.0))
            .collect();
        self.spawn_cleanup(
            pins,
            move || drop(error),
            "connector blocking-I/O original failure panicked during cleanup",
        );
    }

    fn publish_original_failure(&self, error: ConnectorBlockingIoError) {
        let rejected = {
            let mut first = self
                .failure
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if first.is_none() {
                *first = Some(error);
                None
            } else {
                Some(error)
            }
        };
        // A secondary provider payload must not run on a dropped waiter's
        // thread or under the first-failure lock.
        if let Some(error) = rejected {
            self.retire_original_failure(error);
        }
        self.failure_ready.notify_one();
    }

    /// A cleanup has no consuming waiter or unclaimed output. Observe its
    /// original blocking join directly, avoiding a new cleanup for a unit
    /// output and preserving the raw panic payload if provider Drop panics.
    fn spawn_cleanup<F>(
        &self,
        pins: Vec<ConnectorBlockingIoJoinPin>,
        cleanup: F,
        panic_detail: &'static str,
    ) where
        F: FnOnce() + Send + 'static,
    {
        let runtime = self.runtime.clone();
        let retirement = self.clone();
        self.runtime.spawn(async move {
            let joined = runtime
                .spawn_blocking(move || {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(cleanup))
                })
                .await;
            for pin in &pins {
                pin.joined.store(true, Ordering::Release);
            }
            let cause = match joined {
                Ok(Ok(())) => None,
                Ok(Err(payload)) => Some(OriginalFailureCause::OutcomeDrop(Mutex::new(payload))),
                Err(error) => Some(OriginalFailureCause::WorkerJoin(error)),
            };
            if let Some(cause) = cause {
                retirement.publish_original_failure(ConnectorBlockingIoError {
                    detail: panic_detail.to_owned(),
                    original: Arc::new(OriginalFailure {
                        cause,
                        _backing: pins
                            .iter()
                            .map(|pin| ConnectorBlockingIoBacking(Arc::clone(&pin.cell)))
                            .collect(),
                    }),
                });
            }
            // The actual original cleanup join has returned before these
            // pins retire, including a failed cleanup's raw payload handoff.
            drop(pins);
        });
    }

    pub(crate) async fn wait_failure(&self) {
        loop {
            let ready = self.failure_ready.notified();
            if self
                .failure
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_some()
            {
                return;
            }
            ready.await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    pub(crate) fn admitted() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        novarocks_workload_control::ResultWindowGrant,
    ) {
        admitted_class(novarocks_workload_control::ResultWindowClass::Internal)
    }

    pub(crate) fn admitted_class(
        class: novarocks_workload_control::ResultWindowClass,
    ) -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        novarocks_workload_control::ResultWindowGrant,
    ) {
        use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig, WorkloadControl};
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .expect("workload")
            .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .expect("capacity");
        control.mark_ready().expect("ready");
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(WorkRequest::new(WorkClass::Management), class)
            .expect("original admitted work");
        (control, root, window)
    }

    pub(crate) fn until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(Instant::now() < deadline, "original owner did not converge");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub(crate) fn settle_control(control: &novarocks_workload_control::WorkloadControl) {
        while let Some(permit) = control.next_control() {
            permit.acknowledge();
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .expect("runtime")
    }

    fn wait<T: Send + 'static>(
        job: &ConnectorBlockingIoJob<T>,
    ) -> Result<T, ConnectorBlockingIoError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(outcome) = job.try_take() {
                return outcome;
            }
            assert!(Instant::now() < deadline, "blocking-I/O job did not finish");
            std::thread::yield_now();
        }
    }

    #[test]
    fn original_work_and_window_survive_a_dropped_waiter_and_control_ack() {
        use novarocks_workload_control::{CancellationReason, OwnerState};
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let scope = root.owner.scope();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&scope, &window.retain_alias())
                .expect("source responsibility");
        let pin = responsibility.join_pin();
        let (release, released) = mpsc::channel();
        let (started, entered) = mpsc::channel();
        let job = supervisor.spawn_pinned(vec![pin], move || {
            let _responsibility = responsibility;
            started.send(()).expect("start");
            released.recv().expect("release original call");
        });
        entered
            .recv_timeout(Duration::from_secs(2))
            .expect("entered");
        root.owner.cancel(CancellationReason::Requested);
        root.owner.complete();
        root.business.release();
        drop(window);
        drop(job);
        settle_control(&control);
        let held = control.snapshot();
        release.send(()).expect("release");
        until(|| {
            settle_control(&control);
            control.snapshot().scopes.is_empty()
        });
        assert_eq!(held.root_responsibilities, 1);
        assert_eq!(held.result_windows.held_positions, [0, 0, 1, 0]);
        assert!(
            held.scopes
                .iter()
                .any(|child| child.parent == Some(scope.id()) && child.owner == OwnerState::Active)
        );
        assert!(supervisor.take_original_failure().is_none());
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn an_unclaimed_outcome_keeps_its_original_join_and_backing() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .expect("responsibility");
        let pin = responsibility.join_pin();
        let job = supervisor.spawn_pinned(vec![pin], move || {
            drop(responsibility);
            7u8
        });
        root.owner.complete();
        root.business.release();
        drop(window);
        until(|| job.outcome.value.lock().unwrap().is_some());
        let held = control.snapshot();
        drop(job);
        until(|| control.snapshot().scopes.is_empty());
        assert_eq!(held.result_windows.held_positions, [0, 0, 1, 0]);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn a_retained_original_panic_payload_keeps_its_backing_until_last_alias() {
        struct Payload(std::sync::Arc<AtomicBool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .expect("responsibility");
        let pin = responsibility.join_pin();
        let destroyed = Arc::new(AtomicBool::new(false));
        let payload = Payload(Arc::clone(&destroyed));
        let job = supervisor.spawn_pinned(vec![pin], move || {
            let _responsibility = responsibility;
            std::panic::panic_any(payload);
        });
        until(|| job.outcome.value.lock().unwrap().is_some());
        drop(job);
        until(|| supervisor.failure.lock().unwrap().is_some());
        let error = supervisor.take_original_failure().expect("original panic");
        root.owner.complete();
        root.business.release();
        drop(window);
        let held = control.snapshot();
        let payload_held = !destroyed.load(Ordering::Acquire);
        drop(supervisor);
        let last_alias_held = !destroyed.load(Ordering::Acquire);
        drop(error);
        until(|| control.snapshot().scopes.is_empty());
        assert!(payload_held && last_alias_held);
        assert!(destroyed.load(Ordering::Acquire));
        assert_eq!(held.result_windows.held_positions, [0, 0, 1, 0]);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn runtime_drop_before_original_join_cannot_complete_work() {
        use novarocks_workload_control::OwnerState;
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .expect("responsibility");
        let pin = responsibility.join_pin();
        let (release, released) = mpsc::channel();
        let (started, entered) = mpsc::channel();
        let (exited, exit) = mpsc::channel();
        let job = supervisor.spawn_pinned(vec![pin], move || {
            let _responsibility = responsibility;
            started.send(()).expect("start");
            released.recv().expect("release call");
            drop(_responsibility);
            exited.send(()).expect("actual exit");
        });
        entered
            .recv_timeout(Duration::from_secs(2))
            .expect("entered");
        drop(job);
        root.owner.complete();
        root.business.release();
        drop(window);
        runtime.shutdown_background();
        let held = control.snapshot();
        release.send(()).expect("release original call");
        exit.recv_timeout(Duration::from_secs(2))
            .expect("actual exit");
        until(|| {
            control
                .snapshot()
                .scopes
                .iter()
                .any(|scope| scope.owner == OwnerState::Orphaned)
        });
        assert_eq!(held.root_responsibilities, 1);
        assert!(
            !control
                .snapshot()
                .scopes
                .iter()
                .any(|scope| scope.parent.is_some() && scope.own_completed)
        );
        control.close_admission();
        assert!(
            control.shutdown().is_err(),
            "unobserved join must fail closed"
        );
    }

    #[test]
    fn adopting_a_failure_does_not_hide_another_unclaimed_failure() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let claimed = supervisor.spawn_ordinary(|| panic!("claimed failure"));
        until(|| claimed.outcome.value.lock().unwrap().is_some());
        let unclaimed = supervisor.spawn_protected(|| panic!("unclaimed close failure"));
        until(|| unclaimed.outcome.value.lock().unwrap().is_some());
        drop(unclaimed);
        until(|| supervisor.failure.lock().unwrap().is_some());
        let claimed_error = wait(&claimed).expect_err("claimed verdict");
        let orphan_error = supervisor
            .take_original_failure()
            .expect("unclaimed original verdict remains");
        let distinct_originals = !Arc::ptr_eq(&claimed_error.original, &orphan_error.original);
        drop(claimed);
        drop(claimed_error);
        drop(orphan_error);
        assert!(distinct_originals);
        assert!(supervisor.take_original_failure().is_none());
    }

    #[test]
    fn unclaimed_outcome_drop_panic_keeps_the_actual_payload_and_window() {
        struct Payload(Arc<AtomicBool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        struct Outcome {
            _responsibility: ConnectorBlockingIoResponsibility,
            destroyed: Arc<AtomicBool>,
        }
        impl Drop for Outcome {
            fn drop(&mut self) {
                std::panic::panic_any(Payload(Arc::clone(&self.destroyed)));
            }
        }
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .unwrap();
        let pin = responsibility.join_pin();
        let destroyed = Arc::new(AtomicBool::new(false));
        let outcome = Outcome {
            _responsibility: responsibility,
            destroyed: Arc::clone(&destroyed),
        };
        let job = supervisor.spawn_pinned(vec![pin], move || outcome);
        root.owner.complete();
        root.business.release();
        drop(window);
        drop(job);
        until(|| supervisor.failure.lock().unwrap().is_some());
        let held = control.snapshot();
        let payload_held = !destroyed.load(Ordering::Acquire);
        let original = supervisor.take_original_failure().unwrap();
        let has_cleanup_verdict = original.to_string().contains("during cleanup");
        drop(original);
        until(|| control.snapshot().scopes.is_empty());
        assert!(payload_held && has_cleanup_verdict);
        assert!(destroyed.load(Ordering::Acquire));
        assert_eq!(held.result_windows.held_positions, [0, 0, 1, 0]);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn local_source_terminal_settles_its_own_cancel_without_host_shutdown() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) = admitted();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .unwrap();
        let pin = responsibility.join_pin();
        let (release, held) = mpsc::channel();
        let (started, entered) = mpsc::channel();
        let job = supervisor.spawn_pinned(vec![pin], move || {
            let _responsibility = responsibility;
            started.send(()).unwrap();
            held.recv().unwrap();
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        root.owner
            .cancel(novarocks_workload_control::CancellationReason::Requested);
        control.expire_deadlines();
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        drop(window);
        let retained = control.snapshot();
        let joined = Arc::downgrade(&job.outcome);
        drop(job);
        release.send(()).unwrap();
        until(|| {
            joined.upgrade().is_none() && control.snapshot().result_windows.held_positions == [0; 4]
        });
        let actual_terminal = control.snapshot();
        // Settle any remaining notifications before the assertions, so the
        // former orphaned-notification path is an exited, reproducible FAIL.
        settle_control(&control);
        control.close_admission();
        assert!(control.shutdown().is_ok());
        assert_eq!(retained.root_responsibilities, 1);
        assert_eq!(retained.result_windows.held_positions, [0, 0, 1, 0]);
        assert!(
            actual_terminal.scopes.is_empty(),
            "actual source join/backing exit left its local Cancel waiting for Host shutdown: {:?}",
            actual_terminal.scopes
        );
    }

    #[test]
    fn a_claimed_failure_retires_its_original_payload_outside_the_caller() {
        struct Payload {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            destroyed: Arc<AtomicBool>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.started.send(()).unwrap();
                self.release.recv().unwrap();
                self.destroyed.store(true, Ordering::Release);
            }
        }
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) =
            admitted_class(novarocks_workload_control::ResultWindowClass::Local);
        let (release, held) = mpsc::channel();
        let (started, entered) = mpsc::channel();
        let destroyed = Arc::new(AtomicBool::new(false));
        let payload = Payload {
            started,
            release: held,
            destroyed: Arc::clone(&destroyed),
        };
        let job = supervisor
            .spawn_admitted(&root.owner.scope(), &window.retain_alias(), move || {
                std::panic::panic_any(payload);
            })
            .unwrap();
        let error = wait(&job).expect_err("original worker panic");
        drop(job);
        root.owner.complete();
        root.business.release();
        drop(window);
        let (disarm, watch) = mpsc::channel();
        let rescue = release.clone();
        let watchdog = std::thread::spawn(move || {
            if watch.recv_timeout(Duration::from_millis(500)).is_err() {
                let _ = rescue.send(());
            }
        });
        let began = Instant::now();
        let detail = supervisor.present_and_retire_failure(error);
        let elapsed = began.elapsed();
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let retained = control.snapshot();
        let _ = release.send(());
        let _ = disarm.send(());
        watchdog.join().unwrap();
        until(|| control.snapshot().scopes.is_empty());
        control.close_admission();
        assert!(control.shutdown().is_ok());
        assert_eq!(detail, "connector blocking-I/O worker panicked");
        assert!(
            elapsed < Duration::from_millis(500),
            "claimed payload blocked its caller: {elapsed:?}"
        );
        assert_eq!(retained.result_windows.held_positions, [0, 1, 0, 0]);
        assert_eq!(retained.root_responsibilities, 1);
        assert!(destroyed.load(Ordering::Acquire));
    }

    #[test]
    fn admitted_provider_work_refuses_foreign_window_and_scope_record_saturation() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let (first, first_root, first_window) = admitted();
        let (second, second_root, second_window) = admitted();
        let observed = Arc::clone(&calls);
        let refused = supervisor.spawn_admitted(
            &first_root.owner.scope(),
            &second_window.retain_alias(),
            move || {
                observed.fetch_add(1, Ordering::AcqRel);
            },
        );
        assert!(matches!(refused, Err(WorkError::Conflict)));
        for (control, root, window) in [
            (first, first_root, first_window),
            (second, second_root, second_window),
        ] {
            root.owner.complete();
            root.business.release();
            drop(window);
            control.close_admission();
            assert!(control.shutdown().is_ok());
        }
        let control = novarocks_workload_control::WorkloadControl::try_new_counted(
            novarocks_workload_control::WorkloadConfig {
                scope_records_limit: 1,
                root_limit: 1,
                business_limit: 1,
                ..Default::default()
            },
        )
        .unwrap()
        .owner;
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                novarocks_workload_control::ResultWindowClass::Local,
            )
            .unwrap();
        let observed = Arc::clone(&calls);
        let refused =
            supervisor.spawn_admitted(&root.owner.scope(), &window.retain_alias(), move || {
                observed.fetch_add(1, Ordering::AcqRel);
            });
        assert!(matches!(refused, Err(WorkError::Capacity(_))));
        root.owner.complete();
        root.business.release();
        drop(window);
        control.close_admission();
        assert!(control.shutdown().is_ok());
        assert_eq!(
            calls.load(Ordering::Acquire),
            0,
            "refused provider code ran"
        );
    }

    #[test]
    fn a_rejected_secondary_failure_retires_outside_the_dropped_waiter() {
        struct Payload {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            destroyed: Arc<AtomicBool>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.started.send(()).unwrap();
                self.release.recv().unwrap();
                self.destroyed.store(true, Ordering::Release);
            }
        }
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let first = supervisor.spawn_protected(|| panic!("first unclaimed failure"));
        drop(first);
        until(|| supervisor.failure.lock().unwrap().is_some());
        let (control, root, window) =
            admitted_class(novarocks_workload_control::ResultWindowClass::Local);
        let destroyed = Arc::new(AtomicBool::new(false));
        let (started, entered) = mpsc::channel();
        let (release, held) = mpsc::channel();
        let payload = Payload {
            started,
            release: held,
            destroyed: Arc::clone(&destroyed),
        };
        let job = supervisor
            .spawn_admitted(&root.owner.scope(), &window.retain_alias(), move || {
                std::panic::panic_any(payload);
            })
            .unwrap();
        // The original blocking call has really joined and its publisher no
        // longer owns this outcome. Dropping the waiter now owns raw cleanup.
        until(|| {
            Arc::strong_count(&job.outcome) == 1 && job.outcome.value.lock().unwrap().is_some()
        });
        root.owner.complete();
        root.business.release();
        drop(window);
        let (disarm, watch) = mpsc::channel();
        let rescue = release.clone();
        let watchdog = std::thread::spawn(move || {
            if watch.recv_timeout(Duration::from_millis(500)).is_err() {
                let _ = rescue.send(());
            }
        });
        let began = Instant::now();
        drop(job);
        let elapsed = began.elapsed();
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let retained = control.snapshot();
        let _ = release.send(());
        let _ = disarm.send(());
        watchdog.join().unwrap();
        until(|| control.snapshot().scopes.is_empty());
        let first = supervisor.take_original_failure().unwrap();
        assert!(supervisor.take_original_failure().is_none());
        drop(first);
        control.close_admission();
        assert!(control.shutdown().is_ok());
        assert!(
            elapsed < Duration::from_millis(500),
            "secondary original failure blocked the dropped waiter: {elapsed:?}"
        );
        assert_eq!(retained.result_windows.held_positions, [0, 1, 0, 0]);
        assert_eq!(retained.root_responsibilities, 1);
        assert!(destroyed.load(Ordering::Acquire));
    }

    #[test]
    fn an_unclaimed_output_retires_outside_the_dropped_waiter() {
        struct Payload {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            destroyed: Arc<AtomicBool>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.started.send(()).unwrap();
                self.release.recv().unwrap();
                self.destroyed.store(true, Ordering::Release);
            }
        }
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (control, root, window) =
            admitted_class(novarocks_workload_control::ResultWindowClass::Local);
        let destroyed = Arc::new(AtomicBool::new(false));
        let (started, entered) = mpsc::channel();
        let (release, held) = mpsc::channel();
        let payload = Payload {
            started,
            release: held,
            destroyed: Arc::clone(&destroyed),
        };
        let job = supervisor
            .spawn_admitted(&root.owner.scope(), &window.retain_alias(), move || payload)
            .unwrap();
        // The original blocking call has really joined and its publisher no
        // longer owns this outcome. Dropping the waiter now owns raw cleanup.
        until(|| {
            Arc::strong_count(&job.outcome) == 1 && job.outcome.value.lock().unwrap().is_some()
        });
        root.owner.complete();
        root.business.release();
        drop(window);
        let (disarm, watch) = mpsc::channel();
        let rescue = release.clone();
        let watchdog = std::thread::spawn(move || {
            if watch.recv_timeout(Duration::from_millis(500)).is_err() {
                let _ = rescue.send(());
            }
        });
        let began = Instant::now();
        drop(job);
        let elapsed = began.elapsed();
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let retained = control.snapshot();
        let _ = release.send(());
        let _ = disarm.send(());
        watchdog.join().unwrap();
        until(|| control.snapshot().scopes.is_empty());
        assert!(supervisor.take_original_failure().is_none());
        control.close_admission();
        assert!(control.shutdown().is_ok());
        assert!(
            elapsed < Duration::from_millis(500),
            "unclaimed output blocked the dropped waiter: {elapsed:?}"
        );
        assert_eq!(retained.result_windows.held_positions, [0, 1, 0, 0]);
        assert_eq!(retained.root_responsibilities, 1);
        assert!(destroyed.load(Ordering::Acquire));
    }

    #[test]
    fn another_admitted_call_starts_while_the_first_is_held() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (release, released) = mpsc::channel();
        let (started, first_started) = mpsc::channel();
        let first = supervisor.spawn_ordinary(move || {
            started.send(()).expect("publish first start");
            released.recv().expect("release first call");
        });
        first_started
            .recv_timeout(Duration::from_secs(2))
            .expect("first call did not start");

        let second = supervisor.spawn_ordinary(|| 7_u8);
        assert_eq!(wait(&second).expect("second outcome"), 7);
        assert!(
            first.try_take().is_none(),
            "held call must still be running"
        );
        release.send(()).expect("release first call");
        wait(&first).expect("first outcome");
    }

    #[test]
    fn dropping_waiter_does_not_end_the_blocking_call() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let (release, released) = mpsc::channel();
        let (started, first_started) = mpsc::channel();
        let (exited, observed_exit) = mpsc::channel();
        let job = supervisor.spawn_protected(move || {
            started.send(()).expect("publish start");
            released.recv().expect("release call");
            exited.send(()).expect("publish actual exit");
        });
        first_started
            .recv_timeout(Duration::from_secs(2))
            .expect("call did not start");
        drop(job);
        assert_eq!(observed_exit.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.send(()).expect("release call");
        observed_exit
            .recv_timeout(Duration::from_secs(2))
            .expect("blocking call did not really exit");
    }

    #[test]
    fn async_finish_observes_completion_published_before_it_waits() {
        let runtime = runtime();
        let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
        let job = supervisor.spawn_ordinary(|| 17_u8);
        let deadline = Instant::now() + Duration::from_secs(2);
        while job
            .outcome
            .value
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
        {
            assert!(Instant::now() < deadline, "job did not publish completion");
            std::thread::yield_now();
        }

        assert_eq!(runtime.block_on(job.finish()).expect("finished job"), 17);
    }
}

#[cfg(test)]
#[path = "blocking_io/opaque_split_drop_tests.rs"]
mod opaque_split_drop_tests;
