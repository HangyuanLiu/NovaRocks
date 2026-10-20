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

use super::*;
use crate::api::{
    NativeAttemptConvergence, NativeContextConvergence, StatementNativeConvergenceFuture,
    StatementNativeLocalExit, StatementNativeRemoteClosure,
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryId};
use novarocks_workload_control::{
    ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Signal {
    local_exited: AtomicBool,
    remote_closed: AtomicBool,
    calls: AtomicUsize,
    dropped: AtomicUsize,
    observed_address: AtomicUsize,
    shutdowns: AtomicUsize,
    changed: Notify,
}
impl Signal {
    fn local_exit(&self) {
        self.local_exited.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
    fn remote_close(&self) {
        self.remote_closed.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}
struct FixtureOwner {
    execution: QueryExecutionId,
    scope: WorkId,
    contexts: Box<[QueryContextRef]>,
    local: [StatementNativeLocalHolder; 2],
    signal: Arc<Signal>,
    panic_payload: Option<PanicPayload>,
    unresolved: Option<StatementNativeHealthFailure>,
}
struct PanicPayload(Arc<AtomicUsize>);
impl Drop for PanicPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for FixtureOwner {
    fn drop(&mut self) {
        self.signal.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl FixtureOwner {
    fn closure(&self, remote: StatementNativeRemoteClosure) -> StatementNativeClosure {
        StatementNativeClosure::after_actual_exit(
            self.execution,
            self.scope,
            remote,
            self.local
                .iter()
                .copied()
                .map(StatementNativeLocalExit::after_actual_exit)
                .collect::<Vec<_>>(),
        )
    }
    fn evidence(&self) -> StatementNativeClosure {
        self.closure(StatementNativeRemoteClosure::Evidence(
            NativeAttemptConvergence::contexts(
                self.contexts
                    .iter()
                    .copied()
                    .map(NativeContextConvergence::worker_stopped_and_context_fenced)
                    .collect::<Vec<_>>(),
            ),
        ))
    }
}
impl StatementNativeAttemptOwner for FixtureOwner {
    fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    fn contexts(&self) -> &[QueryContextRef] {
        &self.contexts
    }
    fn local_holders(&self) -> &[StatementNativeLocalHolder] {
        &self.local
    }
    fn request_shutdown_cleanup(&mut self) {
        self.signal.shutdowns.fetch_add(1, Ordering::SeqCst);
    }
    fn converge(
        &mut self,
        request: StatementNativeConvergenceRequest,
    ) -> StatementNativeConvergenceFuture<'_> {
        Box::pin(async move {
            assert_eq!(request.execution(), self.execution);
            assert_eq!(request.scope(), self.scope);
            self.signal.calls.fetch_add(1, Ordering::SeqCst);
            self.signal
                .observed_address
                .store(self as *const FixtureOwner as usize, Ordering::SeqCst);
            if let Some(payload) = self.panic_payload.take() {
                std::panic::panic_any(payload);
            }
            if let Some(failure) = self.unresolved.take() {
                return Err(failure);
            }
            loop {
                let changed = self.signal.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let local = self.signal.local_exited.load(Ordering::SeqCst);
                let remote = self.signal.remote_closed.load(Ordering::SeqCst);
                let expired = Instant::now() >= request.remote_observation_deadline();
                if local && remote {
                    return Ok(self.evidence());
                }
                if local && expired {
                    return Ok(self.closure(StatementNativeRemoteClosure::TrackingEnded {
                        exact_contexts: self.contexts.clone(),
                    }));
                }
                if expired {
                    changed.await;
                } else {
                    tokio::select! {
                        _ = &mut changed => {},
                        _ = tokio::time::sleep_until(request.remote_observation_deadline().into()) => {},
                    }
                }
            }
        })
    }
}
fn governance() -> (WorkloadControl, RootWork) {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: (1 << 20) - (1 << 12),
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    (control, root)
}
fn fixture(scope: &WorkScope, signal: Arc<Signal>) -> Box<FixtureOwner> {
    let execution =
        QueryExecutionId::new(QueryId::new(41, 43), AttemptId::new(1).unwrap()).unwrap();
    Box::new(FixtureOwner {
        execution,
        scope: scope.id(),
        contexts: vec![QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )]
        .into_boxed_slice(),
        local: [
            StatementNativeLocalHolder::RootPolls,
            StatementNativeLocalHolder::InternalCpu,
        ],
        signal,
        panic_payload: None,
        unresolved: None,
    })
}
fn registered(
    registry: &StatementNativeResidualRegistry,
    scope: &WorkScope,
    signal: Arc<Signal>,
    budget: Duration,
) -> StatementNativeForegroundLease<FixtureOwner> {
    let owner = fixture(scope, signal);
    registry
        .port(budget)
        .register(owner.execution, scope, owner)
        .unwrap()
}
async fn run_one(registry: &StatementNativeResidualRegistry) -> Result<(), QueryExecutionError> {
    let mut tasks = registry.ready_tasks();
    assert_eq!(tasks.len(), 1);
    tasks.pop().unwrap().await
}

#[test]
fn closed_registration_returns_same_unstarted_box_without_obligation() {
    let registry = StatementNativeResidualRegistry::new(301);
    let (_control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let owner = fixture(&scope, Arc::clone(&signal));
    let address = &*owner as *const FixtureOwner as usize;
    registry.close();
    let error = registry
        .port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap_err();
    assert_eq!(error.error, StatementNativeRegistrationError::Closed);
    assert_eq!(&*error.owner as *const FixtureOwner as usize, address);
    assert_eq!(signal.calls.load(Ordering::SeqCst), 0);
    assert_eq!(registry.len(), 0);
    root.owner.complete();
    root.business.release();
    assert!(
        scope.check().is_err(),
        "closed-before-register created no RunningWork record"
    );
}

#[test]
fn cancelled_registration_preserves_workload_fact_and_returns_original_unstarted_owner() {
    use novarocks_workload_control::{CancellationReason, WorkError};
    let registry = StatementNativeResidualRegistry::new(312);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let owner = fixture(&scope, Arc::clone(&signal));
    let address = &*owner as *const FixtureOwner as usize;
    let reason = CancellationReason::ExplicitKill {
        requester_connection_id: 71,
    };
    root.owner.cancel(reason.clone());
    let before = control.snapshot();
    let rejected = registry
        .port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap_err();
    assert_eq!(&*rejected.owner as *const FixtureOwner as usize, address);
    assert_eq!(
        rejected.error,
        StatementNativeRegistrationError::Governance(WorkError::Cancelled(reason.clone()))
    );
    let input = rejected.error.into_execution_failure_input();
    assert_eq!(input.fact(), WorkError::Cancelled(reason).failure_fact());
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        input.fact().evidence(),
        novarocks_execution_contract::failure::FailureEvidence::StopObservation
    );
    assert_eq!(registry.len(), 0);
    assert_eq!(control.snapshot().obligations, before.obligations);
    assert_eq!(signal.calls.load(Ordering::SeqCst), 0);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    drop(rejected.owner);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    // No Native dispatch occurred. The original product owner still owns and
    // supplies its unstarted terminal fact; error projection does not do it.
    assert_eq!(control.snapshot().root_responsibilities, 1);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert!(scope.cancellation().is_err());
}

#[test]
fn registration_metadata_capacity_preserves_workload_fact_and_original_obligation() {
    use novarocks_workload_control::WorkError;
    let registry = StatementNativeResidualRegistry::new(313);
    let control = WorkloadControl::try_new(
        WorkloadConfig {
            old_attempts_per_work: 1,
            old_attempts_limit: 1,
            unknown_creates_limit: 1,
            obligation_records_limit: 1,
            ..WorkloadConfig::default()
        },
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: (1 << 20) - (1 << 12),
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let original_obligation = scope
        .register_obligation(ObligationKey([0x5a; 32]), ObligationKind::RunningWork)
        .unwrap();
    let signal = Arc::new(Signal::default());
    let owner = fixture(&scope, Arc::clone(&signal));
    let address = &*owner as *const FixtureOwner as usize;
    let rejected = registry
        .port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap_err();
    assert_eq!(&*rejected.owner as *const FixtureOwner as usize, address);
    assert_eq!(
        rejected.error,
        StatementNativeRegistrationError::Governance(WorkError::Capacity("obligation records"))
    );
    let input = rejected.error.into_execution_failure_input();
    assert_eq!(
        input.fact(),
        WorkError::Capacity("obligation records").failure_fact()
    );
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        input.attempt_class(),
        crate::coordination::AttemptFailureClass::ResourceGovernance
    );
    assert_eq!(
        input.detail().as_str(),
        "statement Native responsibility registration was refused by workload control"
    );
    assert_eq!(
        control.snapshot().obligations,
        1,
        "the original record remains; failed registration minted none"
    );
    assert_eq!(registry.len(), 0);
    assert_eq!(signal.calls.load(Ordering::SeqCst), 0);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    drop(rejected.owner);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert!(
        original_obligation.resolve(),
        "only the original record's own completion fact settles it"
    );
    root.owner.complete();
    root.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert!(scope.cancellation().is_err());
}

#[tokio::test]
async fn normal_inline_completion_requires_exact_local_and_remote_facts() {
    let registry = StatementNativeResidualRegistry::new(302);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let mut lease = registered(
        &registry,
        &scope,
        Arc::clone(&signal),
        Duration::from_secs(5),
    );
    lease.begin_terminal_cut(Instant::now()).unwrap();
    let mut wrong = lease.borrow_mut().evidence();
    wrong.local = vec![StatementNativeLocalExit::after_actual_exit(
        StatementNativeLocalHolder::RootPolls,
    )]
    .into_boxed_slice();
    let rejected = lease.complete(wrong).unwrap_err();
    assert_eq!(
        rejected.error,
        StatementNativeCompletionError::InvalidLocalHolders
    );
    let mut lease = rejected.lease;
    assert_eq!(registry.len(), 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    root.owner.complete();
    root.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    let proof = lease.borrow_mut().evidence();
    lease.complete(proof).unwrap();
    assert_eq!(registry.len(), 0);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert!(
        registry.health().is_some(),
        "rejected exact-owner proof retains its health error"
    );
}

#[tokio::test]
async fn shutdown_after_registration_accepts_original_handoff_and_preserves_one_scope() {
    let registry = StatementNativeResidualRegistry::new(303);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let mut lease = registered(
        &registry,
        &scope,
        Arc::clone(&signal),
        Duration::from_secs(5),
    );
    let address = lease.borrow_mut() as *mut FixtureOwner as usize;
    lease.begin_terminal_cut(Instant::now()).unwrap();
    registry.close();
    assert!(lease.shutdown_requested());
    root.owner.complete();
    root.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    lease.handoff();
    assert_eq!(registry.len(), 1);
    let mut task = registry.ready_tasks().pop().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut task)
            .await
            .is_err()
    );
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    signal.local_exit();
    signal.remote_close();
    task.await.unwrap();
    assert_eq!(signal.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(signal.observed_address.load(Ordering::SeqCst), address);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[tokio::test]
async fn dropped_queued_job_restores_same_owner_for_original_registry_shutdown_retry() {
    let mut registry =
        super::super::LogicalExecutionRuntimeRegistry::new(tokio::runtime::Handle::current());
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let owner = fixture(&scope, Arc::clone(&signal));
    let lease = registry
        .statement_native_residual_port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap();
    lease.begin_terminal_cut(Instant::now()).unwrap();
    lease.handoff();
    let queued = registry.statement_native_ready_tasks();
    assert_eq!(queued.len(), 1);
    drop(queued);
    root.owner.complete();
    root.business.release();
    assert!(matches!(
        registry
            .shutdown_until(Instant::now() + Duration::from_millis(1))
            .await,
        Err(super::super::LogicalExecutionRuntimeShutdownError::DeadlineExceeded { .. })
    ));
    assert_eq!(control.snapshot().root_responsibilities, 1);
    signal.local_exit();
    signal.remote_close();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        signal.shutdowns.load(Ordering::SeqCst),
        1,
        "shutdown hook is one-shot across cancelled drain futures"
    );
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[tokio::test]
async fn remote_budget_is_frozen_at_terminal_cut_and_never_ends_live_local_work() {
    let registry = StatementNativeResidualRegistry::new(305);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let lease = registered(
        &registry,
        &scope,
        Arc::clone(&signal),
        Duration::from_secs(5),
    );
    let cut = Instant::now() - Duration::from_secs(10);
    let first = lease.begin_terminal_cut(cut).unwrap();
    let repeated = lease.begin_terminal_cut(Instant::now()).unwrap();
    assert_eq!(
        first.remote_observation_deadline(),
        repeated.remote_observation_deadline()
    );
    root.owner.complete();
    root.business.release();
    lease.handoff();
    let mut task = registry.ready_tasks().pop().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut task)
            .await
            .is_err()
    );
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    signal.local_exit();
    task.await.unwrap();
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert_eq!(
        control
            .snapshot()
            .obligation_endings
            .tracking_ended_remote_unknown,
        1
    );
    assert_eq!(
        control.snapshot().obligation_endings.settled_with_evidence,
        0
    );
}

#[tokio::test]
async fn original_convergence_panic_is_not_repolled_or_retired_and_payload_survives_until_process_exit()
 {
    let registry = StatementNativeResidualRegistry::new(306);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let mut owner = fixture(&scope, Arc::clone(&signal));
    owner.panic_payload = Some(PanicPayload(Arc::clone(&payload_drops)));
    let lease = registry
        .port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap();
    lease.begin_terminal_cut(Instant::now()).unwrap();
    root.owner.complete();
    root.business.release();
    lease.handoff();
    assert!(run_one(&registry).await.is_err());
    assert_eq!(registry.len(), 1);
    assert!(registry.ready_tasks().is_empty());
    assert_eq!(signal.calls.load(Ordering::SeqCst), 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(1), registry.drain())
            .await
            .is_err()
    );
    registry.abandon_for_process_exit();
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        control.snapshot().obligation_endings.settled_with_evidence,
        0
    );
}

#[derive(Debug)]
struct OriginalUnprovenExit;
impl std::fmt::Display for OriginalUnprovenExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original close join missing")
    }
}
impl std::error::Error for OriginalUnprovenExit {}
#[tokio::test]
async fn unproven_original_close_retains_typed_source_and_owner_without_new_work_or_repoll() {
    let registry = StatementNativeResidualRegistry::new(307);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let source = Arc::new(OriginalUnprovenExit);
    let mut owner = fixture(&scope, Arc::clone(&signal));
    owner.unresolved = Some(StatementNativeHealthFailure::actual_exit_unproven(
        "original split close exit is unproven",
        source.clone(),
    ));
    let lease = registry
        .port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap();
    lease.begin_terminal_cut(Instant::now()).unwrap();
    root.owner.complete();
    root.business.release();
    lease.handoff();
    let failure = run_one(&registry).await.unwrap_err();
    assert_eq!(failure.message(), "original split close exit is unproven");
    assert_eq!(registry.len(), 1);
    assert!(registry.ready_tasks().is_empty());
    assert_eq!(signal.calls.load(Ordering::SeqCst), 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(control.snapshot().root_responsibilities, 1);
    registry.abandon_for_process_exit();
}

#[tokio::test]
async fn foreground_panic_returns_original_box_to_cleanup_and_keeps_its_health_failure() {
    let registry = StatementNativeResidualRegistry::new(308);
    let (_control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let mut lease = registered(
        &registry,
        &scope,
        Arc::clone(&signal),
        Duration::from_secs(5),
    );
    let result: Result<(), _> = lease.catch_foreground(|_same_owner| {
        std::panic::panic_any(PanicPayload(Arc::clone(&payload_drops)))
    });
    assert!(result.is_err());
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    signal.local_exit();
    signal.remote_close();
    root.owner.complete();
    root.business.release();
    lease.handoff();
    assert!(
        run_one(&registry).await.is_err(),
        "true convergence does not erase original process health failure"
    );
    assert_eq!(registry.len(), 0);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    registry.abandon_for_process_exit();
    assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn foreground_convergence_panic_retains_same_owner_obligation_and_payload_without_repoll() {
    use std::future::Future;
    let registry = StatementNativeResidualRegistry::new(314);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let mut owner = fixture(&scope, Arc::clone(&signal));
    owner.panic_payload = Some(PanicPayload(Arc::clone(&payload_drops)));
    let original_address = &*owner as *const FixtureOwner as usize;
    let mut lease = registry
        .port(Duration::from_secs(5))
        .register(owner.execution, &scope, owner)
        .unwrap();
    let request = lease.begin_terminal_cut(Instant::now()).unwrap();
    // Poll the original convergence future itself. Its owned payload panics
    // before the first await; this is not a synthetic callback-only panic.
    let caught = lease.catch_convergence(|same_owner| {
        let mut convergence = same_owner.converge(request);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        convergence.as_mut().poll(&mut cx)
    });
    assert!(caught.is_err());
    assert_eq!(
        signal.observed_address.load(Ordering::SeqCst),
        original_address
    );
    assert_eq!(signal.calls.load(Ordering::SeqCst), 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    root.owner.complete();
    root.business.release();
    lease.handoff();
    assert_eq!(registry.len(), 1);
    assert_eq!(control.snapshot().obligations, 1);
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert!(
        registry.ready_tasks().is_empty(),
        "unknown already-panicked convergence must not restart"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(1), registry.drain())
            .await
            .is_err()
    );
    assert_eq!(signal.calls.load(Ordering::SeqCst), 1);
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        control.snapshot().obligation_endings.settled_with_evidence,
        0
    );
    registry.abandon_for_process_exit();
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn original_process_custody_return_is_distinct_and_limited_to_native_callers() {
    let returned = StatementNativeLocalExit::after_original_process_owner_return(
        StatementNativeLocalHolder::NativeOperations,
    )
    .unwrap();
    assert_eq!(
        returned.kind(),
        StatementNativeLocalClosureKind::OriginalProcessOwnerReturn
    );
    assert_eq!(
        StatementNativeLocalExit::after_original_process_owner_return(
            StatementNativeLocalHolder::RootPolls,
        ),
        Err(crate::api::StatementNativeLocalReturnError::HolderRequiresActualExit)
    );
    assert_eq!(
        StatementNativeLocalExit::after_actual_exit(StatementNativeLocalHolder::RootPolls,).kind(),
        StatementNativeLocalClosureKind::ActualExited
    );
}

#[tokio::test]
async fn early_remote_unknown_is_refused_without_discarding_the_original_lease() {
    let registry = StatementNativeResidualRegistry::new(309);
    let (control, root) = governance();
    let scope = root.owner.scope();
    let signal = Arc::new(Signal::default());
    let mut lease = registered(
        &registry,
        &scope,
        Arc::clone(&signal),
        Duration::from_secs(5),
    );
    lease.begin_terminal_cut(Instant::now()).unwrap();
    let owner = lease.borrow_mut();
    let wrong = owner.closure(StatementNativeRemoteClosure::TrackingEnded {
        exact_contexts: owner.contexts.clone(),
    });
    let rejected = lease.complete(wrong).unwrap_err();
    assert_eq!(
        rejected.error,
        StatementNativeCompletionError::RemoteBudgetNotElapsed
    );
    let mut lease = rejected.lease;
    root.owner.complete();
    root.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    let proof = lease.borrow_mut().evidence();
    lease.complete(proof).unwrap();
    assert_eq!(signal.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        control
            .snapshot()
            .obligation_endings
            .tracking_ended_remote_unknown,
        0
    );
}

#[tokio::test]
async fn held_original_local_close_does_not_block_another_ready_native_handoff() {
    let registry = StatementNativeResidualRegistry::new(310);
    let (control1, root1) = governance();
    let scope1 = root1.owner.scope();
    let signal1 = Arc::new(Signal::default());
    let lease1 = registered(
        &registry,
        &scope1,
        Arc::clone(&signal1),
        Duration::from_secs(5),
    );
    lease1.begin_terminal_cut(Instant::now()).unwrap();
    lease1.handoff();
    root1.owner.complete();
    root1.business.release();
    let (control2, root2) = governance();
    let scope2 = root2.owner.scope();
    let signal2 = Arc::new(Signal::default());
    let mut owner2 = fixture(&scope2, Arc::clone(&signal2));
    owner2.execution =
        QueryExecutionId::new(QueryId::new(41, 44), AttemptId::new(1).unwrap()).unwrap();
    owner2.contexts = vec![QueryContextRef::new(
        owner2.execution,
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    )]
    .into_boxed_slice();
    let lease2 = registry
        .port(Duration::from_secs(5))
        .register(owner2.execution, &scope2, owner2)
        .unwrap();
    lease2.begin_terminal_cut(Instant::now()).unwrap();
    let drain = registry.drain();
    tokio::pin!(drain);
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut drain)
            .await
            .is_err()
    );
    signal2.local_exit();
    signal2.remote_close();
    root2.owner.complete();
    root2.business.release();
    lease2.handoff();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut drain)
            .await
            .is_err()
    );
    assert_eq!(
        signal2.dropped.load(Ordering::SeqCst),
        1,
        "another SQL's local close must not serialize new handoff cleanup"
    );
    assert_eq!(signal1.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(control2.snapshot().root_responsibilities, 0);
    assert_eq!(control1.snapshot().root_responsibilities, 1);
    signal1.local_exit();
    signal1.remote_close();
    drain.await;
    assert_eq!(control1.snapshot().root_responsibilities, 0);
}
