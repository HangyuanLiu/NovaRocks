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
//! Scoped failure publication and stop observation of one exact Task owner.
//! This view never chooses a first cause, turns a notification into a decision,
//! or keeps an independent logical conclusion.
use crate::runtime::{
    fragment::{ExecutionFailure, ExecutionReason, ExecutionResult},
    observable::Observable,
};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, SettledContinuation, TaskConclusion, TaskConclusionPort,
    TaskConclusionProposal, TaskFailurePhase, TaskIdentity, TerminationDetail,
};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::{
    error::Error,
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};

/// Worker implements this on the same acceptance domain used by control and
/// lifecycle. `is_stopped` is the owner's cheap presence view; waits register
/// against that same domain. Notifications are readiness, not an arbiter.
pub trait TaskExecutionPort: TaskConclusionPort<ExecutionFailure> {
    /// The exact original runtime health, with no Task acceptance authority.
    fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth>;
    /// Secondary cleanup never submits or refines a Task conclusion. The
    /// original rich source stays borrowed, outside the acceptance fence.
    fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase);
    /// Physical exit is unproved. The actual owner retains work; this reports
    /// only neutral facts to the process supervisor, without accepting a Task.
    fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase);
    fn is_stopped(&self) -> bool;
    fn stop_observable(&self) -> Arc<Observable>;
    fn wait_until_stopped(&self, duration: Duration) -> bool;
}

#[derive(Debug)]
pub struct TaskControlIdentityMismatch;
impl fmt::Display for TaskControlIdentityMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Task conclusion port identity does not match its execution binding")
    }
}
impl Error for TaskControlIdentityMismatch {}
impl FailureProjection for TaskControlIdentityMismatch {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::TaskControlIdentityMismatch.reason(),
        )
    }
}

#[derive(Debug)]
pub struct MissingTaskExecutionControl;
impl fmt::Display for MissingTaskExecutionControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Task execution requires its accepted conclusion owner")
    }
}
impl Error for MissingTaskExecutionControl {}
impl FailureProjection for MissingTaskExecutionControl {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::MissingTaskExecutionControl.reason(),
        )
    }
}

/// A detached neutral metadata reporter. It cannot retain a Task port, a
/// source graph, RuntimeState, input or work resource after control retirement.
#[derive(Clone)]
pub(crate) struct ExecutionExitReporter {
    health: Arc<crate::runtime::execution_health::ExecutionHealth>,
    origin: crate::runtime::execution_health::ExecutionExitOrigin,
}
impl ExecutionExitReporter {
    pub(crate) fn new(
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        origin: crate::runtime::execution_health::ExecutionExitOrigin,
    ) -> Self {
        Self { health, origin }
    }
    pub(crate) fn report(&self, fact: FailureFact) {
        self.health.report_unresolved(self.origin, fact);
    }
    #[cfg(test)]
    pub(crate) fn failure(&self) -> Option<crate::runtime::execution_health::ExecutionExitFault> {
        self.health.failure()
    }
}

/// A preparation or execution scope borrows the same authority. Keeping this
/// narrow owned capability in a job does not retain a RuntimeState, driver,
/// expression arena, input, construction factory or thread work lease.
pub struct TaskExecutionControl {
    identity: TaskIdentity,
    owner: Arc<dyn TaskExecutionPort>,
    phase: TaskFailurePhase,
    /// A local control observation is separate from an originating failure.
    /// Only control receipts use this diagnostic cache. A later legal derived
    /// refinement reads its actual originating source directly from the owner.
    observed_control: OnceLock<ExecutionFailure>,
    exit_reporter: ExecutionExitReporter,
}
impl fmt::Debug for TaskExecutionControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskExecutionControl")
            .field("identity", &self.identity)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}
impl TaskExecutionControl {
    pub fn new(
        identity: TaskIdentity,
        owner: Arc<dyn TaskExecutionPort>,
        phase: TaskFailurePhase,
    ) -> Result<Self, TaskControlIdentityMismatch> {
        if identity != owner.identity() {
            return Err(TaskControlIdentityMismatch);
        }
        let exit_reporter = ExecutionExitReporter::new(
            owner.execution_health(),
            crate::runtime::execution_health::ExecutionExitOrigin::Task { identity, phase },
        );
        Ok(Self {
            identity,
            owner,
            exit_reporter,
            phase,
            observed_control: OnceLock::new(),
        })
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub fn in_phase(&self, phase: TaskFailurePhase) -> Self {
        Self {
            identity: self.identity,
            owner: Arc::clone(&self.owner),
            phase,
            observed_control: OnceLock::new(),
            exit_reporter: ExecutionExitReporter::new(
                self.exit_reporter.health.clone(),
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: self.identity,
                    phase,
                },
            ),
        }
    }
    pub(crate) fn exit_health_reporter(&self) -> ExecutionExitReporter {
        self.exit_reporter.clone()
    }
    pub fn report_failure(&self, cause: ExecutionFailure) {
        self.owner.submit(TaskConclusionProposal::Failed {
            cause,
            phase: self.phase,
        })
    }
    pub fn on_settled(&self, continuation: SettledContinuation<ExecutionFailure>) {
        self.owner.on_settled(continuation);
    }
    pub fn report_cleanup(&self, cause: &ExecutionFailure) {
        self.owner.report_cleanup(cause, self.phase);
    }
    pub fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
        let fact = cause.failure_fact();
        self.owner.report_unresolved_exit(fact, self.phase);
    }
    pub fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.owner.accepted()
    }
    pub fn is_stopped(&self) -> bool {
        self.owner.is_stopped()
    }
    pub fn stop_observable(&self) -> Arc<Observable> {
        self.owner.stop_observable()
    }
    pub fn wait_until_stopped(&self, duration: Duration) -> bool {
        self.owner.wait_until_stopped(duration)
    }
    /// Original failures remain the exact accepted source. Control commands
    /// have no originating failure: this is their typed local observation,
    /// never a reconstructed failure standing in for a source held elsewhere.
    pub fn error(&self) -> Option<ExecutionFailure> {
        if !self.is_stopped() {
            return None;
        }
        let accepted = self
            .owner
            .accepted()
            .expect("a stopped Task has an accepted receipt");
        if let Some(original) = accepted.originating_failure() {
            return Some(original.clone());
        }
        match accepted.neutral().conclusion() {
            TaskConclusion::Terminated(
                TerminationDetail::Canceled(_) | TerminationDetail::Aborted(_),
            ) => Some(
                self.observed_control
                    .get_or_init(|| ExecutionFailure::from_domain(TaskStopObservation(accepted)))
                    .clone(),
            ),
            TaskConclusion::Terminated(TerminationDetail::Failed(_)) => {
                unreachable!("accepted failure receipt requires its original cause")
            }
            TaskConclusion::Succeeded => None,
        }
    }
    pub fn wait_interruptibly(&self, duration: Duration) -> ExecutionResult<()> {
        if !self.wait_until_stopped(duration) {
            return Ok(());
        }
        match self.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
impl crate::runtime::fragment::ExecutionFailureReporter for TaskExecutionControl {
    fn report_cleanup(&self, cause: &ExecutionFailure) {
        TaskExecutionControl::report_cleanup(self, cause);
    }
    fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
        TaskExecutionControl::report_unresolved_exit(self, cause);
    }
    fn report_failure(&self, error: ExecutionFailure) {
        TaskExecutionControl::report_failure(self, error);
    }
}

#[derive(Debug)]
struct TaskStopObservation(Arc<AcceptedTaskConclusion<ExecutionFailure>>);
impl fmt::Display for TaskStopObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.neutral().conclusion() {
            TaskConclusion::Terminated(TerminationDetail::Canceled(reason)) => {
                write!(f, "Task execution observed accepted cancellation: {reason}")
            }
            TaskConclusion::Terminated(TerminationDetail::Aborted(reason)) => {
                write!(f, "Task execution observed accepted abort: {reason}")
            }
            _ => unreachable!("only accepted control conclusions create a stop observation"),
        }
    }
}
impl Error for TaskStopObservation {}
impl FailureProjection for TaskStopObservation {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::stop_observation(ExecutionReason::TaskControlObservedStop.reason())
    }
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) mod test_support {
    use super::*;
    use novarocks_execution_contract::{
        AbortCause, AcceptedConclusion, CancelReason, ConclusionDecision, SafeDetail, TaskFailure,
        decide_conclusion,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        num::{NonZeroU16, NonZeroU64},
        sync::{
            Barrier, Condvar, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    type Snapshot = Arc<AcceptedTaskConclusion<ExecutionFailure>>;
    #[derive(Debug)]
    struct UnownedStop(ExecutionFailure);
    impl fmt::Display for UnownedStop {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Task observed stop without an accepted conclusion")
        }
    }
    impl Error for UnownedStop {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }
    impl FailureProjection for UnownedStop {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Unexpected,
                novarocks_failure_contract::FailureReason::new(
                    novarocks_failure_contract::domain::EXECUTION,
                    NonZeroU16::new(65000).unwrap(),
                ),
            )
        }
    }
    pub(crate) struct Owner {
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        cleanup: Mutex<[Option<FailureFact>; 4]>,
        identity: TaskIdentity,
        accepted: Mutex<Option<Snapshot>>,
        stopped: AtomicBool,
        cv: Condvar,
        observable: Arc<Observable>,
        before_notify: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
    }
    impl Owner {
        pub(crate) fn new() -> Arc<Self> {
            static NEXT: AtomicUsize = AtomicUsize::new(1);
            let sequence = i64::try_from(NEXT.fetch_add(1, Ordering::Relaxed)).unwrap();
            let identity = TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(3901, sequence), AttemptId::new(1).unwrap())
                    .unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            );
            Arc::new(Self {
                health: Arc::new(crate::runtime::execution_health::ExecutionHealth::default()),
                cleanup: Mutex::new([None; 4]),
                identity,
                accepted: Mutex::new(None),
                stopped: AtomicBool::new(false),
                cv: Condvar::new(),
                observable: Arc::new(Observable::new()),
                before_notify: Mutex::new(None),
            })
        }
        pub(crate) fn view(self: &Arc<Self>, phase: TaskFailurePhase) -> TaskExecutionControl {
            TaskExecutionControl::new(self.identity, self.clone(), phase).unwrap()
        }
    }
    impl TaskConclusionPort<ExecutionFailure> for Owner {
        fn identity(&self) -> TaskIdentity {
            self.identity
        }
        fn accepted(&self) -> Option<Snapshot> {
            self.accepted.lock().unwrap().clone()
        }
        fn submit(&self, proposed: TaskConclusionProposal<ExecutionFailure>) {
            // This fixture owns one Task domain. The real Worker owner, its
            // status publication and terminal seal are integrated separately.
            let mut reported_evidence = None;
            let (conclusion, origin) = match proposed {
                TaskConclusionProposal::Succeeded => (TaskConclusion::Succeeded, None),
                TaskConclusionProposal::Canceled(reason) => (
                    TaskConclusion::Terminated(TerminationDetail::Canceled(reason)),
                    None,
                ),
                TaskConclusionProposal::Aborted(reason) => (
                    TaskConclusion::Terminated(TerminationDetail::Aborted(reason)),
                    None,
                ),
                TaskConclusionProposal::Failed { cause, phase } => {
                    let fact = cause.failure_fact();
                    let evidence = fact.evidence();
                    reported_evidence = Some(evidence);
                    let (cause, fact) = if evidence
                        == novarocks_failure_contract::FailureEvidence::StopObservation
                    {
                        let cause = ExecutionFailure::from_domain(UnownedStop(cause));
                        let fact = cause.failure_fact();
                        (cause, fact)
                    } else {
                        (cause, fact)
                    };
                    (
                        TaskConclusion::Terminated(TerminationDetail::Failed(
                            TaskFailure::new_in_phase(
                                fact,
                                cause.safe_detail_for_fact(fact),
                                phase,
                            ),
                        )),
                        Some(cause),
                    )
                }
            };
            let mut current = self.accepted.lock().unwrap();
            let held = current
                .as_ref()
                .map(|snapshot| snapshot.neutral().conclusion());
            let decision = if let Some(evidence) = reported_evidence {
                novarocks_execution_contract::decide_reported_failure(held, &conclusion, evidence)
            } else {
                decide_conclusion(held, &conclusion)
            };
            if decision != ConclusionDecision::Keep {
                let generation = current.as_ref().map_or(1, |snapshot| {
                    snapshot
                        .neutral()
                        .generation()
                        .get()
                        .checked_add(1)
                        .unwrap()
                });
                let receipt = AcceptedConclusion::new(
                    self.identity,
                    NonZeroU64::new(generation).unwrap(),
                    conclusion,
                );
                *current = Some(Arc::new(
                    AcceptedTaskConclusion::new(receipt, origin).unwrap(),
                ));
                self.stopped.store(current.is_some(), Ordering::Release);
            }
            drop(current);
            if decision != ConclusionDecision::Keep {
                let gap = self.before_notify.lock().unwrap().take();
                if let Some((entered, release)) = gap {
                    entered.wait();
                    release.wait();
                }
                self.cv.notify_all();
                self.observable.notify_observers();
            }
        }
        fn on_settled(&self, continuation: SettledContinuation<ExecutionFailure>) {
            let snapshot = self.accepted();
            continuation(snapshot);
        }
    }
    impl TaskExecutionPort for Owner {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.health.clone()
        }
        fn report_cleanup(&self, cause: &ExecutionFailure, _: TaskFailurePhase) {
            let fact = cause.failure_fact();
            let mut cleanup = self.cleanup.lock().unwrap();
            if let Some(slot) = cleanup.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(fact);
            }
        }
        fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
            self.health.report_unresolved(
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: self.identity,
                    phase,
                },
                fact,
            );
        }
        fn is_stopped(&self) -> bool {
            self.stopped.load(Ordering::Acquire)
        }
        fn stop_observable(&self) -> Arc<Observable> {
            self.observable.clone()
        }
        fn wait_until_stopped(&self, duration: Duration) -> bool {
            let guard = self.accepted.lock().unwrap();
            let (_guard, _) = self
                .cv
                .wait_timeout_while(guard, duration, |_| !self.stopped.load(Ordering::Acquire))
                .unwrap();
            self.stopped.load(Ordering::Acquire)
        }
    }
    #[test]
    fn cleanup_and_unknown_exit_leave_the_exact_accepted_task_receipt_unchanged() {
        use novarocks_execution_contract::AbortCause;
        let owner = Owner::new();
        let control = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let accepted = owner.accepted().unwrap();
        let original = accepted;
        let cleanup = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        control.report_cleanup(&cleanup);
        control.report_unresolved_exit(&cleanup);
        let current = control.accepted().unwrap();
        assert!(Arc::ptr_eq(&current, &original));
        assert!(current.originating_failure().is_none());
        assert_eq!(
            owner.cleanup.lock().unwrap()[0],
            Some(cleanup.failure_fact())
        );
        let fault = owner.health.failure().unwrap();
        assert_eq!(fault.fact(), cleanup.failure_fact());
        assert_eq!(
            fault.origin(),
            crate::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: owner.identity,
                phase: TaskFailurePhase::Execution,
            }
        );
        // An independently authored original failure still has its own legal
        // acceptance path; cleanup did not consume or refine that opportunity.
        let before_refined = control.accepted();
        control.report_failure(cleanup);
        let refined = control.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
    }

    pub(crate) fn owner_for(identity: TaskIdentity) -> Arc<Owner> {
        let mut owner = Owner::new();
        Arc::get_mut(&mut owner).unwrap().identity = identity;
        owner
    }
    pub(crate) fn control_for(identity: TaskIdentity) -> Arc<TaskExecutionControl> {
        Arc::new(owner_for(identity).view(TaskFailurePhase::Execution))
    }
    #[test]
    fn fragment_group_has_only_physical_completion_and_the_same_task_acceptance() {
        use crate::exec::pipeline::{
            fragment_context::FragmentContext, global_driver_executor::FragmentCompletion,
        };
        use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
        let owner = Owner::new();
        let state = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(RuntimeErrorState::for_task(Arc::new(
                owner.view(TaskFailurePhase::Execution),
            ))),
        ));
        let fragment = FragmentContext::new(None, state.clone(), None, None, None, None);
        let completion = FragmentCompletion::new(2, state.error_state());
        let reentered = Arc::new(AtomicUsize::new(0));
        let callback_count = reentered.clone();
        let callback_completion = Arc::downgrade(&completion);
        owner.observable.add_observer(Arc::new(move || {
            let completion = callback_completion.upgrade().unwrap();
            assert!(completion.conclusion().unwrap().is_err());
            assert!(completion.stopped_fact().is_none());
            callback_count.fetch_add(1, Ordering::Release);
        }));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let observation = state.error_state().error().unwrap();
        let before_observation = owner.accepted().unwrap();
        fragment.set_final_status(observation);
        assert!(Arc::ptr_eq(&before_observation, &owner.accepted().unwrap()));
        let original = cause();
        fragment.set_final_status(original.clone());
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert!(completion.should_abort());
        assert_eq!(
            reentered.load(Ordering::Acquire),
            2,
            "both Task acceptance and refinement may reenter physical completion"
        );
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&original)
        );
        assert!(completion.stopped_fact().is_none());
        assert!(!completion.driver_finished());
        assert!(completion.stopped_fact().is_none());
        assert!(completion.driver_finished());
        assert!(completion.wait().unwrap_err().same_occurrence(&original));
        let owner = Owner::new();
        let completion = FragmentCompletion::new(
            0,
            Arc::new(RuntimeErrorState::for_task(Arc::new(
                owner.view(TaskFailurePhase::Execution),
            ))),
        );
        assert_eq!(completion.wait(), Ok(()));
        assert!(
            owner.accepted().is_none(),
            "zero drivers do not prove root/finalization work finished"
        );
        let original = cause();
        completion.fail(original.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&original),
            "physical driver exit cannot suppress another work owner's failure"
        );
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
    }

    #[test]
    fn task_runtime_requires_exact_actual_control_and_local_execution_stays_taskless() {
        use crate::runtime::fragment::runtime_state::{RuntimeStateInputs, build_runtime_state};
        let owner = Owner::new();
        let other = Owner::new();
        let inputs = |task_identity, task_control| RuntimeStateInputs {
            query_options: None,
            query_id: None,
            fragment_instance_id: None,
            backend_num: None,
            mem_tracker: None,
            runtime_filter_session: None,
            execution_runtime: None,
            query_memory: None,
            task_identity,
            task_control,
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
        };
        let absent = build_runtime_state(inputs(Some(owner.identity), None)).unwrap_err();
        assert_eq!(
            absent.failure_fact(),
            FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::MissingTaskExecutionControl.reason()
            )
        );
        for identity in [None, Some(other.identity)] {
            let mismatch = build_runtime_state(inputs(
                identity,
                Some(Arc::new(owner.view(TaskFailurePhase::Execution))),
            ))
            .unwrap_err();
            assert_eq!(
                mismatch.failure_fact(),
                TaskControlIdentityMismatch.failure_fact()
            );
        }
        let runtime = build_runtime_state(inputs(
            Some(owner.identity),
            Some(Arc::new(owner.view(TaskFailurePhase::Execution))),
        ))
        .unwrap();
        let original = cause();
        runtime.error_state().set_error(original.clone());
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        let local = build_runtime_state(inputs(None, None)).unwrap();
        assert!(!local.error_state().is_stopped());
        local.error_state().set_error(cause());
        assert!(
            runtime
                .error_state()
                .error()
                .unwrap()
                .same_occurrence(&original)
        );
    }

    #[test]
    fn runtime_facade_observes_task_acceptance_and_legal_original_refinement() {
        use crate::runtime::runtime_state::RuntimeErrorState;
        let owner = Owner::new();
        let runtime =
            RuntimeErrorState::for_task(Arc::new(owner.view(TaskFailurePhase::Execution)));
        assert!(Arc::ptr_eq(&runtime.stop_observable(), &owner.observable));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        assert!(runtime.is_stopped());
        assert!(runtime.wait_until_stopped(Duration::ZERO));
        let observed = runtime.error().unwrap();
        assert_eq!(
            observed.failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
        let before_observation = owner.accepted().unwrap();
        runtime.set_error(observed);
        assert!(Arc::ptr_eq(&before_observation, &owner.accepted().unwrap()));
        let original = cause();
        runtime.set_error(original.clone());
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert!(runtime.error().unwrap().same_occurrence(&original));
        assert!(
            runtime
                .wait_interruptibly(Duration::ZERO)
                .unwrap_err()
                .same_occurrence(&original)
        );
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        let canceled = Owner::new();
        let runtime =
            RuntimeErrorState::for_task(Arc::new(canceled.view(TaskFailurePhase::Execution)));
        canceled.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let receipt = canceled.accepted().unwrap();
        runtime.set_error(cause());
        assert!(Arc::ptr_eq(&receipt, &canceled.accepted().unwrap()));
        assert_eq!(
            runtime.error().unwrap().failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
    }

    fn cause() -> ExecutionFailure {
        novarocks_functions::KernelFailure::ResourceExhausted.into()
    }
    #[test]
    fn an_acceptance_before_notification_is_visible_and_a_later_proposal_keeps_its_source() {
        let owner = Owner::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *owner.before_notify.lock().unwrap() = Some((entered.clone(), release.clone()));
        let publisher = Arc::new(owner.view(TaskFailurePhase::Execution));
        let reader = owner.view(TaskFailurePhase::Execution);
        let original = cause();
        let source = original.clone();
        let job = std::thread::spawn(move || {
            publisher.report_failure(source);
            publisher.accepted().unwrap()
        });
        entered.wait();
        assert!(reader.is_stopped());
        assert!(reader.wait_until_stopped(Duration::ZERO));
        assert!(reader.error().unwrap().same_occurrence(&original));
        let before_later = reader.accepted();
        reader.report_failure(cause());
        let later = reader.accepted().unwrap();
        assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
        assert!(
            later
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        release.wait();
        let first = job.join().unwrap();
        assert!(Arc::ptr_eq(&first, &later));
    }
    #[test]
    fn accepted_control_has_no_fabricated_origin_and_refinement_reads_the_actual_failure() {
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let first = owner.accepted().unwrap();
        let observation = view.error().unwrap();
        assert_eq!(observation.failure_fact().class(), FailureClass::Control);
        assert!(first.originating_failure().is_none());
        let original = cause();
        let before_refined = view.accepted();
        view.report_failure(original.clone());
        let refined = view.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
        assert_eq!(refined.neutral().generation().get(), 2);
        assert!(view.error().unwrap().same_occurrence(&original));
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let canceled = owner.accepted().unwrap();
        let before_later = view.accepted();
        view.report_failure(cause());
        let later = view.accepted().unwrap();
        assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
        assert!(Arc::ptr_eq(&canceled, &later));
        assert!(view.accepted().unwrap().originating_failure().is_none());
    }
    #[test]
    fn scoped_phase_keeps_one_authority_and_success_has_no_late_failure_origin() {
        let owner = Owner::new();
        let prep = owner.view(TaskFailurePhase::Preparation);
        let execution = prep.in_phase(TaskFailurePhase::Execution);
        let original = cause();
        prep.report_failure(original.clone());
        let accepted = prep.accepted().unwrap();
        execution.report_failure(cause());
        let later = execution.accepted().unwrap();
        assert!(Arc::ptr_eq(&accepted, &later));
        let TaskConclusion::Terminated(TerminationDetail::Failed(failure)) =
            accepted.neutral().conclusion()
        else {
            panic!("accepted original failure");
        };
        assert_eq!(failure.phase(), TaskFailurePhase::Preparation);
        assert!(execution.error().unwrap().same_occurrence(&original));
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Succeeded);
        let successful = owner.accepted().unwrap();
        assert!(
            view.is_stopped(),
            "any accepted Task conclusion closes ordinary production"
        );
        view.report_failure(cause());
        let later = view.accepted().unwrap();
        assert!(Arc::ptr_eq(&successful, &later));
        assert!(view.error().is_none());
    }
    #[test]
    fn observed_stop_cannot_refine_peer_placeholder_but_a_genuine_control_failure_can() {
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let first = owner.accepted().unwrap();
        let observed = view.error().unwrap();
        assert_eq!(
            observed.failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
        for observation in [
            observed,
            novarocks_functions::KernelFailure::Cancelled.into(),
            novarocks_functions::KernelFailure::DeadlineExceeded.into(),
        ] {
            let before_later = view.accepted();
            view.report_failure(observation);
            let later = view.accepted().unwrap();
            assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
            assert!(Arc::ptr_eq(&first, &later));
            assert!(later.originating_failure().is_none());
        }
        #[derive(Debug)]
        struct LocalTimeout;
        impl fmt::Display for LocalTimeout {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("local timeout")
            }
        }
        impl Error for LocalTimeout {}
        impl FailureProjection for LocalTimeout {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Control,
                    novarocks_failure_contract::FailureReason::new(
                        novarocks_failure_contract::domain::EXECUTION,
                        NonZeroU16::new(65001).unwrap(),
                    ),
                )
            }
        }
        let original = ExecutionFailure::from_domain(LocalTimeout);
        let before_refined = view.accepted();
        view.report_failure(original.clone());
        let refined = view.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
        assert!(view.error().unwrap().same_occurrence(&original));
    }
    #[test]
    fn unowned_stop_is_an_owner_failure_with_the_actual_observation_as_source() {
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        let observation: ExecutionFailure = novarocks_functions::KernelFailure::Cancelled.into();
        let before_accepted = view.accepted();
        view.report_failure(observation.clone());
        let accepted = view.accepted().unwrap();
        assert!(before_accepted.is_none());
        assert_eq!(accepted.neutral().generation().get(), 1);
        let primary = accepted.originating_failure().unwrap();
        assert_eq!(primary.failure_fact().class(), FailureClass::Unexpected);
        let mut source = primary.source();
        let mut seen = false;
        while let Some(current) = source {
            if let Some(actual) = current.downcast_ref::<ExecutionFailure>() {
                seen |= actual.same_occurrence(&observation);
            }
            source = current.source();
        }
        assert!(seen);
    }

    #[test]
    fn another_task_port_is_rejected_before_work_is_bound() {
        let owner = Owner::new();
        let other = Owner::new();
        let error = TaskExecutionControl::new(other.identity, owner, TaskFailurePhase::Execution)
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
