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

//! Neutral owner tests for the Worker task registry.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use novarocks_execution_contract::task_execution::descriptor::{
    ExchangeTopology, PhysicalFragmentPlan, TaskDescriptor,
};
use novarocks_execution_contract::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    PlanNodeId,
};
use novarocks_execution_contract::task_execution::identity::{
    AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::lease::{LeaseSequence, LeaseValidFor};
use novarocks_execution_contract::task_execution::operation::{
    AcquireQueryContextAdmissionTicket, CreateTask, CredentialUpdate, EstablishQueryContext,
    OperationOutcome, RenewQueryExecutionLease, UpdateQueryContext,
};
use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_types::{NativeCompatibilityId, UniqueId};

use crate::{
    HostRejection, ManualClock, QueryContextHost, ReleasedContextEvidence, RunnableTask,
    SharedFactsRequest, TaskCreationGate, TaskExecutionHost, TaskExecutionMetrics,
    TaskExecutionPorts, TaskExecutionRegistry, TaskExecutionRegistryConfig, TaskProtocolEvent,
    TaskProtocolObserver, TaskResultLifecycle, TaskStatusReporter, WorkerMonotonicClock,
};

#[derive(Debug)]
struct TestPlan(u8);

impl CodecOwnedContent for TestPlan {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([self.0; 16])
    }

    fn encoded_len(&self) -> usize {
        16
    }
}

impl PhysicalFragmentPlan for TestPlan {
    fn contract_version(
        &self,
    ) -> novarocks_execution_contract::task_execution::descriptor::FragmentContractVersion {
        novarocks_execution_contract::task_execution::descriptor::FragmentContractVersion::CURRENT
    }

    fn sink_kind(
        &self,
    ) -> novarocks_execution_contract::task_execution::descriptor::FragmentSinkKind {
        novarocks_execution_contract::task_execution::descriptor::FragmentSinkKind::Result
    }
}

#[derive(Debug)]
struct TestContent(u8);

impl CodecOwnedContent for TestContent {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([self.0; 16])
    }

    fn encoded_len(&self) -> usize {
        16
    }
}

struct TestSecret;

impl ConfidentialContent for TestSecret {
    fn encoded_len(&self) -> usize {
        16
    }

    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        other.encoded_len() == self.encoded_len()
    }
}

struct TestContextHost;

impl QueryContextHost for TestContextHost {
    fn materialize(&self, _request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        Ok(())
    }

    fn release(&self, _context: QueryContextRef) -> ReleasedContextEvidence {
        ReleasedContextEvidence::none()
    }

    fn advance_shared_domain(
        &self,
        _context: QueryContextRef,
        _domain: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        Ok(())
    }
}

#[derive(Debug)]
struct TestRunnable;

impl RunnableTask for TestRunnable {
    fn commit_creation(&self) {}

    fn cancel(&self, _reason: CancelReason) {}

    fn abort(&self, _cause: AbortCause) {}
}

#[derive(Default)]
struct TestTaskHost {
    install_gate: Option<Arc<InstallGate>>,
    receivers_installed: AtomicUsize,
    receivers_removed: AtomicUsize,
    capabilities_installed: AtomicUsize,
    submitted: AtomicUsize,
}

impl TaskExecutionHost for TestTaskHost {
    fn close_context_admission(&self, _context: QueryContextRef) {}

    fn forget_context_admission(&self, _context: QueryContextRef) {}

    fn install_receiver(&self, _descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        if let Some(gate) = &self.install_gate {
            gate.wait_for_release();
        }
        self.receivers_installed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn remove_receiver(&self, _descriptor: &TaskDescriptor) {
        self.receivers_removed.fetch_add(1, Ordering::SeqCst);
    }

    fn install_inbound_capability(
        &self,
        _descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        self.capabilities_installed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn remove_inbound_capability(&self, _descriptor: &TaskDescriptor) {}

    fn submit_runnable(
        &self,
        _descriptor: &TaskDescriptor,
        _reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        self.submitted.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(TestRunnable))
    }

    fn apply_task_domain(
        &self,
        _descriptor: &TaskDescriptor,
        _domain: &novarocks_execution_contract::task_execution::operation::TaskDomainUpdate,
    ) -> Result<Option<u64>, HostRejection> {
        Ok(None)
    }
}

#[derive(Default)]
struct InstallGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl InstallGate {
    fn held() -> Self {
        Self {
            state: Mutex::new(GateState {
                held: true,
                waiter_entered: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().expect("test install gate");
        while !state.waiter_entered {
            state = self.changed.wait(state).expect("test install gate");
        }
    }

    fn wait_for_release(&self) {
        let mut state = self.state.lock().expect("test install gate");
        state.waiter_entered = true;
        self.changed.notify_all();
        while state.held {
            state = self.changed.wait(state).expect("test install gate");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("test install gate");
        state.held = false;
        self.changed.notify_all();
    }
}

struct NoopObserver;

impl TaskProtocolObserver for NoopObserver {
    fn observe(&self, _event: TaskProtocolEvent) {}
}

struct NoopResultLifecycle;

impl TaskResultLifecycle for NoopResultLifecycle {
    fn discard_task(&self, _identity: TaskIdentity) {}

    fn retire_task_result(&self, _identity: TaskIdentity) {}
}

struct NoopMetrics;

impl TaskExecutionMetrics for NoopMetrics {
    fn record_task_created(&self) {}
}

#[derive(Default)]
struct GateState {
    held: bool,
    waiter_entered: bool,
}

#[derive(Default)]
struct BlockingCreationGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl BlockingCreationGate {
    fn held() -> Self {
        Self {
            state: Mutex::new(GateState {
                held: true,
                waiter_entered: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait_until_waiter_entered(&self) {
        let mut state = self.state.lock().expect("test creation gate");
        while !state.waiter_entered {
            state = self.changed.wait(state).expect("test creation gate");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("test creation gate");
        state.held = false;
        self.changed.notify_all();
    }
}

impl TaskCreationGate for BlockingCreationGate {
    fn holds_task_creation(&self, _context: QueryContextRef) -> bool {
        self.state.lock().expect("test creation gate").held
    }

    fn wait_for_task_creation_release(&self, _context: QueryContextRef) {
        let mut state = self.state.lock().expect("test creation gate");
        state.waiter_entered = true;
        self.changed.notify_all();
        while state.held {
            state = self.changed.wait(state).expect("test creation gate");
        }
    }
}

fn test_ports() -> TaskExecutionPorts {
    TaskExecutionPorts::new(
        Arc::new(NoopObserver),
        Arc::new(NoopResultLifecycle),
        Arc::new(NoopMetrics),
    )
}

fn establish(registry: &TaskExecutionRegistry, context: QueryContextRef) -> AdmissionTicketId {
    let ticket = registry
        .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).expect("valid ticket lease"),
            NativeCompatibilityId::new([0x71; 32]),
            registry.admission_epoch_capability(),
        ))
        .acknowledgement()
        .expect("admission ticket")
        .ticket_id();
    let receipt =
        registry.update_query_context(&UpdateQueryContext::Establish(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            ticket,
            Arc::new(TestContent(1)),
            Arc::new(TestContent(2)),
            Arc::new(TestContent(3)),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(TestSecret),
            ),
            LeaseValidFor::new(Duration::from_secs(10)).expect("valid context lease"),
        )));
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
    ticket
}

#[test]
fn lease_index_tracks_only_live_contexts_across_bulk_expiry() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let clock = Arc::new(ManualClock::new());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::new(TestTaskHost::default()),
        test_ports(),
    );

    let contexts: Vec<_> = (1..=32)
        .map(|number| {
            let execution = QueryExecutionId::new(
                QueryId::new(number, 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query id");
            QueryContextRef::new(execution, frontend, backend)
        })
        .collect();
    for (index, context) in contexts.iter().copied().enumerate() {
        establish(&registry, context);
        assert_eq!(registry.indexed_lease_count(), index + 1);
        assert_eq!(registry.context_state(context), QueryContextState::Active);
    }

    clock.advance(Duration::from_secs(9));
    assert_eq!(registry.advance_deadlines().leases_expired, 0);
    assert_eq!(registry.indexed_lease_count(), contexts.len());

    clock.advance(Duration::from_secs(1));
    assert_eq!(registry.advance_deadlines().leases_expired, contexts.len());
    assert_eq!(registry.indexed_lease_count(), 0);
    for context in contexts {
        assert_eq!(
            registry.context_state(context),
            QueryContextState::TerminalRetained
        );
        assert!(registry.status_source(context).is_some());
    }
}

#[test]
fn lease_renewal_and_replay_never_accumulate_expired_index_entries() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(91, 92),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let clock = Arc::new(ManualClock::new());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::new(TestTaskHost::default()),
        test_ports(),
    );
    establish(&registry, context);

    for sequence in 1..=64 {
        clock.advance(Duration::from_secs(1));
        let request = RenewQueryExecutionLease::new(
            TaskOperationId::new_v7(),
            context,
            LeaseSequence::new(sequence),
            LeaseValidFor::new(Duration::from_secs(10)).expect("valid context lease"),
        );
        let renewed =
            registry.update_query_context(&UpdateQueryContext::RenewLease(request.clone()));
        assert_eq!(renewed.outcome(), OperationOutcome::Accepted, "{renewed:?}");
        let replay = registry.update_query_context(&UpdateQueryContext::RenewLease(request));
        assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
        assert_eq!(registry.indexed_lease_count(), 1);
        assert_eq!(registry.context_state(context), QueryContextState::Active);
    }

    clock.advance(Duration::from_secs(9));
    assert_eq!(registry.advance_deadlines().leases_expired, 0);
    assert_eq!(registry.indexed_lease_count(), 1);
    clock.advance(Duration::from_secs(1));
    assert_eq!(registry.advance_deadlines().leases_expired, 1);
    assert_eq!(registry.indexed_lease_count(), 0);
    assert_eq!(
        registry.context_state(context),
        QueryContextState::TerminalRetained
    );
}

#[test]
fn adapter_gate_blocks_runnable_submission_until_it_releases() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(41, 42),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let task_host = Arc::new(TestTaskHost::default());
    let gate = Arc::new(BlockingCreationGate::held());
    let registry = TaskExecutionRegistry::new_with_task_creation_gate(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
        Arc::clone(&gate) as Arc<dyn TaskCreationGate>,
    );

    establish(&registry, context);
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    );
    let descriptor = TaskDescriptor::try_new(
        identity,
        UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).expect("nonzero dop"),
        vec![PlanNodeId::new(1).expect("nonnegative node")],
        ExchangeTopology::default(),
        Arc::new(TestPlan(0x51)),
    )
    .expect("legal descriptor");
    let request = CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
        .expect("legal create");

    let create_registry = Arc::clone(&registry);
    let create = std::thread::spawn(move || create_registry.create_task(&request));
    gate.wait_until_waiter_entered();
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 0);

    gate.release();
    let receipt = create.join().expect("create thread");
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 1);
}

#[test]
fn lost_create_acknowledgement_replays_without_resubmitting_the_runnable() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(51, 52),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let task_host = Arc::new(TestTaskHost::default());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
    );

    establish(&registry, context);
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    );
    let descriptor = TaskDescriptor::try_new(
        identity,
        UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).expect("nonzero dop"),
        vec![PlanNodeId::new(1).expect("nonnegative node")],
        ExchangeTopology::default(),
        Arc::new(TestPlan(0x51)),
    )
    .expect("legal descriptor");
    let request = CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
        .expect("legal create");

    let first = registry.create_task(&request);
    let replay = registry.create_task(&request);
    assert_eq!(first.outcome(), OperationOutcome::Accepted, "{first:?}");
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(replay.acknowledgement(), first.acknowledgement());
    assert_eq!(task_host.receivers_installed.load(Ordering::SeqCst), 1);
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_exact_creates_share_one_runnable_and_receipt() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(61, 62),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let task_host = Arc::new(TestTaskHost::default());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
    );

    establish(&registry, context);
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    );
    let descriptor = TaskDescriptor::try_new(
        identity,
        UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).expect("nonzero dop"),
        vec![PlanNodeId::new(1).expect("nonnegative node")],
        ExchangeTopology::default(),
        Arc::new(TestPlan(0x51)),
    )
    .expect("legal descriptor");

    let mut creates = Vec::new();
    for _ in 0..4 {
        let registry = Arc::clone(&registry);
        let descriptor = descriptor.clone();
        creates.push(std::thread::spawn(move || {
            registry.create_task(
                &CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
                    .expect("legal create"),
            )
        }));
    }
    let receipts: Vec<_> = creates
        .into_iter()
        .map(|create| create.join().expect("create thread"))
        .collect();
    let accepted: Vec<_> = receipts
        .iter()
        .filter(|receipt| receipt.outcome() == OperationOutcome::Accepted)
        .collect();
    assert_eq!(accepted.len(), 1, "{receipts:?}");
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.outcome() == OperationOutcome::Idempotent)
            .count(),
        3,
        "{receipts:?}"
    );
    for receipt in &receipts {
        assert_eq!(receipt.acknowledgement(), accepted[0].acknowledgement());
    }
    assert_eq!(task_host.receivers_installed.load(Ordering::SeqCst), 1);
    assert_eq!(task_host.capabilities_installed.load(Ordering::SeqCst), 1);
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(registry.counters().tasks_created, 1);
}

#[test]
fn conflicting_descriptor_is_refused_without_touching_the_live_task() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(71, 72),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let task_host = Arc::new(TestTaskHost::default());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
    );

    establish(&registry, context);
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    );
    let descriptor = |fingerprint| {
        TaskDescriptor::try_new(
            identity,
            UniqueId::new(1, 1),
            std::num::NonZeroUsize::new(1).expect("nonzero dop"),
            vec![PlanNodeId::new(1).expect("nonnegative node")],
            ExchangeTopology::default(),
            Arc::new(TestPlan(fingerprint)),
        )
        .expect("legal descriptor")
    };
    let accepted = registry.create_task(
        &CreateTask::try_new(
            TaskOperationId::new_v7(),
            context,
            descriptor(5),
            Vec::new(),
        )
        .expect("legal create"),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );

    let conflicting = registry.create_task(
        &CreateTask::try_new(
            TaskOperationId::new_v7(),
            context,
            descriptor(6),
            Vec::new(),
        )
        .expect("legal create"),
    );
    assert_eq!(conflicting.outcome(), OperationOutcome::CreateConflict);
    assert!(conflicting.acknowledgement().is_none());
    assert!(registry.has_live_task(identity));
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(task_host.receivers_removed.load(Ordering::SeqCst), 0);
}

#[test]
fn conflicting_descriptor_does_not_preempt_a_creation_in_progress() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(81, 82),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let install_gate = Arc::new(InstallGate::held());
    let task_host = Arc::new(TestTaskHost {
        install_gate: Some(Arc::clone(&install_gate)),
        ..TestTaskHost::default()
    });
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
    );

    establish(&registry, context);
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    );
    let descriptor = |fingerprint| {
        TaskDescriptor::try_new(
            identity,
            UniqueId::new(1, 1),
            std::num::NonZeroUsize::new(1).expect("nonzero dop"),
            vec![PlanNodeId::new(1).expect("nonnegative node")],
            ExchangeTopology::default(),
            Arc::new(TestPlan(fingerprint)),
        )
        .expect("legal descriptor")
    };
    let owner_request = CreateTask::try_new(
        TaskOperationId::new_v7(),
        context,
        descriptor(5),
        Vec::new(),
    )
    .expect("legal create");
    let owner_registry = Arc::clone(&registry);
    let owner = std::thread::spawn(move || owner_registry.create_task(&owner_request));
    install_gate.wait_until_entered();

    let conflicting = registry.create_task(
        &CreateTask::try_new(
            TaskOperationId::new_v7(),
            context,
            descriptor(9),
            Vec::new(),
        )
        .expect("legal create"),
    );
    assert_eq!(conflicting.outcome(), OperationOutcome::CreateConflict);

    install_gate.release();
    let accepted = owner.join().expect("owner create thread");
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert!(registry.has_live_task(identity));
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 1);
}
