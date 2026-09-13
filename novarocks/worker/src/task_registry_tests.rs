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
use novarocks_execution_contract::task_execution::lease::LeaseValidFor;
use novarocks_execution_contract::task_execution::operation::{
    AcquireQueryContextAdmissionTicket, CreateTask, CredentialUpdate, EstablishQueryContext,
    OperationOutcome, UpdateQueryContext,
};
use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
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
struct TestPlan;

impl CodecOwnedContent for TestPlan {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([0x51; 16])
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
    submitted: AtomicUsize,
}

impl TaskExecutionHost for TestTaskHost {
    fn close_context_admission(&self, _context: QueryContextRef) {}

    fn forget_context_admission(&self, _context: QueryContextRef) {}

    fn install_receiver(&self, _descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        Ok(())
    }

    fn remove_receiver(&self, _descriptor: &TaskDescriptor) {}

    fn install_inbound_capability(
        &self,
        _descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
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
        Arc::new(TestPlan),
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
