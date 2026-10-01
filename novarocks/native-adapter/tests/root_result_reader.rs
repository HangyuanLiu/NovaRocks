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

//! Real Worker context/admission tests for the transport-independent reader.
//! These tests install no RPC and make no HTTP/H2 ownership claim.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{
    ResultWriteAdmission, ResultWriteCredit, RootResultWriteSpec,
};
use novarocks_execution_contract::TaskOutputFacts;
use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
use novarocks_execution_contract::task_execution::creation::{
    CreationContent, FrozenBytes, PreparedTaskFacts, TaskCreationInput,
};
use novarocks_execution_contract::task_execution::descriptor::{
    ExchangeTopology, FragmentSinkKind, TaskDescriptor,
};
use novarocks_execution_contract::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    PlanNodeId,
};
use novarocks_execution_contract::task_execution::identity::{
    QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::lease::{LeaseSequence, LeaseValidFor};
use novarocks_execution_contract::task_execution::operation::{
    AcquireQueryContextAdmissionTicket, CreateTask, CredentialUpdate, EstablishQueryContext,
    OperationOutcome, QuiesceQueryContext, ReleaseQueryContext, RenewQueryExecutionLease,
    TaskDomainUpdate, UpdateQueryContext,
};
use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_native_adapter::root_result_reader::{
    NativeRootReadRefusal, NativeRootReadResponse, NativeRootResultReader, NativeRootResultReply,
};
use novarocks_result_contract::{
    ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
    RenderPresentation, RootOutputContract, RootOutputKind, RootProfileId, RootProfileV1,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId, StageId, TaskId, UniqueId,
};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use novarocks_worker::root_result_channel::RootResultChannel;
use novarocks_worker::{
    HostRejection, ManualClock, PreparedTaskInstallation, QueryContextHost,
    ReleasedContextEvidence, RunnableTask, SharedFactsRequest, TaskExecutionHost,
    TaskExecutionMetrics, TaskExecutionPorts, TaskExecutionRegistry, TaskExecutionRegistryConfig,
    TaskProtocolEvent, TaskProtocolObserver, TaskResultLifecycle, TaskStatusReporter,
    WorkerMonotonicClock, WorkerResultRetainedLimits,
};

const FIXED: usize = RootProfileV1::SCHEMA_BACKING_BYTES;
const SEGMENT: usize = RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES;
const COPY: usize = 2 * SEGMENT;
const PROCESS: usize = 512 * 1024 * 1024;

#[derive(Debug)]
struct Assignment;
impl CreationContent for Assignment {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
    }
    fn encoded_len(&self) -> usize {
        8
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
#[derive(Debug)]
struct Content;
impl CodecOwnedContent for Content {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([1; 16])
    }
    fn encoded_len(&self) -> usize {
        16
    }
}
struct Secret;
impl ConfidentialContent for Secret {
    fn encoded_len(&self) -> usize {
        16
    }
    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        other.encoded_len() == 16
    }
}
struct ContextHost;
impl QueryContextHost for ContextHost {
    fn materialize(&self, _: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        Ok(())
    }
    fn release(&self, _: QueryContextRef) -> ReleasedContextEvidence {
        ReleasedContextEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: QueryContextRef,
        _: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        Ok(())
    }
}
struct Noop;
impl TaskProtocolObserver for Noop {
    fn observe(&self, _: TaskProtocolEvent) {}
}
impl TaskResultLifecycle for Noop {
    fn discard_task(&self, _: TaskIdentity) {}
    fn retire_task_result(&self, _: TaskIdentity) {}
}
impl TaskExecutionMetrics for Noop {
    fn record_task_created(&self) {}
}
#[derive(Debug)]
struct Runnable;
impl RunnableTask for Runnable {
    fn commit_creation(&self) {}
    fn cancel(&self, _: CancelReason) {}
    fn abort(&self, _: AbortCause) {}
}
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }
    fn entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |s| !s.0)
            .unwrap();
        assert!(state.0 && !timeout.timed_out());
    }
    fn open(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}
struct Host {
    root: Mutex<Option<Arc<RootResultChannel>>>,
    reporter: Mutex<Option<TaskStatusReporter>>,
    gate: Option<Arc<Gate>>,
}
impl TaskExecutionHost for Host {
    fn close_context_admission(&self, _: QueryContextRef) {}
    fn retire_context_execution(&self, _: QueryContextRef) {}
    fn forget_context_admission(&self, _: QueryContextRef) {}
    fn install_receiver(
        &self,
        _: &TaskDescriptor,
        _: TaskCreationInput,
    ) -> Result<PreparedTaskInstallation, HostRejection> {
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        PreparedTaskInstallation::new(
            PreparedTaskFacts::new(FragmentSinkKind::Result),
            self.root.lock().unwrap().take(),
        )
    }
    fn remove_receiver(&self, _: &TaskDescriptor) {}
    fn install_inbound_capability(&self, _: &TaskDescriptor) -> Result<(), HostRejection> {
        Ok(())
    }
    fn remove_inbound_capability(&self, _: &TaskDescriptor) {}
    fn submit_runnable(
        &self,
        _: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        *self.reporter.lock().unwrap() = Some(reporter);
        Ok(Arc::new(Runnable))
    }
    fn apply_task_domain(
        &self,
        _: &TaskDescriptor,
        _: &TaskDomainUpdate,
    ) -> Result<Option<u64>, HostRejection> {
        Ok(None)
    }
}
struct Fixture {
    registry: Arc<TaskExecutionRegistry>,
    reader: NativeRootResultReader,
    root: Arc<RootResultChannel>,
    budget: Arc<ResultRetainedBudget>,
    host: Arc<Host>,
    clock: Arc<ManualClock>,
    context: QueryContextRef,
}
impl Fixture {
    fn new(count_only: bool) -> Self {
        let fixture = Self::uninstalled(count_only, None);
        fixture.install();
        fixture
    }
    fn uninstalled(count_only: bool, gate: Option<Arc<Gate>>) -> Self {
        let backend = BackendProcessId::new_v7();
        let execution =
            QueryExecutionId::new(QueryId::new(32100, 32101), AttemptId::new(1).unwrap()).unwrap();
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            backend,
        );
        let output = if count_only {
            FrozenRootOutput::CountOnly
        } else {
            FrozenRootOutput::ClientRows(
                ClientRenderSchema::try_new(
                    vec![RenderColumn {
                        source_ordinal: 0,
                        source_slot: Some(1),
                        name: "v".into(),
                        field: RenderField {
                            presentation: RenderPresentation::ScalarText,
                            nullable: false,
                            native_type: NativeRenderType::SignedInteger(64),
                        },
                    }],
                    1,
                )
                .unwrap(),
            )
        };
        let limits = WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, PROCESS).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let root = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: identity,
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
            },
            Arc::clone(&budget),
            limits,
        )
        .unwrap();
        let host = Arc::new(Host {
            root: Mutex::new(Some(Arc::clone(&root))),
            reporter: Mutex::new(None),
            gate,
        });
        let clock = Arc::new(ManualClock::new());
        let registry = TaskExecutionRegistry::new(
            TaskExecutionRegistryConfig::for_process(backend, 17, 9),
            Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
            Arc::new(ContextHost),
            Arc::clone(&host) as Arc<dyn TaskExecutionHost>,
            TaskExecutionPorts::new(Arc::new(Noop), Arc::new(Noop), Arc::new(Noop)),
        );
        let lease = || LeaseValidFor::new(Duration::from_secs(10)).unwrap();
        let ticket = registry
            .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
                TaskOperationId::new_v7(),
                context,
                lease(),
                NativeCompatibilityId::new([0x71; 32]),
                registry.admission_epoch_capability(),
            ))
            .acknowledgement()
            .unwrap()
            .ticket_id();
        assert_eq!(
            registry
                .update_query_context(&UpdateQueryContext::Establish(EstablishQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                    ticket,
                    Arc::new(Content),
                    Arc::new(Content),
                    Arc::new(Content),
                    CredentialUpdate::new(
                        CredentialLeaseId::new(1),
                        CredentialEpoch::FIRST,
                        Arc::new(Secret)
                    ),
                    lease()
                )))
                .outcome(),
            OperationOutcome::Accepted
        );
        Self {
            reader: NativeRootResultReader::new(Arc::clone(&registry)),
            registry,
            root,
            budget,
            host,
            clock,
            context,
        }
    }
    fn install(&self) {
        let descriptor = TaskDescriptor::try_new(
            self.root.spec().task,
            UniqueId::new(1, 1),
            NonZeroUsize::new(1).unwrap(),
            vec![PlanNodeId::new(1).unwrap()],
            ExchangeTopology::default(),
        )
        .unwrap();
        let create = CreateTask::try_new(
            TaskOperationId::new_v7(),
            self.context,
            descriptor,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            self.registry
                .create_task(
                    &create,
                    TaskCreationInput::new(
                        FrozenBytes::freeze(Bytes::from_static(b"root")),
                        Box::new(Assignment)
                    )
                )
                .outcome(),
            OperationOutcome::Accepted
        );
    }
    fn request(&self, wanted: Option<u64>, consumed: u64, wait: u64) -> RootResultRead {
        RootResultRead::try_new(
            self.root.spec().task,
            self.root.spec().contract.profile(),
            self.root.spec().contract.kind(),
            wanted.map(|w| NonZeroU64::new(w).unwrap()),
            consumed,
            Duration::from_millis(wait),
        )
        .unwrap()
    }
    fn finish(&self) {
        let producer = self.root.start_producer().unwrap();
        self.root.note_rows(1).unwrap();
        self.root.request_finish().unwrap();
        if self.root.spec().contract.kind() == RootOutputKind::CountOnly {
            self.root.publish_end().unwrap();
        } else {
            let mut builder = self.root.try_segment().unwrap().unwrap();
            builder.output()[..6].copy_from_slice(b"\x02\0\0\0\x011");
            self.root.publish_segment(builder, 6, true).unwrap();
        }
        drop(producer);
        let reporter = self.host.reporter.lock().unwrap().as_ref().unwrap().clone();
        reporter.running();
        reporter.finished(TaskOutputFacts::new(true));
        reporter.release_output();
        reporter.note_actual_stopped();
        reporter.note_resources_converged();
        assert_eq!(self.registry.advance_deadlines().tasks_retired, 1);
    }
    fn fill_process(&self, bytes: usize) -> ResultWriteCredit {
        granted(self.budget.try_reserve_process(bytes).unwrap())
    }
    fn seal(&self) {
        assert_eq!(
            self.registry
                .quiesce_query_context(&QuiesceQueryContext::new(
                    TaskOperationId::new_v7(),
                    self.context
                ))
                .outcome(),
            OperationOutcome::Accepted
        );
        assert_eq!(
            self.registry
                .release_query_context(&ReleaseQueryContext::new(
                    TaskOperationId::new_v7(),
                    self.context
                ))
                .outcome(),
            OperationOutcome::Accepted
        );
    }
}
fn granted(admission: ResultWriteAdmission) -> ResultWriteCredit {
    match admission {
        ResultWriteAdmission::Granted(credit) => credit,
        ResultWriteAdmission::Blocked => panic!("expected capacity"),
    }
}
fn owned(response: NativeRootReadResponse) -> NativeRootResultReply {
    match response {
        NativeRootReadResponse::Owned(reply) => reply,
        NativeRootReadResponse::Refused(reason) => panic!("read refused: {reason:?}"),
        NativeRootReadResponse::AwaitTerminalControl { accepted_consumed } => {
            panic!("sealed at {accepted_consumed}")
        }
    }
}
async fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn context_read_survives_task_retirement_and_horizon() {
    let fixture = Fixture::new(false);
    fixture.finish();
    for sequence in 1..=25 {
        fixture.clock.advance(Duration::from_secs(5));
        assert_eq!(
            fixture
                .registry
                .update_query_context(&UpdateQueryContext::RenewLease(
                    RenewQueryExecutionLease::new(
                        TaskOperationId::new_v7(),
                        fixture.context,
                        LeaseSequence::new(sequence),
                        LeaseValidFor::new(Duration::from_secs(10)).unwrap()
                    )
                ))
                .outcome(),
            OperationOutcome::Accepted
        );
        fixture.registry.advance_deadlines();
    }
    let request = fixture.request(Some(1), 0, 1);
    for _ in 0..2 {
        let reply = owned(fixture.reader.read(&request).await);
        let RootReadOutcome::Data(data) = &reply.reply().outcome else {
            panic!("expected Data")
        };
        assert_eq!(data.body().as_ref(), b"\x02\0\0\0\x011");
        assert_eq!(data.end_after_data().unwrap().output_rows, 1);
        assert_eq!(reply.reply().accepted_consumed, 0);
    }
    let ack = owned(fixture.reader.read(&fixture.request(None, 2, 1)).await);
    assert_eq!(ack.reply().accepted_consumed, 2);
    assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
}

#[tokio::test]
async fn count_only_end_and_ack_only_use_typed_reply() {
    let fixture = Fixture::new(true);
    fixture.finish();
    let end = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    assert!(matches!(end.reply().outcome, RootReadOutcome::End(e) if e.output_rows == 1));
    drop(end);
    let ack = owned(fixture.reader.read(&fixture.request(None, 1, 1)).await);
    assert_eq!(
        ack.ownership().backing_capacity_bytes(),
        2 * RootProfileV1::ENVELOPE_BYTES
    );
    assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
}

#[tokio::test]
async fn long_poll_cancellation_releases_original_admission_and_pregrants() {
    let fixture = Fixture::new(false);
    let request = fixture.request(Some(1), 0, 1000);
    let mut read = Box::pin(fixture.reader.read(&request));
    pending(read.as_mut()).await;
    assert!(!fixture.root.physical_idle());
    drop(read);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(PROCESS - FIXED));
}

#[tokio::test]
async fn two_send_positions_follow_last_exported_owner() {
    let fixture = Fixture::new(false);
    let request = fixture.request(Some(1), 0, 1);
    let first = owned(fixture.reader.read(&request).await);
    let owner = first.ownership();
    let second = owned(fixture.reader.read(&request).await);
    drop(first);
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::Busy)
    ));
    drop(owner);
    let third = owned(fixture.reader.read(&request).await);
    drop(second);
    drop(third);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn full_copy_credit_stays_until_actual_owner_drop() {
    let fixture = Fixture::new(false);
    let reply = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    let owner = reply.ownership();
    assert_eq!(owner.backing_capacity_bytes(), COPY);
    drop(reply);
    let filler = fixture.fill_process(PROCESS - FIXED - COPY);
    assert!(matches!(
        fixture.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    assert!(!fixture.root.physical_idle());
    drop(owner);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn shared_process_and_root_pregrants_refuse_without_leaking_read_positions() {
    let fixture = Fixture::new(false);
    let request = fixture.request(Some(1), 0, 1);
    let process = fixture.fill_process(PROCESS - FIXED - COPY + 1);
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::BackingCapacity)
    ));
    assert!(fixture.root.physical_idle());
    drop(process);
    let root = granted(
        fixture
            .root
            .try_reserve(256 * 1024 * 1024 - FIXED - COPY + 1)
            .unwrap(),
    );
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::BackingCapacity)
    ));
    drop(root);
    drop(owned(fixture.reader.read(&request).await));
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn full_budget_ack_only_uses_fixed_control_metadata() {
    let fixture = Fixture::new(false);
    fixture.finish();
    let data = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    drop(data);
    let filler = fixture.fill_process(PROCESS - FIXED - SEGMENT);
    let ack = owned(fixture.reader.read(&fixture.request(None, 1, 1)).await);
    assert_eq!(ack.reply().accepted_consumed, 1);
    assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
    assert_eq!(
        ack.ownership().backing_capacity_bytes(),
        2 * RootProfileV1::ENVELOPE_BYTES
    );
    drop(ack);
    drop(fixture.fill_process(SEGMENT));
    drop(filler);
}

#[tokio::test]
async fn fixed_metadata_refusal_does_not_apply_ack_or_keep_admission() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    // Reserve almost all fixed metadata, independently of data capacity.
    let metadata = fixture.root.try_reserve_metadata(FIXED - 8192).unwrap();
    let request = fixture.request(None, 1, 1);
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::MetadataCapacity)
    ));
    assert_eq!(fixture.root.snapshot().consumed_through, 0);
    drop(metadata);
    assert!(
        !fixture.root.physical_idle(),
        "unacknowledged Data remains owned"
    );
    let ack = owned(fixture.reader.read(&request).await);
    assert_eq!(ack.reply().accepted_consumed, 1);
    drop(ack);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(PROCESS - FIXED));
}

#[tokio::test]
async fn two_ack_only_sends_share_one_fixed_reservation_each() {
    let fixture = Fixture::new(false);
    let filler = fixture.fill_process(PROCESS - FIXED);
    let request = fixture.request(None, 0, 1);
    let first = owned(fixture.reader.read(&request).await);
    let second = owned(fixture.reader.read(&request).await);
    assert_eq!(first.ownership().backing_capacity_bytes(), 8192);
    assert_eq!(second.ownership().backing_capacity_bytes(), 8192);
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::Busy)
    ));
    drop(first);
    drop(second);
    drop(filler);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn ack_and_fetch_retire_original_backing_before_copy_pregrant() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let filler = fixture.fill_process(PROCESS - FIXED - COPY);
    let end = owned(fixture.reader.read(&fixture.request(Some(2), 1, 1)).await);
    assert_eq!(end.reply().accepted_consumed, 1);
    assert!(matches!(end.reply().outcome, RootReadOutcome::End(_)));
    drop(end);
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn acknowledged_original_alias_still_blocks_copy_pregrant() {
    let fixture = Fixture::new(false);
    fixture.finish();
    let reply = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    let alias = match &reply.reply().outcome {
        RootReadOutcome::Data(data) => data.body().clone(),
        _ => panic!("Data"),
    };
    drop(reply);
    let filler = fixture.fill_process(PROCESS - FIXED - COPY);
    let request = fixture.request(Some(2), 1, 1);
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::BackingCapacity)
    ));
    assert_eq!(
        fixture.root.snapshot().consumed_through,
        1,
        "ACK applied despite copy refusal"
    );
    drop(alias);
    let end = owned(fixture.reader.read(&request).await);
    assert_eq!(end.reply().accepted_consumed, 1);
    drop(end);
    drop(filler);
}

#[tokio::test]
async fn release_wakes_admitted_long_poll_and_late_route_uses_real_ack() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    drop(owned(
        fixture.reader.read(&fixture.request(None, 1, 1)).await,
    ));
    let request = fixture.request(Some(3), 1, 1000);
    let mut read = Box::pin(fixture.reader.read(&request));
    pending(read.as_mut()).await;
    fixture.seal();
    assert_eq!(
        fixture.registry.context_state(fixture.context),
        QueryContextState::Releasing
    );
    let reply = owned(
        tokio::time::timeout(Duration::from_millis(100), read)
            .await
            .unwrap(),
    );
    assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
    assert_eq!(reply.reply().accepted_consumed, 1);
    assert!(matches!(
        fixture.reader.read(&fixture.request(Some(3), 2, 1)).await,
        NativeRootReadResponse::AwaitTerminalControl {
            accepted_consumed: 1
        }
    ));
    assert_eq!(fixture.root.snapshot().consumed_through, 1);
    drop(reply);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(fixture.context),
        QueryContextState::TerminalRetained
    );
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn ack_retirement_callback_seals_before_send_growth_and_returns_actual_frontier() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let fired = Arc::new(AtomicBool::new(false));
    let once = Arc::clone(&fired);
    let registry = Arc::downgrade(&fixture.registry);
    let context = fixture.context;
    let subscription = fixture
        .root
        .writable_observable()
        .try_subscribe(Arc::new(move || {
            if once.swap(true, Ordering::AcqRel) {
                return;
            }
            let registry = registry.upgrade().unwrap();
            assert_eq!(
                registry
                    .quiesce_query_context(&QuiesceQueryContext::new(
                        TaskOperationId::new_v7(),
                        context
                    ))
                    .outcome(),
                OperationOutcome::Accepted
            );
            assert_eq!(
                registry
                    .release_query_context(&ReleaseQueryContext::new(
                        TaskOperationId::new_v7(),
                        context
                    ))
                    .outcome(),
                OperationOutcome::Accepted
            );
        }))
        .unwrap();
    assert!(matches!(
        fixture.reader.read(&fixture.request(Some(2), 1, 1)).await,
        NativeRootReadResponse::AwaitTerminalControl {
            accepted_consumed: 1
        }
    ));
    assert!(fired.load(Ordering::Acquire));
    assert_eq!(fixture.root.snapshot().consumed_through, 1);
    assert!(
        fixture.root.physical_idle(),
        "original admission and metadata rolled back"
    );
    drop(subscription);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
}

#[tokio::test]
async fn identity_and_output_mismatch_are_typed_without_fallback() {
    let fixture = Fixture::new(false);
    let task = fixture.root.spec().task;
    let unknown = TaskIdentity::new(
        task.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(2).unwrap(),
        task.backend_process_id(),
    );
    let request = RootResultRead::try_new(
        unknown,
        RootProfileId::V1,
        RootOutputKind::ClientRows,
        NonZeroU64::new(1),
        0,
        Duration::from_millis(1),
    )
    .unwrap();
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::UnknownRoot)
    ));
    let request = RootResultRead::try_new(
        task,
        RootProfileId::V1,
        RootOutputKind::CountOnly,
        NonZeroU64::new(1),
        0,
        Duration::from_millis(1),
    )
    .unwrap();
    assert!(matches!(
        fixture.reader.read(&request).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::Mismatch)
    ));
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn installation_in_progress_is_preparing_without_new_read_holder() {
    let gate = Arc::new(Gate::default());
    let fixture = Arc::new(Fixture::uninstalled(false, Some(Arc::clone(&gate))));
    let creator = Arc::clone(&fixture);
    let job = std::thread::spawn(move || creator.install());
    gate.entered();
    assert!(matches!(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
        NativeRootReadResponse::Refused(NativeRootReadRefusal::Preparing)
    ));
    assert!(fixture.root.physical_idle());
    gate.open();
    job.join().unwrap();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
}
