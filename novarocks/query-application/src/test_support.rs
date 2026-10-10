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

//! Feature-gated integration harness for Native role-adapter tests.
//!
//! The harness exposes observed behavior while retaining every registry,
//! actor, attempt, output, join, and retirement authority inside the query
//! application crate. Production adapters cannot use this module because the
//! production dependency does not enable `test-support`.

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, QueryContextRef, ResultPacketSequence,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{
    CancellationReason, LocalResourceAuthority, ResourceConfig, RootWork, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::watch;

use crate::api::{
    ExecutionHandle, ExecutionOutput, QueryExecutionError, QueryResultStream, ResultField,
    ResultSchema,
};
use crate::coordination::{
    AdmissionIssueReceipt, AdmissionIssueSettlement, AttemptActivationIdentity,
    ContextStandDownSnapshot, LogicalExecutionActor, LogicalExecutionActorConfig,
    LogicalExecutionActorError, LogicalExecutionActorSnapshot, LogicalExecutionJoinReadiness,
    LogicalExecutionOutputTransfer, LogicalExecutionRegistration, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, RunningAttemptPermit,
};

use crate::api::result::{
    EndDelivery, QueryResultTransport, ResultDelivery, ResultDeliveryDisposition,
    ResultDeliveryReceipt,
};

/// Test-only observation of one move-only protocol delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestResultDeliveryDisposition {
    Completed,
    Failed(QueryExecutionError),
    Dropped,
}

/// Test-only receipt for the actor side of one protocol delivery.
pub struct TestResultDeliveryReceipt(ResultDeliveryReceipt);

impl TestResultDeliveryReceipt {
    pub async fn wait(self) -> TestResultDeliveryDisposition {
        match self
            .0
            .await
            .expect("test result delivery owner remains alive")
        {
            ResultDeliveryDisposition::Completed => TestResultDeliveryDisposition::Completed,
            ResultDeliveryDisposition::Failed(error) => {
                TestResultDeliveryDisposition::Failed(error)
            }
            ResultDeliveryDisposition::Dropped => TestResultDeliveryDisposition::Dropped,
        }
    }
}

/// Feature-gated producer for exercising a real [`QueryResultStream`] from a
/// role adapter without exposing production construction authority.
pub struct ResultStreamTestProducer {
    execution_id: QueryExecutionId,
    transport: QueryResultTransport,
    failure: watch::Sender<Option<QueryExecutionError>>,
    workload: WorkloadControl,
    root: Option<RootWork>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
}

impl ResultStreamTestProducer {
    pub fn open(
        execution_id: QueryExecutionId,
        fields: Vec<ResultField>,
        delivery_capacity: usize,
        resource_config: ResourceConfig,
    ) -> Result<
        (
            Self,
            ExecutionHandle,
            LocalResourceAuthority,
            TestResultDeliveryReceipt,
        ),
        QueryExecutionError,
    > {
        Self::open_with_carrier(
            execution_id,
            fields,
            delivery_capacity,
            resource_config,
            crate::api::ResultRowCarrier::relayed(
                novarocks_result_contract::RootOutputKind::ClientRows,
                Some(
                    novarocks_result_contract::ClientRowProfile::try_new(
                        novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                        novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
                    )
                    .expect("frozen client profile"),
                ),
            )?,
        )
    }

    pub fn open_with_carrier(
        execution_id: QueryExecutionId,
        fields: Vec<ResultField>,
        delivery_capacity: usize,
        resource_config: ResourceConfig,
        carrier: crate::api::ResultRowCarrier,
    ) -> Result<
        (
            Self,
            ExecutionHandle,
            LocalResourceAuthority,
            TestResultDeliveryReceipt,
        ),
        QueryExecutionError,
    > {
        let workload = WorkloadControl::try_new(WorkloadConfig::default(), resource_config)
            .expect("test result workload config must be valid");
        let capacity = Some({
            workload
                .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
                .expect("test result capacity")
        });
        workload
            .mark_ready()
            .expect("test result workload becomes ready");
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("test result root work is admitted");
        let window = capacity.as_ref().map(|capacity| {
            let class = match carrier {
                crate::api::ResultRowCarrier::Relayed {
                    kind: novarocks_result_contract::RootOutputKind::ClientRows,
                    ..
                } => novarocks_workload_control::ResultWindowClass::Client,
                _ => novarocks_workload_control::ResultWindowClass::Internal,
            };
            capacity
                .try_acquire(&root.owner.scope(), class)
                .expect("test root window")
        });
        let schema = ResultSchema::new(fields);
        let (transport, schema_receipt, failure, stream) = QueryResultStream::try_channel(
            execution_id.query_id(),
            schema,
            carrier,
            delivery_capacity,
        )?;
        let handle = ExecutionHandle::new(
            root.owner.cancellation_requester(),
            ExecutionOutput::Rows(stream),
        );
        let resources = workload.resources().unwrap();
        Ok((
            Self {
                execution_id,
                transport,
                failure,
                workload,
                root: Some(root),
                window,
                capacity,
            },
            handle,
            resources,
            TestResultDeliveryReceipt(schema_receipt),
        ))
    }

    pub fn result_capacity(&self) -> novarocks_workload_control::ResultCapacityHandle {
        self.capacity
            .as_ref()
            .expect("test relayed capacity")
            .clone()
    }

    /// Encoded ClientRows fixture transferred through the real V1 validator.
    pub async fn enqueue_client_body(
        &self,
        sequence: u64,
        body: Vec<u8>,
        rows: u64,
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        use novarocks_execution_contract::TaskIdentity;
        use novarocks_execution_contract::root_result::{
            RootReadOutcome, RootResultData, RootResultReply,
        };
        use novarocks_result_contract::{
            ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
        };
        use novarocks_types::{BackendProcessId, StageId, TaskId};
        let data = RootResultData::try_new(
            RootOutputKind::ClientRows,
            std::num::NonZeroU64::new(sequence.checked_add(1).ok_or_else(|| {
                crate::api::QueryExecutionError::new(
                    crate::api::QueryExecutionErrorKind::InvalidRequest,
                    "fixture sequence overflow",
                )
            })?)
            .unwrap(),
            bytes::Bytes::from(body),
            None,
        )
        .map_err(|error| {
            crate::api::QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        self.enqueue_segment(
            sequence,
            RootResultReply {
                root_task: TaskIdentity::new(
                    self.execution_id,
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind: RootOutputKind::ClientRows,
                accepted_consumed: sequence,
                outcome: RootReadOutcome::Data(data),
            },
            Some((
                ClientRowProfile::try_new(
                    RootProfileV1::SEGMENT_BYTES,
                    RootProfileV1::ROW_PAYLOAD_BYTES,
                )
                .unwrap(),
                ClientRowStreamCursor::default(),
            )),
            rows,
        )
        .await
    }

    /// Test-only transfer through the real move-only segment/receipt boundary.
    pub async fn enqueue_segment(
        &self,
        sequence: u64,
        reply: novarocks_execution_contract::root_result::RootResultReply,
        client_rows: Option<(
            novarocks_result_contract::ClientRowProfile,
            novarocks_result_contract::ClientRowStreamCursor,
        )>,
        rows: u64,
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        let window = self
            .window
            .as_ref()
            .expect("relayed test stream has a window");
        let backing = match &reply.outcome {
            novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                data.body().len() as u64 + 4096
            }
            _ => 4096,
        };
        let retained =
            crate::api::RetainedRootReply::try_new(reply, window.retain_alias(), backing).map_err(
                |error| {
                    QueryExecutionError::new(
                        crate::api::QueryExecutionErrorKind::InvalidRequest,
                        error.to_string(),
                    )
                },
            )?;
        let (delivery, receipt) = crate::api::RootSegmentDelivery::try_new(
            self.execution_id,
            ResultPacketSequence::new(sequence),
            retained,
            client_rows,
            rows,
        )?;
        let permit = self.transport.reserve_owned().await?;
        self.transport
            .enqueue(permit, ResultDelivery::Segment(delivery));
        Ok(TestResultDeliveryReceipt(receipt))
    }

    /// Component-only two-item resident window, using the original publish/take/freeze owners.
    /// This performs no Native fetch and proves no Native actor or task lifecycle.
    /// Both items have frontier zero and no piggyback End; no delivery receipt has completed.
    pub async fn enqueue_resident_client_pair(
        &self,
        replies: [novarocks_execution_contract::root_result::RootResultReply; 2],
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        use crate::api::{
            ResidentRootSegment, RetainedRootReply, RootRelayResidentWindow, RootReplyView,
        };
        use novarocks_result_contract::{
            ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
        };
        let invalid = || {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                "component resident pair differs from one contiguous ClientRows task",
            )
        };
        let window = self.window.as_ref().expect("relayed test window");
        let [first, next] = replies;
        if first.root_task != next.root_task
            || first.root_task.query_execution_id() != self.execution_id
            || [&first, &next].iter().any(|reply| {
                reply.kind != RootOutputKind::ClientRows
                    || reply.profile != RootProfileId::V1
                    || reply.accepted_consumed != 0
            })
        {
            return Err(invalid());
        }
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let resident = RootRelayResidentWindow::default();
        let mut cursor = ClientRowStreamCursor::new();
        let mut delivery = None;
        for (index, reply) in [first, next].into_iter().enumerate() {
            let backing = match &reply.outcome {
                novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                    if data.end_after_data().is_some() {
                        return Err(invalid());
                    }
                    data.body().len() as u64 + 4096
                }
                _ => return Err(invalid()),
            };
            let retained = std::sync::Arc::new(
                RetainedRootReply::try_new(reply, window.retain_alias(), backing)
                    .map_err(|_| invalid())?,
            );
            let RootReplyView::Data { sequence, body, .. } = retained.outcome() else {
                return Err(invalid());
            };
            if sequence.get() != index as u64 + 1 {
                return Err(invalid());
            }
            let after = cursor
                .validate_body(profile, body)
                .map_err(|_| invalid())?
                .after();
            let rows = after.completed_rows() - cursor.completed_rows();
            if !resident.publish(ResidentRootSegment {
                sequence,
                reply: retained.clone(),
                client_rows: Some((profile, cursor)),
                rows,
            }) {
                return Err(invalid());
            }
            if index == 0 {
                let _original_delivering = resident.take().ok_or_else(invalid)?;
                delivery = Some(crate::api::RootSegmentDelivery::try_new(
                    self.execution_id,
                    ResultPacketSequence::new(0),
                    retained,
                    Some((profile, cursor)),
                    rows,
                )?);
            }
            cursor = after;
        }
        cursor.validate_end().map_err(|_| invalid())?;
        let (delivery, receipt) = delivery.expect("first resident delivery");
        let permit = self.transport.reserve_owned().await?;
        self.transport.enqueue(
            permit,
            ResultDelivery::Segment(delivery.with_resident_window(resident)),
        );
        Ok(TestResultDeliveryReceipt(receipt))
    }

    pub async fn enqueue_end(&self, sequence: u64) -> TestResultDeliveryReceipt {
        let (delivery, receipt) =
            EndDelivery::success_eof(self.execution_id, ResultPacketSequence::new(sequence));
        let permit = self
            .transport
            .reserve_owned()
            .await
            .expect("test result stream remains connected");
        self.transport
            .enqueue(permit, ResultDelivery::End(delivery));
        TestResultDeliveryReceipt(receipt)
    }

    pub fn fail(&self, error: QueryExecutionError) {
        self.failure.send_replace(Some(error));
    }

    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        self.root
            .as_ref()
            .expect("test result root remains active")
            .owner
            .scope()
            .cancellation()
            .expect("test result scope remains observable")
            .reason()
    }

    pub fn finish(mut self) {
        drop(self.transport);
        drop(self.failure);
        let root = self.root.take().expect("test result root finishes once");
        root.owner.complete();
        root.business.release();
    }
}

/// Complete Query Application ownership for one actor-driven integration test.
#[must_use = "the test logical execution must converge and call finish"]
pub struct LogicalExecutionTestHarness {
    registry_owner: Option<LogicalExecutionRuntimeRegistry>,
    registry: LogicalExecutionRuntimeRegistryHandle,
    registration: Option<LogicalExecutionRegistration>,
    actor: Option<LogicalExecutionActor>,
    initial: Option<crate::coordination::AttemptInstantiationPermit>,
    running: Option<RunningAttemptPermit>,
    output: Option<LogicalExecutionOutputTransfer>,
}

impl std::fmt::Debug for LogicalExecutionTestHarness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogicalExecutionTestHarness")
            .field(
                "initial_execution",
                &self
                    .registration
                    .as_ref()
                    .map(LogicalExecutionRegistration::initial_execution),
            )
            .field("initial_pending", &self.initial.is_some())
            .field("running", &self.running.is_some())
            .finish_non_exhaustive()
    }
}

impl LogicalExecutionTestHarness {
    pub fn install(
        runtime: Handle,
        config: LogicalExecutionActorConfig,
        initial_execution: novarocks_types::QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> Result<Self, LogicalExecutionRuntimeRegistryError> {
        let registry_owner = LogicalExecutionRuntimeRegistry::new(runtime);
        let registry = registry_owner.handle();
        let (registration, initial, output) = registry
            .reserve(initial_execution, contexts)?
            .spawn_and_install(config)?
            .into_parts();
        let actor = registry.actor(&registration)?;
        Ok(Self {
            registry_owner: Some(registry_owner),
            registry,
            registration: Some(registration),
            actor: Some(actor),
            initial: Some(initial),
            running: None,
            output: Some(output),
        })
    }

    pub async fn activate_initial(
        &mut self,
    ) -> Result<AttemptActivationIdentity, LogicalExecutionActorError> {
        let initial = self
            .initial
            .take()
            .expect("the initial attempt may be activated only once");
        let running = self.actor().activate(initial.ready()).await?;
        let identity = running.identity();
        self.running = Some(running);
        Ok(identity)
    }

    /// Borrows the same narrow Native drive authority production active
    /// attempts receive. The running permit remains owned by this harness.
    pub fn native_attempt_drive(&self) -> crate::coordination::NativeAttemptDrive {
        crate::coordination::NativeAttemptDrive::new(
            self.running
                .as_ref()
                .expect("the test attempt must be activated before Native drive"),
        )
    }

    /// Abandons the active attempt so tests can drive its real stand-down and
    /// registry-shutdown path after exercising a borrowed Native drive.
    pub fn abandon_running_attempt(&mut self) {
        drop(
            self.running
                .take()
                .expect("the test attempt must be activated before abandonment"),
        );
    }

    /// Begins the exact admission issue used by Native Abort adapter tests and
    /// abandons the running permit so the actor must supervise stand-down.
    pub async fn begin_admission_issue_and_abandon(
        &mut self,
        request: AcquireQueryContextAdmissionTicket,
    ) -> Result<(AttemptActivationIdentity, AdmissionIssueReceipt), LogicalExecutionActorError>
    {
        let running = self
            .running
            .take()
            .expect("the test attempt must be activated before admission");
        let activation = running.identity();
        let result = running.begin_admission_issue(request).await;
        match result {
            Ok(receipt) => {
                drop(running);
                Ok((activation, receipt))
            }
            Err(error) => {
                self.running = Some(running);
                Err(error)
            }
        }
    }

    pub async fn settle_late_admission_issue(
        &self,
        activation: AttemptActivationIdentity,
        receipt: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<crate::coordination::AdmissionIssueDisposition, LogicalExecutionActorError> {
        self.actor()
            .settle_late_admission_issue(activation, receipt, settlement)
            .await
    }

    pub async fn stand_down_snapshot(
        &self,
        context: QueryContextRef,
    ) -> Result<Option<ContextStandDownSnapshot>, LogicalExecutionActorError> {
        self.actor().stand_down_snapshot(context).await
    }

    pub async fn actor_snapshot(
        &self,
    ) -> Result<LogicalExecutionActorSnapshot, LogicalExecutionActorError> {
        self.actor().snapshot().await
    }

    pub async fn join_readiness(
        &self,
    ) -> Result<LogicalExecutionJoinReadiness, LogicalExecutionRuntimeRegistryError> {
        self.registry.join_readiness(self.registration()).await
    }

    pub async fn observe_worker_stopped_and_context_fenced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.registry
            .observe_worker_stopped_and_context_fenced(self.registration(), context)
            .await
    }

    pub async fn observe_worker_process_replaced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.registry
            .observe_worker_process_replaced(self.registration(), context)
            .await
    }

    /// Starts bounded registry shutdown while retaining the owner in `self`.
    /// A timeout, runtime error, or cancellation of this future leaves the
    /// harness available for an exact retry with a later deadline.
    pub async fn finish_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionRuntimeShutdownError> {
        drop(self.output.take());
        drop(self.running.take());
        drop(self.initial.take());
        drop(self.actor.take());
        drop(self.registration.take());
        self.registry_owner
            .as_mut()
            .expect("the test registry owner may be shut down only once")
            .shutdown_until(deadline)
            .await
    }

    fn actor(&self) -> &LogicalExecutionActor {
        self.actor
            .as_ref()
            .expect("the test actor remains owned until finish")
    }

    fn registration(&self) -> &LogicalExecutionRegistration {
        self.registration
            .as_ref()
            .expect("the test registration remains owned until finish")
    }
}
