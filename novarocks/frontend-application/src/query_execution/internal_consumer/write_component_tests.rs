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

//! Child of write.rs: production has no test-only seed construction port.

use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime,
        fragment_transport::{
            DynamicFilterRead, DynamicFilterReadError, FinalTaskInfoRead, TaskResultTransport,
        },
    },
    query_execution::{
        internal_consumer::{
            attempt_handoff::component_tests::OriginalAttempt,
            test_fixture::{internal_write_description, offer_original_credentials},
        },
        internal_result_cpu::InternalResultCpuOwner,
        write_session::tests::{
            commit_fragment_bytes, fixture_with_outcome, known_committed, request_context,
        },
    },
};
use bytes::Bytes;
use novarocks_execution_contract::{
    DomainVersion, OperationOutcome, ResultPacketSequence, SafeDetail, TaskFailure,
    TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureReason},
    root_result::{RootReadOutcome, RootReadResponse, RootResultData, RootResultReply},
};
use novarocks_query_application::coordination::LogicalConclusion;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{LogicalExecutionNativePort, LogicalNativeOpenRequest, RetainedRootReply},
    coordination::{LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig},
    execution_error::ExecutionFailureInput,
};
use novarocks_result_contract::RootProfileId;
use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
use novarocks_workload_control::ResultWindowClass;
use std::{
    future::Future,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    task::Poll,
    time::{Duration, Instant},
};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Write component watchdog: {stage}"))
}
#[derive(Debug)]
struct ReservationOnly;
impl LogicalExecutionNativePort for ReservationOnly {
    fn bind_in(
        &self,
        _source: &mut Option<LogicalNativeOpenRequest>,
        _target: &mut Option<novarocks_query_application::api::LogicalNativeSession>,
    ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
        panic!("reserving an identity cannot bind Native work")
    }
}
struct UnavailableInfo;
impl TaskResultTransport for UnavailableInfo {
    fn final_task_info(
        &self,
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        Box::pin(async { Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone)) })
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("write consumer must not fetch diagnostic dynamic filters")
    }
}
fn original_failure() -> TaskFailure {
    TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(NonZeroU16::new(u16::MAX).unwrap(), NonZeroU16::MIN),
        ),
        SafeDetail::new("original fixed Write Task failure").unwrap(),
        TaskFailurePhase::Execution,
    )
}
async fn run_case(fail_writer: bool) {
    let (mut identity_owner, client) =
        crate::query_execution::logical_execution_test_profile::process(
            tokio::runtime::Handle::current(),
            Arc::new(ReservationOnly),
            QueryProcessNamespace::new(0x9b7),
            FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroU32::MIN,
                    Duration::from_secs(1),
                ),
            ),
        );
    let initial = InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
    let credentials = initial.credentials();
    let execution = credentials.execution();
    let context = initial
        .connector_request_context(request_context(), None)
        .unwrap();
    offer_original_credentials(context.vended_credential_lease_sink().unwrap().as_ref());
    // Consume the actual original Round source through its actual one-shot
    // drain. This neither seals TerminalOnly nor constructs a storage table.
    let source = credentials.take_source(execution).unwrap();
    let leases = source.test_into_credential_leases().unwrap();
    assert!(!leases.leases().is_empty());
    let native_storage = leases.into_storage();
    credentials.verify_native_storage(&native_storage).unwrap();
    assert!(credentials.take_source(execution).is_err());
    let (description, plan) = internal_write_description();
    let (mut attempt, resources, activation) = watchdog(
        "original admission and Establish",
        OriginalAttempt::new_bound(
            execution,
            Arc::new(UnavailableInfo),
            description.clone(),
            plan,
            true,
            Some(Arc::clone(&native_storage)),
            true,
        ),
    )
    .await;
    assert!(Arc::ptr_eq(&attempt.native_credentials, &native_storage));
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = Arc::clone(&fixture.session);
    let window = attempt
        .capacity
        .try_acquire(&attempt.scope, ResultWindowClass::Internal)
        .unwrap();
    let capacity_binding =
        QueryResultCapacityBinding::try_new(&attempt.scope, window.retain_alias()).unwrap();
    drop(window);
    let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    // This child module exercises the closed consumer. The frozen-template
    // from_frozen constructor remains a separately named integration gate.
    let frozen_targets =
        [novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap()];
    let seed = WriteComputationSeed {
        contract: RootWriteDecodeContract::try_new(&frozen_targets, session.targets()).unwrap(),
        decode: session.computation_decode(&frozen_targets).unwrap(),
        session: Arc::clone(&session),
        initial: Arc::clone(&credentials),
    };
    session
        .retain_computation_storage_resolver(credentials.terminal_storage_resolver())
        .unwrap();
    let (consumer, offers, mut output) = write_computation_consumer(
        &description,
        plan,
        seed,
        DecodedConsumerRuntime {
            computation: None,
            binding: capacity_binding.clone(),
            cpu: cpu_owner.runtime(),
            runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            cancellation: QueryCancellationView::governed(
                attempt.scope.cancellation().unwrap(),
                None,
            ),
        },
    )
    .unwrap();
    let custody = output.cpu_custody();
    let mut producer = offers.reserve(execution).unwrap();
    producer
        .stage(execution, attempt.root, plan, resources)
        .unwrap();
    attempt.bind_original_drive(&mut producer);
    let body =
        crate::query_execution::write_result::tests::component_write_body(commit_fragment_bytes());
    let backing = body.len() as u64;
    let segment = Arc::new(
        RetainedRootReply::try_new(
            RootReadResponse::Reply(RootResultReply {
                root_task: attempt.root,
                profile: RootProfileId::V1,
                kind: RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1),
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1),
                        NonZeroU64::MIN,
                        Bytes::from(body),
                        None,
                    )
                    .unwrap(),
                ),
            }),
            capacity_binding.window_alias(),
            backing,
        )
        .unwrap(),
    );
    let root_status = attempt
        .round
        .execution()
        .task(attempt.root.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let end = RootResultEnd {
        sequence: NonZeroU64::new(2).unwrap(),
        output_rows: 2,
    };
    let terminal;
    {
        let logical = &attempt.logical;
        let consume = logical.run_registered_internal_consumer(Arc::clone(&consumer), activation);
        let deliver = async {
            logical
                .deliver_original_internal_segment(ResultPacketSequence::new(0), segment, 2)
                .await
                .unwrap();
            logical
                .deliver_original_internal_end(attempt.root, root_status, end)
                .await
                .unwrap();
        };
        tokio::pin!(consume, deliver);
        watchdog("EOF CPU receipt and actual End.complete before writer proof", async {
            tokio::select! { biased;
                result = consume.as_mut() => panic!("Write finished before declared writers: {result:?}"),
                () = deliver.as_mut() => {},
            }
        }).await;
        assert!(logical.original_internal_outcome().borrow().is_none());
        assert_eq!(
            session.accumulated_row_count().unwrap(),
            0,
            "EOF decoded the original generation; union is forbidden before writer proof"
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        std::future::poll_fn(|cx| {
            assert!(consume.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let tail = attempt.pending_upstream.as_ref().unwrap();
        tail.observe_original_consumed_packets(&mut attempt.round, 1);
        let expected = if fail_writer {
            let selected = tail.publish_original_failure(&mut attempt.round, original_failure());
            Some(ExecutionFailureInput::from_selected_task(&selected).unwrap())
        } else {
            tail.publish_original_writer_success(&mut attempt.round);
            None
        };
        // Observe real TE failure before any QA failure-watch injection. The
        // tracker must retain and project the exact original Task fact.
        let result = watchdog(
            "original writer proof then second CPU union receipt or exact failure",
            consume.as_mut(),
        )
        .await;
        if let Some(expected) = expected {
            let error = result.expect_err("original failed writer cannot reach Ready");
            assert_eq!(error.execution_failure(), Some(&expected));
            terminal = novarocks_query_application::api::NativeAttemptTerminal::Failed(
                novarocks_query_application::api::NativeAttemptPreparationFailure::from_input(
                    expected,
                ),
            );
            assert_eq!(session.accumulated_row_count().unwrap(), 0);
            assert_eq!(
                logical.actor_snapshot().await.unwrap().conclusion,
                Some(LogicalConclusion::Failed)
            );
            assert!(!matches!(
                *logical.original_internal_outcome().borrow(),
                Some(Ok(_))
            ));
        } else {
            result.unwrap();
            terminal = novarocks_query_application::api::NativeAttemptTerminal::Completed;
            let token = logical
                .original_internal_outcome()
                .borrow()
                .clone()
                .unwrap()
                .unwrap();
            let ready = output.take_ready(token).unwrap();
            assert_eq!(ready.ready, token);
            assert_eq!(ready.ready.end(), end);
            assert_eq!(ready.ready.root(), attempt.root);
            assert_eq!(ready.ready.activation().execution(), execution);
            assert_eq!(ready.accepted.row_count(), 3);
            assert!(!ready.accepted.is_empty());
            assert!(ready.accepted.belongs_to(&session));
            assert!(Arc::ptr_eq(&ready.session, &session));
            assert!(Arc::ptr_eq(&ready.credentials, &native_storage));
            assert_eq!(session.accumulated_row_count().unwrap(), 3);
            assert!(output.take_ready(token).is_err());
            assert_eq!(
                fixture.recorded.lock().unwrap().finish,
                0,
                "Ready is stable union custody, not Provider publication"
            );
            assert_eq!(fixture.recorded.lock().unwrap().abort, 0);
            drop(ready);
        }
    }
    let tail = attempt.pending_upstream.take().unwrap();
    tail.begin_original_convergence(&mut attempt.round, Some(&terminal));
    attempt.release = tail.complete(&mut attempt.round, &attempt.acks);
    watchdog("original CPU custody actual exit", custody.join_closed())
        .await
        .unwrap();
    drop((
        custody,
        output,
        consumer,
        producer,
        offers,
        capacity_binding,
        session,
        fixture,
    ));
    assert_eq!(attempt.capacity.snapshot().held_positions, [0; 4]);
    watchdog("original Task/Native/QA owners drain", attempt.finish()).await;
    drop((context, native_storage, credentials, initial));
    watchdog(
        "original finite CPU executor actual exit",
        cpu_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    watchdog(
        "reservation-only QA owner actual exit",
        identity_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn fixed_write_eof_then_original_writer_proof_then_second_cpu_union_reaches_ready() {
    run_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn fixed_write_original_te_task_failure_after_end_keeps_exact_cause_and_no_union() {
    run_case(true).await;
}
