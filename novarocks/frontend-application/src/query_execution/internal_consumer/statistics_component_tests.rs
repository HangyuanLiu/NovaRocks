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

//! Fixed statistics consumers over the actual original QA/TaskRound fixture.

use super::*;
use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::{
    internal_consumer::{
        decoded::{DecodedConsumerRuntime, statistics_computation_consumer},
        test_fixture::internal_statistics_description,
    },
    internal_result_cpu::InternalResultCpuOwner,
    statistics::{StatisticsCollectionProgram, StatisticsExecutionPolicy},
};
use arrow::{
    array::{ArrayRef, BinaryArray, Int32Builder, ListBuilder, MapBuilder, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use novarocks_execution_contract::{
    ResultPacketSequence, SafeDetail, TaskFailure, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureReason},
    root_result::{RootReadOutcome, RootReadResponse, RootResultData, RootResultReply},
};
use novarocks_query_application::coordination::LogicalConclusion;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{QueryExecutionError, RetainedRootReply},
    cancellation::QueryCancellationView,
    execution_error::ExecutionFailureInput,
};
use novarocks_result_contract::{InternalResultDomain, RootOutputKind, RootProfileId};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableHandle, StatisticsArtifactIdentity,
    StatisticsRequiredAggregation, StatisticsScanColumn,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_workload_control::ResultWindowClass;
use std::{future::Future, num::NonZeroU16};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Statistics component watchdog: {stage}"))
}

fn original_program() -> StatisticsCollectionProgram {
    let identity = StatisticsArtifactIdentity::try_new(vec![1], "test/blob").unwrap();
    let required = StatisticsRequiredAggregation::try_new(
        StatisticsScanColumn::try_new(0, "v", FunctionValueType::new(DataType::Int64, false))
            .unwrap(),
        "$test_stat",
        identity,
    )
    .unwrap();
    let program = StatisticsCollectionProgram::try_new(
        ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("statistics_component").unwrap(),
            Bytes::from_static(b"table"),
        )
        .unwrap(),
        Some(7),
        vec![required],
        StatisticsExecutionPolicy::try_new(Duration::from_secs(30)).unwrap(),
    )
    .unwrap();
    assert_eq!(program.required_aggregations().len(), 1);
    program
}

/// The ordinary Backend statistics encoder is the only record author. The
/// fixture uses the same four-column relation as statistics.rs's decoder tests.
fn original_statistics_body() -> Vec<u8> {
    use novarocks_native_adapter::root_statistics_codec::{
        StatisticsArtifactEncoder, StatisticsCodecStatus, StatisticsCodecTotals,
    };
    let mut fields = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    fields.values().append_value(1);
    fields.append(true);
    let fields = Arc::new(fields.finish()) as ArrayRef;
    let types = Arc::new(StringArray::from(vec!["test/blob"])) as ArrayRef;
    let bodies = Arc::new(BinaryArray::from_iter_values([
        b"original artifact".as_slice()
    ])) as ArrayRef;
    let mut properties = MapBuilder::new(
        Some(arrow::array::MapFieldNames {
            entry: "entries".to_owned(),
            key: "key".to_owned(),
            value: "value".to_owned(),
        }),
        arrow::array::StringBuilder::new(),
        arrow::array::StringBuilder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    properties.append(true).unwrap();
    let properties = Arc::new(properties.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("input_fields", fields.data_type().clone(), false),
        Field::new("blob_type", DataType::Utf8, false),
        Field::new("body", DataType::Binary, true),
        Field::new("properties", properties.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![fields, types, bodies, properties]).unwrap();
    let mut encoder =
        StatisticsArtifactEncoder::try_new(batch, StatisticsCodecTotals::default()).unwrap();
    let mut body = Vec::new();
    let mut output = vec![0_u8; 9];
    loop {
        let turn = encoder.step(&mut output).unwrap();
        body.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == StatisticsCodecStatus::InputComplete {
            break;
        }
    }
    body
}

#[derive(Clone, Copy)]
enum Case {
    Success,
    MissingArtifact,
    ProtocolFailureFirst,
    OriginalFailureStreamAfterEnd,
}

fn original_failure() -> TaskFailure {
    TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(NonZeroU16::new(u16::MAX).unwrap(), NonZeroU16::MIN),
        ),
        SafeDetail::new("original statistics Task failure").unwrap(),
        TaskFailurePhase::Execution,
    )
}

async fn run_case(case: Case) {
    let (description, plan) = internal_statistics_description();
    let (mut attempt, resources, activation) = watchdog(
        "original admission and Establish",
        OriginalAttempt::new_with_description(
            1,
            Arc::new(UnavailableFinalInfo),
            description.clone(),
            plan,
            true,
        ),
    )
    .await;
    // Capacity was configured on this original WorkloadControl, but Profile
    // fixtures still reserve no result position. Statistics borrows this exact
    // actor's original scope and acquires one real complete Internal window.
    assert_eq!(attempt.capacity.snapshot().held_positions, [0; 4]);
    let window = attempt
        .capacity
        .try_acquire(&attempt.scope, ResultWindowClass::Internal)
        .unwrap();
    let binding =
        QueryResultCapacityBinding::try_new(&attempt.scope, window.retain_alias()).unwrap();
    drop(window);
    let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let program = original_program();
    let (consumer, offers, mut output) = statistics_computation_consumer(
        &description,
        plan,
        &program,
        DecodedConsumerRuntime {
            computation: None,
            binding: binding.clone(),
            cpu: cpu_owner.runtime(),
            runtime,
            cancellation: QueryCancellationView::governed(
                attempt.scope.cancellation().unwrap(),
                None,
            ),
        },
    )
    .unwrap();
    let custody = output.cpu_custody();
    let mut producer = offers.reserve(attempt.root.query_execution_id()).unwrap();
    producer
        .stage(
            attempt.root.query_execution_id(),
            attempt.root,
            plan,
            resources,
        )
        .unwrap();
    attempt.bind_original_drive(&mut producer);
    let with_body = !matches!(case, Case::MissingArtifact);
    let segment = with_body.then(|| {
        let body = original_statistics_body();
        let backing = body.len() as u64;
        Arc::new(
            RetainedRootReply::try_new(
                RootReadResponse::Reply(RootResultReply {
                    root_task: attempt.root,
                    profile: RootProfileId::V1,
                    kind: RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
                    accepted_consumed: 0,
                    outcome: RootReadOutcome::Data(
                        RootResultData::try_new(
                            RootOutputKind::InternalFacts(
                                InternalResultDomain::StatisticsArtifactV1,
                            ),
                            NonZeroU64::MIN,
                            Bytes::from(body),
                            None,
                        )
                        .unwrap(),
                    ),
                }),
                binding.window_alias(),
                backing,
            )
            .unwrap(),
        )
    });
    let status = attempt
        .round
        .execution()
        .task(attempt.root.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let end = RootResultEnd {
        sequence: NonZeroU64::new(if with_body { 2 } else { 1 }).unwrap(),
        output_rows: u64::from(with_body),
    };
    let mut artifact_body = None;
    let mut native_failure = None;
    {
        let root = attempt.root;
        let logical = &attempt.logical;
        let consume = logical.run_registered_internal_consumer(Arc::clone(&consumer), activation);
        let deliver = async {
            if let Some(segment) = segment {
                logical
                    .deliver_original_internal_segment(ResultPacketSequence::new(0), segment, 1)
                    .await
                    .unwrap();
            }
            logical
                .deliver_original_internal_end(root, status, end)
                .await
        };
        tokio::pin!(consume, deliver);
        watchdog("End CPU receipt precedes upstream terminal facts", async {
        tokio::select! {
            biased;
            result = consume.as_mut() => panic!("statistics completed before its original nonterminal upstream: {result:?}"),
            result = deliver.as_mut() => result.unwrap(),
        }
    }).await;
        assert!(
            logical.original_internal_outcome().borrow().is_none(),
            "End is not the Ready proof"
        );
        std::future::poll_fn(|cx| {
            assert!(
                consume.as_mut().poll(cx).is_pending(),
                "must wait for the real upstream facts"
            );
            Poll::Ready(())
        })
        .await;

        attempt
            .pending_upstream
            .as_ref()
            .unwrap()
            .observe_original_consumed_end(&mut attempt.round, u64::from(with_body));
        let expected_failure = if matches!(
            case,
            Case::ProtocolFailureFirst | Case::OriginalFailureStreamAfterEnd
        ) {
            let selected = attempt
                .pending_upstream
                .as_ref()
                .unwrap()
                .publish_original_failure(&mut attempt.round, original_failure());
            let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
            assert!(logical.original_internal_outcome().borrow().is_none());
            if matches!(case, Case::OriginalFailureStreamAfterEnd) {
                // This is the actual production failure-freeze path after End;
                // consumer retirement must wake its failure stream with this input.
                watchdog(
                    "real pump failure retirement after End",
                    logical.freeze_original_internal_failure(QueryExecutionError::from_execution(
                        input.clone(),
                    )),
                )
                .await
                .unwrap();
            }
            Some(input)
        } else {
            None
        };
        if expected_failure.is_none() {
            let tail = attempt.pending_upstream.take().unwrap();
            attempt.release = tail.complete(&mut attempt.round, &attempt.acks);
        }
        let result = watchdog(
            "second CPU receipt or exact original Task failure",
            consume.as_mut(),
        )
        .await;

        match case {
            Case::Success => {
                result.unwrap();
                let token = logical
                    .original_internal_outcome()
                    .borrow()
                    .clone()
                    .unwrap()
                    .unwrap();
                let ready = output.take_ready(token).unwrap();
                assert_eq!(ready.ready, token);
                assert_eq!(ready.artifacts.len(), 1);
                assert_eq!(ready.artifacts[0].body().as_ref(), b"original artifact");
                assert!(Arc::ptr_eq(
                    ready.credentials.as_ref().unwrap(),
                    &attempt.native_credentials
                ));
                artifact_body = Some(ready.artifacts[0].body().clone());
                assert!(output.take_ready(token).is_err());
                drop(ready);
            }
            Case::MissingArtifact => {
                assert!(
                    result.is_err(),
                    "nonempty original program must fail its second membership check"
                );
                let outcome = logical.original_internal_outcome();
                assert!(
                    matches!(*outcome.borrow(), Some(Err(_))),
                    "missing artifact cannot publish Ready"
                );
            }
            Case::ProtocolFailureFirst | Case::OriginalFailureStreamAfterEnd => {
                let error = result.expect_err("the original Task failure vetoes readiness");
                assert_eq!(
                    error.execution_failure(),
                    expected_failure.as_ref(),
                    "never replace the original Task site/phase/fact with a generic proof or retirement error"
                );
                assert!(!matches!(
                    *logical.original_internal_outcome().borrow(),
                    Some(Ok(_))
                ));
                if matches!(case, Case::OriginalFailureStreamAfterEnd) {
                    assert_eq!(logical.actor_snapshot().await.unwrap().conclusion, None);
                    // The freeze retired this consumer, so its failure/exit
                    // cannot replace the original Native terminal decision.
                    native_failure = Some(error);
                } else {
                    assert_eq!(
                        logical.actor_snapshot().await.unwrap().conclusion,
                        Some(LogicalConclusion::Failed),
                    );
                }
            }
        }
    }
    if let Some(error) = native_failure {
        // Both consumer and delivery futures have relinquished their borrow.
        // Move the exact original permit into the same terminal actor command
        // as the production Native pump; the actor owns Abort issuance.
        assert_eq!(
            watchdog(
                "original Native failure chooses the NoRecovery conclusion",
                attempt.logical.fail_original_running_attempt(error.clone()),
            )
            .await
            .unwrap(),
            LogicalConclusion::Failed,
        );
        let outcome = attempt.logical.original_internal_outcome();
        assert_eq!(outcome.borrow().as_ref(), Some(&Err(error)));
    }
    if let Some(tail) = attempt.pending_upstream.take() {
        attempt.release = tail.complete(&mut attempt.round, &attempt.acks);
    }
    watchdog("stable CPU custody actual close", custody.join_closed())
        .await
        .unwrap();
    drop(custody);
    drop(output);
    drop(consumer);
    drop(binding);
    drop(producer);
    drop(offers);
    if artifact_body.is_some() {
        assert_eq!(
            attempt.capacity.snapshot().held_positions[2],
            1,
            "the separately cloned original artifact body retains the window"
        );
    }
    drop(artifact_body);
    assert_eq!(attempt.capacity.snapshot().held_positions, [0; 4]);
    watchdog("all original Task/Native/QA owners drain", attempt.finish()).await;
    watchdog(
        "actual finite CPU executor exits",
        cpu_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_end_precedes_all_success_then_second_receipt_reaches_real_ready() {
    run_case(Case::Success).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_nonempty_program_missing_artifact_refuses_second_receipt_and_ready() {
    run_case(Case::MissingArtifact).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_original_te_failure_before_qa_watch_preserves_exact_task_cause() {
    run_case(Case::ProtocolFailureFirst).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_original_failure_stream_wakes_after_end_without_waiting_for_ready() {
    run_case(Case::OriginalFailureStreamAfterEnd).await;
}
