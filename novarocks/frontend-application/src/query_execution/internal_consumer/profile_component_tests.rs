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

//! Real Profile consumers over the original handoff/TaskRound/QA fixture.

use super::*;
use crate::query_execution::internal_consumer::profile::{
    ReadyProfileComputation, profile_computation_consumer,
};
use crate::task_execution::profile_diagnostics::{
    ProfileDiagnosticClosure, ProfileDiagnosticMissing, ProfileTaskDiagnostic,
};
use novarocks_query_application::api::{InternalComputationConsumer, QueryExecutionError};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Profile component watchdog: {stage}"))
}

#[derive(Default)]
struct FinalInfoProbe {
    entered: AtomicBool,
    completed: AtomicBool,
    exited: AtomicBool,
}

struct FinalInfoExit(Arc<FinalInfoProbe>);
impl Drop for FinalInfoExit {
    fn drop(&mut self) {
        self.0.exited.store(true, Ordering::SeqCst);
    }
}

struct HeldOriginalFinalInfo {
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    probe: Arc<FinalInfoProbe>,
}
impl TaskResultTransport for HeldOriginalFinalInfo {
    fn final_task_info(
        &self,
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        let gate = self.gate.lock().unwrap().take().expect("one original read");
        let probe = Arc::clone(&self.probe);
        Box::pin(async move {
            let _exit = FinalInfoExit(Arc::clone(&probe));
            probe.entered.store(true, Ordering::SeqCst);
            gate.await.expect("release the original final-info read");
            probe.completed.store(true, Ordering::SeqCst);
            Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone))
        })
    }

    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("Profile consumer must not fetch dynamic filters")
    }
}

/// The biased actor watch is observed before polling a now-ready consumer.
/// This deliberately exercises take_ready while source.accept's caller has
/// not resumed to publish any local completion marker.
async fn run_fixed_consumer_to_real_cut(
    attempt: &OriginalAttempt,
    consumer: Arc<dyn InternalComputationConsumer>,
    activation: InternalConsumerActivation,
    output: &mut crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    prior_cut: Option<ComputationReadyToken>,
) -> ReadyProfileComputation {
    let status = attempt
        .round
        .execution()
        .task(attempt.root.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let mut outcome = attempt.logical.original_internal_outcome();
    let consume = attempt
        .logical
        .run_registered_internal_consumer(consumer, activation);
    let deliver = attempt.logical.deliver_original_internal_end(
        attempt.root,
        status,
        RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 0,
        },
    );
    tokio::pin!(consume, deliver);
    let mut consumed: Option<Result<(), QueryExecutionError>> = None;
    let mut delivered = false;
    let token = watchdog("original actor accepts fixed Profile proof", async {
        loop {
            if let Some(selected) = outcome.borrow().clone() {
                break selected.expect("the fixed Profile computation succeeds");
            }
            tokio::select! {
                biased;
                changed = outcome.changed() => changed.expect("original actor outcome remains live"),
                result = consume.as_mut(), if consumed.is_none() => {
                    consumed = Some(result);
                }
                result = deliver.as_mut(), if !delivered => {
                    result.expect("the original End is actually consumed");
                    delivered = true;
                }
            }
        }
    })
    .await;
    assert!(
        consumed.is_none(),
        "the actor cut must be receivable before the consumer resumes after source.accept"
    );
    if let Some(prior_cut) = prior_cut {
        assert!(output.take_ready(prior_cut).is_err());
    }
    let received = output
        .take_ready(token)
        .expect("staged custody and the actual actor cut transfer atomically");
    assert!(output.take_ready(token).is_err(), "one transfer only");
    if consumed.is_none() {
        watchdog("original consumer actual future exit", consume.as_mut())
            .await
            .unwrap();
    }
    if !delivered {
        watchdog("original End delivery receipt", deliver.as_mut())
            .await
            .unwrap();
    }
    received
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_profile_reaches_real_ready_while_original_final_info_is_held() {
    let probe = Arc::new(FinalInfoProbe::default());
    let (release, gate) = tokio::sync::oneshot::channel();
    let transport: Arc<dyn TaskResultTransport> = Arc::new(HeldOriginalFinalInfo {
        gate: Mutex::new(Some(gate)),
        probe: Arc::clone(&probe),
    });
    let (mut attempt, resources, activation) = watchdog(
        "build original actor and settle original Establish",
        OriginalAttempt::new_with_transport(1, transport),
    )
    .await;
    let (description, plan) = internal_count_description();
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
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
    let expected = attempt
        .round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    // Start the actual original diagnostic request before proposing Ready.
    for _ in 0..=expected.len() {
        std::future::poll_fn(|cx| {
            attempt.diagnostics.poll_observe(&attempt.round, cx);
            Poll::Ready(())
        })
        .await;
    }
    assert!(probe.entered.load(Ordering::SeqCst));
    assert!(!probe.completed.load(Ordering::SeqCst));
    assert!(!probe.exited.load(Ordering::SeqCst));
    let mut received =
        run_fixed_consumer_to_real_cut(&attempt, consumer, activation, &mut output, None).await;
    let token = received.ready_token();
    assert_eq!(received.output_rows(), 0);
    assert_eq!(received.expected_tasks(), expected.as_slice());
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &attempt.native_credentials
    ));
    assert!(received.current_diagnostics().unwrap().is_none());
    assert!(!probe.completed.load(Ordering::SeqCst));
    assert!(!probe.exited.load(Ordering::SeqCst));
    assert!(matches!(
        attempt.logical.actor_snapshot().await.unwrap().conclusion,
        Some(novarocks_query_application::coordination::LogicalConclusion::ComputationReady)
    ));
    attempt
        .diagnostics
        .begin_profile_observation(token)
        .unwrap();
    attempt.clock.advance(Duration::from_secs(1));
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_profile_observation(
            &attempt.round,
            attempt.clock.now(),
            false,
            cx,
        );
        Poll::Ready(())
    })
    .await;
    let original = received.current_diagnostics().unwrap().unwrap();
    assert_eq!(original.ready, token);
    assert_eq!(
        original.closure,
        ProfileDiagnosticClosure::ObservationDeadline
    );
    assert_eq!(original.tasks.len(), expected.len());
    assert!(original.tasks.iter().any(|(_, fact)| matches!(
        fact,
        ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
    )));
    let observed = watchdog(
        "read the original immutable diagnostic snapshot",
        received.wait_diagnostics(),
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&original, &observed));
    assert!(!attempt.diagnostics.is_closed());
    assert!(!probe.exited.load(Ordering::SeqCst));
    release.send(()).unwrap();
    watchdog("original held diagnostic RPC actually exits", async {
        while !attempt.diagnostics.is_closed() {
            std::future::poll_fn(|cx| {
                attempt.diagnostics.poll_profile_observation(
                    &attempt.round,
                    attempt.clock.now(),
                    false,
                    cx,
                );
                Poll::Ready(())
            })
            .await;
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(probe.completed.load(Ordering::SeqCst));
    assert!(probe.exited.load(Ordering::SeqCst));
    producer.retire_unclaimed();
    drop((producer, received, original, observed, output));
    watchdog("original TaskRound and actor drain", attempt.finish()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_profile_deadline_freezes_pending_before_the_original_rpc_returns_on_that_poll() {
    let probe = Arc::new(FinalInfoProbe::default());
    let (release, gate) = tokio::sync::oneshot::channel();
    let transport: Arc<dyn TaskResultTransport> = Arc::new(HeldOriginalFinalInfo {
        gate: Mutex::new(Some(gate)),
        probe: Arc::clone(&probe),
    });
    let (mut attempt, resources, activation) = watchdog(
        "build original actor for the deadline boundary",
        OriginalAttempt::new_with_transport(1, transport),
    )
    .await;
    let (description, plan) = internal_count_description();
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
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
    let pending_task = attempt.round.execution().accepted_terminal_tasks()[0];
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_observe(&attempt.round, cx);
        Poll::Ready(())
    })
    .await;
    let initially_pending = probe.entered.load(Ordering::SeqCst)
        && !probe.completed.load(Ordering::SeqCst)
        && !probe.exited.load(Ordering::SeqCst)
        && !attempt.diagnostics.is_closed();
    let received =
        run_fixed_consumer_to_real_cut(&attempt, consumer, activation, &mut output, None).await;
    let token = received.ready_token();
    attempt
        .diagnostics
        .begin_profile_observation(token)
        .unwrap();
    // Sending only readies this original F; no other task polls it. Its first
    // return is deliberately the same poll that reaches the observation cut.
    let released = release.send(()).is_ok();
    let not_polled_after_release =
        !probe.completed.load(Ordering::SeqCst) && !probe.exited.load(Ordering::SeqCst);
    let remaining = token
        .diagnostic_deadline()
        .saturating_duration_since(attempt.clock.now());
    attempt.clock.advance(remaining);
    let at_exact_deadline = attempt.clock.now() == token.diagnostic_deadline();
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_profile_observation(
            &attempt.round,
            attempt.clock.now(),
            false,
            cx,
        );
        Poll::Ready(())
    })
    .await;
    let original = received.current_diagnostics().unwrap();
    let original_read_exited = probe.completed.load(Ordering::SeqCst)
        && probe.exited.load(Ordering::SeqCst)
        && attempt.diagnostics.is_closed();
    let cutoff_closed = attempt.diagnostics.final_info_closed();
    let final_deadline = attempt.diagnostics.observation_deadline();
    // A second actual poll cannot replace the same immutable cut with the
    // original transport's now-received Unavailable(Gone) answer either.
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_profile_observation(
            &attempt.round,
            attempt.clock.now(),
            false,
            cx,
        );
        Poll::Ready(())
    })
    .await;
    let late = received.current_diagnostics().unwrap();
    producer.retire_unclaimed();
    drop((producer, received, output));
    watchdog(
        "boundary original TaskRound and actor drain",
        attempt.finish(),
    )
    .await;

    assert!(initially_pending);
    assert!(released);
    assert!(not_polled_after_release);
    assert!(at_exact_deadline);
    assert!(original_read_exited);
    assert!(cutoff_closed);
    assert_eq!(final_deadline, None);
    let original = original.expect("the exact Ready deadline freezes the original snapshot");
    let late = late.expect("the original immutable cut remains visible");
    assert!(Arc::ptr_eq(&original, &late));
    assert_eq!(original.ready, token);
    assert_eq!(
        original.closure,
        ProfileDiagnosticClosure::ObservationDeadline
    );
    assert!(original.tasks.iter().any(|(task, fact)| {
        *task == pending_task
            && matches!(
                fact,
                ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
            )
    }));
    assert!(!original.tasks.iter().any(|(task, fact)| {
        *task == pending_task
            && matches!(
                fact,
                ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::Unavailable(
                    OperationOutcome::Gone
                ))
            )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_foreign_old_claim_and_cut_cannot_consume_new_profile_custody() {
    // These are independent actual actors, as in the original handoff tests.
    // This protects capability/cut isolation, not the QA recovery algorithm.
    let (mut prior, prior_resources, prior_activation) =
        watchdog("prior actor original Establish", OriginalAttempt::new(1)).await;
    drop(prior_resources);
    prior
        .round
        .bind_protocol_drive(&prior.logical.native_attempt_drive())
        .unwrap();
    let prior_cut = watchdog(
        "prior original actor cut",
        prior.accept_original_end(prior_activation),
    )
    .await;
    let (stale, stale_resources, stale_activation) =
        watchdog("stale actor original Establish", OriginalAttempt::new(1)).await;
    drop(stale_resources);
    let (mut next, resources, activation) =
        watchdog("next actor original Establish", OriginalAttempt::new(2)).await;
    let (description, plan) = internal_count_description();
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
    let mut producer = offers.reserve(next.root.query_execution_id()).unwrap();
    producer
        .stage(next.root.query_execution_id(), next.root, plan, resources)
        .unwrap();
    next.bind_original_drive(&mut producer);
    let rejected = watchdog(
        "stale actual source cannot claim the next offer",
        stale
            .logical
            .run_registered_internal_consumer(Arc::clone(&consumer), stale_activation),
    )
    .await;
    assert!(rejected.is_err());
    // The failed foreign claim must not poison the receiving holder's first
    // actor fence. A real next source still reaches its actual cut.
    let received =
        run_fixed_consumer_to_real_cut(&next, consumer, activation, &mut output, Some(prior_cut))
            .await;
    assert_eq!(received.ready_token().root(), next.root);
    assert!(
        received
            .expected_tasks()
            .iter()
            .all(|task| task.query_execution_id() == next.root.query_execution_id())
    );
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &next.native_credentials
    ));
    producer.retire_unclaimed();
    drop((producer, received, output));
    watchdog("prior actual actor drain", prior.finish()).await;
    watchdog("stale actual actor drain", stale.finish()).await;
    watchdog("next actual actor drain", next.finish()).await;
}
