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

// Included as a child of actor::tests to share the existing governed fixtures.
use super::*;
use crate::api::{
    DecodedInternalEndReceipt, InternalEndDelivery, InternalResultDelivery, PlanSeal,
};

fn internal_config(
    execution: QueryExecutionId,
    port: Arc<dyn ReplacementQualificationEffectPort>,
) -> LogicalExecutionActorConfig {
    // This component fixture starts after description validation. It preserves
    // the existing row carrier used by internal profiling consumption.
    let mut config = recovery_config(execution, port, 2);
    config.output_mode = LogicalOutputMode::InternalResultStream;
    config.internal_plan = Some(PlanSeal::Version(
        novarocks_physical_plan::PlanVersionId::try_new([97; 16]).unwrap(),
    ));
    config.result_schema = None;
    config
}

struct InternalFixture {
    owner: LogicalExecutionActorOwner,
    running: Arc<RunningAttemptPermit>,
    observer: RootResultObserver,
    activation: InternalConsumerActivation,
    _output: LogicalExecutionOutputTransfer,
}

impl InternalFixture {
    async fn new(tag: i64) -> Self {
        Self::with_clock(tag, Arc::new(ProcessMonotonicClock::new())).await
    }

    async fn with_clock(tag: i64, clock: Arc<dyn MonotonicClock>) -> Self {
        Self::with_clock_and_qualification(
            tag,
            clock,
            Arc::new(DelayedQualificationPort::default()),
        )
        .await
    }

    async fn with_clock_and_qualification(
        tag: i64,
        clock: Arc<dyn MonotonicClock>,
        qualification: Arc<DelayedQualificationPort>,
    ) -> Self {
        let first = execution(tag);
        let config = internal_config(first, qualification).with_clock(clock);
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
        let root = root_task(first, 1);
        let activation = running.bind_internal_consumer(root).await.unwrap();
        let observer = running.bind_root_result(root).await.unwrap();
        Self {
            owner,
            running,
            observer,
            activation,
            _output: output,
        }
    }

    async fn end(
        &mut self,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> (
        InternalEndDelivery,
        JoinHandle<Result<(), LogicalExecutionActorError>>,
    ) {
        let root = self.observer.root;
        self.observer
            .observe_local_root_end(root, end)
            .await
            .unwrap();
        let running = Arc::clone(&self.running);
        let delivery = tokio::spawn(async move { running.deliver_internal_end(root, end).await });
        let InternalResultDelivery::End(end) =
            self.activation.stream.next().await.unwrap().unwrap()
        else {
            panic!("the actor must deliver its original internal End");
        };
        (end, delivery)
    }

    async fn decoded_end(&mut self) -> DecodedInternalEndReceipt {
        let (end, delivery) = self.end(result_end(0)).await;
        let receipt = end.complete().await.unwrap();
        delivery.await.unwrap().unwrap();
        receipt
    }

    async fn finished(&self) {
        self.observer
            .observe_status(root_status(self.observer.root, 1, TaskState::Finished))
            .await
            .unwrap();
    }
}

async fn poll_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    assert!(matches!(
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await,
        Poll::Pending
    ));
}

#[tokio::test]
async fn internal_end_failure_preserves_domain_error_and_never_emits_ready() {
    let mut fixture = InternalFixture::new(12_001).await;
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    let error = actual_consumer_body_failure();
    end.fail(error.clone());
    assert!(delivery.await.unwrap().is_err());
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
    assert!(
        !fixture
            .owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .output_visible
    );
}

#[tokio::test]
async fn internal_end_decodes_while_running_and_waits_for_root_before_ready() {
    let mut fixture = InternalFixture::new(12_002).await;
    let decoded = fixture.decoded_end().await;
    let snapshot = fixture.owner.actor().snapshot().await.unwrap();
    assert!(matches!(snapshot.phase, ExecutionPhase::Running { .. }));
    assert_eq!(snapshot.conclusion, None);
    assert!(!snapshot.output_visible);
    let InternalConsumerActivation {
        source,
        ready,
        stream: _stream,
    } = fixture.activation;
    let mut acceptance = Box::pin(source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    fixture.owner.actor().snapshot().await.unwrap();
    assert!(ready.borrow().is_none());
    fixture
        .observer
        .observe_status(root_status(fixture.observer.root, 1, TaskState::Finished))
        .await
        .unwrap();
    let token = acceptance.await.unwrap();
    assert_eq!(token.root(), fixture.observer.root);
    assert_eq!(token.end(), result_end(0));
    assert!(token.diagnostic_deadline() > token.cut());
    assert!(token.convergence_deadline() > token.cut());
    assert_eq!(ready.borrow().as_ref().unwrap().as_ref().unwrap(), &token);
    assert!(
        fixture
            .owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .output_visible
    );
}

#[tokio::test]
async fn cancellation_before_internal_ready_refuses_the_proof() {
    let mut fixture = InternalFixture::new(12_003).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Cancelled).await;
    assert!(
        !fixture
            .owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .output_visible
    );
    assert!(fixture.activation.source.accept(decoded).await.is_err());
    assert!(fixture.activation.ready.borrow().as_ref().unwrap().is_err());
}

#[tokio::test]
async fn internal_ready_before_cancellation_remains_the_first_conclusion() {
    let mut fixture = InternalFixture::new(12_004).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let token = fixture.activation.source.accept(decoded).await.unwrap();
    let snapshot =
        wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::ComputationReady).await;
    assert!(snapshot.output_visible);
    let mut closed = fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*closed.borrow() {
            closed.changed().await.unwrap();
        }
    })
    .await
    .expect("late shutdown must close the settled actor ingress");
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap(),
        &token
    );
}

#[tokio::test]
async fn dropped_source_while_proof_is_pending_fails_closed() {
    let mut fixture = InternalFixture::new(12_005).await;
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    fixture.owner.actor().snapshot().await.unwrap();
    assert!(fixture.activation.ready.borrow().is_none());
    drop(acceptance);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert!(fixture.activation.ready.borrow().as_ref().unwrap().is_err());
}

#[tokio::test]
async fn internal_end_requires_the_bound_root_and_original_end_facts() {
    let mut fixture = InternalFixture::new(12_006).await;
    let root = fixture.observer.root;
    fixture
        .observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .running
            .deliver_internal_end(root_task(root.query_execution_id(), 2), result_end(0))
            .await
            .unwrap_err(),
        LogicalExecutionActorError::StaleAuthority
    );
    let mut wrong_rows = result_end(0);
    wrong_rows.output_rows = 1;
    assert_eq!(
        fixture
            .running
            .deliver_internal_end(root, wrong_rows)
            .await
            .unwrap_err(),
        LogicalExecutionActorError::ResultProtocolViolation
    );
    assert_eq!(
        fixture
            .running
            .deliver_internal_end(root, result_end(1))
            .await
            .unwrap_err(),
        LogicalExecutionActorError::ResultProtocolViolation
    );
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );
    // Rejected proposals do not replace the original facts or poison the source.
    let running = Arc::clone(&fixture.running);
    let delivery =
        tokio::spawn(async move { running.deliver_internal_end(root, result_end(0)).await });
    let InternalResultDelivery::End(end) = fixture.activation.stream.next().await.unwrap().unwrap()
    else {
        panic!("the original End must still be accepted");
    };
    let decoded = end.complete().await.unwrap();
    delivery.await.unwrap().unwrap();
    fixture.finished().await;
    fixture.activation.source.accept(decoded).await.unwrap();
}

// The clock hook injects an already-authorized ingress event at the top of the
// actor turn immediately following proof receipt. It adds no production hook.
struct CutIngressClock {
    next: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl fmt::Debug for CutIngressClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CutIngressClock").finish_non_exhaustive()
    }
}
impl CutIngressClock {
    fn new() -> Self {
        Self {
            next: Mutex::new(None),
        }
    }
    fn arm(&self, hook: impl FnOnce() + Send + 'static) {
        assert!(self.next.lock().unwrap().replace(Box::new(hook)).is_none());
    }
}
impl MonotonicClock for CutIngressClock {
    fn now(&self) -> MonotonicInstant {
        if let Some(hook) = self.next.lock().unwrap().take() {
            hook();
        }
        MonotonicInstant::from_origin(Duration::from_nanos(1))
    }
}

#[tokio::test]
async fn registry_close_in_ready_turn_is_reduced_before_the_cut() {
    let clock = Arc::new(CutIngressClock::new());
    let mut fixture = InternalFixture::with_clock(12_007, clock.clone()).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    let close = fixture.owner.actor().registry_close_requested.clone();
    clock.arm(move || {
        close.send_replace(Some(RegistryClose::Shutdown));
    });
    assert!(acceptance.await.is_err());
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .kind(),
        QueryExecutionErrorKind::Cancelled
    );
}

#[tokio::test]
async fn queued_root_failure_in_ready_turn_prevents_the_cut() {
    let clock = Arc::new(CutIngressClock::new());
    let mut fixture = InternalFixture::with_clock(12_008, clock.clone()).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    let terminal = fixture.observer.terminal_mailbox.clone();
    let root = fixture.observer.root;
    let activation = fixture.observer.activation;
    let (reply, response) = oneshot::channel();
    clock.arm(move || {
        terminal.try_send(RootTerminalObservation {
            activation,
            root,
            // The reserved ingress refines the root seal with the exact
            // attempt's authoritative failure before readiness is accepted.
            status: root_status(root, 1, TaskState::Finished),
            terminal_failure: RootTerminalFailure::Authoritative(actual_consumer_body_failure()),
            reply,
        }).unwrap();
    });
    assert!(response.await.unwrap().is_err());
    fixture.owner.actor().snapshot().await.unwrap();
    assert!(fixture.activation.ready.borrow().is_none());
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );
    // The exact attempt owner still chooses failure/replacement after the fact.
    drop(acceptance);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
}

async fn replace_internal_attempt(
    owner: &LogicalExecutionActorOwner,
    running: RunningAttemptPermit,
    port: &DelayedQualificationPort,
) -> RunningAttemptPermit {
    let successor = replacement(running.identity().execution(), 2);
    let qualification = owner
        .actor()
        .begin_replacement(
            running,
            AttemptFailureClass::RecoverableInfrastructure,
            successor,
            Vec::new(),
        )
        .await
        .unwrap();
    let mut activation = Box::pin(owner.actor().activate_replacement(qualification));
    poll_pending(activation.as_mut()).await;
    qualify(port.submissions.lock().unwrap().pop().unwrap());
    owner
        .actor()
        .activate(activation.await.unwrap().ready())
        .await
        .unwrap()
}

#[tokio::test]
async fn old_source_and_retained_input_cannot_mutate_or_release_the_successor() {
    let first = execution(12_009);
    let port = Arc::new(DelayedQualificationPort::default());
    let (owner, initial, _output) =
        spawn_logical_execution_actor(&Handle::current(), internal_config(first, port.clone()))
            .unwrap()
            .into_parts();
    let outcome = owner.actor().internal_outcome().unwrap();
    let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
    let (control, window_owner, capacity, segment) = result_segment(first, 1);
    let root = segment.root_task();
    let mut old = running.bind_internal_consumer(root).await.unwrap();
    let old_observer = running.bind_root_result(root).await.unwrap();
    let sender = Arc::clone(&running);
    let delivery = tokio::spawn(async move {
        sender
            .deliver_root_segment(
                ResultPacketSequence::new(0),
                segment,
                Some((result_profile(), ClientRowStreamCursor::default())),
                2,
                crate::api::RootRelayResidentWindow::default(),
            )
            .await
    });
    let InternalResultDelivery::Segment(segment) = old.stream.next().await.unwrap().unwrap() else {
        panic!("the original segment must reach its consumer");
    };
    let (input, consumption) = segment.into_parts();
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    // Abandon the pump waiter, not the consumer's retained original input.
    delivery.abort();
    assert!(delivery.await.unwrap_err().is_cancelled());
    let running = Arc::try_unwrap(running).expect("the pump waiter relinquished its permit");
    let successor = replace_internal_attempt(&owner, running, &port).await;
    drop(old_observer);
    let next_root = root_task(successor.identity().execution(), 1);
    let mut next = successor.bind_internal_consumer(next_root).await.unwrap();
    let next_observer = successor.bind_root_result(next_root).await.unwrap();
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    assert!(old.ready.has_changed().is_err());
    assert!(old.ready.borrow().is_none());
    assert!(outcome.borrow().is_none());
    assert!(
        !outcome.has_changed().unwrap(),
        "retirement leaves the execution observer alive and undecided"
    );
    assert!(
        old.source
            .fail(actual_consumer_body_failure())
            .await
            .is_err()
    );
    assert_eq!(owner.actor().snapshot().await.unwrap().conclusion, None);
    // Completing a retired receipt must not advance the successor's watermark.
    consumption.complete();
    assert_eq!(
        owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .accepted_result_packets,
        0
    );
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    drop(input);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    next_observer
        .observe_local_root_end(next_root, result_end(0))
        .await
        .unwrap();
    let sender = Arc::new(successor);
    let end_sender = Arc::clone(&sender);
    let end_task = tokio::spawn(async move {
        end_sender
            .deliver_internal_end(next_root, result_end(0))
            .await
    });
    let InternalResultDelivery::End(end) = next.stream.next().await.unwrap().unwrap() else {
        panic!("the successor must own a distinct End");
    };
    let decoded = end.complete().await.unwrap();
    end_task.await.unwrap().unwrap();
    next_observer
        .observe_status(root_status(next_root, 1, TaskState::Finished))
        .await
        .unwrap();
    let token = next.source.accept(decoded).await.unwrap();
    wait_for_conclusion(owner.actor(), LogicalConclusion::ComputationReady).await;
    assert_eq!(outcome.borrow().as_ref().unwrap().as_ref().unwrap(), &token);
    window_owner.complete();
    drop(control);
}

#[test]
fn internal_profile_data_and_end_remain_replaceable_until_ready() {
    let first = execution(12_010);
    let mut state = LogicalExecutionState::new(
        first,
        RecoveryMode::RestartAttemptBeforeVisibility,
        ExecutionEffect::None,
        LogicalOutputMode::InternalResultStream,
        2,
    )
    .unwrap();
    let capability = state.attempt_capability(first).unwrap();
    state.mark_running(&capability).unwrap();
    let data = state
        .begin_result_delivery(&capability, 0, ResultPacket::Data(()))
        .unwrap();
    assert!(!state.output_visible());
    state.complete_result_delivery(data).unwrap();
    let end = state.begin_internal_end(&capability, 1).unwrap();
    state.complete_result_delivery(end).unwrap();
    assert!(!state.output_visible());
    assert!(matches!(state.phase(), ExecutionPhase::Running { .. }));
    state
        .begin_replacement(
            &capability,
            replacement(first, 2),
            AttemptFailureClass::RecoverableInfrastructure,
            false,
        )
        .unwrap();

    let mut accepted = LogicalExecutionState::new(
        first,
        RecoveryMode::RestartAttemptBeforeVisibility,
        ExecutionEffect::None,
        LogicalOutputMode::InternalResultStream,
        2,
    )
    .unwrap();
    let capability = accepted.attempt_capability(first).unwrap();
    accepted.mark_running(&capability).unwrap();
    let end = accepted.begin_internal_end(&capability, 0).unwrap();
    accepted.complete_result_delivery(end).unwrap();
    accepted.accept_computation_ready(&capability).unwrap();
    assert!(accepted.output_visible());
    assert_eq!(
        accepted
            .begin_replacement(
                &capability,
                replacement(first, 2),
                AttemptFailureClass::RecoverableInfrastructure,
                false
            )
            .unwrap_err(),
        ActorStateError::AlreadyConcluded
    );
}

#[tokio::test]
async fn supervised_source_drop_retains_the_actual_consumer_exit_error() {
    let fixture = InternalFixture::new(12_010).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let error = result_protocol_failure("the internal domain rejected its actual decoded result");
    drop(fixture.activation.source);
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None,
        "the reserved actual-exit owner still holds its verdict"
    );
    exit.complete(Err(error.clone()));
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
    assert_eq!(
        fixture
            .owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
}

#[tokio::test]
async fn pending_proof_source_loss_waits_for_its_reserved_typed_exit() {
    let mut fixture = InternalFixture::new(12_011).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    fixture.owner.actor().snapshot().await.unwrap();
    drop(acceptance);
    fixture
        .observer
        .observe_status(root_status(fixture.observer.root, 1, TaskState::Finished))
        .await
        .unwrap();
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );
    let error = result_protocol_failure("the abandoned proof retained its actual consumer failure");
    exit.complete(Err(error.clone()));
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
}

#[tokio::test]
async fn consumer_exit_after_ready_cannot_revoke_the_accepted_cut() {
    let mut fixture = InternalFixture::new(12_012).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let accepted = fixture.activation.source.accept(decoded).await.unwrap();
    exit.complete(Err(result_protocol_failure(
        "a late consumer failure is only diagnostic",
    )));
    let snapshot = fixture.owner.actor().snapshot().await.unwrap();
    assert_eq!(
        snapshot.conclusion,
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap(),
        &accepted
    );
    assert_eq!(
        fixture
            .owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap(),
        &accepted,
    );
}

async fn assert_internal_exit_is_pending(
    actor: &LogicalExecutionActor,
    ready: &watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
) {
    assert_eq!(
        actor.snapshot().await.unwrap().conclusion,
        None,
        "a delivery destructor cannot preempt the reserved actual-exit verdict"
    );
    assert!(
        ready.borrow().is_none(),
        "no generic failure or Ready may escape"
    );
    assert!(actor.internal_outcome().unwrap().borrow().is_none());
}

async fn assert_internal_exit_preserves_error(
    actor: &LogicalExecutionActor,
    ready: &watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    error: QueryExecutionError,
) {
    wait_for_conclusion(actor, LogicalConclusion::Failed).await;
    assert_eq!(
        actor
            .internal_outcome()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
    assert_eq!(
        ready.borrow().as_ref().unwrap().as_ref().unwrap_err(),
        &error
    );
}

#[tokio::test]
async fn supervised_segment_drop_waits_for_its_original_exit_error() {
    let mut fixture = InternalFixture::new(12_013).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let root = fixture.observer.root;
    let (control, window_owner, capacity, original) = result_segment(root.query_execution_id(), 1);
    // The shared result fixture mints a fresh backend process identity. Bind
    // this fixture reply to the exact root already registered by InternalFixture.
    let mut response = original.response().clone();
    let novarocks_execution_contract::root_result::RootReadResponse::Reply(reply) = &mut response
    else {
        panic!("the shared result fixture must produce one root reply");
    };
    reply.root_task = root;
    let segment = Arc::new(
        RetainedRootReply::try_new(response, original.retain_physical_guard(), 4096).unwrap(),
    );
    drop(original);
    let running = Arc::clone(&fixture.running);
    let delivery = tokio::spawn(async move {
        running
            .deliver_root_segment(
                ResultPacketSequence::new(0),
                segment,
                Some((result_profile(), ClientRowStreamCursor::default())),
                2,
                crate::api::RootRelayResidentWindow::default(),
            )
            .await
    });
    let InternalResultDelivery::Segment(segment) =
        fixture.activation.stream.next().await.unwrap().unwrap()
    else {
        panic!("the supervised consumer must receive the original segment");
    };
    let (input, consumption) = segment.into_parts();
    drop(consumption);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.unwrap().is_err());
    drop(input);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    window_owner.complete();
    drop(control);
}

#[tokio::test]
async fn supervised_end_drop_waits_for_its_original_exit_error() {
    let mut fixture = InternalFixture::new(12_014).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    drop(end);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.unwrap().is_err());
}

#[tokio::test]
async fn supervised_end_acknowledgement_loss_waits_for_its_original_exit_error() {
    let mut fixture = InternalFixture::new(12_015).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    let mut completion = Box::pin(end.complete());
    // Poll once without yielding to the actor: Completed has been sent, while
    // the actor-issued decoded receipt is still waiting for its receiver.
    poll_pending(completion.as_mut()).await;
    drop(completion);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.unwrap().is_err());
}

#[tokio::test]
async fn supervised_end_receiver_closed_before_first_item_preserves_exit_error() {
    let fixture = InternalFixture::new(12_016).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let root = fixture.observer.root;
    fixture
        .observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    let mut delivery = Box::pin(fixture.running.deliver_internal_end(root, result_end(0)));
    // Queue the pump command, then let the consumer exit before its first next().
    // The actor must reserve against the now-closed registered internal channel.
    poll_pending(delivery.as_mut()).await;
    drop(fixture.activation.stream);
    drop(fixture.activation.source);
    tokio::time::timeout(
        Duration::from_secs(1),
        assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready),
    )
    .await
    .expect("a closed reservation must not busy-loop or preempt actual exit");
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.is_err());
}

#[tokio::test]
async fn supervised_segment_receiver_closed_before_first_item_preserves_exit_error() {
    let fixture = InternalFixture::new(12_017).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let root = fixture.observer.root;
    let (control, window_owner, capacity, original) = result_segment(root.query_execution_id(), 1);
    let mut response = original.response().clone();
    let novarocks_execution_contract::root_result::RootReadResponse::Reply(reply) = &mut response
    else {
        panic!("the shared result fixture must produce one root reply");
    };
    reply.root_task = root;
    let segment = Arc::new(
        RetainedRootReply::try_new(response, original.retain_physical_guard(), 4096).unwrap(),
    );
    drop(original);
    let mut delivery = Box::pin(fixture.running.deliver_root_segment(
        ResultPacketSequence::new(0),
        segment,
        Some((result_profile(), ClientRowStreamCursor::default())),
        2,
        crate::api::RootRelayResidentWindow::default(),
    ));
    poll_pending(delivery.as_mut()).await;
    drop(fixture.activation.stream);
    drop(fixture.activation.source);
    tokio::time::timeout(
        Duration::from_secs(1),
        assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready),
    )
    .await
    .expect("a closed reservation must not busy-loop or preempt actual exit");
    assert_eq!(
        capacity.snapshot().held_positions,
        [1, 0, 0, 0],
        "the deferred pending delivery still owns its original payload"
    );
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.is_err());
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    window_owner.complete();
    drop(control);
}

#[tokio::test]
async fn deferred_end_cancellation_settles_delivery_before_actual_exit() {
    let mut fixture = InternalFixture::new(12_018).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let outcome = fixture.owner.actor().internal_outcome().unwrap();
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    drop(end);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    let mut closed = fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Cancelled).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), delivery)
            .await
            .expect(
                "accepted cancellation settles deferred delivery without waiting for actual exit"
            )
            .unwrap()
            .unwrap_err(),
        LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Cancelled),
    );
    let cancellation = outcome
        .borrow()
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap_err()
        .clone();
    assert_eq!(cancellation.kind(), QueryExecutionErrorKind::Cancelled);
    assert!(
        !fixture
            .owner
            .actor()
            .registry_join_readiness(false)
            .await
            .unwrap(),
        "actual consumer exit remains independently outstanding after logical cancellation"
    );
    exit.complete(Err(actual_consumer_body_failure()));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*closed.borrow() {
            closed.changed().await.unwrap();
        }
    })
    .await
    .expect("actual exit allows the cancelled actor to close ingress");
    assert_eq!(
        outcome.borrow().as_ref().unwrap().as_ref().unwrap_err(),
        &cancellation
    );
}

#[tokio::test]
async fn deferred_end_retirement_keeps_execution_outcome_open_until_successor_failure() {
    let first = execution(12_019);
    let port = Arc::new(DelayedQualificationPort::default());
    let (owner, initial, _output) =
        spawn_logical_execution_actor(&Handle::current(), internal_config(first, port.clone()))
            .unwrap()
            .into_parts();
    let outcome = owner.actor().internal_outcome().unwrap();
    let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
    let root = root_task(first, 1);
    let mut old = running.bind_internal_consumer(root).await.unwrap();
    let exit = old.supervise_exit().await.unwrap();
    let observer = running.bind_root_result(root).await.unwrap();
    observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    let sender = Arc::clone(&running);
    let delivery =
        tokio::spawn(async move { sender.deliver_internal_end(root, result_end(0)).await });
    let InternalResultDelivery::End(end) = old.stream.next().await.unwrap().unwrap() else {
        panic!("the old consumer must receive its original End");
    };
    drop(end);
    assert_internal_exit_is_pending(owner.actor(), &old.ready).await;
    // Relinquish only the waiting API future so the attempt permit can move
    // into replacement. The actor still owns the exact deferred End metadata.
    delivery.abort();
    assert!(delivery.await.unwrap_err().is_cancelled());
    let running = Arc::try_unwrap(running).expect("the old pump released its handle");
    let successor = replace_internal_attempt(&owner, running, &port).await;
    assert!(old.ready.has_changed().is_err());
    assert!(outcome.borrow().is_none());
    assert!(!outcome.has_changed().unwrap());
    exit.complete(Err(actual_consumer_body_failure()));
    let next_root = root_task(successor.identity().execution(), 1);
    let next = successor.bind_internal_consumer(next_root).await.unwrap();
    let _next_observer = successor.bind_root_result(next_root).await.unwrap();
    let error = result_protocol_failure("the replacement consumer rejected its original result");
    next.source.fail(error.clone()).await.unwrap();
    wait_for_conclusion(owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        outcome.borrow().as_ref().unwrap().as_ref().unwrap_err(),
        &error
    );
}

fn internal_end_task_failure(
    identity: TaskIdentity,
    phase: novarocks_execution_contract::TaskFailurePhase,
    reason: crate::execution_error::QueryFailureReason,
    detail: &'static str,
) -> QueryExecutionError {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
    };
    let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
        identity,
        TerminationDetail::Failed(TaskFailure::new_in_phase(
            reason.failure_fact(),
            SafeDetail::new(detail).unwrap(),
            phase,
        )),
    );
    QueryExecutionError::from_execution(
        crate::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap(),
    )
}

async fn join_original_internal_actor(owner: LogicalExecutionActorOwner) {
    let (actor, join) = owner.into_test_parts();
    drop(actor);
    tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("the original internal actor must actually exit")
        .unwrap();
}

#[tokio::test]
async fn provisional_end_failure_waiter_receives_the_original_frozen_task_input() {
    use crate::execution_error::{ExecutionFailureSite, QueryFailureReason};
    use novarocks_execution_contract::{TaskFailurePhase, failure::FailureProjection};

    let mut fixture = InternalFixture::new(12_401).await;
    // Complete the real actor-delivered End, but never propose Ready.
    let decoded = fixture.decoded_end().await;
    let origin = root_task(fixture.running.identity().execution(), 2);
    let original = internal_end_task_failure(
        origin,
        TaskFailurePhase::Preparation,
        QueryFailureReason::TransportCapacity,
        "the original peer Task exhausted its admitted preparation position",
    );
    let original_input = original.execution_failure().unwrap().clone();
    assert_eq!(
        original_input.site(),
        ExecutionFailureSite::Task {
            identity: origin,
            phase: TaskFailurePhase::Preparation,
        }
    );
    assert_eq!(
        original_input.fact(),
        QueryFailureReason::TransportCapacity.failure_fact()
    );
    assert!(fixture.activation.stream.current_failure().is_none());
    let mut waiting = Box::pin(fixture.activation.stream.failure());
    poll_pending(waiting.as_mut()).await;
    fixture
        .running
        .freeze_result_attempt_failure(original.clone())
        .await
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the retired original stream must wake its pending failure waiter");
    assert_eq!(observed.execution_failure(), Some(&original_input));
    assert_eq!(observed, original);
    assert_eq!(
        fixture.activation.stream.current_failure(),
        Some(original.clone())
    );
    assert!(fixture.activation.ready.borrow().is_none());
    assert!(
        fixture
            .owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .is_none()
    );
    let snapshot = fixture.owner.actor().snapshot().await.unwrap();
    assert_eq!(snapshot.conclusion, None);
    assert!(!snapshot.output_visible);
    // Attempt freezing still does not choose the logical terminal verdict.
    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = fixture;
    let running = Arc::try_unwrap(running).expect("the original End sender actually exited");
    assert_eq!(
        owner
            .actor()
            .fail_attempt_with_error(running, original)
            .await
            .unwrap(),
        LogicalConclusion::Failed
    );
    drop((decoded, observer, activation, _output));
    join_original_internal_actor(owner).await;
}

#[tokio::test]
async fn replaced_provisional_end_failure_stays_with_its_original_stream() {
    use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
    use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, failure::FailureProjection};

    let qualification = Arc::new(DelayedQualificationPort::default());
    let mut fixture = InternalFixture::with_clock_and_qualification(
        12_402,
        Arc::new(ProcessMonotonicClock::new()),
        Arc::clone(&qualification),
    )
    .await;
    let old_decoded = fixture.decoded_end().await;
    let original = QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        SafeDetail::new("the original attempt lost its exact backend before Ready").unwrap(),
        ExecutionFailureSite::Backend {
            process: BackendProcessId::new_v7(),
        },
    ));
    assert_eq!(
        original.execution_failure().unwrap().attempt_class(),
        AttemptFailureClass::RecoverableInfrastructure
    );
    fixture
        .running
        .freeze_result_attempt_failure(original.clone())
        .await
        .unwrap();
    assert_eq!(fixture.activation.stream.failure().await, original);
    let InternalFixture {
        owner,
        running,
        observer: old_observer,
        activation: mut old,
        _output,
    } = fixture;
    let running = Arc::try_unwrap(running).expect("the old original End sender actually exited");
    let successor = replace_internal_attempt(&owner, running, &qualification).await;
    let next_root = root_task(successor.identity().execution(), 1);
    let next_activation = successor.bind_internal_consumer(next_root).await.unwrap();
    let next_observer = successor.bind_root_result(next_root).await.unwrap();
    let mut next = InternalFixture {
        owner,
        running: Arc::new(successor),
        observer: next_observer,
        activation: next_activation,
        _output,
    };
    assert!(old.source.accept(old_decoded).await.is_err());
    let next_decoded = next.decoded_end().await;
    assert!(next.activation.stream.current_failure().is_none());
    assert_eq!(old.stream.current_failure(), Some(original.clone()));
    assert!(
        next.owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .is_none()
    );

    let current = internal_end_task_failure(
        root_task(next.running.identity().execution(), 3),
        TaskFailurePhase::Preparation,
        QueryFailureReason::TransportCapacity,
        "the successor peer Task exhausted its own preparation position",
    );
    assert_ne!(current.execution_failure(), original.execution_failure());
    let mut waiting = Box::pin(next.activation.stream.failure());
    poll_pending(waiting.as_mut()).await;
    next.running
        .freeze_result_attempt_failure(current.clone())
        .await
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the successor stream must receive only its original failure");
    assert_eq!(observed.execution_failure(), current.execution_failure());
    assert_eq!(observed, current);
    assert_eq!(old.stream.failure().await, original);
    assert!(old.ready.borrow().is_none());
    assert!(next.activation.ready.borrow().is_none());
    assert_eq!(
        next.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );

    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = next;
    let running =
        Arc::try_unwrap(running).expect("the successor original End sender actually exited");
    assert_eq!(
        owner
            .actor()
            .fail_attempt_with_error(running, current)
            .await
            .unwrap(),
        LogicalConclusion::Failed
    );
    drop((
        old_observer,
        old.stream,
        old.ready,
        next_decoded,
        observer,
        activation,
        _output,
    ));
    join_original_internal_actor(owner).await;
}

#[tokio::test]
async fn provisional_end_failure_waiter_receives_the_accepted_governed_cancellation() {
    use crate::execution_error::{ExecutionFailureSite, QueryFailureReason};
    use novarocks_execution_contract::failure::FailureProjection;

    let mut fixture = InternalFixture::new(12_403).await;
    let decoded = fixture.decoded_end().await;
    let mut outcome = fixture.owner.actor().internal_outcome().unwrap();
    let mut waiting = Box::pin(fixture.activation.stream.failure());
    poll_pending(waiting.as_mut()).await;
    fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    let observed = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("governed cancellation must wake the provisional End waiter");
    let input = observed.execution_failure().unwrap();
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(input.fact(), QueryFailureReason::Cancelled.failure_fact());
    assert_eq!(
        observed.message(),
        "logical execution was cancelled before success EOF"
    );
    assert_eq!(
        fixture.activation.stream.current_failure(),
        Some(observed.clone())
    );
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &observed
    );
    let accepted = tokio::time::timeout(
        Duration::from_secs(5),
        outcome.wait_for(|value| value.is_some()),
    )
    .await
    .expect("the original actor must publish its selected outcome")
    .unwrap();
    assert_eq!(accepted.as_ref().unwrap().as_ref().unwrap_err(), &observed);
    drop(accepted);
    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = fixture;
    drop((decoded, running, observer, activation, _output, outcome));
    join_original_internal_actor(owner).await;
}
