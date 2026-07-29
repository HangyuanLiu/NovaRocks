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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use novarocks::UniqueId;
use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::contract::{DistributedQueryIntent, QueryId};
use novarocks::query_execution::fragment_transport::{
    ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope,
};
use novarocks::query_execution::lifecycle::{
    AttemptId, BackendQueryControl, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantQueryOptions, ParticipantRole, QueryAbortRequest, QueryControlAttach,
    QueryControlAttachment, QueryControlCommand, QueryControlEndpoint, QueryControlEvent,
    QueryExecutionId, QueryInitAck, QueryInitBarrier, QueryInitOutcome, QueryInitPlan,
    QueryInitRequest, QueryLifecycleError, QueryLifecycleErrorCode, QueryLifecycleIngress,
    QueryTerminationAck, QueryTerminationReason, RuntimeFilterContribution,
};
use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
use novarocks::runtime::query_options::QueryOptions;
use novarocks::service::grpc_query_lifecycle_client::new_grpc_query_lifecycle_transport;
use novarocks::service::grpc_server::GrpcService;
use novarocks::service::native_fragment_ingress::{
    NativeFragmentAccepted, NativeFragmentCancelRequest, NativeFragmentIngress,
    NativeFragmentIngressError, NativeFragmentRequest,
};

use super::barrier::{
    FrontendQueryLifecycleBarrier, FrontendQueryLifecycleConfig, PreReadyAttemptGuard,
};
use super::lease::{AttemptControl, FrontendLifecycleMetrics};
use super::{
    QueryControlSession, QueryLifecycleTarget, QueryLifecycleTransport,
    QueryLifecycleTransportError, QueryLifecycleTransportErrorKind,
};
use crate::coordinator::query_registry::{ActiveQueryAttemptControl, FrontendQueryRegistry};

#[derive(Default)]
struct NoopFragmentDispatcher;

impl FragmentDispatcher for NoopFragmentDispatcher {
    fn submit_fragment(
        &self,
        _backend_idx: usize,
        _submission: NativeFragmentEnvelope,
    ) -> Result<(), String> {
        unreachable!("query lifecycle unit tests do not submit fragments")
    }

    fn fetch_result(
        &self,
        _backend_idx: usize,
        _finst_id: UniqueId,
        _max_wait_ms: i64,
        _expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String> {
        unreachable!("query lifecycle unit tests do not fetch results")
    }

    fn cancel_fragments(&self, _backend_idx: usize, _query_id: QueryId, _finst_ids: &[UniqueId]) {}

    fn backend_count(&self) -> usize {
        3
    }
}

#[derive(Clone)]
struct RecordingSession {
    state: Arc<(Mutex<RecordingSessionState>, Condvar)>,
}

#[derive(Default)]
struct RecordingSessionState {
    commands: Vec<QueryControlCommand>,
    events: VecDeque<Result<QueryControlEvent, QueryLifecycleTransportError>>,
    send_errors: VecDeque<QueryLifecycleTransportError>,
}

impl RecordingSession {
    fn with_events(
        events: impl IntoIterator<Item = Result<QueryControlEvent, QueryLifecycleTransportError>>,
    ) -> Self {
        let state = RecordingSessionState {
            events: events.into_iter().collect(),
            ..RecordingSessionState::default()
        };
        Self {
            state: Arc::new((Mutex::new(state), Condvar::new())),
        }
    }

    fn commands(&self) -> Vec<QueryControlCommand> {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .commands
            .clone()
    }

    fn fail_next_send(&self, error: QueryLifecycleTransportError) {
        self.state
            .0
            .lock()
            .expect("recording session lock")
            .send_errors
            .push_back(error);
    }
}

impl QueryControlSession for RecordingSession {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
        let mut state = self.state.0.lock().expect("recording session lock");
        if let Some(error) = state.send_errors.pop_front() {
            state.commands.push(command);
            return Err(error);
        }
        let terminal = match &command {
            QueryControlCommand::Abort { .. } => Some(QueryTerminationReason::CoordinatorAbort),
            QueryControlCommand::Finalize => Some(QueryTerminationReason::CoordinatorFinalize),
            QueryControlCommand::Heartbeat { .. } => None,
        };
        state.commands.push(command);
        if let Some(reason) = terminal {
            state
                .events
                .push_back(Ok(QueryControlEvent::TerminationAccepted { reason }));
        }
        drop(state);
        self.state.1.notify_all();
        Ok(())
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError> {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().expect("recording session lock");
        if state.events.is_empty() {
            let (next, _) = ready
                .wait_timeout(state, timeout)
                .expect("recording session wait");
            state = next;
        }
        state.events.pop_front().unwrap_or_else(|| {
            Err(QueryLifecycleTransportError::new(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                "recording session receive timed out",
            ))
        })
    }
}

#[derive(Clone)]
struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
struct RecordingTransportState {
    init_results: BTreeMap<usize, VecDeque<Result<QueryInitAck, QueryLifecycleTransportError>>>,
    attach_results: BTreeMap<
        usize,
        VecDeque<Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError>>,
    >,
    init_calls: Vec<(QueryLifecycleTarget, QueryInitRequest)>,
    attach_calls: Vec<(QueryLifecycleTarget, QueryControlAttach)>,
    abort_calls: Vec<(QueryLifecycleTarget, QueryAbortRequest)>,
    abort_results:
        BTreeMap<usize, VecDeque<Result<QueryTerminationAck, QueryLifecycleTransportError>>>,
}

impl RecordingTransport {
    fn ready(plan: &QueryInitPlan) -> (Self, BTreeMap<usize, RecordingSession>) {
        let mut state = RecordingTransportState::default();
        let mut sessions = BTreeMap::new();
        for backend_idx in plan.backend_ids() {
            let participant = plan
                .participant(backend_idx)
                .expect("fixture participant exists");
            state.init_results.insert(
                backend_idx,
                VecDeque::from([Ok(QueryInitAck::new(
                    plan.execution_id(),
                    participant.digest(),
                    QueryInitOutcome::Applied,
                ))]),
            );
            let session = RecordingSession::with_events([Ok(QueryControlEvent::ControlReady)]);
            state.attach_results.insert(
                backend_idx,
                VecDeque::from([Ok(Arc::new(session.clone()) as Arc<dyn QueryControlSession>)]),
            );
            sessions.insert(backend_idx, session);
        }
        (
            Self {
                state: Arc::new(Mutex::new(state)),
            },
            sessions,
        )
    }

    fn init_calls(&self) -> Vec<(QueryLifecycleTarget, QueryInitRequest)> {
        self.state
            .lock()
            .expect("recording transport lock")
            .init_calls
            .clone()
    }

    fn attach_targets(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("recording transport lock")
            .attach_calls
            .iter()
            .map(|(target, _)| target.backend_idx())
            .collect()
    }

    fn abort_targets(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("recording transport lock")
            .abort_calls
            .iter()
            .map(|(target, _)| target.backend_idx())
            .collect()
    }
}

impl QueryLifecycleTransport for RecordingTransport {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        _timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
        let mut state = self.state.lock().expect("recording transport lock");
        state.init_calls.push((target, request));
        state
            .init_results
            .get_mut(&target.backend_idx())
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                Err(QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::InvalidResponse,
                    "unexpected InitQuery call",
                ))
            })
    }

    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        _timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
        let mut state = self.state.lock().expect("recording transport lock");
        state.attach_calls.push((target, attach));
        state
            .attach_results
            .get_mut(&target.backend_idx())
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                Err(QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::InvalidResponse,
                    "unexpected control attach call",
                ))
            })
    }

    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        _timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError> {
        let mut state = self.state.lock().expect("recording transport lock");
        state.abort_calls.push((target, request.clone()));
        state
            .abort_results
            .get_mut(&target.backend_idx())
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                Ok(QueryTerminationAck::new(
                    request.execution_id(),
                    QueryTerminationReason::CoordinatorAbort,
                ))
            })
    }
}

fn transport_error(
    kind: QueryLifecycleTransportErrorKind,
    detail: &str,
) -> QueryLifecycleTransportError {
    QueryLifecycleTransportError::new(kind, detail)
}

fn query_execution_id() -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(71, 72),
        AttemptId::new(1).expect("fixture attempt id"),
    )
    .expect("fixture execution id")
}

fn manifest(
    execution_id: QueryExecutionId,
    backend_idx: usize,
    service_only: bool,
) -> ParticipantManifest {
    let endpoint = QueryControlEndpoint::new("127.0.0.1", 18_000 + backend_idx as u16)
        .expect("fixture backend endpoint");
    let backend =
        ParticipantBackendIdentity::new(backend_idx as u64, endpoint, 90 + backend_idx as u64)
            .expect("fixture backend identity");
    let (roles, fragments, runtime_filter) = if service_only {
        (
            BTreeSet::from([ParticipantRole::RuntimeFilterService]),
            BTreeSet::new(),
            Some(
                RuntimeFilterContribution::empty_for_contract_test(
                    execution_id,
                    backend_idx as u32 + 1,
                )
                .expect("fixture runtime-filter contribution"),
            ),
        )
    } else {
        (
            BTreeSet::from([ParticipantRole::FragmentExecutor]),
            BTreeSet::from([UniqueId {
                hi: 100,
                lo: backend_idx as i64 + 1,
            }]),
            None,
        )
    };
    ParticipantManifest::new(
        execution_id,
        backend,
        roles,
        fragments,
        ParticipantQueryOptions::new(QueryOptions::default()),
        1_900_000_000_000,
        [],
        runtime_filter,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 19_000).expect("fixture report endpoint"),
    )
    .expect("fixture participant manifest")
}

fn query_init_plan(service_only_backend: Option<usize>) -> QueryInitPlan {
    let execution_id = query_execution_id();
    QueryInitPlan::from_manifests_for_contract_test(
        execution_id,
        (0..3).map(|backend_idx| {
            (
                backend_idx,
                manifest(
                    execution_id,
                    backend_idx,
                    service_only_backend == Some(backend_idx),
                ),
            )
        }),
    )
    .expect("fixture init plan")
}

fn registry_for(
    plan: &QueryInitPlan,
) -> (
    Arc<FrontendQueryRegistry>,
    super::super::query_registry::ActiveQueryGuard,
) {
    let registry = Arc::new(FrontendQueryRegistry::default());
    let guard = registry
        .register(
            plan.execution_id().query_id(),
            DistributedQueryIntent::Result,
            Arc::new(NoopFragmentDispatcher),
        )
        .expect("register fixture query");
    (registry, guard)
}

fn config() -> FrontendQueryLifecycleConfig {
    FrontendQueryLifecycleConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(150),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .expect("fixture lifecycle config")
}

#[test]
fn frontend_query_lifecycle_config_requires_three_heartbeat_intervals() {
    let invalid = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(100),
        Duration::from_millis(20),
        Duration::from_millis(20),
    );
    assert!(invalid.is_err(), "50/100 must violate the 3x bound");

    FrontendQueryLifecycleConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(150),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .expect("50/150 must satisfy the 3x bound");
}

#[test]
fn query_control_barrier_precedes_submission() {
    let plan = query_init_plan(Some(2));
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let lease = barrier
        .initialize_all(plan)
        .expect("all Init and ControlReady acknowledgements precede submission eligibility");

    assert_eq!(sorted(transport.attach_targets()), vec![0, 1, 2]);
    assert_eq!(transport.init_calls().len(), 3);
    lease.finalize().expect("finalize lifecycle fixture");
}

#[test]
fn frontend_query_lifecycle_pre_ready_guard_unwind_aborts_and_unbinds() {
    let plan = query_init_plan(None);
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let materialized = super::manifest::materialize(plan).expect("materialize fixture plan");
    let metrics = Arc::new(FrontendLifecycleMetrics::default());
    let control = AttemptControl::new(
        execution_id,
        Arc::new(transport.clone()),
        Arc::downgrade(&registry),
        config(),
        Arc::clone(&metrics),
    );
    control.set_attempted(&materialized.participants);
    let active: Arc<dyn ActiveQueryAttemptControl> = control.clone();
    let binding = registry
        .bind_active_attempt(execution_id, active)
        .expect("bind fixture attempt");
    let guard = PreReadyAttemptGuard::new(control, binding);
    let initialized = materialized.participants[0].clone();
    let init_transport = transport.clone();

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        init_transport
            .init_query(
                initialized.target,
                initialized.request,
                Duration::from_millis(20),
            )
            .expect("first participant Init succeeds before interruption");
        panic!("deterministic interruption after a successful Init");
    }));
    assert!(unwind.is_err(), "fixture must unwind");
    assert_eq!(transport.init_calls().len(), 1);
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);

    let replacement = AttemptControl::new(
        execution_id,
        Arc::new(transport),
        Arc::downgrade(&registry),
        config(),
        metrics,
    );
    let replacement_control: Arc<dyn ActiveQueryAttemptControl> = replacement.clone();
    let replacement_binding = registry
        .bind_active_attempt(execution_id, replacement_control)
        .expect("unwind guard must clear the registry binding");
    replacement.abort_before_ready("fixture cleanup".to_string());
    drop(replacement_binding);
}

fn sorted(mut values: Vec<usize>) -> Vec<usize> {
    values.sort_unstable();
    values
}

fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn frontend_query_lifecycle_all_participant_barrier_aborts_attempted_targets() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().attach_results.insert(
        2,
        VecDeque::from([Err(transport_error(
            QueryLifecycleTransportErrorKind::Unavailable,
            "backend 2 attach failed",
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("one failed attach must not produce a lifecycle lease"),
        Err(error) => error,
    };

    assert!(
        error.message().contains("backend 2 attach failed"),
        "{error}"
    );
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
}

#[test]
fn frontend_query_lifecycle_unknown_init_ack_retries_same_request_once() {
    let plan = query_init_plan(None);
    let retry_digest = plan.participant(1).unwrap().digest();
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([
            Err(transport_error(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                "InitAck was lost",
            )),
            Ok(QueryInitAck::new(
                execution_id,
                retry_digest,
                QueryInitOutcome::AlreadyApplied,
            )),
        ]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    barrier
        .initialize_all(plan)
        .expect("same-digest retry must recover")
        .finalize()
        .expect("fixture finalize");

    let calls = transport.init_calls();
    let backend_one = calls
        .iter()
        .filter(|(target, _)| target.backend_idx() == 1)
        .collect::<Vec<_>>();
    assert_eq!(backend_one.len(), 2);
    assert_eq!(
        backend_one[0].1.manifest().execution_id(),
        backend_one[1].1.manifest().execution_id()
    );
    assert_eq!(backend_one[0].1.digest(), backend_one[1].1.digest());
    assert_eq!(
        calls
            .iter()
            .filter(|(target, _)| target.backend_idx() != 1)
            .count(),
        2
    );
}

#[test]
fn frontend_query_lifecycle_business_rejection_is_not_retried() {
    let plan = query_init_plan(None);
    let rejected_digest = plan.participant(1).unwrap().digest();
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([Ok(QueryInitAck::new(
            execution_id,
            rejected_digest,
            QueryInitOutcome::RejectedCapacity,
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("business rejection must fail the barrier"),
        Err(error) => error,
    };

    assert!(error.message().contains("RejectedCapacity"), "{error}");
    assert_eq!(
        transport
            .init_calls()
            .iter()
            .filter(|(target, _)| target.backend_idx() == 1)
            .count(),
        1
    );
    assert_eq!(barrier.metrics_snapshot().manifest_conflicts, 0);
}

#[test]
fn frontend_query_lifecycle_manifest_conflict_is_classified() {
    let plan = query_init_plan(None);
    let digest = plan.participant(1).unwrap().digest();
    let execution_id = plan.execution_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([Ok(QueryInitAck::new(
            execution_id,
            digest,
            QueryInitOutcome::RejectedConflict,
        ))]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, config());

    match barrier.initialize_all(plan) {
        Ok(_) => panic!("manifest conflict must fail the barrier"),
        Err(error) => assert!(error.message().contains("RejectedConflict"), "{error}"),
    }

    assert_eq!(barrier.metrics_snapshot().manifest_conflicts, 1);
}

#[test]
fn frontend_query_lifecycle_rollback_preserves_primary_error() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    {
        let mut state = transport.state.lock().unwrap();
        state.attach_results.insert(
            2,
            VecDeque::from([Err(transport_error(
                QueryLifecycleTransportErrorKind::Unavailable,
                "primary attach failure",
            ))]),
        );
        state.abort_results.insert(
            1,
            VecDeque::from([Err(transport_error(
                QueryLifecycleTransportErrorKind::Unavailable,
                "rollback transport failure",
            ))]),
        );
    }
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("attach failure must fail the barrier"),
        Err(error) => error,
    };

    assert!(
        error
            .message()
            .starts_with("backend 2 control attach failed"),
        "{error}"
    );
    assert!(
        error.message().contains("rollback transport failure"),
        "{error}"
    );
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
    assert_eq!(barrier.metrics_snapshot().attach_failed, 1);
}

#[test]
fn frontend_query_lifecycle_unary_fallback_accepts_first_wins_terminal_reasons() {
    let plan = query_init_plan(None);
    let execution_id = plan.execution_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    {
        let mut state = transport.state.lock().unwrap();
        for (backend_idx, reason) in [
            (0, QueryTerminationReason::CoordinatorStreamLost),
            (1, QueryTerminationReason::CoordinatorHeartbeatTimeout),
            (2, QueryTerminationReason::LocalFailure),
        ] {
            sessions[&backend_idx].fail_next_send(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "control stream already closed after backend termination",
            ));
            state.abort_results.insert(
                backend_idx,
                VecDeque::from([Ok(QueryTerminationAck::new(execution_id, reason))]),
            );
        }
    }
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    let message = lease.abort_preserving("primary execution failure".to_string());

    assert_eq!(message, "primary execution failure");
    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
}

#[test]
fn frontend_query_lifecycle_unknown_init_cleanup_is_classified() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    transport.state.lock().unwrap().init_results.insert(
        1,
        VecDeque::from([
            Err(transport_error(
                QueryLifecycleTransportErrorKind::DeadlineExceeded,
                "first InitAck outcome unknown",
            )),
            Err(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "retry InitAck outcome unknown",
            )),
        ]),
    );
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, config());

    match barrier.initialize_all(plan) {
        Ok(_) => panic!("unresolved Init outcome must fail the barrier"),
        Err(error) => assert!(error.message().contains("unknown outcome"), "{error}"),
    }

    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.init_failed, 1);
    assert_eq!(snapshot.init_uncertain_cleanup, 1);
}

#[test]
fn frontend_query_lifecycle_epoch_mismatch_is_classified_before_init() {
    let plan = query_init_plan(None);
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    registry.replace_live_backends(
        1,
        &[
            LiveBackendTarget::new(0, "127.0.0.1:18000".parse().unwrap(), 90),
            LiveBackendTarget::new(1, "127.0.0.1:18001".parse().unwrap(), 999),
            LiveBackendTarget::new(2, "127.0.0.1:18002".parse().unwrap(), 92),
        ],
    );
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    match barrier.initialize_all(plan) {
        Ok(_) => panic!("stale backend generation must fail the barrier"),
        Err(error) => assert!(error.message().contains("stale"), "{error}"),
    }

    assert!(transport.init_calls().is_empty());
    assert_eq!(barrier.metrics_snapshot().backend_epoch_mismatches, 1);
}

#[test]
fn frontend_query_lifecycle_drop_cleanup_failure_is_observable() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    {
        let mut state = transport.state.lock().unwrap();
        for backend_idx in 0..3 {
            sessions[&backend_idx].fail_next_send(transport_error(
                QueryLifecycleTransportErrorKind::StreamClosed,
                "drop cleanup stream unavailable",
            ));
            state.abort_results.insert(
                backend_idx,
                VecDeque::from([Err(transport_error(
                    QueryLifecycleTransportErrorKind::Unavailable,
                    "drop cleanup unary fallback unavailable",
                ))]),
            );
        }
    }
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    drop(lease);

    assert_eq!(sorted(transport.abort_targets()), vec![0, 1, 2]);
    assert_eq!(barrier.metrics_snapshot().cleanup_failures, 3);
}

#[test]
fn frontend_query_lifecycle_lease_drop_without_finalize_aborts_all() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");
    drop(lease);

    for session in sessions.values() {
        assert!(
            session
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
}

#[test]
fn frontend_query_lifecycle_lease_finalize_sends_once() {
    let plan = query_init_plan(None);
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::new(transport), registry, config());

    barrier
        .initialize_all(plan)
        .expect("all participants ready")
        .finalize()
        .expect("finalize all participants");

    for session in sessions.values() {
        assert_eq!(
            session
                .commands()
                .iter()
                .filter(|command| matches!(command, QueryControlCommand::Finalize))
                .count(),
            1
        );
    }
}

#[test]
fn frontend_query_lifecycle_lease_duplicate_abort_is_idempotent() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport.clone()),
        Arc::clone(&registry),
        config(),
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    registry
        .request_active_attempt_abort(query_id, "first abort".to_string())
        .expect("first abort request");
    registry
        .request_active_attempt_abort(query_id, "duplicate abort".to_string())
        .expect("duplicate abort request");
    drop(lease);

    for session in sessions.values() {
        assert_eq!(
            session
                .commands()
                .iter()
                .filter(|command| matches!(command, QueryControlCommand::Abort { .. }))
                .count(),
            1
        );
    }
}

#[test]
fn frontend_query_lifecycle_lease_local_failure_aborts_other_participants() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    sessions.get(&0).unwrap().state.0.lock().unwrap().events = VecDeque::from([
        Ok(QueryControlEvent::ControlReady),
        Ok(QueryControlEvent::LocalFailure {
            code: "LOCAL_SCAN_FAILURE".to_string(),
            detail: "backend 0 scan failed".to_string(),
        }),
    ]);
    for backend_idx in [1, 2] {
        sessions
            .get(&backend_idx)
            .unwrap()
            .state
            .0
            .lock()
            .unwrap()
            .events = VecDeque::from([
            Ok(QueryControlEvent::ControlReady),
            Ok(QueryControlEvent::HeartbeatAck { sequence: 1 }),
        ]);
    }
    let (registry, _query) = registry_for(&plan);
    let local_failure_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(20),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .unwrap();
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        local_failure_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    wait_until(Duration::from_secs(1), || {
        registry
            .first_failure(query_id)
            .is_some_and(|failure| failure.contains("backend 0 scan failed"))
            && [1, 2].into_iter().all(|backend_idx| {
                sessions[&backend_idx]
                    .commands()
                    .iter()
                    .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
            })
    });
    for backend_idx in [1, 2] {
        assert!(
            sessions[&backend_idx]
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.local_failures, 1);
    assert_eq!(snapshot.coordinator_lost, 0);
    assert_eq!(snapshot.heartbeat_timeouts, 0);
    drop(lease);
}

#[test]
fn frontend_query_lifecycle_lease_service_only_participant_joins_barrier() {
    let plan = query_init_plan(Some(2));
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    barrier
        .initialize_all(plan)
        .expect("service-only participant must become control ready")
        .finalize()
        .expect("fixture finalize");

    assert_eq!(sorted(transport.attach_targets()), vec![0, 1, 2]);
    let service_request = transport
        .init_calls()
        .into_iter()
        .find(|(target, _)| target.backend_idx() == 2)
        .expect("service-only InitQuery");
    assert_eq!(
        service_request.1.manifest().roles(),
        &BTreeSet::from([ParticipantRole::RuntimeFilterService])
    );
    assert!(
        service_request
            .1
            .manifest()
            .expected_fragment_instance_ids()
            .is_empty()
    );
}

#[test]
fn frontend_query_lifecycle_heartbeat_timeout_fails_closed() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    for session in sessions.values() {
        session.state.0.lock().unwrap().events =
            VecDeque::from([Ok(QueryControlEvent::ControlReady)]);
    }
    let (registry, _query) = registry_for(&plan);
    let heartbeat_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .unwrap();
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        heartbeat_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    wait_until(Duration::from_secs(1), || {
        registry
            .first_failure(query_id)
            .is_some_and(|failure| failure.contains("heartbeat"))
    });
    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.heartbeat_timeouts, 1);
    assert_eq!(snapshot.local_failures, 0);
    assert_eq!(snapshot.coordinator_lost, 0);
    drop(lease);
}

#[test]
fn frontend_query_lifecycle_stream_loss_is_classified_as_coordinator_lost() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    sessions.get(&0).unwrap().state.0.lock().unwrap().events = VecDeque::from([
        Ok(QueryControlEvent::ControlReady),
        Err(transport_error(
            QueryLifecycleTransportErrorKind::StreamClosed,
            "backend 0 stream closed",
        )),
    ]);
    for backend_idx in [1, 2] {
        sessions
            .get(&backend_idx)
            .unwrap()
            .state
            .0
            .lock()
            .unwrap()
            .events = VecDeque::from([
            Ok(QueryControlEvent::ControlReady),
            Ok(QueryControlEvent::HeartbeatAck { sequence: 1 }),
        ]);
    }
    let (registry, _query) = registry_for(&plan);
    let stream_loss_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .unwrap();
    let barrier = FrontendQueryLifecycleBarrier::new(
        Arc::new(transport),
        Arc::clone(&registry),
        stream_loss_config,
    );
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    wait_until(Duration::from_secs(1), || {
        registry
            .first_failure(query_id)
            .is_some_and(|failure| failure.contains("stream lost"))
    });
    let snapshot = barrier.metrics_snapshot();
    assert_eq!(snapshot.coordinator_lost, 1);
    assert_eq!(snapshot.local_failures, 0);
    assert_eq!(snapshot.heartbeat_timeouts, 0);
    drop(lease);
}

#[test]
fn frontend_query_lifecycle_query_registry_pre_init_cancellation_blocks_fanout() {
    let plan = query_init_plan(None);
    let query_id = plan.execution_id().query_id();
    let (transport, _) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    registry
        .latch_failure_and_cancel(query_id, "client cancelled before InitQuery")
        .expect("latch pre-init cancellation");
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport.clone()), registry, config());

    let error = match barrier.initialize_all(plan) {
        Ok(_) => panic!("pre-init cancellation must not produce a lifecycle lease"),
        Err(error) => error,
    };

    assert!(error.message().contains("client cancelled"), "{error}");
    assert!(transport.init_calls().is_empty());
}

#[test]
fn frontend_query_lifecycle_query_registry_service_only_backend_loss_aborts_attempt() {
    let plan = query_init_plan(Some(2));
    let query_id = plan.execution_id().query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let (registry, _query) = registry_for(&plan);
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport), Arc::clone(&registry), config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants ready");

    assert_eq!(
        registry.backend_failed(2, "service-only backend unavailable".to_string()),
        vec![query_id]
    );
    for session in sessions.values() {
        assert!(
            session
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
    drop(lease);
}

#[derive(Default)]
struct RecordingLegacyCancellationDispatcher {
    cancellations: std::sync::atomic::AtomicUsize,
}

impl FragmentDispatcher for RecordingLegacyCancellationDispatcher {
    fn submit_fragment(
        &self,
        _backend_idx: usize,
        _submission: NativeFragmentEnvelope,
    ) -> Result<(), String> {
        unreachable!("cancellation test does not submit fragments")
    }

    fn fetch_result(
        &self,
        _backend_idx: usize,
        _finst_id: UniqueId,
        _max_wait_ms: i64,
        _expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String> {
        unreachable!("cancellation test does not fetch fragments")
    }

    fn cancel_fragments(&self, _backend_idx: usize, _query_id: QueryId, _finst_ids: &[UniqueId]) {
        self.cancellations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn backend_count(&self) -> usize {
        3
    }
}

#[test]
fn query_cancel_aborts_all_participants() {
    let plan = query_init_plan(Some(2));
    let execution_id = plan.execution_id();
    let query_id = execution_id.query_id();
    let (transport, sessions) = RecordingTransport::ready(&plan);
    let dispatcher = Arc::new(RecordingLegacyCancellationDispatcher::default());
    let registry = Arc::new(FrontendQueryRegistry::default());
    let _query = registry
        .register(query_id, DistributedQueryIntent::Result, dispatcher.clone())
        .expect("register cancellation fixture");
    let barrier =
        FrontendQueryLifecycleBarrier::new(Arc::new(transport), Arc::clone(&registry), config());
    let lease = barrier
        .initialize_all(plan)
        .expect("all participants become control-ready");
    let submitted = manifest(execution_id, 0, false)
        .expected_fragment_instance_ids()
        .iter()
        .next()
        .copied()
        .expect("fragment participant has one instance");
    registry
        .record_attempt(query_id, 0, submitted)
        .expect("record the only submitted fragment");
    registry
        .finish_attempt(query_id)
        .expect("finish the only submission");

    registry
        .latch_failure_and_cancel(query_id, "client requested statement cancellation")
        .expect("first cancellation wins");

    for session in sessions.values() {
        assert_eq!(
            session
                .commands()
                .iter()
                .filter(|command| matches!(command, QueryControlCommand::Abort { .. }))
                .count(),
            1,
            "every initialized participant, including service-only, receives one stream Abort"
        );
    }
    assert_eq!(
        dispatcher
            .cancellations
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "QLC-1A cancellation must not fall back to attempted fragment cancellation"
    );
    drop(lease);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_crosses_generated_grpc_service() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;

    let execution_id = query_execution_id();
    let backend = LiveBackendTarget::new(7, endpoint, 77);
    let live_manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::from_live_backend(backend).expect("backend identity"),
        [ParticipantRole::FragmentExecutor],
        [UniqueId { hi: 801, lo: 1 }],
        ParticipantQueryOptions::new(QueryOptions::default()),
        1_900_000_000_000,
        [],
        None,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 19_000).expect("report endpoint"),
    )
    .expect("live manifest");
    let digest = QueryInitRequest::from_manifest(live_manifest.clone()).digest();
    let plan = QueryInitPlan::from_manifests_for_contract_test(execution_id, [(7, live_manifest)])
        .expect("live plan");
    let (registry, _query) = registry_for(&plan);
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");
    let live_config = FrontendQueryLifecycleConfig::new(
        Duration::from_millis(100),
        Duration::from_millis(300),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .expect("live lifecycle config");
    let barrier = FrontendQueryLifecycleBarrier::new(Arc::clone(&transport), registry, live_config);

    barrier
        .initialize_all(plan)
        .expect("Init and ControlReady cross the generated gRPC service")
        .finalize()
        .expect("Finalize crosses the same control stream");
    let abort_ack = transport
        .abort_query(
            QueryLifecycleTarget::new(7, endpoint, 77),
            QueryAbortRequest::new(execution_id, digest, "idempotent cleanup")
                .expect("abort request"),
            Duration::from_secs(2),
        )
        .expect("AbortQuery crosses the generated gRPC service");
    assert_eq!(abort_ack.execution_id(), execution_id);

    assert_eq!(
        ingress
            .initialized_backend
            .lock()
            .expect("initialized backend")
            .clone(),
        Some(ParticipantBackendIdentity::from_live_backend(backend).expect("identity"))
    );
    assert!(ingress.finalized.load(std::sync::atomic::Ordering::Acquire));

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_backpressures_and_surfaces_stream_reset() {
    let gate = Arc::new(LiveHeartbeatGate::default());
    let ingress = Arc::new(LiveLifecycleIngress {
        gate: Some(Arc::clone(&gate)),
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress).await;
    let backend = LiveBackendTarget::new(7, endpoint, 88);
    let target = QueryLifecycleTarget::new(7, endpoint, 88);
    let live_manifest = ParticipantManifest::new(
        query_execution_id(),
        ParticipantBackendIdentity::from_live_backend(backend).expect("backend identity"),
        [ParticipantRole::FragmentExecutor],
        [UniqueId { hi: 802, lo: 1 }],
        ParticipantQueryOptions::new(QueryOptions::default()),
        1_900_000_000_000,
        [],
        None,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 19_000).expect("report endpoint"),
    )
    .expect("live manifest");
    let request = QueryInitRequest::from_manifest(live_manifest);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            QueryControlAttach::new(execution_id, digest, 9).expect("attach"),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        QueryControlEvent::ControlReady
    );

    for sequence in 0..32 {
        session
            .send(QueryControlCommand::Heartbeat {
                sequence,
                sent_mono_ns: sequence,
            })
            .expect("bounded command");
    }
    let error = session
        .send(QueryControlCommand::Heartbeat {
            sequence: 33,
            sent_mono_ns: 33,
        })
        .expect_err("the 33rd unacknowledged command must backpressure");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::Backpressure);
    wait_until(Duration::from_secs(2), || {
        gate.entered.load(std::sync::atomic::Ordering::Acquire)
    });
    gate.release
        .store(true, std::sync::atomic::Ordering::Release);
    let error = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("server reset must close the stream");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed);

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_closes_commands_before_terminal_is_observed() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 89);
    let target = QueryLifecycleTarget::new(7, endpoint, 89);
    let request = live_init_request(backend, 803);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            QueryControlAttach::new(execution_id, digest, 11).expect("attach"),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        QueryControlEvent::ControlReady
    );

    session
        .send(QueryControlCommand::Finalize)
        .expect("send finalize");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("TerminationAccepted"),
        QueryControlEvent::TerminationAccepted {
            reason: QueryTerminationReason::CoordinatorFinalize,
        }
    );
    let error = session
        .send(QueryControlCommand::Heartbeat {
            sequence: 1,
            sent_mono_ns: 1,
        })
        .expect_err("terminal observation must imply a closed command side");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed);

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_ack_releases_only_its_pending_command() {
    let ingress = Arc::new(LiveLifecycleIngress {
        manual_heartbeat_acks: true,
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 90);
    let target = QueryLifecycleTarget::new(7, endpoint, 90);
    let request = live_init_request(backend, 804);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            QueryControlAttach::new(execution_id, digest, 12).expect("attach"),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        QueryControlEvent::ControlReady
    );

    for sequence in 0..32 {
        session
            .send(QueryControlCommand::Heartbeat {
                sequence,
                sent_mono_ns: sequence,
            })
            .expect("fill pending command capacity");
    }
    assert_eq!(
        session
            .send(QueryControlCommand::Heartbeat {
                sequence: 32,
                sent_mono_ns: 32,
            })
            .expect_err("33rd pending command must backpressure")
            .kind(),
        QueryLifecycleTransportErrorKind::Backpressure
    );

    ingress.send_control_event(QueryControlEvent::HeartbeatAck { sequence: 0 });
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("matching heartbeat acknowledgement"),
        QueryControlEvent::HeartbeatAck { sequence: 0 }
    );
    session
        .send(QueryControlCommand::Heartbeat {
            sequence: 32,
            sent_mono_ns: 32,
        })
        .expect("one matching acknowledgement releases exactly one slot");
    assert_eq!(
        session
            .send(QueryControlCommand::Heartbeat {
                sequence: 33,
                sent_mono_ns: 33,
            })
            .expect_err("only one slot was released")
            .kind(),
        QueryLifecycleTransportErrorKind::Backpressure
    );

    ingress.send_control_event(QueryControlEvent::HeartbeatAck { sequence: 0 });
    let error = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("duplicate acknowledgement must terminate the invalid stream");
    assert_eq!(
        error.kind(),
        QueryLifecycleTransportErrorKind::InvalidResponse
    );
    assert_eq!(
        session
            .send(QueryControlCommand::Heartbeat {
                sequence: 33,
                sent_mono_ns: 33,
            })
            .expect_err("duplicate acknowledgement must not release capacity")
            .kind(),
        QueryLifecycleTransportErrorKind::InvalidResponse
    );

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_rejects_mismatched_terminal_ack() {
    let ingress = Arc::new(LiveLifecycleIngress {
        manual_terminal_acks: true,
        ..Default::default()
    });
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 91);
    let target = QueryLifecycleTarget::new(7, endpoint, 91);
    let request = live_init_request(backend, 805);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");
    transport
        .init_query(target, request, Duration::from_secs(2))
        .expect("InitQuery");
    let session = transport
        .attach_control(
            target,
            QueryControlAttach::new(execution_id, digest, 13).expect("attach"),
            Duration::from_secs(2),
        )
        .expect("attach");
    assert_eq!(
        session
            .recv_timeout(Duration::from_secs(2))
            .expect("ControlReady"),
        QueryControlEvent::ControlReady
    );

    session
        .send(QueryControlCommand::Finalize)
        .expect("send finalize");
    ingress.send_control_event(QueryControlEvent::TerminationAccepted {
        reason: QueryTerminationReason::CoordinatorAbort,
    });
    let error = session
        .recv_timeout(Duration::from_secs(2))
        .expect_err("Finalize must not accept an Abort acknowledgement");
    assert_eq!(
        error.kind(),
        QueryLifecycleTransportErrorKind::InvalidResponse
    );

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_pre_submission_timeout_is_definite() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress).await;
    let backend = LiveBackendTarget::new(7, endpoint, 92);
    let target = QueryLifecycleTarget::new(7, endpoint, 92);
    let request = live_init_request(backend, 806);
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");

    let error = transport
        .init_query(target, request, Duration::ZERO)
        .expect_err("channel acquisition deadline is a definite pre-submission failure");
    assert_eq!(error.kind(), QueryLifecycleTransportErrorKind::Unavailable);
    assert!(!error.is_unknown_init_outcome());

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_query_lifecycle_live_transport_post_submission_timeout_is_unknown() {
    let ingress = Arc::new(LiveLifecycleIngress::default());
    let (endpoint, shutdown_tx, server) = spawn_frontend_live_server(ingress.clone()).await;
    let backend = LiveBackendTarget::new(7, endpoint, 93);
    let target = QueryLifecycleTarget::new(7, endpoint, 93);
    let transport =
        new_grpc_query_lifecycle_transport(&[backend]).expect("production lifecycle transport");

    transport
        .init_query(
            target,
            live_init_request(backend, 807),
            Duration::from_secs(2),
        )
        .expect("warm the channel before the delayed request");
    *ingress.init_delay.lock().expect("init delay") = Some(Duration::from_millis(100));
    let error = transport
        .init_query(
            target,
            live_init_request(backend, 808),
            Duration::from_millis(20),
        )
        .expect_err("submitted InitQuery must time out while the server is handling it");
    assert!(matches!(
        error.kind(),
        QueryLifecycleTransportErrorKind::DeadlineExceeded
            | QueryLifecycleTransportErrorKind::StreamClosed
    ));
    assert!(error.is_unknown_init_outcome());

    let _ = shutdown_tx.send(());
    server.await.expect("join live lifecycle server");
}

fn live_init_request(backend: LiveBackendTarget, finst_high: i64) -> QueryInitRequest {
    let execution_id = query_execution_id();
    QueryInitRequest::from_manifest(
        ParticipantManifest::new(
            execution_id,
            ParticipantBackendIdentity::from_live_backend(backend).expect("backend identity"),
            [ParticipantRole::FragmentExecutor],
            [UniqueId {
                hi: finst_high,
                lo: 1,
            }],
            ParticipantQueryOptions::new(QueryOptions::default()),
            1_900_000_000_000,
            [],
            None,
            Duration::from_secs(30),
            QueryControlEndpoint::new("127.0.0.1", 19_000).expect("report endpoint"),
        )
        .expect("live manifest"),
    )
}

async fn spawn_frontend_live_server(
    ingress: Arc<dyn QueryLifecycleIngress>,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind live lifecycle server");
    let endpoint = listener.local_addr().expect("live lifecycle endpoint");
    let incoming = futures::stream::unfold(listener, |listener| async {
        let item = listener.accept().await.map(|(stream, _)| stream);
        Some((item, listener))
    });
    let service = GrpcService::with_fragment_execution(
        Arc::new(RejectNativeFragments),
        ingress,
        Arc::new(AcceptReports),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                novarocks::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                    service,
                ),
            )
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve live lifecycle server");
    });
    (endpoint, shutdown_tx, server)
}

#[derive(Default)]
struct LiveLifecycleIngress {
    initialized: Mutex<
        Option<(
            QueryExecutionId,
            novarocks::query_execution::lifecycle::ParticipantManifestDigest,
        )>,
    >,
    initialized_backend: Mutex<Option<ParticipantBackendIdentity>>,
    finalized: Arc<std::sync::atomic::AtomicBool>,
    gate: Option<Arc<LiveHeartbeatGate>>,
    manual_heartbeat_acks: bool,
    manual_terminal_acks: bool,
    init_delay: Mutex<Option<Duration>>,
    control_events: Mutex<Option<tokio::sync::mpsc::Sender<QueryControlEvent>>>,
}

impl LiveLifecycleIngress {
    fn send_control_event(&self, event: QueryControlEvent) {
        self.control_events
            .lock()
            .expect("control events")
            .as_ref()
            .expect("attached control stream")
            .try_send(event)
            .expect("inject control event");
    }
}

impl QueryLifecycleIngress for LiveLifecycleIngress {
    fn bind_backend_identity(&self, _backend_id: u64) -> Result<(), QueryLifecycleError> {
        Ok(())
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        if let Some(delay) = *self.init_delay.lock().expect("init delay") {
            std::thread::sleep(delay);
        }
        let execution_id = request.manifest().execution_id();
        let digest = request.digest();
        *self
            .initialized_backend
            .lock()
            .expect("initialized backend") = Some(request.manifest().backend().clone());
        *self.initialized.lock().expect("initialized") = Some((execution_id, digest));
        QueryInitAck::new(execution_id, digest, QueryInitOutcome::Applied)
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        Ok(QueryTerminationAck::new(
            request.execution_id(),
            QueryTerminationReason::CoordinatorAbort,
        ))
    }

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        if *self.initialized.lock().expect("initialized")
            != Some((attach.execution_id(), attach.digest()))
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "attach identity or digest mismatch",
            ));
        }
        let (events, receiver) = tokio::sync::mpsc::channel(32);
        events
            .try_send(QueryControlEvent::ControlReady)
            .expect("ControlReady");
        *self.control_events.lock().expect("control events") = Some(events.clone());
        Ok(QueryControlAttachment {
            control: Arc::new(LiveBackendControl {
                events,
                finalized: Arc::clone(&self.finalized),
                gate: self.gate.clone(),
                manual_heartbeat_acks: self.manual_heartbeat_acks,
                manual_terminal_acks: self.manual_terminal_acks,
            }),
            events: receiver,
        })
    }
}

struct LiveBackendControl {
    events: tokio::sync::mpsc::Sender<QueryControlEvent>,
    finalized: Arc<std::sync::atomic::AtomicBool>,
    gate: Option<Arc<LiveHeartbeatGate>>,
    manual_heartbeat_acks: bool,
    manual_terminal_acks: bool,
}

impl BackendQueryControl for LiveBackendControl {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError> {
        if let Some(gate) = &self.gate {
            gate.entered
                .store(true, std::sync::atomic::Ordering::Release);
            while !gate.release.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::yield_now();
            }
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Transport,
                "reset live test stream",
            ));
        }
        if self.manual_heartbeat_acks {
            return Ok(());
        }
        self.events
            .try_send(QueryControlEvent::HeartbeatAck { sequence })
            .map_err(live_control_error)
    }

    fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
        if self.manual_terminal_acks {
            return Ok(());
        }
        self.events
            .try_send(QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorAbort,
            })
            .map_err(live_control_error)
    }

    fn finalize(&self) -> Result<(), QueryLifecycleError> {
        self.finalized
            .store(true, std::sync::atomic::Ordering::Release);
        if self.manual_terminal_acks {
            return Ok(());
        }
        self.events
            .try_send(QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            })
            .map_err(live_control_error)
    }

    fn coordinator_lost(&self, _reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        Ok(())
    }
}

#[derive(Default)]
struct LiveHeartbeatGate {
    entered: std::sync::atomic::AtomicBool,
    release: std::sync::atomic::AtomicBool,
}

fn live_control_error(
    error: tokio::sync::mpsc::error::TrySendError<QueryControlEvent>,
) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, error.to_string())
}

struct RejectNativeFragments;

impl NativeFragmentIngress for RejectNativeFragments {
    fn submit(
        &self,
        _request: NativeFragmentRequest,
    ) -> Result<NativeFragmentAccepted, NativeFragmentIngressError> {
        Err(NativeFragmentIngressError::new(
            "live lifecycle test does not submit fragments",
        ))
    }

    fn cancel(
        &self,
        _request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError> {
        Ok(())
    }
}

struct AcceptReports;

impl NativeReportHandler for AcceptReports {
    fn handle_native_report(
        &self,
        _report: novarocks::proto::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError> {
        Ok(())
    }
}
