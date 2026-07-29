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
use novarocks::query_execution::contract::{DistributedQueryIntent, QueryId};
use novarocks::query_execution::fragment_transport::{
    ExpectedOutputSchemaView, FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope,
};
use novarocks::query_execution::lifecycle::{
    AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions,
    ParticipantRole, QueryAbortRequest, QueryControlAttach, QueryControlCommand,
    QueryControlEndpoint, QueryControlEvent, QueryExecutionId, QueryInitAck, QueryInitBarrier,
    QueryInitOutcome, QueryInitPlan, QueryInitRequest, QueryTerminationAck, QueryTerminationReason,
    RuntimeFilterContribution,
};
use novarocks::runtime::query_options::QueryOptions;

use super::barrier::{FrontendQueryLifecycleBarrier, FrontendQueryLifecycleConfig};
use super::{
    QueryControlSession, QueryLifecycleTarget, QueryLifecycleTransport,
    QueryLifecycleTransportError, QueryLifecycleTransportErrorKind,
};
use crate::coordinator::query_registry::FrontendQueryRegistry;

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
}

impl QueryControlSession for RecordingSession {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
        let mut state = self.state.0.lock().expect("recording session lock");
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
        Duration::from_millis(100),
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .expect("fixture lifecycle config")
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
    });
    for backend_idx in [1, 2] {
        assert!(
            sessions[&backend_idx]
                .commands()
                .iter()
                .any(|command| matches!(command, QueryControlCommand::Abort { .. }))
        );
    }
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
