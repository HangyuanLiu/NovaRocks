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

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use novarocks::UniqueId;
use novarocks::query_execution::contract::QueryId;
use novarocks::query_execution::lifecycle::metrics::BackendQueryLifecycleMetricsSnapshot;
use novarocks::query_execution::lifecycle::{
    AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions,
    ParticipantRole, QueryAbortRequest, QueryControlAttach, QueryControlEndpoint,
    QueryControlEvent, QueryExecutionId, QueryInitOutcome, QueryInitRequest, QueryLifecycleError,
    QueryLifecycleErrorCode, QueryTerminationReason, RuntimeFilterContribution,
};
use novarocks::runtime::fragment::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentOutcome,
};
use novarocks::runtime::query_options::QueryOptions;

use super::entry::QueryLifecyclePhase;
use super::registry::{
    MonotonicClock, QueryLifecycleLocalRuntime, QueryLifecycleMetricsSink, QueryLifecycleRegistry,
    QueryLifecycleRegistryConfig,
};

const LOCAL_BACKEND_ID: u64 = 7;
const LOCAL_START_EPOCH: u64 = 11;
const ATTEMPT_1: u64 = 1;

#[derive(Clone)]
struct ManualClock {
    base: Instant,
    offset: Arc<Mutex<Duration>>,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self {
            base: Instant::now(),
            offset: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        *self.offset.lock().expect("manual clock offset") += duration;
    }
}

impl MonotonicClock for ManualClock {
    fn now(&self) -> Instant {
        self.base + *self.offset.lock().expect("manual clock offset")
    }
}

#[derive(Clone, Default)]
struct RecordingLocalRuntime {
    state: Arc<RecordingLocalRuntimeState>,
}

#[derive(Default)]
struct RecordingLocalRuntimeState {
    install_calls: Mutex<Vec<QueryExecutionId>>,
    abort_calls: Mutex<Vec<QueryExecutionId>>,
    terminations: Mutex<Vec<(QueryExecutionId, Vec<UniqueId>, QueryTerminationReason)>>,
    install_gate: Mutex<InstallGate>,
    install_gate_changed: Condvar,
    fail_install: Mutex<bool>,
    fail_abort: Mutex<bool>,
}

#[derive(Default)]
struct RecordingMetricsSink {
    snapshots: Mutex<Vec<BackendQueryLifecycleMetricsSnapshot>>,
}

impl RecordingMetricsSink {
    fn last_snapshot(&self) -> BackendQueryLifecycleMetricsSnapshot {
        *self
            .snapshots
            .lock()
            .expect("metrics snapshots")
            .last()
            .expect("published metrics snapshot")
    }
}

impl QueryLifecycleMetricsSink for RecordingMetricsSink {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        _termination_reasons: [u64; 6],
    ) {
        self.snapshots
            .lock()
            .expect("metrics snapshots")
            .push(snapshot);
    }
}

#[derive(Default)]
struct InstallGate {
    block: bool,
    entered: bool,
}

impl RecordingLocalRuntime {
    fn block_install(&self) {
        self.state.install_gate.lock().expect("install gate").block = true;
    }

    fn wait_until_install_enters(&self) {
        let mut gate = self.state.install_gate.lock().expect("install gate");
        while !gate.entered {
            gate = self
                .state
                .install_gate_changed
                .wait(gate)
                .expect("install gate wait");
        }
    }

    fn release_install(&self) {
        let mut gate = self.state.install_gate.lock().expect("install gate");
        gate.block = false;
        self.state.install_gate_changed.notify_all();
    }

    fn runtime_filter_install_calls(&self) -> usize {
        self.state
            .install_calls
            .lock()
            .expect("install calls")
            .len()
    }

    fn runtime_filter_abort_calls(&self) -> usize {
        self.state.abort_calls.lock().expect("abort calls").len()
    }

    fn fail_install(&self) {
        *self.state.fail_install.lock().expect("fail install") = true;
    }

    fn fail_abort(&self) {
        *self.state.fail_abort.lock().expect("fail abort") = true;
    }

    fn allow_abort(&self) {
        *self.state.fail_abort.lock().expect("fail abort") = false;
    }
}

impl QueryLifecycleLocalRuntime for RecordingLocalRuntime {
    fn install_runtime_filter(
        &self,
        execution_id: QueryExecutionId,
        _contribution: RuntimeFilterContribution,
    ) -> Result<(), QueryLifecycleError> {
        self.state
            .install_calls
            .lock()
            .expect("install calls")
            .push(execution_id);
        let mut gate = self.state.install_gate.lock().expect("install gate");
        gate.entered = true;
        self.state.install_gate_changed.notify_all();
        while gate.block {
            gate = self
                .state
                .install_gate_changed
                .wait(gate)
                .expect("install gate wait");
        }
        if *self.state.fail_install.lock().expect("fail install") {
            Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                "injected runtime filter install failure",
            ))
        } else {
            Ok(())
        }
    }

    fn abort_runtime_filter(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), QueryLifecycleError> {
        self.state
            .abort_calls
            .lock()
            .expect("abort calls")
            .push(execution_id);
        if *self.state.fail_abort.lock().expect("fail abort") {
            Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                "injected runtime filter abort failure",
            ))
        } else {
            Ok(())
        }
    }

    fn terminate_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        reason: QueryTerminationReason,
    ) {
        self.state.terminations.lock().expect("terminations").push((
            execution_id,
            expected_instances.to_vec(),
            reason,
        ));
    }
}

fn registry_config(max_active_entries: usize) -> QueryLifecycleRegistryConfig {
    QueryLifecycleRegistryConfig {
        max_active_entries,
        tombstone_capacity: 16_384,
        tombstone_retention: Duration::from_millis(120_000),
        heartbeat_timeout: Duration::from_millis(5_000),
        pre_start_timeout: Duration::from_millis(30_000),
    }
}

fn registry_with(
    runtime: RecordingLocalRuntime,
    max_active_entries: usize,
) -> Arc<QueryLifecycleRegistry> {
    registry_with_clock(
        runtime,
        max_active_entries,
        Arc::new(ManualClock::default()),
    )
}

fn registry_with_clock(
    runtime: RecordingLocalRuntime,
    max_active_entries: usize,
    clock: Arc<ManualClock>,
) -> Arc<QueryLifecycleRegistry> {
    QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        registry_config(max_active_entries),
        clock,
    )
}

#[test]
fn query_control_attachment_requires_backend_identity_binding() {
    let runtime = RecordingLocalRuntime::default();
    let registry = QueryLifecycleRegistry::new_unbound(
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        registry_config(8),
    );
    let request = init_request_fixture(700, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::RejectedStaleBackend
    );
    registry
        .bind_backend_identity(LOCAL_BACKEND_ID)
        .expect("first FE-assigned identity binds");
    assert_eq!(
        registry
            .bind_backend_identity(LOCAL_BACKEND_ID + 1)
            .expect_err("backend identity takeover must fail")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );
    assert_eq!(
        registry.init_query(request).outcome(),
        QueryInitOutcome::Applied
    );
}

#[test]
fn fresh_unbound_registry_reports_no_restoration_relevant_state_after_binding() {
    let registry = QueryLifecycleRegistry::new_unbound(
        LOCAL_START_EPOCH,
        Arc::new(RecordingLocalRuntime::default()),
        registry_config(8),
    );

    registry
        .bind_backend_identity(LOCAL_BACKEND_ID)
        .expect("first FE-assigned identity binds");
    let status = registry.restoration_status();

    assert_eq!(status.control_ready, 0);
    assert_eq!(status.active_lifecycle, 0);
    assert_eq!(status.fragment_admissions, 0);
    assert_eq!(status.fragment_acceptances, 0);
    assert!(!status.restored);
}

fn execution_id(query_low: i64, attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(0x514c_4302, query_low),
        AttemptId::new(attempt).expect("nonzero attempt"),
    )
    .expect("nonzero query execution id")
}

fn init_request_fixture(
    query_low: i64,
    attempt: u64,
    start_epoch: u64,
    query_deadline_unix_ms: u64,
) -> QueryInitRequest {
    let execution_id = execution_id(query_low, attempt);
    let runtime_filter = RuntimeFilterContribution::empty_for_contract_test(execution_id, 3)
        .expect("valid runtime filter contribution");
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            LOCAL_BACKEND_ID,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
            start_epoch,
        )
        .expect("valid backend identity"),
        [ParticipantRole::RuntimeFilterService],
        [],
        ParticipantQueryOptions::new(QueryOptions::default()),
        query_deadline_unix_ms,
        [],
        Some(runtime_filter),
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid participant manifest");
    QueryInitRequest::from_manifest(manifest)
}

fn fragment_init_request_fixture(query_low: i64, expected: &[UniqueId]) -> QueryInitRequest {
    let execution_id = execution_id(query_low, ATTEMPT_1);
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            LOCAL_BACKEND_ID,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
            LOCAL_START_EPOCH,
        )
        .expect("valid backend identity"),
        [ParticipantRole::FragmentExecutor],
        expected.iter().copied(),
        ParticipantQueryOptions::new(QueryOptions::default()),
        10_000,
        [],
        None,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid fragment participant manifest");
    QueryInitRequest::from_manifest(manifest)
}

fn attach_control(
    registry: &Arc<QueryLifecycleRegistry>,
    request: &QueryInitRequest,
) -> novarocks::query_execution::lifecycle::QueryControlAttachment {
    registry
        .attach_control(
            QueryControlAttach::new(request.manifest().execution_id(), request.digest(), 1)
                .expect("valid control attach"),
        )
        .expect("control attaches")
}

#[test]
fn query_lifecycle_registry_same_digest_init_is_idempotent_and_installs_once() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(1, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry.init_query(request).outcome(),
        QueryInitOutcome::AlreadyApplied
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_abort_digest_mismatch_keeps_live_entry_attachable() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(101, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let different = init_request_fixture(102, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );

    assert_eq!(
        registry
            .abort_query(
                QueryAbortRequest::new(
                    request.manifest().execution_id(),
                    different.digest(),
                    "mismatched digest must not terminate",
                )
                .expect("valid mismatched abort request"),
            )
            .expect_err("digest mismatch is rejected")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );

    registry
        .attach_control(
            QueryControlAttach::new(request.manifest().execution_id(), request.digest(), 1)
                .expect("valid control attach"),
        )
        .expect("digest mismatch must leave the live entry attachable");
}

#[test]
fn query_lifecycle_terminal_event_survives_saturated_heartbeat_queue() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(103, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);

    // ControlReady plus sixteen ACKs saturate the normal event budget while the
    // dedicated terminal permit remains reserved.
    for sequence in 1..=16 {
        attachment
            .control
            .heartbeat(sequence)
            .expect("heartbeat ACK fits the normal event budget");
    }
    attachment
        .control
        .abort("saturated event queue".to_string())
        .expect("abort is accepted despite ACK backpressure");

    let mut events = Vec::new();
    while let Ok(event) = attachment.events.try_recv() {
        events.push(event);
    }
    assert!(
        events.contains(&QueryControlEvent::TerminationAccepted {
            reason: QueryTerminationReason::CoordinatorAbort,
        }),
        "terminal acceptance must not be dropped behind heartbeat ACKs: {events:?}"
    );
}

#[test]
fn query_lifecycle_registry_different_digest_conflicts() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                2,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome(),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(init_request_fixture(
                2,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                20_000,
            ))
            .outcome(),
        QueryInitOutcome::RejectedConflict
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_registry_capacity_rejects_without_install() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 1);

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                3,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome(),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(init_request_fixture(
                4,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome(),
        QueryInitOutcome::RejectedCapacity
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_registry_backend_epoch_mismatch_rejects() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                5,
                ATTEMPT_1,
                LOCAL_START_EPOCH + 1,
                10_000,
            ))
            .outcome(),
        QueryInitOutcome::RejectedStaleBackend
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}

#[test]
fn query_lifecycle_registry_unbound_application_identity_rejects_init() {
    let runtime = RecordingLocalRuntime::default();
    let registry = QueryLifecycleRegistry::new_unbound(
        LOCAL_START_EPOCH,
        Arc::new(runtime.clone()),
        registry_config(8),
    );

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                51,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome(),
        QueryInitOutcome::RejectedStaleBackend
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}

#[test]
fn query_lifecycle_init_abort_race_never_publishes_initialized_and_rolls_back_once() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(6, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();

    let init_registry = Arc::clone(&registry);
    let init_thread = std::thread::spawn(move || init_registry.init_query(request));
    runtime.wait_until_install_enters();

    let termination = registry
        .abort_query(
            QueryAbortRequest::new(execution_id, digest, "cancel init race")
                .expect("valid abort request"),
        )
        .expect("abort is accepted");
    assert_eq!(
        termination.accepted_reason(),
        QueryTerminationReason::CoordinatorAbort
    );
    runtime.release_install();

    assert_eq!(
        init_thread.join().expect("init thread").outcome(),
        QueryInitOutcome::RejectedTerminated
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert!(!registry.was_ever_initialized(execution_id));
}

#[test]
fn query_lifecycle_initializing_to_terminating_publishes_metrics_immediately() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    let metrics = Arc::new(RecordingMetricsSink::default());
    let registry = QueryLifecycleRegistry::new_with_clock_and_metrics(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime.clone()),
        registry_config(8),
        Arc::new(ManualClock::default()),
        Arc::clone(&metrics) as Arc<dyn QueryLifecycleMetricsSink>,
    );
    let request = init_request_fixture(7, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();

    let init_registry = Arc::clone(&registry);
    let init_thread = std::thread::spawn(move || init_registry.init_query(request));
    runtime.wait_until_install_enters();
    assert_eq!(metrics.last_snapshot().initializing, 1);

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, digest, "metrics while init blocks")
                .expect("valid abort"),
        )
        .expect("abort is accepted");
    let terminating = metrics.last_snapshot();
    assert_eq!(terminating.initializing, 0);
    assert_eq!(terminating.terminating, 1);
    assert_eq!(terminating.tombstones, 0);

    runtime.release_install();
    init_thread.join().expect("init thread");
}

#[test]
fn query_lifecycle_admission_requires_control_ready_and_commits_exactly_once() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId { hi: 71, lo: 1 };
    let request = fragment_init_request_fixture(71, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );

    assert_eq!(
        registry
            .admit_fragment(execution_id, expected)
            .expect_err("fragment before ControlReady must fail")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );

    let mut attachment = attach_control(&registry, &request);
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady event"),
        novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady
    );
    registry
        .admit_fragment(execution_id, expected)
        .expect("exact fragment is admitted")
        .commit()
        .expect("fragment admission commits");
    assert_eq!(
        registry
            .admit_fragment(execution_id, expected)
            .expect_err("accepted fragment cannot be admitted twice")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );
}

#[test]
fn query_lifecycle_admission_rejects_outside_set_and_service_only_participant() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId { hi: 72, lo: 1 };
    let unexpected = UniqueId { hi: 72, lo: 2 };
    let fragment_request = fragment_init_request_fixture(72, &[expected]);
    let fragment_execution = fragment_request.manifest().execution_id();
    assert_eq!(
        registry.init_query(fragment_request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _fragment_control = attach_control(&registry, &fragment_request);
    assert_eq!(
        registry
            .admit_fragment(fragment_execution, unexpected)
            .expect_err("fragment outside exact set must fail")
            .code(),
        QueryLifecycleErrorCode::InvalidManifest
    );

    let service_request = init_request_fixture(73, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let service_execution = service_request.manifest().execution_id();
    assert_eq!(
        registry.init_query(service_request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _service_control = attach_control(&registry, &service_request);
    assert_eq!(
        registry
            .admit_fragment(service_execution, expected)
            .expect_err("service-only participant cannot admit fragments")
            .code(),
        QueryLifecycleErrorCode::InvalidManifest
    );
}

#[test]
fn query_lifecycle_admission_dropped_permit_rolls_back_in_flight() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId { hi: 74, lo: 1 };
    let request = fragment_init_request_fixture(74, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);

    drop(
        registry
            .admit_fragment(execution_id, expected)
            .expect("first permit"),
    );
    registry
        .admit_fragment(execution_id, expected)
        .expect("dropped permit releases in-flight slot")
        .commit()
        .expect("fragment admission commits");
}

#[test]
fn query_lifecycle_registry_abort_rejects_late_permit_commit() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId { hi: 75, lo: 1 };
    let request = fragment_init_request_fixture(75, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let permit = registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit");

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "abort before permit commit")
                .expect("valid abort"),
        )
        .expect("abort is accepted");

    assert_eq!(
        permit
            .commit()
            .expect_err("late permit commit must not authorize fragment start")
            .code(),
        QueryLifecycleErrorCode::Terminated
    );
    assert_eq!(
        registry
            .admit_fragment(execution_id, expected)
            .expect_err("abort must reject every later fragment request")
            .code(),
        QueryLifecycleErrorCode::Terminated
    );
}

#[test]
fn fragment_failure_emits_query_local_failure() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId { hi: 76, lo: 1 };
    let request = fragment_init_request_fixture(76, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady event"),
        QueryControlEvent::ControlReady
    );
    registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");

    registry.record_fragment_terminal(
        expected,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "pipeline worker failed",
        )),
    );

    assert_eq!(
        attachment.events.try_recv().expect("LocalFailure event"),
        QueryControlEvent::LocalFailure {
            code: "FRAGMENT_EXECUTION_FAILED".to_string(),
            detail: "fragment execution error (pipeline): pipeline worker failed".to_string(),
        }
    );
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::LocalFailure)
    );
}

#[test]
fn query_lifecycle_registry_rejects_fragment_executor_without_exact_set() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(76, &[]))
            .outcome(),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}

#[test]
fn query_lifecycle_attach_distinguishes_duplicate_active_from_terminated() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(77, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let attach =
        QueryControlAttach::new(execution_id, request.digest(), 1).expect("valid control attach");

    let Err(duplicate_error) = registry.attach_control(attach.clone()) else {
        panic!("duplicate active attach must conflict");
    };
    assert_eq!(duplicate_error.code(), QueryLifecycleErrorCode::Conflict);
    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "terminate before attach")
                .expect("valid abort"),
        )
        .expect("abort is accepted");
    let Err(terminated_error) = registry.attach_control(attach) else {
        panic!("terminated attach must be terminal");
    };
    assert_eq!(terminated_error.code(), QueryLifecycleErrorCode::Terminated);
}

#[test]
fn query_lifecycle_tombstone_capacity_evicts_only_oldest_tombstone() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.tombstone_capacity = 2;
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        config,
        Arc::new(ManualClock::default()),
    );
    let active = init_request_fixture(80, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry.init_query(active.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut terminated = Vec::new();
    for query_low in [81, 82, 83] {
        let request = init_request_fixture(query_low, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry.init_query(request.clone()).outcome(),
            QueryInitOutcome::Applied
        );
        registry
            .abort_query(
                QueryAbortRequest::new(execution_id, request.digest(), "bounded tombstone")
                    .expect("valid abort"),
            )
            .expect("abort is accepted");
        terminated.push(execution_id);
    }

    assert!(registry.contains(active.manifest().execution_id()));
    assert!(!registry.contains(terminated[0]));
    assert!(registry.contains(terminated[1]));
    assert!(registry.contains(terminated[2]));
}

#[test]
fn query_lifecycle_tombstone_releases_active_capacity() {
    let registry = registry_with(RecordingLocalRuntime::default(), 1);
    let first = init_request_fixture(84, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry.init_query(first.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                first.manifest().execution_id(),
                first.digest(),
                "release capacity",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                85,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome(),
        QueryInitOutcome::Applied
    );
}

#[test]
fn query_lifecycle_tombstone_retention_reclaims_expired_tombstone_incrementally() {
    let clock = Arc::new(ManualClock::default());
    let mut config = registry_config(8);
    config.tombstone_retention = Duration::from_millis(10);
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(RecordingLocalRuntime::default()),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
    );
    let terminated = init_request_fixture(86, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let terminated_id = terminated.manifest().execution_id();
    assert_eq!(
        registry.init_query(terminated.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(terminated_id, terminated.digest(), "retention")
                .expect("valid abort"),
        )
        .expect("abort is accepted");
    assert!(registry.contains(terminated_id));

    clock.advance(Duration::from_millis(11));
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(
                87,
                &[UniqueId { hi: 87, lo: 1 }],
            ))
            .outcome(),
        QueryInitOutcome::Applied
    );
    assert!(!registry.contains(terminated_id));
}

#[test]
fn query_lifecycle_pre_start_timeout_terminates_fragment_participant_without_accept() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 8, Arc::clone(&clock));
    let expected = UniqueId { hi: 90, lo: 1 };
    let request = fragment_init_request_fixture(90, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);

    clock.advance(Duration::from_millis(30_001));
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::PreStartTimeout)
    );
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
}

#[test]
fn query_lifecycle_pre_start_timeout_is_disarmed_by_first_accept_and_service_control_ready() {
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(RecordingLocalRuntime::default(), 8, Arc::clone(&clock));
    let expected = UniqueId { hi: 91, lo: 1 };
    let fragment_request = fragment_init_request_fixture(91, &[expected]);
    let fragment_execution = fragment_request.manifest().execution_id();
    assert_eq!(
        registry.init_query(fragment_request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let fragment_control = attach_control(&registry, &fragment_request);
    registry
        .admit_fragment(fragment_execution, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");

    let service_request = init_request_fixture(92, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let service_execution = service_request.manifest().execution_id();
    assert_eq!(
        registry.init_query(service_request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let service_control = attach_control(&registry, &service_request);

    clock.advance(Duration::from_millis(30_001));
    fragment_control
        .control
        .heartbeat(1)
        .expect("fragment control heartbeat");
    service_control
        .control
        .heartbeat(1)
        .expect("service control heartbeat");
    registry.sweep_expired(clock.now());
    assert_eq!(
        registry.phase(fragment_execution),
        Some(QueryLifecyclePhase::ControlAttached)
    );
    assert_eq!(
        registry.phase(service_execution),
        Some(QueryLifecyclePhase::ControlAttached)
    );
}

#[test]
fn query_lifecycle_heartbeat_timeout_terminates_control_attached_entry() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 8, Arc::clone(&clock));
    let expected = UniqueId { hi: 99, lo: 1 };
    let request = fragment_init_request_fixture(99, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);

    clock.advance(Duration::from_millis(5_001));
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::CoordinatorHeartbeatTimeout)
    );
    assert_eq!(registry.metrics_snapshot().heartbeat_timeouts, 1);
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
}

#[test]
fn query_lifecycle_registry_metrics_follow_state_rejection_and_termination() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId { hi: 93, lo: 1 };
    let request = fragment_init_request_fixture(93, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let initialized = registry.metrics_snapshot();
    assert_eq!(initialized.initialized, 1);
    assert_eq!(initialized.control_attached, 0);

    let _ = registry
        .admit_fragment(execution_id, expected)
        .expect_err("admission before control is rejected");
    assert_eq!(registry.metrics_snapshot().admission_rejected, 1);

    let _control = attach_control(&registry, &request);
    let attached = registry.metrics_snapshot();
    assert_eq!(attached.initialized, 0);
    assert_eq!(attached.control_attached, 1);

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "metrics termination")
                .expect("valid abort"),
        )
        .expect("abort is accepted");
    let terminated = registry.metrics_snapshot();
    assert_eq!(terminated.control_attached, 0);
    assert_eq!(terminated.tombstones, 1);
    assert_eq!(terminated.terminations, 1);
}

#[test]
fn query_lifecycle_registry_termination_is_first_wins_and_runs_local_cleanup_once() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(94, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);

    attachment
        .control
        .abort("first reason".to_string())
        .expect("first abort");
    attachment.control.finalize().expect("repeated finalize");
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady"),
        novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady
    );
    for _ in 0..2 {
        assert_eq!(
            attachment.events.try_recv().expect("termination accepted"),
            novarocks::query_execution::lifecycle::QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorAbort,
            }
        );
    }
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::CoordinatorAbort)
    );
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
}

#[test]
fn query_lifecycle_registry_same_digest_concurrent_init_is_single_flight() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(95, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    let first_registry = Arc::clone(&registry);
    let first_request = request.clone();
    let first = std::thread::spawn(move || first_registry.init_query(first_request).outcome());
    runtime.wait_until_install_enters();
    let second_registry = Arc::clone(&registry);
    let second = std::thread::spawn(move || second_registry.init_query(request).outcome());
    runtime.release_install();

    assert_eq!(first.join().expect("first init"), QueryInitOutcome::Applied);
    assert_eq!(
        second.join().expect("second init"),
        QueryInitOutcome::AlreadyApplied
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_registry_runtime_filter_install_failure_rolls_back_workspace() {
    let runtime = RecordingLocalRuntime::default();
    runtime.fail_install();
    let registry = registry_with(runtime.clone(), 1);
    let request = init_request_fixture(96, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();

    assert_eq!(
        registry.init_query(request).outcome(),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(
                97,
                &[UniqueId { hi: 97, lo: 1 }],
            ))
            .outcome(),
        QueryInitOutcome::Applied
    );
}

#[test]
fn query_lifecycle_runtime_filter_abort_failure_retains_capacity_until_sweep_retry() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 1, Arc::clone(&clock));
    let request = init_request_fixture(961, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    runtime.fail_abort();

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "abort with cleanup failure")
                .expect("valid abort"),
        )
        .expect("abort is accepted");

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Terminating)
    );
    assert_eq!(registry.metrics_snapshot().terminating, 1);
    assert_eq!(registry.metrics_snapshot().tombstones, 0);
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(
                962,
                &[UniqueId { hi: 962, lo: 1 }],
            ))
            .outcome(),
        QueryInitOutcome::RejectedCapacity
    );

    runtime.allow_abort();
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 2);
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(
                963,
                &[UniqueId { hi: 963, lo: 1 }],
            ))
            .outcome(),
        QueryInitOutcome::Applied
    );
}

#[test]
fn query_lifecycle_install_and_abort_failure_stays_terminating_until_retry() {
    let runtime = RecordingLocalRuntime::default();
    runtime.fail_install();
    runtime.fail_abort();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 1, Arc::clone(&clock));
    let request = init_request_fixture(964, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();

    assert_eq!(
        registry.init_query(request).outcome(),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Terminating)
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);

    runtime.allow_abort();
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 2);
}

#[test]
fn query_lifecycle_install_failure_racing_abort_preserves_first_reason_and_cleanup_once() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    runtime.fail_install();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(97, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest();

    let init_registry = Arc::clone(&registry);
    let init_thread = std::thread::spawn(move || init_registry.init_query(request));
    runtime.wait_until_install_enters();
    assert_eq!(
        registry
            .abort_query(
                QueryAbortRequest::new(execution_id, digest, "abort failed install")
                    .expect("valid abort"),
            )
            .expect("abort is accepted")
            .accepted_reason(),
        QueryTerminationReason::CoordinatorAbort
    );
    runtime.release_install();

    assert_eq!(
        init_thread.join().expect("init thread").outcome(),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::CoordinatorAbort)
    );
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
}

#[test]
fn query_lifecycle_registry_abort_before_init_leaves_fail_closed_tombstone() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(98, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "abort before init")
                .expect("valid abort"),
        )
        .expect("abort-before-init is accepted");

    assert_eq!(
        registry.init_query(request).outcome(),
        QueryInitOutcome::RejectedTerminated
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}
