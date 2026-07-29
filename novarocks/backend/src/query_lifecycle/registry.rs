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

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use novarocks::UniqueId;
use novarocks::novarocks_logging::{info, warn};
use novarocks::query_execution::lifecycle::metrics::BackendQueryLifecycleMetricsSnapshot;
use novarocks::query_execution::lifecycle::{
    BackendQueryControl, ParticipantManifestDigest, ParticipantRole, QueryAbortRequest,
    QueryControlAttach, QueryControlAttachment, QueryControlEvent, QueryExecutionId, QueryInitAck,
    QueryInitOutcome, QueryInitRequest, QueryLifecycleError, QueryLifecycleErrorCode,
    QueryLifecycleIngress, QueryTerminationAck, QueryTerminationReason, RuntimeFilterContribution,
};
use novarocks::runtime::fragment::FragmentOutcome;

use super::entry::{QueryLifecycleEntry, QueryLifecyclePhase};

const CONTROL_EVENT_BUFFER_CAPACITY: usize = 16;

pub(crate) trait QueryLifecycleLocalRuntime: Send + Sync + 'static {
    fn install_runtime_filter(
        &self,
        execution_id: QueryExecutionId,
        contribution: RuntimeFilterContribution,
    ) -> Result<(), QueryLifecycleError>;

    fn abort_runtime_filter(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), QueryLifecycleError>;

    fn terminate_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        reason: QueryTerminationReason,
    );
}

pub(crate) trait MonotonicClock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

pub(crate) trait QueryLifecycleMetricsSink: Send + Sync + 'static {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    );
}

struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

struct PrometheusQueryLifecycleMetricsSink;

impl QueryLifecycleMetricsSink for PrometheusQueryLifecycleMetricsSink {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    ) {
        novarocks::service::publish_backend_query_lifecycle_metrics(snapshot, termination_reasons);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct QueryLifecycleRegistryConfig {
    pub(crate) max_active_entries: usize,
    pub(crate) tombstone_capacity: usize,
    pub(crate) tombstone_retention: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) pre_start_timeout: Duration,
}

impl QueryLifecycleRegistryConfig {
    pub(crate) fn from_runtime_config(
        runtime: &novarocks::common::app_config::RuntimeConfig,
    ) -> Self {
        Self {
            max_active_entries: runtime.query_control_max_active_entries,
            tombstone_capacity: runtime.query_control_tombstone_capacity,
            tombstone_retention: Duration::from_millis(
                runtime.query_control_tombstone_retention_ms,
            ),
            heartbeat_timeout: Duration::from_millis(runtime.query_control_heartbeat_timeout_ms),
            pre_start_timeout: Duration::from_millis(runtime.query_control_pre_start_timeout_ms),
        }
    }
}

pub(crate) struct QueryLifecycleRegistry {
    state: Mutex<QueryLifecycleRegistryState>,
    local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
    config: QueryLifecycleRegistryConfig,
    local_backend_id: Mutex<Option<u64>>,
    local_start_epoch: u64,
    clock: Arc<dyn MonotonicClock>,
    metrics: Arc<dyn QueryLifecycleMetricsSink>,
    self_weak: Weak<QueryLifecycleRegistry>,
}

struct QueryLifecycleRegistryState {
    entries: BTreeMap<QueryExecutionId, Arc<QueryLifecycleEntry>>,
    fragment_executions: BTreeMap<UniqueId, QueryExecutionId>,
    tombstones: VecDeque<QueryExecutionId>,
    active_entries: usize,
    init_conflicts: u64,
    admission_rejected: u64,
    heartbeat_timeouts: u64,
    terminations: u64,
    termination_reasons: [u64; 6],
    pre_init_tombstones: BTreeMap<QueryExecutionId, PreInitTombstone>,
}

struct PreInitTombstone {
    digest: ParticipantManifestDigest,
    reason: QueryTerminationReason,
    terminated_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryLifecycleRestorationStatus {
    pub(crate) control_ready: usize,
    pub(crate) active_lifecycle: usize,
    pub(crate) fragment_admissions: usize,
    pub(crate) fragment_acceptances: usize,
    pub(crate) lifecycle_entries: usize,
    pub(crate) lifecycle_tombstones: usize,
    pub(crate) pre_init_tombstones: usize,
    pub(crate) tombstone_index: usize,
    pub(crate) restored: bool,
}

impl Default for QueryLifecycleRegistryState {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            fragment_executions: BTreeMap::new(),
            tombstones: VecDeque::new(),
            active_entries: 0,
            init_conflicts: 0,
            admission_rejected: 0,
            heartbeat_timeouts: 0,
            terminations: 0,
            termination_reasons: [0; 6],
            pre_init_tombstones: BTreeMap::new(),
        }
    }
}

struct InitWorkspace {
    registry: Arc<QueryLifecycleRegistry>,
    entry: Arc<QueryLifecycleEntry>,
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
}

pub(crate) struct FragmentAdmissionPermit {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
    entry: Arc<QueryLifecycleEntry>,
    committed: bool,
}

impl fmt::Debug for FragmentAdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FragmentAdmissionPermit")
            .field("execution_id", &self.execution_id)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("committed", &self.committed)
            .finish()
    }
}

struct RegistryQueryControl {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
}

impl QueryLifecycleRegistry {
    #[allow(dead_code)]
    pub(crate) fn new(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
    ) -> Arc<Self> {
        Self::new_with_clock(
            local_backend_id,
            local_start_epoch,
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
        )
    }

    pub(crate) fn new_with_clock(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Arc<Self> {
        Self::new_with_clock_and_metrics(
            local_backend_id,
            local_start_epoch,
            local_runtime,
            config,
            clock,
            Arc::new(PrometheusQueryLifecycleMetricsSink),
        )
    }

    pub(crate) fn new_with_clock_and_metrics(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
    ) -> Arc<Self> {
        Self::new_with_backend_identity(
            Some(local_backend_id),
            local_start_epoch,
            local_runtime,
            config,
            clock,
            metrics,
        )
    }

    pub(crate) fn new_unbound(
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
    ) -> Arc<Self> {
        Self::new_with_backend_identity(
            None,
            local_start_epoch,
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
            Arc::new(PrometheusQueryLifecycleMetricsSink),
        )
    }

    fn new_with_backend_identity(
        local_backend_id: Option<u64>,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
    ) -> Arc<Self> {
        assert!(config.max_active_entries > 0);
        assert!(config.tombstone_capacity > 0);
        assert!(!config.tombstone_retention.is_zero());
        assert!(!config.heartbeat_timeout.is_zero());
        assert!(!config.pre_start_timeout.is_zero());
        Arc::new_cyclic(|self_weak| Self {
            state: Mutex::new(QueryLifecycleRegistryState::default()),
            local_runtime,
            config,
            local_backend_id: Mutex::new(local_backend_id),
            local_start_epoch,
            clock,
            metrics,
            self_weak: self_weak.clone(),
        })
    }

    fn local_backend_id(&self) -> Option<u64> {
        *self
            .local_backend_id
            .lock()
            .expect("query lifecycle backend identity lock")
    }

    pub(crate) fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError> {
        let mut local_backend_id = self
            .local_backend_id
            .lock()
            .expect("query lifecycle backend identity lock");
        match *local_backend_id {
            None => {
                *local_backend_id = Some(backend_id);
                drop(local_backend_id);
                let status = self.restoration_status();
                if query_lifecycle_test_markers_enabled() {
                    eprintln!(
                        "NOVAROCKS_QUERY_LIFECYCLE_RESTORE_STATUS backend_id={} start_epoch={} control_ready={} active_lifecycle={} fragment_admissions={} fragment_acceptances={} lifecycle_entries={} lifecycle_tombstones={} pre_init_tombstones={} tombstone_index={} restored={}",
                        backend_id,
                        self.local_start_epoch,
                        status.control_ready,
                        status.active_lifecycle,
                        status.fragment_admissions,
                        status.fragment_acceptances,
                        status.lifecycle_entries,
                        status.lifecycle_tombstones,
                        status.pre_init_tombstones,
                        status.tombstone_index,
                        status.restored
                    );
                }
                Ok(())
            }
            Some(current) if current == backend_id => Ok(()),
            Some(current) => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                format!(
                    "backend identity is already bound to {current}; refusing reassignment to {backend_id}"
                ),
            )),
        }
    }

    pub(crate) fn restoration_status(&self) -> QueryLifecycleRestorationStatus {
        let state = self.state.lock().expect("query lifecycle registry lock");
        let mut control_ready = 0;
        let mut fragment_admissions = 0;
        let mut fragment_acceptances = 0;
        let mut lifecycle_tombstones = 0;
        for entry in state.entries.values() {
            let entry_state = entry.state.lock().expect("query lifecycle entry lock");
            control_ready += usize::from(entry_state.phase == QueryLifecyclePhase::ControlAttached);
            fragment_admissions += entry_state.in_flight_fragments.len();
            fragment_acceptances += entry_state.accepted_fragments.len();
            lifecycle_tombstones +=
                usize::from(entry_state.phase == QueryLifecyclePhase::Tombstone);
        }
        fragment_acceptances = fragment_acceptances.max(state.fragment_executions.len());
        let active_lifecycle = state.active_entries;
        let lifecycle_entries = state.entries.len();
        let pre_init_tombstones = state.pre_init_tombstones.len();
        let tombstone_index = state.tombstones.len();
        let restored = control_ready != 0
            || active_lifecycle != 0
            || fragment_admissions != 0
            || fragment_acceptances != 0
            || lifecycle_entries != 0
            || lifecycle_tombstones != 0
            || pre_init_tombstones != 0
            || tombstone_index != 0;
        QueryLifecycleRestorationStatus {
            control_ready,
            active_lifecycle,
            fragment_admissions,
            fragment_acceptances,
            lifecycle_entries,
            lifecycle_tombstones,
            pre_init_tombstones,
            tombstone_index,
            restored,
        }
    }

    pub(crate) fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        let execution_id = request.manifest().execution_id();
        let digest = request.digest();
        if request
            .manifest()
            .roles()
            .contains(&ParticipantRole::FragmentExecutor)
            && request
                .manifest()
                .expected_fragment_instance_ids()
                .is_empty()
        {
            let ack = QueryInitAck::new(
                execution_id,
                digest,
                QueryInitOutcome::RejectedInvalidManifest,
            );
            self.log_init(&ack);
            return ack;
        }
        if self.local_backend_id() != Some(request.manifest().backend().backend_id())
            || request.manifest().backend().start_epoch() != self.local_start_epoch
        {
            let ack =
                QueryInitAck::new(execution_id, digest, QueryInitOutcome::RejectedStaleBackend);
            self.log_init(&ack);
            return ack;
        }

        let entry = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
            if let Some(tombstone) = state.pre_init_tombstones.get(&execution_id) {
                let outcome = if tombstone.digest == digest {
                    QueryInitOutcome::RejectedTerminated
                } else {
                    state.init_conflicts = state.init_conflicts.saturating_add(1);
                    QueryInitOutcome::RejectedConflict
                };
                let ack = QueryInitAck::new(execution_id, digest, outcome);
                drop(state);
                self.log_init(&ack);
                self.publish_metrics();
                return ack;
            }
            if let Some(entry) = state.entries.get(&execution_id).cloned() {
                if entry.digest != digest {
                    state.init_conflicts = state.init_conflicts.saturating_add(1);
                    let ack =
                        QueryInitAck::new(execution_id, digest, QueryInitOutcome::RejectedConflict);
                    drop(state);
                    self.log_init(&ack);
                    self.publish_metrics();
                    return ack;
                }
                drop(state);
                let ack = self.wait_for_existing_init(entry, execution_id, digest);
                self.log_init(&ack);
                return ack;
            }
            if state.active_entries >= self.config.max_active_entries {
                let ack =
                    QueryInitAck::new(execution_id, digest, QueryInitOutcome::RejectedCapacity);
                drop(state);
                self.log_init(&ack);
                return ack;
            }
            let entry = Arc::new(QueryLifecycleEntry::initializing(
                request.manifest().clone(),
                digest,
            ));
            state.entries.insert(execution_id, Arc::clone(&entry));
            state.active_entries += 1;
            entry
        };
        self.publish_metrics();
        let ack = InitWorkspace {
            registry: self
                .self_weak
                .upgrade()
                .expect("query lifecycle registry is alive during method call"),
            entry,
            execution_id,
            digest,
        }
        .install_and_publish();
        self.log_init(&ack);
        self.publish_metrics();
        ack
    }

    fn wait_for_existing_init(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
    ) -> QueryInitAck {
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        while state.phase == QueryLifecyclePhase::Initializing && state.init_outcome.is_none() {
            state = entry
                .init_completed
                .wait(state)
                .expect("query lifecycle init wait");
        }
        let outcome = match (state.phase, state.init_outcome) {
            (_, Some(outcome)) if outcome != QueryInitOutcome::Applied => outcome,
            (QueryLifecyclePhase::Initialized | QueryLifecyclePhase::ControlAttached, _) => {
                QueryInitOutcome::AlreadyApplied
            }
            (QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone, _) => {
                QueryInitOutcome::RejectedTerminated
            }
            (QueryLifecyclePhase::Initializing, _) => state
                .init_outcome
                .unwrap_or(QueryInitOutcome::RejectedInvalidManifest),
        };
        QueryInitAck::new(execution_id, digest, outcome)
    }

    pub(crate) fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        let execution_id = request.execution_id();
        let entry = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
            if let Some(entry) = state.entries.get(&execution_id).cloned() {
                Some(entry)
            } else {
                let reason = state
                    .pre_init_tombstones
                    .get(&execution_id)
                    .map(|tombstone| tombstone.reason)
                    .unwrap_or(QueryTerminationReason::CoordinatorAbort);
                if !state.pre_init_tombstones.contains_key(&execution_id) {
                    state.pre_init_tombstones.insert(
                        execution_id,
                        PreInitTombstone {
                            digest: request.digest(),
                            reason,
                            terminated_at: self.clock.now(),
                        },
                    );
                    state.tombstones.push_back(execution_id);
                    state.terminations = state.terminations.saturating_add(1);
                    state.termination_reasons[termination_reason_index(reason)] = state
                        .termination_reasons[termination_reason_index(reason)]
                    .saturating_add(1);
                    self.enforce_tombstone_capacity_locked(&mut state);
                }
                drop(state);
                info!(
                    target: "novarocks::query_lifecycle",
                    query_id = ?execution_id.query_id(),
                    attempt_id = execution_id.attempt_id().get(),
                    backend_id = ?self.local_backend_id(),
                    start_epoch = self.local_start_epoch,
                    digest = %format_digest(request.digest()),
                    outcome = "terminated",
                    reason = ?reason,
                    "backend query lifecycle terminated before init"
                );
                self.publish_metrics();
                return Ok(QueryTerminationAck::new(execution_id, reason));
            }
        };
        let entry = entry.expect("existing entry");
        if entry.digest != request.digest() {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "abort digest conflicts with initialized manifest",
            ));
        }
        let reason = self.request_termination(entry, QueryTerminationReason::CoordinatorAbort);
        Ok(QueryTerminationAck::new(execution_id, reason))
    }

    pub(crate) fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&attach.execution_id())
            .cloned();
        let Some(entry) = entry else {
            return Err(self.attach_error(
                &attach,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle entry is not active",
                "missing",
            ));
        };
        if entry.digest != attach.digest() {
            return Err(self.attach_error(
                &attach,
                QueryLifecycleErrorCode::Conflict,
                "query control digest conflicts with initialized manifest",
                "digest_mismatch",
            ));
        }
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(CONTROL_EVENT_BUFFER_CAPACITY + 2);
        events_tx
            .try_send(QueryControlEvent::ControlReady)
            .map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    format!("publish ControlReady failed: {error}"),
                )
            })?;
        let terminal_event_permit = events_tx.clone().try_reserve_owned().map_err(|error| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                format!("reserve terminal control event failed: {error}"),
            )
        })?;
        {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            match state.phase {
                QueryLifecyclePhase::Initialized => {}
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone => {
                    let phase = phase_name(state.phase);
                    drop(state);
                    return Err(self.attach_error(
                        &attach,
                        QueryLifecycleErrorCode::Terminated,
                        "query lifecycle entry has terminated",
                        phase,
                    ));
                }
                QueryLifecyclePhase::Initializing | QueryLifecyclePhase::ControlAttached => {
                    let phase = phase_name(state.phase);
                    drop(state);
                    return Err(self.attach_error(
                        &attach,
                        QueryLifecycleErrorCode::Conflict,
                        "query control can attach only to an initialized entry",
                        phase,
                    ));
                }
            }
            state.phase = QueryLifecyclePhase::ControlAttached;
            state.frontend_owner_epoch = Some(attach.frontend_owner_epoch());
            state.last_heartbeat = Some(self.clock.now());
            state.events = Some(events_tx.clone());
            state.terminal_event_permit = Some(terminal_event_permit);
            if !entry
                .manifest
                .roles()
                .contains(&ParticipantRole::FragmentExecutor)
            {
                state.pre_start_deadline = None;
            }
        }
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?attach.execution_id().query_id(),
            attempt_id = attach.execution_id().attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(attach.digest()),
            outcome = "control_attached",
            reason = "none",
            "backend query lifecycle control attached"
        );
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_CONTROL_READY execution_id={} backend_id={} expected_fragments={}",
                format_execution_id(attach.execution_id()),
                self.local_backend_id().unwrap_or_default(),
                entry.manifest.expected_fragment_instance_ids().len()
            );
        }
        self.publish_metrics();
        Ok(QueryControlAttachment {
            control: Arc::new(RegistryQueryControl {
                registry: self.self_weak.clone(),
                execution_id: attach.execution_id(),
            }),
            events: events_rx,
        })
    }

    fn attach_error(
        &self,
        attach: &QueryControlAttach,
        code: QueryLifecycleErrorCode,
        detail: &'static str,
        phase: &'static str,
    ) -> QueryLifecycleError {
        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?attach.execution_id().query_id(),
            attempt_id = attach.execution_id().attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(attach.digest()),
            outcome = "attach_rejected",
            reason = detail,
            phase,
            "backend query lifecycle control attach rejected"
        );
        QueryLifecycleError::new(code, detail)
    }

    pub(crate) fn admit_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Result<FragmentAdmissionPermit, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query is not active",
            ));
        };
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            )
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle has terminated",
            ));
        }
        if state.phase != QueryLifecyclePhase::ControlAttached {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "query control is not ready",
            ));
        }
        if !entry
            .manifest
            .roles()
            .contains(&ParticipantRole::FragmentExecutor)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "service-only participant cannot admit fragments",
            ));
        }
        if !entry
            .manifest
            .expected_fragment_instance_ids()
            .contains(&fragment_instance_id)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "fragment instance is outside the participant manifest",
            ));
        }
        if state.accepted_fragments.contains(&fragment_instance_id)
            || !state.in_flight_fragments.insert(fragment_instance_id)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "fragment instance was already admitted",
            ));
        }
        drop(state);
        Ok(FragmentAdmissionPermit {
            registry: self.self_weak.clone(),
            execution_id,
            fragment_instance_id,
            entry,
            committed: false,
        })
    }

    fn admission_error(
        &self,
        execution_id: QueryExecutionId,
        code: QueryLifecycleErrorCode,
        detail: &'static str,
    ) -> QueryLifecycleError {
        let digest = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            state.admission_rejected = state.admission_rejected.saturating_add(1);
            state
                .entries
                .get(&execution_id)
                .map(|entry| format_digest(entry.digest))
                .unwrap_or_else(|| "unknown".to_string())
        };
        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            attempt_id = execution_id.attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %digest,
            outcome = "admission_rejected",
            reason = detail,
            "backend query lifecycle fragment admission rejected"
        );
        self.publish_metrics();
        QueryLifecycleError::new(code, detail)
    }

    pub(crate) fn sweep_expired(&self, now: Instant) {
        let entries = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, now, 64);
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        for entry in entries {
            let (termination_retry, expiration) = {
                let state = entry.state.lock().expect("query lifecycle entry lock");
                if state.phase == QueryLifecyclePhase::Terminating {
                    (state.init_outcome.and(state.termination_reason), None)
                } else if state.phase == QueryLifecyclePhase::Tombstone {
                    (None, None)
                } else if state
                    .pre_start_deadline
                    .is_some_and(|deadline| now >= deadline)
                {
                    (None, Some(QueryTerminationReason::PreStartTimeout))
                } else if state.phase == QueryLifecyclePhase::ControlAttached
                    && state.last_heartbeat.is_some_and(|heartbeat| {
                        now.saturating_duration_since(heartbeat) >= self.config.heartbeat_timeout
                    })
                {
                    (
                        None,
                        Some(QueryTerminationReason::CoordinatorHeartbeatTimeout),
                    )
                } else {
                    (None, None)
                }
            };
            if let Some(reason) = termination_retry {
                let execution_id = entry.manifest.execution_id();
                if self.try_complete_runtime_filter_cleanup(&entry, execution_id) {
                    self.publish_tombstone(&entry, execution_id, reason);
                }
                continue;
            }
            if let Some(reason) = expiration {
                self.request_termination(entry, reason);
            }
        }
    }

    fn request_termination(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
    ) -> QueryTerminationReason {
        self.request_termination_with_event(entry, requested_reason, None)
    }

    fn request_termination_with_event(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
        terminal_event: Option<QueryControlEvent>,
    ) -> QueryTerminationReason {
        let (execution_id, expected_instances, initializing, terminal_event_permit) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if let Some(reason) = state.termination_reason {
                return reason;
            }
            state.termination_reason = Some(requested_reason);
            let initializing = state.phase == QueryLifecyclePhase::Initializing;
            state.phase = QueryLifecyclePhase::Terminating;
            if state.runtime_filter_installed {
                state.runtime_filter_cleanup_required = true;
            }
            (
                entry.manifest.execution_id(),
                entry
                    .manifest
                    .expected_fragment_instance_ids()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>(),
                initializing,
                state.terminal_event_permit.take(),
            )
        };

        if let Some(permit) = terminal_event_permit {
            drop(permit.send(
                terminal_event.unwrap_or(QueryControlEvent::TerminationAccepted {
                    reason: requested_reason,
                }),
            ));
        }
        self.publish_metrics();
        self.local_runtime
            .terminate_query(execution_id, &expected_instances, requested_reason);
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id={} backend_id={} reason={requested_reason:?} expected_fragments={}",
                format_execution_id(execution_id),
                self.local_backend_id().unwrap_or_default(),
                expected_instances.len()
            );
        }
        if requested_reason == QueryTerminationReason::CoordinatorHeartbeatTimeout {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            state.heartbeat_timeouts = state.heartbeat_timeouts.saturating_add(1);
        }
        let cleanup_complete = self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        if !initializing && cleanup_complete {
            self.publish_tombstone(&entry, execution_id, requested_reason);
        }
        requested_reason
    }

    pub(crate) fn record_fragment_terminal(
        &self,
        fragment_instance_id: UniqueId,
        outcome: &FragmentOutcome,
    ) {
        let execution_id = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .fragment_executions
            .remove(&fragment_instance_id);
        if matches!(outcome, FragmentOutcome::Succeeded) {
            return;
        }
        let Some(execution_id) = execution_id else {
            warn!(
                target: "novarocks::query_lifecycle",
                finst_id = %fragment_instance_id,
                "fragment terminal fact has no committed query lifecycle admission"
            );
            return;
        };
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return;
        };
        let (code, detail) = match outcome {
            FragmentOutcome::Failed(error) => {
                ("FRAGMENT_EXECUTION_FAILED".to_string(), error.to_string())
            }
            FragmentOutcome::Cancelled { reason } => (
                "FRAGMENT_CANCELLED".to_string(),
                reason.detail().to_string(),
            ),
            FragmentOutcome::Succeeded => return,
        };
        self.request_termination_with_event(
            entry,
            QueryTerminationReason::LocalFailure,
            Some(QueryControlEvent::LocalFailure { code, detail }),
        );
    }

    fn try_complete_runtime_filter_cleanup(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
    ) -> bool {
        {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if !state.runtime_filter_cleanup_required {
                return true;
            }
            if state.runtime_filter_cleanup_in_flight {
                return false;
            }
            state.runtime_filter_cleanup_in_flight = true;
        }

        let cleanup_result = self.local_runtime.abort_runtime_filter(execution_id);
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        state.runtime_filter_cleanup_in_flight = false;
        if cleanup_result.is_ok() {
            state.runtime_filter_cleanup_required = false;
            state.runtime_filter_installed = false;
            return true;
        }
        drop(state);

        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            attempt_id = execution_id.attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(entry.digest),
            outcome = "runtime_filter_cleanup_failed",
            reason = "local runtime rejected runtime-filter cleanup",
            error = %cleanup_result.expect_err("cleanup result was checked"),
            "backend query lifecycle runtime-filter cleanup will be retried"
        );
        self.publish_metrics();
        false
    }

    fn publish_tombstone(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        reason: QueryTerminationReason,
    ) {
        {
            let mut entry_state = entry.state.lock().expect("query lifecycle entry lock");
            if entry_state.phase == QueryLifecyclePhase::Tombstone {
                return;
            }
            if entry_state.runtime_filter_cleanup_required
                || entry_state.runtime_filter_cleanup_in_flight
            {
                return;
            }
            entry_state.phase = QueryLifecyclePhase::Tombstone;
            entry_state.termination_reason.get_or_insert(reason);
            entry_state.terminated_at = Some(self.clock.now());
            entry_state
                .init_outcome
                .get_or_insert(QueryInitOutcome::RejectedTerminated);
            entry.init_completed.notify_all();
        }
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        state.active_entries = state.active_entries.saturating_sub(1);
        state.tombstones.push_back(execution_id);
        state.terminations = state.terminations.saturating_add(1);
        state.termination_reasons[termination_reason_index(reason)] =
            state.termination_reasons[termination_reason_index(reason)].saturating_add(1);
        self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
        self.enforce_tombstone_capacity_locked(&mut state);
        drop(state);
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            attempt_id = execution_id.attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(entry.digest),
            outcome = "terminated",
            reason = ?reason,
            "backend query lifecycle terminated"
        );
        self.publish_metrics();
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_LIFECYCLE_CLEANUP execution_id={} backend_id={} active=false tombstone=true reason={reason:?}",
                format_execution_id(execution_id),
                self.local_backend_id().unwrap_or_default()
            );
        }
    }

    fn clean_tombstones_locked(
        &self,
        state: &mut QueryLifecycleRegistryState,
        now: Instant,
        limit: usize,
    ) {
        let mut removed = 0;
        while removed < limit {
            let Some(execution_id) = state.tombstones.front().copied() else {
                break;
            };
            let terminated_at = state
                .pre_init_tombstones
                .get(&execution_id)
                .map(|tombstone| tombstone.terminated_at)
                .or_else(|| {
                    state.entries.get(&execution_id).and_then(|entry| {
                        entry
                            .state
                            .lock()
                            .expect("query lifecycle entry lock")
                            .terminated_at
                    })
                });
            if !terminated_at.is_some_and(|at| {
                now.saturating_duration_since(at) >= self.config.tombstone_retention
            }) {
                break;
            }
            state.tombstones.pop_front();
            state.pre_init_tombstones.remove(&execution_id);
            if state.entries.get(&execution_id).is_some_and(|entry| {
                entry
                    .state
                    .lock()
                    .expect("query lifecycle entry lock")
                    .phase
                    == QueryLifecyclePhase::Tombstone
            }) {
                state.entries.remove(&execution_id);
            }
            removed += 1;
        }
    }

    fn enforce_tombstone_capacity_locked(&self, state: &mut QueryLifecycleRegistryState) {
        while state.tombstones.len() > self.config.tombstone_capacity {
            let execution_id = state
                .tombstones
                .pop_front()
                .expect("tombstone length checked");
            state.pre_init_tombstones.remove(&execution_id);
            if state.entries.get(&execution_id).is_some_and(|entry| {
                entry
                    .state
                    .lock()
                    .expect("query lifecycle entry lock")
                    .phase
                    == QueryLifecyclePhase::Tombstone
            }) {
                state.entries.remove(&execution_id);
            }
        }
    }

    fn heartbeat(
        &self,
        execution_id: QueryExecutionId,
        sequence: u64,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let events = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.phase != QueryLifecyclePhase::ControlAttached
                || state.termination_reason.is_some()
            {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query control is not active",
                ));
            }
            state.last_heartbeat = Some(self.clock.now());
            state.events.clone()
        };
        if let Some(events) = events {
            events
                .try_send(QueryControlEvent::HeartbeatAck { sequence })
                .map_err(|error| {
                    QueryLifecycleError::new(
                        QueryLifecycleErrorCode::Internal,
                        format!("publish heartbeat ack failed: {error}"),
                    )
                })?;
        }
        Ok(())
    }

    fn terminate_from_control(
        &self,
        execution_id: QueryExecutionId,
        reason: QueryTerminationReason,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let repeated = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason
            .is_some();
        if matches!(
            reason,
            QueryTerminationReason::CoordinatorStreamLost
                | QueryTerminationReason::CoordinatorHeartbeatTimeout
        ) {
            warn!(
                target: "novarocks::query_lifecycle",
                query_id = ?execution_id.query_id(),
                attempt_id = execution_id.attempt_id().get(),
                backend_id = ?self.local_backend_id(),
                start_epoch = self.local_start_epoch,
                digest = %format_digest(entry.digest),
                outcome = "coordinator_lost",
                reason = ?reason,
                "backend query lifecycle coordinator lost"
            );
        }
        let accepted = self.request_termination(Arc::clone(&entry), reason);
        if repeated {
            let events = entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .events
                .clone();
            if let Some(events) = events {
                let _ =
                    events.try_send(QueryControlEvent::TerminationAccepted { reason: accepted });
            }
        }
        Ok(())
    }

    fn active_entry(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<Arc<QueryLifecycleEntry>, QueryLifecycleError> {
        self.state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle entry is not active",
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn phase(&self, execution_id: QueryExecutionId) -> Option<QueryLifecyclePhase> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        let phase = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .phase;
        Some(phase)
    }

    #[cfg(test)]
    pub(crate) fn was_ever_initialized(&self, execution_id: QueryExecutionId) -> bool {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        entry.is_some_and(|entry| {
            entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .ever_initialized
        })
    }

    #[cfg(test)]
    pub(crate) fn termination_reason(
        &self,
        execution_id: QueryExecutionId,
    ) -> Option<QueryTerminationReason> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        let reason = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason;
        reason
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, execution_id: QueryExecutionId) -> bool {
        let state = self.state.lock().expect("query lifecycle registry lock");
        state.entries.contains_key(&execution_id)
            || state.pre_init_tombstones.contains_key(&execution_id)
    }

    pub(crate) fn metrics_snapshot(&self) -> BackendQueryLifecycleMetricsSnapshot {
        let state = self.state.lock().expect("query lifecycle registry lock");
        fold_metrics_locked(&state).0
    }

    fn publish_metrics(&self) {
        let (snapshot, termination_reasons) = {
            let state = self.state.lock().expect("query lifecycle registry lock");
            fold_metrics_locked(&state)
        };
        self.metrics.publish(snapshot, termination_reasons);
    }

    fn log_init(&self, ack: &QueryInitAck) {
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?ack.execution_id().query_id(),
            attempt_id = ack.execution_id().attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(ack.digest()),
            outcome = ?ack.outcome(),
            reason = "none",
            "backend query lifecycle init"
        );
        if query_lifecycle_test_markers_enabled()
            && matches!(
                ack.outcome(),
                QueryInitOutcome::Applied | QueryInitOutcome::AlreadyApplied
            )
        {
            let expected_fragments = self
                .state
                .lock()
                .expect("query lifecycle registry lock")
                .entries
                .get(&ack.execution_id())
                .map(|entry| entry.manifest.expected_fragment_instance_ids().len())
                .unwrap_or_default();
            let marker = if ack.outcome() == QueryInitOutcome::Applied {
                "NOVAROCKS_QUERY_INIT_APPLIED"
            } else {
                "NOVAROCKS_QUERY_INIT_IDEMPOTENT"
            };
            eprintln!(
                "{marker} execution_id={} backend_id={} expected_fragments={expected_fragments}",
                format_execution_id(ack.execution_id()),
                self.local_backend_id().unwrap_or_default()
            );
        }
    }
}

impl InitWorkspace {
    fn install_and_publish(self) -> QueryInitAck {
        let contribution = self.entry.manifest.runtime_filter().cloned();
        let has_runtime_filter = contribution.is_some();
        let install_result = contribution.map_or(Ok(()), |contribution| {
            self.registry
                .local_runtime
                .install_runtime_filter(self.execution_id, contribution)
        });
        if install_result.is_err() {
            let (reason, terminate_locally) = {
                let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
                state.init_outcome = Some(QueryInitOutcome::RejectedInvalidManifest);
                let terminate_locally = state.termination_reason.is_none();
                let reason = *state
                    .termination_reason
                    .get_or_insert(QueryTerminationReason::LocalFailure);
                state.phase = QueryLifecyclePhase::Terminating;
                state.runtime_filter_cleanup_required = has_runtime_filter;
                self.entry.init_completed.notify_all();
                (reason, terminate_locally)
            };
            if terminate_locally {
                let expected_instances = self
                    .entry
                    .manifest
                    .expected_fragment_instance_ids()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                self.registry.local_runtime.terminate_query(
                    self.execution_id,
                    &expected_instances,
                    reason,
                );
            }
            if self
                .registry
                .try_complete_runtime_filter_cleanup(&self.entry, self.execution_id)
            {
                self.registry
                    .publish_tombstone(&self.entry, self.execution_id, reason);
            }
            return QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::RejectedInvalidManifest,
            );
        }

        let terminated = {
            let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
            state.runtime_filter_installed = contribution_is_present(&self.entry);
            if state.termination_reason.is_some() {
                if state.runtime_filter_installed {
                    state.runtime_filter_cleanup_required = true;
                }
                state.init_outcome = Some(QueryInitOutcome::RejectedTerminated);
                self.entry.init_completed.notify_all();
                true
            } else {
                state.phase = QueryLifecyclePhase::Initialized;
                state.ever_initialized = true;
                state.init_outcome = Some(QueryInitOutcome::Applied);
                state.pre_start_deadline =
                    Some(self.registry.clock.now() + self.registry.config.pre_start_timeout);
                self.entry.init_completed.notify_all();
                false
            }
        };
        if terminated {
            let reason = self
                .entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .termination_reason
                .expect("termination was observed");
            if self
                .registry
                .try_complete_runtime_filter_cleanup(&self.entry, self.execution_id)
            {
                self.registry
                    .publish_tombstone(&self.entry, self.execution_id, reason);
            }
            QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::RejectedTerminated,
            )
        } else {
            QueryInitAck::new(self.execution_id, self.digest, QueryInitOutcome::Applied)
        }
    }
}

impl FragmentAdmissionPermit {
    pub(crate) fn commit(mut self) -> Result<(), QueryLifecycleError> {
        let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            )
        {
            let reason = state.termination_reason;
            let expected_instances = self
                .entry
                .manifest
                .expected_fragment_instance_ids()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            drop(state);
            if let (Some(registry), Some(reason)) = (self.registry.upgrade(), reason) {
                // Termination may have raced ahead of the service registration/control
                // publication protected by this permit. Re-drive local termination after
                // those resources exist so the rejected admission cannot leave a live worker.
                registry.local_runtime.terminate_query(
                    self.execution_id,
                    &expected_instances,
                    reason,
                );
            }
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle terminated before fragment admission commit",
            ));
        }
        if state.phase != QueryLifecyclePhase::ControlAttached {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "query control is not ready for fragment admission commit",
            ));
        }
        if !state.in_flight_fragments.remove(&self.fragment_instance_id) {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "fragment admission permit is no longer in flight",
            ));
        }
        state.accepted_fragments.insert(self.fragment_instance_id);
        state.pre_start_deadline = None;
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?;
        let mut registry_state = registry
            .state
            .lock()
            .expect("query lifecycle registry lock");
        let previous = registry_state
            .fragment_executions
            .insert(self.fragment_instance_id, self.execution_id);
        if let Some(previous_execution_id) = previous {
            registry_state
                .fragment_executions
                .insert(self.fragment_instance_id, previous_execution_id);
            drop(registry_state);
            state.accepted_fragments.remove(&self.fragment_instance_id);
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "fragment instance already belongs to a committed query lifecycle admission",
            ));
        }
        drop(registry_state);
        drop(state);
        self.committed = true;
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={} backend_id={} finst_id={}",
                format_execution_id(self.execution_id),
                self.registry
                    .upgrade()
                    .and_then(|registry| registry.local_backend_id())
                    .unwrap_or_default(),
                self.fragment_instance_id
            );
        }
        Ok(())
    }
}

impl Drop for FragmentAdmissionPermit {
    fn drop(&mut self) {
        if !self.committed {
            self.entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .in_flight_fragments
                .remove(&self.fragment_instance_id);
        }
    }
}

impl BackendQueryControl for RegistryQueryControl {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .heartbeat(self.execution_id, sequence)
    }

    fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(self.execution_id, QueryTerminationReason::CoordinatorAbort)
    }

    fn finalize(&self) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(
                self.execution_id,
                QueryTerminationReason::CoordinatorFinalize,
            )
    }

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        if query_lifecycle_test_markers_enabled() {
            let backend_id = self
                .registry
                .upgrade()
                .and_then(|registry| registry.local_backend_id())
                .unwrap_or_default();
            eprintln!(
                "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST execution_id={} backend_id={} reason={reason:?}",
                format_execution_id(self.execution_id),
                backend_id
            );
        }
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(self.execution_id, reason)
    }
}

fn format_execution_id(execution_id: QueryExecutionId) -> String {
    format!(
        "{}:{}:{}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get()
    )
}

#[cfg(debug_assertions)]
pub(super) fn query_lifecycle_test_markers_enabled() -> bool {
    novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
        .is_some()
}

#[cfg(not(debug_assertions))]
pub(super) fn query_lifecycle_test_markers_enabled() -> bool {
    false
}

impl QueryLifecycleIngress for QueryLifecycleRegistry {
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError> {
        QueryLifecycleRegistry::bind_backend_identity(self, backend_id)
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        QueryLifecycleRegistry::init_query(self, request)
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        QueryLifecycleRegistry::abort_query(self, request)
    }

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        QueryLifecycleRegistry::attach_control(self, attach)
    }
}

fn fold_metrics_locked(
    state: &QueryLifecycleRegistryState,
) -> (BackendQueryLifecycleMetricsSnapshot, [u64; 6]) {
    let mut snapshot = BackendQueryLifecycleMetricsSnapshot {
        tombstones: state.tombstones.len(),
        admission_rejected: state.admission_rejected,
        init_conflicts: state.init_conflicts,
        heartbeat_timeouts: state.heartbeat_timeouts,
        terminations: state.terminations,
        ..BackendQueryLifecycleMetricsSnapshot::default()
    };
    for entry in state.entries.values() {
        match entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .phase
        {
            QueryLifecyclePhase::Initializing => snapshot.initializing += 1,
            QueryLifecyclePhase::Initialized => snapshot.initialized += 1,
            QueryLifecyclePhase::ControlAttached => snapshot.control_attached += 1,
            QueryLifecyclePhase::Terminating => snapshot.terminating += 1,
            QueryLifecyclePhase::Tombstone => {}
        }
    }
    (snapshot, state.termination_reasons)
}

fn contribution_is_present(entry: &QueryLifecycleEntry) -> bool {
    entry.manifest.runtime_filter().is_some()
}

const fn phase_name(phase: QueryLifecyclePhase) -> &'static str {
    match phase {
        QueryLifecyclePhase::Initializing => "initializing",
        QueryLifecyclePhase::Initialized => "initialized",
        QueryLifecyclePhase::ControlAttached => "control_attached",
        QueryLifecyclePhase::Terminating => "terminating",
        QueryLifecyclePhase::Tombstone => "tombstone",
    }
}

fn termination_reason_index(reason: QueryTerminationReason) -> usize {
    match reason {
        QueryTerminationReason::CoordinatorAbort => 0,
        QueryTerminationReason::CoordinatorFinalize => 1,
        QueryTerminationReason::CoordinatorStreamLost => 2,
        QueryTerminationReason::CoordinatorHeartbeatTimeout => 3,
        QueryTerminationReason::LocalFailure => 4,
        QueryTerminationReason::PreStartTimeout => 5,
    }
}

fn format_digest(digest: ParticipantManifestDigest) -> String {
    use std::fmt::Write;

    let mut formatted = String::with_capacity(64);
    for byte in digest.as_bytes() {
        write!(&mut formatted, "{byte:02x}").expect("write digest to string");
    }
    formatted
}

#[allow(dead_code)]
fn internal_error(detail: impl Into<String>) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, detail)
}
