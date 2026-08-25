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

//! FE-owned observed backend topology.
//!
//! BE announce is the only descriptor source.  FE pull heartbeats prove the
//! exact descriptor.  The registry stores orthogonal raw facts, and derives
//! eligibility from them; it has no durable membership catalogue or seed
//! reconciliation path.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_proto::membership::{BackendProcessDescriptor, BackendReportedState};
use novarocks_types::{BackendProcessId, ClusterRole};
use novarocks_version::native_build_identity;
use tokio::runtime::Handle;

use crate::common::backend_topology::{
    BackendTopologyError, BackendTopologyMetricsSnapshot, BackendTopologyPort,
    BackendTopologySnapshot, BackendTopologyValidationError, HeartbeatOutcome, LiveBackendTarget,
    publish_backend_topology_metrics,
};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport::heartbeat as native_heartbeat;
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};

#[derive(Clone, Debug)]
pub struct ClusterBackendOpenConfig {
    role: ClusterRole,
    heartbeat_interval: Duration,
    heartbeat_timeout_retries: u32,
    announce_lease_ttl: Duration,
}

impl ClusterBackendOpenConfig {
    pub fn new(
        role: ClusterRole,
        heartbeat_interval: Duration,
        heartbeat_timeout_retries: u32,
        announce_lease_ttl: Duration,
    ) -> Result<Self, String> {
        if heartbeat_interval.is_zero()
            || heartbeat_timeout_retries == 0
            || announce_lease_ttl.is_zero()
        {
            return Err(
                "cluster backend heartbeat and announce lease configuration must be non-zero"
                    .to_string(),
            );
        }
        Ok(Self {
            role,
            heartbeat_interval,
            heartbeat_timeout_retries,
            announce_lease_ttl,
        })
    }
    pub const fn role(&self) -> ClusterRole {
        self.role
    }
    pub const fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }
    pub const fn heartbeat_timeout_retries(&self) -> u32 {
        self.heartbeat_timeout_retries
    }
    pub const fn announce_lease_ttl(&self) -> Duration {
        self.announce_lease_ttl
    }
}

type HeartbeatProbe =
    dyn Fn(SocketAddr, BackendProcessId) -> HeartbeatOutcome + Send + Sync + 'static;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Compatibility {
    Unknown,
    Compatible,
    Incompatible(String),
}
impl Compatibility {
    fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }
    fn detail(&self) -> &str {
        match self {
            Self::Unknown => "awaiting exact heartbeat",
            Self::Compatible => "",
            Self::Incompatible(detail) => detail,
        }
    }
}

#[derive(Clone, Debug)]
struct BackendFacts {
    descriptor: BackendProcessDescriptor,
    announce_lease_valid: bool,
    announce_lease_expires_at: std::time::Instant,
    last_announce_ms: i64,
    exact_identity_verified: bool,
    reported_state: BackendReportedState,
    compatibility: Compatibility,
    endpoint_owned: bool,
    /// An exact replacement has transferred this endpoint to another process.
    /// A stale old process may not reclaim it by announcing again.
    superseded: bool,
    num_cores: u32,
    last_heartbeat_ms: i64,
    missed_heartbeats: u32,
    scheduled_fragments: u64,
    last_err: Option<String>,
}

impl BackendFacts {
    fn eligible(&self) -> bool {
        self.announce_lease_valid
            && self.exact_identity_verified
            && self.reported_state == BackendReportedState::Running
            && self.compatibility.is_compatible()
            && self.endpoint_owned
    }
}

struct TopologyState {
    timeout_retries: u32,
    revision: u64,
    terminal_error: Option<String>,
    // Primary index: a process is never inferred from an address.
    processes: BTreeMap<BackendProcessId, BackendFacts>,
    // Verified endpoint owner only. This is part of the eligibility predicate.
    endpoint_owners: BTreeMap<SocketAddr, BackendProcessId>,
    // Announce creates a pending replacement; exact pull transfers ownership.
    pending_endpoint_owners: BTreeMap<SocketAddr, BackendProcessId>,
}

#[derive(Clone, Copy)]
struct HeartbeatSignal {
    generation: u64,
    stopping: bool,
}

pub(crate) struct ClusterBackendService {
    state: Mutex<TopologyState>,
    heartbeat_interval: Duration,
    announce_lease_ttl: Duration,
    heartbeat_probe: Arc<HeartbeatProbe>,
    heartbeat_thread: Mutex<Option<JoinHandle<()>>>,
    heartbeat_round: Mutex<()>,
    heartbeat_signal: Mutex<HeartbeatSignal>,
    heartbeat_wake: Condvar,
    topology_wake: Condvar,
    #[cfg(test)]
    _test_runtime_owner: Option<Arc<tokio::runtime::Runtime>>,
}

impl ClusterBackendService {
    pub(crate) async fn open(
        config: ClusterBackendOpenConfig,
        runtime: Handle,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Arc<Self>, String> {
        if config.role() == ClusterRole::Be {
            return Err("role=be must not open ClusterBackendService".to_string());
        }
        let service = Arc::new(Self::new(&config, move |endpoint, process_id| {
            native_heartbeat(&data_runtime, endpoint, process_id)
        }));
        let _ = runtime;
        // Only a BE can create its immutable ProcessId descriptor through
        // AnnounceBackend.
        service.publish_snapshot();
        Ok(service)
    }

    fn new<F>(config: &ClusterBackendOpenConfig, probe: F) -> Self
    where
        F: Fn(SocketAddr, BackendProcessId) -> HeartbeatOutcome + Send + Sync + 'static,
    {
        Self {
            state: Mutex::new(TopologyState {
                timeout_retries: config.heartbeat_timeout_retries(),
                revision: 0,
                terminal_error: None,
                processes: BTreeMap::new(),
                endpoint_owners: BTreeMap::new(),
                pending_endpoint_owners: BTreeMap::new(),
            }),
            heartbeat_interval: config.heartbeat_interval(),
            announce_lease_ttl: config.announce_lease_ttl(),
            heartbeat_probe: Arc::new(probe),
            heartbeat_thread: Mutex::new(None),
            heartbeat_round: Mutex::new(()),
            heartbeat_signal: Mutex::new(HeartbeatSignal {
                generation: 0,
                stopping: false,
            }),
            heartbeat_wake: Condvar::new(),
            topology_wake: Condvar::new(),
            #[cfg(test)]
            _test_runtime_owner: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_transient_for_test(timeout_retries: u32) -> Self {
        let config = ClusterBackendOpenConfig::new(
            ClusterRole::AllInOne,
            Duration::from_millis(1),
            timeout_retries.max(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let handle = runtime.handle().clone();
        let data_runtime = FrontendDataRuntime::new(handle);
        let mut service = Self::new(&config, move |endpoint, process_id| {
            native_heartbeat(&data_runtime, endpoint, process_id)
        });
        service._test_runtime_owner = Some(runtime);
        service
    }

    #[cfg(test)]
    pub(crate) fn from_captured_targets_for_test(targets: &[LiveBackendTarget]) -> Self {
        let service = Self::new_transient_for_test(1);
        let mut state = service.state.lock().unwrap();
        for target in targets {
            let process_id = target.process_id().expect("captured process id");
            let endpoint = target.endpoint().expect("captured endpoint");
            state.endpoint_owners.insert(endpoint, process_id);
            state.processes.insert(
                process_id,
                BackendFacts {
                    descriptor: target.descriptor().clone(),
                    announce_lease_valid: true,
                    announce_lease_expires_at: std::time::Instant::now()
                        + service.announce_lease_ttl,
                    last_announce_ms: now_ms(),
                    exact_identity_verified: true,
                    reported_state: BackendReportedState::Running,
                    compatibility: Compatibility::Compatible,
                    endpoint_owned: true,
                    superseded: false,
                    num_cores: 0,
                    last_heartbeat_ms: 0,
                    missed_heartbeats: 0,
                    scheduled_fragments: 0,
                    last_err: None,
                },
            );
        }
        drop(state);
        service
    }

    /// Announce establishes a lease and a pending candidate, never eligibility.
    pub(crate) fn record_announce(
        &self,
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
    ) -> Result<(), String> {
        if reported_state == BackendReportedState::Unspecified {
            return Err("announced backend state must be running or draining".to_string());
        }
        let endpoint = descriptor_socket_addr(&descriptor)?;
        let process_id = descriptor
            .process_id()
            .map_err(|error| format!("invalid announced backend process id: {error}"))?;
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        let before = eligible_members(&state);
        let old = state.processes.get(&process_id).cloned();
        if old.as_ref().is_some_and(|facts| facts.superseded) {
            return Err(format!(
                "announced backend process {process_id} was already superseded at its endpoint"
            ));
        }
        if old
            .as_ref()
            .is_some_and(|facts| facts.descriptor.as_proto() != descriptor.as_proto())
        {
            return Err(format!(
                "announced backend process {process_id} changed its immutable descriptor"
            ));
        }
        let same_descriptor = old
            .as_ref()
            .is_some_and(|facts| facts.descriptor.as_proto() == descriptor.as_proto());
        let endpoint_owned = state.endpoint_owners.get(&endpoint) == Some(&process_id);
        let facts = state
            .processes
            .entry(process_id)
            .or_insert_with(|| BackendFacts {
                descriptor: descriptor.clone(),
                announce_lease_valid: true,
                announce_lease_expires_at: std::time::Instant::now() + self.announce_lease_ttl,
                last_announce_ms: now_ms(),
                exact_identity_verified: false,
                reported_state,
                compatibility: Compatibility::Unknown,
                endpoint_owned,
                superseded: false,
                num_cores: 0,
                last_heartbeat_ms: 0,
                missed_heartbeats: 0,
                scheduled_fragments: 0,
                last_err: None,
            });
        facts.descriptor = descriptor;
        facts.announce_lease_valid = true;
        facts.announce_lease_expires_at = std::time::Instant::now() + self.announce_lease_ttl;
        facts.last_announce_ms = now_ms();
        facts.reported_state = latched_reported_state(facts.reported_state, reported_state);
        facts.endpoint_owned = endpoint_owned;
        if !same_descriptor {
            facts.exact_identity_verified = false;
            facts.compatibility = Compatibility::Unknown;
        }
        if !endpoint_owned {
            state.pending_endpoint_owners.insert(endpoint, process_id);
        } else {
            state.pending_endpoint_owners.remove(&endpoint);
        }
        let changed = advance_if_eligible_changed(&mut state, before)?;
        drop(state);
        if changed {
            self.publish_snapshot();
        }
        self.wake_heartbeat_manager();
        Ok(())
    }

    pub(crate) fn start_heartbeat_manager(self: &Arc<Self>) -> Result<(), String> {
        let mut thread = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?;
        if thread.is_some() {
            return Ok(());
        }
        self.heartbeat_signal
            .lock()
            .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?
            .stopping = false;
        let service = Arc::clone(self);
        let interval = self.heartbeat_interval;
        *thread = Some(
            std::thread::Builder::new()
                .name("frontend-heartbeat-manager".to_string())
                .spawn(move || service.run_heartbeat_manager(interval))
                .map_err(|error| format!("spawn frontend heartbeat manager failed: {error}"))?,
        );
        Ok(())
    }

    pub(crate) fn stop_heartbeat_manager(&self) -> Result<(), String> {
        {
            let mut signal = self
                .heartbeat_signal
                .lock()
                .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?;
            signal.stopping = true;
            signal.generation = signal.generation.wrapping_add(1);
        }
        self.heartbeat_wake.notify_all();
        if let Some(join) = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?
            .take()
        {
            join.join()
                .map_err(|payload| format!("frontend heartbeat manager panicked: {payload:?}"))?;
        }
        Ok(())
    }

    fn run_heartbeat_manager(&self, interval: Duration) {
        let mut observed_generation = 0;
        loop {
            if self.heartbeat_is_stopping() {
                return;
            }
            self.refresh_expired_announce_leases(std::time::Instant::now());
            {
                let _round = self
                    .heartbeat_round
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                for (process_id, endpoint) in self.heartbeat_rows() {
                    match (self.heartbeat_probe)(endpoint, process_id) {
                        HeartbeatOutcome::Ok {
                            descriptor,
                            reported_state,
                            num_cores,
                            now_ms,
                        } => self.record_heartbeat_success(
                            process_id,
                            descriptor,
                            reported_state,
                            num_cores,
                            now_ms,
                        ),
                        HeartbeatOutcome::Failed { err } => {
                            self.record_heartbeat_failure_with_error(process_id, err);
                        }
                    }
                }
            }
            let signal = self
                .heartbeat_signal
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if signal.stopping {
                return;
            }
            let signal = if signal.generation == observed_generation {
                self.heartbeat_wake
                    .wait_timeout_while(signal, interval, |s| {
                        !s.stopping && s.generation == observed_generation
                    })
                    .unwrap_or_else(|p| p.into_inner())
                    .0
            } else {
                signal
            };
            if signal.stopping {
                return;
            }
            observed_generation = signal.generation;
        }
    }

    fn heartbeat_rows(&self) -> Vec<(BackendProcessId, SocketAddr)> {
        let state = self.state.lock().unwrap();
        state
            .processes
            .iter()
            .filter_map(|(id, facts)| {
                facts
                    .announce_lease_valid
                    .then(|| {
                        descriptor_socket_addr(&facts.descriptor)
                            .ok()
                            .map(|endpoint| (*id, endpoint))
                    })
                    .flatten()
            })
            .collect()
    }
    fn heartbeat_is_stopping(&self) -> bool {
        self.heartbeat_signal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopping
    }
    fn wake_heartbeat_manager(&self) {
        let mut signal = self
            .heartbeat_signal
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        signal.generation = signal.generation.wrapping_add(1);
        drop(signal);
        self.heartbeat_wake.notify_all();
    }

    pub(crate) fn record_heartbeat_success(
        &self,
        process_id: BackendProcessId,
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
        num_cores: u32,
        now_ms: i64,
    ) {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let mut state = self.state.lock().unwrap();
        let before = eligible_members(&state);
        let Some(announced) = state.processes.get(&process_id).cloned() else {
            return;
        };
        let exact = descriptor.as_proto() == announced.descriptor.as_proto();
        let compatibility = if !exact {
            Compatibility::Incompatible("heartbeat descriptor does not match announce".to_string())
        } else if descriptor.build_identity() != native_build_identity() {
            Compatibility::Incompatible(format!(
                "native build identity mismatch (expected {}, observed {})",
                native_build_identity(),
                descriptor.build_identity()
            ))
        } else {
            Compatibility::Compatible
        };
        let Ok(endpoint) = descriptor_socket_addr(&announced.descriptor) else {
            return;
        };
        let transfer = exact
            && compatibility.is_compatible()
            && reported_state == BackendReportedState::Running
            && state.pending_endpoint_owners.get(&endpoint) == Some(&process_id);
        let old_owner = transfer
            .then(|| state.endpoint_owners.get(&endpoint).copied())
            .flatten();
        if transfer {
            state.pending_endpoint_owners.remove(&endpoint);
            state.endpoint_owners.insert(endpoint, process_id);
            if let Some(old) = old_owner
                .filter(|old| *old != process_id)
                .and_then(|old| state.processes.get_mut(&old))
            {
                old.endpoint_owned = false;
                old.superseded = true;
            }
        }
        let endpoint_owned = state.endpoint_owners.get(&endpoint) == Some(&process_id);
        if let Some(facts) = state.processes.get_mut(&process_id) {
            facts.exact_identity_verified = exact;
            facts.reported_state = latched_reported_state(facts.reported_state, reported_state);
            facts.compatibility = compatibility;
            facts.endpoint_owned = endpoint_owned;
            facts.num_cores = num_cores;
            facts.last_heartbeat_ms = now_ms;
            facts.missed_heartbeats = 0;
            facts.last_err = None;
        }
        let changed = advance_if_eligible_changed(&mut state, before).unwrap_or(false);
        drop(state);
        if changed {
            self.publish_snapshot();
        }
    }

    #[cfg(test)]
    pub(crate) fn record_heartbeat_failure(&self, process_id: BackendProcessId) -> bool {
        self.record_heartbeat_failure_with_error(process_id, "heartbeat failed")
    }
    fn record_heartbeat_failure_with_error(
        &self,
        process_id: BackendProcessId,
        error: impl Into<String>,
    ) -> bool {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let mut state = self.state.lock().unwrap();
        let before = eligible_members(&state);
        let timeout = state.timeout_retries;
        let Some(facts) = state.processes.get_mut(&process_id) else {
            return false;
        };
        facts.missed_heartbeats = facts.missed_heartbeats.saturating_add(1);
        facts.last_err = Some(error.into());
        if facts.missed_heartbeats >= timeout {
            facts.exact_identity_verified = false;
        }
        let changed = advance_if_eligible_changed(&mut state, before).unwrap_or(false);
        drop(state);
        // Loss affects future admission but is not a query failure event.
        if changed {
            self.publish_snapshot();
        }
        changed
    }

    fn publish_snapshot(&self) {
        let metrics = {
            let state = self.state.lock().unwrap();
            metrics_snapshot(&state)
        };
        publish_backend_topology_metrics(metrics);
        self.topology_wake.notify_all();
    }

    fn refresh_expired_announce_leases(&self, now: std::time::Instant) -> bool {
        let mut state = self.state.lock().unwrap();
        let before = eligible_members(&state);
        for facts in state.processes.values_mut() {
            if facts.announce_lease_valid && facts.announce_lease_expires_at <= now {
                facts.announce_lease_valid = false;
            }
        }
        let changed = advance_if_eligible_changed(&mut state, before).unwrap_or(false);
        drop(state);
        if changed {
            self.publish_snapshot();
        }
        changed
    }

    fn snapshot_inner(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        let state = self
            .state
            .lock()
            .map_err(|_| BackendTopologyError::Unavailable {
                message: "lock frontend topology failed".to_string(),
            })?;
        if let Some(message) = &state.terminal_error {
            return Err(BackendTopologyError::Unavailable {
                message: message.clone(),
            });
        }
        BackendTopologySnapshot::try_new(state.revision, live_targets(&state))
    }
}

impl BackendTopologyPort for ClusterBackendService {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        self.snapshot_inner()
    }
    fn validate_snapshot(
        &self,
        expected: &BackendTopologySnapshot,
    ) -> Result<(), BackendTopologyValidationError> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let current = self
            .snapshot_inner()
            .map_err(BackendTopologyValidationError::Unavailable)?;
        if current.revision() != expected.revision() {
            return Err(BackendTopologyValidationError::RevisionChanged {
                captured_revision: expected.revision(),
                current_revision: current.revision(),
            });
        }
        if current != *expected {
            return Err(
                BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: current.revision(),
                },
            );
        }
        Ok(())
    }
    fn wait_for_eligible_after(
        &self,
        revision: u64,
        deadline: std::time::Instant,
    ) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BackendTopologyError::Unavailable {
                message: "lock frontend topology failed".to_string(),
            })?;
        loop {
            if let Some(message) = &state.terminal_error {
                return Err(BackendTopologyError::Unavailable {
                    message: message.clone(),
                });
            }
            let snapshot = BackendTopologySnapshot::try_new(state.revision, live_targets(&state))?;
            if snapshot.revision() > revision && !snapshot.targets().is_empty() {
                return Ok(snapshot);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(BackendTopologyError::Unavailable {
                    message: format!(
                        "timed out waiting for an eligible backend topology revision after {revision}"
                    ),
                });
            }
            let (next, _) = self
                .topology_wake
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("frontend topology wait lock");
            state = next;
        }
    }
    fn record_successful_stage(&self, backend_idx: usize, fragment_count: usize) {
        let mut state = self.state.lock().unwrap();
        let process_id = live_targets(&state)
            .get(backend_idx)
            .and_then(|target| target.process_id().ok());
        if let Some(facts) = process_id.and_then(|id| state.processes.get_mut(&id)) {
            facts.scheduled_fragments = facts
                .scheduled_fragments
                .saturating_add(fragment_count as u64);
        }
        crate::common::backend_topology::record_successful_stage(backend_idx, fragment_count);
    }
    fn show_backends(&self) -> Result<QueryResult, String> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        let names = [
            "ProcessId",
            "Endpoint",
            "LeaseValid",
            "IdentityVerified",
            "ReportedState",
            "Compatible",
            "EndpointOwned",
            "Eligible",
            "ScheduledFragments",
            "LastAnnounceAt",
            "LastHeartbeatAt",
            "BuildIdentity",
            "DiagnosticStatus",
            "StatusDetail",
        ];
        let mut columns = vec![Vec::new(); names.len()];
        for (process_id, facts) in &state.processes {
            let endpoint = facts
                .descriptor
                .endpoint()
                .map_err(|error| format!("invalid registered endpoint: {error}"))?;
            columns[0].push(process_id.to_string());
            columns[1].push(format!("{}:{}", endpoint.host(), endpoint.port()));
            columns[2].push(facts.announce_lease_valid.to_string());
            columns[3].push(facts.exact_identity_verified.to_string());
            columns[4].push(format!("{:?}", facts.reported_state));
            columns[5].push(facts.compatibility.is_compatible().to_string());
            columns[6].push(facts.endpoint_owned.to_string());
            columns[7].push(facts.eligible().to_string());
            columns[8].push(facts.scheduled_fragments.to_string());
            columns[9].push(facts.last_announce_ms.to_string());
            columns[10].push(facts.last_heartbeat_ms.to_string());
            columns[11].push(facts.descriptor.build_identity().to_string());
            columns[12].push(diagnostic_status(facts));
            columns[13].push(
                facts
                    .last_err
                    .clone()
                    .unwrap_or_else(|| facts.compatibility.detail().to_string()),
            );
        }
        let arrays = columns
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>)
            .collect();
        let schema = Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, false))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|error| format!("build SHOW BACKENDS result failed: {error}"))?;
        Ok(QueryResult {
            columns: names
                .iter()
                .map(|name| QueryResultColumn {
                    name: (*name).to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                })
                .collect(),
            chunks: vec![record_batch_to_chunk(batch)?],
        })
    }
}

fn latched_reported_state(
    current: BackendReportedState,
    observed: BackendReportedState,
) -> BackendReportedState {
    if current == BackendReportedState::Draining || observed == BackendReportedState::Draining {
        BackendReportedState::Draining
    } else {
        observed
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn diagnostic_status(facts: &BackendFacts) -> String {
    let mut states = Vec::new();
    if !facts.announce_lease_valid {
        states.push("Stale");
    }
    if !facts.exact_identity_verified {
        states.push("Lost");
    }
    if facts.reported_state == BackendReportedState::Draining {
        states.push("Draining");
    }
    if !facts.compatibility.is_compatible() {
        states.push("Incompatible");
    }
    if facts.superseded {
        states.push("Replaced");
    }
    if states.is_empty() {
        "Live".to_string()
    } else {
        states.join("|")
    }
}

fn descriptor_socket_addr(descriptor: &BackendProcessDescriptor) -> Result<SocketAddr, String> {
    let endpoint = descriptor
        .endpoint()
        .map_err(|error| format!("announced backend endpoint is invalid: {error}"))?;
    // Current transport is SocketAddr. Reject DNS explicitly rather than
    // resolving/rebinding it here; NWT-2 owns the host+port carrier cut.
    let host = endpoint.host().parse::<IpAddr>().map_err(|error| format!("announced backend endpoint host {} must be an IP address for current native transport: {error}", endpoint.host()))?;
    Ok(SocketAddr::new(host, endpoint.port()))
}
fn eligible_members(state: &TopologyState) -> BTreeSet<(BackendProcessId, SocketAddr)> {
    state
        .processes
        .iter()
        .filter_map(|(id, facts)| {
            facts
                .eligible()
                .then(|| {
                    descriptor_socket_addr(&facts.descriptor)
                        .ok()
                        .map(|endpoint| (*id, endpoint))
                })
                .flatten()
        })
        .collect()
}
fn live_targets(state: &TopologyState) -> Vec<LiveBackendTarget> {
    state
        .processes
        .values()
        .filter(|facts| facts.eligible())
        .enumerate()
        .map(|(membership_ordinal, facts)| {
            LiveBackendTarget::new(membership_ordinal, facts.descriptor.clone())
        })
        .collect()
}
fn metrics_snapshot(state: &TopologyState) -> BackendTopologyMetricsSnapshot {
    let mut metrics = BackendTopologyMetricsSnapshot {
        entries: state.processes.len(),
        revision: state.revision,
        ..BackendTopologyMetricsSnapshot::default()
    };
    for facts in state.processes.values() {
        metrics.announce_lease_valid += usize::from(facts.announce_lease_valid);
        metrics.identity_verified += usize::from(facts.exact_identity_verified);
        match facts.reported_state {
            BackendReportedState::Running => metrics.reported_running += 1,
            BackendReportedState::Draining => metrics.reported_draining += 1,
            BackendReportedState::Unspecified => {}
        }
        match facts.compatibility {
            Compatibility::Compatible => metrics.compatibility_compatible += 1,
            Compatibility::Incompatible(_) => metrics.compatibility_incompatible += 1,
            Compatibility::Unknown => metrics.compatibility_unknown += 1,
        }
        metrics.endpoint_owned += usize::from(facts.endpoint_owned);
        metrics.endpoint_unowned += usize::from(!facts.endpoint_owned);
        if facts.eligible() {
            metrics.eligible += 1;
        }
    }
    metrics
}
fn advance_if_eligible_changed(
    state: &mut TopologyState,
    before: BTreeSet<(BackendProcessId, SocketAddr)>,
) -> Result<bool, String> {
    if before == eligible_members(state) {
        return Ok(false);
    }
    if let Some(message) = &state.terminal_error {
        return Err(message.clone());
    }
    state.revision = state.revision.checked_add(1).ok_or_else(|| {
        let message = "frontend topology revision space is exhausted".to_string();
        state.terminal_error = Some(message.clone());
        message
    })?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::ClusterBackendService;
    use crate::common::backend_topology::BackendTopologyPort;
    use novarocks_proto::lifecycle::QueryControlEndpoint;
    use novarocks_proto::membership::{BackendProcessDescriptor, BackendReportedState};
    use novarocks_types::BackendProcessId;
    use novarocks_version::native_build_identity;
    use std::net::SocketAddr;
    use std::sync::Arc;
    fn descriptor(endpoint: SocketAddr) -> BackendProcessDescriptor {
        BackendProcessDescriptor::new(
            BackendProcessId::new_v7(),
            QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port()).unwrap(),
            "test",
            native_build_identity(),
        )
        .unwrap()
    }
    fn verify(service: &ClusterBackendService, descriptor: &BackendProcessDescriptor) {
        service.record_heartbeat_success(
            descriptor.process_id().unwrap(),
            descriptor.clone(),
            BackendReportedState::Running,
            2,
            1,
        );
    }
    #[test]
    fn announcement_is_not_eligible_until_exact_pull() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        assert!(service.snapshot().unwrap().targets().is_empty());
        verify(&service, &descriptor);
        assert_eq!(service.snapshot().unwrap().targets().len(), 1);
    }

    #[test]
    fn wait_for_eligible_after_requires_a_new_verified_revision() {
        let service = Arc::new(ClusterBackendService::new_transient_for_test(1));
        let revision = service.snapshot().expect("initial snapshot").revision();
        let waiter = {
            let service = Arc::clone(&service);
            std::thread::spawn(move || {
                service.wait_for_eligible_after(
                    revision,
                    std::time::Instant::now() + std::time::Duration::from_secs(1),
                )
            })
        };
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);

        let snapshot = waiter
            .join()
            .expect("topology waiter must not panic")
            .expect("new verified backend must wake waiter");
        assert!(snapshot.revision() > revision);
        assert_eq!(
            snapshot.targets()[0].process_id().unwrap(),
            descriptor.process_id().unwrap()
        );
    }
    #[test]
    fn replacement_is_pending_until_exact_pull_transfers_eligibility() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let endpoint = "127.0.0.1:9070".parse().unwrap();
        let old = descriptor(endpoint);
        service
            .record_announce(old.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &old);
        let new = descriptor(endpoint);
        service
            .record_announce(new.clone(), BackendReportedState::Running)
            .unwrap();
        assert_eq!(
            service.snapshot().unwrap().targets()[0]
                .process_id()
                .unwrap(),
            old.process_id().unwrap()
        );
        verify(&service, &new);
        assert_eq!(
            service.snapshot().unwrap().targets()[0]
                .process_id()
                .unwrap(),
            new.process_id().unwrap()
        );
    }
    #[test]
    fn heartbeat_loss_never_sends_unavailable() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        assert!(service.record_heartbeat_failure(descriptor.process_id().unwrap()));
        assert!(service.snapshot().unwrap().targets().is_empty());
    }

    #[test]
    fn announce_lease_expiry_removes_only_new_query_eligibility() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        let revision = service.snapshot().unwrap().revision();

        assert!(service.refresh_expired_announce_leases(
            std::time::Instant::now() + std::time::Duration::from_secs(2)
        ));
        let snapshot = service.snapshot().unwrap();
        assert!(snapshot.targets().is_empty());
        assert_eq!(snapshot.revision(), revision + 1);
    }

    #[test]
    fn draining_is_monotonic_across_later_running_observations() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);

        service
            .record_announce(descriptor.clone(), BackendReportedState::Draining)
            .unwrap();
        service.record_heartbeat_success(
            descriptor.process_id().unwrap(),
            descriptor,
            BackendReportedState::Running,
            2,
            2,
        );

        assert!(service.snapshot().unwrap().targets().is_empty());
        assert_eq!(
            service
                .state
                .lock()
                .unwrap()
                .processes
                .values()
                .next()
                .unwrap()
                .reported_state,
            BackendReportedState::Draining
        );
    }
    #[test]
    fn show_exposes_orthogonal_facts() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        let columns = service
            .show_backends()
            .unwrap()
            .columns
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        assert!(columns.contains(&"LeaseValid".to_string()));
        assert!(columns.contains(&"IdentityVerified".to_string()));
        assert!(columns.contains(&"EndpointOwned".to_string()));
        assert!(columns.contains(&"Eligible".to_string()));
        assert!(columns.contains(&"DiagnosticStatus".to_string()));
    }
}
