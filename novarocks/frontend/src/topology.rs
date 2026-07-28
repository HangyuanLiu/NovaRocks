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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::query_execution::backend::{
    BackendLifecycleState, BackendQueryEvent, BackendQueryEventSink, BackendTopologyMetadataStore,
    BackendTopologyMetricsSnapshot, BackendTopologyPort, HeartbeatOutcome, LiveBackendTarget,
    PersistedBackendTopology, publish_backend_topology_metrics,
};
use novarocks::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};

type HeartbeatProbe = dyn Fn(u32, SocketAddr) -> HeartbeatOutcome + Send + Sync + 'static;

#[derive(Clone, Debug)]
struct FrontendBackendEntry {
    endpoint: SocketAddr,
    state: BackendLifecycleState,
    start_epoch: u64,
    version: String,
    num_cores: u32,
    last_heartbeat_ms: i64,
    missed_heartbeats: u32,
    scheduled_fragments: u64,
    last_err: Option<String>,
    decommission_started: Option<Instant>,
}

struct TopologyState {
    timeout_retries: Option<u32>,
    decommission_timeout: Duration,
    topology_revision: u64,
    entries: BTreeMap<usize, FrontendBackendEntry>,
    next_backend_idx: usize,
    configured_seed_endpoints: Vec<SocketAddr>,
    metadata_store: Option<Arc<dyn BackendTopologyMetadataStore>>,
}

#[derive(Clone, Copy)]
struct HeartbeatSignal {
    generation: u64,
    stopping: bool,
}

/// Concrete frontend topology controller.
///
/// This is deliberately owned by `novarocks-frontend`: the core crate only
/// knows the `BackendTopologyPort` trait and has no global topology locator.
pub(crate) struct FrontendTopologyController {
    state: Mutex<TopologyState>,
    query_events: Mutex<Option<Arc<dyn BackendQueryEventSink>>>,
    heartbeat_probe: Arc<HeartbeatProbe>,
    heartbeat_thread: Mutex<Option<JoinHandle<()>>>,
    heartbeat_round: Mutex<()>,
    heartbeat_signal: Mutex<HeartbeatSignal>,
    heartbeat_wake: Condvar,
}

impl FrontendTopologyController {
    pub(crate) fn new_unconfigured() -> Self {
        Self::new_with_optional_probe(None, |be_id, endpoint| {
            novarocks::service::cluster_heartbeat::grpc_heartbeat(be_id, endpoint)
        })
    }

    #[cfg(test)]
    pub(crate) fn new(timeout_retries: u32) -> Self {
        Self::new_with_probe(timeout_retries, |be_id, endpoint| {
            novarocks::service::cluster_heartbeat::grpc_heartbeat(be_id, endpoint)
        })
    }

    #[cfg(test)]
    fn new_with_probe<F>(timeout_retries: u32, heartbeat_probe: F) -> Self
    where
        F: Fn(u32, SocketAddr) -> HeartbeatOutcome + Send + Sync + 'static,
    {
        Self::new_with_optional_probe(Some(timeout_retries.max(1)), heartbeat_probe)
    }

    fn new_with_optional_probe<F>(timeout_retries: Option<u32>, heartbeat_probe: F) -> Self
    where
        F: Fn(u32, SocketAddr) -> HeartbeatOutcome + Send + Sync + 'static,
    {
        Self {
            state: Mutex::new(TopologyState {
                timeout_retries,
                decommission_timeout: Duration::from_secs(300),
                topology_revision: 0,
                entries: BTreeMap::new(),
                next_backend_idx: 0,
                configured_seed_endpoints: Vec::new(),
                metadata_store: None,
            }),
            query_events: Mutex::new(None),
            heartbeat_probe: Arc::new(heartbeat_probe),
            heartbeat_thread: Mutex::new(None),
            heartbeat_round: Mutex::new(()),
            heartbeat_signal: Mutex::new(HeartbeatSignal {
                generation: 0,
                stopping: false,
            }),
            heartbeat_wake: Condvar::new(),
        }
    }

    pub(crate) fn configure_lifecycle(
        &self,
        timeout_retries: u32,
        decommission_timeout: Duration,
        configured_seed_endpoints: Vec<SocketAddr>,
    ) -> Result<(), String> {
        if self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?
            .is_some()
        {
            return Err(
                "frontend topology lifecycle cannot be reconfigured after heartbeat start"
                    .to_string(),
            );
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        state.timeout_retries = Some(timeout_retries.max(1));
        state.decommission_timeout = decommission_timeout;
        state.configured_seed_endpoints = configured_seed_endpoints.clone();
        for endpoint in configured_seed_endpoints {
            add_backend_entry(&mut state, endpoint)?;
        }
        drop(state);
        self.publish_snapshot();
        Ok(())
    }

    pub(crate) fn attach_query_events(&self, events: Arc<dyn BackendQueryEventSink>) {
        *self
            .query_events
            .lock()
            .expect("frontend topology event sink lock") = Some(events);
        self.publish_snapshot();
    }

    pub(crate) fn detach_query_events(&self) {
        self.query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }

    pub(crate) fn start_heartbeat_manager(
        self: &Arc<Self>,
        interval: Duration,
    ) -> Result<(), String> {
        if self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?
            .timeout_retries
            .is_none()
        {
            return Err("frontend topology lifecycle is not configured".to_string());
        }
        let mut heartbeat_thread = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?;
        if heartbeat_thread.is_some() {
            return Ok(());
        }
        {
            let mut signal = self
                .heartbeat_signal
                .lock()
                .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?;
            signal.stopping = false;
        }
        let controller = Arc::clone(self);
        let join = std::thread::Builder::new()
            .name("frontend-heartbeat-manager".to_string())
            .spawn(move || {
                let mut observed_generation = 0;
                loop {
                    if controller.heartbeat_is_stopping() {
                        return;
                    }
                    {
                        let _round = controller
                            .heartbeat_round
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if controller.heartbeat_is_stopping() {
                            return;
                        }
                        controller.process_decommissioning_once();
                        for (backend_idx, entry) in controller.heartbeat_rows() {
                            if controller.heartbeat_is_stopping() {
                                return;
                            }
                            let Ok(be_id) = u32::try_from(backend_idx) else {
                                continue;
                            };
                            match (controller.heartbeat_probe)(be_id, entry.endpoint) {
                                HeartbeatOutcome::Ok {
                                    start_epoch,
                                    version,
                                    num_cores,
                                    now_ms,
                                } => controller.record_heartbeat_success(
                                    backend_idx,
                                    start_epoch,
                                    version,
                                    num_cores,
                                    now_ms,
                                ),
                                HeartbeatOutcome::Failed { err } => {
                                    controller
                                        .record_heartbeat_failure_with_error(backend_idx, err);
                                }
                            }
                        }
                    }
                    let signal = controller
                        .heartbeat_signal
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if signal.stopping {
                        return;
                    }
                    let signal = if signal.generation == observed_generation {
                        controller
                            .heartbeat_wake
                            .wait_timeout_while(signal, interval, |signal| {
                                !signal.stopping && signal.generation == observed_generation
                            })
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .0
                    } else {
                        signal
                    };
                    if signal.stopping {
                        return;
                    }
                    observed_generation = signal.generation;
                }
            })
            .map_err(|error| format!("spawn frontend heartbeat manager failed: {error}"))?;
        *heartbeat_thread = Some(join);
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
        let join = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?
            .take();
        let join_result = match join {
            Some(join) => join
                .join()
                .map_err(|payload| format!("frontend heartbeat manager panicked: {payload:?}")),
            None => Ok(()),
        };
        self.heartbeat_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .stopping = false;
        join_result
    }

    fn heartbeat_is_stopping(&self) -> bool {
        self.heartbeat_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .stopping
    }

    fn wake_heartbeat_manager(&self) {
        let mut signal = self
            .heartbeat_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        signal.generation = signal.generation.wrapping_add(1);
        drop(signal);
        self.heartbeat_wake.notify_all();
    }

    pub(crate) fn record_heartbeat_success(
        &self,
        backend_idx: usize,
        start_epoch: u64,
        version: impl Into<String>,
        num_cores: u32,
        now_ms: i64,
    ) {
        let mut state = self.state.lock().expect("frontend topology lock");
        let (old_state, old_epoch, persisted) = {
            let Some(entry) = state.entries.get_mut(&backend_idx) else {
                return;
            };
            if entry.state == BackendLifecycleState::Decommissioning {
                return;
            }
            let old_state = entry.state;
            let old_epoch = entry.start_epoch;
            entry.state = BackendLifecycleState::Live;
            entry.start_epoch = start_epoch;
            entry.version = version.into();
            entry.num_cores = num_cores;
            entry.last_heartbeat_ms = now_ms;
            entry.missed_heartbeats = 0;
            entry.last_err = None;
            (
                old_state,
                old_epoch,
                PersistedBackendTopology::new(backend_idx, entry.endpoint, entry.state),
            )
        };
        let restarted = (old_epoch != 0 && start_epoch != 0 && old_epoch != start_epoch).then_some(
            BackendQueryEvent::Restarted {
                backend_idx,
                old_epoch,
                new_epoch: start_epoch,
            },
        );
        let metadata_store = state.metadata_store.clone();
        if old_state != BackendLifecycleState::Live || old_epoch != start_epoch {
            state.topology_revision = state.topology_revision.saturating_add(1);
        }
        drop(state);
        if old_state != BackendLifecycleState::Live {
            if let Some(store) = metadata_store {
                let _ = store.upsert_backend(persisted);
            }
        }
        self.publish_snapshot();
        if let Some(event) = restarted {
            self.dispatch_event(event);
        }
    }

    /// Records one failed frontend-owned heartbeat. Returns `true` exactly
    /// when this round transitions the backend to Lost.
    #[cfg(test)]
    pub(crate) fn record_heartbeat_failure(&self, backend_idx: usize) -> bool {
        self.record_heartbeat_failure_with_error(backend_idx, "heartbeat failed")
    }

    fn record_heartbeat_failure_with_error(
        &self,
        backend_idx: usize,
        error: impl Into<String>,
    ) -> bool {
        let mut state = self.state.lock().expect("frontend topology lock");
        let timeout_retries = state
            .timeout_retries
            .expect("heartbeat lifecycle is configured before heartbeat results are recorded");
        let (transitioned, persisted) = {
            let Some(entry) = state.entries.get_mut(&backend_idx) else {
                return false;
            };
            if entry.state == BackendLifecycleState::Decommissioning {
                return false;
            }
            entry.missed_heartbeats = entry.missed_heartbeats.saturating_add(1);
            entry.last_err = Some(error.into());
            let transitioned = entry.state != BackendLifecycleState::Lost
                && entry.missed_heartbeats >= timeout_retries;
            if transitioned {
                entry.state = BackendLifecycleState::Lost;
            }
            (
                transitioned,
                transitioned.then(|| {
                    PersistedBackendTopology::new(backend_idx, entry.endpoint, entry.state)
                }),
            )
        };
        if transitioned {
            state.topology_revision = state.topology_revision.saturating_add(1);
        }
        let metadata_store = state.metadata_store.clone();
        drop(state);
        if transitioned {
            if let (Some(store), Some(persisted)) = (metadata_store, persisted) {
                let _ = store.upsert_backend(persisted);
            }
            self.publish_snapshot();
            self.dispatch_event(BackendQueryEvent::Unavailable {
                backend_idx,
                reason: format!("backend {backend_idx} lost after heartbeat timeout"),
            });
        }
        transitioned
    }

    fn rows(&self) -> Vec<(usize, FrontendBackendEntry)> {
        self.state
            .lock()
            .expect("frontend topology lock")
            .entries
            .iter()
            .map(|(idx, entry)| (*idx, entry.clone()))
            .collect()
    }

    fn heartbeat_rows(&self) -> Vec<(usize, FrontendBackendEntry)> {
        self.rows()
            .into_iter()
            .filter(|(_, entry)| entry.state != BackendLifecycleState::Decommissioning)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn backend_count_for_test(&self) -> usize {
        self.state
            .lock()
            .expect("frontend topology lock")
            .entries
            .len()
    }

    #[cfg(test)]
    pub(crate) fn scheduled_fragment_count_for_test(&self, backend_idx: usize) -> u64 {
        self.state
            .lock()
            .expect("frontend topology lock")
            .entries
            .get(&backend_idx)
            .map_or(0, |entry| entry.scheduled_fragments)
    }

    fn publish_snapshot(&self) {
        let (revision, live, metrics) = {
            let state = self.state.lock().expect("frontend topology lock");
            let live =
                state
                    .entries
                    .iter()
                    .filter_map(|(backend_idx, entry)| {
                        (entry.state == BackendLifecycleState::Live).then_some(
                            LiveBackendTarget::new(*backend_idx, entry.endpoint, entry.start_epoch),
                        )
                    })
                    .collect();
            let mut metrics = BackendTopologyMetricsSnapshot::default();
            for entry in state.entries.values() {
                match entry.state {
                    BackendLifecycleState::Registering => metrics.registering += 1,
                    BackendLifecycleState::Live => metrics.live += 1,
                    BackendLifecycleState::Lost => metrics.lost += 1,
                    BackendLifecycleState::Decommissioning => metrics.decommissioning += 1,
                }
            }
            (state.topology_revision, live, metrics)
        };
        publish_backend_topology_metrics(metrics);
        let Some(events) = self
            .query_events
            .lock()
            .expect("frontend topology event sink lock")
            .clone()
        else {
            return;
        };
        let _ = catch_unwind(AssertUnwindSafe(|| {
            events.replace_live_backends(revision, live)
        }));
    }

    fn dispatch_event(&self, event: BackendQueryEvent) {
        let Some(events) = self
            .query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return;
        };
        let _ = catch_unwind(AssertUnwindSafe(|| events.on_backend_event(event)));
    }

    fn backend_has_active_queries(&self, backend_idx: usize) -> bool {
        self.query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|events| {
                catch_unwind(AssertUnwindSafe(|| {
                    events.backend_has_active_queries(backend_idx)
                }))
                .unwrap_or(true)
            })
    }

    fn remove_backend(&self, backend_idx: usize, endpoint: SocketAddr) -> Result<(), String> {
        let mut state = self.state.lock().expect("frontend topology lock");
        let matches = state
            .entries
            .get(&backend_idx)
            .is_some_and(|entry| entry.endpoint == endpoint);
        if !matches {
            return Ok(());
        }
        if let Some(store) = state.metadata_store.clone() {
            store.delete_backend(endpoint)?;
        }
        state.entries.remove(&backend_idx);
        state.topology_revision = state.topology_revision.saturating_add(1);
        drop(state);
        self.publish_snapshot();
        Ok(())
    }

    fn process_decommissioning_once(&self) {
        let candidates = self
            .rows()
            .into_iter()
            .filter_map(|(backend_idx, entry)| {
                (entry.state == BackendLifecycleState::Decommissioning)
                    .then_some((backend_idx, entry))
            })
            .collect::<Vec<_>>();
        let timeout = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .decommission_timeout;
        for (backend_idx, entry) in candidates {
            let active = self.backend_has_active_queries(backend_idx);
            let timed_out = entry
                .decommission_started
                .is_some_and(|started| started.elapsed() >= timeout);
            if active && !timed_out {
                continue;
            }
            if active {
                self.dispatch_event(BackendQueryEvent::Unavailable {
                    backend_idx,
                    reason: format!("backend {backend_idx} decommission timed out"),
                });
            }
            let _ = self.remove_backend(backend_idx, entry.endpoint);
        }
    }
}

fn add_backend_entry(state: &mut TopologyState, endpoint: SocketAddr) -> Result<usize, String> {
    if let Some(backend_idx) = state
        .entries
        .iter()
        .find_map(|(backend_idx, entry)| (entry.endpoint == endpoint).then_some(*backend_idx))
    {
        return Ok(backend_idx);
    }
    let backend_idx = state.next_backend_idx;
    state.next_backend_idx = state
        .next_backend_idx
        .checked_add(1)
        .ok_or_else(|| "frontend backend id overflow".to_string())?;
    state.entries.insert(
        backend_idx,
        FrontendBackendEntry {
            endpoint,
            state: BackendLifecycleState::Registering,
            start_epoch: 0,
            version: String::new(),
            num_cores: 0,
            last_heartbeat_ms: 0,
            missed_heartbeats: 0,
            scheduled_fragments: 0,
            last_err: None,
            decommission_started: None,
        },
    );
    state.topology_revision = state.topology_revision.saturating_add(1);
    Ok(backend_idx)
}

impl BackendTopologyPort for FrontendTopologyController {
    fn live_backends(&self) -> Vec<LiveBackendTarget> {
        self.rows()
            .into_iter()
            .filter_map(|(backend_idx, entry)| {
                (entry.state == BackendLifecycleState::Live).then_some(LiveBackendTarget::new(
                    backend_idx,
                    entry.endpoint,
                    entry.start_epoch,
                ))
            })
            .collect()
    }

    fn record_successful_fragment_submission(&self, backend_idx: usize) {
        let mut state = self.state.lock().expect("frontend topology lock");
        if let Some(entry) = state.entries.get_mut(&backend_idx) {
            entry.scheduled_fragments = entry.scheduled_fragments.saturating_add(1);
        }
    }

    fn install_metadata_store(
        &self,
        store: Arc<dyn BackendTopologyMetadataStore>,
    ) -> Result<(), String> {
        let persisted = store.load_backends()?;
        let mut restored = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        let mut next_backend_idx = 0usize;
        for backend in persisted {
            if restored.contains_key(&backend.backend_idx()) {
                return Err(format!(
                    "duplicate persisted backend id {}",
                    backend.backend_idx()
                ));
            }
            if let Some(existing) = endpoints.insert(backend.endpoint(), backend.backend_idx()) {
                return Err(format!(
                    "persisted backend endpoint {} has duplicate ids {existing} and {}",
                    backend.endpoint(),
                    backend.backend_idx()
                ));
            }
            next_backend_idx =
                next_backend_idx.max(backend.backend_idx().checked_add(1).ok_or_else(|| {
                    format!("persisted backend id {} overflows", backend.backend_idx())
                })?);
            let restored_state = match backend.state() {
                BackendLifecycleState::Decommissioning => BackendLifecycleState::Decommissioning,
                BackendLifecycleState::Registering
                | BackendLifecycleState::Live
                | BackendLifecycleState::Lost => BackendLifecycleState::Registering,
            };
            restored.insert(
                backend.backend_idx(),
                FrontendBackendEntry {
                    endpoint: backend.endpoint(),
                    state: restored_state,
                    start_epoch: 0,
                    version: String::new(),
                    num_cores: 0,
                    last_heartbeat_ms: 0,
                    missed_heartbeats: 0,
                    scheduled_fragments: 0,
                    last_err: None,
                    decommission_started: (restored_state
                        == BackendLifecycleState::Decommissioning)
                        .then(Instant::now),
                },
            );
        }

        let heartbeat_round = self
            .heartbeat_round
            .lock()
            .map_err(|_| "lock frontend topology heartbeat round failed".to_string())?;
        let mut state = self.state.lock().expect("frontend topology lock");
        state.entries = restored;
        state.next_backend_idx = next_backend_idx;
        state.metadata_store = Some(store);
        for endpoint in state.configured_seed_endpoints.clone() {
            add_backend_entry(&mut state, endpoint)?;
        }
        state.topology_revision = state.topology_revision.saturating_add(1);
        drop(state);
        drop(heartbeat_round);
        self.publish_snapshot();
        self.wake_heartbeat_manager();
        Ok(())
    }

    fn add_backend(&self, endpoint: SocketAddr) -> Result<(), String> {
        let mut state = self.state.lock().expect("frontend topology lock");
        if state
            .entries
            .values()
            .any(|entry| entry.endpoint == endpoint)
        {
            return Ok(());
        }
        let backend_idx = add_backend_entry(&mut state, endpoint)?;
        let metadata_store = state.metadata_store.clone();
        drop(state);
        if let Some(store) = metadata_store {
            if let Err(error) = store.upsert_backend(PersistedBackendTopology::new(
                backend_idx,
                endpoint,
                BackendLifecycleState::Registering,
            )) {
                let mut state = self.state.lock().expect("frontend topology lock");
                state.entries.remove(&backend_idx);
                state.topology_revision = state.topology_revision.saturating_add(1);
                drop(state);
                self.publish_snapshot();
                return Err(error);
            }
        }
        self.publish_snapshot();
        self.wake_heartbeat_manager();
        Ok(())
    }

    fn drop_backend(&self, endpoint: SocketAddr, force: bool) -> Result<(), String> {
        let mut state = self.state.lock().expect("frontend topology lock");
        let backend_idx = state
            .entries
            .iter()
            .find_map(|(idx, entry)| (entry.endpoint == endpoint).then_some(*idx))
            .ok_or_else(|| format!("backend {endpoint} not found"))?;
        if force {
            drop(state);
            self.remove_backend(backend_idx, endpoint)?;
            self.dispatch_event(BackendQueryEvent::Unavailable {
                backend_idx,
                reason: format!("backend {backend_idx} dropped forcefully"),
            });
            return Ok(());
        }
        let entry = state
            .entries
            .get_mut(&backend_idx)
            .expect("backend index was resolved from the same topology snapshot");
        if entry.state != BackendLifecycleState::Decommissioning {
            entry.state = BackendLifecycleState::Decommissioning;
            entry.decommission_started = Some(Instant::now());
            state.topology_revision = state.topology_revision.saturating_add(1);
        }
        let metadata_store = state.metadata_store.clone();
        drop(state);
        self.publish_snapshot();

        if !self.backend_has_active_queries(backend_idx) {
            return self.remove_backend(backend_idx, endpoint);
        }
        if let Some(store) = metadata_store {
            store.upsert_backend(PersistedBackendTopology::new(
                backend_idx,
                endpoint,
                BackendLifecycleState::Decommissioning,
            ))?;
        }
        self.wake_heartbeat_manager();
        Ok(())
    }

    fn show_backends(&self) -> Result<QueryResult, String> {
        let column_names = [
            "BackendId",
            "Host",
            "GrpcPort",
            "State",
            "ScheduledFragments",
        ];
        let mut columns = vec![Vec::<String>::new(); column_names.len()];
        for (backend_idx, entry) in self.rows() {
            columns[0].push(backend_idx.to_string());
            columns[1].push(entry.endpoint.ip().to_string());
            columns[2].push(entry.endpoint.port().to_string());
            columns[3].push(entry.state.as_str().to_string());
            columns[4].push(entry.scheduled_fragments.to_string());
        }
        let fields = column_names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>();
        let arrays = columns
            .into_iter()
            .map(|values| {
                std::sync::Arc::new(StringArray::from(values))
                    as std::sync::Arc<dyn arrow::array::Array>
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| format!("build SHOW BACKENDS result failed: {error}"))?;
        Ok(QueryResult {
            columns: column_names
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use novarocks::query_execution::backend::{
        BackendLifecycleState, BackendQueryEvent, BackendQueryEventSink,
        BackendTopologyMetadataStore, BackendTopologyPort, HeartbeatOutcome, LiveBackendTarget,
        PersistedBackendTopology,
    };

    use super::FrontendTopologyController;

    #[derive(Default)]
    struct RecordingQueryEvents {
        active: AtomicBool,
        events: Mutex<Vec<BackendQueryEvent>>,
        snapshots: Mutex<Vec<Vec<LiveBackendTarget>>>,
        order: Mutex<Vec<&'static str>>,
    }

    impl BackendQueryEventSink for RecordingQueryEvents {
        fn on_backend_event(&self, event: BackendQueryEvent) {
            self.order.lock().unwrap().push("event");
            self.events.lock().unwrap().push(event);
        }

        fn backend_has_active_queries(&self, _backend_idx: usize) -> bool {
            self.active.load(Ordering::SeqCst)
        }

        fn replace_live_backends(&self, _revision: u64, backends: Vec<LiveBackendTarget>) {
            self.order.lock().unwrap().push("snapshot");
            self.snapshots.lock().unwrap().push(backends);
        }
    }

    #[derive(Default)]
    struct RecordingMetadataStore {
        loaded: Mutex<Vec<PersistedBackendTopology>>,
        upserts: Mutex<Vec<PersistedBackendTopology>>,
        deletes: Mutex<Vec<SocketAddr>>,
        fail_deletes: AtomicBool,
    }

    impl BackendTopologyMetadataStore for RecordingMetadataStore {
        fn load_backends(&self) -> Result<Vec<PersistedBackendTopology>, String> {
            Ok(self.loaded.lock().unwrap().clone())
        }

        fn upsert_backend(&self, backend: PersistedBackendTopology) -> Result<(), String> {
            self.upserts.lock().unwrap().push(backend);
            Ok(())
        }

        fn delete_backend(&self, endpoint: SocketAddr) -> Result<(), String> {
            if self.fail_deletes.load(Ordering::SeqCst) {
                return Err("injected backend metadata delete failure".to_string());
            }
            self.deletes.lock().unwrap().push(endpoint);
            Ok(())
        }
    }

    #[test]
    fn frontend_controller_owns_live_snapshot_and_management() {
        let controller = FrontendTopologyController::new(1);
        let endpoint: SocketAddr = "127.0.0.1:9070".parse().unwrap();

        controller.add_backend(endpoint).unwrap();
        assert!(controller.live_backends().is_empty());

        controller.record_heartbeat_success(0, 17, "test", 2, 100);
        assert_eq!(controller.live_backends().len(), 1);

        controller.record_successful_fragment_submission(0);
        let shown = controller.show_backends().unwrap();
        assert_eq!(shown.chunks.len(), 1);
        assert_eq!(
            shown
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            [
                "BackendId",
                "Host",
                "GrpcPort",
                "State",
                "ScheduledFragments",
            ],
            "the native cross-process runner consumes this compact frontend topology contract"
        );

        assert!(controller.record_heartbeat_failure(0));
        assert!(controller.live_backends().is_empty());

        controller.drop_backend(endpoint, true).unwrap();
        assert!(controller.live_backends().is_empty());
    }

    #[test]
    fn changed_start_epoch_publishes_new_snapshot_before_restart_event() {
        let controller = FrontendTopologyController::new(3);
        let events = Arc::new(RecordingQueryEvents::default());
        controller.attach_query_events(events.clone());
        let endpoint: SocketAddr = "127.0.0.1:9071".parse().unwrap();
        controller.add_backend(endpoint).unwrap();
        controller.record_heartbeat_success(0, 17, "v1", 2, 100);
        events.order.lock().unwrap().clear();

        controller.record_heartbeat_success(0, 18, "v2", 4, 200);

        assert_eq!(
            events.events.lock().unwrap().as_slice(),
            [BackendQueryEvent::Restarted {
                backend_idx: 0,
                old_epoch: 17,
                new_epoch: 18,
            }]
        );
        assert_eq!(
            events.order.lock().unwrap().as_slice(),
            ["snapshot", "event"],
            "the query registry must see the new generation before restart cancellation"
        );
        assert_eq!(controller.live_backends()[0].start_epoch(), 18);
    }

    #[test]
    fn configured_heartbeat_retry_threshold_is_applied() {
        let controller = FrontendTopologyController::new(1);
        let endpoint: SocketAddr = "127.0.0.1:9072".parse().unwrap();
        controller
            .configure_lifecycle(2, Duration::from_secs(1), vec![endpoint])
            .unwrap();
        controller.record_heartbeat_success(0, 1, "v", 1, 1);

        assert!(!controller.record_heartbeat_failure(0));
        assert_eq!(controller.live_backends().len(), 1);
        assert!(controller.record_heartbeat_failure(0));
        assert!(controller.live_backends().is_empty());
    }

    #[test]
    fn unconfigured_controller_cannot_start_heartbeat_manager() {
        let controller = Arc::new(FrontendTopologyController::new_unconfigured());

        let error = controller
            .start_heartbeat_manager(Duration::from_millis(1))
            .expect_err("application topology must receive cluster lifecycle config first");

        assert!(error.contains("lifecycle is not configured"), "{error}");
    }

    #[test]
    fn heartbeat_manager_stop_joins_and_allows_clean_restart() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_probe = Arc::clone(&calls);
        let controller = Arc::new(FrontendTopologyController::new_with_probe(
            3,
            move |_be_id, _endpoint| {
                calls_for_probe.fetch_add(1, Ordering::SeqCst);
                HeartbeatOutcome::Failed {
                    err: "expected test failure".to_string(),
                }
            },
        ));
        let endpoint: SocketAddr = "127.0.0.1:9073".parse().unwrap();
        controller
            .configure_lifecycle(3, Duration::from_secs(1), vec![])
            .unwrap();
        controller
            .start_heartbeat_manager(Duration::from_secs(60))
            .unwrap();
        controller.add_backend(endpoint).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while calls.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "heartbeat did not wake for ADD");
            std::thread::yield_now();
        }

        controller.stop_heartbeat_manager().unwrap();
        let stopped_calls = calls.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            stopped_calls,
            "a joined manager cannot probe after shutdown returns"
        );

        controller
            .start_heartbeat_manager(Duration::from_secs(60))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while calls.load(Ordering::SeqCst) == stopped_calls {
            assert!(
                Instant::now() < deadline,
                "restarted heartbeat manager did not probe"
            );
            std::thread::yield_now();
        }
        controller.stop_heartbeat_manager().unwrap();
    }

    #[test]
    fn non_force_drop_drains_active_queries_then_deletes_persisted_backend() {
        let controller = FrontendTopologyController::new(3);
        let events = Arc::new(RecordingQueryEvents::default());
        events.active.store(true, Ordering::SeqCst);
        controller.attach_query_events(events.clone());
        let store = Arc::new(RecordingMetadataStore::default());
        controller.install_metadata_store(store.clone()).unwrap();
        let endpoint: SocketAddr = "127.0.0.1:9074".parse().unwrap();
        controller.add_backend(endpoint).unwrap();
        controller.record_heartbeat_success(0, 11, "v", 1, 1);

        controller.drop_backend(endpoint, false).unwrap();

        assert!(controller.live_backends().is_empty());
        assert_eq!(controller.backend_count_for_test(), 1);
        assert_eq!(
            store.upserts.lock().unwrap().last().unwrap().state(),
            BackendLifecycleState::Decommissioning
        );
        assert!(events.events.lock().unwrap().is_empty());

        events.active.store(false, Ordering::SeqCst);
        controller.process_decommissioning_once();
        assert_eq!(controller.backend_count_for_test(), 0);
        assert_eq!(store.deletes.lock().unwrap().as_slice(), [endpoint]);
    }

    #[test]
    fn force_drop_fails_active_queries_and_removes_persisted_backend_immediately() {
        let controller = FrontendTopologyController::new(3);
        let events = Arc::new(RecordingQueryEvents::default());
        events.active.store(true, Ordering::SeqCst);
        controller.attach_query_events(events.clone());
        let store = Arc::new(RecordingMetadataStore::default());
        controller.install_metadata_store(store.clone()).unwrap();
        let endpoint: SocketAddr = "127.0.0.1:9075".parse().unwrap();
        controller.add_backend(endpoint).unwrap();

        controller.drop_backend(endpoint, true).unwrap();

        assert_eq!(controller.backend_count_for_test(), 0);
        assert_eq!(
            store
                .upserts
                .lock()
                .unwrap()
                .iter()
                .map(PersistedBackendTopology::state)
                .collect::<Vec<_>>(),
            [BackendLifecycleState::Registering],
            "force DROP must delete directly without persisting an intermediate decommissioning state"
        );
        assert_eq!(store.deletes.lock().unwrap().as_slice(), [endpoint]);
        assert_eq!(
            events.events.lock().unwrap().as_slice(),
            [BackendQueryEvent::Unavailable {
                backend_idx: 0,
                reason: "backend 0 dropped forcefully".to_string(),
            }]
        );
    }

    #[test]
    fn force_drop_delete_failure_preserves_live_topology_and_queries() {
        let controller = FrontendTopologyController::new(3);
        let events = Arc::new(RecordingQueryEvents::default());
        events.active.store(true, Ordering::SeqCst);
        controller.attach_query_events(events.clone());
        let store = Arc::new(RecordingMetadataStore::default());
        controller.install_metadata_store(store.clone()).unwrap();
        let endpoint: SocketAddr = "127.0.0.1:9082".parse().unwrap();
        controller.add_backend(endpoint).unwrap();
        controller.record_heartbeat_success(0, 11, "v", 1, 1);
        store.fail_deletes.store(true, Ordering::SeqCst);

        let error = controller
            .drop_backend(endpoint, true)
            .expect_err("durable delete failure must reject force DROP");

        assert!(
            error.contains("injected backend metadata delete failure"),
            "{error}"
        );
        assert_eq!(controller.backend_count_for_test(), 1);
        assert_eq!(
            controller.live_backends(),
            [LiveBackendTarget::new(0, endpoint, 11)],
            "failed durable deletion must roll the backend back into the schedulable topology"
        );
        assert!(
            events.events.lock().unwrap().is_empty(),
            "queries must not fail before the durable DROP commits"
        );
        let rows = controller.rows();
        assert_eq!(rows[0].1.state, BackendLifecycleState::Live);
        assert!(rows[0].1.decommission_started.is_none());
    }

    #[test]
    fn metadata_restore_preserves_backend_ids_before_adding_config_seeds() {
        let controller = FrontendTopologyController::new(3);
        let configured: SocketAddr = "127.0.0.1:9076".parse().unwrap();
        let persisted: SocketAddr = "127.0.0.1:9077".parse().unwrap();
        controller
            .configure_lifecycle(3, Duration::from_secs(1), vec![configured])
            .unwrap();
        let store = Arc::new(RecordingMetadataStore::default());
        store
            .loaded
            .lock()
            .unwrap()
            .push(PersistedBackendTopology::new(
                7,
                persisted,
                BackendLifecycleState::Live,
            ));

        controller.install_metadata_store(store).unwrap();

        let rows = controller.rows();
        assert!(
            controller.live_backends().is_empty(),
            "a persisted Live row must not be scheduled before a fresh heartbeat proves its process epoch"
        );
        assert_eq!(
            rows[0].1.state,
            BackendLifecycleState::Registering,
            "persisted physical liveness is stale across frontend restart"
        );
        assert_eq!(
            rows.iter()
                .map(|(backend_idx, entry)| (*backend_idx, entry.endpoint))
                .collect::<Vec<_>>(),
            vec![(7, persisted), (8, configured)]
        );
    }

    #[test]
    fn metadata_restore_waits_for_an_inflight_heartbeat_round() {
        let (probe_started_tx, probe_started_rx) = mpsc::channel();
        let (release_probe_tx, release_probe_rx) = mpsc::channel();
        let release_probe_rx = Mutex::new(release_probe_rx);
        let probe_calls = AtomicUsize::new(0);
        let controller = Arc::new(FrontendTopologyController::new_with_probe(
            3,
            move |_be_id, _endpoint| {
                if probe_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    probe_started_tx.send(()).unwrap();
                    release_probe_rx.lock().unwrap().recv().unwrap();
                }
                HeartbeatOutcome::Failed {
                    err: "expected test failure".to_string(),
                }
            },
        ));
        let configured: SocketAddr = "127.0.0.1:9078".parse().unwrap();
        controller
            .configure_lifecycle(3, Duration::from_secs(1), vec![configured])
            .unwrap();
        controller
            .start_heartbeat_manager(Duration::from_secs(60))
            .unwrap();
        probe_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("heartbeat probe must enter before metadata restore");

        let store = Arc::new(RecordingMetadataStore::default());
        let restored: SocketAddr = "127.0.0.1:9079".parse().unwrap();
        store
            .loaded
            .lock()
            .unwrap()
            .push(PersistedBackendTopology::new(
                7,
                restored,
                BackendLifecycleState::Registering,
            ));
        let controller_for_install = Arc::clone(&controller);
        let (install_done_tx, install_done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            install_done_tx
                .send(controller_for_install.install_metadata_store(store))
                .unwrap();
        });

        assert!(
            install_done_rx
                .recv_timeout(Duration::from_millis(20))
                .is_err(),
            "metadata replacement must wait until an in-flight probe can no longer update the old backend index"
        );
        release_probe_tx.send(()).unwrap();
        install_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("metadata install thread must finish")
            .expect("metadata restore");
        controller.stop_heartbeat_manager().unwrap();

        assert_eq!(
            controller
                .rows()
                .into_iter()
                .map(|(backend_idx, entry)| (backend_idx, entry.endpoint))
                .collect::<Vec<_>>(),
            [(7, restored), (8, configured)]
        );
    }
}
