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

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

mod lifecycle;

#[cfg(not(test))]
pub(crate) use lifecycle::{HeartbeatManagerHandle, spawn_heartbeat_manager};
pub(crate) use lifecycle::{RegistryEventSink, run_heartbeat_round};

static GLOBAL_REGISTRY: OnceLock<Mutex<Option<Arc<BackendRegistry>>>> = OnceLock::new();

pub(crate) type BeId = u32;
pub(crate) type LiveBackend = (usize, SocketAddr);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LiveBackendSnapshot {
    entries: Vec<LiveBackend>,
}

impl LiveBackendSnapshot {
    pub(crate) fn new(entries: Vec<LiveBackend>) -> Self {
        Self { entries }
    }

    pub(crate) fn from_endpoints(backends: Vec<SocketAddr>) -> Self {
        Self::new(backends.into_iter().enumerate().collect())
    }

    pub(crate) fn entries(&self) -> &[LiveBackend] {
        &self.entries
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendState {
    Registering,
    Live,
    Lost,
    Decommissioning,
}

#[derive(Clone, Debug)]
pub(crate) struct BackendEntry {
    pub(crate) be_id: BeId,
    pub(crate) endpoint: SocketAddr,
    pub(crate) state: BackendState,
    pub(crate) start_epoch: u64,
    pub(crate) last_heartbeat_ms: i64,
    pub(crate) missed_heartbeats: u32,
    pub(crate) last_err: Option<String>,
    pub(crate) version: String,
    pub(crate) num_cores: u32,
    // Incremented by dispatchers when fragments are assigned.
    pub(crate) scheduled_fragments: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum HeartbeatOutcome {
    Ok {
        start_epoch: u64,
        version: String,
        num_cores: u32,
        now_ms: i64,
    },
    Failed {
        err: String,
    },
}

#[cfg(test)]
impl HeartbeatOutcome {
    pub(crate) fn ok(start_epoch: u64, now_ms: i64) -> Self {
        Self::Ok {
            start_epoch,
            version: "test".to_string(),
            num_cores: 1,
            now_ms,
        }
    }

    pub(crate) fn failed(err: &str) -> Self {
        Self::Failed {
            err: err.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistryEvent {
    BackendLost {
        be_id: BeId,
    },
    BackendRestarted {
        be_id: BeId,
        old_epoch: u64,
        new_epoch: u64,
    },
}

pub(crate) struct BackendRegistry {
    inner: Mutex<RegistryInner>,
}

struct RegistryInner {
    timeout_retries: u32,
    next_be_id: BeId,
    topology_revision: u64,
    entries: BTreeMap<BeId, BackendEntry>,
    endpoint_to_id: HashMap<SocketAddr, BeId>,
}

impl BackendRegistry {
    pub(crate) fn new(timeout_retries: u32) -> Self {
        Self {
            inner: Mutex::new(RegistryInner {
                timeout_retries: timeout_retries.max(1),
                next_be_id: 0,
                topology_revision: 0,
                entries: BTreeMap::new(),
                endpoint_to_id: HashMap::new(),
            }),
        }
    }

    pub(crate) fn add_backend(&self, endpoint: SocketAddr) -> BeId {
        self.add_backend_with_state(endpoint, BackendState::Registering)
    }

    pub(crate) fn add_backend_with_state(&self, endpoint: SocketAddr, state: BackendState) -> BeId {
        let mut inner = self.inner.lock().unwrap();
        if let Some(be_id) = inner.endpoint_to_id.get(&endpoint) {
            return *be_id;
        }

        let be_id = inner.next_be_id;
        inner.next_be_id = inner
            .next_be_id
            .checked_add(1)
            .expect("backend id overflow");
        inner.entries.insert(
            be_id,
            BackendEntry {
                be_id,
                endpoint,
                state,
                start_epoch: 0,
                last_heartbeat_ms: 0,
                missed_heartbeats: 0,
                last_err: None,
                version: String::new(),
                num_cores: 0,
                scheduled_fragments: 0,
            },
        );
        inner.endpoint_to_id.insert(endpoint, be_id);
        inner.topology_revision = inner.topology_revision.saturating_add(1);
        be_id
    }

    pub(crate) fn restore_backend(&self, be_id: BeId, endpoint: SocketAddr, state: BackendState) {
        let mut inner = self.inner.lock().unwrap();
        if inner.entries.contains_key(&be_id) {
            return;
        }
        if inner.endpoint_to_id.contains_key(&endpoint) {
            return;
        }
        inner.next_be_id = inner.next_be_id.max(be_id.saturating_add(1));
        inner.entries.insert(
            be_id,
            BackendEntry {
                be_id,
                endpoint,
                state,
                start_epoch: 0,
                last_heartbeat_ms: 0,
                missed_heartbeats: 0,
                last_err: None,
                version: String::new(),
                num_cores: 0,
                scheduled_fragments: 0,
            },
        );
        inner.endpoint_to_id.insert(endpoint, be_id);
        inner.topology_revision = inner.topology_revision.saturating_add(1);
    }

    pub(crate) fn seed_from_config(&self, endpoints: &[SocketAddr]) {
        for endpoint in endpoints {
            self.add_backend(*endpoint);
        }
    }

    pub(crate) fn apply_heartbeat_result(
        &self,
        be_id: BeId,
        outcome: HeartbeatOutcome,
    ) -> Vec<RegistryEvent> {
        let mut inner = self.inner.lock().unwrap();
        let timeout_retries = inner.timeout_retries;
        let Some(entry) = inner.entries.get_mut(&be_id) else {
            return Vec::new();
        };
        if entry.state == BackendState::Decommissioning {
            return Vec::new();
        }

        let previous_state = entry.state;
        let previous_start_epoch = entry.start_epoch;
        let events = match outcome {
            HeartbeatOutcome::Ok {
                start_epoch,
                version,
                num_cores,
                now_ms,
            } => {
                let mut events = Vec::new();
                if entry.start_epoch != 0 && start_epoch != 0 && entry.start_epoch != start_epoch {
                    events.push(RegistryEvent::BackendRestarted {
                        be_id,
                        old_epoch: entry.start_epoch,
                        new_epoch: start_epoch,
                    });
                }
                entry.start_epoch = start_epoch;
                entry.version = version;
                entry.num_cores = num_cores;
                entry.last_heartbeat_ms = now_ms;
                entry.missed_heartbeats = 0;
                entry.last_err = None;
                entry.state = BackendState::Live;
                events
            }
            HeartbeatOutcome::Failed { err } => {
                entry.missed_heartbeats = entry.missed_heartbeats.saturating_add(1);
                entry.last_err = Some(err);
                if entry.state != BackendState::Lost && entry.missed_heartbeats >= timeout_retries {
                    entry.state = BackendState::Lost;
                    vec![RegistryEvent::BackendLost { be_id }]
                } else {
                    Vec::new()
                }
            }
        };
        if entry.state != previous_state || entry.start_epoch != previous_start_epoch {
            inner.topology_revision = inner.topology_revision.saturating_add(1);
        }
        events
    }

    pub(crate) fn live_endpoints(&self) -> Vec<(BeId, SocketAddr)> {
        self.live_snapshot().1
    }

    pub(crate) fn live_snapshot(&self) -> (u64, Vec<(BeId, SocketAddr)>) {
        let inner = self.inner.lock().unwrap();
        (
            inner.topology_revision,
            inner
                .entries
                .iter()
                .filter_map(|(be_id, entry)| {
                    (entry.state == BackendState::Live).then_some((*be_id, entry.endpoint))
                })
                .collect(),
        )
    }

    pub(crate) fn live_backend_generation_snapshot(&self) -> (u64, Vec<(BeId, SocketAddr, u64)>) {
        let inner = self.inner.lock().unwrap();
        (
            inner.topology_revision,
            inner
                .entries
                .iter()
                .filter_map(|(be_id, entry)| {
                    (entry.state == BackendState::Live).then_some((
                        *be_id,
                        entry.endpoint,
                        entry.start_epoch,
                    ))
                })
                .collect(),
        )
    }

    pub(crate) fn snapshot(&self) -> Vec<BackendEntry> {
        self.inner
            .lock()
            .unwrap()
            .entries
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn all_for_heartbeat(&self) -> Vec<(BeId, SocketAddr)> {
        self.inner
            .lock()
            .unwrap()
            .entries
            .iter()
            .filter_map(|(be_id, entry)| {
                (entry.state != BackendState::Decommissioning).then_some((*be_id, entry.endpoint))
            })
            .collect()
    }

    pub(crate) fn mark_decommissioning(&self, endpoint: SocketAddr) -> Result<BeId, String> {
        let mut inner = self.inner.lock().unwrap();
        let Some(be_id) = inner.endpoint_to_id.get(&endpoint).copied() else {
            return Err(format!("backend {endpoint} not found"));
        };
        if let Some(entry) = inner.entries.get_mut(&be_id) {
            if entry.state != BackendState::Decommissioning {
                entry.state = BackendState::Decommissioning;
                inner.topology_revision = inner.topology_revision.saturating_add(1);
            }
        }
        Ok(be_id)
    }

    pub(crate) fn remove(&self, be_id: BeId) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.remove(&be_id) {
            // A removed endpoint may be added again, but logical ids are never reused.
            inner.endpoint_to_id.remove(&entry.endpoint);
            inner.topology_revision = inner.topology_revision.saturating_add(1);
        }
    }

    pub(crate) fn record_scheduled_fragment(&self, be_id: BeId) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.get_mut(&be_id) {
            entry.scheduled_fragments = entry.scheduled_fragments.saturating_add(1);
        }
    }

    pub(crate) fn count_live(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .entries
            .values()
            .filter(|entry| entry.state == BackendState::Live)
            .count()
    }

    pub(crate) fn count_by_state(&self, state: BackendState) -> usize {
        self.inner
            .lock()
            .unwrap()
            .entries
            .values()
            .filter(|entry| entry.state == state)
            .count()
    }
}

/// Install the process-wide backend topology registry. Idempotent: first writer wins.
pub(crate) fn install_backend_registry(reg: Arc<BackendRegistry>) -> bool {
    let mut guard = global_registry_cell().lock().unwrap();
    if guard.is_none() {
        *guard = Some(reg);
        return true;
    }
    false
}

/// The process-wide backend topology registry, when installed by the composition root.
pub(crate) fn backend_registry() -> Option<Arc<BackendRegistry>> {
    global_registry_cell().lock().unwrap().clone()
}

fn global_registry_cell() -> &'static Mutex<Option<Arc<BackendRegistry>>> {
    GLOBAL_REGISTRY.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn replace_backend_registry_for_test(reg: Option<Arc<BackendRegistry>>) {
    *global_registry_cell().lock().unwrap() = reg;
}

#[cfg(test)]
pub(crate) struct BackendRegistryTestGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl BackendRegistryTestGuard {
    pub(crate) fn new() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let guard = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        replace_backend_registry_for_test(None);
        Self { _guard: guard }
    }
}

#[cfg(test)]
impl Drop for BackendRegistryTestGuard {
    fn drop(&mut self) {
        replace_backend_registry_for_test(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn ep(p: u16) -> SocketAddr {
        format!("127.0.0.1:{p}").parse().unwrap()
    }

    #[test]
    fn add_then_first_heartbeat_goes_live() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));
        assert!(
            reg.live_endpoints().is_empty(),
            "registering is not live yet"
        );
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        assert!(ev.is_empty());
        assert_eq!(reg.live_endpoints(), vec![(id, ep(9070))]);
    }

    #[test]
    fn seed_from_config_assigns_backend_idx_ids_in_order() {
        let reg = BackendRegistry::new(3);
        reg.seed_from_config(&[ep(9070), ep(9071), ep(9072)]);

        let ids: Vec<BeId> = reg
            .snapshot()
            .into_iter()
            .map(|entry| entry.be_id)
            .collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn duplicate_endpoint_reuses_id_without_duplicate_entry() {
        let reg = BackendRegistry::new(3);

        let first = reg.add_backend(ep(9070));
        let second = reg.add_backend(ep(9070));

        assert_eq!(first, second);
        assert_eq!(reg.snapshot().len(), 1);
    }

    #[test]
    fn n_missed_heartbeats_goes_lost_and_emits_event_once() {
        let reg = BackendRegistry::new(2);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        assert!(
            reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x"))
                .is_empty()
        );
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x"));
        assert_eq!(ev, vec![RegistryEvent::BackendLost { be_id: id }]);
        assert!(reg.live_endpoints().is_empty());
        assert!(
            reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x"))
                .is_empty()
        );
    }

    #[test]
    fn recovery_clears_miss_counter() {
        let reg = BackendRegistry::new(2);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x"));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::failed("x"));
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 2000));
        assert!(ev.is_empty());
        assert_eq!(reg.live_endpoints(), vec![(id, ep(9070))]);
    }

    #[test]
    fn zero_start_epoch_is_unknown_and_does_not_emit_restart() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));

        assert!(
            reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(0, 1000))
                .is_empty()
        );
        assert!(
            reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(2, 1500))
                .is_empty()
        );
        assert!(
            reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(0, 2000))
                .is_empty()
        );
    }

    #[test]
    fn epoch_change_emits_restart_event() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        let ev = reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(2, 1500));
        assert_eq!(
            ev,
            vec![RegistryEvent::BackendRestarted {
                be_id: id,
                old_epoch: 1,
                new_epoch: 2
            }]
        );
        assert_eq!(reg.live_endpoints(), vec![(id, ep(9070))]);
    }

    #[test]
    fn decommission_excludes_from_live() {
        let reg = BackendRegistry::new(3);
        let id = reg.add_backend(ep(9070));
        reg.apply_heartbeat_result(id, HeartbeatOutcome::ok(1, 1000));
        reg.mark_decommissioning(ep(9070)).unwrap();
        assert!(reg.live_endpoints().is_empty());
        assert!(reg.all_for_heartbeat().is_empty());
    }

    #[test]
    fn remove_does_not_compact_or_reuse_logical_ids() {
        let reg = BackendRegistry::new(3);
        let first = reg.add_backend(ep(9070));
        let second = reg.add_backend(ep(9071));

        reg.remove(first);
        let readded = reg.add_backend(ep(9070));

        assert_eq!(second, 1);
        assert_eq!(readded, 2);
        let ids: Vec<BeId> = reg
            .snapshot()
            .into_iter()
            .map(|entry| entry.be_id)
            .collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
