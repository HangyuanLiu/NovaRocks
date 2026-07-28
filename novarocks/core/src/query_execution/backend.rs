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

//! Neutral frontend-facing backend topology and lifecycle boundary.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::runtime::endpoint::RuntimeEndpoint;

/// Frontend-owned topology and backend-management boundary consumed by core.
///
/// Core intentionally has no registry singleton, heartbeat loop, or role-aware
/// backend-management implementation. Composition roots inject this port.
pub trait BackendTopologyPort: Send + Sync + 'static {
    fn live_backends(&self) -> Vec<LiveBackendTarget>;

    fn record_successful_fragment_submission(&self, backend_idx: usize);

    fn install_metadata_store(
        &self,
        store: Arc<dyn BackendTopologyMetadataStore>,
    ) -> Result<(), String>;

    fn add_backend(&self, endpoint: SocketAddr) -> Result<(), String>;

    fn drop_backend(&self, endpoint: SocketAddr, force: bool) -> Result<(), String>;

    fn show_backends(&self) -> Result<crate::runtime::query_result::QueryResult, String>;
}

pub type BackendTopologyService = Arc<dyn BackendTopologyPort>;
pub type BeId = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendLifecycleState {
    Registering,
    Live,
    Lost,
    Decommissioning,
}

impl BackendLifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registering => "Registering",
            Self::Live => "Live",
            Self::Lost => "Lost",
            Self::Decommissioning => "Decommissioning",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "Registering" => Ok(Self::Registering),
            "Live" => Ok(Self::Live),
            "Lost" => Ok(Self::Lost),
            "Decommissioning" => Ok(Self::Decommissioning),
            other => Err(format!("invalid persisted backend state '{other}'")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedBackendTopology {
    backend_idx: usize,
    endpoint: SocketAddr,
    state: BackendLifecycleState,
}

impl PersistedBackendTopology {
    pub const fn new(
        backend_idx: usize,
        endpoint: SocketAddr,
        state: BackendLifecycleState,
    ) -> Self {
        Self {
            backend_idx,
            endpoint,
            state,
        }
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub const fn state(&self) -> BackendLifecycleState {
        self.state
    }
}

/// Synchronous metadata boundary used by the frontend topology owner.
///
/// Core supplies the adapter over its existing metadata repository; frontend
/// owns lifecycle policy and never imports the repository implementation.
pub trait BackendTopologyMetadataStore: Send + Sync + 'static {
    fn load_backends(&self) -> Result<Vec<PersistedBackendTopology>, String>;

    fn upsert_backend(&self, backend: PersistedBackendTopology) -> Result<(), String>;

    fn delete_backend(&self, endpoint: SocketAddr) -> Result<(), String>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendTopologyMetricsSnapshot {
    pub registering: usize,
    pub live: usize,
    pub lost: usize,
    pub decommissioning: usize,
}

/// Publishes the latest frontend-owned topology counts to the shared process
/// metrics endpoint. A scrape reads this snapshot and never resets it.
pub fn publish_backend_topology_metrics(snapshot: BackendTopologyMetricsSnapshot) {
    crate::service::metrics_http::publish_backend_topology_metrics(snapshot);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LiveBackendSnapshot {
    entries: Vec<(usize, SocketAddr)>,
}

impl LiveBackendSnapshot {
    pub(crate) fn new(entries: Vec<(usize, SocketAddr)>) -> Self {
        Self { entries }
    }

    pub(crate) fn from_endpoints(backends: Vec<SocketAddr>) -> Self {
        Self::new(backends.into_iter().enumerate().collect())
    }

    pub(crate) fn entries(&self) -> &[(usize, SocketAddr)] {
        &self.entries
    }
}

#[derive(Clone, Debug)]
pub enum HeartbeatOutcome {
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

/// Core-local scheduling metric. Topology accounting is performed by the
/// frontend-owned port at the composition boundary.
pub fn record_successful_fragment_submission(_backend_idx: usize) {
    crate::service::metrics_http::observe_fragment_scheduled();
}

/// Resolves the report endpoint after the coordinator gRPC listener has bound.
///
/// A configured port of zero requests an ephemeral listener, so its actual
/// bound port must be read at query time rather than frozen during host open.
pub struct CoordinatorReportEndpoint {
    endpoint: RuntimeEndpoint,
}

impl CoordinatorReportEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, String> {
        Ok(Self {
            endpoint: RuntimeEndpoint::new(host, i32::from(port))?,
        })
    }

    pub fn from_socket_addr(endpoint: SocketAddr) -> Self {
        Self {
            endpoint: RuntimeEndpoint::from_socket_addr(endpoint),
        }
    }

    pub(crate) fn into_runtime_endpoint(self) -> RuntimeEndpoint {
        self.endpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendQueryEvent {
    Unavailable {
        backend_idx: usize,
        reason: String,
    },
    Restarted {
        backend_idx: usize,
        old_epoch: u64,
        new_epoch: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveBackendTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    start_epoch: u64,
}

impl LiveBackendTarget {
    pub fn new(backend_idx: usize, endpoint: SocketAddr, start_epoch: u64) -> Self {
        Self {
            backend_idx,
            endpoint,
            start_epoch,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(self) -> SocketAddr {
        self.endpoint
    }

    pub const fn start_epoch(self) -> u64 {
        self.start_epoch
    }
}

/// Frontend-owned query activity consumed by the core backend registry.
///
/// The sink owns query-wide failure and exact remote cancellation. Core only
/// forwards lifecycle facts and never performs BE-local query cleanup.
pub trait BackendQueryEventSink: Send + Sync + 'static {
    fn on_backend_event(&self, event: BackendQueryEvent);

    fn backend_has_active_queries(&self, backend_idx: usize) -> bool;

    fn replace_live_backends(&self, revision: u64, backends: Vec<LiveBackendTarget>);
}

pub trait CoordinatorReportEndpointSink: Send + Sync + 'static {
    fn set_bound_port(&self, port: u16);
}

#[cfg(test)]
pub(crate) struct NoopBackendQueryEventSink;

#[cfg(test)]
impl BackendQueryEventSink for NoopBackendQueryEventSink {
    fn on_backend_event(&self, _event: BackendQueryEvent) {}

    fn backend_has_active_queries(&self, _backend_idx: usize) -> bool {
        false
    }

    fn replace_live_backends(&self, _revision: u64, _backends: Vec<LiveBackendTarget>) {}
}

#[cfg(test)]
pub(crate) struct NoopCoordinatorReportEndpointSink;

#[cfg(test)]
impl CoordinatorReportEndpointSink for NoopCoordinatorReportEndpointSink {
    fn set_bound_port(&self, _port: u16) {}
}

#[cfg(test)]
pub(crate) struct NoopBackendTopologyPort;

#[cfg(test)]
impl BackendTopologyPort for NoopBackendTopologyPort {
    fn live_backends(&self) -> Vec<LiveBackendTarget> {
        Vec::new()
    }

    fn record_successful_fragment_submission(&self, _backend_idx: usize) {}

    fn install_metadata_store(
        &self,
        _store: Arc<dyn BackendTopologyMetadataStore>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn add_backend(&self, _endpoint: SocketAddr) -> Result<(), String> {
        Err("backend topology port is not installed".to_string())
    }

    fn drop_backend(&self, _endpoint: SocketAddr, _force: bool) -> Result<(), String> {
        Err("backend topology port is not installed".to_string())
    }

    fn show_backends(&self) -> Result<crate::runtime::query_result::QueryResult, String> {
        Err("backend topology port is not installed".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::CoordinatorReportEndpoint;

    #[test]
    fn coordinator_report_endpoint_accepts_advertised_dns_hostnames() {
        CoordinatorReportEndpoint::new("frontend.internal", 19070)
            .expect("advertised DNS hostname is a valid same-wire endpoint");
    }
}
