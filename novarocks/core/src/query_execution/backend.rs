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

use crate::runtime::endpoint::RuntimeEndpoint;

pub fn record_successful_fragment_submission(backend_idx: usize) {
    crate::service::metrics_http::observe_fragment_scheduled();
    if let Some(membership) = crate::query_execution::backend_registry::cluster_membership() {
        membership.record_scheduled_fragment(
            backend_idx as crate::query_execution::backend_registry::BeId,
        );
    }
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
mod tests {
    use super::CoordinatorReportEndpoint;

    #[test]
    fn coordinator_report_endpoint_accepts_advertised_dns_hostnames() {
        CoordinatorReportEndpoint::new("frontend.internal", 19070)
            .expect("advertised DNS hostname is a valid same-wire endpoint");
    }
}
