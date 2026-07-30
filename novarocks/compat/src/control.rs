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

//! Per-compat-host control facts learned from FE heartbeat traffic.
//!
//! This state deliberately belongs to a compat application instance.  It is
//! not a process-wide registry: callers pass an `Arc<FrontendControlState>`
//! to the services that need FE control facts.

use std::sync::Mutex;

use novarocks::thrift::types;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiskReportReservation {
    pub(crate) generation: u64,
    pub(crate) fe_addr: types::TNetworkAddress,
    pub(crate) backend_host: String,
}

#[derive(Debug, Default)]
struct ControlFacts {
    fe_addr: Option<types::TNetworkAddress>,
    fe_http_port: Option<i32>,
    backend_host: Option<String>,
    disk_report_generation: u64,
    disk_report_in_flight: bool,
    disk_reported: bool,
}

/// Mutable control-plane observations scoped to one compat application host.
#[derive(Debug, Default)]
pub(crate) struct FrontendControlState {
    facts: Mutex<ControlFacts>,
}

impl FrontendControlState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Records the FE endpoint and local backend host learned from a heartbeat.
    ///
    /// A changed FE endpoint invalidates an older disk report.  Completion from
    /// that older endpoint is ignored by generation when it eventually returns.
    pub(crate) fn observe_heartbeat(
        &self,
        fe_addr: &types::TNetworkAddress,
        fe_http_port: Option<i32>,
        backend_host: String,
    ) {
        let Ok(mut facts) = self.facts.lock() else {
            return;
        };

        let changed = facts.fe_addr.as_ref().is_none_or(|current| {
            current.hostname != fe_addr.hostname || current.port != fe_addr.port
        });
        if changed {
            facts.fe_addr = Some(fe_addr.clone());
            facts.disk_report_generation = facts.disk_report_generation.wrapping_add(1);
            facts.disk_report_in_flight = false;
            facts.disk_reported = false;
        }
        if let Some(port) = fe_http_port.filter(|port| *port > 0) {
            facts.fe_http_port = Some(port);
        }
        facts.backend_host = Some(backend_host);
    }

    pub(crate) fn latest_fe_addr(&self) -> Option<types::TNetworkAddress> {
        self.facts.lock().ok()?.fe_addr.clone()
    }

    pub(crate) fn latest_fe_http_port(&self) -> Option<i32> {
        self.facts.lock().ok()?.fe_http_port
    }

    pub(crate) fn latest_backend_host(&self) -> Option<String> {
        self.facts.lock().ok()?.backend_host.clone()
    }

    /// Reserves exactly one disk report for the current FE endpoint.
    pub(crate) fn begin_disk_report(&self) -> Option<DiskReportReservation> {
        let Ok(mut facts) = self.facts.lock() else {
            return None;
        };
        if facts.disk_reported || facts.disk_report_in_flight {
            return None;
        }
        let fe_addr = facts.fe_addr.clone()?;
        let backend_host = facts.backend_host.clone()?;
        facts.disk_report_in_flight = true;
        Some(DiskReportReservation {
            generation: facts.disk_report_generation,
            fe_addr,
            backend_host,
        })
    }

    pub(crate) fn finish_disk_report(&self, reservation: &DiskReportReservation, succeeded: bool) {
        let Ok(mut facts) = self.facts.lock() else {
            return;
        };
        if facts.disk_report_generation != reservation.generation
            || facts.fe_addr.as_ref() != Some(&reservation.fe_addr)
        {
            return;
        }
        facts.disk_report_in_flight = false;
        facts.disk_reported = succeeded;
    }
}

#[cfg(test)]
mod tests {
    use super::FrontendControlState;
    use novarocks::thrift::types::TNetworkAddress;

    fn address(hostname: &str, port: i32) -> TNetworkAddress {
        TNetworkAddress::new(hostname.to_string(), port)
    }

    #[test]
    fn suppresses_duplicate_disk_report_for_same_fe() {
        let state = FrontendControlState::new();
        let fe = address("fe-a", 9020);
        state.observe_heartbeat(&fe, Some(8030), "be-a".to_string());

        let reservation = state.begin_disk_report().expect("first report");
        assert!(state.begin_disk_report().is_none());
        state.finish_disk_report(&reservation, true);
        assert!(state.begin_disk_report().is_none());
    }

    #[test]
    fn ignores_completion_from_replaced_fe() {
        let state = FrontendControlState::new();
        let first = address("fe-a", 9020);
        let second = address("fe-b", 9020);
        state.observe_heartbeat(&first, None, "be-a".to_string());
        let old = state.begin_disk_report().expect("first report");

        state.observe_heartbeat(&second, Some(8030), "be-a".to_string());
        state.finish_disk_report(&old, true);

        let next = state.begin_disk_report().expect("new FE report");
        assert_eq!(next.fe_addr, second);
        assert_eq!(state.latest_fe_http_port(), Some(8030));
    }
}
