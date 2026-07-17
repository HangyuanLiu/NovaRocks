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

use crate::coordinator::ports::CoordinatorObserver;
use crate::runtime::endpoint::RuntimeEndpoint;

pub(crate) struct PrometheusCoordinatorObserver;

impl CoordinatorObserver for PrometheusCoordinatorObserver {
    fn fragment_scheduled(&self) {
        crate::service::metrics_http::observe_fragment_scheduled();
    }
}

pub(crate) fn coordinator_report_endpoint() -> Result<RuntimeEndpoint, String> {
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("cannot read coordinator config: {e}"))?;
    let advertised_host = crate::common::network::advertise_host().ok();
    let bound_port = crate::service::grpc_server::grpc_server_bound_port().ok();
    select_coordinator_report_endpoint(
        &cfg.server.host,
        cfg.server.grpc_port,
        advertised_host.as_deref(),
        bound_port,
    )
}

fn select_coordinator_report_endpoint(
    configured_host: &str,
    configured_port: u16,
    advertised_host: Option<&str>,
    bound_port: Option<u16>,
) -> Result<RuntimeEndpoint, String> {
    RuntimeEndpoint::new(
        advertised_host.unwrap_or(configured_host),
        i32::from(bound_port.unwrap_or(configured_port)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_endpoint_prefers_advertised_host_and_bound_port() {
        let endpoint = select_coordinator_report_endpoint(
            "configured.internal",
            9070,
            Some("advertised.internal"),
            Some(19070),
        )
        .expect("report endpoint");

        assert_eq!(endpoint.host(), "advertised.internal");
        assert_eq!(endpoint.port(), 19070);
    }

    #[test]
    fn report_endpoint_falls_back_to_configured_host_and_port() {
        let endpoint = select_coordinator_report_endpoint("configured.internal", 9070, None, None)
            .expect("report endpoint");

        assert_eq!(endpoint.host(), "configured.internal");
        assert_eq!(endpoint.port(), 9070);
    }
}
