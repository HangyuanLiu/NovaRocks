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
    let host = crate::common::network::advertise_host().unwrap_or_else(|_| cfg.server.host.clone());
    let port =
        crate::service::grpc_server::grpc_server_bound_port().unwrap_or(cfg.server.grpc_port);
    RuntimeEndpoint::new(host, port as i32)
}
