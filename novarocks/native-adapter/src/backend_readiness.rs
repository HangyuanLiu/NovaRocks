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

//! Native endpoint readiness probing for Backend role composition.

use std::time::Duration;

use novarocks_types::NativeEndpoint;

use crate::BackendDataRuntime;

/// Confirms that this process's advertised Native endpoint is connectable.
///
/// The role keeps ownership of listener startup and cleanup. This transport
/// adapter owns only the authenticated channel acquisition and its timeout.
pub fn wait_for_backend_native_endpoint_ready(
    runtime: &BackendDataRuntime,
    endpoint: NativeEndpoint,
    timeout: Duration,
) -> Result<(), String> {
    let connector = runtime.native_transport().connector_for(endpoint.clone())?;
    runtime.block_on(async move {
        tokio::time::timeout(timeout, connector.connect())
            .await
            .map_err(|_| {
                format!(
                    "advertised Native endpoint {endpoint} did not become ready within {}ms",
                    timeout.as_millis()
                )
            })?
            .map(|_| ())
            .map_err(|error| {
                format!("advertised Native endpoint {endpoint} readiness failed: {error}")
            })
    })
}
