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

#[cfg(test)]
#[path = "backend_readiness_worker_tests.rs"]
mod worker_tests;

/// Confirms an HTTP/2 channel to this process's advertised Native endpoint.
///
/// The role keeps ownership of listener startup and cleanup. This transport
/// adapter owns transport admission, pool acquisition, and its timeout. It
/// does not submit an application RPC or attest its JWT authentication.
pub fn wait_for_backend_native_endpoint_ready(
    runtime: &BackendDataRuntime,
    endpoint: NativeEndpoint,
    class: crate::native_transport_capacity::TransportClass,
    timeout: Duration,
) -> Result<(), String> {
    let original_worker = runtime
        .channels()
        .transient_original_channel_worker()
        .map_err(|error| format!("Native readiness Worker election refused: {error}"))?;
    let connector = runtime.native_transport().connector_for(endpoint.clone())?;
    let channel_endpoint = crate::native_client::capacity_endpoint(runtime, &endpoint, class)?;
    let connector = tower::service_fn(move |_| {
        let connector = connector.clone();
        async move {
            connector
                .connect()
                .await
                .map(hyper_util::rt::TokioIo::new)
                .map_err(std::io::Error::other)
        }
    });
    runtime.block_on(async move {
        let connect = async move {
            match original_worker {
                Some(worker) => {
                    channel_endpoint
                        .connect_with_connector_and_original_worker(connector, worker)
                        .await
                }
                None => channel_endpoint.connect_with_connector(connector).await,
            }
        };
        let channel = tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| {
                format!(
                    "advertised Native endpoint {endpoint} did not become ready within {}ms",
                    timeout.as_millis()
                )
            })?
            .map_err(|error| {
                format!("advertised Native endpoint {endpoint} readiness failed: {error}")
            })?;
        // The actual connection task retains its pools until physical exit.
        drop(channel);
        Ok(())
    })
}
