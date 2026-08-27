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

//! Process-boundary Native trust acceptance scenarios.
//!
//! These scenarios deliberately use the existing cross-process harness rather
//! than constructing a second mini-cluster. Raw HTTP/2 probes exercise the
//! listener-wide admission layer before routing, while public SQL proves the
//! normal FE-to-BE production path continues through the same 1FE+3BE launch.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::client;
use http::{Request, header};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    NativeTrustFixture, NativeTrustFixtureMode, ParticipantTerminalOutcomeKind,
    QueryLifecycleStructuredSnapshot, ServerHandle,
};
use novarocks_native_trust::{NativeEndpointConnector, NativeTrust};
use novarocks_types::NativeEndpoint;

const REQUIRED_BACKENDS: usize = 3;
const GRPC_UNAUTHENTICATED: u16 = 16;
const GRPC_UNIMPLEMENTED: u16 = 12;
const UNKNOWN_NATIVE_PATH: &str = "/novarocks.NovaRocksGrpc/Nwt3Unknown";
const HEARTBEAT_PATH: &str = "/novarocks.NovaRocksGrpc/Heartbeat";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(NativeTrustPositive {
            name: "native-trust/plaintext-ip",
            fixture: NativeTrustFixture::plaintext_ip(),
        }),
        Box::new(NativeTrustPositive {
            name: "native-trust/automatic-dns",
            fixture: NativeTrustFixture::automatic_dns(),
        }),
        Box::new(NativeTrustPositive {
            name: "native-trust/pem-ip",
            fixture: NativeTrustFixture::pem_ip(),
        }),
        Box::new(NativeTrustNegative::domain_mismatch()),
        Box::new(NativeTrustNegative::plaintext_tls_mismatch()),
        Box::new(NativeTrustNegative::automatic_pem_mismatch()),
    ]
}

struct NativeTrustPositive {
    name: &'static str,
    fixture: NativeTrustFixture,
}

impl Scenario for NativeTrustPositive {
    fn name(&self) -> &'static str {
        self.name
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.handle().native_trust_mode() == self.fixture.mode(),
            "Native trust harness launched a different transport profile"
        );
        let endpoint = context.handle().native_be_endpoint(0)?;
        let trust = context.handle().native_probe_trust()?;

        assert_authentication_order(context, &endpoint, &trust, self.fixture.mode())?;
        context.action(
            "proved listener-wide missing/invalid/valid JWT ordering on a real Native BE listener",
        );

        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect Native trust acceptance MySQL client")?,
        )?;
        let mut previous_execution = context
            .handle()
            .query_lifecycle_structured_snapshot()?
            .and_then(|snapshot| snapshot.execution_id);
        let mut snapshots = Vec::new();
        let deadline = context.deadline();
        for ordinal in 1..=3 {
            let rows: Vec<i64> = connection
                .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
                .with_context(|| {
                    format!("run distributed Native trust acceptance query {ordinal}")
                })?;
            ensure!(
                rows == vec![1, 2],
                "Native trust acceptance query {ordinal} returned unexpected rows: {rows:?}"
            );
            let snapshot = context
                .handle()
                .await_query_lifecycle_structured_snapshot_after(
                    previous_execution.as_deref(),
                    deadline,
                )
                .with_context(|| {
                    format!("read FE lifecycle snapshot for Native trust query {ordinal}")
                })?;
            previous_execution = snapshot.execution_id.clone();
            assert_successful_lifecycle(&snapshot)?;
            snapshots.push(snapshot);
        }
        for backend in 0..REQUIRED_BACKENDS {
            context
                .handle()
                .assert_be_log(backend, "NOVAROCKS_QUERY_INIT_APPLIED")?;
        }
        context.action(format!(
            "proved real 1FE+3BE topology, FE-to-BE lifecycle admission across every BE, and BE-to-FE terminal delivery with transport={:?}; terminal snapshots={}",
            self.fixture.mode()
            , snapshots.len()
        ));
        Ok(())
    }
}

struct NativeTrustNegative {
    name: &'static str,
    fixture: NativeTrustFixture,
    probe_mode: NativeTrustFixtureMode,
    wrong_reference: bool,
}

impl NativeTrustNegative {
    fn domain_mismatch() -> Self {
        Self {
            name: "native-trust/reject-jwt-domain-mismatch",
            fixture: NativeTrustFixture::automatic_dns(),
            probe_mode: NativeTrustFixtureMode::Automatic,
            wrong_reference: true,
        }
    }

    fn plaintext_tls_mismatch() -> Self {
        Self {
            name: "native-trust/reject-plaintext-tls-mismatch",
            fixture: NativeTrustFixture::plaintext_ip(),
            probe_mode: NativeTrustFixtureMode::Automatic,
            wrong_reference: false,
        }
    }

    fn automatic_pem_mismatch() -> Self {
        Self {
            name: "native-trust/reject-automatic-pem-mismatch",
            fixture: NativeTrustFixture::automatic_dns(),
            probe_mode: NativeTrustFixtureMode::Pem,
            wrong_reference: false,
        }
    }
}

impl Scenario for NativeTrustNegative {
    fn name(&self) -> &'static str {
        self.name
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let endpoint = if self.wrong_reference {
            NativeEndpoint::from_host_port("127.0.0.1", context.handle().runtime().be[0].grpc)
                .map_err(anyhow::Error::msg)
                .context("construct intentionally wrong automatic TLS reference")?
        } else {
            context.handle().native_be_endpoint(0)?
        };
        let connector = context
            .handle()
            .native_probe_connector(endpoint, self.probe_mode)?;
        let failure = connect_probe(connector).expect_err("mismatched Native transport must fail");
        let diagnostic = format!("{failure:#}");
        ensure!(
            !diagnostic.contains("Bearer "),
            "mismatched Native transport error leaked an authorization value"
        );
        context.action(format!(
            "rejected Native mismatch fixture={:?} probe={:?} before any authenticated RPC dispatch",
            self.fixture.mode(),
            self.probe_mode
        ));
        Ok(())
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    ensure!(
        count == REQUIRED_BACKENDS,
        "{} requires a real 1FE+3BE Native cluster, got 1FE+{count}BE",
        context.name()
    );
    context.action("verified real independent-process 1FE+3BE Native topology");
    Ok(())
}

fn assert_authentication_order(
    context: &mut ScenarioContext,
    endpoint: &NativeEndpoint,
    trust: &NativeTrust,
    mode: NativeTrustFixtureMode,
) -> Result<()> {
    let missing = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        UNKNOWN_NATIVE_PATH,
        None,
        None,
    )?;
    ensure!(
        missing.http_status == 200 && missing.grpc_status == Some(GRPC_UNAUTHENTICATED),
        "missing JWT must fail listener admission before unknown-path fallback, got {missing:?}"
    );
    let invalid = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        UNKNOWN_NATIVE_PATH,
        Some("Bearer invalid.native.token"),
        None,
    )?;
    ensure!(
        invalid.http_status == 200 && invalid.grpc_status == Some(GRPC_UNAUTHENTICATED),
        "invalid JWT must fail listener admission before unknown-path fallback, got {invalid:?}"
    );
    let authorization = authorization_header(trust)?;
    let valid_unknown = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        UNKNOWN_NATIVE_PATH,
        Some(&authorization),
        None,
    )?;
    ensure!(
        valid_unknown.http_status == 200 && valid_unknown.grpc_status == Some(GRPC_UNIMPLEMENTED),
        "valid JWT must reach the Native unknown-path fallback, got {valid_unknown:?}"
    );
    let valid_heartbeat = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        HEARTBEAT_PATH,
        Some(&authorization),
        Some(&[0, 0, 0, 0, 2, 0x08, 0x01]),
    )?;
    ensure!(
        valid_heartbeat.grpc_status != Some(GRPC_UNAUTHENTICATED),
        "valid JWT must reach representative Native RPC dispatch, got {valid_heartbeat:?}"
    );
    Ok(())
}

fn assert_successful_lifecycle(snapshot: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    ensure!(
        !snapshot.participant_outcomes.is_empty(),
        "Native trust query produced no BE lifecycle participant outcome"
    );
    ensure!(
        snapshot.error_source.is_none(),
        "Native trust query lifecycle reported an error source: {:?}",
        snapshot.error_source
    );
    ensure!(
        snapshot
            .participant_outcomes
            .iter()
            .all(|outcome| matches!(outcome, ParticipantTerminalOutcomeKind::Proof)),
        "Native trust query contained a non-proof terminal outcome: {:?}",
        snapshot.participant_outcomes
    );
    Ok(())
}

#[derive(Debug)]
struct GrpcProbe {
    http_status: u16,
    grpc_status: Option<u16>,
}

fn authorization_header(trust: &NativeTrust) -> Result<String> {
    let mut request = tonic::Request::new(());
    trust
        .apply_client_authorization(request.metadata_mut())
        .map_err(anyhow::Error::msg)
        .context("issue valid Native trust test JWT")?;
    request
        .metadata()
        .get("authorization")
        .context("Native trust interceptor did not add authorization metadata")?
        .to_str()
        .context("Native trust authorization metadata was not ASCII")
        .map(ToOwned::to_owned)
}

fn connect_probe(connector: NativeEndpointConnector) -> Result<()> {
    tokio::runtime::Runtime::new()
        .context("create Native mismatch probe runtime")?
        .block_on(async move {
            let stream = connector
                .connect()
                .await
                .map_err(anyhow::Error::msg)
                .context("connect mismatched Native transport")?;
            let (_sender, connection) = client::handshake(stream)
                .await
                .context("perform HTTP/2 handshake over mismatched Native transport")?;
            drop(connection);
            Ok(())
        })
}

fn raw_grpc_probe(
    connector: NativeEndpointConnector,
    path: &str,
    authorization: Option<&str>,
    body: Option<&[u8]>,
) -> Result<GrpcProbe> {
    tokio::runtime::Runtime::new()
        .context("create Native raw probe runtime")?
        .block_on(async move {
            let stream = connector
                .connect()
                .await
                .map_err(anyhow::Error::msg)
                .context("connect Native raw probe")?;
            let (mut sender, connection) = client::handshake(stream)
                .await
                .context("perform Native raw probe HTTP/2 handshake")?;
            let driver = tokio::spawn(async move { connection.await });
            let mut request = Request::builder()
                .method("POST")
                .uri(path)
                .header(header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers");
            if let Some(authorization) = authorization {
                request = request.header(header::AUTHORIZATION, authorization);
            }
            let request = request.body(()).context("build Native raw probe request")?;
            let end_of_stream = body.is_none();
            let (response, mut send_stream) = sender
                .send_request(request, end_of_stream)
                .context("send Native raw probe request")?;
            if let Some(body) = body {
                send_stream
                    .send_data(Bytes::copy_from_slice(body), true)
                    .context("send Native raw probe gRPC frame")?;
            }
            let response = response
                .await
                .context("receive Native raw probe response")?;
            let http_status = response.status().as_u16();
            let header_status = grpc_status(response.headers());
            let mut body = response.into_body();
            while body
                .data()
                .await
                .transpose()
                .context("read Native raw probe body")?
                .is_some()
            {}
            let trailer_status = body
                .trailers()
                .await
                .context("read Native raw probe trailers")?
                .as_ref()
                .and_then(grpc_status);
            // Native listeners are long-lived. The probe has received the
            // complete response, so waiting for the peer to close would turn
            // a successful keep-alive into a scenario timeout.
            driver.abort();
            let _ = driver.await;
            Ok(GrpcProbe {
                http_status,
                grpc_status: header_status.or(trailer_status),
            })
        })
}

fn grpc_status(headers: &http::HeaderMap) -> Option<u16> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_the_full_native_trust_transport_matrix() {
        let names = scenarios()
            .into_iter()
            .map(|scenario| scenario.name())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "native-trust/plaintext-ip",
                "native-trust/automatic-dns",
                "native-trust/pem-ip",
                "native-trust/reject-jwt-domain-mismatch",
                "native-trust/reject-plaintext-tls-mismatch",
                "native-trust/reject-automatic-pem-mismatch",
            ]
        );
    }
}
