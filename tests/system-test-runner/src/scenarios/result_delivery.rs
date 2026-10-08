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

use super::result_delivery_baseline::await_idle;
use crate::actors::mysql_stream::{AsyncMysqlStream, TextColumnObservation, TextResultObservation};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::ServerHandle;
use novarocks_proto_models::{novarocks as proto, result as result_proto};
use prost::Message;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    topology: String,
    segment_bytes: u64,
    mysql_u24_payload_bytes: u64,
    mysql_client_max_packet_bytes: u32,
    scope: String,
    cases: Vec<WireCase>,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct WireCase {
    name: String,
    sql: String,
    expected_columns: u64,
    expected_schema: Vec<TextColumnObservation>,
    expected_rows: u64,
    expected_row_payload_bytes: u64,
    expected_packets: u64,
    expected_row_sha256: String,
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(WireBoundary(
            "result-delivery/many-small-rows-cross-segment",
        )),
        Box::new(WireBoundary("result-delivery/large-row-cross-u24")),
        Box::new(RootReadRefusal),
        Box::new(ContextRootRetention(RootProtocolMode::None)),
        Box::new(ContextRootRetention(RootProtocolMode::ZeroAck)),
        Box::new(ContextRootRetention(RootProtocolMode::FinalAck)),
    ]
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefusalManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    path: String,
    cases: Vec<RefusalCase>,
    health: HealthQuery,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RefusalCase {
    name: String,
    profile_id: u32,
    kind: String,
    wanted_sequence: u64,
    grpc_status: u16,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthQuery {
    sql: String,
    expected_row_sha256: String,
}

fn foreign_root_task() -> proto::TaskIdentity {
    proto::TaskIdentity {
        query_execution_id: Some(proto::QueryExecutionId {
            query_id: Some(novarocks_proto_models::common::UniqueId { hi: 1, lo: 2 }),
            attempt_id: 1,
        }),
        stage_id: 1,
        task_id: 1,
        // Deliberately foreign, but structurally valid. A valid V1 read
        // must reach exact-process refusal; each malformed field below
        // must instead be rejected before that same route lookup.
        backend_process_id: Some(proto::BackendProcessId {
            value: novarocks_types::BackendProcessId::new_v7()
                .to_bytes()
                .to_vec(),
        }),
    }
}

fn refusal_request(
    case: &RefusalCase,
    root_task: &proto::TaskIdentity,
) -> Result<proto::FetchRootResultRequest> {
    use result_proto::root_output_kind::Kind;
    let kind = match case.kind.as_str() {
        "client_rows_true" => Some(Kind::ClientRows(true)),
        "client_rows_false" => Some(Kind::ClientRows(false)),
        "absent" => None,
        "unknown_domain_999" => Some(Kind::InternalFacts(999)),
        _ => anyhow::bail!("unknown frozen root refusal case"),
    };
    Ok(proto::FetchRootResultRequest {
        root_task: Some(root_task.clone()),
        profile_id: case.profile_id,
        output_kind: kind.map(|kind| result_proto::RootOutputKind { kind: Some(kind) }),
        wanted_sequence: Some(case.wanted_sequence),
        consumed_sequence: 0,
        max_wait_millis: 100,
    })
}

struct RootReadRefusal;

impl Scenario for RootReadRefusal {
    fn name(&self) -> &'static str {
        "result-delivery/root-read-profile-kind-refusal"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "root refusal requires native 1FE+3BE"
        );
        let manifest: RefusalManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/root-read-refusal-freeze-v1.json"
        ))?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && !manifest.scope.is_empty()
                && manifest.cases.len() == 7
                && manifest.path == "/novarocks.NovaRocksGrpc/FetchRootResult",
            "unsupported root refusal manifest"
        );
        let epoch = Instant::now();
        await_idle(context, "root-refusal", "before", epoch)?;
        let root_task = foreign_root_task();
        let mut observations = Vec::new();
        for case in &manifest.cases {
            let payload = refusal_request(case, &root_task)?.encode_to_vec();
            let mut frame = vec![0];
            frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
            frame.extend_from_slice(&payload);
            let response =
                super::native_trust::bounded_authenticated_probe(context, &manifest.path, &frame)?;
            let matches =
                response.http_status == 200 && response.grpc_status == Some(case.grpc_status);
            observations.push(serde_json::json!({"expected":case,"response":response,"request_bytes":frame.len()}));
            std::fs::write(
                context
                    .scenario_root()
                    .join("root-read-refusal-observations.json"),
                serde_json::to_vec_pretty(&observations)?,
            )?;
            ensure!(
                matches,
                "root read refusal differs from frozen case {}",
                case.name
            );
        }
        let before = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        let timeout = context
            .remaining("root refusal follow-up query")?
            .min(Duration::from_secs(10));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let health = runtime.block_on(async {
            let mut stream =
                AsyncMysqlStream::connect(context.mysql_user(), context.mysql_port(), timeout)
                    .await?;
            Ok::<_, anyhow::Error>(
                stream
                    .observe_text_query(&manifest.health.sql, Duration::ZERO)
                    .await,
            )
        })?;
        std::fs::write(
            context.scenario_root().join("root-refusal-follow-up.json"),
            serde_json::to_vec_pretty(&health)?,
        )?;
        ensure!(
            health.error.is_none()
                && health.rows == 1
                && health.row_payload_bytes == 5
                && health.packets == 5
                && health.row_sha256 == manifest.health.expected_row_sha256
                && health.schema
                    == [TextColumnObservation {
                        name: "total".to_owned(),
                        mysql_type: 8
                    }],
            "native follow-up query failed after root refusals"
        );
        let after = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            before
                .iter()
                .zip(&after)
                .all(|(a, b)| a.is_finite() && b.is_finite() && b >= a)
                && before.iter().zip(&after).any(|(a, b)| b > a),
            "follow-up never created a native task"
        );
        await_idle(context, "root-refusal", "after", epoch)?;
        context.record_phase_observation(
            "root-read-refusal",
            1,
            1,
            1,
            "authenticated-native-root",
            7,
            "passed",
            BTreeMap::from([("refusal_probes", 7), ("health_rows", health.rows)]),
        )?;
        context.action("verified seven frozen authenticated root request refusals, exact native follow-up rows and public owner convergence");
        Ok(())
    }
}

struct WireBoundary(&'static str);

impl Scenario for WireBoundary {
    fn name(&self) -> &'static str {
        self.0
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "wire boundary requires native 1FE+3BE"
        );
        let manifest: Manifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/result-delivery-wire-boundary-v4.json"
        ))?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && manifest.segment_bytes == 1_048_576
                && manifest.mysql_u24_payload_bytes == 0x00ff_ffff
                && manifest.mysql_client_max_packet_bytes == 67_108_864
                && !manifest.scope.is_empty()
                && manifest.cases.len() == 2,
            "unsupported frozen wire boundary manifest"
        );
        let case = manifest
            .cases
            .iter()
            .find(|case| case.name == self.name())
            .ok_or_else(|| anyhow::anyhow!("wire boundary case missing from frozen manifest"))?;
        ensure!(
            case.expected_row_payload_bytes > manifest.segment_bytes,
            "wire boundary must cross a native segment"
        );
        ensure!(
            case.expected_schema.len() as u64 == case.expected_columns,
            "frozen schema count mismatch"
        );
        let epoch = Instant::now();
        await_idle(context, "wire-boundary", "before", epoch)?;
        let before: Vec<_> = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<_>>()?;
        let timeout = context
            .remaining("wire boundary query")?
            .min(Duration::from_secs(30));
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let observation = runtime.block_on(async {
            let mut stream = AsyncMysqlStream::connect_with_max_packet_bytes(
                &user,
                port,
                timeout,
                manifest.mysql_client_max_packet_bytes,
            )
            .await?;
            Ok::<_, anyhow::Error>(stream.observe_text_query(&case.sql, Duration::ZERO).await)
        })?;
        std::fs::write(
            context
                .scenario_root()
                .join("wire-boundary-observation.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version":1,"expected":case,"observation":observation
            }))?,
        )?;
        ensure!(
            observation.error.is_none(),
            "wire query failed: {:?}",
            observation.error
        );
        ensure!(
            observation.columns == case.expected_columns
                && observation.schema == case.expected_schema
                && observation.rows == case.expected_rows,
            "wire result schema or row count disagrees with independent oracle"
        );
        ensure!(
            observation.row_payload_bytes == case.expected_row_payload_bytes
                && observation.packets == case.expected_packets,
            "wire payload length or U24 packet count disagrees with independent oracle"
        );
        ensure!(
            observation.row_sha256 == case.expected_row_sha256,
            "wire row bytes or row order disagree with independent oracle"
        );
        let after: Vec<_> = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<_>>()?;
        let mut created = 0_u64;
        for (before, after) in before.iter().zip(&after) {
            ensure!(
                before.is_finite() && after.is_finite() && after >= before,
                "invalid native task creation counter"
            );
            created += (after - before) as u64;
        }
        ensure!(
            created > 0,
            "wire query never executed a native backend task"
        );
        context.record_phase_observation(
            "wire-correctness",
            1,
            1,
            1,
            "public-mysql-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", observation.rows),
                ("row_payload_bytes", observation.row_payload_bytes),
                ("packets", observation.packets),
                ("native_tasks_created", created),
            ]),
        )?;
        context.action("verified native row bytes, order, packet sequence, one schema and terminal success against an independent frozen oracle");
        await_idle(context, "wire-boundary", "after", epoch)?;
        context.action("observed two consecutive idle FE governance/window and BE reservation/ingress snapshots after writer exit");
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    sql: String,
    segment_bytes: u64,
    native_row_bytes: u64,
    client_receive_buffer_bytes: u32,
    max_applied_receive_buffer_bytes: u32,
    mysql_client_max_packet_bytes: u32,
    query_observation_ms: u64,
    metadata_wait_ms: u64,
    held_phase_ms: u64,
    phase_sample_interval_ms: u64,
    phase_max_samples: usize,
    expected_columns: u64,
    expected_rows: u64,
    expected_row_payload_bytes: u64,
    expected_packets: u64,
    expected_schema: Vec<TextColumnObservation>,
    expected_row_sha256: String,
}

const ROOT_RESOURCES: [&str; 14] = [
    "channels",
    "terminal_task_records",
    "producers_running",
    "producers_exited",
    "ends_published",
    "ends_acknowledged",
    "sealed",
    "data_positions",
    "payload_bytes",
    "segments",
    "deliveries",
    "retained_reservations",
    "metadata_holders",
    "metadata_bytes",
];

pub(super) fn root_census(rows: &[serde_json::Value]) -> Result<Option<BTreeMap<String, u64>>> {
    use super::result_delivery_baseline::metric;
    let available = metric(
        rows,
        "novarocks_backend_root_ownership_snapshot_available",
        &[],
    )?;
    ensure!(available <= 1, "invalid root census availability");
    if available == 0 {
        ensure!(
            !rows
                .iter()
                .any(|row| row["tags"]["metric"] == "novarocks_backend_root_ownership"),
            "unavailable root census published stale ownership"
        );
        return Ok(None);
    }
    ROOT_RESOURCES
        .iter()
        .map(|name| {
            Ok((
                (*name).to_owned(),
                metric(
                    rows,
                    "novarocks_backend_root_ownership",
                    &[("resource", name)],
                )?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()
        .map(Some)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RootProtocolMode {
    None,
    ZeroAck,
    FinalAck,
}
struct ContextRootRetention(RootProtocolMode);

impl Scenario for ContextRootRetention {
    fn name(&self) -> &'static str {
        match self.0 {
            RootProtocolMode::None => "result-delivery/producer-exit-context-retention",
            RootProtocolMode::ZeroAck => "result-delivery/installed-root-zero-ack-normal-wire",
            RootProtocolMode::FinalAck => {
                "result-delivery/installed-root-replay-final-ack-interference"
            }
        }
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "root retention requires native 1FE+3BE"
        );
        let manifest: RetentionManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/root-context-retention-freeze-v1.json"
        ))?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && !manifest.scope.is_empty()
                && manifest.segment_bytes == 1048576
                && manifest.native_row_bytes == manifest.expected_row_payload_bytes + 4
                && manifest.native_row_bytes > manifest.segment_bytes
                && manifest.native_row_bytes <= 2 * manifest.segment_bytes
                && manifest.query_observation_ms == 20000
                && manifest.held_phase_ms == 5000
                && manifest.metadata_wait_ms == 5000
                && manifest.phase_sample_interval_ms == 100
                && manifest.phase_max_samples == 51,
            "unsupported root retention freeze"
        );
        let epoch = Instant::now();
        await_idle(context, "root-retention", "before", epoch)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let mut stream = runtime.block_on(AsyncMysqlStream::connect_with_receive_buffer(
            context.mysql_user(),
            context.mysql_port(),
            Duration::from_millis(manifest.query_observation_ms),
            manifest.mysql_client_max_packet_bytes,
            manifest.client_receive_buffer_bytes,
        ))?;
        let connection_id = stream.connection_id()?;
        let before_logs = (0..3)
            .map(|index| context.handle().be_log_contents(index))
            .collect::<Result<Vec<_>>>()?;
        let applied = stream
            .receive_buffer_bytes()
            .context("missing applied client receive buffer")?;
        ensure!(
            applied > 0 && applied <= manifest.max_applied_receive_buffer_bytes,
            "OS client receive window exceeds frozen probe bound"
        );
        let (ready, metadata_ready) = tokio::sync::oneshot::channel();
        let (resume, resume_read) = tokio::sync::oneshot::channel();
        let sql = manifest.sql.clone();
        let job = runtime.spawn(async move {
            stream
                .observe_text_query_with_metadata_pause(
                    &sql,
                    Duration::ZERO,
                    Some((ready, resume_read)),
                )
                .await
        });
        let mut samples = Vec::new();
        let held = (|| -> Result<()> {
            runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_millis(manifest.metadata_wait_ms),
                        metadata_ready,
                    )
                    .await
                })
                .context("root retention metadata deadline")??;
            let deadline = Instant::now() + Duration::from_millis(manifest.held_phase_ms);
            let ports: Vec<_> = context
                .handle()
                .runtime()
                .be
                .iter()
                .map(|be| be.http)
                .collect();
            let mut consecutive = 0;
            loop {
                context.remaining("held root census")?;
                ensure!(
                    Instant::now() < deadline && samples.len() < manifest.phase_max_samples,
                    "root producer never retired with two context-held Data positions and End"
                );
                ensure!(
                    !job.is_finished(),
                    "client actor exited before held-root observation"
                );
                let mut roots = Vec::new();
                for port in &ports {
                    let client = reqwest::blocking::Client::builder()
                        .timeout(
                            deadline
                                .saturating_duration_since(Instant::now())
                                .min(Duration::from_secs(1)),
                        )
                        .build()?;
                    let rows: serde_json::Value = client
                        .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                        .send()?
                        .error_for_status()?
                        .json()?;
                    roots.push(root_census(
                        rows.as_array()
                            .context("invalid root census metric array")?,
                    )?);
                }
                let occupied: Vec<_> = roots
                    .iter()
                    .filter_map(Option::as_ref)
                    .filter(|root| root["channels"] != 0)
                    .collect();
                let matches = roots.iter().all(Option::is_some)
                    && occupied.len() == 1
                    && occupied[0]["channels"] == 1
                    && occupied[0]["terminal_task_records"] == 1
                    && occupied[0]["producers_running"] == 0
                    && occupied[0]["producers_exited"] == 1
                    && occupied[0]["ends_published"] == 1
                    && occupied[0]["ends_acknowledged"] == 0
                    && occupied[0]["sealed"] == 0
                    && occupied[0]["data_positions"] == 2
                    && occupied[0]["payload_bytes"] == manifest.native_row_bytes
                    && occupied[0]["segments"] == 2
                    && occupied[0]["metadata_bytes"] > 0;
                samples.push(
                    serde_json::json!({"elapsed_micros":epoch.elapsed().as_micros(),
                    "applied_receive_buffer_bytes":applied,"roots":roots,"matches":matches}),
                );
                std::fs::write(
                    context
                        .scenario_root()
                        .join("root-context-held-census.json"),
                    serde_json::to_vec_pretty(&samples)?,
                )?;
                ensure!(
                    Instant::now() < deadline,
                    "held root census exceeded absolute phase deadline"
                );
                consecutive = if matches { consecutive + 1 } else { 0 };
                if consecutive == 2 {
                    if self.0 != RootProtocolMode::None {
                        let occupied_be = roots
                            .iter()
                            .position(|root| {
                                root.as_ref().is_some_and(|root| root["channels"] == 1)
                            })
                            .context("held root has no exact backend")?;
                        installed_root_protocol(context, &before_logs, occupied_be, self.0, &job)?;
                        if self.0 == RootProtocolMode::FinalAck {
                            let kill_deadline = Instant::now()
                                + context
                                    .remaining("interference KILL")?
                                    .min(Duration::from_secs(2));
                            context
                                .handle()
                                .kill_query_until(connection_id, kill_deadline)?;
                            ensure!(
                                Instant::now() < kill_deadline,
                                "interference KILL exceeded deadline"
                            );
                        }
                    }
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(manifest.phase_sample_interval_ms));
            }
        })();
        // Resume even a failed probe, then join and preserve the real wire
        // outcome before returning its failure. No background actor is hidden.
        let _ = resume.send(());
        let observation = runtime.block_on(job).context("join paused root client")?;
        std::fs::write(
            context
                .scenario_root()
                .join("root-context-retention-wire.json"),
            serde_json::to_vec_pretty(&observation)?,
        )?;
        held?;
        if self.0 != RootProtocolMode::FinalAck {
            ensure!(
                observation.error.is_none()
                    && observation.columns == manifest.expected_columns
                    && observation.schema == manifest.expected_schema
                    && observation.rows == manifest.expected_rows
                    && observation.row_payload_bytes == manifest.expected_row_payload_bytes
                    && observation.packets == manifest.expected_packets
                    && observation.row_sha256 == manifest.expected_row_sha256,
                "resumed client bytes differ from independent frozen oracle"
            );
        } else {
            ensure!(
                observation.columns == manifest.expected_columns
                    && observation.schema == manifest.expected_schema,
                "protocol interference wire metadata changed"
            );
        }
        await_idle(context, "root-retention", "after", epoch)?;
        context.record_phase_observation(
            "producer-retired-context-held-root",
            1,
            1,
            1,
            "public-mysql-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", observation.rows),
                ("held_data_positions", 2),
                ("census_samples", u64::try_from(samples.len())?),
            ]),
        )?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledProtocolManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    retention_input_sha256: String,
    phase_ms: u64,
    max_wait_millis: u64,
    correction_of: BTreeMap<String, String>,
    request_frame_bytes: usize,
    response_data_bytes: usize,
    max_identity_candidates: usize,
    data1_sha256: String,
    data2_sha256: String,
    data1_bytes: usize,
    data2_bytes: usize,
    end_sequence: u64,
    output_rows: u64,
    operations: Vec<String>,
    cases: Vec<String>,
}

fn installed_root_request(
    task: &proto::TaskIdentity,
    wanted_sequence: Option<u64>,
    consumed_sequence: u64,
    max_wait_millis: u64,
) -> proto::FetchRootResultRequest {
    proto::FetchRootResultRequest {
        root_task: Some(task.clone()),
        profile_id: 1,
        output_kind: Some(result_proto::RootOutputKind {
            kind: Some(result_proto::root_output_kind::Kind::ClientRows(true)),
        }),
        wanted_sequence,
        consumed_sequence,
        max_wait_millis,
    }
}

fn installed_root_protocol(
    context: &mut ScenarioContext,
    before_logs: &[String],
    backend: usize,
    mode: RootProtocolMode,
    job: &tokio::task::JoinHandle<TextResultObservation>,
) -> Result<()> {
    use novarocks_execution_contract::root_result::RootReadOutcome;
    use sha2::{Digest, Sha256};
    let freeze: InstalledProtocolManifest = serde_json::from_str(include_str!(
        "../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v2.json"
    ))?;
    let retention_digest = format!(
        "{:x}",
        Sha256::digest(include_bytes!(
            "../../../../docs/testing/mem-1-m07/inputs/root-context-retention-freeze-v1.json"
        ))
    );
    ensure!(
        freeze.schema_version == 2
            && freeze.topology == "1FE+3BE"
            && !freeze.scope.is_empty()
            && freeze.retention_input_sha256 == retention_digest
            && freeze.phase_ms == 5000
            && freeze.max_wait_millis == 100
            && freeze.correction_of.len() == 3
            && freeze.correction_of["path"]
                == "docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v1.json"
            && !freeze.correction_of["reason"].is_empty()
            && freeze.correction_of["sha256"]
                == format!(
                    "{:x}",
                    Sha256::digest(include_bytes!(
                        "../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v1.json"
                    ))
                )
            && freeze.max_identity_candidates == 8
            && freeze.request_frame_bytes == 4096
            && freeze.response_data_bytes == 1048576 + 4096
            && freeze.data1_bytes == 1048576
            && freeze.data2_bytes == 8
            && freeze.end_sequence == 3
            && freeze.output_rows == 1
            && freeze.cases
                == [
                    "result-delivery/installed-root-zero-ack-normal-wire",
                    "result-delivery/installed-root-replay-final-ack-interference"
                ]
            && freeze.operations
                == [
                    "ack0",
                    "ack0",
                    "fetch1",
                    "fetch2-end3",
                    "replay1",
                    "ack3",
                    "ack3",
                    "retired1"
                ],
        "unsupported installed-root protocol freeze"
    );
    let deadline = Instant::now() + Duration::from_millis(freeze.phase_ms);
    let after_logs = (0..3)
        .map(|index| context.handle().be_log_contents(index))
        .collect::<Result<Vec<_>>>()?;
    let candidates = super::result_delivery_root_protocol::parse_created_task_candidates(
        before_logs,
        &after_logs,
        backend,
    )?;
    ensure!(
        candidates.len() <= freeze.max_identity_candidates,
        "root identity candidates exceeded freeze"
    );
    let mut observations = Vec::new();
    let mut installed = None;
    for candidate in candidates {
        ensure!(
            Instant::now() < deadline && !job.is_finished(),
            "root discovery expired or actor exited"
        );
        let request = installed_root_request(&candidate, None, 0, freeze.max_wait_millis);
        let (reply, mut observation) = super::result_delivery_root_protocol::probe_candidate(
            context, backend, &request, 0, deadline,
        )?;
        observation["operation"] = serde_json::json!("discover-exact-root-by-zero-ack");
        observations.push(observation);
        std::fs::write(
            context.scenario_root().join("installed-root-protocol.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
        if let Some(reply) = reply {
            ensure!(
                matches!(reply.outcome, RootReadOutcome::AckOnly) && reply.accepted_consumed == 0,
                "root discovery changed or did not acknowledge the zero frontier"
            );
            ensure!(installed.is_none(), "more than one actual task owns a root");
            installed = Some(candidate);
        }
    }
    let task = installed.context("no actual task identity routes to the held installed root")?;
    let mut proven = 0;
    let count = if mode == RootProtocolMode::ZeroAck {
        2
    } else {
        freeze.operations.len()
    };
    for (index, operation) in freeze.operations.iter().take(count).enumerate() {
        ensure!(
            Instant::now() < deadline && !job.is_finished(),
            "installed-root protocol phase expired or actor exited"
        );
        let (wanted, consumed) = match operation.as_str() {
            "ack0" => (None, 0),
            "fetch1" | "replay1" => (Some(1), 0),
            "fetch2-end3" => (Some(2), 0),
            "ack3" => (None, proven),
            "retired1" => (Some(1), proven),
            _ => anyhow::bail!("unknown frozen root operation"),
        };
        let request = installed_root_request(&task, wanted, consumed, freeze.max_wait_millis);
        let (reply, mut observation) = super::result_delivery_root_protocol::probe(
            context, backend, &request, proven, deadline,
        )?;
        let matches = match (&reply.outcome, operation.as_str()) {
            (RootReadOutcome::AckOnly, "ack0") => reply.accepted_consumed == 0,
            (RootReadOutcome::AckOnly, "ack3") => {
                proven == freeze.end_sequence && reply.accepted_consumed == proven
            }
            (RootReadOutcome::Retired, "retired1") => {
                proven == freeze.end_sequence && reply.accepted_consumed == proven
            }
            (RootReadOutcome::Data(data), "fetch1" | "replay1") => {
                let digest = format!("{:x}", Sha256::digest(data.body()));
                data.sequence().get() == 1
                    && data.end_after_data().is_none()
                    && data.body().len() == freeze.data1_bytes
                    && digest == freeze.data1_sha256
                    && reply.accepted_consumed == 0
            }
            (RootReadOutcome::Data(data), "fetch2-end3") => {
                let end = data
                    .end_after_data()
                    .context("frozen final Data has no End")?;
                let matches = data.sequence().get() == 2
                    && data.body().len() == freeze.data2_bytes
                    && format!("{:x}", Sha256::digest(data.body())) == freeze.data2_sha256
                    && end.sequence.get() == freeze.end_sequence
                    && end.output_rows == freeze.output_rows
                    && reply.accepted_consumed == 0;
                if matches {
                    proven = end.sequence.get();
                }
                matches
            }
            _ => false,
        };
        observation["operation"] = serde_json::json!(operation);
        observation["matches"] = serde_json::json!(matches);
        observation["proven_delivered_consumed"] = serde_json::json!(proven);
        observations.push(observation);
        std::fs::write(
            context.scenario_root().join("installed-root-protocol.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
        ensure!(
            matches,
            "installed root operation {operation} differs from frozen oracle"
        );
        // Each typed reply owns only client-side decoded bytes. Release it
        // before another RPC; it never stands in for a backend holder receipt.
        drop(reply);
        let client = reqwest::blocking::Client::builder()
            .timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1)),
            )
            .build()?;
        let rows: serde_json::Value = client
            .get(format!(
                "http://127.0.0.1:{}/metrics?type=json",
                context.handle().runtime().be[backend].http
            ))
            .send()?
            .error_for_status()?
            .json()?;
        let census = root_census(rows.as_array().context("invalid installed-root census")?)?
            .context("installed-root census unavailable")?;
        let retired = index >= 5;
        ensure!(
            census["channels"] == 1
                && census["terminal_task_records"] == 1
                && census["producers_exited"] == 1
                && census["producers_running"] == 0
                && census["ends_published"] == 1
                && census["sealed"] == 0
                && census["ends_acknowledged"] == u64::from(retired)
                && census["data_positions"] == if retired { 0 } else { 2 }
                && census["payload_bytes"] == if retired { 0 } else { 1048584 },
            "installed root changed outside its proven ACK frontier"
        );
        let census_path = context
            .scenario_root()
            .join(format!("installed-root-census-{index}.json"));
        std::fs::write(census_path, serde_json::to_vec_pretty(&census)?)?;
        ensure!(
            Instant::now() < deadline && !job.is_finished(),
            "installed-root chain exceeded deadline or paused actor exited"
        );
    }
    Ok(())
}

#[cfg(test)]
mod census_tests {
    #[test]
    fn frozen_installed_root_requests_pass_the_production_decoder() {
        let freeze: super::InstalledProtocolManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v2.json"
        ))
        .unwrap();
        let task = super::foreign_root_task();
        for (wanted, consumed) in [
            (None, 0),
            (Some(1), 0),
            (Some(2), 0),
            (None, 3),
            (Some(1), 3),
        ] {
            let request =
                super::installed_root_request(&task, wanted, consumed, freeze.max_wait_millis);
            assert!(
                novarocks_task_codec::root_result::decode_read(
                    &request,
                    novarocks_proto_codec::FieldPath::root("frozen_probe")
                )
                .is_ok()
            );
        }
        let invalid = super::installed_root_request(&task, None, 0, 0);
        assert!(
            novarocks_task_codec::root_result::decode_read(
                &invalid,
                novarocks_proto_codec::FieldPath::root("invalid_zero_wait")
            )
            .is_err()
        );
    }

    use super::*;
    #[test]
    fn census_missing_partial_duplicate_stale_and_invalid_samples_are_refused() {
        use serde_json::json;
        let available = json!({"tags":{"metric":"novarocks_backend_root_ownership_snapshot_available"},"value":1});
        assert!(root_census(&[]).is_err());
        assert!(root_census(std::slice::from_ref(&available)).is_err());
        let mut rows = vec![available.clone()];
        rows.extend(ROOT_RESOURCES.iter().map(|name| json!({"tags":{"metric":"novarocks_backend_root_ownership","resource":name},"value":0})));
        assert!(root_census(&rows).unwrap().is_some());
        rows.push(rows[1].clone());
        assert!(root_census(&rows).is_err());
        rows.pop();
        rows[0]["value"] = json!(0);
        assert!(root_census(&rows).is_err());
        assert_eq!(root_census(&rows[..1]).unwrap(), None);
        rows[0] = available;
        rows[1]["value"] = json!(-1);
        assert!(root_census(&rows).is_err());
    }
}
