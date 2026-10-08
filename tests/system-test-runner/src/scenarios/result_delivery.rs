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
use crate::actors::mysql_stream::{AsyncMysqlStream, TextColumnObservation};
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
        Box::new(ContextRootRetention),
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
                .to_vec()
                .into(),
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

fn root_census(rows: &[serde_json::Value]) -> Result<Option<BTreeMap<String, u64>>> {
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

struct ContextRootRetention;

impl Scenario for ContextRootRetention {
    fn name(&self) -> &'static str {
        "result-delivery/producer-exit-context-retention"
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

#[cfg(test)]
mod census_tests {
    use super::*;
    #[test]
    fn census_missing_partial_duplicate_stale_and_invalid_samples_are_refused() {
        use serde_json::json;
        let available = json!({"tags":{"metric":"novarocks_backend_root_ownership_snapshot_available"},"value":1});
        assert!(root_census(&[]).is_err());
        assert!(root_census(&[available.clone()]).is_err());
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
