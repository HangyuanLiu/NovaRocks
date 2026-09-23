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

//! Native UEA-4A-2 G0 calibration. The pilot records real Iceberg reads and
//! process samples. A formal candidate requires I03 operation attribution.

use super::connector::require_three_backends;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::process_resources::{ProcessResourceMonitor, ProcessResourceSample};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const WORKLOAD_ENV: &str = "NOVAROCKS_UEA4A2_WORKLOAD_MANIFEST";
const FIXTURE_ENV: &str = "NOVAROCKS_UEA4A2_FIXTURE_MANIFEST";
const GROUP_ENV: &str = "NOVAROCKS_UEA4A2_GROUP";
const PILOT_ENV: &str = "NOVAROCKS_UEA4A2_BASELINE_PILOT";
const CALIBRATION_SECONDS_ENV: &str = "NOVAROCKS_UEA4A2_CALIBRATION_SECONDS";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_SECRET_ACCESS_KEY";
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const PILOT_IDLE: Duration = Duration::from_secs(2);
const PILOT_POST_DRAIN: Duration = Duration::from_secs(2);
const PROXY_LOG_POLL: Duration = Duration::from_millis(100);
const PROXY_LOG_INTERVAL: Duration = Duration::from_secs(1);
const PROXY_LOG_REQUEST_INTERVAL: u64 = 4096;
const PILOT_CACHE_OVERLAY: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";

#[derive(Deserialize)]
struct Workload {
    schema_version: u32,
    scenario: String,
    topology: Topology,
    measurement: Measurement,
    pilot: Option<Pilot>,
}

#[derive(Deserialize)]
struct Topology {
    frontend_count: usize,
    backend_count: usize,
    launch_profile: String,
}

#[derive(Deserialize)]
struct Measurement {
    repetitions: usize,
    window_seconds: u64,
    minimum_short_query_samples_per_window: usize,
}

#[derive(Deserialize)]
struct Pilot {
    catalog: String,
    queries: Vec<PilotQuery>,
    rss_workloads: Vec<String>,
}

#[derive(Clone, Deserialize)]
struct PilotQuery {
    name: String,
    sql: String,
    expected_row_count: u64,
    clients: usize,
    min_query_interval_ms: u64,
    warmup_ms: u64,
}

impl Workload {
    fn load() -> Result<(Self, String)> {
        let path = env_path(WORKLOAD_ENV)?;
        let bytes = fs::read(&path)
            .with_context(|| format!("read UEA-4A-2 workload {}", path.display()))?;
        let workload: Self = serde_json::from_slice(&bytes).context("decode UEA-4A-2 workload")?;
        ensure!(
            workload.schema_version == 1 && workload.scenario == "uea4/iceberg-range-performance",
            "wrong UEA-4A-2 workload schema or scenario"
        );
        ensure!(
            workload.topology.frontend_count == 1
                && workload.topology.backend_count == 3
                && workload.topology.launch_profile == "performance",
            "UEA-4A-2 requires native 1FE+3BE performance topology"
        );
        ensure!(
            workload.measurement.repetitions == 3
                && workload.measurement.window_seconds == 120
                && workload.measurement.minimum_short_query_samples_per_window == 1000,
            "UEA-4A-2 requires three 120-second windows and 1000 short queries per window"
        );
        if pilot_mode()? {
            let pilot = workload
                .pilot
                .as_ref()
                .context("G0 pilot needs an explicit pilot workload")?;
            ensure!(
                pilot.catalog == "from-fixture-manifest",
                "G0 pilot catalog must be bound to the published fixture"
            );
            ensure!(
                pilot.queries.len() == 1
                    && pilot.rss_workloads.len() == 1
                    && pilot.rss_workloads[0] == pilot.queries[0].name,
                "G0 pilot requires one matching query and RSS workload"
            );
            let query = &pilot.queries[0];
            if calibration_seconds()?.is_some() {
                ensure!(
                    !query.name.is_empty()
                        && query
                            .name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                    "I00 calibration workload name is invalid"
                );
                calibration_id_range(&query.sql)?;
            } else {
                ensure!(
                    query.name == "uea4a2-iceberg-short-count"
                        && query.sql == "SELECT COUNT(*) FROM ${table} WHERE id BETWEEN 1 AND 10"
                        && query.expected_row_count == 10,
                    "G0 pilot must use the frozen short Iceberg scan"
                );
            }
            ensure!(
                (1..=64).contains(&query.clients)
                    && query.min_query_interval_ms > 0
                    && query.warmup_ms >= 1000
                    && query.expected_row_count > 0,
                "G0 pilot query lacks clients, warmup, or oracle"
            );
        } else {
            bail!(
                "UEA-4A-2 formal G0-G3 is unavailable until I03 operation/control attribution and the four-writer oracle are implemented"
            );
        }
        Ok((workload, sha256(&bytes)))
    }
}

#[derive(Deserialize)]
struct PilotFixture {
    schema_version: u32,
    fixture_kind: String,
    table_location: String,
    warehouse_uri: String,
    rest_uri: String,
    s3_endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
    database: String,
    table: String,
    data_file_count: u64,
    row_count: u64,
    objects_sha256: String,
    artifacts: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PilotOracle {
    row_count: u64,
    min_id: u64,
    max_id: u64,
    id_sum: u64,
    value_sum: u64,
}

#[derive(Deserialize)]
struct PilotObject {
    key: String,
    etag: String,
    sha256: Option<String>,
    size: u64,
}

struct CheckedFixture {
    facts: PilotFixture,
    objects: Vec<PilotObject>,
    manifest_sha256: String,
}

impl CheckedFixture {
    fn load(query: &PilotQuery, calibration: bool) -> Result<Self> {
        let path = env_path(FIXTURE_ENV)?;
        let bytes =
            fs::read(&path).with_context(|| format!("read UEA-4A-2 fixture {}", path.display()))?;
        let digest = sha256(&bytes);
        let root = path
            .parent()
            .context("fixture manifest has no parent directory")?;
        let ready = fs::read_to_string(root.join("READY")).context("read exact fixture READY")?;
        ensure!(
            ready.trim() == format!("sha256:{digest}"),
            "fixture READY does not bind the exact manifest"
        );
        let facts: PilotFixture =
            serde_json::from_slice(&bytes).context("decode G0 pilot fixture")?;
        ensure!(
            facts.schema_version == 1,
            "G0 pilot requires a versioned published Iceberg fixture"
        );
        let a4_calibration = calibration && facts.fixture_kind == "uea4a4-iceberg-performance-v1";
        let short_fixture = facts.fixture_kind == "uea4a2-iceberg-short-v1";
        ensure!(
            a4_calibration || short_fixture,
            "G0 pilot requires the published short Iceberg fixture; A4 is calibration only"
        );
        ensure!(
            if a4_calibration {
                facts.data_file_count >= 16 && facts.row_count == 768
            } else {
                facts.data_file_count >= 1 && facts.row_count == 4096
            },
            "G0 pilot fixture has the wrong source data shape"
        );
        ensure!(
            facts.rest_uri.starts_with("http://127.0.0.1:")
                || facts.rest_uri.starts_with("http://localhost:"),
            "G0 pilot REST endpoint must be loopback"
        );
        ensure!(
            facts.s3_endpoint.starts_with("http://127.0.0.1:")
                || facts.s3_endpoint.starts_with("http://localhost:"),
            "G0 pilot S3 endpoint must be loopback"
        );
        ensure!(
            facts.table_location.starts_with("s3://") && !facts.table_location.contains('?'),
            "G0 pilot table location must be an exact S3 path"
        );
        for id in [
            &facts.database,
            &facts.table,
            &facts.credential_name,
            &facts.credential_generation,
        ] {
            ensure!(
                !id.is_empty()
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "invalid fixture identifier"
            );
        }
        for (name, hash) in &facts.artifacts {
            ensure!(
                !name.contains('/') && !name.contains("..") && is_sha256(hash),
                "invalid fixture artifact name or hash"
            );
            ensure!(
                sha256_file(&root.join(name))? == *hash,
                "fixture artifact hash mismatch: {name}"
            );
        }
        ensure!(
            facts.artifacts.get("objects.json") == Some(&facts.objects_sha256),
            "fixture object inventory hash is not published"
        );
        let objects_bytes =
            fs::read(root.join("objects.json")).context("read G0 pilot object inventory")?;
        ensure!(
            sha256(&objects_bytes) == facts.objects_sha256,
            "fixture object inventory changed after artifact verification"
        );
        let objects: Vec<PilotObject> =
            serde_json::from_slice(&objects_bytes).context("decode G0 pilot object inventory")?;
        let mut distinct_keys = BTreeSet::new();
        let data_files = objects
            .iter()
            .filter(|object| object.key.starts_with("data/") && object.key.ends_with(".parquet"))
            .count();
        ensure!(
            u64::try_from(data_files).ok() == Some(facts.data_file_count)
                && objects.iter().all(|object| {
                    (object.key.starts_with("metadata/")
                        || (object.key.starts_with("data/") && object.key.ends_with(".parquet")))
                        && !object.key.contains("..")
                        && !object.etag.is_empty()
                        && object.size > 0
                        && (!short_fixture
                            || !object.key.starts_with("data/")
                            || object.sha256.as_deref().is_some_and(is_sha256))
                        && distinct_keys.insert(&object.key)
                }),
            "fixture source object inventory does not match declared data files"
        );
        if short_fixture && data_files == 1 {
            let data_object = objects
                .iter()
                .find(|object| object.key.starts_with("data/"))
                .context("short fixture has no source data object")?;
            ensure!(
                data_object.sha256.as_ref() == facts.artifacts.get("data.parquet"),
                "short fixture data artifact does not match its source object"
            );
        }
        let oracle_bytes =
            fs::read(root.join("oracle.json")).context("read G0 pilot row oracle")?;
        ensure!(
            facts.artifacts.get("oracle.json") == Some(&sha256(&oracle_bytes)),
            "fixture oracle hash mismatch"
        );
        let oracle: PilotOracle =
            serde_json::from_slice(&oracle_bytes).context("decode G0 pilot oracle")?;
        let expected_rows = if a4_calibration { 768 } else { 4096 };
        let expected_id_sum = (expected_rows - 1) * expected_rows / 2;
        ensure!(
            oracle.row_count == expected_rows
                && oracle.row_count == facts.row_count
                && oracle.min_id == 0
                && oracle.max_id == expected_rows - 1
                && oracle.id_sum == expected_id_sum
                && if a4_calibration {
                    oracle.value_sum > 0
                } else {
                    oracle.value_sum == 3 * expected_id_sum
                },
            "G0 pilot row oracle is inconsistent"
        );
        if calibration {
            let (lower, upper) = calibration_id_range(&query.sql)?;
            let lower = lower.max(oracle.min_id);
            let upper = upper.min(oracle.max_id);
            let expected = if lower > upper { 0 } else { upper - lower + 1 };
            ensure!(
                query.expected_row_count == expected,
                "I00 calibration expected count does not match the published Iceberg oracle"
            );
        }
        Ok(Self {
            facts,
            objects,
            manifest_sha256: digest,
        })
    }

    fn table_name(&self) -> String {
        format!("uea4a2_pilot.{}.{}", self.facts.database, self.facts.table)
    }

    fn catalog_sql(&self, endpoint: &str) -> String {
        let f = &self.facts;
        format!(
            "CREATE EXTERNAL CATALOG uea4a2_pilot PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{}\",\"credential.object-store-metadata.generation\"=\"{}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"{}\",\"credential.object-store-data.generation\"=\"{}\")",
            sql_string(&f.rest_uri),
            sql_string(&f.warehouse_uri),
            sql_string(endpoint),
            sql_string(&f.region),
            f.credential_name,
            f.credential_generation,
            f.credential_name,
            f.credential_generation
        )
    }
}

struct LiveFixture {
    checked: CheckedFixture,
    proxy: DelayedS3Proxy,
}

#[derive(Default)]
struct RangePerformance {
    fixture: Mutex<Option<LiveFixture>>,
}

impl Scenario for RangePerformance {
    fn name(&self) -> &'static str {
        "uea4/iceberg-range-performance"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile, _uea1: Option<&Path>) -> Result<()> {
        ensure!(
            profile == LaunchProfile::Performance,
            "UEA-4A-2 requires --launch-profile performance"
        );
        let (workload, _) = Workload::load()?;
        let query = &workload.pilot.as_ref().context("missing pilot")?.queries[0];
        CheckedFixture::load(query, calibration_seconds()?.is_some())?;
        group()?;
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (workload, _) = Workload::load()?;
        let fixture = CheckedFixture::load(
            &workload.pilot.as_ref().context("missing pilot")?.queries[0],
            calibration_seconds()?.is_some(),
        )?;
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: fixture.facts.s3_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        for (index, object) in fixture.objects.iter().enumerate() {
            let uri = format!(
                "{}/{}",
                fixture.facts.table_location.trim_end_matches('/'),
                object.key
            );
            let path = uri
                .strip_prefix("s3://")
                .context("fixture object is not S3")?;
            proxy.label_object(&format!("/{path}"), &format!("object_{index}"))?;
        }
        let access =
            env::var("AWS_S3_ACCESS_KEY_ID").context("G0 pilot requires AWS_S3_ACCESS_KEY_ID")?;
        let secret = env::var("AWS_S3_SECRET_ACCESS_KEY")
            .context("G0 pilot requires AWS_S3_SECRET_ACCESS_KEY")?;
        ensure!(
            !access.is_empty() && !secret.is_empty(),
            "G0 pilot S3 credentials must be nonempty"
        );
        let mut child = CrossProcessChildEnvironment::default();
        for values in [&mut child.fe, &mut child.be] {
            values.insert(ACCESS_KEY_ENV.to_owned(), access.clone());
            values.insert(SECRET_KEY_ENV.to_owned(), secret.clone());
        }
        let metadata = format!(
            "{}\n{PILOT_CACHE_OVERLAY}",
            credential_overlay("object-store-metadata", &fixture.facts)
        );
        let data = format!(
            "{}\n{PILOT_CACHE_OVERLAY}",
            credential_overlay("object-store-data", &fixture.facts)
        );
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("G0 pilot fixture lock poisoned"))? = Some(LiveFixture {
            checked: fixture,
            proxy,
        });
        Ok(ScenarioLaunchConfig {
            child_environment: child,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(metadata),
                be: Some(data),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.launch_profile() == LaunchProfile::Performance,
            "G0 pilot must use the native performance profile"
        );
        let (workload, workload_sha256) = Workload::load()?;
        let calibration_seconds = calibration_seconds()?;
        let pilot = workload.pilot.as_ref().context("missing pilot")?;
        let query = &pilot.queries[0];
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("G0 pilot fixture lock poisoned"))?;
        let fixture = fixture
            .as_ref()
            .context("G0 pilot fixture missing after cluster launch")?;
        let trace_path = context
            .scenario_root()
            .join(if calibration_seconds.is_some() {
                "calibration-query-trace.json"
            } else {
                "query-trace.json"
            });
        let resource_path = context
            .scenario_root()
            .join(if calibration_seconds.is_some() {
                "calibration-resource-trace.json"
            } else {
                "resource-trace.json"
            });
        let proxy_event_path = context
            .scenario_root()
            .join(if calibration_seconds.is_some() {
                "calibration-proxy-events.jsonl"
            } else {
                "proxy-events.jsonl"
            });
        let proxy_connection_path =
            context
                .scenario_root()
                .join(if calibration_seconds.is_some() {
                    "calibration-proxy-connections.jsonl"
                } else {
                    "proxy-connections.jsonl"
                });
        let proxy_event_file =
            File::create(&proxy_event_path).context("create G0 pilot proxy event trace")?;
        let proxy_connection_file = File::create(&proxy_connection_path)
            .context("create G0 pilot proxy connection trace")?;
        let (windows, rss) = thread::scope(|scope| -> Result<_> {
            let (stop_tx, stop_rx) = mpsc::channel();
            let log_worker = scope.spawn(|| {
                persist_proxy_logs(
                    &fixture.proxy,
                    proxy_event_file,
                    proxy_connection_file,
                    stop_rx,
                )
            });
            let observed = (|| -> Result<_> {
                let mut setup = mysql_actor::connect(
                    context.mysql_user(),
                    context.mysql_port(),
                    context.remaining("connect G0 pilot setup")?,
                )?;
                setup
                    .query_drop(fixture.checked.catalog_sql(fixture.proxy.endpoint()))
                    .context("create G0 pilot Iceberg catalog")?;
                let sql = query.sql.replace("${table}", &fixture.checked.table_name());
                let count: Option<u64> = setup
                    .query_first(&sql)
                    .context("verify G0 pilot filtered-count oracle")?;
                ensure!(
                    count == Some(query.expected_row_count),
                    "G0 pilot filtered-count oracle mismatch"
                );
                drop(setup);

                let monitor = ProcessResourceMonitor::start_with_identities_and_runner(
                    context.process_resource_identities()?,
                    context.name(),
                    SAMPLE_INTERVAL,
                )?;
                let measured = (|| -> Result<(Vec<Window>, Vec<WindowTimes>)> {
                    let mut windows = Vec::new();
                    let mut times = Vec::new();
                    let repetitions = if calibration_seconds.is_some() { 1 } else { 3 };
                    for repetition in 0..repetitions {
                        let duration = Duration::from_secs(calibration_seconds.unwrap_or(120));
                        context.action(format!(
                            "start G0 {} real Iceberg window repetition={repetition} duration_seconds={}",
                            if calibration_seconds.is_some() {
                                "calibration"
                            } else {
                                "pilot"
                            },
                            duration.as_secs()
                        ));
                        let idle_start = monitor.elapsed_millis();
                        thread::sleep(PILOT_IDLE);
                        let idle_end = monitor.elapsed_millis();
                        let (mut window, mut timing) = run_pilot_window(
                            context,
                            query,
                            repetition,
                            &sql,
                            &fixture.proxy,
                            &monitor,
                            duration,
                        )?;
                        timing.idle_start = idle_start;
                        timing.idle_end = idle_end;
                        timing.post_start = monitor.elapsed_millis();
                        thread::sleep(PILOT_POST_DRAIN);
                        timing.post_end = monitor.elapsed_millis();
                        window.rss_timing = timing.clone();
                        windows.push(window);
                        times.push(timing);
                    }
                    Ok((windows, times))
                })();
                let samples = monitor.finish(&resource_path)?;
                let (windows, times) = measured?;
                fs::write(&trace_path, serde_json::to_vec_pretty(&windows)?)
                    .context("write G0 pilot raw query trace")?;
                let rss = rss_summary(samples.samples(), query, &times)?;
                Ok((windows, rss))
            })();
            drop(stop_tx);
            log_worker
                .join()
                .map_err(|_| anyhow::anyhow!("G0 pilot proxy log collector panicked"))??;
            observed
        })?;
        let attachments = BTreeMap::from([
            ("query_trace", attachment(&trace_path)?),
            ("resource_trace", attachment(&resource_path)?),
            ("proxy_event_trace", attachment(&proxy_event_path)?),
            (
                "proxy_connection_trace",
                attachment(&proxy_connection_path)?,
            ),
        ]);
        if let Some(seconds) = calibration_seconds {
            let window = windows
                .into_iter()
                .next()
                .context("I00 calibration did not produce its single window")?;
            let window_end_ms = window.started_ms as f64 + window.duration_ms as f64;
            let completed_in_window = window
                .queries
                .iter()
                .filter(|record| record.ended_ms <= window_end_ms)
                .count();
            let successful_in_window = window
                .queries
                .iter()
                .filter(|record| record.ended_ms <= window_end_ms && record.status == "success")
                .count();
            let total_success = window
                .queries
                .iter()
                .filter(|record| record.status == "success")
                .count();
            let total_failure = window.queries.len() - total_success;
            let report = CalibrationReceipt {
                schema_version: 2,
                calibration_only: true,
                g0_gate_eligible: false,
                group: group()?,
                topology: ReceiptTopology { fe: 1, be: 3 },
                workload_sha256,
                fixture_manifest_sha256: fixture.checked.manifest_sha256.clone(),
                base_config_sha256: sha256_file(context.base_config_path())?,
                runner_sha256: sha256_file(&env::current_exe().context("locate scenario runner")?)?,
                binary_sha256: sha256_file(context.primary_binary())?,
                effective_config_sha256: context
                    .effective_launch_config_evidence()
                    .semantics_sha256()
                    .to_owned(),
                workload: query.name.clone(),
                clients: query.clients,
                min_query_interval_ms: query.min_query_interval_ms,
                warmup_ms: query.warmup_ms,
                measurement_seconds: seconds,
                completed_in_window,
                successful_in_window,
                total_success,
                total_failure,
                proxy_gets: window.proxy_gets,
                proxy_heads: window.proxy_heads,
                proxy_upstream_bytes: window.proxy_upstream_bytes,
                proxy_completed_bytes: window.proxy_completed_bytes,
                proxy_connections_accepted: window.proxy_connections_accepted,
                proxy_event_overflow: window.proxy_event_overflow,
                rss,
                attachments,
            };
            let path = context.scenario_root().join("uea4a2-calibration.json");
            fs::write(&path, serde_json::to_vec_pretty(&report)?)
                .context("write I00 calibration receipt")?;
            context.action(
                "captured short I00 calibration only; this receipt is ineligible for the G0 gate",
            );
            return Ok(());
        }
        let report = PilotReceipt {
            schema_version: 3,
            group: group()?,
            topology: ReceiptTopology { fe: 1, be: 3 },
            pilot: true,
            workload_sha256,
            fixture_manifest_sha256: fixture.checked.manifest_sha256.clone(),
            base_config_sha256: sha256_file(context.base_config_path())?,
            runner_sha256: sha256_file(&env::current_exe().context("locate scenario runner")?)?,
            binary_sha256: sha256_file(context.primary_binary())?,
            effective_config_sha256: context
                .effective_launch_config_evidence()
                .semantics_sha256()
                .to_owned(),
            oracle_passed: false,
            attribution_complete: false,
            proxy_lifetime: ProxyLifetime::from_snapshot(fixture.proxy.snapshot()),
            windows,
            controls: Vec::new(),
            rss: BTreeMap::from([(query.name.clone(), rss)]),
            attachments,
        };
        let path = context.scenario_root().join("uea4a2-performance.json");
        fs::write(&path, serde_json::to_vec_pretty(&report)?).context("write G0 pilot receipt")?;
        ensure!(report.windows.iter().all(|w| w.queries.len() >= 1000 && w.queries.iter().all(|q| q.status == "success")), "G0 pilot has too few successful samples or query failures");
        context.action("captured G0 pilot raw Iceberg query and per-process resource evidence; formal attribution remains unavailable");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("G0 pilot fixture lock poisoned"))?
            .take();
        Ok(())
    }
}

fn persist_proxy_logs(
    proxy: &DelayedS3Proxy,
    event_file: File,
    connection_file: File,
    stop: mpsc::Receiver<()>,
) -> Result<()> {
    let mut events = BufWriter::new(event_file);
    let mut connections = BufWriter::new(connection_file);
    let mut last_drain = Instant::now();
    let mut last_requests = 0;
    drain_proxy_logs(proxy, &mut events, &mut connections)?;
    loop {
        let stopped = match stop.recv_timeout(PROXY_LOG_POLL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => true,
            Err(RecvTimeoutError::Timeout) => false,
        };
        let snapshot = proxy.snapshot();
        ensure!(
            snapshot.event_overflow == 0,
            "G0 pilot proxy raw event log overflowed"
        );
        let requests = snapshot.gets.saturating_add(snapshot.heads);
        if stopped
            || last_drain.elapsed() >= PROXY_LOG_INTERVAL
            || requests.saturating_sub(last_requests) >= PROXY_LOG_REQUEST_INTERVAL
        {
            drain_proxy_logs(proxy, &mut events, &mut connections)?;
            last_drain = Instant::now();
            last_requests = requests;
        }
        if stopped {
            return Ok(());
        }
    }
}

fn drain_proxy_logs(
    proxy: &DelayedS3Proxy,
    events: &mut BufWriter<File>,
    connections: &mut BufWriter<File>,
) -> Result<()> {
    for event in proxy.take_event_log()? {
        serde_json::to_writer(
            &mut *events,
            &serde_json::json!({
                "kind": format!("{:?}", event.kind),
                "elapsed_millis": event.elapsed_millis,
                "request_id": event.request_id,
                "connection_id": event.connection_id,
                "protocol": event.protocol,
                "method": event.method.as_str(),
                "object_id": event.object_id,
                "read_class": event.read_class.map(|class| format!("{class:?}")),
                "range": event.range,
                "bytes": event.bytes,
            }),
        )
        .context("write G0 pilot proxy event")?;
        events.write_all(b"\n")?;
    }
    for event in proxy.take_connection_log()? {
        serde_json::to_writer(
            &mut *connections,
            &serde_json::json!({
                "kind": format!("{:?}", event.kind),
                "elapsed_millis": event.elapsed_millis,
                "connection_id": event.connection_id,
            }),
        )
        .context("write G0 pilot proxy connection event")?;
        connections.write_all(b"\n")?;
    }
    events.flush().context("flush G0 pilot proxy events")?;
    connections
        .flush()
        .context("flush G0 pilot proxy connections")?;
    ensure!(
        proxy.snapshot().event_overflow == 0,
        "G0 pilot proxy raw log overflowed during drain"
    );
    Ok(())
}

fn credential_overlay(purpose: &str, fixture: &PilotFixture) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{}\"\ngeneration = \"{}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n",
        fixture.credential_name, fixture.credential_generation
    )
}

#[derive(Serialize)]
struct QueryRecord {
    started_ms: f64,
    ended_ms: f64,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct Window {
    workload: String,
    repetition: usize,
    duration_ms: u64,
    started_ms: u128,
    warmup_drained: bool,
    tail_drained: bool,
    tail_drain_ms: u128,
    queries: Vec<QueryRecord>,
    proxy_gets: u64,
    proxy_heads: u64,
    proxy_upstream_bytes: u64,
    proxy_completed_bytes: u64,
    proxy_connections_accepted: u64,
    proxy_upstream_connect_attempts: u64,
    proxy_upstream_connections_established: u64,
    proxy_upstream_http1_responses: u64,
    proxy_upstream_http2_responses: u64,
    proxy_upstream_other_protocol_responses: u64,
    proxy_event_overflow: u64,
    proxy_observation_start_ms: u128,
    proxy_observation_end_ms: u128,
    rss_timing: WindowTimes,
}

#[derive(Clone, Default, Serialize)]
struct WindowTimes {
    idle_start: u128,
    idle_end: u128,
    measure_start: u128,
    measure_end: u128,
    post_start: u128,
    post_end: u128,
}

fn run_pilot_window(
    context: &ScenarioContext,
    query: &PilotQuery,
    repetition: usize,
    sql: &str,
    proxy: &DelayedS3Proxy,
    monitor: &ProcessResourceMonitor,
    duration: Duration,
) -> Result<(Window, WindowTimes)> {
    let mut connections = Vec::with_capacity(query.clients);
    for _ in 0..query.clients {
        connections.push(mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect G0 pilot query client")?,
        )?);
    }
    let warmup_end = Instant::now() + Duration::from_millis(query.warmup_ms);
    while Instant::now() < warmup_end {
        for connection in &mut connections {
            verify_count(connection, sql, query.expected_row_count)?;
        }
    }
    let proxy_observation_start_ms = proxy.elapsed_millis();
    let before = proxy.snapshot();
    let gate = Arc::new(Barrier::new(query.clients + 1));
    let start = Arc::new(OnceLock::<(Instant, u128)>::new());
    let mut workers = Vec::new();
    for mut connection in connections {
        let gate = Arc::clone(&gate);
        let start = Arc::clone(&start);
        let sql = sql.to_owned();
        let expected = query.expected_row_count;
        let query_interval_ms = query.min_query_interval_ms;
        workers.push(thread::spawn(move || -> Result<Vec<QueryRecord>> {
            gate.wait();
            let (origin, base_ms) = *start.get().context("G0 pilot window start missing")?;
            let end = origin + duration;
            let mut records = Vec::new();
            loop {
                let begun = Instant::now();
                if begun >= end {
                    break;
                }
                let result = verify_count(&mut connection, &sql, expected);
                let error = result.as_ref().err().map(|error| format!("{error:#}"));
                records.push(QueryRecord {
                    started_ms: base_ms as f64
                        + begun.duration_since(origin).as_secs_f64() * 1000.0,
                    ended_ms: base_ms as f64
                        + Instant::now().duration_since(origin).as_secs_f64() * 1000.0,
                    status: if result.is_ok() { "success" } else { "error" },
                    error,
                });
                if result.is_err() {
                    break;
                }
                let next_start = begun + Duration::from_millis(query_interval_ms);
                if let Some(wait) = next_start.checked_duration_since(Instant::now()) {
                    thread::sleep(wait);
                }
            }
            Ok(records)
        }));
    }
    let measure_start = monitor.elapsed_millis();
    let origin = Instant::now();
    start
        .set((origin, measure_start))
        .map_err(|_| anyhow::anyhow!("G0 pilot window start already set"))?;
    gate.wait();
    let end = origin + duration;
    while Instant::now() < end {
        thread::sleep(SAMPLE_INTERVAL.min(end.saturating_duration_since(Instant::now())));
    }
    let measure_end = monitor.elapsed_millis();
    let mut records = Vec::new();
    for worker in workers {
        records.extend(
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("G0 pilot query worker panicked"))??,
        );
    }
    let drain_ms = Instant::now().saturating_duration_since(end).as_millis();
    let after = proxy.snapshot();
    let proxy_observation_end_ms = proxy.elapsed_millis();
    ensure!(
        after.upstream_errors == before.upstream_errors,
        "G0 pilot proxy failed a real object request"
    );
    let gets = after.gets.saturating_sub(before.gets);
    ensure!(
        gets > 0,
        "G0 pilot window did not read a real Iceberg object through the proxy"
    );
    Ok((
        Window {
            workload: query.name.clone(),
            repetition,
            duration_ms: u64::try_from(duration.as_millis())
                .context("G0 measurement duration overflows milliseconds")?,
            started_ms: measure_start,
            warmup_drained: true,
            tail_drained: true,
            tail_drain_ms: drain_ms,
            queries: records,
            proxy_gets: gets,
            proxy_heads: after.heads.saturating_sub(before.heads),
            proxy_upstream_bytes: after
                .upstream_bytes_read
                .saturating_sub(before.upstream_bytes_read),
            proxy_completed_bytes: after
                .completed_response_bytes
                .saturating_sub(before.completed_response_bytes),
            proxy_connections_accepted: after
                .observed_be_connections
                .saturating_sub(before.observed_be_connections),
            proxy_upstream_connect_attempts: after
                .upstream_connect_attempts
                .saturating_sub(before.upstream_connect_attempts),
            proxy_upstream_connections_established: after
                .upstream_connections_established
                .saturating_sub(before.upstream_connections_established),
            proxy_upstream_http1_responses: after
                .upstream_http1_responses
                .saturating_sub(before.upstream_http1_responses),
            proxy_upstream_http2_responses: after
                .upstream_http2_responses
                .saturating_sub(before.upstream_http2_responses),
            proxy_upstream_other_protocol_responses: after
                .upstream_other_protocol_responses
                .saturating_sub(before.upstream_other_protocol_responses),
            proxy_event_overflow: after.event_overflow.saturating_sub(before.event_overflow),
            proxy_observation_start_ms,
            proxy_observation_end_ms,
            rss_timing: WindowTimes::default(),
        },
        WindowTimes {
            measure_start,
            measure_end,
            ..Default::default()
        },
    ))
}

fn verify_count(connection: &mut mysql::Conn, sql: &str, expected: u64) -> Result<()> {
    let actual: Option<u64> = connection
        .query_first(sql)
        .context("execute real G0 pilot Iceberg scan")?;
    ensure!(
        actual == Some(expected),
        "G0 pilot Iceberg row oracle mismatch"
    );
    Ok(())
}

#[derive(Serialize)]
struct RssRound {
    idle_bytes: u64,
    peak_bytes: u64,
    steady_bytes: u64,
    post_drain_bytes: u64,
    observed_post_drain_seconds: f64,
    active_current: Option<u64>,
    active_next: Option<u64>,
    active_claims: Option<u64>,
    undrained_operations: Option<u64>,
}

fn rss_summary(
    samples: &[ProcessResourceSample],
    query: &PilotQuery,
    times: &[WindowTimes],
) -> Result<BTreeMap<String, Vec<RssRound>>> {
    let mut by_role = BTreeMap::new();
    for role in ["be-0", "be-1", "be-2"] {
        let mut rounds = Vec::new();
        for t in times {
            let idle = rss_range(samples, role, t.idle_start, t.idle_end)?;
            let active = rss_range(samples, role, t.idle_end, t.post_end)?;
            let steady = rss_range(
                samples,
                role,
                t.measure_start + (t.measure_end - t.measure_start) / 2,
                t.measure_end,
            )?;
            let post = rss_range(samples, role, t.post_start, t.post_end)?;
            rounds.push(RssRound {
                idle_bytes: median(idle),
                peak_bytes: *active
                    .iter()
                    .max()
                    .context("missing active-window peak RSS")?,
                steady_bytes: median(steady),
                post_drain_bytes: median(post),
                observed_post_drain_seconds: (t.post_end - t.post_start) as f64 / 1000.0,
                active_current: None,
                active_next: None,
                active_claims: None,
                undrained_operations: None,
            });
        }
        by_role.insert(role.to_owned(), rounds);
    }
    ensure!(!query.name.is_empty(), "G0 pilot RSS workload name missing");
    Ok(by_role)
}

fn rss_range(
    samples: &[ProcessResourceSample],
    role: &str,
    start: u128,
    end: u128,
) -> Result<Vec<u64>> {
    let values: Vec<_> = samples
        .iter()
        .filter(|s| s.role == role && s.elapsed_millis >= start && s.elapsed_millis <= end)
        .map(|s| s.rss_bytes)
        .collect();
    ensure!(
        !values.is_empty() && values.iter().all(Option::is_some),
        "G0 pilot RSS missing for {role} in {start}..{end}"
    );
    Ok(values.into_iter().flatten().collect())
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

#[derive(Serialize)]
struct ReceiptTopology {
    fe: usize,
    be: usize,
}

#[derive(Serialize)]
struct Attachment {
    path: String,
    sha256: String,
}

#[derive(Serialize)]
struct PilotReceipt {
    schema_version: u32,
    group: String,
    topology: ReceiptTopology,
    pilot: bool,
    workload_sha256: String,
    fixture_manifest_sha256: String,
    base_config_sha256: String,
    runner_sha256: String,
    binary_sha256: String,
    effective_config_sha256: String,
    oracle_passed: bool,
    attribution_complete: bool,
    proxy_lifetime: ProxyLifetime,
    windows: Vec<Window>,
    controls: Vec<serde_json::Value>,
    rss: BTreeMap<String, BTreeMap<String, Vec<RssRound>>>,
    attachments: BTreeMap<&'static str, Attachment>,
}

#[derive(Serialize)]
struct ProxyLifetime {
    upstream_connect_attempts: u64,
    upstream_connections_established: u64,
    upstream_http1_responses: u64,
    upstream_http2_responses: u64,
    upstream_other_protocol_responses: u64,
}

impl ProxyLifetime {
    fn from_snapshot(snapshot: novarocks_cluster_harness::delayed_s3::DelayedS3Snapshot) -> Self {
        Self {
            upstream_connect_attempts: snapshot.upstream_connect_attempts,
            upstream_connections_established: snapshot.upstream_connections_established,
            upstream_http1_responses: snapshot.upstream_http1_responses,
            upstream_http2_responses: snapshot.upstream_http2_responses,
            upstream_other_protocol_responses: snapshot.upstream_other_protocol_responses,
        }
    }
}

#[derive(Serialize)]
struct CalibrationReceipt {
    schema_version: u32,
    calibration_only: bool,
    g0_gate_eligible: bool,
    group: String,
    topology: ReceiptTopology,
    workload_sha256: String,
    fixture_manifest_sha256: String,
    base_config_sha256: String,
    runner_sha256: String,
    binary_sha256: String,
    effective_config_sha256: String,
    workload: String,
    clients: usize,
    min_query_interval_ms: u64,
    warmup_ms: u64,
    measurement_seconds: u64,
    completed_in_window: usize,
    successful_in_window: usize,
    total_success: usize,
    total_failure: usize,
    proxy_gets: u64,
    proxy_heads: u64,
    proxy_upstream_bytes: u64,
    proxy_completed_bytes: u64,
    proxy_connections_accepted: u64,
    proxy_event_overflow: u64,
    rss: BTreeMap<String, Vec<RssRound>>,
    attachments: BTreeMap<&'static str, Attachment>,
}

fn attachment(path: &Path) -> Result<Attachment> {
    Ok(Attachment {
        path: path
            .file_name()
            .context("attachment has no filename")?
            .to_string_lossy()
            .into_owned(),
        sha256: sha256_file(path)?,
    })
}

fn group() -> Result<String> {
    let value = env::var(GROUP_ENV).with_context(|| format!("{GROUP_ENV} required"))?;
    ensure!(
        value == "g0-a" || value == "g0-b",
        "G0 pilot group must be g0-a or g0-b"
    );
    Ok(value)
}

fn pilot_mode() -> Result<bool> {
    match env::var(PILOT_ENV) {
        Ok(value) if value == "1" => Ok(true),
        Ok(_) => bail!("{PILOT_ENV} must be 1 when set"),
        Err(env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error).context("read G0 pilot mode"),
    }
}

fn calibration_seconds() -> Result<Option<u64>> {
    match env::var(CALIBRATION_SECONDS_ENV) {
        Ok(value) => {
            let seconds: u64 = value
                .parse()
                .with_context(|| format!("{CALIBRATION_SECONDS_ENV} must be an integer"))?;
            ensure!(
                (15..=90).contains(&seconds),
                "{CALIBRATION_SECONDS_ENV} must be between 15 and 90"
            );
            ensure!(
                pilot_mode()?,
                "I00 calibration requires the explicit G0 baseline pilot mode"
            );
            Ok(Some(seconds))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).context("read I00 calibration duration"),
    }
}

fn calibration_id_range(sql: &str) -> Result<(u64, u64)> {
    let bounds = sql
        .strip_prefix("SELECT COUNT(*) FROM ${table} WHERE id BETWEEN ")
        .context("I00 calibration requires an exact real-table id BETWEEN count query")?;
    let (lower, upper) = bounds
        .split_once(" AND ")
        .context("I00 calibration count query requires two id bounds")?;
    ensure!(
        !lower.is_empty()
            && !upper.is_empty()
            && lower.bytes().all(|byte| byte.is_ascii_digit())
            && upper.bytes().all(|byte| byte.is_ascii_digit()),
        "I00 calibration id bounds must be decimal integers"
    );
    let lower: u64 = lower.parse().context("parse I00 calibration lower id")?;
    let upper: u64 = upper.parse().context("parse I00 calibration upper id")?;
    ensure!(lower <= upper, "I00 calibration id bounds are reversed");
    Ok((lower, upper))
}

fn env_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(env::var(name).with_context(|| {
        format!("{name} must name a published manifest")
    })?))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::<RangePerformance>::default()]
}
