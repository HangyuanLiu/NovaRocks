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

//! Short real-provider planning regression check for UEA-4A-4.
//!
//! The same scenario and workload run against B0 and candidate binaries. This
//! coarse check only rejects large regressions; it is not a formal benchmark.

use super::connector::require_three_backends;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::process_resources::ProcessResourceMonitor;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const WORKLOAD_ENV: &str = "NOVAROCKS_UEA4A4_WORKLOAD_MANIFEST";
const ICEBERG_FIXTURE_ENV: &str = "NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST";
const PAIMON_FIXTURE_ENV: &str = "NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY";
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    schema_version: u32,
    purpose: String,
    warehouse_concurrency_limit: usize,
    clients: usize,
    warmup_ms: u64,
    duration_ms: u64,
    repetitions: usize,
    slow_s3_delay_ms: u64,
    minimum_data_files: u64,
    iceberg_query: String,
    paimon_query: String,
}

impl Workload {
    fn load() -> Result<(Self, String)> {
        let path = env::var_os(WORKLOAD_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../benchmarks/uea4a4/workload.json")
            });
        let bytes = fs::read(&path)
            .with_context(|| format!("read UEA-4A-4 workload {}", path.display()))?;
        let workload: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode UEA-4A-4 workload {}", path.display()))?;
        ensure!(
            workload.schema_version == 1,
            "unsupported UEA-4A-4 workload version"
        );
        ensure!(
            workload.purpose == "short-regression",
            "UEA-4A-4 requires the short-regression workload"
        );
        ensure!(
            workload.duration_ms == 20_000
                && workload.repetitions == 1
                && workload.warmup_ms == 3_000,
            "UEA-4A-4 short regression requires one 20-second window per provider and mode with a three-second warmup"
        );
        ensure!(
            (1..=64).contains(&workload.clients)
                && (1..=64).contains(&workload.warehouse_concurrency_limit)
                && workload.clients > workload.warehouse_concurrency_limit
                && workload.slow_s3_delay_ms > 0
                && workload.minimum_data_files >= 16,
            "UEA-4A-4 short workload lacks a saturated real-provider fixture"
        );
        for sql in [&workload.iceberg_query, &workload.paimon_query] {
            ensure!(
                sql.starts_with("SELECT ") && sql.contains("${table}") && !sql.contains(';'),
                "UEA-4A-4 workload query must be one SELECT with a table placeholder"
            );
        }
        Ok((workload, sha256(&bytes)))
    }
}

#[derive(Clone, Debug, Deserialize)]
struct ProviderFixture {
    schema_version: u32,
    fixture_kind: String,
    warehouse_uri: String,
    s3_endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
    database: String,
    table: String,
    snapshot_id: serde_json::Value,
    schema_id: serde_json::Value,
    data_file_count: u64,
    row_count: u64,
    objects_sha256: String,
    #[serde(default)]
    rest_uri: Option<String>,
}

#[derive(Clone)]
struct CheckedFixture {
    provider: &'static str,
    facts: ProviderFixture,
    manifest_sha256: String,
}

impl CheckedFixture {
    fn load(variable: &str, provider: &'static str, minimum_data_files: u64) -> Result<Self> {
        let path = PathBuf::from(
            env::var(variable)
                .with_context(|| format!("{variable} must name a published fixture"))?,
        );
        let bytes = fs::read(&path)
            .with_context(|| format!("read {provider} fixture manifest {}", path.display()))?;
        let manifest_sha256 = sha256(&bytes);
        let root = path
            .parent()
            .context("fixture manifest has no parent directory")?;
        let ready = fs::read_to_string(root.join("READY"))
            .with_context(|| format!("read {provider} fixture READY"))?;
        ensure!(
            ready.trim() == format!("sha256:{manifest_sha256}"),
            "{provider} fixture READY does not bind the exact manifest"
        );
        let facts: ProviderFixture = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode {provider} fixture manifest"))?;
        ensure!(
            facts.schema_version == 1,
            "unsupported {provider} fixture version"
        );
        ensure!(
            facts.fixture_kind == format!("uea4a4-{provider}-performance-v1"),
            "unexpected {provider} fixture kind"
        );
        ensure!(
            facts.data_file_count >= minimum_data_files && facts.row_count > 0,
            "{provider} fixture is too small for short planning regression"
        );
        for identifier in [&facts.database, &facts.table] {
            ensure!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "{provider} fixture contains an invalid database or table identifier"
            );
        }
        for identifier in [&facts.credential_name, &facts.credential_generation] {
            ensure!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
                "{provider} fixture contains an invalid identifier"
            );
        }
        ensure!(
            !facts.snapshot_id.is_null()
                && !facts.schema_id.is_null()
                && facts.snapshot_id.as_str().is_none_or(|id| !id.is_empty())
                && facts.schema_id.as_str().is_none_or(|id| !id.is_empty()),
            "{provider} fixture omitted snapshot or schema identity"
        );
        ensure!(
            facts.s3_endpoint.starts_with("http://127.0.0.1:")
                || facts.s3_endpoint.starts_with("http://localhost:"),
            "{provider} fixture must use a loopback HTTP S3 endpoint"
        );
        ensure!(
            provider != "iceberg" || facts.rest_uri.as_deref().is_some_and(|uri| !uri.is_empty()),
            "Iceberg fixture omitted REST Catalog endpoint"
        );
        let objects = fs::read(root.join("objects.json"))
            .with_context(|| format!("read {provider} fixture object inventory"))?;
        ensure!(
            sha256(&objects) == facts.objects_sha256,
            "{provider} fixture object inventory hash mismatch"
        );
        Ok(Self {
            provider,
            facts,
            manifest_sha256,
        })
    }

    fn catalog_name(&self, slow: bool) -> String {
        format!(
            "uea4a4_{}_{}",
            self.provider,
            if slow { "slow" } else { "normal" }
        )
    }

    fn table_name(&self, slow: bool) -> String {
        format!(
            "{}.{}.{}",
            self.catalog_name(slow),
            self.facts.database,
            self.facts.table
        )
    }

    fn catalog_sql(&self, slow: bool, s3_endpoint: &str) -> String {
        let name = self.catalog_name(slow);
        let properties = if self.provider == "iceberg" {
            format!(
                "\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\"",
                sql_string(self.facts.rest_uri.as_deref().unwrap_or_default()),
                sql_string(&self.facts.warehouse_uri)
            )
        } else {
            format!(
                "\"paimon.catalog.type\"=\"filesystem\",\"warehouse\"=\"{}\"",
                sql_string(&self.facts.warehouse_uri)
            )
        };
        format!(
            "CREATE EXTERNAL CATALOG {name} PROPERTIES(\"type\"=\"{}\",{properties},\
             \"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\
             \"aws.s3.enable_path_style_access\"=\"true\",\
             \"credential.object-store-metadata.consumer-role\"=\"frontend\",\
             \"credential.object-store-metadata.mode\"=\"static\",\
             \"credential.object-store-metadata.name\"=\"{}\",\
             \"credential.object-store-metadata.generation\"=\"{}\",\
             \"credential.object-store-data.consumer-role\"=\"backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{}\",\
             \"credential.object-store-data.generation\"=\"{}\")",
            self.provider,
            sql_string(s3_endpoint),
            sql_string(&self.facts.region),
            self.facts.credential_name,
            self.facts.credential_generation,
            self.facts.credential_name,
            self.facts.credential_generation,
        )
    }
}

struct LiveFixtures {
    iceberg: CheckedFixture,
    paimon: CheckedFixture,
    iceberg_slow: DelayedS3Proxy,
    paimon_slow: DelayedS3Proxy,
}

struct CatalogPlanningPerformance {
    fixtures: Mutex<Option<LiveFixtures>>,
    smoke_only: bool,
}

impl CatalogPlanningPerformance {
    fn new(smoke_only: bool) -> Self {
        Self {
            fixtures: Mutex::new(None),
            smoke_only,
        }
    }
}

impl Scenario for CatalogPlanningPerformance {
    fn name(&self) -> &'static str {
        if self.smoke_only {
            "uea4/catalog-planning-smoke"
        } else {
            "uea4/catalog-planning-performance"
        }
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _uea1_workload_manifest: Option<&Path>,
    ) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::Performance,
            "UEA-4A-4 performance requires --launch-profile performance"
        );
        let (workload, _) = Workload::load()?;
        CheckedFixture::load(ICEBERG_FIXTURE_ENV, "iceberg", workload.minimum_data_files)?;
        CheckedFixture::load(PAIMON_FIXTURE_ENV, "paimon", workload.minimum_data_files)?;
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (workload, _) = Workload::load()?;
        let iceberg =
            CheckedFixture::load(ICEBERG_FIXTURE_ENV, "iceberg", workload.minimum_data_files)?;
        let paimon =
            CheckedFixture::load(PAIMON_FIXTURE_ENV, "paimon", workload.minimum_data_files)?;
        let delay = Duration::from_millis(workload.slow_s3_delay_ms);
        let iceberg_slow = DelayedS3Proxy::start(DelayedS3Config {
            downstream: iceberg.facts.s3_endpoint.clone(),
            delay,
        })?;
        let paimon_slow = DelayedS3Proxy::start(DelayedS3Config {
            downstream: paimon.facts.s3_endpoint.clone(),
            delay,
        })?;
        let access_key = env::var("AWS_S3_ACCESS_KEY_ID")
            .context("UEA-4A-4 performance requires AWS_S3_ACCESS_KEY_ID")?;
        let secret_key = env::var("AWS_S3_SECRET_ACCESS_KEY")
            .context("UEA-4A-4 performance requires AWS_S3_SECRET_ACCESS_KEY")?;
        ensure!(
            !access_key.is_empty() && !secret_key.is_empty(),
            "UEA-4A-4 fixture credentials must be nonempty"
        );
        let mut child_environment = CrossProcessChildEnvironment::default();
        for values in [&mut child_environment.fe, &mut child_environment.be] {
            values.insert(ACCESS_KEY_ENV.to_owned(), access_key.clone());
            values.insert(SECRET_KEY_ENV.to_owned(), secret_key.clone());
        }
        let mut fe = format!(
            "[runtime.frontend_workload]\nconcurrency_limit = {}\nwaiting_limit = 512\ncapacity_wait_timeout_ms = 30000\n",
            workload.warehouse_concurrency_limit
        );
        let mut be = String::new();
        let mut credentials = std::collections::BTreeSet::new();
        for fixture in [&iceberg, &paimon] {
            credentials.insert((
                fixture.facts.credential_name.as_str(),
                fixture.facts.credential_generation.as_str(),
            ));
        }
        for (name, generation) in credentials {
            fe.push_str(&credential_overlay(
                "object-store-metadata",
                name,
                generation,
            ));
            be.push_str(&credential_overlay("object-store-data", name, generation));
        }
        *self
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))? = Some(LiveFixtures {
            iceberg,
            paimon,
            iceberg_slow,
            paimon_slow,
        });
        Ok(ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(fe),
                be: Some(be),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.launch_profile() == LaunchProfile::Performance,
            "UEA-4A-4 performance requires the native performance launch profile"
        );
        let (workload, workload_sha256) = Workload::load()?;
        let fixtures = self
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect UEA-4A-4 setup session")?,
        )?;
        for (fixture, proxy) in [
            (&fixtures.iceberg, &fixtures.iceberg_slow),
            (&fixtures.paimon, &fixtures.paimon_slow),
        ] {
            for (slow, endpoint) in [
                (false, fixture.facts.s3_endpoint.as_str()),
                (true, proxy.endpoint()),
            ] {
                connection
                    .query_drop(fixture.catalog_sql(slow, endpoint))
                    .with_context(|| format!("create {} benchmark catalog", fixture.provider))?;
                let table = fixture.table_name(slow);
                let count: Option<u64> = connection
                    .query_first(format!("SELECT COUNT(*) FROM {table}"))
                    .with_context(|| format!("verify {} benchmark table", fixture.provider))?;
                ensure!(
                    count == Some(fixture.facts.row_count),
                    "{} benchmark table row count does not match its published fixture",
                    fixture.provider
                );
            }
        }
        drop(connection);

        if self.smoke_only {
            context.action("verified real Iceberg and Paimon catalogs with identical performance configuration");
            return Ok(());
        }

        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            context.name(),
            SAMPLE_INTERVAL,
        )?;
        let execution = (|| -> Result<Vec<WindowReport>> {
            let mut reports = Vec::with_capacity(workload.repetitions * 4);
            for (fixture, proxy, query) in [
                (
                    &fixtures.iceberg,
                    &fixtures.iceberg_slow,
                    &workload.iceberg_query,
                ),
                (
                    &fixtures.paimon,
                    &fixtures.paimon_slow,
                    &workload.paimon_query,
                ),
            ] {
                for slow in [false, true] {
                    let sql = query.replace("${table}", &fixture.table_name(slow));
                    for repetition in 0..workload.repetitions {
                        context.action(format!(
                            "start short UEA-4A-4 window provider={} mode={} repetition={}",
                            fixture.provider,
                            if slow { "slow-remote" } else { "normal" },
                            repetition
                        ));
                        reports.push(run_window(
                            context,
                            &workload,
                            fixture.provider,
                            slow,
                            repetition,
                            &sql,
                            proxy,
                        )?);
                    }
                }
            }
            Ok(reports)
        })();
        let resource_path = context.scenario_root().join("process-resources.json");
        let resources = monitor.finish(&resource_path)?;
        let windows = execution?;
        ensure!(
            !resources.samples().is_empty(),
            "UEA-4A-4 collected no process samples"
        );
        let mut peak_rss_bytes_by_role = BTreeMap::new();
        for sample in resources.samples() {
            if let Some(rss) = sample.rss_bytes {
                let peak = peak_rss_bytes_by_role
                    .entry(sample.role.clone())
                    .or_insert(0_u64);
                *peak = (*peak).max(rss);
            }
        }
        ensure!(
            peak_rss_bytes_by_role.len() == 4,
            "UEA-4A-4 requires RSS samples for the FE and all three BEs"
        );
        let report = PerformanceReport {
            schema_version: 1,
            status: "short-regression-observed",
            server_binary_sha256: sha256_file(context.primary_binary())?,
            workload_sha256,
            iceberg_fixture_sha256: fixtures.iceberg.manifest_sha256.clone(),
            paimon_fixture_sha256: fixtures.paimon.manifest_sha256.clone(),
            effective_config_sha256: context
                .effective_launch_config_evidence()
                .semantics_sha256()
                .to_owned(),
            peak_rss_bytes_by_role,
            windows,
        };
        let path = context.scenario_root().join("uea4a4-performance.json");
        fs::write(&path, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("write UEA-4A-4 raw report {}", path.display()))?;
        context.action(format!(
            "wrote short UEA-4A-4 regression evidence to {}",
            path.display()
        ));
        ensure!(
            report
                .windows
                .iter()
                .all(|window| window.errors == 0 && window.completed > 0),
            "UEA-4A-4 short regression had query errors or an empty window"
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?
            .take();
        Ok(())
    }
}

fn credential_overlay(purpose: &str, name: &str, generation: &str) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{name}\"\ngeneration = \"{generation}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n"
    )
}

#[derive(Serialize)]
struct QueryLatency {
    total_micros: u128,
    completed_after_window: bool,
}

#[derive(Serialize)]
struct WindowReport {
    provider: &'static str,
    mode: &'static str,
    repetition: usize,
    duration_ms: u64,
    warmup_micros: u128,
    completed: usize,
    throughput_per_second: f64,
    p95_micros: u128,
    errors: usize,
    slow_gets: u64,
    slow_heads: u64,
    drain_micros: u128,
    samples: Vec<QueryLatency>,
}

#[derive(Serialize)]
struct PerformanceReport {
    schema_version: u32,
    status: &'static str,
    server_binary_sha256: String,
    workload_sha256: String,
    iceberg_fixture_sha256: String,
    paimon_fixture_sha256: String,
    effective_config_sha256: String,
    peak_rss_bytes_by_role: BTreeMap<String, u64>,
    windows: Vec<WindowReport>,
}

fn run_window(
    context: &mut ScenarioContext,
    workload: &Workload,
    provider: &'static str,
    slow: bool,
    repetition: usize,
    sql: &str,
    proxy: &DelayedS3Proxy,
) -> Result<WindowReport> {
    let timeout = context.remaining("connect UEA-4A-4 workload clients")?;
    let mut connections = Vec::with_capacity(workload.clients);
    for _ in 0..workload.clients {
        connections.push(mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            timeout,
        )?);
    }
    let (connections, warmup_micros) = warm_connections(
        connections,
        sql,
        workload.warmup_ms,
        workload.warehouse_concurrency_limit,
    )?;
    let before = proxy.snapshot();
    let gate = Arc::new(Barrier::new(workload.clients + 1));
    let deadline = Arc::new(OnceLock::new());
    let mut workers = Vec::with_capacity(workload.clients);
    for mut connection in connections {
        let gate = Arc::clone(&gate);
        let deadline = Arc::clone(&deadline);
        let sql = sql.to_owned();
        workers.push(thread::spawn(
            move || -> Result<(Vec<QueryLatency>, usize)> {
                gate.wait();
                let end = *deadline.get().context("UEA-4A-4 window deadline missing")?;
                let mut samples = Vec::new();
                let mut errors = 0;
                while Instant::now() < end {
                    let started = Instant::now();
                    match execute_query(&mut connection, &sql) {
                        Ok(_) => samples.push(QueryLatency {
                            total_micros: started.elapsed().as_micros(),
                            completed_after_window: Instant::now() > end,
                        }),
                        Err(_) => {
                            errors += 1;
                            break;
                        }
                    }
                }
                Ok((samples, errors))
            },
        ));
    }
    let duration = Duration::from_millis(workload.duration_ms);
    let end = Instant::now() + duration;
    deadline
        .set(end)
        .map_err(|_| anyhow::anyhow!("UEA-4A-4 window deadline already set"))?;
    gate.wait();
    while Instant::now() < end {
        thread::sleep(SAMPLE_INTERVAL.min(end.saturating_duration_since(Instant::now())));
    }
    let mut samples = Vec::new();
    let mut errors = 0;
    for worker in workers {
        let (worker_samples, worker_errors) = worker
            .join()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 client panicked"))??;
        samples.extend(worker_samples);
        errors += worker_errors;
    }
    let drain_micros = Instant::now().saturating_duration_since(end).as_micros();
    let after = proxy.snapshot();
    ensure!(
        after.upstream_errors == before.upstream_errors,
        "UEA-4A-4 delayed S3 proxy observed an upstream error"
    );
    let gets = after.gets.saturating_sub(before.gets);
    let heads = after.heads.saturating_sub(before.heads);
    if slow {
        ensure!(
            gets + heads > 0,
            "slow {provider} window never used delayed S3 GET/HEAD"
        );
    } else {
        ensure!(
            gets + heads == 0,
            "normal {provider} window used the delayed S3 endpoint"
        );
    }
    let mut latencies = samples
        .iter()
        .map(|sample| sample.total_micros)
        .collect::<Vec<_>>();
    let completed = samples
        .iter()
        .filter(|sample| !sample.completed_after_window)
        .count();
    Ok(WindowReport {
        provider,
        mode: if slow { "slow-remote" } else { "normal" },
        repetition,
        duration_ms: workload.duration_ms,
        warmup_micros,
        completed,
        throughput_per_second: completed as f64 / duration.as_secs_f64(),
        p95_micros: if latencies.is_empty() {
            0
        } else {
            percentile(&mut latencies, 95)
        },
        errors,
        slow_gets: gets,
        slow_heads: heads,
        drain_micros,
        samples,
    })
}

fn warm_connections(
    mut connections: Vec<mysql::Conn>,
    sql: &str,
    warmup_ms: u64,
    active_clients: usize,
) -> Result<(Vec<mysql::Conn>, u128)> {
    ensure!(
        active_clients > 0 && active_clients <= connections.len(),
        "UEA-4A-4 warmup client count exceeds the admitted slot count"
    );
    let idle = connections.split_off(active_clients);
    let gate = Arc::new(Barrier::new(connections.len() + 1));
    let deadline = Arc::new(OnceLock::new());
    let mut workers = Vec::with_capacity(connections.len());
    for mut connection in connections {
        let gate = Arc::clone(&gate);
        let deadline = Arc::clone(&deadline);
        let sql = sql.to_owned();
        workers.push(thread::spawn(move || -> Result<mysql::Conn> {
            gate.wait();
            let end = *deadline.get().context("UEA-4A-4 warmup deadline missing")?;
            while Instant::now() < end {
                execute_query(&mut connection, &sql).context("warm real UEA-4A-4 provider")?;
            }
            Ok(connection)
        }));
    }
    let start = Instant::now();
    deadline
        .set(start + Duration::from_millis(warmup_ms))
        .map_err(|_| anyhow::anyhow!("UEA-4A-4 warmup deadline already set"))?;
    gate.wait();
    let mut warmed = Vec::with_capacity(workers.len());
    for worker in workers {
        warmed.push(
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("UEA-4A-4 warmup client panicked"))??,
        );
    }
    warmed.extend(idle);
    let elapsed = start.elapsed().as_micros();
    ensure!(
        elapsed >= Duration::from_millis(warmup_ms).as_micros(),
        "UEA-4A-4 warmup did not run for its fixed duration"
    );
    Ok((warmed, elapsed))
}

fn execute_query(connection: &mut mysql::Conn, sql: &str) -> Result<u128> {
    let started = Instant::now();
    let mut result = connection
        .query_iter(sql)
        .context("execute UEA-4A-4 provider SQL")?;
    let mut first_row = None;
    let mut rows = 0;
    for row in result.by_ref() {
        row.context("read UEA-4A-4 provider row")?;
        first_row.get_or_insert_with(|| started.elapsed().as_micros());
        rows += 1;
    }
    ensure!(rows > 0, "UEA-4A-4 provider query returned no rows");
    Ok(first_row.context("UEA-4A-4 provider query has no first row")?)
}

fn percentile(samples: &mut [u128], percentile: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * percentile / 100]
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
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
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(CatalogPlanningPerformance::new(true)),
        Box::new(CatalogPlanningPerformance::new(false)),
    ]
}
