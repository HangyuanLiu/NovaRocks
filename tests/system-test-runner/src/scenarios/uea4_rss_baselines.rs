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

//! I00 RSS baselines from immutable published Iceberg tables. These are G0
//! observations, not formal G0-G3 acceptance receipts.

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
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

const BASELINE_ENV: &str = "NOVAROCKS_UEA4A2_BASELINE_MANIFEST";
const SMOKE_SECONDS_ENV: &str = "NOVAROCKS_UEA4A2_RSS_SMOKE_SECONDS";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_SECRET_ACCESS_KEY";
const INTERVAL: Duration = Duration::from_millis(100);
const IDLE: Duration = Duration::from_secs(2);
const POST: Duration = Duration::from_secs(2);
const CACHE_OVERLAY: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";
const CATALOG: &str = "uea4a2_rss_baseline";

#[derive(Clone, Copy)]
enum Kind {
    A4,
    Ssb,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::A4 => "uea4/rss-baseline-a4",
            Self::Ssb => "uea4/rss-baseline-ssb",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::A4 => "a4-small-files",
            Self::Ssb => "ssb-wide-row-group",
        }
    }
}

#[derive(Deserialize)]
struct BaselineManifest {
    schema_version: u32,
    fixture_kind: String,
    files: BTreeMap<String, PublishedFile>,
    a4_small_files: PublishedTable,
    ssb_wide_row_group: PublishedTable,
}

#[derive(Deserialize)]
struct PublishedFile {
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
struct PublishedTable {
    database: String,
    table: String,
    table_location: String,
    snapshot_id: i64,
    current_snapshot_data_file_count: u64,
    #[serde(default)]
    current_snapshot_records: Option<u64>,
    #[serde(default)]
    oracle: Option<PublishedOracle>,
    #[serde(default)]
    selected_data_object: Option<String>,
    #[serde(default)]
    selected_data_bytes: Option<u64>,
    #[serde(default)]
    selected_data_etag: Option<String>,
    #[serde(default)]
    selected_data_sha256: Option<String>,
    #[serde(default)]
    selected_data_rows: Option<u64>,
    #[serde(default)]
    table_count_oracle: Option<u64>,
    #[serde(default)]
    published_ssb_query_revenue_oracle: Option<u64>,
    #[serde(default)]
    warehouse: Option<String>,
}

#[derive(Deserialize)]
struct PublishedOracle {
    row_count: u64,
    id_sum: u64,
    value_sum: u64,
}

#[derive(Deserialize)]
struct A4Manifest {
    schema_version: u32,
    fixture_kind: String,
    database: String,
    table: String,
    table_location: String,
    snapshot_id: i64,
    data_file_count: u64,
    row_count: u64,
    rest_uri: String,
    warehouse_uri: String,
    s3_endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
    objects_sha256: String,
    artifacts: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct A4Object {
    key: String,
    etag: String,
    size: u64,
}

#[derive(Deserialize)]
struct WideOracle {
    schema_version: u32,
    snapshot_id: i64,
    row_count: u64,
    projection: Vec<String>,
    sum: BTreeMap<String, i128>,
    selected_file_sha256: String,
    selected_file_bytes: u64,
    selected_file_rows: u64,
    selected_file_row_groups: u64,
    selected_projected_column_chunk_compressed_bytes: BTreeMap<String, u64>,
    selected_projected_compressed_bytes: u64,
    sql_sha256: String,
    spark_output_sha256: String,
}

struct CheckedBaseline {
    digest: String,
    source: PublishedTable,
    a4: A4Manifest,
    a4_data_objects: Vec<String>,
    selected_object: Option<String>,
    expected: Vec<i128>,
    sql: String,
    projection: Vec<String>,
    selected_projected_compressed_bytes: Option<u64>,
    selected_column_chunk_bytes: BTreeMap<String, u64>,
    published_q11_sql: Option<String>,
    catalog_sql: String,
}

impl CheckedBaseline {
    fn load(kind: Kind) -> Result<Self> {
        let path = PathBuf::from(
            env::var(BASELINE_ENV).with_context(|| format!("{BASELINE_ENV} is required"))?,
        );
        let root = path.parent().context("baseline manifest has no parent")?;
        let bytes = fs::read(&path).context("read published baseline manifest")?;
        let digest = hash(&bytes);
        let baseline: BaselineManifest = serde_json::from_slice(&bytes)?;
        ensure!(
            baseline.schema_version == 1
                && baseline.fixture_kind == "uea4a2-existing-iceberg-baselines-v1",
            "wrong published RSS baseline manifest"
        );
        for (name, file) in &baseline.files {
            ensure!(
                !name.starts_with('/')
                    && !name.split('/').any(|part| part == ".." || part.is_empty())
                    && is_hash(&file.sha256),
                "invalid published baseline artifact path or hash"
            );
            let artifact = root.join(name);
            ensure!(
                fs::metadata(&artifact)?.len() == file.bytes
                    && hash_file(&artifact)? == file.sha256,
                "published RSS baseline artifact changed: {name}"
            );
        }
        let a4_path = root.join("a4/manifest.json");
        let a4_bytes = fs::read(&a4_path)?;
        let a4_digest = hash(&a4_bytes);
        ensure!(
            fs::read_to_string(root.join("a4/READY"))?.trim() == format!("sha256:{a4_digest}"),
            "A4 READY does not bind its exact manifest"
        );
        let a4: A4Manifest = serde_json::from_slice(&a4_bytes)?;
        let a4_source = &baseline.a4_small_files;
        ensure!(
            a4.schema_version == 1
                && a4.fixture_kind == "uea4a4-iceberg-performance-v1"
                && a4.database == a4_source.database
                && a4.table == a4_source.table
                && a4.table_location == a4_source.table_location
                && a4.snapshot_id == a4_source.snapshot_id
                && a4.data_file_count == 240
                && a4.data_file_count == a4_source.current_snapshot_data_file_count
                && a4.row_count == 768
                && a4_source.current_snapshot_records == Some(a4.row_count)
                && a4_source.oracle.as_ref().is_some_and(|oracle| {
                    oracle.row_count == 768 && oracle.id_sum == 294528 && oracle.value_sum == 8832
                }),
            "A4 published table, snapshot, or row oracle differs from its READY fixture"
        );
        for (name, expected) in &a4.artifacts {
            ensure!(
                !name.contains('/') && !name.contains("..") && is_hash(expected),
                "invalid A4 artifact entry"
            );
            ensure!(
                hash_file(&root.join("a4").join(name))? == *expected,
                "A4 artifact hash mismatch: {name}"
            );
        }
        ensure!(
            a4.artifacts.get("objects.json") == Some(&a4.objects_sha256),
            "A4 object inventory is not published"
        );
        let a4_oracle: Value = serde_json::from_slice(&fs::read(root.join("a4/oracle.json"))?)?;
        ensure!(
            a4_oracle["row_count"] == 768
                && a4_oracle["id_sum"] == 294528
                && a4_oracle["value_sum"] == 8832,
            "A4 exact published oracle changed"
        );
        let a4_objects: Vec<A4Object> =
            serde_json::from_slice(&fs::read(root.join("a4/objects.json"))?)?;
        let mut object_keys = BTreeSet::new();
        let a4_data_objects: Vec<String> = a4_objects
            .iter()
            .filter(|object| object.key.starts_with("data/") && object.key.ends_with(".parquet"))
            .map(|object| format!("{}/{}", a4.table_location, object.key))
            .collect();
        ensure!(
            a4_data_objects.len() == 240
                && a4_objects.iter().all(|object| {
                    !object.key.contains("..")
                        && object_keys.insert(&object.key)
                        && !object.etag.is_empty()
                        && object.size > 0
                }),
            "A4 exact object inventory is invalid"
        );

        let source = match kind {
            Kind::A4 => baseline.a4_small_files,
            Kind::Ssb => baseline.ssb_wide_row_group,
        };
        let (
            expected,
            sql,
            projection,
            selected_projected_compressed_bytes,
            selected_column_chunk_bytes,
            selected_object,
            published_q11_sql,
            catalog_sql,
        ) = match kind {
            Kind::A4 => {
                let sql = format!(
                    "SELECT COUNT(*), SUM(id), SUM(value) FROM {CATALOG}.{}.{}",
                    source.database, source.table
                );
                let catalog = catalog_sql(&a4, "rest", &a4.warehouse_uri);
                (
                    vec![768, 294528, 8832],
                    sql,
                    vec!["id".to_owned(), "value".to_owned()],
                    None,
                    BTreeMap::new(),
                    None,
                    None,
                    catalog,
                )
            }
            Kind::Ssb => {
                let ready: Value = serde_json::from_slice(&fs::read(root.join("ssb/READY.json"))?)?;
                let published = fs::read_to_string(root.join("ssb/published-manifest.txt"))?;
                let published: Value = serde_json::from_str(
                    published
                        .lines()
                        .next()
                        .context("SSB published manifest is empty")?,
                )?;
                let warehouse = source
                    .warehouse
                    .as_deref()
                    .context("SSB warehouse absent")?;
                ensure!(
                    ready["state"] == "ReadyValid"
                        && ready["exact_warehouse"] == warehouse
                        && ready["dataset_key"]["suite"] == "ssb"
                        && ready["dataset_key"]["scale"] == "1"
                        && published["database"] == source.database
                        && published["dataset_key"] == ready["dataset_key"]
                        && source.database == "ssb"
                        && source.table == "lineorder"
                        && source.current_snapshot_data_file_count == 8
                        && source.table_count_oracle == Some(6_001_171)
                        && source.published_ssb_query_revenue_oracle == Some(219_159_726_134),
                    "SSB READY, published manifest, table, or oracle mismatch"
                );
                let selected = source
                    .selected_data_object
                    .as_deref()
                    .context("SSB selected data object absent")?;
                let spark: Value =
                    serde_json::from_slice(&fs::read(root.join("ssb/spark-manifest.json"))?)?;
                ensure!(
                    selected.starts_with(&format!("{}/data/", source.table_location))
                        && source.selected_data_bytes == Some(20_324_223)
                        && source.selected_data_rows == Some(832_000)
                        && source
                            .selected_data_etag
                            .as_ref()
                            .is_some_and(|v| !v.is_empty())
                        && source
                            .selected_data_sha256
                            .as_ref()
                            .is_some_and(|v| is_hash(v))
                        && spark["source_object"] == selected
                        && spark["sha256"]
                            == source.selected_data_sha256.as_deref().unwrap_or_default()
                        && spark["file_bytes"] == 20_324_223
                        && spark["rows"] == 832_000
                        && spark["row_groups"] == 1
                        && spark["ready_sha256"] == hash_file(&root.join("ssb/READY.json"))?,
                    "SSB selected wide-row-group object differs from the published fixture"
                );
                let q11 = fs::read_to_string(root.join("ssb/q1.1.sql"))?;
                let q11_result = fs::read_to_string(root.join("ssb/q1.1.result"))?;
                ensure!(
                    q11.to_ascii_lowercase().contains("sum(lo_revenue)")
                        && q11
                            .to_ascii_lowercase()
                            .contains("from lineorder join dates")
                        && q11_result.trim() == "revenue\n219159726134",
                    "SSB published Q1.1 SQL is not the expected data scan"
                );
                let catalog = catalog_sql(&a4, "hadoop", warehouse);
                let published_q11_sql = format!(
                    "SELECT SUM(lo_revenue) FROM {CATALOG}.ssb.lineorder JOIN {CATALOG}.ssb.dates ON lo_orderdate = d_datekey WHERE d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25"
                );
                let wide: WideOracle = serde_json::from_slice(&fs::read(
                    root.join("ssb/wide-projection-oracle.json"),
                )?)?;
                let wide_sql = fs::read_to_string(root.join("ssb/wide-projection.sql"))?;
                let spark_output =
                    fs::read_to_string(root.join("ssb/wide-projection-spark-output.txt"))?;
                let spark_version =
                    fs::read_to_string(root.join("ssb/wide-projection-spark-version.txt"))?;
                let projection = [
                    "lo_orderkey",
                    "lo_partkey",
                    "lo_ordtotalprice",
                    "lo_revenue",
                ];
                ensure!(
                    wide.schema_version == 1
                        && wide.snapshot_id == source.snapshot_id
                        && wide.row_count == 6_001_171
                        && wide
                            .projection
                            .iter()
                            .map(String::as_str)
                            .collect::<Vec<_>>()
                            == projection
                        && wide.selected_file_sha256
                            == source.selected_data_sha256.as_deref().unwrap_or_default()
                        && wide.selected_file_bytes == 20_324_223
                        && wide.selected_file_rows == 832_000
                        && wide.selected_file_row_groups == 1
                        && wide.selected_projected_compressed_bytes == 10_662_065
                        && wide
                            .selected_projected_column_chunk_compressed_bytes
                            .values()
                            .sum::<u64>()
                            == wide.selected_projected_compressed_bytes
                        && wide.sql_sha256 == hash(wide_sql.as_bytes())
                        && wide.spark_output_sha256 == hash(spark_output.as_bytes())
                        && spark_version.contains("version 3.5.5"),
                    "SSB wide projection lacks a bound independent Spark oracle"
                );
                let expected: Vec<i128> = std::iter::once(i128::from(wide.row_count))
                    .chain(
                        projection
                            .iter()
                            .map(|name| wide.sum.get(*name).copied().unwrap_or(i128::MIN)),
                    )
                    .collect();
                ensure!(
                    expected.iter().all(|value| *value != i128::MIN)
                        && wide.selected_projected_column_chunk_compressed_bytes.len() == 4
                        && projection.iter().all(|name| {
                            wide.selected_projected_column_chunk_compressed_bytes
                                .contains_key(*name)
                        })
                        && spark_output.contains(&format!(
                            "{}\t{}\t{}\t{}\t{}",
                            expected[0], expected[1], expected[2], expected[3], expected[4]
                        )),
                    "SSB Spark output does not contain the exact four-column aggregate"
                );
                let spark_statement = wide_sql
                    .lines()
                    .find(|line| line.starts_with("SELECT COUNT(*) AS row_count,"))
                    .context("SSB Spark full-scan SQL statement missing")?;
                ensure!(
                    spark_statement.contains("FROM uea4a2_ssb.ssb.lineorder;")
                        && wide_sql.contains("FROM uea4a2_ssb.ssb.lineorder.refs"),
                    "SSB Spark SQL does not address the published table and main ref"
                );
                let sql = spark_statement
                    .trim_end_matches(';')
                    .replace("uea4a2_ssb", CATALOG);
                (
                    expected,
                    sql,
                    wide.projection,
                    Some(wide.selected_projected_compressed_bytes),
                    wide.selected_projected_column_chunk_compressed_bytes,
                    Some(selected.to_owned()),
                    Some(published_q11_sql),
                    catalog,
                )
            }
        };
        ensure!(
            source.table_location.starts_with("s3://")
                && source
                    .database
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_')
                && source
                    .table
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_')
                && a4.s3_endpoint.starts_with("http://127.0.0.1:"),
            "published RSS baseline contains an invalid location or identifier"
        );
        Ok(Self {
            digest,
            source,
            a4,
            a4_data_objects,
            selected_object,
            expected,
            sql,
            projection,
            selected_projected_compressed_bytes,
            selected_column_chunk_bytes,
            published_q11_sql,
            catalog_sql,
        })
    }
}

fn catalog_sql(a4: &A4Manifest, kind: &str, warehouse: &str) -> String {
    let uri = if kind == "rest" {
        format!(",\"uri\"=\"{}\"", sql_string(&a4.rest_uri))
    } else {
        String::new()
    };
    format!(
        "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"{kind}\"{uri},\"iceberg.catalog.warehouse\"=\"{}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{}\",\"credential.object-store-metadata.generation\"=\"{}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"{}\",\"credential.object-store-data.generation\"=\"{}\")",
        sql_string(warehouse),
        "${proxy_endpoint}",
        sql_string(&a4.region),
        a4.credential_name,
        a4.credential_generation,
        a4.credential_name,
        a4.credential_generation,
    )
}

struct LiveBaseline {
    checked: CheckedBaseline,
    proxy: DelayedS3Proxy,
}

struct RssBaseline {
    kind: Kind,
    live: Mutex<Option<LiveBaseline>>,
}

impl RssBaseline {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            live: Mutex::new(None),
        }
    }
}

impl Scenario for RssBaseline {
    fn name(&self) -> &'static str {
        self.kind.name()
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile, _uea1: Option<&Path>) -> Result<()> {
        ensure!(
            profile == LaunchProfile::Performance,
            "UEA-4A-2 RSS baseline requires native performance profile"
        );
        CheckedBaseline::load(self.kind)?;
        smoke_seconds()?;
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let checked = CheckedBaseline::load(self.kind)?;
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: checked.a4.s3_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        if matches!(self.kind, Kind::A4) {
            for (index, object) in checked.a4_data_objects.iter().enumerate() {
                let path = object
                    .strip_prefix("s3://")
                    .context("A4 data object not S3")?;
                proxy.label_object(&format!("/{path}"), &format!("a4-data-{index:03}"))?;
            }
        }
        let access = env::var("AWS_S3_ACCESS_KEY_ID").context("RSS baseline needs S3 key")?;
        let secret =
            env::var("AWS_S3_SECRET_ACCESS_KEY").context("RSS baseline needs S3 secret")?;
        ensure!(
            !access.is_empty() && !secret.is_empty(),
            "empty S3 credentials"
        );
        let mut child = CrossProcessChildEnvironment::default();
        for values in [&mut child.fe, &mut child.be] {
            values.insert(ACCESS_KEY_ENV.to_owned(), access.clone());
            values.insert(SECRET_KEY_ENV.to_owned(), secret.clone());
        }
        let metadata = format!(
            "{}\n{CACHE_OVERLAY}",
            credential_overlay("object-store-metadata", &checked.a4)
        );
        let data = format!(
            "{}\n{CACHE_OVERLAY}",
            credential_overlay("object-store-data", &checked.a4)
        );
        *self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("RSS baseline lock poisoned"))? =
            Some(LiveBaseline { checked, proxy });
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
            "wrong profile"
        );
        let guard = self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("RSS baseline lock poisoned"))?;
        let live = guard
            .as_ref()
            .context("RSS baseline missing after launch")?;
        let smoke_seconds = smoke_seconds()?;
        let root = context.scenario_root().to_path_buf();
        let resource_path = root.join("resource-trace.json");
        let query_path = root.join("query-trace.json");
        let event_path = root.join("proxy-events.jsonl");
        let connection_path = root.join("proxy-connections.jsonl");
        let (stop_tx, stop_rx) = mpsc::channel();
        let (windows, rss) = thread::scope(|scope| -> Result<_> {
            let collector = scope.spawn(|| {
                persist_proxy_logs(
                    &live.proxy,
                    File::create(&event_path)?,
                    File::create(&connection_path)?,
                    stop_rx,
                )
            });
            let observed = (|| -> Result<_> {
                let mut conn = mysql_actor::connect(
                    context.mysql_user(),
                    context.mysql_port(),
                    context.remaining("connect RSS baseline client")?,
                )?;
                conn.query_drop(
                    live.checked
                        .catalog_sql
                        .replace("${proxy_endpoint}", live.proxy.endpoint()),
                )
                .context("create published Iceberg baseline catalog")?;
                preflight(&mut conn, &live.checked, self.kind)?;
                if matches!(self.kind, Kind::Ssb) {
                    label_ssb_data_objects(&mut conn, &live.checked, &live.proxy)?;
                }
                let monitor = ProcessResourceMonitor::start_with_identities_and_runner(
                    context.process_resource_identities()?,
                    context.name(),
                    INTERVAL,
                )?;
                let measured = (|| -> Result<Vec<WindowRecord>> {
                    let mut windows = Vec::new();
                    for repetition in 0..if smoke_seconds.is_some() { 1 } else { 3 } {
                        let duration = Duration::from_secs(smoke_seconds.unwrap_or(120));
                        context.action(format!(
                            "start published {} full-scan RSS window repetition={repetition} duration_seconds={}",
                            self.kind.label(), duration.as_secs()
                        ));
                        windows.push(run_window(
                            context,
                            &mut conn,
                            &live.checked,
                            &live.proxy,
                            &monitor,
                            repetition,
                            duration,
                        )?);
                    }
                    Ok(windows)
                })();
                let samples = monitor.finish(&resource_path)?;
                let windows = measured?;
                preflight(&mut conn, &live.checked, self.kind)
                    .context("published Iceberg baseline changed after the RSS windows")?;
                fs::write(&query_path, serde_json::to_vec_pretty(&windows)?)?;
                let rss = rss_summary(samples.samples(), &windows)?;
                Ok((windows, rss))
            })();
            drop(stop_tx);
            collector
                .join()
                .map_err(|_| anyhow::anyhow!("RSS baseline proxy collector panicked"))??;
            observed
        })?;
        let (data_objects, data_connections) =
            verify_data_events(&event_path, &windows, self.kind)?;
        let proxy_total = live.proxy.snapshot();
        ensure!(
            proxy_total.event_overflow == 0,
            "RSS baseline proxy event overflow after scenario"
        );
        let attachments = BTreeMap::from([
            ("resource_trace", attachment(&resource_path)?),
            ("query_trace", attachment(&query_path)?),
            ("proxy_events", attachment(&event_path)?),
            ("proxy_connections", attachment(&connection_path)?),
        ]);
        let receipt = Receipt {
            schema_version: 1,
            baseline_only: true,
            smoke_only: smoke_seconds.is_some(),
            workload: self.kind.label(),
            topology: [1, 3],
            fixture_manifest_sha256: live.checked.digest.clone(),
            snapshot_id: live.checked.source.snapshot_id,
            primary_binary_sha256: hash_file(context.primary_binary())?,
            runner_sha256: hash_file(&env::current_exe()?)?,
            base_config_sha256: hash_file(context.base_config_path())?,
            effective_config_sha256: context
                .effective_launch_config_evidence()
                .semantics_sha256()
                .to_owned(),
            projection: live.checked.projection.clone(),
            selected_projected_compressed_bytes: live.checked.selected_projected_compressed_bytes,
            selected_column_chunk_bytes: live.checked.selected_column_chunk_bytes.clone(),
            proxy_process_cpu_rss: "runner-process-upper-bound-includes-client-and-orchestration",
            proxy_total_gets: proxy_total.gets,
            proxy_total_upstream_connect_attempts: proxy_total.upstream_connect_attempts,
            proxy_total_upstream_connections_established: proxy_total
                .upstream_connections_established,
            proxy_total_upstream_http1_responses: proxy_total.upstream_http1_responses,
            proxy_total_upstream_http2_responses: proxy_total.upstream_http2_responses,
            proxy_total_upstream_other_protocol_responses: proxy_total
                .upstream_other_protocol_responses,
            observed_data_objects: data_objects.len(),
            data_connection_ids: data_connections.into_iter().collect(),
            windows,
            rss,
            attachments,
        };
        fs::write(
            root.join(if smoke_seconds.is_some() {
                "uea4a2-rss-smoke.json"
            } else {
                "uea4a2-rss-baseline.json"
            }),
            serde_json::to_vec_pretty(&receipt)?,
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        *self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("RSS baseline lock poisoned"))? = None;
        Ok(())
    }
}

fn verify_data_events(
    path: &Path,
    windows: &[WindowRecord],
    kind: Kind,
) -> Result<(BTreeSet<String>, BTreeSet<u64>)> {
    let mut objects = BTreeSet::new();
    let mut connections = BTreeSet::new();
    for line in BufReader::new(File::open(path)?).lines() {
        let event: Value = serde_json::from_str(&line?)?;
        let Some(label) = event["object_id"].as_str() else {
            continue;
        };
        let data = match kind {
            Kind::A4 => label.starts_with("a4-data-"),
            Kind::Ssb => label.starts_with("ssb-data-") || label == "ssb-selected-wide-row-group",
        };
        if !data || event["method"] != "GET" {
            continue;
        }
        let time = event["elapsed_millis"]
            .as_u64()
            .context("data event has no monotonic time")? as u128;
        if !windows.iter().any(|window| {
            time >= window.proxy_observation_start_ms && time <= window.proxy_observation_end_ms
        }) {
            continue;
        }
        let connection_id = event["connection_id"]
            .as_u64()
            .context("data GET has no connection identity")?;
        objects.insert(label.to_owned());
        connections.insert(connection_id);
    }
    let expected = match kind {
        Kind::A4 => 240,
        Kind::Ssb => 8,
    };
    ensure!(
        objects.len() == expected && !connections.is_empty(),
        "RSS baseline did not read every current data file in its measured windows"
    );
    if matches!(kind, Kind::Ssb) {
        ensure!(
            objects.contains("ssb-selected-wide-row-group"),
            "SSB selected wide-row-group object was not read"
        );
    }
    Ok((objects, connections))
}

fn preflight(conn: &mut mysql::Conn, checked: &CheckedBaseline, kind: Kind) -> Result<()> {
    let table = format!(
        "{CATALOG}.{}.{}",
        checked.source.database, checked.source.table
    );
    let snapshot_sql = format!("SELECT snapshot_id FROM {table}$refs WHERE name = 'main'");
    let snapshot: Option<i64> = conn.query_first(snapshot_sql)?;
    ensure!(
        snapshot == Some(checked.source.snapshot_id),
        "published Iceberg current snapshot changed"
    );
    let files_sql =
        format!("SELECT COUNT(*), SUM(record_count) FROM {table}$files WHERE content = 0");
    let rows = numbers(conn, &files_sql)?;
    let expected_rows = match kind {
        Kind::A4 => 768,
        Kind::Ssb => 6_001_171,
    };
    ensure!(
        rows == [
            i128::from(checked.source.current_snapshot_data_file_count),
            expected_rows
        ],
        "published Iceberg current data-file inventory changed"
    );
    if let Some(selected) = &checked.selected_object {
        let escaped = selected.replace('\'', "''");
        let alternate = selected.replacen("s3://", "s3a://", 1).replace('\'', "''");
        let selected_sql = format!(
            "SELECT COUNT(*), SUM(record_count) FROM {table}$files WHERE content = 0 AND file_path IN ('{escaped}', '{alternate}')"
        );
        ensure!(
            numbers(conn, &selected_sql)? == [1, 832_000],
            "selected SSB wide-row-group object left the current snapshot"
        );
    }
    ensure!(
        numbers(conn, &checked.sql)? == checked.expected,
        "published RSS baseline aggregate oracle mismatch"
    );
    if matches!(kind, Kind::Ssb) {
        ensure!(
            numbers(
                conn,
                checked
                    .published_q11_sql
                    .as_deref()
                    .context("SSB published Q1.1 SQL missing")?
            )? == [219_159_726_134],
            "SSB published Q1.1 revenue oracle mismatch"
        );
    }
    Ok(())
}

fn label_ssb_data_objects(
    conn: &mut mysql::Conn,
    checked: &CheckedBaseline,
    proxy: &DelayedS3Proxy,
) -> Result<()> {
    let table = format!(
        "{CATALOG}.{}.{}",
        checked.source.database, checked.source.table
    );
    let rows: Vec<mysql::Row> = conn.query(format!(
        "SELECT file_path FROM {table}$files WHERE content = 0"
    ))?;
    let mut objects = BTreeSet::new();
    for row in rows {
        let mysql::Value::Bytes(bytes) = row.as_ref(0).context("SSB file path absent")? else {
            bail!("SSB file path is not a string");
        };
        let uri = std::str::from_utf8(bytes)?.replacen("s3a://", "s3://", 1);
        ensure!(
            uri.starts_with(&format!("{}/data/", checked.source.table_location))
                && objects.insert(uri),
            "SSB current data object is outside the published table or duplicated"
        );
    }
    ensure!(
        objects.len() == 8
            && checked
                .selected_object
                .as_ref()
                .is_some_and(|selected| objects.contains(selected)),
        "SSB published selected object is absent from eight current data files"
    );
    for (index, object) in objects.iter().enumerate() {
        let path = object
            .strip_prefix("s3://")
            .context("SSB data object not S3")?;
        let label = if checked.selected_object.as_deref() == Some(object.as_str()) {
            "ssb-selected-wide-row-group".to_owned()
        } else {
            format!("ssb-data-{index:03}")
        };
        proxy.label_object(&format!("/{path}"), &label)?;
    }
    Ok(())
}

fn numbers(conn: &mut mysql::Conn, sql: &str) -> Result<Vec<i128>> {
    let row: mysql::Row = conn
        .query_first(sql)?
        .context("aggregate returned no row")?;
    (0..row.len())
        .map(|index| {
            let value = row.as_ref(index).context("aggregate column missing")?;
            match value {
                mysql::Value::UInt(number) => Ok(i128::from(*number)),
                mysql::Value::Int(number) => Ok(i128::from(*number)),
                mysql::Value::Bytes(bytes) => std::str::from_utf8(bytes)?
                    .parse::<i128>()
                    .context("aggregate is not an integer"),
                _ => bail!("aggregate is not an integer"),
            }
        })
        .collect()
}

#[derive(Clone, Serialize)]
struct Markers {
    idle_start_ms: u128,
    idle_end_ms: u128,
    warmup_start_ms: u128,
    warmup_end_ms: u128,
    measurement_start_ms: u128,
    measurement_end_ms: u128,
    tail_start_ms: u128,
    tail_end_ms: u128,
    post_start_ms: u128,
    post_end_ms: u128,
}

#[derive(Serialize)]
struct QueryRecord {
    started_ms: u128,
    ended_ms: u128,
    result: Vec<i128>,
}

#[derive(Serialize)]
struct WindowRecord {
    repetition: usize,
    duration_seconds: u64,
    expected: Vec<i128>,
    queries: Vec<QueryRecord>,
    markers: Markers,
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
    backend_tasks_created: [f64; 3],
}

fn run_window(
    context: &mut ScenarioContext,
    conn: &mut mysql::Conn,
    checked: &CheckedBaseline,
    proxy: &DelayedS3Proxy,
    monitor: &ProcessResourceMonitor,
    repetition: usize,
    duration: Duration,
) -> Result<WindowRecord> {
    let idle_start_ms = monitor.elapsed_millis();
    thread::sleep(IDLE);
    let idle_end_ms = monitor.elapsed_millis();
    let warmup_start_ms = monitor.elapsed_millis();
    ensure!(
        numbers(conn, &checked.sql)? == checked.expected,
        "warmup oracle mismatch"
    );
    let warmup_end_ms = monitor.elapsed_millis();
    let before = proxy.snapshot();
    let tasks_before = backend_task_counts(context)?;
    let proxy_observation_start_ms = proxy.elapsed_millis();
    let measurement_start_ms = monitor.elapsed_millis();
    let start = Instant::now();
    let mut queries = Vec::new();
    while start.elapsed() < duration {
        let started_ms = monitor.elapsed_millis();
        let result = numbers(conn, &checked.sql)?;
        let ended_ms = monitor.elapsed_millis();
        ensure!(
            result == checked.expected,
            "measurement aggregate oracle mismatch"
        );
        queries.push(QueryRecord {
            started_ms,
            ended_ms,
            result,
        });
    }
    let measurement_end_ms = measurement_start_ms + duration.as_millis();
    let tail_start_ms = measurement_end_ms;
    let tail_end_ms = monitor.elapsed_millis();
    let tasks_after = backend_task_counts(context)?;
    let mut backend_tasks_created = [0.0; 3];
    for index in 0..3 {
        ensure!(
            tasks_after[index] >= tasks_before[index],
            "backend task-created counter regressed"
        );
        backend_tasks_created[index] = tasks_after[index] - tasks_before[index];
    }
    ensure!(
        backend_tasks_created.iter().sum::<f64>() > 0.0,
        "RSS baseline observed no native BE task placement"
    );
    let after = proxy.snapshot();
    let proxy_observation_end_ms = proxy.elapsed_millis();
    let post_start_ms = monitor.elapsed_millis();
    thread::sleep(POST);
    let post_end_ms = monitor.elapsed_millis();
    ensure!(
        queries
            .iter()
            .any(|query| query.ended_ms <= measurement_end_ms),
        "RSS baseline had no completed aggregate inside its measurement window"
    );
    ensure!(
        after.gets > before.gets
            && after.upstream_errors == before.upstream_errors
            && after.event_overflow == before.event_overflow,
        "RSS baseline did not preserve complete real S3 request evidence"
    );
    Ok(WindowRecord {
        repetition,
        duration_seconds: duration.as_secs(),
        expected: checked.expected.clone(),
        queries,
        markers: Markers {
            idle_start_ms,
            idle_end_ms,
            warmup_start_ms,
            warmup_end_ms,
            measurement_start_ms,
            measurement_end_ms,
            tail_start_ms,
            tail_end_ms,
            post_start_ms,
            post_end_ms,
        },
        proxy_gets: after.gets - before.gets,
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
        proxy_event_overflow: after.event_overflow - before.event_overflow,
        proxy_observation_start_ms,
        proxy_observation_end_ms,
        backend_tasks_created,
    })
}

fn backend_task_counts(context: &mut ScenarioContext) -> Result<[f64; 3]> {
    let mut counts = [0.0; 3];
    for (index, count) in counts.iter_mut().enumerate() {
        *count = context
            .handle()
            .backend_task_execution_tasks_created(index)?;
        ensure!(
            count.is_finite() && *count >= 0.0,
            "invalid BE task-created counter"
        );
    }
    Ok(counts)
}

#[derive(Serialize)]
struct RssRound {
    idle_bytes: u64,
    peak_bytes: u64,
    steady_bytes: u64,
    post_drain_bytes: u64,
    cpu_user_nanos: u64,
    cpu_system_nanos: u64,
}

fn rss_summary(
    samples: &[ProcessResourceSample],
    windows: &[WindowRecord],
) -> Result<BTreeMap<String, Vec<RssRound>>> {
    let mut by_role = BTreeMap::new();
    for role in ["be-0", "be-1", "be-2", "runner-proxy"] {
        let mut rounds = Vec::new();
        for window in windows {
            let m = &window.markers;
            let idle = sample_range(samples, role, m.idle_start_ms, m.idle_end_ms)?;
            let active = sample_range(samples, role, m.measurement_start_ms, m.tail_end_ms)?;
            let peak = sample_range(samples, role, m.warmup_start_ms, m.post_end_ms)?;
            let steady = sample_range(
                samples,
                role,
                (m.measurement_start_ms + m.measurement_end_ms) / 2,
                m.measurement_end_ms,
            )?;
            let post = sample_range(samples, role, m.post_start_ms, m.post_end_ms)?;
            let cpu_start = active.first().context("missing initial CPU sample")?;
            let cpu_end = active.last().context("missing final CPU sample")?;
            rounds.push(RssRound {
                idle_bytes: median_rss(&idle)?,
                peak_bytes: peak
                    .iter()
                    .map(|sample| sample.rss_bytes.context("RSS sample unavailable"))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .max()
                    .context("warmup-through-post RSS samples missing")?,
                steady_bytes: median_rss(&steady)?,
                post_drain_bytes: median_rss(&post)?,
                cpu_user_nanos: cpu_end
                    .cpu_user_nanos
                    .context("final user CPU unavailable")?
                    .checked_sub(
                        cpu_start
                            .cpu_user_nanos
                            .context("initial user CPU unavailable")?,
                    )
                    .context("user CPU counter regressed")?,
                cpu_system_nanos: cpu_end
                    .cpu_system_nanos
                    .context("final system CPU unavailable")?
                    .checked_sub(
                        cpu_start
                            .cpu_system_nanos
                            .context("initial system CPU unavailable")?,
                    )
                    .context("system CPU counter regressed")?,
            });
        }
        by_role.insert(role.to_owned(), rounds);
    }
    Ok(by_role)
}

fn sample_range<'a>(
    samples: &'a [ProcessResourceSample],
    role: &str,
    start: u128,
    end: u128,
) -> Result<Vec<&'a ProcessResourceSample>> {
    let matching: Vec<_> = samples
        .iter()
        .filter(|sample| {
            sample.role == role && sample.elapsed_millis >= start && sample.elapsed_millis <= end
        })
        .collect();
    ensure!(
        !matching.is_empty(),
        "missing {role} resource samples in {start}..{end}"
    );
    Ok(matching)
}

fn median_rss(samples: &[&ProcessResourceSample]) -> Result<u64> {
    let mut values: Vec<_> = samples
        .iter()
        .map(|sample| sample.rss_bytes.context("RSS sample unavailable"))
        .collect::<Result<_>>()?;
    values.sort_unstable();
    Ok(values[values.len() / 2])
}

#[derive(Serialize)]
struct Attachment {
    path: String,
    sha256: String,
}

#[derive(Serialize)]
struct Receipt {
    schema_version: u32,
    baseline_only: bool,
    smoke_only: bool,
    workload: &'static str,
    topology: [usize; 2],
    fixture_manifest_sha256: String,
    snapshot_id: i64,
    primary_binary_sha256: String,
    runner_sha256: String,
    base_config_sha256: String,
    effective_config_sha256: String,
    projection: Vec<String>,
    selected_projected_compressed_bytes: Option<u64>,
    selected_column_chunk_bytes: BTreeMap<String, u64>,
    proxy_process_cpu_rss: &'static str,
    proxy_total_gets: u64,
    proxy_total_upstream_connect_attempts: u64,
    proxy_total_upstream_connections_established: u64,
    proxy_total_upstream_http1_responses: u64,
    proxy_total_upstream_http2_responses: u64,
    proxy_total_upstream_other_protocol_responses: u64,
    observed_data_objects: usize,
    data_connection_ids: Vec<u64>,
    windows: Vec<WindowRecord>,
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
        sha256: hash_file(path)?,
    })
}

fn persist_proxy_logs(
    proxy: &DelayedS3Proxy,
    events: File,
    connections: File,
    stop: mpsc::Receiver<()>,
) -> Result<()> {
    let mut events = BufWriter::new(events);
    let mut connections = BufWriter::new(connections);
    let mut last = Instant::now();
    let mut previous_requests = 0;
    loop {
        let stopped = match stop.recv_timeout(INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => true,
            Err(RecvTimeoutError::Timeout) => false,
        };
        let snapshot = proxy.snapshot();
        ensure!(
            snapshot.event_overflow == 0,
            "RSS baseline proxy event overflow"
        );
        let requests = snapshot.gets.saturating_add(snapshot.heads);
        if stopped
            || last.elapsed() >= Duration::from_secs(1)
            || requests.saturating_sub(previous_requests) >= 4096
        {
            for event in proxy.take_event_log()? {
                serde_json::to_writer(
                    &mut events,
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
                )?;
                events.write_all(b"\n")?;
            }
            for event in proxy.take_connection_log()? {
                serde_json::to_writer(
                    &mut connections,
                    &serde_json::json!({
                        "kind": format!("{:?}", event.kind),
                        "elapsed_millis": event.elapsed_millis,
                        "connection_id": event.connection_id,
                    }),
                )?;
                connections.write_all(b"\n")?;
            }
            events.flush()?;
            connections.flush()?;
            last = Instant::now();
            previous_requests = requests;
        }
        if stopped {
            ensure!(
                proxy.snapshot().event_overflow == 0,
                "RSS baseline proxy event overflow"
            );
            return Ok(());
        }
    }
}

fn credential_overlay(purpose: &str, fixture: &A4Manifest) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{}\"\ngeneration = \"{}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n",
        fixture.credential_name, fixture.credential_generation
    )
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn smoke_seconds() -> Result<Option<u64>> {
    match env::var(SMOKE_SECONDS_ENV) {
        Ok(value) => {
            let seconds: u64 = value
                .parse()
                .with_context(|| format!("{SMOKE_SECONDS_ENV} must be an integer"))?;
            ensure!(
                (15..=90).contains(&seconds),
                "{SMOKE_SECONDS_ENV} must be between 15 and 90"
            );
            Ok(Some(seconds))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).context("read RSS baseline smoke duration"),
    }
}

fn is_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|c| c.is_ascii_hexdigit())
}

fn hash(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

fn hash_file(path: &Path) -> Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(RssBaseline::new(Kind::A4)),
        Box::new(RssBaseline::new(Kind::Ssb)),
    ]
}
