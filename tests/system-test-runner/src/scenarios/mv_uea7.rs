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

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail};
use mysql::Conn;
use mysql::prelude::Queryable;
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, ServerHandle,
};
use novarocks_sql_test_runner::publication_fault_fixture::{
    CatalogResponseLossControl, CatalogResponseLossFixture,
};
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

const CREDENTIAL_NAME: &str = "uea7-mv-data";
const CREDENTIAL_GENERATION: &str = "v1";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA7_MV_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA7_MV_S3_SECRET_ACCESS_KEY";
const POLL_INTERVAL: Duration = Duration::from_millis(250);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(MvManagementReadmission::default())]
}

/// Owns the private REST/S3 services from cluster launch through teardown.
/// A restarted FE must still see the same external catalog and object store.
pub(super) struct ManagedMvRestFixture {
    rest: IsolatedIcebergRestFixture,
    catalog_proxy: Option<CatalogResponseLossFixture>,
    create_catalog_sql: String,
}

impl ManagedMvRestFixture {
    pub(super) fn start(
        scenario_root: &Path,
        catalog: &str,
    ) -> Result<(Self, ScenarioLaunchConfig)> {
        let rest = IsolatedIcebergRestFixture::start(scenario_root)
            .context("start private Iceberg REST and MinIO fixture for managed MV")?;
        Self::from_rest(rest, catalog, None)
    }

    pub(super) fn start_with_catalog_proxy(
        scenario_root: &Path,
        catalog: &str,
    ) -> Result<(Self, ScenarioLaunchConfig)> {
        let rest = IsolatedIcebergRestFixture::start(scenario_root)
            .context("start private REST authority for commit response loss")?;
        let proxy = CatalogResponseLossFixture::start(rest.endpoints().rest_uri.clone())?;
        Self::from_rest(rest, catalog, Some(proxy))
    }

    fn from_rest(
        rest: IsolatedIcebergRestFixture,
        catalog: &str,
        catalog_proxy: Option<CatalogResponseLossFixture>,
    ) -> Result<(Self, ScenarioLaunchConfig)> {
        let endpoints = rest.endpoints().clone();
        let catalog_uri = catalog_proxy
            .as_ref()
            .map_or(endpoints.rest_uri.as_str(), |proxy| proxy.uri());
        let identity = rest.static_s3_identity();
        let create_catalog_sql = format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\
             \"type\"=\"iceberg\",\
             \"iceberg.catalog.type\"=\"rest\",\
             \"uri\"=\"{}\",\
             \"warehouse\"=\"{}\",\
             \"credential.object-store-metadata.consumer-role\"=\"frontend\",\
             \"credential.object-store-metadata.mode\"=\"static\",\
             \"credential.object-store-metadata.name\"=\"{CREDENTIAL_NAME}\",\
             \"credential.object-store-metadata.generation\"=\"{CREDENTIAL_GENERATION}\",\
             \"credential.object-store-data.consumer-role\"=\"backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{CREDENTIAL_NAME}\",\
             \"credential.object-store-data.generation\"=\"{CREDENTIAL_GENERATION}\",\
             \"aws.s3.endpoint\"=\"{}\",\
             \"aws.s3.region\"=\"us-east-1\",\
             \"aws.s3.enable_path_style_access\"=\"true\")",
            catalog_uri, endpoints.rest_warehouse, endpoints.minio_endpoint,
        );
        let mut child_environment = CrossProcessChildEnvironment::default();
        for child in [&mut child_environment.fe, &mut child_environment.be] {
            child.insert(ACCESS_KEY_ENV.to_string(), identity.access_key_id.clone());
            child.insert(
                SECRET_KEY_ENV.to_string(),
                identity.secret_access_key.clone(),
            );
        }
        let metadata_credential = format!(
            r#"
[[connector.credentials]]
purpose = "object-store-metadata"
name = "{CREDENTIAL_NAME}"
generation = "{CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "${{ENV:{ACCESS_KEY_ENV}}}"
access_key_secret = "${{ENV:{SECRET_KEY_ENV}}}"
"#
        );
        let data_credential =
            metadata_credential.replace("object-store-metadata", "object-store-data");
        let launch = ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(metadata_credential),
                be: Some(data_credential),
                ..Default::default()
            },
            ..Default::default()
        };
        Ok((
            Self {
                rest,
                catalog_proxy,
                create_catalog_sql,
            },
            launch,
        ))
    }

    pub(super) fn create_catalog_sql(&self) -> &str {
        &self.create_catalog_sql
    }

    pub(super) fn rest_uri(&self) -> &str {
        &self.rest.endpoints().rest_uri
    }

    pub(super) fn run_recursive_spark_until(
        &self,
        receipt_root: &Path,
        stage: &str,
        invocation: &str,
        deadline: std::time::Instant,
    ) -> Result<serde_json::Value> {
        let expected = recursive_spark_invocation(stage)?;
        if invocation != expected {
            bail!("recursive Spark invocation differs from its frozen stage");
        }
        // Compile the committed inputs into the runner. The isolated fixture's
        // workspace is its private runtime, not the repository source checkout.
        let inputs = [
            include_str!("../../../sql/fixtures/iceberg-delete-applicability/generate.scala"),
            include_str!("../../../sql/fixtures/mv-visible-content-encodings/fixture.scala"),
        ];
        let input_bytes = inputs.iter().try_fold(0usize, |n, input| {
            n.checked_add(input.len() + 1)
                .context("recursive input size overflow")
        })?;
        if input_bytes > 256 * 1024 {
            bail!("compiled recursive Spark inputs exceed their byte budget");
        }
        let mut script = String::with_capacity(input_bytes + 512);
        for input in inputs {
            script.push_str(input);
            script.push('\n');
        }
        script.push_str("try {\n  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, \"ns\", \"recursive_types\")\n  ");
        script.push_str(expected);
        script.push_str(
            "\n} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }\n",
        );
        if script.len() > 256 * 1024 || std::time::Instant::now() >= deadline {
            bail!("recursive Spark input exceeds its byte/time budget");
        }
        let directory = receipt_root.join("recursive-spark").join(stage);
        std::fs::create_dir_all(&directory)?;
        let job = self
            .rest
            .run_owned_spark_until(&script, &directory, deadline)?;
        if job.exit_code != 0 || !job.cleanup.confirmed_gone {
            bail!(
                "recursive Spark stage {stage} failed or cleanup is unconfirmed; see {}",
                directory.display()
            );
        }
        let output = read_recursive_file(&job.stdout_path, 4 * 1024 * 1024, deadline)?;
        let receipt = parse_recursive_spark_receipt(&output, stage)?;
        std::fs::write(
            directory.join("stage-receipt.json"),
            serde_json::to_vec_pretty(&receipt)?,
        )?;
        Ok(receipt)
    }

    pub(super) fn corrupt_private_visible_tuple(
        &self,
        namespace: &str,
        table: &str,
        timeout: Duration,
    ) -> Result<String> {
        super::mv_physical_corruption::replace_visible_value(
            self.rest_uri(),
            &self.rest.endpoints().minio_endpoint,
            self.rest.static_s3_identity(),
            namespace,
            table,
            timeout,
        )
    }

    pub(super) fn catalog_proxy_control(
        &self,
        deadline: std::time::Instant,
    ) -> Result<CatalogResponseLossControl> {
        self.catalog_proxy
            .as_ref()
            .context("managed MV catalog proxy is not installed")?
            .control(deadline)
    }

    pub(super) fn shutdown(&mut self) -> Result<()> {
        // Stop the transparent proxy before its authoritative downstream.
        self.catalog_proxy.take();
        self.rest
            .shutdown()
            .context("shutdown private managed MV REST fixture")
    }
}

#[derive(Default)]
struct MvManagementReadmission {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvManagementReadmission {
    fn name(&self) -> &'static str {
        "mv/management-readmission"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (rest, launch) = ManagedMvRestFixture::start(scenario_root, "uea7_mv_readmission")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *fixture = Some(rest);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        execute(
            context,
            &mut conn,
            "create private REST catalog",
            &create_catalog_sql,
        )?;
        execute(
            context,
            &mut conn,
            "create managed MV namespace",
            "CREATE DATABASE uea7_mv_readmission.ns",
        )?;
        execute(
            context,
            &mut conn,
            "create row-lineage source table",
            "CREATE TABLE uea7_mv_readmission.ns.orders (k1 INT, v2 BIGINT) \
             TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
        )?;
        execute(
            context,
            &mut conn,
            "insert source rows",
            "INSERT INTO uea7_mv_readmission.ns.orders VALUES (1, 10), (2, 20)",
        )?;
        select_catalog_and_namespace(context, &mut conn, "uea7_mv_readmission")?;
        execute(
            context,
            &mut conn,
            "create document-managed Iceberg MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') \
             AS SELECT k1, SUM(v2) AS total FROM orders GROUP BY k1",
        )?;
        execute(
            context,
            &mut conn,
            "publish first MV snapshot",
            "REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE",
        )?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read first publication before FE restart",
        )?;
        require_status_phase(
            context,
            &mut conn,
            "uea7_mv_readmission",
            "orders_mv",
            "MANAGEABLE",
            "verify the original FE manages the published MV",
        )?;
        drop(conn);

        context.action("restart FE while preserving the private REST catalog and its MV documents");
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE with the same private REST catalog")?;
        let mut conn = connect(context)?;
        let closed = wait_for_status_phase(
            context,
            &mut conn,
            "uea7_mv_readmission",
            "orders_mv",
            "AWAITING_EFFECT_SETTLEMENT",
            "wait for the new FE to discover the closed MV",
        )?;
        require_property(&closed, "UnsettledEffects", "1")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM uea7_mv_readmission.ns.orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read lake publication while management is closed",
        )?;
        select_catalog_and_namespace(context, &mut conn, "uea7_mv_readmission")?;
        context.action("verify refresh is refused before readmission");
        let error = match conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE") {
            Err(error) => error,
            Ok(()) => bail!(
                "closed management accepted refresh before readmission; {}",
                context.diagnostics()
            ),
        };
        if !error
            .to_string()
            .contains("MV target requires a successful fresh Current observation")
        {
            bail!(
                "closed MV refresh returned an unexpected error {error}; {}",
                context.diagnostics()
            );
        }

        // The challenge is process-local and one-use. Read a fresh status,
        // then declare exactly the incarnation that status says is unsettled.
        let status = status(context, &mut conn, "uea7_mv_readmission", "orders_mv")?;
        require_property(&status, "Phase", "AWAITING_EFFECT_SETTLEMENT")?;
        let challenge = property(&status, "Challenge")?;
        let previous_incarnation = property(&status, "UnsettledEffect1Incarnation")?;
        let resume_sql = format!(
            "CALL novarocks_mv_resume_management('uea7_mv_readmission', 'ns', 'orders_mv', \
             '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
             'the system scenario replaced the declared frontend process before this statement')"
        );
        context.action("declare the previous FE incarnation isolated and readmit the MV");
        let resumed: Vec<(String, Option<String>)> = conn
            .query(resume_sql)
            .context("resume closed MV through the public management procedure")?;
        let settled = property(&resumed, "SettledEffects")?
            .parse::<usize>()
            .context("parse settled effect count")?;
        if settled != 1 {
            bail!(
                "readmission settled {settled} effects, expected one; {}",
                context.diagnostics()
            );
        }
        require_status_phase(
            context,
            &mut conn,
            "uea7_mv_readmission",
            "orders_mv",
            "MANAGEABLE",
            "verify management reopened after canonical Current readmission",
        )?;
        execute(
            context,
            &mut conn,
            "insert source row after readmission",
            "INSERT INTO orders VALUES (1, 5)",
        )?;
        execute(
            context,
            &mut conn,
            "publish an explicit full refresh after readmission",
            "REFRESH MATERIALIZED VIEW orders_mv FULL WITH SYNC MODE",
        )?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 15), (2, 20)],
            "read second publication after readmission",
        )?;
        context.action(
            "proved restart closure, exact operator readmission, and subsequent publication",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

pub(super) fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    if context.handle().be_count() != 3 {
        bail!(
            "{} requires native 1FE+3BE, but runner launched {} BE(s)",
            context.name(),
            context.handle().be_count()
        );
    }
    context.action("confirmed native 1FE+3BE topology");
    Ok(())
}

pub(super) fn connect(context: &mut ScenarioContext) -> Result<Conn> {
    let timeout = context.remaining("connect MySQL client")?;
    context.action("connect through public MySQL protocol");
    mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)
}

pub(super) fn execute(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    action: &str,
    sql: &str,
) -> Result<()> {
    context.remaining(action)?;
    context.action(action);
    conn.query_drop(sql)
        .with_context(|| format!("{action}: {sql}"))
}

pub(super) fn select_catalog_and_namespace(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select managed MV catalog",
        &format!("SET CATALOG {catalog}"),
    )?;
    execute(context, conn, "select managed MV namespace", "USE ns")
}

pub(super) fn assert_rows(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
) -> Result<()> {
    context.remaining(action)?;
    context.action(action);
    let actual: Vec<(i32, i64)> = conn
        .query(sql)
        .with_context(|| format!("{action}: {sql}"))?;
    if actual != expected {
        bail!(
            "{action} returned {actual:?}, expected {expected:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

pub(super) fn status(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<Vec<(String, Option<String>)>> {
    context.remaining("read managed MV status")?;
    conn.query(format!(
        "CALL novarocks_mv_management_status('{catalog}', 'ns', '{mv}')"
    ))
    .context("read managed MV status")
}

pub(super) fn property<'a>(rows: &'a [(String, Option<String>)], name: &str) -> Result<&'a str> {
    rows.iter()
        .find(|(key, _)| key == name)
        .and_then(|(_, value)| value.as_deref())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("managed MV status is missing {name}: {rows:?}"))
}

pub(super) fn require_property(
    rows: &[(String, Option<String>)],
    name: &str,
    expected: &str,
) -> Result<()> {
    let actual = property(rows, name)?;
    if actual != expected {
        bail!("managed MV {name} is {actual:?}, expected {expected:?}: {rows:?}");
    }
    Ok(())
}

pub(super) fn require_status_phase(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
    expected: &str,
    action: &str,
) -> Result<Vec<(String, Option<String>)>> {
    context.action(action);
    let rows = status(context, conn, catalog, mv)?;
    require_property(&rows, "Phase", expected)?;
    Ok(rows)
}

pub(super) fn wait_for_status_phase(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
    expected: &str,
    action: &str,
) -> Result<Vec<(String, Option<String>)>> {
    context.action(action);
    let mut last = String::new();
    while context.remaining(action).is_ok() {
        match status(context, conn, catalog, mv) {
            Ok(rows) if property(&rows, "Phase").ok() == Some(expected) => return Ok(rows),
            Ok(rows) => last = format!("{rows:?}"),
            Err(error) => last = error.to_string(),
        }
        thread::sleep(POLL_INTERVAL);
    }
    bail!(
        "timed out waiting for {action}: expected phase {expected}, last observation {last}; {}",
        context.diagnostics()
    )
}

fn recursive_spark_invocation(stage: &str) -> Result<&'static str> {
    match stage {
        "initialize" => Ok("RecursiveTypeFixture.initialize(\"ns\")"),
        "mutate" => Ok("RecursiveTypeFixture.mutate(\"ns\")"),
        "initial" => Ok("RecursiveTypeFixture.observe(\"ns\",\"initial\")"),
        "restored" => Ok("RecursiveTypeFixture.observe(\"ns\",\"restored\")"),
        "incremental" => Ok("RecursiveTypeFixture.observe(\"ns\",\"incremental\")"),
        "full" => Ok("RecursiveTypeFixture.observe(\"ns\",\"full\")"),
        _ => bail!("unknown frozen recursive Spark stage"),
    }
}

fn read_recursive_file(path: &Path, cap: usize, deadline: std::time::Instant) -> Result<Vec<u8>> {
    use std::io::Read;
    if std::time::Instant::now() >= deadline {
        bail!("recursive fixture absolute deadline elapsed");
    }
    let file = std::fs::File::open(path)
        .with_context(|| format!("open bounded recursive fixture file {}", path.display()))?;
    if file.metadata()?.len() > cap as u64 {
        bail!("recursive fixture file exceeds its byte budget");
    }
    let mut bytes = Vec::new();
    file.take(cap as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > cap || std::time::Instant::now() >= deadline {
        bail!("recursive fixture read exceeds its byte/time budget");
    }
    Ok(bytes)
}

fn parse_recursive_spark_receipt(output: &[u8], stage: &str) -> Result<serde_json::Value> {
    recursive_spark_invocation(stage)?;
    let expected_record = match stage {
        "initialize" => "recursive_source_initial",
        "mutate" => "recursive_source_changed",
        _ => "recursive_mv_observed",
    };
    if output.len() > 4 * 1024 * 1024 {
        bail!("recursive Spark log exceeds its byte budget");
    }
    let output = std::str::from_utf8(output).context("recursive Spark stdout is not UTF-8")?;
    let mut receipt = None;
    for line in output.lines() {
        let Some(payload) = line.strip_prefix("UEA4G_RECEIPT ") else {
            continue;
        };
        if payload.len() > 256 * 1024 {
            bail!("recursive Spark receipt exceeds its byte budget");
        }
        let value: serde_json::Value =
            serde_json::from_str(payload).context("malformed recursive Spark receipt")?;
        let record = value
            .get("record")
            .and_then(serde_json::Value::as_str)
            .context("recursive Spark receipt lacks a record identity")?;
        if record != expected_record {
            continue;
        }
        if matches!(stage, "initial" | "restored" | "incremental" | "full")
            && value.get("stage").and_then(serde_json::Value::as_str) != Some(stage)
        {
            bail!("recursive Spark observation stage differs from the frozen invocation");
        }
        if receipt.replace(value).is_some() {
            bail!("recursive Spark exact stage receipt is duplicated");
        }
    }
    receipt.context("recursive Spark exact stage receipt is absent")
}

#[cfg(test)]
mod recursive_spark_receipt_tests {
    use super::parse_recursive_spark_receipt;

    #[test]
    fn exact_stage_receipt_is_required_and_unique() {
        let valid = br#"noise
UEA4G_RECEIPT {"record":"runtime"}
UEA4G_RECEIPT {"record":"recursive_mv_observed","stage":"restored","snapshot":7}
"#;
        assert_eq!(
            parse_recursive_spark_receipt(valid, "restored").unwrap()["snapshot"],
            7
        );
        assert!(parse_recursive_spark_receipt(valid, "initial").is_err());
        assert!(
            parse_recursive_spark_receipt(b"UEA4G_RECEIPT {\"record\":\"runtime\"}\n", "initial")
                .is_err()
        );
        let mut duplicate = valid.to_vec();
        duplicate.extend_from_slice(valid);
        assert!(parse_recursive_spark_receipt(&duplicate, "restored").is_err());
    }

    #[test]
    fn malformed_and_oversized_receipts_fail_closed() {
        assert!(parse_recursive_spark_receipt(b"UEA4G_RECEIPT {bad}", "initialize").is_err());
        let huge = format!(
            "UEA4G_RECEIPT {{\"record\":\"runtime\",\"text\":\"{}\"}}",
            "x".repeat(256 * 1024)
        );
        assert!(parse_recursive_spark_receipt(huge.as_bytes(), "initialize").is_err());
        assert!(parse_recursive_spark_receipt(b"", "invented").is_err());
    }
}
