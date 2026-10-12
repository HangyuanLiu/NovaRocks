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

//! Warm, concurrent reads of independently generated delete families and
//! snapshot-specific visibility across a private DELETE publication.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::iceberg_delete_applicability::{
    CATALOG, create_catalog_sql_from_parts, credential_overlay_with_cache, table_name,
};
use super::provider_mysql_actors::run_checks;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::CrossProcessChildEnvironment;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const CORPUS_ENV: &str = "NOVAROCKS_ICEBERG_DELETE_CORPUS_RECEIPT";
const ACCESS_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_ACCESS";
const SECRET_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_SECRET";
const REPRESENTATIVE_CASES: [&str; 6] = [
    "same_commit_dv_equality",
    "same_commit_position_equality",
    "multi_target_position",
    "multi_blob_dv",
    "equality_null",
    "equality_promoted_int",
];
type RowBag = Vec<(Option<i64>, Option<i64>, Option<String>)>;

#[derive(Deserialize)]
struct CorpusReceipt {
    mode: String,
    runtime: CorpusRuntime,
    cases: Vec<CorpusCase>,
}
#[derive(Deserialize)]
struct CorpusRuntime {
    iceberg: String,
    requested_warehouse: String,
}
#[derive(Clone, Deserialize)]
struct CorpusCase {
    case: String,
    table: String,
    snapshot: i64,
    independent_expected_rows: RowBag,
    independent_oracle_matched: bool,
    java_row_read: JavaRowRead,
}
#[derive(Clone, Deserialize)]
struct JavaRowRead {
    status: String,
}
struct WarmFixture {
    receipt_path: PathBuf,
    receipt_sha256: String,
    cases: Vec<CorpusCase>,
    rest_uri: String,
    warehouse: String,
    s3_endpoint: String,
}

impl WarmFixture {
    fn load() -> Result<Self> {
        let receipt_path = PathBuf::from(
            env::var(CORPUS_ENV)
                .context("warm delete reads require the independently prepared corpus receipt")?,
        );
        let bytes = fs::read(&receipt_path)?;
        let receipt: CorpusReceipt = serde_json::from_slice(&bytes)?;
        ensure!(
            receipt.mode == "corpus" && receipt.runtime.iceberg.contains("1.11.0"),
            "warm delete reads require the official Java Iceberg corpus producer"
        );
        let mut cases = Vec::new();
        for name in REPRESENTATIVE_CASES {
            let selected = receipt
                .cases
                .iter()
                .filter(|case| case.case == name)
                .collect::<Vec<_>>();
            ensure!(
                selected.len() == 1,
                "corpus must contain one independently verified {name} case"
            );
            let case = selected[0];
            ensure!(
                case.independent_oracle_matched && case.java_row_read.status == "success",
                "corpus case {name} has no independently verified Java row bag"
            );
            table_name(&case.table)?;
            cases.push(case.clone());
        }
        let rest_uri = env::var("NOVAROCKS_ICEBERG_REST_URI")?;
        let s3_endpoint = env::var("AWS_S3_ENDPOINT")?;
        for endpoint in [&rest_uri, &s3_endpoint] {
            ensure!(
                endpoint.starts_with("http://127.0.0.1:")
                    || endpoint.starts_with("http://localhost:"),
                "warm delete fixture endpoint must be loopback"
            );
        }
        ensure!(
            receipt.runtime.requested_warehouse == env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")?,
            "corpus warehouse differs from the explicitly bound fixture publication"
        );
        Ok(Self {
            receipt_path,
            receipt_sha256: format!("{:x}", Sha256::digest(bytes)),
            cases,
            rest_uri,
            warehouse: receipt.runtime.requested_warehouse,
            s3_endpoint,
        })
    }
}

#[derive(Default)]
struct IcebergDeleteWarmConcurrent {
    fixture: Mutex<Option<WarmFixture>>,
}
impl Scenario for IcebergDeleteWarmConcurrent {
    fn name(&self) -> &'static str {
        "connector/iceberg-delete-warm-concurrent"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let fixture = WarmFixture::load()?;
        let access = env::var("AWS_S3_ACCESS_KEY_ID")?;
        let secret = env::var("AWS_S3_SECRET_ACCESS_KEY")?;
        ensure!(
            !access.is_empty() && !secret.is_empty(),
            "delete fixture credentials must be nonempty"
        );
        let mut child = CrossProcessChildEnvironment::default();
        for role in [&mut child.fe, &mut child.be] {
            role.insert(ACCESS_ENV.to_owned(), access.clone());
            role.insert(SECRET_ENV.to_owned(), secret.clone());
        }
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))? = Some(fixture);
        Ok(ScenarioLaunchConfig {
            child_environment: child,
            config_overlay: credential_overlay_with_cache(true),
            ..Default::default()
        })
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?;
        let fixture = slot
            .as_ref()
            .context("warm delete fixture was not prepared")?;
        let baseline = resource_baseline(context)?;
        let mut control = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect warm delete fixture")?,
        )?;
        control.query_drop("SET enable_scan_datacache = true")?;
        control.query_drop(create_catalog_sql_from_parts(
            &fixture.rest_uri,
            &fixture.warehouse,
            &fixture.s3_endpoint,
        ))?;
        let mut observations = Vec::new();
        for case in &fixture.cases {
            let table = table_name(&case.table)?;
            let sql = format!(
                "SELECT id, p, value FROM {table} FOR VERSION AS OF {}",
                case.snapshot
            );
            verify_row_bag(&mut control, &sql, &case.independent_expected_rows)?;
            let actors = (0..4)
                .map(|_| (sql.clone(), case.independent_expected_rows.clone()))
                .collect::<Vec<_>>();
            concurrent_row_bags(context, actors, &format!("delete-warm-{}", case.case))?;
            observations.push(
                json!({"case":case.case,"snapshot":case.snapshot,"warm_verified":true,"actors":4}),
            );
        }
        let attempt_identity = format!(
            "{:x}",
            Sha256::digest(context.scenario_root().as_os_str().as_encoded_bytes())
        );
        let namespace = format!("delete_visibility_{}", &attempt_identity[..16]);
        let table = format!("{CATALOG}.{namespace}.visible_rows");
        control.query_drop(format!("CREATE DATABASE {CATALOG}.{namespace}"))?;
        let private_operation = (|| -> Result<()> {
            control.query_drop(format!("CREATE TABLE {table} (id INT, p INT, value STRING) TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true')"))?;
            control.query_drop(format!(
                "INSERT INTO {table} VALUES (1,1,'one'),(2,1,'two'),(3,1,'three')"
            ))?;
            control.query_drop(format!("ALTER TABLE {table} CREATE TAG before_delete"))?;
            let old = format!("SELECT id,p,value FROM {table} FOR VERSION AS OF 'before_delete'");
            let latest = format!("SELECT id,p,value FROM {table}");
            let seed: RowBag = [(1, "one"), (2, "two"), (3, "three")]
                .into_iter()
                .map(|(id, value)| (Some(id), Some(1), Some(value.to_owned())))
                .collect();
            let surviving: RowBag = [(1, "one"), (3, "three")]
                .into_iter()
                .map(|(id, value)| (Some(id), Some(1), Some(value.to_owned())))
                .collect();
            verify_row_bag(&mut control, &old, &seed)?;
            verify_row_bag(&mut control, &latest, &seed)?;
            control.query_drop(format!("DELETE FROM {table} WHERE id = 2"))?;
            concurrent_row_bags(
                context,
                vec![
                    (old.clone(), seed.clone()),
                    (latest.clone(), surviving.clone()),
                    (old, seed),
                    (latest, surviving),
                ],
                "delete-snapshot-visibility",
            )?;
            context.action("pinned and warmed old snapshot before DELETE; concurrent old/latest actors independently checked their snapshot-specific row bags");
            Ok(())
        })();
        let cleanup = (|| -> Result<()> {
            // Only the attempt's private namespace is mutable or cleaned up.
            control.query_drop(format!("DROP TABLE IF EXISTS {table} FORCE"))?;
            control.query_drop(format!("DROP DATABASE {CATALOG}.{namespace}"))?;
            Ok(())
        })();
        drop(control);
        let exit = await_resource_convergence(
            context,
            &baseline,
            "warm delete and snapshot visibility readers",
        );
        fs::write(
            context.scenario_root().join("delete-warm-concurrent.json"),
            serde_json::to_vec_pretty(&json!({
                "corpus_receipt":fixture.receipt_path,"corpus_sha256":fixture.receipt_sha256,
                "observations":observations,"private_operation":private_operation.as_ref().err().map(ToString::to_string),
                "private_cleanup":cleanup.as_ref().err().map(ToString::to_string),
                "resource_exit":exit.as_ref().err().map(ToString::to_string),
            }))?,
        )?;
        private_operation?;
        cleanup?;
        exit?;
        context.action("warm concurrent equality, position and DV reads matched independent producer rows and native owners exited");
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?
            .take();
        Ok(())
    }
}
fn verify_row_bag(connection: &mut mysql::Conn, sql: &str, expected: &RowBag) -> Result<()> {
    let mut actual: RowBag = connection.query(sql)?;
    let mut expected = expected.clone();
    actual.sort();
    expected.sort();
    ensure!(
        actual == expected,
        "delete row bag differs from its independent input: actual={actual:?}, expected={expected:?}"
    );
    Ok(())
}
fn concurrent_row_bags(
    context: &mut ScenarioContext,
    queries: Vec<(String, RowBag)>,
    label: &str,
) -> Result<()> {
    let checks = queries
        .into_iter()
        .map(|(sql, expected)| {
            Box::new(move |connection: &mut mysql::Conn| -> Result<()> {
                connection.query_drop("SET enable_scan_datacache = true")?;
                verify_row_bag(connection, &sql, &expected)
            }) as Box<dyn FnOnce(&mut mysql::Conn) -> Result<()> + Send>
        })
        .collect();
    run_checks(context, label, checks)
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::<IcebergDeleteWarmConcurrent>::default()]
}
