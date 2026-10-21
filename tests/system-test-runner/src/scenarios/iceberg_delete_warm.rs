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
use novarocks_cluster_harness::fixture_run::{RunBudget, canonical_s3};
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
    run_budget: Option<RunBudget>,
    machine: Option<MachineBinding>,
}

struct MachineBinding {
    root: String,
    daemon: String,
    version: String,
    generation: String,
}

impl MachineBinding {
    fn from_environment(budget: &RunBudget) -> Result<Self> {
        let read = |name: &str| -> Result<String> {
            let value = env::var(name)
                .with_context(|| format!("warm scratch writes require original {name}"))?;
            ensure!(!value.is_empty(), "warm scratch binding omitted {name}");
            Ok(value)
        };
        let binding = Self {
            root: read("NOVA_FIXTURE_MACHINE_ROOT")?,
            daemon: read("NOVA_FIXTURE_MACHINE_DAEMON")?,
            version: read("NOVA_FIXTURE_MACHINE_VERSION")?,
            generation: read("NOVA_FIXTURE_MACHINE_GENERATION")?,
        };
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(read("NOVA_ENV_MANIFEST")?)?)?;
        let machine = &manifest["runtime"]["machine"];
        budget.validate_machine(machine)?;
        ensure!(
            machine["control_root"] == binding.root
                && machine["daemon_id"] == binding.daemon
                && machine["version"] == binding.version
                && machine["generation"] == binding.generation,
            "warm scratch binding differs from its immutable publication"
        );
        Ok(binding)
    }
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
            run_budget: None,
            machine: None,
        })
    }
}

#[derive(Default)]
struct IcebergDeleteWarmConcurrent {
    fixture: Mutex<Option<WarmFixture>>,
}
impl Scenario for IcebergDeleteWarmConcurrent {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "iceberg_delete_warm",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
    fn launch_config_with_fixture_run(
        &self,
        root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        budget.check_work()?;
        let machine = MachineBinding::from_environment(budget)?;
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()?;
        let publication = PathBuf::from(env::var("NOVA_ENV_REST_ENV_FILE")?).canonicalize()?;
        novarocks_cluster_harness::fixture_run::startup_sweep(&repo, &publication, budget)?;
        let config = self.launch_config(root)?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?;
        let fixture = slot
            .as_mut()
            .context("warm delete fixture was not prepared")?;
        fixture.run_budget = Some(budget.clone());
        fixture.machine = Some(machine);
        Ok(config)
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
        let budget = fixture
            .run_budget
            .as_ref()
            .context("warm delete writes require the original fixture budget")?;
        let machine = fixture
            .machine
            .as_ref()
            .context("warm delete omitted original machine binding")?;
        budget.check_work()?;
        let namespace = budget.identity().resource_name("warm")?;
        let scope = format!("{}/warm", budget.identity().scope_root);
        let table = format!("{CATALOG}.{namespace}.visible_rows");
        let mut registration = novarocks_cluster_harness::resource_registration::register(
            context.scenario_root(),
            "warm-delete-scratch",
            &namespace,
            &scope,
        )?;
        control.query_drop(format!("CREATE DATABASE {CATALOG}.{namespace}"))?;
        let mut location_verified = false;
        let private_operation = (|| -> Result<()> {
            // SQL has no namespace LOCATION clause. Keep its DDL oracle and
            // set the owned REST namespace location before the first table.
            bind_scratch_namespace(&fixture.rest_uri, &namespace, &scope, budget)?;
            control.query_drop(format!("CREATE TABLE {table} (id INT, p INT, value STRING) TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true')"))?;
            verify_scratch_table(
                &fixture.rest_uri,
                &namespace,
                "visible_rows",
                &scope,
                budget,
            )?;
            location_verified = true;
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
            ensure!(
                location_verified,
                "warm delete cleanup retains an unverified table location"
            );
            // Only the attempt's private namespace is mutable or cleaned up.
            control.query_drop(format!("DROP TABLE IF EXISTS {table} FORCE"))?;
            control.query_drop(format!("DROP DATABASE {CATALOG}.{namespace}"))?;
            cleanup_scratch_prefix(budget, machine, &scope, context.scenario_root())?;
            Ok(())
        })();
        let cleanup_registration = registration.settle(&cleanup);
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
        cleanup_registration?;
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

fn rest_client(budget: &RunBudget) -> Result<reqwest::blocking::Client> {
    budget.check_work()?;
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(budget.work_remaining())
        .build()
        .context("build bounded scratch namespace client")
}

fn bind_scratch_namespace(
    uri: &str,
    namespace: &str,
    scope: &str,
    budget: &RunBudget,
) -> Result<()> {
    let path = format!("{}/v1/namespaces/{namespace}", uri.trim_end_matches('/'));
    rest_client(budget)?
        .post(format!("{path}/properties"))
        .json(&json!({"updates":{"location":scope},"removals":[]}))
        .send()?
        .error_for_status()
        .context("bind warm delete namespace to its original scratch scope")?;
    let answer: serde_json::Value = rest_client(budget)?
        .get(path)
        .send()?
        .error_for_status()?
        .json()?;
    ensure!(
        answer["namespace"] == json!([namespace]) && answer["properties"]["location"] == scope,
        "warm delete namespace did not retain its exact scratch location"
    );
    Ok(())
}

fn verify_scratch_table(
    uri: &str,
    namespace: &str,
    table: &str,
    scope: &str,
    budget: &RunBudget,
) -> Result<()> {
    let answer: serde_json::Value = rest_client(budget)?
        .get(format!(
            "{}/v1/namespaces/{namespace}/tables/{table}",
            uri.trim_end_matches('/')
        ))
        .send()?
        .error_for_status()?
        .json()?;
    let location = answer["metadata"]["location"]
        .as_str()
        .context("warm delete table omitted actual location")?;
    verify_table_location(location, scope)
}

fn verify_table_location(location: &str, scope: &str) -> Result<()> {
    let location = canonical_s3(location)?;
    let scope = canonical_s3(scope)?;
    ensure!(
        location.starts_with(&format!("{scope}/")),
        "warm delete table escaped its original scratch scope"
    );
    Ok(())
}

fn cleanup_scratch_prefix(
    budget: &RunBudget,
    machine: &MachineBinding,
    scope: &str,
    root: &Path,
) -> Result<()> {
    ensure!(
        scope == format!("{}/warm", budget.identity().scope_root),
        "warm cleanup requires its exact original prefix"
    );
    let target = format!(
        "store/{}/",
        scope
            .strip_prefix("s3://")
            .context("warm cleanup has no S3 scope")?
    );
    let target = format!("'{}'", target.replace('\'', "'\"'\"'"));
    let script = format!(
        "export MC_HOST_store=\"http://${{MINIO_ROOT_USER}}:${{MINIO_ROOT_PASSWORD}}@minio:9000\"\nmc --json rm --recursive --force {target}\n"
    );
    let script_path = root.join("warm-scratch-cleanup.sh");
    fs::write(&script_path, script)?;
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let mut command = novarocks_test_support::ManagedCommand::new("python3");
    command
        .env_clear()
        .envs(env::vars_os().filter(|(key, _)| {
            matches!(
                key.to_str(),
                Some(
                    "PATH"
                        | "HOME"
                        | "TMPDIR"
                        | "DOCKER_CONTEXT"
                        | "DOCKER_HOST"
                        | "DOCKER_CONFIG"
                        | "DOCKER_TLS_VERIFY"
                        | "DOCKER_CERT_PATH"
                )
            )
        }))
        .arg(repo.join("docker/iceberg-rest/machine_mc.py"));
    for (argument, value) in [
        ("--root", &machine.root),
        ("--daemon", &machine.daemon),
        ("--expected-version", &machine.version),
        ("--expected-generation", &machine.generation),
    ] {
        command.arg(argument).arg(value);
    }
    command
        .args(["--cleanup", "--script-file"])
        .arg(&script_path)
        .env(
            novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV,
            budget.to_json()?,
        );
    let outcome = novarocks_test_support::capture_command_until(
        command,
        fs::File::create(root.join("warm-cleanup.stdout"))?,
        fs::File::create(root.join("warm-cleanup.stderr"))?,
        budget.deadline(),
    )?;
    ensure!(
        !outcome.timed_out && outcome.failures.is_empty() && outcome.output.status.success(),
        "warm scratch cleanup did not confirm actual mc exit; diagnostics={}",
        outcome.diagnostic_root().display()
    );
    outcome.release_diagnostics()?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutable_table_location_requires_a_canonical_descendant() {
        let scope = "s3://warehouse/worktrees/test/scratch/run/warm";
        assert!(verify_table_location(&format!("{scope}/visible_rows"), scope).is_ok());
        for location in [
            scope.to_owned(),
            format!("{scope}_other/visible_rows"),
            format!("{scope}/../benchmark/table"),
            format!("{scope}/%2e%2e/table"),
            format!("{scope}//table"),
            "s3://warehouse/benchmarks/table".to_owned(),
        ] {
            assert!(
                verify_table_location(&location, scope).is_err(),
                "{location}"
            );
        }
    }
}
