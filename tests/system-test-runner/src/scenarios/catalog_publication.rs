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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicit publication properties exercised through native SQL and a real Java catalog.
//! The fixture observes exact service phases; the scenario owns every actor to actual exit.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::publication::PublicationFixture;
use novarocks_cluster_harness::publication_service::{CommitOutcome, HoldStage};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, ServerHandle,
};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const CATALOG: &str = "publication_catalog";
const EVIDENCE_MS: u64 = 500;
const TABLE: &str = "publication_catalog.ns.orders";

#[derive(Clone, Copy)]
enum Property {
    LateCommit,
    StreamDeadline,
    ExpiryConcurrency,
    ManifestReprepare,
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    [
        Property::LateCommit,
        Property::StreamDeadline,
        Property::ExpiryConcurrency,
        Property::ManifestReprepare,
    ]
    .into_iter()
    .map(|property| {
        Box::new(PublicationScenario {
            property,
            fixture: Mutex::new(None),
        }) as Box<dyn Scenario>
    })
    .collect()
}
struct PublicationScenario {
    property: Property,
    fixture: Mutex<Option<PublicationFixture>>,
}
impl Scenario for PublicationScenario {
    fn name(&self) -> &'static str {
        match self.property {
            Property::LateCommit => "catalog-publication/late-commit-observation",
            Property::StreamDeadline => "catalog-publication/stream-deadline",
            Property::ExpiryConcurrency => "catalog-publication/expiry-concurrency",
            Property::ManifestReprepare => "catalog-publication/manifest-reprepare",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?;
        ensure!(
            slot.is_none(),
            "publication fixture was prepared more than once"
        );
        let fixture = PublicationFixture::start(root)?;
        let identity = fixture.rest().static_s3_identity();
        let mut environment = CrossProcessChildEnvironment::default();
        for role in [&mut environment.fe, &mut environment.be] {
            role.insert(
                "NOVA_PUBLICATION_ACCESS_KEY".into(),
                identity.access_key_id.clone(),
            );
            role.insert(
                "NOVA_PUBLICATION_SECRET_KEY".into(),
                identity.secret_access_key.clone(),
            );
        }
        let credential = r#"
[[connector.credentials]]
purpose = "object-store-metadata"
name = "publication-fixture"
generation = "v1"
kind = "s3"
access_key_id = "${ENV:NOVA_PUBLICATION_ACCESS_KEY}"
access_key_secret = "${ENV:NOVA_PUBLICATION_SECRET_KEY}"
"#
        .to_owned();
        *slot = Some(fixture);
        Ok(ScenarioLaunchConfig {
            child_environment: environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(credential.clone()),
                be: Some(credential.replace("object-store-metadata", "object-store-data")),
                ..Default::default()
            },
            ..Default::default()
        })
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "publication scenarios require native 1FE+3BE"
        );
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?;
        let fixture = fixture
            .as_ref()
            .context("publication fixture not prepared")?;
        let budget = if matches!(
            self.property,
            Property::LateCommit | Property::StreamDeadline
        ) {
            EVIDENCE_MS
        } else {
            30_000
        };
        let connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        connection.run(|conn| {
            // Fixture preparation has a separate generous budget. Only the
            // selected publication consumes the deliberately short policy.
            conn.query_drop(catalog_sql(fixture, "publication_setup", 30_000))?;
            conn.query_drop("CREATE DATABASE publication_setup.ns")?;
            conn.query_drop("CREATE TABLE publication_setup.ns.orders (k INT) TBLPROPERTIES (\"format-version\"=\"2\", \"history.expire.min-snapshots-to-keep\"=\"1\")")?;
            conn.query_drop(catalog_sql(fixture, CATALOG, budget))?;
            match self.property {
                Property::LateCommit => late_commit(context, fixture),
                Property::StreamDeadline => stream_deadline(context, fixture),
                Property::ExpiryConcurrency | Property::ManifestReprepare => {
                    for value in 1..=4 { conn.query_drop(format!("INSERT INTO {TABLE} VALUES ({value})"))?; }
                    maintenance_race(context, fixture, self.property)
                }
            }
        })
    }
    fn teardown(&self) -> Result<()> {
        if let Some(mut fixture) = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?
            .take()
        {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

fn catalog_sql(fixture: &PublicationFixture, catalog: &str, budget: u64) -> String {
    let endpoints = fixture.rest().endpoints();
    format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
        "type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{}", "warehouse"="{}",
        "novarocks.publication.evidence-timeout-ms"="{budget}",
        "credential.object-store-metadata.consumer-role"="frontend", "credential.object-store-metadata.mode"="static",
        "credential.object-store-metadata.name"="publication-fixture", "credential.object-store-metadata.generation"="v1",
        "credential.object-store-data.consumer-role"="backend", "credential.object-store-data.mode"="static",
        "credential.object-store-data.name"="publication-fixture", "credential.object-store-data.generation"="v1",
        "aws.s3.endpoint"="{}", "aws.s3.region"="us-east-1", "aws.s3.enable_path_style_access"="true")"#,
        fixture.uri(),
        endpoints.rest_warehouse,
        endpoints.minio_endpoint
    )
}
fn sql(context: &ScenarioContext, statement: &str) -> Result<()> {
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|conn| conn.query_drop(statement).map_err(Into::into))
}
fn metadata(fixture: &PublicationFixture) -> Result<Value> {
    Ok(reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(format!(
            "{}/v1/namespaces/ns/tables/orders",
            fixture.rest().endpoints().rest_uri
        ))
        .send()?
        .error_for_status()?
        .json::<Value>()?["metadata"]
        .clone())
}
fn save(context: &ScenarioContext, name: &str, value: &Value) -> Result<()> {
    fs::write(
        context.scenario_root().join(name),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}
fn require_unknown(outcome: &Result<()>) -> Result<()> {
    let error = outcome
        .as_ref()
        .err()
        .context("publication unexpectedly returned success")?;
    ensure!(
        format!("{error:#}")
            .to_ascii_lowercase()
            .contains("unknown"),
        "SQL did not return Unknown: {error:#}"
    );
    Ok(())
}
fn late_commit(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let before = metadata(fixture)?;
    let control = fixture.proxy()?;
    let counts = control.mutation_counts("ns", "orders");
    let mut hold = fixture.service().arm("ns.orders")?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let started = Instant::now();
    let query = std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?.run(|conn| {
            conn.query_drop(format!("ALTER TABLE {TABLE} ADD COLUMN late_value BIGINT"))
                .map_err(Into::into)
        })
    });
    let phase = (|| -> Result<()> {
        hold.wait_until_held(context.deadline())?;
        // The server has checked requirements but has not committed. The real
        // FE automatically reconciles against this still-old metadata.
        ensure!(
            metadata(fixture)? == before,
            "metadata advanced before held commit"
        );
        Ok(())
    })();
    // For this property the SQL must finish while the original server request remains held.
    // A failure still releases before joining; no actor handle is detached.
    let query_result = if phase.is_ok() {
        query
            .join()
            .map_err(|_| anyhow::anyhow!("publication SQL actor panicked"))?
    } else {
        let _ = hold.release();
        let _ = query.join();
        phase?;
        unreachable!()
    };
    let assertions = (|| -> Result<()> {
        require_unknown(&query_result)?;
        ensure!(
            started.elapsed() >= Duration::from_millis(EVIDENCE_MS * 4 / 5),
            "SQL completed before evidence budget"
        );
        ensure!(
            metadata(fixture)? == before,
            "automatic reconcile saw a newer metadata version"
        );
        ensure!(
            control
                .mutation_counts("ns", "orders")
                .table_commit_forwarded
                == counts.table_commit_forwarded + 1,
            "Unknown dispatched another mutation"
        );
        ensure!(
            control
                .mutation_counts("ns", "orders")
                .table_load_while_commit_pending
                > counts.table_load_while_commit_pending,
            "FE did not load old metadata while the exact original commit remained pending"
        );

        Ok(())
    })();
    let release = hold.release();
    let exited = hold.wait_for_original_exit(context.deadline());
    fs::write(
        context.scenario_root().join("service-trace.ndjson"),
        hold.trace()?,
    )?;
    assertions?;
    release?;
    ensure!(
        exited? == CommitOutcome::Succeeded,
        "late original request did not commit"
    );
    let after = metadata(fixture)?;
    ensure!(
        after["current-schema-id"] != before["current-schema-id"],
        "late schema commit was not visible"
    );
    ensure!(
        control
            .mutation_counts("ns", "orders")
            .table_commit_forwarded
            == counts.table_commit_forwarded + 1,
        "late request was replayed"
    );
    save(
        context,
        "late-commit.json",
        &json!({"before":before,"after":after,"statement":"Unknown","original":"Succeeded","commit_method_actual_exit":true,"sql_actor_actual_exit":true,"evidence_budget_ms":EVIDENCE_MS}),
    )?;
    context.action("observed server requirements hold, SQL Unknown on old metadata, one dispatch, late success, and actual exit");
    sql(
        context,
        &catalog_sql(fixture, "publication_response", 30_000),
    )?;
    post_commit_reload(context, fixture)
}
fn post_commit_reload(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let mut hold = fixture
        .service()
        .arm_at("ns.orders", HoldStage::AfterPersistentCommit)?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let query = std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?.run(|conn| {
            conn.query_drop(
                "ALTER TABLE publication_response.ns.orders ADD COLUMN post_commit BIGINT",
            )
            .map_err(Into::into)
        })
    });
    let phase = (|| -> Result<Value> {
        hold.wait_until_held(context.deadline())?;
        let applied = metadata(fixture)?;
        reqwest::blocking::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build()?
            .post(format!("{}/v1/namespaces/ns/tables/orders", fixture.rest().endpoints().rest_uri))
            .json(&json!({"requirements":[{"type":"assert-table-uuid","uuid":applied["table-uuid"]}],"updates":[{"action":"set-properties","updates":{"publication.competitor":"after-commit"}}]}))
            .send()?.error_for_status()?;
        Ok(applied)
    })();
    let released = hold.release();
    let outcome = query
        .join()
        .map_err(|_| anyhow::anyhow!("post-commit SQL actor panicked"))?;
    let exited = hold.wait_for_original_exit(context.deadline());
    fs::write(
        context.scenario_root().join("post-commit-trace.ndjson"),
        hold.trace()?,
    )?;
    let applied = phase?;
    released?;
    outcome.context("post-commit response should preserve Applied evidence")?;
    ensure!(
        exited? == CommitOutcome::Succeeded,
        "post-commit wrapper did not exit after success"
    );
    let after = metadata(fixture)?;
    ensure!(
        after["current-schema-id"] == applied["current-schema-id"],
        "handler reload lost the original schema"
    );
    ensure!(
        after["properties"]["publication.competitor"] == "after-commit",
        "handler reload did not observe the successor metadata"
    );
    save(
        context,
        "post-commit-reload.json",
        &json!({"applied":applied,"after":after,"original":"Succeeded","statement":"Applied","commit_method_actual_exit":true,"sql_actor_actual_exit":true}),
    )?;
    context.action("observed persisted commit before handler reload, successor publication, and original success exit");
    Ok(())
}

fn stream_deadline(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let control = fixture.proxy()?;
    for (action, statement) in [
        (
            "table-commit",
            format!("ALTER TABLE {TABLE} ADD COLUMN streamed BIGINT"),
        ),
        (
            "catalog-mutation",
            format!("CREATE TABLE {CATALOG}.ns.created (k INT)"),
        ),
        (
            "catalog-mutation",
            format!("CREATE DATABASE {CATALOG}.streamed_namespace"),
        ),
        (
            "catalog-mutation",
            format!("CREATE VIEW {CATALOG}.ns.streamed_view AS SELECT 1 AS k"),
        ),
    ] {
        let guard = control.arm_next(action, "stream-response")?;
        let started = Instant::now();
        let outcome = sql(context, &statement);
        let elapsed = started.elapsed();
        let evidence = guard.finish();
        require_unknown(&outcome)?;
        let trace = evidence?.summary();
        ensure!(
            trace.contains("response-stream-chunk") && trace.contains("response-stream-exited"),
            "slow response did not stream and exit: {trace}"
        );
        ensure!(
            elapsed >= Duration::from_millis(EVIDENCE_MS * 4 / 5)
                && elapsed < Duration::from_secs(15),
            "slow response escaped total budget: {elapsed:?}"
        );
        context.action(format!("slow body returned Unknown under {EVIDENCE_MS} ms evidence budget and body owner exited: {action}"));
    }
    // A bodyless success status is already decisive evidence. It must not
    // wait for an artificial body or regress when that body owner closes.
    for statement in [
        format!("DROP VIEW {CATALOG}.ns.streamed_view"),
        format!("DROP TABLE {CATALOG}.ns.created"),
        format!("DROP DATABASE {CATALOG}.streamed_namespace"),
    ] {
        let guard = control.arm_next("catalog-mutation", "stream-response")?;
        let outcome = sql(context, &statement);
        let trace = guard.finish()?.summary();
        outcome.context("bodyless successful mutation lost Applied evidence")?;
        ensure!(
            trace.contains("response-stream-exited"),
            "bodyless response owner did not exit"
        );
        context
            .action("bodyless catalog success retained Applied evidence and response owner exited");
    }
    Ok(())
}
fn maintenance_race(
    context: &mut ScenarioContext,
    fixture: &PublicationFixture,
    property: Property,
) -> Result<()> {
    let before = metadata(fixture)?;
    let candidate = before["snapshots"]
        .as_array()
        .context("snapshot list missing")?
        .first()
        .context("snapshot graph empty")?["snapshot-id"]
        .as_i64()
        .context("snapshot ID missing")?;
    let candidate_list = before["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|snapshot| snapshot["snapshot-id"] == candidate)
        .unwrap()["manifest-list"]
        .as_str()
        .context("candidate manifest list missing")?
        .to_owned();
    ensure!(
        fixture.rest().object_exists(&candidate_list)?,
        "candidate manifest list absent before expiry"
    );
    let mut hold = fixture.service().arm("ns.orders")?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let statement = if matches!(property, Property::ExpiryConcurrency) {
        format!(
            "ALTER TABLE {TABLE} EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00' RETAIN LAST 1"
        )
    } else {
        format!("ALTER TABLE {TABLE} REWRITE MANIFESTS")
    };
    let query = std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?
            .run(|conn| conn.query_drop(statement).map_err(Into::into))
    });
    let mut competitor_snapshot = Value::Null;
    let mut competitor_list = String::new();
    let phase = (|| -> Result<()> {
        hold.wait_until_held(context.deadline())?;
        if matches!(property, Property::ExpiryConcurrency) {
            reqwest::blocking::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build()?
                .post(format!("{}/v1/namespaces/ns/tables/orders", fixture.rest().endpoints().rest_uri))
                .json(&json!({"requirements":[{"type":"assert-table-uuid","uuid":before["table-uuid"]},{"type":"assert-ref-snapshot-id","ref":"protected","snapshot-id":null}],"updates":[{"action":"set-snapshot-ref","ref-name":"protected","type":"tag","snapshot-id":candidate}]}))
                .send()?.error_for_status()?;
        } else {
            // A distinct logical catalog is deliberately an external writer relative
            // to this catalog's local expiry/ref coordination domain.
            sql(
                context,
                &catalog_sql(fixture, "publication_competitor", 30_000),
            )?;
            sql(
                context,
                "INSERT INTO publication_competitor.ns.orders VALUES (99)",
            )?;
            let appended = metadata(fixture)?;
            competitor_snapshot = appended["current-snapshot-id"].clone();
            competitor_list = appended["snapshots"]
                .as_array()
                .context("append snapshots missing")?
                .iter()
                .find(|snapshot| snapshot["snapshot-id"] == competitor_snapshot)
                .context("append snapshot missing")?["manifest-list"]
                .as_str()
                .context("append manifest list missing")?
                .to_owned();
        }
        Ok(())
    })();
    let release = hold.release();
    let outcome = query
        .join()
        .map_err(|_| anyhow::anyhow!("maintenance SQL actor panicked"))?;
    let original = hold.wait_for_original_exit(context.deadline());
    fs::write(
        context.scenario_root().join("service-trace.ndjson"),
        hold.trace()?,
    )?;
    phase?;
    release?;
    ensure!(
        original? == CommitOutcome::Conflict,
        "original maintenance request did not hit actual JDBC conflict"
    );
    outcome.context("maintenance did not successfully reprepare after definite conflict")?;
    let after = metadata(fixture)?;
    let snapshots = after["snapshots"]
        .as_array()
        .context("result snapshots missing")?;
    if matches!(property, Property::ExpiryConcurrency) {
        ensure!(
            fixture.rest().object_exists(&candidate_list)?,
            "expiry deleted protected manifest list"
        );
        ensure!(
            after["refs"]["protected"]["snapshot-id"] == candidate,
            "expiry deleted concurrently added ref"
        );
        ensure!(
            snapshots
                .iter()
                .any(|snapshot| snapshot["snapshot-id"] == candidate),
            "expiry deleted protected snapshot"
        );
    } else {
        ensure!(
            fixture.rest().object_exists(&competitor_list)?,
            "rewrite deleted concurrent append manifest list"
        );
        ensure!(
            snapshots
                .iter()
                .any(|snapshot| snapshot["snapshot-id"] == competitor_snapshot),
            "rewrite lost concurrent append snapshot"
        );
    }
    let expected: Vec<i32> = if matches!(property, Property::ExpiryConcurrency) {
        vec![1, 2, 3, 4]
    } else {
        vec![1, 2, 3, 4, 99]
    };
    let actual: Vec<i32> = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|conn| Ok(conn.query(format!("SELECT k FROM {TABLE} ORDER BY k"))?))?;
    ensure!(
        actual == expected,
        "maintenance race lost source rows: {actual:?}"
    );
    save(
        context,
        "maintenance-race.json",
        &json!({"before":before,"after":after,"protected_snapshot":candidate,"append_snapshot":competitor_snapshot,"rows":actual,"original":"Conflict","commit_method_actual_exit":true,"sql_actor_actual_exit":true}),
    )?;
    context.action("observed real server CAS conflict, successful reprepare, retained exact snapshot/ref IDs, and source readability");
    Ok(())
}
