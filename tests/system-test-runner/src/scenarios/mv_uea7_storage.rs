// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

use super::mv_uea7::{
    ManagedMvRestFixture, assert_rows, connect, execute, property, require_property,
    require_three_backends, select_catalog_and_namespace, status, wait_for_status_phase,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use reqwest::blocking::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

const CATALOG: &str = "uea7_mv_storage";
const NAMESPACE: &str = "ns";
const VIEW: &str = "orders_mv";
const DOCUMENT_MANIFEST_KEY: &str = "novarocks.documents.v1";

#[derive(Default)]
pub(super) struct MvStorageContract {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStorageContract {
    fn name(&self) -> &'static str {
        "mv/storage-contract"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (rest, launch) = ManagedMvRestFixture::start(scenario_root, CATALOG)?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        ensure!(
            fixture.is_none(),
            "managed MV fixture was initialized more than once"
        );
        *fixture = Some(rest);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (create_catalog_sql, rest_uri) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };

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
            &format!("CREATE DATABASE {CATALOG}.{NAMESPACE}"),
        )?;
        execute(
            context,
            &mut conn,
            "create row-lineage source table",
            &format!(
                "CREATE TABLE {CATALOG}.{NAMESPACE}.orders (k1 INT, v2 BIGINT) \
                 TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
            ),
        )?;
        execute(
            context,
            &mut conn,
            "insert source rows for a decisive aggregate rewrite",
            &format!(
                "INSERT INTO {CATALOG}.{NAMESPACE}.orders \
                 SELECT CASE WHEN n % 2 = 0 THEN 1 ELSE 2 END, \
                 CAST(n % 100 AS BIGINT) \
                 FROM TABLE(generate_series(1, 4000)) t(n)"
            ),
        )?;
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
        execute(
            context,
            &mut conn,
            "create document-managed Iceberg MV without publishing",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') \
             AS SELECT k1, SUM(v2) AS total FROM orders GROUP BY k1",
        )?;
        context.action("verify CREATE wrote table-level D/L/C and no snapshot or P to REST");
        let created = load_graph(context, &rest_uri, 0)?;

        execute(
            context,
            &mut conn,
            "publish the first exact MV output",
            "REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE",
        )?;
        context.action("verify first P binds its exact snapshot and table-level D/L revisions");
        let first = load_graph(context, &rest_uri, 1)?;
        ensure!(
            first.definition == created.definition
                && first.interpretation == created.interpretation,
            "the first publication changed the CREATE D/L revisions"
        );
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 98_000), (2, 100_000)],
            "read the first publication directly",
        )?;
        require_rewrite(context, &mut conn)?;
        let mut independent_reader = connect(context)?;
        select_catalog_and_namespace(context, &mut independent_reader, CATALOG)?;
        require_rewrite(context, &mut independent_reader)?;
        drop(independent_reader);
        drop(conn);

        context.action("restart FE while preserving the private REST catalog and its MV documents");
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE after the first MV publication")?;
        let mut conn = connect(context)?;
        let closed = wait_for_status_phase(
            context,
            &mut conn,
            CATALOG,
            VIEW,
            "AWAITING_EFFECT_SETTLEMENT",
            "wait for read-only management after FE restart",
        )?;
        require_property(&closed, "UnsettledEffects", "1")?;
        assert_rows(
            context,
            &mut conn,
            &format!("SELECT k1, total FROM {CATALOG}.{NAMESPACE}.{VIEW} ORDER BY k1"),
            &[(1, 98_000), (2, 100_000)],
            "read the lake publication on the restarted FE",
        )?;
        let after_restart = load_graph(context, &rest_uri, 1)?;
        ensure!(
            after_restart == first,
            "FE restart changed the exact D/L/P attachment graph"
        );
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
        context.action("verify refresh remains closed until an operator declaration");
        let error = match conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE") {
            Err(error) => error,
            Ok(()) => bail!(
                "restarted FE refreshed the MV before management readmission; {}",
                context.diagnostics()
            ),
        };
        ensure!(
            error
                .to_string()
                .contains("MV target requires a successful fresh Current observation"),
            "closed MV refresh returned an unrelated error {error}; {}",
            context.diagnostics()
        );

        let closed = status(context, &mut conn, CATALOG, VIEW)?;
        require_property(&closed, "Phase", "AWAITING_EFFECT_SETTLEMENT")?;
        let challenge = property(&closed, "Challenge")?;
        let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
        context.action("readmit management through the public operator procedure");
        let resumed: Vec<(String, Option<String>)> = conn
            .query(format!(
                "CALL novarocks_mv_resume_management('{CATALOG}', '{NAMESPACE}', '{VIEW}', \
                 '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
                 'the system scenario replaced the declared frontend process before this statement')"
            ))
            .context("resume managed MV after FE restart")?;
        require_property(&resumed, "SettledEffects", "1")?;
        execute(
            context,
            &mut conn,
            "insert source changes after management readmission",
            "INSERT INTO orders VALUES (1, 5), (2, 7)",
        )?;
        execute(
            context,
            &mut conn,
            "publish a second exact MV output through FULL refresh",
            "REFRESH MATERIALIZED VIEW orders_mv FULL WITH SYNC MODE",
        )?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 98_005), (2, 100_007)],
            "read the second publication directly",
        )?;
        context.action("verify FULL produced a new exact P and an Iceberg overwrite");
        let second = load_graph(context, &rest_uri, 2)?;
        ensure!(
            second.definition == first.definition && second.interpretation == first.interpretation,
            "FULL refresh changed the stable D/L revisions"
        );
        ensure!(
            second.publications[0] == first.publications[0]
                && second.publications[1] != first.publications[0],
            "FULL refresh did not retain the original exact P and add a distinct P"
        );
        ensure!(
            second.last_operation.as_deref() == Some("overwrite"),
            "FULL refresh did not publish an Iceberg overwrite: {:?}",
            second.last_operation
        );
        context.action(
            "proved CREATE, exact publications, restart reads, readmission and FULL overwrite",
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

fn require_rewrite(context: &mut ScenarioContext, conn: &mut mysql::Conn) -> Result<()> {
    context.remaining("explain base aggregate rewrite")?;
    context.action("verify the base aggregate rewrites onto the published MV");
    let plan: Vec<(String,)> = conn
        .query("EXPLAIN SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1")
        .context("explain aggregate over the MV source")?;
    ensure!(
        plan.iter()
            .any(|(line,)| line.contains("rewritten with mv: orders_mv")),
        "published MV did not rewrite the base aggregate: {plan:?}; {}",
        context.diagnostics()
    );
    Ok(())
}

#[derive(Debug, PartialEq)]
struct DocumentGraph {
    definition: Value,
    interpretation: Value,
    publications: Vec<(i64, Value)>,
    last_operation: Option<String>,
}

fn load_graph(context: &ScenarioContext, rest_uri: &str, expected: usize) -> Result<DocumentGraph> {
    let url = format!(
        "{}/v1/namespaces/{NAMESPACE}/tables/{VIEW}",
        rest_uri.trim_end_matches('/')
    );
    let client = Client::builder()
        .no_proxy()
        .timeout(
            context
                .remaining("load exact MV REST metadata")?
                .min(Duration::from_secs(30)),
        )
        .build()
        .context("build isolated REST client")?;
    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("load exact MV REST metadata at {url}"))?
        .error_for_status()
        .with_context(|| format!("REST did not return MV metadata at {url}"))?;
    let table: Value = response.json().context("decode exact MV REST metadata")?;
    let metadata = table
        .get("metadata")
        .context("REST response lacks metadata")?;
    let table_manifest = manifest(&metadata["properties"], "table metadata")?;
    let mut table_documents = HashMap::new();
    for document in documents(&table_manifest, "table metadata")? {
        let name = document["name"]
            .as_str()
            .context("table document lacks name")?;
        ensure!(
            document["attachment"]["kind"] == "table-metadata",
            "{name} is not a table-level document"
        );
        ensure!(
            table_documents.insert(name, document).is_none(),
            "duplicate table-level document {name}"
        );
    }
    for name in ["definition", "interpretation", "configuration"] {
        ensure!(
            table_documents.contains_key(name),
            "MV REST metadata lacks table-level {name}"
        );
    }
    ensure!(
        !table_documents.contains_key("publication"),
        "MV REST metadata has a table-level P"
    );
    let definition = table_documents["definition"]["revision"].clone();
    let interpretation = table_documents["interpretation"]["revision"].clone();
    ensure!(
        !definition.is_null() && !interpretation.is_null(),
        "D/L revisions are missing from canonical table metadata"
    );
    let snapshots = metadata["snapshots"]
        .as_array()
        .context("MV REST metadata lacks snapshots")?;
    ensure!(
        snapshots.len() == expected,
        "expected {expected} MV snapshots, observed {}",
        snapshots.len()
    );
    if expected == 0 {
        ensure!(
            metadata["current-snapshot-id"].is_null(),
            "unpublished MV already has a current snapshot"
        );
    }

    let mut publications = Vec::with_capacity(expected);
    let mut last_operation = None;
    for snapshot in snapshots {
        let snapshot_id = snapshot["snapshot-id"]
            .as_i64()
            .context("MV snapshot lacks an exact ID")?;
        let snapshot_manifest = manifest(&snapshot["summary"], "snapshot summary")?;
        let snapshot_documents = documents(&snapshot_manifest, "snapshot summary")?;
        ensure!(
            snapshot_documents.len() == 1,
            "snapshot {snapshot_id} has {} documents instead of one P",
            snapshot_documents.len()
        );
        let publication = &snapshot_documents[0];
        ensure!(
            publication["name"] == "publication"
                && publication["attachment"]["kind"] == "exact-output"
                && publication["attachment"]["snapshot_id"].as_i64() == Some(snapshot_id),
            "P does not bind exact output {snapshot_id}"
        );
        let references = publication["references"]
            .as_array()
            .context("P lacks D/L references")?;
        ensure!(
            references.len() == 2,
            "P on snapshot {snapshot_id} does not have exactly two D/L references"
        );
        let mut seen = HashMap::new();
        for reference in references {
            let name = reference["name"]
                .as_str()
                .context("P reference lacks name")?;
            ensure!(
                seen.insert(name, ()).is_none(),
                "P on snapshot {snapshot_id} repeats {name}"
            );
            let table_document = table_documents
                .get(name)
                .with_context(|| format!("P references absent table-level document {name}"))?;
            ensure!(
                reference["revision"] == table_document["revision"],
                "P on snapshot {snapshot_id} does not reference exact {name} revision"
            );
        }
        ensure!(
            seen.contains_key("definition") && seen.contains_key("interpretation"),
            "P on snapshot {snapshot_id} does not reference D and L"
        );
        let revision = publication["revision"].clone();
        ensure!(
            !revision.is_null(),
            "P on snapshot {snapshot_id} has no revision"
        );
        ensure!(
            publications
                .iter()
                .all(|(id, prior_revision)| *id != snapshot_id && prior_revision != &revision),
            "duplicate snapshot identity or P revision at snapshot {snapshot_id}"
        );
        publications.push((snapshot_id, revision));
        last_operation = snapshot["summary"]["operation"].as_str().map(str::to_owned);
    }
    if expected > 0 {
        ensure!(
            metadata["current-snapshot-id"].as_i64() == publications.last().map(|(id, _)| *id),
            "current MV snapshot does not carry the last exact P"
        );
    }
    Ok(DocumentGraph {
        definition,
        interpretation,
        publications,
        last_operation,
    })
}

fn manifest(properties: &Value, owner: &str) -> Result<Value> {
    let encoded = properties[DOCUMENT_MANIFEST_KEY]
        .as_str()
        .with_context(|| format!("{owner} lacks {DOCUMENT_MANIFEST_KEY}"))?;
    serde_json::from_str(encoded).with_context(|| format!("decode {owner} document manifest"))
}

fn documents<'a>(manifest: &'a Value, owner: &str) -> Result<&'a [Value]> {
    ensure!(
        manifest["version"].as_u64() == Some(1),
        "{owner} has an unsupported document manifest version"
    );
    manifest["documents"]
        .as_array()
        .map(Vec::as_slice)
        .with_context(|| format!("{owner} manifest lacks documents"))
}
