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

use super::mv_uea7::{
    ManagedMvRestFixture, assert_rows, connect, execute, property, require_status_phase,
    require_three_backends, select_catalog_and_namespace, status,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use reqwest::blocking::Client;
use serde_json::{Value, json};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

const CATALOG: &str = "uea7_mv_incarnation";
const NAMESPACE: &str = "ns";
const VIEW: &str = "orders_mv";
const COMPETING_INCARNATION: &str = "0197a0b5-0000-7001-8000-000000000001";
const MARKER_PROPERTY: &str = "novarocks.managed.incarnation";

/// A controlled external marker commit stands in for a second process that
/// accidentally uses the same deployment identity. The scenario never starts
/// concurrent managers or claims that the marker is a lease or a fence.
#[derive(Default)]
pub(super) struct MvIncarnationMismatch {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvIncarnationMismatch {
    fn name(&self) -> &'static str {
        "mv/incarnation-mismatch"
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
            "insert source rows",
            &format!("INSERT INTO {CATALOG}.{NAMESPACE}.orders VALUES (1, 10), (2, 20)"),
        )?;
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
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
            "read the first publication before marker change",
        )?;
        let initial_status = require_status_phase(
            context,
            &mut conn,
            CATALOG,
            VIEW,
            "MANAGEABLE",
            "confirm initial managed incarnation is admitted",
        )?;
        let local_incarnation = property(&initial_status, "LocalIncarnation")?;
        ensure!(
            local_incarnation != COMPETING_INCARNATION,
            "test incarnation unexpectedly matches the FE process"
        );

        // Only the isolated fixture is modified. A table-UUID requirement
        // ensures the injection cannot accidentally target a replaced object.
        context.action("replace the exact MV table's diagnostic incarnation through private REST");
        change_remote_incarnation(context, &rest_uri, local_incarnation)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read the historical publication after Current marker changed",
        )?;

        context.action("trigger a new managed write after another incarnation was observed");
        let first_error =
            match conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE") {
                Err(error) => error,
                Ok(()) => bail!(
                    "refresh committed after a different Current incarnation was persisted; {}",
                    context.diagnostics()
                ),
            };
        let message = first_error.to_string();
        context.action(format!(
            "first refresh rejection after marker change: {message}"
        ));
        ensure!(
            message.contains(
                "MV Current marker names another process incarnation; management is closed"
            ),
            "refresh failed before detecting the foreign Current incarnation: {message}; {}",
            context.diagnostics()
        );
        let closed = status(context, &mut conn, CATALOG, VIEW)?;
        let phase = property(&closed, "Phase")?;
        ensure!(
            phase == "INCARNATION_MISMATCH",
            "fresh Current incarnation mismatch did not close MV management admission: {closed:?}; {}",
            context.diagnostics()
        );

        // A second attempt must not restore access or overwrite the remote
        // marker with the original value.
        context.action("verify the closed target does not readmit itself on retry");
        if conn
            .query_drop("REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE")
            .is_ok()
        {
            bail!(
                "second refresh reopened management after incarnation mismatch; {}",
                context.diagnostics()
            );
        }
        let remote = load_rest_table(context, &rest_uri)?;
        ensure!(
            marker(&remote)? == COMPETING_INCARNATION,
            "the old manager overwrote the competing incarnation after detecting it; {}",
            context.diagnostics()
        );
        context.action("proved Current incarnation mismatch closes management without changing old publication");
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

fn rest_client(context: &ScenarioContext) -> Result<Client> {
    Client::builder()
        .no_proxy()
        .timeout(
            context
                .remaining("call isolated Iceberg REST")?
                .min(Duration::from_secs(30)),
        )
        .build()
        .context("build isolated Iceberg REST client")
}

fn table_url(rest_uri: &str) -> String {
    format!(
        "{}/v1/namespaces/{NAMESPACE}/tables/{VIEW}",
        rest_uri.trim_end_matches('/')
    )
}

fn load_rest_table(context: &ScenarioContext, rest_uri: &str) -> Result<Value> {
    let response = rest_client(context)?
        .get(table_url(rest_uri))
        .send()
        .context("load managed MV table directly from isolated REST")?;
    let status = response.status();
    let body = response
        .text()
        .context("read isolated REST table response")?;
    ensure!(
        status.is_success(),
        "isolated REST table load returned HTTP {status}: {}",
        body.chars().take(1024).collect::<String>()
    );
    serde_json::from_str(&body).context("decode isolated REST table response")
}

fn marker(table: &Value) -> Result<&str> {
    table["metadata"]["properties"][MARKER_PROPERTY]
        .as_str()
        .context("isolated REST table has no managed incarnation property")
}

fn change_remote_incarnation(
    context: &ScenarioContext,
    rest_uri: &str,
    original_incarnation: &str,
) -> Result<()> {
    let current = load_rest_table(context, rest_uri)?;
    ensure!(
        marker(&current)? == original_incarnation,
        "isolated REST marker did not match this FE's admitted incarnation"
    );
    let uuid = current["metadata"]["table-uuid"]
        .as_str()
        .context("isolated REST table has no UUID")?;
    let body = json!({
        "requirements": [{ "type": "assert-table-uuid", "uuid": uuid }],
        "updates": [{
            "action": "set-properties",
            "updates": { (MARKER_PROPERTY): COMPETING_INCARNATION }
        }]
    });
    let response = rest_client(context)?
        .post(table_url(rest_uri))
        .json(&body)
        .send()
        .context("commit competing incarnation into isolated REST catalog")?;
    let status = response.status();
    let response_body = response
        .text()
        .context("read competing incarnation commit response")?;
    ensure!(
        status.is_success(),
        "isolated REST incarnation update returned HTTP {status}: {}",
        response_body.chars().take(1024).collect::<String>()
    );
    let current = load_rest_table(context, rest_uri)?;
    ensure!(
        marker(&current)? == COMPETING_INCARNATION,
        "isolated REST did not persist the competing incarnation"
    );
    Ok(())
}
