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

//! Runner-owned REST namespace placement; production SQL DDL and its oracle remain unchanged.
use crate::types::ConnectionConfig;
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{ScratchIdentity, canonical_s3};
use novarocks_parser::ast::{
    CatalogStatement, DmlStatement, IcebergStatement, LiteralKind, MaintenanceStatement,
    MaterializedViewStatement, ObjectName, SessionStatement, SetTarget, SetValue, SetWord,
    Statement, TableStatement, ViewStatement,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Debug)]
enum Catalog {
    Rest(String),
    Hadoop(String),
    Other,
}
#[derive(Clone, Default, Debug)]
pub(crate) struct CatalogFacts(Arc<Mutex<BTreeMap<String, Catalog>>>);
#[derive(Clone, Debug)]
pub(crate) struct NamespaceGuard {
    identity: ScratchIdentity,
    facts: CatalogFacts,
    catalog: Option<String>,
    namespace: Option<String>,
    deadline: Instant,
}
impl NamespaceGuard {
    pub fn new(
        identity: &ScratchIdentity,
        facts: CatalogFacts,
        conn: &ConnectionConfig,
        deadline: Instant,
    ) -> Self {
        Self {
            identity: identity.clone(),
            facts,
            catalog: conn.catalog.clone(),
            namespace: conn.db.clone(),
            deadline,
        }
    }
    pub fn for_cleanup(&self, deadline: Instant) -> Self {
        let mut guard = self.clone();
        guard.deadline = deadline;
        guard
    }
    pub fn connection_context(&self, base: &ConnectionConfig) -> ConnectionConfig {
        ConnectionConfig {
            catalog: self.catalog.clone(),
            db: self.namespace.clone(),
            ..base.clone()
        }
    }
    pub fn set_database(&mut self, value: &str) {
        self.namespace = Some(value.to_string());
    }
    fn catalog_fact(&self, name: &str) -> Result<Option<Catalog>> {
        Ok(self
            .facts
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL catalog facts lock poisoned"))?
            .get(name)
            .cloned())
    }
    fn target(
        &self,
        name: &ObjectName,
        database: bool,
    ) -> Result<Option<(String, String, Option<String>)>> {
        let parts = name
            .parts
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>();
        Ok(match (database, parts.as_slice()) {
            (true, [catalog, namespace]) => Some((catalog.clone(), namespace.clone(), None)),
            (true, [namespace]) => self
                .catalog
                .clone()
                .map(|catalog| (catalog, namespace.clone(), None)),
            (false, [catalog, namespace, table]) => {
                Some((catalog.clone(), namespace.clone(), Some(table.clone())))
            }
            (false, [catalog, namespace, table, reference])
                if reference
                    .strip_prefix("branch_")
                    .or_else(|| reference.strip_prefix("tag_"))
                    .is_some_and(|name| !name.is_empty()) =>
            {
                Some((catalog.clone(), namespace.clone(), Some(table.clone())))
            }
            (false, [namespace, table]) => self
                .catalog
                .clone()
                .map(|catalog| (catalog, namespace.clone(), Some(table.clone()))),
            (false, [table]) => self
                .catalog
                .clone()
                .zip(self.namespace.clone())
                .map(|(catalog, namespace)| (catalog, namespace, Some(table.clone()))),
            _ => None,
        })
    }
    fn owned(&self, namespace: &str) -> bool {
        namespace
            .strip_prefix(&format!("{}_", self.identity.run_id))
            .is_some_and(|p| {
                !p.is_empty()
                    && p.len() <= 5
                    && p.bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            })
    }
    fn within(&self, uri: &str) -> Result<bool> {
        let value = canonical_s3(uri)?;
        Ok(value == self.identity.scope_root
            || value.starts_with(&format!("{}/", self.identity.scope_root)))
    }
    fn ensure_write(&self, name: &ObjectName, database: bool) -> Result<()> {
        let Some((catalog, namespace, _)) = self.target(name, database)? else {
            return Ok(());
        };
        match self.catalog_fact(&catalog)? {
            Some(Catalog::Rest(_)) => ensure!(
                self.owned(&namespace),
                "shared REST write requires an exact namespace of the admitted run"
            ),
            Some(Catalog::Hadoop(root)) if root.starts_with("s3://") => {
                ensure!(
                    self.within(&root)?,
                    "prepared Hadoop warehouse is read-only"
                );
            }
            _ => {}
        }
        Ok(())
    }
    pub fn before(&self, sql: &str) -> Result<()> {
        ensure!(
            Instant::now() < self.deadline,
            "original SQL namespace clock expired"
        );
        let Ok(statements) = novarocks_parser::parse(sql) else {
            return Ok(());
        };
        for statement in statements {
            match statement {
                Statement::Catalog(CatalogStatement::CreateDatabase(value)) => {
                    self.ensure_write(&value.name, true)?
                }
                Statement::Table(TableStatement::Create(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Catalog(CatalogStatement::DropDatabase(value)) => {
                    self.ensure_write(&value.name, true)?
                }
                Statement::Catalog(CatalogStatement::DropTable(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Catalog(CatalogStatement::TruncateTable(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Dml(DmlStatement::Delete(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::Update(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::Merge(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::AddEqualityDelete(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::Insert(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::CreateTableAsSelect(value)) => {
                    let TableStatement::Create(table) = value.table;
                    self.ensure_write(&table.name, false)?;
                }
                Statement::Iceberg(IcebergStatement::AlterTable(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::View(ViewStatement::Create(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::View(ViewStatement::Drop(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Create(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Drop(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Alter(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Refresh(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Maintenance(MaintenanceStatement::Optimize(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::Maintenance(MaintenanceStatement::RewriteManifests(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::Maintenance(MaintenanceStatement::ExpireSnapshots(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::Maintenance(MaintenanceStatement::RemoveOrphanFiles(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub fn after(&mut self, sql: &str) -> Result<()> {
        let Ok(statements) = novarocks_parser::parse(sql) else {
            return Ok(());
        };
        for statement in statements {
            match statement {
                Statement::Catalog(CatalogStatement::CreateCatalog(value)) => {
                    let properties = value
                        .properties
                        .iter()
                        .filter_map(|p| match (&p.key.kind, &p.value.kind) {
                            (LiteralKind::String(k), LiteralKind::String(v)) => {
                                Some((k.to_ascii_lowercase(), v.clone()))
                            }
                            _ => None,
                        })
                        .collect::<BTreeMap<_, _>>();
                    // Mirror the provider's accepted aliases and precedence. This runs
                    // after successful SQL; it must project the same catalog facts.
                    let kind = properties.get("iceberg.catalog.type");
                    let provider = properties.get("type");
                    let fact =
                        if provider.is_some_and(|value| !value.eq_ignore_ascii_case("iceberg")) {
                            Catalog::Other
                        } else {
                            match kind.map(|value| value.to_ascii_lowercase()).as_deref() {
                                Some("rest") => Catalog::Rest(
                                    properties
                                        .get("uri")
                                        .or_else(|| properties.get("iceberg.catalog.uri"))
                                        .context("REST catalog omitted exact URI")?
                                        .trim_end_matches('/')
                                        .to_string(),
                                ),
                                None | Some("hadoop") => Catalog::Hadoop(
                                    properties
                                        .get("iceberg.catalog.warehouse")
                                        .or_else(|| properties.get("warehouse"))
                                        .context("Hadoop catalog omitted exact warehouse")?
                                        .clone(),
                                ),
                                _ => Catalog::Other,
                            }
                        };
                    self.facts
                        .0
                        .lock()
                        .map_err(|_| anyhow::anyhow!("SQL catalog facts lock poisoned"))?
                        .insert(value.name.value, fact);
                }
                Statement::Catalog(CatalogStatement::CreateDatabase(value)) => {
                    if let Some((catalog, namespace, _)) = self.target(&value.name, true)? {
                        if let Some(Catalog::Rest(base)) = self.catalog_fact(&catalog)? {
                            ensure!(
                                self.owned(&namespace),
                                "REST placement refused a namespace outside the original run"
                            );
                            self.place_namespace(&base, &namespace)?;
                        }
                    }
                }
                Statement::Table(TableStatement::Create(value)) => {
                    self.verify_table(&value.name)?
                }
                Statement::Dml(DmlStatement::CreateTableAsSelect(value)) => {
                    let TableStatement::Create(table) = value.table;
                    self.verify_table(&table.name)?;
                }
                Statement::Session(SessionStatement::Use(value)) => {
                    if let Some(catalog) = value.catalog {
                        self.catalog = Some(catalog.value);
                    }
                    self.namespace = Some(value.database.value);
                }
                Statement::Session(SessionStatement::Set(value)) => {
                    for assignment in value.assignments {
                        if matches!(assignment.target, SetTarget::Catalog { .. }) {
                            self.catalog = match assignment.value {
                                SetValue::Words(words) if words.len() == 1 => match &words[0] {
                                    SetWord::Ident(v) => Some(v.value.clone()),
                                    SetWord::Literal(v) => match &v.kind {
                                        LiteralKind::String(v) => Some(v.clone()),
                                        _ => None,
                                    },
                                },
                                _ => None,
                            };
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn remaining(&self) -> Result<std::time::Duration> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        ensure!(!left.is_zero(), "original REST namespace deadline expired");
        Ok(left)
    }
    fn namespace_url(base: &str, namespace: &str) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&format!("{base}/v1/namespaces/"))?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid REST namespace base URI"))?
            .pop_if_empty()
            .push(namespace);
        Ok(url)
    }
    fn place_namespace(&self, base: &str, namespace: &str) -> Result<()> {
        let location = format!("{}/rest/{namespace}", self.identity.scope_root);
        let url = Self::namespace_url(base, namespace)?;
        let mut properties = url.clone();
        properties.path_segments_mut().unwrap().push("properties");
        reqwest::blocking::Client::new()
            .post(properties)
            .timeout(self.remaining()?)
            .json(&serde_json::json!({"removals":[],"updates":{"location":location}}))
            .send()?
            .error_for_status()?;
        let response: serde_json::Value = reqwest::blocking::Client::new()
            .get(url)
            .timeout(self.remaining()?)
            .send()?
            .error_for_status()?
            .json()?;
        ensure!(
            response["properties"]["location"].as_str() == Some(location.as_str()),
            "REST namespace did not retain its exact admitted run location"
        );
        Ok(())
    }
    fn verify_table(&self, name: &ObjectName) -> Result<()> {
        let Some((catalog, namespace, Some(table))) = self.target(name, false)? else {
            return Ok(());
        };
        if let Some(Catalog::Rest(base)) = self.catalog_fact(&catalog)? {
            ensure!(
                self.owned(&namespace),
                "REST table validation refused a foreign namespace"
            );
            let mut url = Self::namespace_url(&base, &namespace)?;
            url.path_segments_mut().unwrap().push("tables").push(&table);
            let response: serde_json::Value = reqwest::blocking::Client::new()
                .get(url)
                .timeout(self.remaining()?)
                .send()?
                .error_for_status()?
                .json()?;
            let location = response["metadata"]["location"]
                .as_str()
                .context("REST table metadata omitted actual location")?;
            ensure!(
                canonical_s3(location)?
                    .starts_with(&format!("{}/rest/{namespace}/", self.identity.scope_root)),
                "REST table actual location lies outside its admitted run namespace scope"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    fn guard() -> NamespaceGuard {
        let identity = ScratchIdentity::create("s3://novarocks/guard-tests").unwrap();
        NamespaceGuard::new(
            &identity,
            CatalogFacts::default(),
            &ConnectionConfig {
                mysql: "mysql".into(),
                host: "127.0.0.1".into(),
                port: "0".into(),
                user: "root".into(),
                password: None,
                catalog: Some("catalog".into()),
                db: None,
            },
            Instant::now() + Duration::from_secs(10),
        )
    }
    fn server(responses: Vec<serde_json::Value>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let thread = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 1024];
                loop {
                    let n = stream.read(&mut chunk).unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&bytes[..end]);
                        let size = head
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|n| n.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + size {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                let body = response.to_string();
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
            }
            requests
        });
        (base, thread)
    }
    #[test]
    fn rest_catalog_aliases_follow_provider_uri_precedence() {
        for (properties, expected) in [
            (r#""uri"="http://127.0.0.1:1/""#, "http://127.0.0.1:1"),
            (
                r#""iceberg.catalog.uri"="http://127.0.0.1:2""#,
                "http://127.0.0.1:2",
            ),
            (
                r#""uri"="http://127.0.0.1:3", "iceberg.catalog.uri"="http://127.0.0.1:4""#,
                "http://127.0.0.1:3",
            ),
        ] {
            let mut guard = guard();
            guard.after(&format!(
                r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="REST", {properties});"#
            )).unwrap();
            assert!(
                matches!(guard.catalog_fact("catalog").unwrap(), Some(Catalog::Rest(uri)) if uri == expected)
            );
        }
        let mut guard = guard();
        assert!(guard.after(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest");"#).is_err());
    }
    #[test]
    fn non_iceberg_catalog_does_not_acquire_a_hadoop_write_guard() {
        let mut guard = guard();
        guard.after(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="paimon", "warehouse"="s3://novarocks/ready");"#).unwrap();
        assert!(matches!(
            guard.catalog_fact("catalog").unwrap(),
            Some(Catalog::Other)
        ));
    }
    #[test]
    fn hadoop_catalog_aliases_follow_provider_default_and_warehouse_precedence() {
        for (properties, expected) in [
            (r#""warehouse"="s3://novarocks/a""#, "s3://novarocks/a"),
            (
                r#""iceberg.catalog.warehouse"="s3://novarocks/b""#,
                "s3://novarocks/b",
            ),
            (
                r#""iceberg.catalog.type"="HADOOP", "warehouse"="s3://novarocks/c", "iceberg.catalog.warehouse"="s3://novarocks/d""#,
                "s3://novarocks/d",
            ),
        ] {
            let mut guard = guard();
            guard.after(&format!(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", {properties});"#)).unwrap();
            assert!(
                matches!(guard.catalog_fact("catalog").unwrap(), Some(Catalog::Hadoop(root)) if root == expected)
            );
            assert!(guard.before("CREATE DATABASE catalog.foreign").is_err());
        }
    }
    #[test]
    fn successful_sql_namespace_is_placed_then_read_back_at_exact_run_root() {
        let mut guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        let location = format!("{}/rest/{namespace}", guard.identity.scope_root);
        let (base, thread) = server(vec![
            serde_json::json!({}),
            serde_json::json!({"properties":{"location":location}}),
        ]);
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest(base));
        guard
            .after(&format!("CREATE DATABASE {namespace};"))
            .unwrap();
        let requests = thread.join().unwrap();
        assert!(requests[0].starts_with(&format!("POST /v1/namespaces/{namespace}/properties ")));
        assert!(requests[0].contains(&location));
        assert!(requests[1].starts_with(&format!("GET /v1/namespaces/{namespace} ")));
    }
    #[test]
    fn ignored_namespace_property_is_a_failure() {
        let mut guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        let (base, thread) = server(vec![
            serde_json::json!({}),
            serde_json::json!({"properties":{"location":"s3://novarocks/default"}}),
        ]);
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest(base));
        assert!(
            guard
                .after(&format!("CREATE DATABASE {namespace};"))
                .unwrap_err()
                .to_string()
                .contains("did not retain")
        );
        thread.join().unwrap();
    }
    #[test]
    fn actual_table_location_outside_original_scope_is_a_failure() {
        let guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        let (base, thread) = server(vec![
            serde_json::json!({"metadata":{"location":"s3://novarocks/default/table"}}),
        ]);
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest(base));
        let mut guard = guard;
        assert!(
            guard
                .after(&format!("CREATE TABLE {namespace}.t (id INT);"))
                .unwrap_err()
                .to_string()
                .contains("outside")
        );
        thread.join().unwrap();
    }
    #[test]
    fn readonly_warehouses_and_foreign_rest_namespaces_reject_mutation() {
        let guard = guard();
        guard.facts.0.lock().unwrap().insert(
            "catalog".into(),
            Catalog::Hadoop("s3://novarocks/benchmark".into()),
        );
        for sql in [
            "CREATE TABLE ns.t (id INT)",
            "INSERT INTO ns.t SELECT 1",
            "DROP DATABASE ns",
            "DELETE FROM ns.t",
        ] {
            assert!(guard.before(sql).is_err(), "{sql}");
        }
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest("http://127.0.0.1:1".into()));
        assert!(guard.before("CREATE DATABASE readonly_namespace").is_err());
        assert!(guard.before("SELECT * FROM readonly_namespace.t").is_ok());
    }
    #[test]
    fn qualified_branch_writes_require_the_original_rest_namespace() {
        let guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest("http://127.0.0.1:1".into()));
        for reference in ["branch_dev", "tag_release"] {
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.foreign.t.{reference} VALUES (1)"
                    ))
                    .unwrap_err()
                    .to_string()
                    .contains("exact namespace")
            );
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.{namespace}.t.{reference} VALUES (1)"
                    ))
                    .is_ok()
            );
        }
        for reference in ["unknown", "branch_", "tag_"] {
            let statements = novarocks_parser::parse(&format!(
                "INSERT INTO catalog.foreign.t.{reference} VALUES (1)"
            ))
            .unwrap();
            let Statement::Dml(DmlStatement::Insert(value)) = &statements[0] else {
                panic!("expected INSERT");
            };
            assert!(guard.target(&value.target, false).unwrap().is_none());
        }
    }
    #[test]
    fn expired_clock_rejects_before_rest_network_access() {
        let mut guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest("http://127.0.0.1:1".into()));
        guard.deadline = Instant::now();
        assert!(
            guard
                .before(&format!("CREATE DATABASE {namespace}"))
                .is_err()
        );
        assert!(
            guard
                .place_namespace("http://127.0.0.1:1", &namespace)
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
    }
}
