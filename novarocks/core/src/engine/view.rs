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

//! Session-view registry seam.
//!
//! FEH-4a extracts the in-memory `CREATE VIEW` registry that used to live as a
//! raw `RwLock<HashMap>` field on `StandaloneState` behind a `ViewCatalog`
//! trait. The core adapter `InMemoryViewCatalog` preserves the previous
//! behavior in normal operation; the only deviation is that lock-poison
//! handling is unified to `.expect()` (matching the rewrite site's existing
//! pattern). FEH-4b replaces this adapter with a frontend, StateStore-backed
//! implementation injected through the server seam.

use std::collections::HashMap;
use std::sync::RwLock;

use sqlparser::ast as sqlast;

/// Error returned by [`ViewCatalog::create`]. The caller owns the frozen
/// user-facing message so the trait stays free of formatting policy.
#[derive(Debug)]
pub(crate) enum ViewCatalogError {
    /// The `(db, name)` key already exists and `or_replace` was false.
    AlreadyExists,
}

/// Read/write seam over the session-view registry. Object-safe, `Send + Sync`.
/// Exposes only registry primitives; name normalization, Iceberg-vs-session
/// routing, result ordering and error text stay in the callers.
pub(crate) trait ViewCatalog: Send + Sync {
    /// Atomic full clone of the registry for the pre-analyzer rewrite walk.
    /// Mirrors the single read-lock snapshot the rewrite used to take.
    fn snapshot(&self) -> HashMap<(String, String), Box<sqlast::Query>>;

    /// Session view names whose database key equals `db` (already normalized
    /// by the caller). Unsorted; the caller sorts to match SHOW VIEWS output.
    fn list_in_database(&self, db: &str) -> Vec<String>;

    /// Atomic check-and-insert. Returns `Err(AlreadyExists)` when the key is
    /// present and `!or_replace`; otherwise inserts/replaces.
    fn create(
        &self,
        db: String,
        name: String,
        query: Box<sqlast::Query>,
        or_replace: bool,
    ) -> Result<(), ViewCatalogError>;

    /// Remove `(db, name)`. Absent key is a silent no-op (matches the previous
    /// unconditional `remove` that ignored `IF EXISTS`).
    fn remove(&self, db: &str, name: &str);

    /// Remove every entry whose database key equals `db` (caller passes the
    /// already-normalized database name).
    fn remove_database(&self, db: &str);
}

/// Core, process-memory `ViewCatalog`. Wraps the same `RwLock<HashMap>` the
/// registry used before FEH-4a. Replaced by a frontend impl in FEH-4b.
#[derive(Default)]
pub(crate) struct InMemoryViewCatalog {
    views: RwLock<HashMap<(String, String), Box<sqlast::Query>>>,
}

impl InMemoryViewCatalog {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl ViewCatalog for InMemoryViewCatalog {
    fn snapshot(&self) -> HashMap<(String, String), Box<sqlast::Query>> {
        self.views.read().expect("view registry read lock").clone()
    }

    fn list_in_database(&self, db: &str) -> Vec<String> {
        self.views
            .read()
            .expect("view registry read lock")
            .keys()
            .filter(|(database, _)| database == db)
            .map(|(_, view)| view.clone())
            .collect()
    }

    fn create(
        &self,
        db: String,
        name: String,
        query: Box<sqlast::Query>,
        or_replace: bool,
    ) -> Result<(), ViewCatalogError> {
        let mut views = self.views.write().expect("view registry write lock");
        if views.contains_key(&(db.clone(), name.clone())) && !or_replace {
            return Err(ViewCatalogError::AlreadyExists);
        }
        views.insert((db, name), query);
        Ok(())
    }

    fn remove(&self, db: &str, name: &str) {
        self.views
            .write()
            .expect("view registry write lock")
            .remove(&(db.to_string(), name.to_string()));
    }

    fn remove_database(&self, db: &str) {
        self.views
            .write()
            .expect("view registry write lock")
            .retain(|(view_db, _), _| view_db != db);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;
    use sqlparser::parser::Parser;

    fn parse_query(sql: &str) -> Box<sqlast::Query> {
        let mut parser = Parser::new(&StarRocksDialect).try_with_sql(sql).unwrap();
        match parser.parse_statement().unwrap() {
            sqlast::Statement::Query(q) => q,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn create_inserts_then_snapshot_returns_it() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v".into(), parse_query("SELECT 1 AS a"), false)
            .expect("insert");
        let snap = c.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains_key(&("db".to_string(), "v".to_string())));
    }

    #[test]
    fn create_rejects_existing_without_or_replace() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v".into(), parse_query("SELECT 1"), false)
            .expect("first insert");
        let err = c.create("db".into(), "v".into(), parse_query("SELECT 2"), false);
        assert!(matches!(err, Err(ViewCatalogError::AlreadyExists)));
        // unchanged body
        assert_eq!(
            c.snapshot()[&("db".into(), "v".into())].to_string(),
            "SELECT 1"
        );
    }

    #[test]
    fn create_or_replace_overwrites() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v".into(), parse_query("SELECT 1"), false)
            .expect("first");
        c.create("db".into(), "v".into(), parse_query("SELECT 2"), true)
            .expect("replace");
        assert_eq!(
            c.snapshot()[&("db".into(), "v".into())].to_string(),
            "SELECT 2"
        );
    }

    #[test]
    fn remove_is_silent_noop_when_absent() {
        let c = InMemoryViewCatalog::new();
        c.remove("db", "missing"); // must not panic
        c.create("db".into(), "v".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.remove("db", "v");
        assert!(c.snapshot().is_empty());
    }

    #[test]
    fn remove_database_only_drops_matching_db() {
        let c = InMemoryViewCatalog::new();
        c.create("a".into(), "v".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.create("b".into(), "v".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.remove_database("a");
        let snap = c.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains_key(&("b".to_string(), "v".to_string())));
    }

    #[test]
    fn list_in_database_returns_matching_views_unsorted() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v2".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.create("db".into(), "v1".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.create("other".into(), "x".into(), parse_query("SELECT 1"), false)
            .unwrap();
        let mut names = c.list_in_database("db");
        names.sort();
        assert_eq!(names, vec!["v1".to_string(), "v2".to_string()]);
    }
}
