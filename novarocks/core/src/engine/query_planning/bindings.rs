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

//! Application-owned query-local table bindings.
//!
//! A binding is deliberately more than a catalog table.  It captures the
//! exact connector control lease, table handle, incarnation and statistics
//! data version selected during admission.  SQL receives only the opaque
//! `SqlTableBindingId`; preparation and statistics must validate that token
//! against this store rather than acquiring a current connector generation.

use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::connector::backend::ResolvedTableStatisticsPin;
use crate::sql::binding::{SqlTableBindingId, SqlTableBindingScopeId};
use crate::sql::catalog::ResolvedAnalyzerTable;

static NEXT_BINDING_SCOPE: AtomicU64 = AtomicU64::new(1);

/// Canonical request-local lookup identity.  Names are normalized before the
/// binding is inserted, so a resolve failure is memoized just like success.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QueryTableBindingKey {
    catalog: String,
    namespace: String,
    table: String,
    selector: QueryTableBindingSelector,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum QueryTableBindingSelector {
    StrictBaseTable,
    Snapshot(i64),
    TimestampMillis(i64),
}

impl QueryTableBindingKey {
    pub(crate) fn strict_base(catalog: &str, namespace: &str, table: &str) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::StrictBaseTable,
        )
    }

    pub(crate) fn snapshot(catalog: &str, namespace: &str, table: &str, snapshot_id: i64) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::Snapshot(snapshot_id),
        )
    }

    pub(crate) fn timestamp_millis(
        catalog: &str,
        namespace: &str,
        table: &str,
        timestamp_millis: i64,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::TimestampMillis(timestamp_millis),
        )
    }

    fn new(
        catalog: &str,
        namespace: &str,
        table: &str,
        selector: QueryTableBindingSelector,
    ) -> Self {
        Self {
            catalog: catalog.to_ascii_lowercase(),
            namespace: namespace.to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            selector,
        }
    }
}

/// One successful application materialization.  Opaque connector authority
/// stays here; neither the SQL scan vocabulary nor SQL catalog facts contain
/// a provider table, files, cloud properties, or serialized metadata.
#[derive(Clone)]
pub(crate) struct QueryTableBinding {
    pub(crate) resolved: ResolvedAnalyzerTable,
    pub(crate) statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub(crate) planning_lease: Option<novarocks_spi::connector::ConnectorControlPlanningLease>,
}

struct StoredBinding {
    id: SqlTableBindingId,
}

/// Exact application authority paired with one compiler request.
pub(crate) struct QueryTableBindingStore {
    scope: SqlTableBindingScopeId,
    next_ordinal: Mutex<u32>,
    entries: Mutex<HashMap<QueryTableBindingKey, Result<StoredBinding, String>>>,
    by_id: Mutex<HashMap<SqlTableBindingId, Arc<QueryTableBinding>>>,
}

impl QueryTableBindingStore {
    /// Allocate one fresh process-local scope.  Scope exhaustion is explicit
    /// rather than silently reusing a token from another query.
    pub(crate) fn try_new() -> Result<Self, String> {
        let raw_scope = NEXT_BINDING_SCOPE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| "SQL table binding scope space is exhausted".to_string())?;
        let scope = NonZeroU64::new(raw_scope)
            .ok_or_else(|| "SQL table binding scope space is exhausted".to_string())?;
        Ok(Self {
            scope: SqlTableBindingScopeId::new(scope),
            next_ordinal: Mutex::new(0),
            entries: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn scope(&self) -> SqlTableBindingScopeId {
        self.scope
    }

    /// Memoize both success and failure.  The supplied load closure executes
    /// at most once for a canonical key in this request.
    pub(crate) fn resolve_or_insert(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce() -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        let mut entries = self.entries.lock().expect("query table binding lock");
        if let Some(entry) = entries.get(&key) {
            return entry.as_ref().map(|stored| stored.id).map_err(Clone::clone);
        }

        let result = load().and_then(|binding| {
            let id = self.allocate_id()?;
            let binding = Arc::new(binding);
            self.by_id
                .lock()
                .expect("query table binding by-id lock")
                .insert(id, Arc::clone(&binding));
            Ok(StoredBinding { id })
        });
        let response = result
            .as_ref()
            .map(|stored| stored.id)
            .map_err(Clone::clone);
        entries.insert(key, result);
        response
    }

    pub(crate) fn binding(&self, id: SqlTableBindingId) -> Result<Arc<QueryTableBinding>, String> {
        if !id.belongs_to(self.scope) {
            return Err("SQL table binding token belongs to a different request".to_string());
        }
        self.by_id
            .lock()
            .expect("query table binding by-id lock")
            .get(&id)
            .cloned()
            .ok_or_else(|| "SQL table binding token is missing from this request".to_string())
    }

    pub(crate) fn statistics_pin(
        &self,
        id: SqlTableBindingId,
    ) -> Result<Option<ResolvedTableStatisticsPin>, String> {
        Ok(self.binding(id)?.statistics_pin.clone())
    }

    pub(crate) fn planning_lease(
        &self,
        id: SqlTableBindingId,
    ) -> Result<Option<novarocks_spi::connector::ConnectorControlPlanningLease>, String> {
        Ok(self.binding(id)?.planning_lease.clone())
    }

    fn allocate_id(&self) -> Result<SqlTableBindingId, String> {
        let mut next = self
            .next_ordinal
            .lock()
            .expect("query table binding ordinal lock");
        *next = next
            .checked_add(1)
            .ok_or_else(|| "SQL table binding ordinal space is exhausted".to_string())?;
        let ordinal = NonZeroU32::new(*next)
            .ok_or_else(|| "SQL table binding ordinal space is exhausted".to_string())?;
        Ok(SqlTableBindingId::new(self.scope, ordinal))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore};

    fn local_binding() -> QueryTableBinding {
        QueryTableBinding {
            resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                Some("default_catalog"),
                "db",
                crate::sql::planner::table::TableDef {
                    name: "orders".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::planner::table::ScanSource::ConnectorPinned,
                },
            ),
            statistics_pin: None,
            planning_lease: None,
        }
    }

    #[test]
    fn sqlx2_binding_store_memoizes_failure_once_per_request() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let attempts = AtomicUsize::new(0);
        let key = QueryTableBindingKey::strict_base("ICE", "DB", "ORDERS");

        let first = store.resolve_or_insert(key.clone(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("missing table".to_string())
        });
        let second = store.resolve_or_insert(key, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("must not load twice".to_string())
        });

        assert_eq!(first.unwrap_err(), "missing table");
        assert_eq!(second.unwrap_err(), "missing table");
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sqlx2_binding_store_rejects_cross_request_tokens_before_submission() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let token = crate::sql::binding::SqlTableBindingId::new(
            first.scope(),
            std::num::NonZeroU32::new(1).expect("nonzero"),
        );

        assert!(second.binding(token).is_err());
    }

    #[test]
    fn sqlx2_binding_store_reuses_one_exact_binding_only_within_the_request() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let key = QueryTableBindingKey::strict_base("ice", "db", "orders");

        let first_token = first
            .resolve_or_insert(key.clone(), || Ok(local_binding()))
            .expect("first token");
        let repeated_token = first
            .resolve_or_insert(key.clone(), || Err("must not reload".to_string()))
            .expect("repeated token");
        let second_token = second
            .resolve_or_insert(key, || Ok(local_binding()))
            .expect("second token");

        assert_eq!(first_token, repeated_token);
        assert_ne!(first_token, second_token);
        assert_eq!(
            first
                .binding(first_token)
                .expect("exact binding")
                .resolved
                .planner
                .name,
            "orders"
        );
        assert!(second.binding(first_token).is_err());
    }
}
