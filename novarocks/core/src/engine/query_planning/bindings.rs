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

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::connector::backend::ResolvedTableStatisticsPin;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergTableInfo,
};
use crate::sql::binding::{SqlTableBindingId, SqlTableBindingScopeId};
use crate::sql::catalog::ResolvedAnalyzerTable;
use crate::sql::planner::table::{
    ScanSource, SqlMetadataTableKind, SqlScanKind, SqlScanSource, SqlTableIdentity,
    SqlUkFkTableFacts,
};

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum QueryTableBindingSelector {
    StrictBaseTable,
    /// A terminal writer target. This remains separate from a read binding
    /// for the same physical table because the writer's frozen physical
    /// schema may include hidden lineage or MV state columns that a scan does
    /// not expose.
    WriteTarget,
    Snapshot(i64),
    TimestampMillis(i64),
    Metadata(SqlMetadataTableKind),
    /// One frozen materialized-view target.  The target UUID distinguishes a
    /// recreated table at the same name, while the snapshot keeps target-state
    /// and target-locator scans on the exact refresh baseline.
    MvTarget {
        target_table_uuid: String,
        frozen_snapshot_id: Option<i64>,
    },
}

impl QueryTableBindingKey {
    /// Resolve the synthetic time-travel analyzer identity to the canonical
    /// physical table and snapshot selector before it reaches the request
    /// local memo.  This overlay is intentionally local to the binding
    /// store; it must never register a synthetic table in the global catalog.
    pub(crate) fn analysis_lookup(catalog: &str, namespace: &str, table: &str) -> Self {
        if let Some((base_table, snapshot_id)) = parse_time_travel_overlay_identity(table) {
            return Self::snapshot(catalog, namespace, base_table, snapshot_id);
        }
        Self::strict_base(catalog, namespace, table)
    }
    pub(crate) fn strict_base(catalog: &str, namespace: &str, table: &str) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::StrictBaseTable,
        )
    }

    /// Reserve an exact terminal writer target.  A write must never reuse a
    /// same-name read binding: those bindings carry different SQL facts while
    /// both remain valid for their independently frozen application roles.
    pub(crate) fn write_target(catalog: &str, namespace: &str, table: &str) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::WriteTarget,
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

    pub(crate) fn metadata(
        catalog: &str,
        namespace: &str,
        table: &str,
        kind: SqlMetadataTableKind,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::Metadata(kind),
        )
    }

    /// Identity for a materialized-view refresh target captured during
    /// admission.  This is deliberately distinct from a normal base-table or
    /// time-travel key: both target-state and target-locator scans must reuse
    /// this same frozen materialization, never a later target generation.
    pub(crate) fn mv_target(
        catalog: &str,
        namespace: &str,
        table: &str,
        target_table_uuid: &str,
        frozen_snapshot_id: Option<i64>,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::MvTarget {
                target_table_uuid: target_table_uuid.to_ascii_lowercase(),
                frozen_snapshot_id,
            },
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

pub(crate) fn parse_time_travel_overlay_identity(table: &str) -> Option<(&str, i64)> {
    let encoded = table.strip_prefix("__sqlx1_tt_")?;
    let (base_table, snapshot_id) = encoded.rsplit_once('_')?;
    (!base_table.is_empty())
        .then(|| snapshot_id.parse::<i64>().ok())
        .flatten()
        .map(|snapshot_id| (base_table, snapshot_id))
}

/// One successful application materialization.  Opaque connector authority
/// stays here; neither the SQL scan vocabulary nor SQL catalog facts contain
/// a provider table, files, cloud properties, or serialized metadata.
#[derive(Clone)]
pub(crate) struct QueryTableBinding {
    pub(crate) resolved: ResolvedAnalyzerTable,
    pub(crate) statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub(crate) planning_lease: Option<novarocks_spi::connector::ConnectorControlPlanningLease>,
    /// Provider facts required by scan preparation.  This is deliberately
    /// application-owned and paired with the same token as `resolved`; it is
    /// never embedded in a SQL logical or distributed plan.
    pub(crate) scan_materialization: Option<QueryScanMaterialization>,
    /// Exact snapshot-window physical facts admitted for a SQL delta scan.
    /// They remain application-owned and are retrieved only through this
    /// binding's request-local token during preparation.
    pub(crate) delta_runtime_plans: BTreeMap<
        (i64, i64),
        crate::query_execution::preparation::scan::IcebergDeltaScanRuntimePlan,
    >,
}

/// Exact provider scan facts retained after admission.  The concrete Iceberg
/// representation is temporary only at this application boundary while SQL
/// callers are migrated to `SqlScanSource`; preparation must obtain it by the
/// request-local binding token rather than from a planner table.
#[derive(Clone)]
pub(crate) enum QueryScanMaterialization {
    IcebergDataFiles {
        table: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
        binding: IcebergDataFileBinding,
    },
    /// Exact provider facts for one metadata alias.  These are intentionally
    /// retained outside the SQL plan: the compiler sees only `SqlMetadata`
    /// plus this binding token, while preparation/native assembly recover the
    /// unchanged Iceberg carrier from the same request-local store.
    IcebergMetadata {
        table: IcebergTableInfo,
        metadata_table_type: SqlMetadataTableKind,
        serialized_table: String,
        metadata_payload: Option<String>,
    },
    /// Exact target facts retained for one MV refresh.  SQL sees the binding
    /// token and target-state/locator facts only; the provider table, frozen
    /// files, and admission lease remain application-owned in this store.
    ///
    /// `frozen_snapshot_id` is separate from `table.current_snapshot_id`: the
    /// latter is provider metadata and may be absent for the empty-target
    /// bootstrap case, whereas the former is the refresh contract identity.
    IcebergMvTarget {
        table: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
        binding: IcebergDataFileBinding,
        target_table_uuid: String,
        frozen_snapshot_id: Option<i64>,
        /// Admission-frozen facts for the aggregate target-state lane.  These
        /// stay with the provider materialization rather than the SQL scan:
        /// SQL only states whether an allow-list is required, while
        /// preparation derives matching files from this exact file set.
        target_state_partition_filter: crate::mv::model::TargetPartitionFilter,
        target_partition_contract: Option<crate::mv::persistence::schema::MvPartitionContract>,
    },
}

impl QueryTableBinding {
    pub(crate) fn local(mut resolved: ResolvedAnalyzerTable, binding: SqlTableBindingId) -> Self {
        let identity = &resolved.catalog.identity;
        resolved.planner.source = ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: identity.catalog.clone(),
                namespace: identity.namespace.clone(),
                table: identity.table.clone(),
            },
            SqlScanKind::ConnectorRead,
        ));
        Self {
            resolved,
            statistics_pin: None,
            planning_lease: None,
            scan_materialization: None,
            delta_runtime_plans: BTreeMap::new(),
        }
    }

    /// Verify that the application materializer paired this table with the
    /// token it reserved.  Concrete provider scans are never accepted here:
    /// they must already live in `scan_materialization` before SQL receives
    /// the table facts.
    pub(crate) fn validate_sql_scan_binding(
        &self,
        binding: SqlTableBindingId,
    ) -> Result<(), String> {
        match &self.resolved.planner.source {
            ScanSource::Sql(source) if source.binding == binding => Ok(()),
            ScanSource::Sql(_) => Err(
                "catalog materialization produced a SQL scan with a different request binding"
                    .to_string(),
            ),
        }
    }
}

/// Convert only the two optimizer constraint properties while this
/// application binding still owns the admitted provider descriptor.  Parse
/// failures preserve the historical conservative behavior: the SQL scan gets
/// no UK/FK facts instead of a guessed constraint.  The serialized metadata
/// itself never crosses into the SQL scan source.
pub(crate) fn sql_ukfk_facts_from_admitted_table(table: &IcebergTableInfo) -> SqlUkFkTableFacts {
    let Some(serialized) = table.serialized_metadata.as_deref() else {
        return SqlUkFkTableFacts::default();
    };
    let Ok(metadata) = serde_json::from_str::<iceberg::spec::TableMetadata>(serialized) else {
        return SqlUkFkTableFacts::default();
    };
    let properties = metadata
        .properties()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    SqlUkFkTableFacts::from_frozen_properties(&properties)
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

    /// Construct a deterministic request-local store for owner-side unit
    /// fixtures.  Production admission must always use `try_new`, which
    /// allocates a process-unique scope.  Tests use this only alongside
    /// `test_sql_scan_source`, whose token has the same fixed scope.
    #[cfg(test)]
    pub(crate) fn try_new_with_scope_for_test(scope: NonZeroU64) -> Self {
        Self {
            scope: SqlTableBindingScopeId::new(scope),
            next_ordinal: Mutex::new(0),
            entries: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
        }
    }

    /// Stable, redacted identity material for one admitted binding set.
    ///
    /// This is used to bind application-side prepared artifacts (for example
    /// CTAS) to the exact catalog/statistics/control generation used during
    /// compilation. Opaque provider bytes are hashed rather than embedded so
    /// the digest cannot become a provider payload carrier.
    pub(crate) fn stable_digest_material(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};

        let mut material = Vec::new();
        material.extend_from_slice(&self.scope().get().get().to_be_bytes());
        for (binding_id, binding) in self.captured_bindings() {
            material.extend_from_slice(&binding_id.ordinal().get().to_be_bytes());
            let identity = binding.resolved.catalog.identity.fqn();
            material.extend_from_slice(&(identity.len() as u64).to_be_bytes());
            material.extend_from_slice(identity.as_bytes());
            if let Some(pin) = &binding.statistics_pin {
                material.extend_from_slice(pin.table.owner().as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(Sha256::digest(pin.table.payload()).as_slice());
                material.extend_from_slice(Sha256::digest(pin.data_version.as_bytes()).as_slice());
            }
            if let Some(lease) = &binding.planning_lease {
                let descriptor = lease.binding().descriptor();
                material.extend_from_slice(descriptor.provider_id.as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(descriptor.instance_id.as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(&lease.binding().incarnation().to_bytes());
            }
            material.push(0xff);
        }
        material
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
        self.resolve_or_insert_with_id(key, |_| load())
    }

    /// Reserve the request-local token before projecting provider facts into a
    /// SQL table.  The loader cannot observe any other request's token, and
    /// the provisional token is inserted only when materialization succeeds.
    ///
    /// This is the admission seam used by the `SqlScanSource` cutover: the
    /// application loader receives the exact token that the resulting SQL
    /// table will carry, while the concrete scan authority remains in this
    /// store. Failed loads remain memoized and never publish their token.
    pub(crate) fn resolve_or_insert_with_id(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        let mut entries = self.entries.lock().expect("query table binding lock");
        if let Some(entry) = entries.get(&key) {
            return entry.as_ref().map(|stored| stored.id).map_err(Clone::clone);
        }

        let result = self.allocate_id().and_then(|id| {
            load(id).map(|binding| {
                let binding = Arc::new(binding);
                self.by_id
                    .lock()
                    .expect("query table binding by-id lock")
                    .insert(id, Arc::clone(&binding));
                StoredBinding { id }
            })
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

    /// Recover provider scan facts only through the exact request-local token.
    /// A missing materialization is a submission-time contract failure, not a
    /// reason to resolve a current table or connector generation.
    pub(crate) fn scan_materialization(
        &self,
        id: SqlTableBindingId,
    ) -> Result<Option<QueryScanMaterialization>, String> {
        Ok(self.binding(id)?.scan_materialization.clone())
    }

    /// Return the immutable bindings captured during admission.  The caller
    /// may project them into compiler input, but must not use this view to
    /// acquire a newer connector generation.
    pub(crate) fn captured_bindings(&self) -> Vec<(SqlTableBindingId, Arc<QueryTableBinding>)> {
        let mut bindings: Vec<_> = self
            .by_id
            .lock()
            .expect("query table binding by-id lock")
            .iter()
            .map(|(id, binding)| (*id, Arc::clone(binding)))
            .collect();
        bindings.sort_by_key(|(id, _)| id.ordinal().get());
        bindings
    }

    /// Lookup the exact resolution retained for the old physical scan facts
    /// while production callers are moved to `SqlScanSource`.  The result is
    /// still retrieved from this one token store; this helper never acquires a
    /// current connector generation.
    pub(crate) fn strict_base_binding(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<Arc<QueryTableBinding>> {
        self.binding_for_key(&QueryTableBindingKey::strict_base(
            catalog, namespace, table,
        ))
    }

    /// Return the explicitly admitted Iceberg writer binding for one target.
    /// Read and write bindings for the same physical table are intentionally
    /// distinct: the writer token owns its physical output schema and exact
    /// lease, while scans retain their own selector and materialization.
    pub(crate) fn admitted_iceberg_write_binding_id(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<SqlTableBindingId, String> {
        let key = QueryTableBindingKey::write_target(catalog, namespace, table);
        let Some(binding) = self.binding_for_key(&key) else {
            return Err(format!(
                "SQL write target {catalog}.{namespace}.{table} was not admitted into this query binding store"
            ));
        };
        match binding.scan_materialization.as_ref() {
            Some(
                QueryScanMaterialization::IcebergDataFiles { .. }
                | QueryScanMaterialization::IcebergMvTarget { .. },
            ) => match &binding.resolved.planner.source {
                ScanSource::Sql(source) => Ok(source.binding),
                _ => Err("SQL write target binding is missing its SQL binding token".to_string()),
            },
            _ => Err(format!(
                "SQL write target {catalog}.{namespace}.{table} is missing admitted Iceberg provider facts"
            )),
        }
    }

    /// Transitional lookup for legacy physical scan facts.  It resolves only
    /// the request-local key already captured by admission; no provider call
    /// or latest-generation acquire is possible here.
    pub(crate) fn iceberg_data_file_binding_id(
        &self,
        table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
        binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding,
    ) -> Option<SqlTableBindingId> {
        use crate::connector::iceberg::scan_model::IcebergDataFileBinding;

        let key = match binding {
            IcebergDataFileBinding::ExplicitFiles => table.current_snapshot_id.map(|snapshot_id| {
                QueryTableBindingKey::snapshot(
                    &table.catalog,
                    &table.namespace,
                    &table.table,
                    snapshot_id,
                )
            }),
            _ => Some(QueryTableBindingKey::strict_base(
                &table.catalog,
                &table.namespace,
                &table.table,
            )),
        }?;
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(&key)
            .and_then(|entry| entry.as_ref().ok().map(|stored| stored.id))
    }

    /// Return the admission-frozen binding for one metadata alias.  Metadata
    /// scans must not reuse a base-table token: the provider may resolve the
    /// alias through a distinct table handle and schema version.
    pub(crate) fn metadata_binding_id(
        &self,
        table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
        kind: SqlMetadataTableKind,
    ) -> Option<SqlTableBindingId> {
        let key =
            QueryTableBindingKey::metadata(&table.catalog, &table.namespace, &table.table, kind);
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(&key)
            .and_then(|entry| entry.as_ref().ok().map(|stored| stored.id))
    }

    /// Return the one admission-frozen MV target binding.  The UUID and
    /// snapshot are part of the lookup key so a recreated target or a later
    /// refresh baseline can never reuse an earlier request's authority.
    pub(crate) fn mv_target_binding_id(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        target_table_uuid: &str,
        frozen_snapshot_id: Option<i64>,
    ) -> Option<SqlTableBindingId> {
        let key = QueryTableBindingKey::mv_target(
            catalog,
            namespace,
            table,
            target_table_uuid,
            frozen_snapshot_id,
        );
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(&key)
            .and_then(|entry| entry.as_ref().ok().map(|stored| stored.id))
    }

    fn binding_for_key(&self, key: &QueryTableBindingKey) -> Option<Arc<QueryTableBinding>> {
        let id = self
            .entries
            .lock()
            .expect("query table binding lock")
            .get(key)
            .and_then(|entry| entry.as_ref().ok().map(|stored| stored.id))?;
        self.binding(id).ok()
    }

    #[cfg(test)]
    pub(crate) fn insert_strict_base_binding_for_test(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: QueryTableBinding,
    ) {
        let key = QueryTableBindingKey::strict_base(catalog, namespace, table);
        self.resolve_or_insert(key, || Ok(binding))
            .expect("test binding insertion must allocate a token");
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
        let binding = crate::sql::binding::SqlTableBindingId::new(
            crate::sql::binding::SqlTableBindingScopeId::new(
                std::num::NonZeroU64::new(1).expect("scope"),
            ),
            std::num::NonZeroU32::new(1).expect("ordinal"),
        );
        QueryTableBinding::local(
            crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                Some("default_catalog"),
                "db",
                crate::sql::planner::table::TableDef {
                    name: "orders".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::planner::table::ScanSource::Sql(
                        crate::sql::planner::table::SqlScanSource::new(
                            binding,
                            crate::sql::planner::table::SqlTableIdentity {
                                catalog: "default_catalog".to_string(),
                                namespace: "db".to_string(),
                                table: "orders".to_string(),
                            },
                            crate::sql::planner::table::SqlScanKind::ConnectorRead,
                        ),
                    ),
                },
            ),
            binding,
        )
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

    #[test]
    fn sqlx2_binding_digest_is_stable_per_request_and_scoped_across_requests() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let first_key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        first
            .resolve_or_insert(first_key, || Ok(local_binding()))
            .expect("first binding");

        let first_digest = first.stable_digest_material();
        assert_eq!(first_digest, first.stable_digest_material());

        let second = QueryTableBindingStore::try_new().expect("second store");
        let second_key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        second
            .resolve_or_insert(second_key, || Ok(local_binding()))
            .expect("second binding");
        assert_ne!(first_digest, second.stable_digest_material());
    }

    #[test]
    fn sqlx2_binding_loader_receives_the_token_published_by_the_store() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        let observed = std::sync::Mutex::new(None);

        let token = store
            .resolve_or_insert_with_id(key, |id| {
                *observed.lock().expect("observed token lock") = Some(id);
                Ok(local_binding())
            })
            .expect("binding token");

        assert_eq!(
            *observed.lock().expect("observed token lock"),
            Some(token),
            "the SQL projection must carry the exact token published by admission"
        );
        assert!(store.binding(token).is_ok());
    }

    #[test]
    fn sqlx2_binding_metadata_alias_uses_a_distinct_request_local_token() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let base = store
            .resolve_or_insert(
                QueryTableBindingKey::strict_base("ice", "db", "orders"),
                || Ok(local_binding()),
            )
            .expect("base token");
        let metadata_key = QueryTableBindingKey::metadata(
            "ice",
            "db",
            "orders",
            crate::sql::planner::table::SqlMetadataTableKind::Snapshots,
        );
        let metadata = store
            .resolve_or_insert(metadata_key.clone(), || Ok(local_binding()))
            .expect("metadata token");
        let repeated = store
            .resolve_or_insert(metadata_key, || {
                Err("must not reload metadata alias".to_string())
            })
            .expect("memoized metadata token");

        assert_ne!(base, metadata);
        assert_eq!(metadata, repeated);
    }

    #[test]
    fn sqlx2_binding_writer_target_is_distinct_from_same_name_scan() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let scan = store
            .resolve_or_insert(
                QueryTableBindingKey::strict_base("ice", "db", "orders"),
                || Ok(local_binding()),
            )
            .expect("scan token");
        let writer_key = QueryTableBindingKey::write_target("ice", "db", "orders");
        let writer = store
            .resolve_or_insert(writer_key.clone(), || Ok(local_binding()))
            .expect("writer token");
        let repeated = store
            .resolve_or_insert(writer_key, || {
                Err("must not rematerialize writer".to_string())
            })
            .expect("memoized writer token");

        assert_ne!(scan, writer);
        assert_eq!(writer, repeated);
    }

    #[test]
    fn sqlx2_binding_mv_target_reuses_one_frozen_target_only_within_the_request() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let key = QueryTableBindingKey::mv_target(
            "ice",
            "analytics",
            "orders_mv",
            "target-uuid-a",
            Some(42),
        );

        let first_token = first
            .resolve_or_insert(key.clone(), || Ok(local_binding()))
            .expect("first MV target token");
        let repeated_token = first
            .resolve_or_insert(key.clone(), || {
                Err("must not rematerialize target".to_string())
            })
            .expect("memoized MV target token");
        let second_token = second
            .resolve_or_insert(key, || Ok(local_binding()))
            .expect("second MV target token");

        assert_eq!(first_token, repeated_token);
        assert_ne!(first_token, second_token);
        assert_eq!(
            first.mv_target_binding_id("ICE", "ANALYTICS", "ORDERS_MV", "TARGET-UUID-A", Some(42),),
            Some(first_token),
        );
        assert!(second.binding(first_token).is_err());
    }

    #[test]
    fn sqlx2_binding_mv_target_keeps_uuid_and_snapshot_in_the_identity() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let first = store
            .resolve_or_insert(
                QueryTableBindingKey::mv_target(
                    "ice",
                    "analytics",
                    "orders_mv",
                    "target-uuid-a",
                    Some(42),
                ),
                || Ok(local_binding()),
            )
            .expect("first target");
        let recreated = store
            .resolve_or_insert(
                QueryTableBindingKey::mv_target(
                    "ice",
                    "analytics",
                    "orders_mv",
                    "target-uuid-b",
                    Some(42),
                ),
                || Ok(local_binding()),
            )
            .expect("recreated target");
        let later_snapshot = store
            .resolve_or_insert(
                QueryTableBindingKey::mv_target(
                    "ice",
                    "analytics",
                    "orders_mv",
                    "target-uuid-a",
                    Some(43),
                ),
                || Ok(local_binding()),
            )
            .expect("later snapshot target");

        assert_ne!(first, recreated);
        assert_ne!(first, later_snapshot);
    }
}
