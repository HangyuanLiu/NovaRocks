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

//! Application catalog materialization for one admitted SQL request.
//!
//! This is intentionally outside `sql::catalog`: it owns connector-facing
//! resolution and the exact binding store used later by statistics and scan
//! preparation.  SQL sees the resulting neutral table facts solely through
//! `PlannerTableProvider`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use novarocks_catalog::partition::LegacyRangePartition;
use novarocks_catalog::provider::CatalogProvider;
use novarocks_catalog::table::CatalogTable;

use crate::engine::query_planning::bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::sql::binding::SqlTableBindingId;
use crate::sql::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable,
};
use crate::sql::planner::table::TableDef;

/// Convert an admitted Iceberg provider envelope into the one SQL-facing
/// table shape.  The caller supplies the token allocated by
/// `QueryTableBindingStore`; every concrete descriptor remains paired with
/// that token in `QueryScanMaterialization`.
pub(crate) fn iceberg_query_binding_from_materialization(
    materialization: crate::connector::iceberg::provider::IcebergQueryTableMaterialization,
    catalog: &str,
    namespace: &str,
    sql_table_name: &str,
    binding: SqlTableBindingId,
) -> Result<QueryTableBinding, String> {
    iceberg_query_binding_from_materialization_with_delta_plans(
        materialization,
        catalog,
        namespace,
        sql_table_name,
        binding,
        BTreeMap::new(),
    )
}

/// Equivalent to [`iceberg_query_binding_from_materialization`] with
/// application-admitted snapshot-window delta facts.  SQL still receives only
/// the binding token; preparation recovers this map from the same store.
pub(crate) fn iceberg_query_binding_from_materialization_with_delta_plans(
    materialization: crate::connector::iceberg::provider::IcebergQueryTableMaterialization,
    catalog: &str,
    namespace: &str,
    sql_table_name: &str,
    binding: SqlTableBindingId,
    delta_runtime_plans: BTreeMap<
        (i64, i64),
        crate::query_execution::preparation::scan::IcebergDeltaScanRuntimePlan,
    >,
) -> Result<QueryTableBinding, String> {
    use crate::connector::iceberg::scan_model::IcebergDataFileBinding;
    use crate::sql::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
    };

    let version = match materialization.binding {
        IcebergDataFileBinding::CurrentSnapshot => SqlTableVersionSelector::Current,
        IcebergDataFileBinding::ExplicitFiles => SqlTableVersionSelector::Snapshot(
            materialization.table.current_snapshot_id.ok_or_else(|| {
                format!(
                    "frozen Iceberg input '{}.{}.{}' has no snapshot identity",
                    materialization.table.catalog,
                    materialization.table.namespace,
                    materialization.table.table
                )
            })?,
        ),
    };
    let kind = match materialization.binding {
        IcebergDataFileBinding::CurrentSnapshot => SqlScanKind::Data { version },
        IcebergDataFileBinding::ExplicitFiles => SqlScanKind::FrozenInputSet { version },
    };
    let table_identity = SqlTableIdentity {
        catalog: materialization.table.catalog.clone(),
        namespace: materialization.table.namespace.clone(),
        table: materialization.table.table.clone(),
    };
    let planner = TableDef {
        name: sql_table_name.to_string(),
        columns: materialization.columns,
        iceberg_row_lineage_metadata_columns: materialization.iceberg_row_lineage_metadata_columns,
        source: ScanSource::Sql(
            SqlScanSource::new(binding, table_identity, kind).with_ukfk_facts(
                super::bindings::sql_ukfk_facts_from_admitted_table(&materialization.table),
            ),
        ),
    };
    Ok(QueryTableBinding {
        resolved: ResolvedAnalyzerTable::from_planner(Some(catalog), namespace, planner),
        statistics_pin: materialization.statistics_pin,
        planning_lease: Some(materialization.planning_lease),
        scan_materialization: Some(QueryScanMaterialization::IcebergDataFiles {
            table: materialization.table,
            files: materialization.files,
            binding: materialization.binding,
        }),
        delta_runtime_plans,
    })
}

/// Application materializer for connector-controlled table metadata.  The
/// interface is intentionally application-owned because it returns an exact
/// lease alongside planner facts.  It is not part of SQL's vocabulary.
pub(crate) trait QueryTableBindingLoader: Send + Sync {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String>;

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
        binding: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String>;
}

/// Application-owned catalog facade.  Its binding store is request-local and
/// retained by the caller as post-compile context; the SQL catalog trait does
/// not expose it.
pub(crate) struct CatalogServiceMaterializer<'a> {
    current_catalog: Option<&'a str>,
    service: &'a crate::engine::query_planning::catalog_runtime::QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
    /// Request-scoped synthetic relations used by application rewrite flows.
    /// They are intentionally kept next to the binding store instead of the
    /// shared memory catalog: SQL can only observe their projected tokenized
    /// scan after this materializer has admitted the exact connector lease.
    query_local_overlays: HashMap<(String, String), QueryLocalTableOverlay>,
}

/// One application-owned relation overlay for a generated query.
///
/// The overlay is a binding factory, not a `TableDef`: generated COW and MV
/// reads must supply their frozen provider facts to the request-local store
/// before SQL sees the resulting tokenized table.  Keeping the factory here
/// prevents a synthetic relation from leaking into the shared catalog.
#[derive(Clone)]
pub(crate) struct QueryLocalTableOverlay {
    namespace: String,
    table: String,
    key: QueryTableBindingKey,
    materialize:
        Arc<dyn Fn(SqlTableBindingId) -> Result<QueryTableBinding, String> + Send + Sync + 'static>,
}

impl QueryLocalTableOverlay {
    pub(crate) fn new(
        namespace: impl Into<String>,
        table: impl Into<String>,
        key: QueryTableBindingKey,
        materialize: impl Fn(SqlTableBindingId) -> Result<QueryTableBinding, String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let namespace = namespace.into();
        Self {
            table: table.into(),
            namespace,
            key,
            materialize: Arc::new(materialize),
        }
    }

    fn key(&self) -> (String, String) {
        (
            self.namespace.to_ascii_lowercase(),
            self.table.to_ascii_lowercase(),
        )
    }
}

impl<'a> CatalogServiceMaterializer<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        service: &'a crate::engine::query_planning::catalog_runtime::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self::new_with_query_local_overlays(current_catalog, service, bindings, loader, Vec::new())
    }

    pub(crate) fn new_with_query_local_overlays(
        current_catalog: Option<&'a str>,
        service: &'a crate::engine::query_planning::catalog_runtime::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
        overlays: Vec<QueryLocalTableOverlay>,
    ) -> Self {
        Self {
            current_catalog,
            service,
            bindings,
            loader,
            query_local_overlays: overlays
                .into_iter()
                .map(|overlay| (overlay.key(), overlay))
                .collect(),
        }
    }

    pub(crate) fn query_table_bindings(&self) -> Arc<QueryTableBindingStore> {
        Arc::clone(&self.bindings)
    }

    /// Publish one application-resolved table only after its scan has been
    /// projected into the SQL vocabulary with the token allocated for this
    /// request.  Provider loaders may temporarily use a legacy carrier while
    /// decoding connector metadata, but that carrier must not escape this
    /// method into analysis or the compiler.
    fn bind_for_sql(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        self.bindings.resolve_or_insert_with_id(key, |binding_id| {
            project_binding_for_sql(binding_id, load(binding_id)?)
        })
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                if let Some(overlay) = self
                    .query_local_overlays
                    .get(&(database.to_ascii_lowercase(), table.to_ascii_lowercase()))
                    .cloned()
                {
                    return self.resolve_query_local_overlay(overlay);
                }
                let planner = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock")
                    .get(database, table)?;
                let key = QueryTableBindingKey::analysis_lookup("default_catalog", database, table);
                let token = self.bind_for_sql(key, |binding| {
                    Ok(QueryTableBinding::local(
                        ResolvedAnalyzerTable::from_planner(
                            Some("default_catalog"),
                            database,
                            planner,
                        ),
                        binding,
                    ))
                })?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
            Some(catalog) => {
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader
                        .load_strict_base_table(catalog, database, table, binding_id)
                })?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
        }
    }

    /// Materialize a generated local relation through the same request store
    /// as ordinary external tables.  The factory receives the exact token it
    /// must attach to the SQL table, while frozen provider facts remain paired
    /// with that token in the returned application binding.
    fn resolve_query_local_overlay(
        &self,
        overlay: QueryLocalTableOverlay,
    ) -> Result<ResolvedAnalyzerTable, String> {
        let token =
            self.bind_for_sql(overlay.key, |binding_id| (overlay.materialize)(binding_id))?;
        Ok(self.bindings.binding(token)?.resolved.clone())
    }

    fn metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self
                .service
                .local()
                .read()
                .expect("catalog service local read lock")
                .get(database, table),
            Some(catalog) => {
                let key =
                    QueryTableBindingKey::metadata(catalog, database, table, metadata_table_type);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader.load_metadata_table(
                        catalog,
                        database,
                        table,
                        metadata_table_type,
                        binding_id,
                    )
                })?;
                Ok(self.bindings.binding(token)?.resolved.planner.clone())
            }
        }
    }
}

fn project_binding_for_sql(
    binding_id: SqlTableBindingId,
    binding: QueryTableBinding,
) -> Result<QueryTableBinding, String> {
    binding.validate_sql_scan_binding(binding_id)?;
    Ok(binding)
}

impl CatalogProvider for CatalogServiceMaterializer<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(None, database, table)
            .map(|resolved| resolved.catalog)
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
            .map(|resolved| resolved.catalog)
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        self.service
            .local()
            .read()
            .expect("catalog service local read lock")
            .get_legacy_range_partition(database, table, partition)
    }
}

impl PlannerTableProvider for CatalogServiceMaterializer<'_> {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl IcebergMetadataTableProvider for CatalogServiceMaterializer<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
    ) -> Result<TableDef, String> {
        self.metadata_table_def(catalog, database, table, metadata_table_type)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::sql::catalog::PlannerTableProvider;
    use crate::sql::planner::table::ScanSource;

    fn binding_id(scope: u64, ordinal: u32) -> SqlTableBindingId {
        SqlTableBindingId::new(
            crate::sql::binding::SqlTableBindingScopeId::new(
                NonZeroU64::new(scope).expect("non-zero scope"),
            ),
            NonZeroU32::new(ordinal).expect("non-zero ordinal"),
        )
    }

    fn local_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        QueryTableBinding::local(
            ResolvedAnalyzerTable::from_planner(
                Some("default_catalog"),
                "db",
                TableDef {
                    name: "orders".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::planner::table::test_sql_scan_source(
                        crate::sql::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
            ),
            binding,
        )
    }

    fn frozen_overlay_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        let table = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/orders".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let planner = TableDef {
            name: "__nr_cow_orders".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(crate::sql::planner::table::SqlScanSource::new(
                binding,
                crate::sql::planner::table::SqlTableIdentity {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "orders".to_string(),
                },
                crate::sql::planner::table::SqlScanKind::FrozenInputSet {
                    version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(7),
                },
            )),
        };
        QueryTableBinding {
            resolved: ResolvedAnalyzerTable::from_planner(Some("ice"), "db", planner),
            statistics_pin: None,
            planning_lease: None,
            scan_materialization: Some(QueryScanMaterialization::IcebergDataFiles {
                table,
                files: vec![],
                binding: IcebergDataFileBinding::ExplicitFiles,
            }),
            delta_runtime_plans: BTreeMap::new(),
        }
    }

    struct OverlayLoader;

    impl QueryTableBindingLoader for OverlayLoader {
        fn load_strict_base_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _binding: SqlTableBindingId,
        ) -> Result<QueryTableBinding, String> {
            Ok(local_binding(_binding))
        }

        fn load_metadata_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> Result<QueryTableBinding, String> {
            Err("metadata is not part of this overlay fixture".to_string())
        }
    }

    #[test]
    fn sqlx2_application_materializer_projects_local_scan_before_publication() {
        let binding =
            project_binding_for_sql(binding_id(101, 1), local_binding(binding_id(101, 1)))
                .expect("local scan must be tokenized before SQL receives it");

        assert!(matches!(
            binding.resolved.planner.source,
            crate::sql::planner::table::ScanSource::Sql(ref source)
                if source.binding == binding_id(101, 1)
        ));
    }

    #[test]
    fn sqlx2_application_materializer_rejects_foreign_scan_token() {
        let binding = local_binding(binding_id(102, 2));

        let error = match project_binding_for_sql(binding_id(102, 1), binding) {
            Ok(_) => panic!("foreign token must not enter this request"),
            Err(error) => error,
        };
        assert!(error.contains("different request binding"));
    }

    #[test]
    fn sqlx2_application_materializer_error_memoizes_by_canonical_identity() {
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        let attempts = AtomicUsize::new(0);
        let key = QueryTableBindingKey::strict_base("ICEBERG", "DB", "TABLE");

        let first = bindings.resolve_or_insert(key.clone(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("missing table".to_string())
        });
        let second = bindings.resolve_or_insert(key, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("must not load twice".to_string())
        });

        assert_eq!(first.unwrap_err(), "missing table");
        assert_eq!(second.unwrap_err(), "missing table");
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sqlx2_application_time_travel_overlay_uses_physical_snapshot_key() {
        let overlay = QueryTableBindingKey::analysis_lookup("ice", "db", "__sqlx1_tt_orders_42");
        let physical = QueryTableBindingKey::snapshot("ICE", "DB", "orders", 42);
        assert_eq!(overlay, physical);
    }

    #[test]
    fn sqlx2_application_cow_overlay_is_tokenized_without_local_catalog_registration() {
        let service = crate::engine::query_planning::catalog_runtime::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let materializer = CatalogServiceMaterializer::new_with_query_local_overlays(
            Some("default_catalog"),
            &service,
            Arc::clone(&bindings),
            Box::new(OverlayLoader),
            vec![QueryLocalTableOverlay::new(
                "db",
                "__nr_cow_orders",
                QueryTableBindingKey::snapshot("ice", "db", "orders", 7),
                |binding| Ok(frozen_overlay_binding(binding)),
            )],
        );

        let resolved = materializer
            .resolve_table_for_analysis(None, "db", "__nr_cow_orders")
            .expect("query-local overlay resolves");
        let ScanSource::Sql(source) = resolved.planner.source else {
            panic!("SQL must not receive the overlay's legacy scan source");
        };
        assert!(source.binding.belongs_to(bindings.scope()));
        assert!(matches!(
            source.kind,
            crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. }
        ));
        assert!(matches!(
            bindings
                .scan_materialization(source.binding)
                .expect("binding materialization"),
            Some(QueryScanMaterialization::IcebergDataFiles { .. })
        ));
        assert!(
            service
                .local()
                .read()
                .expect("catalog read")
                .get("db", "__nr_cow_orders")
                .is_err()
        );
    }
}
