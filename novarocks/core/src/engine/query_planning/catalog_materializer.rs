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

use std::sync::Arc;

use novarocks_catalog::partition::LegacyRangePartition;
use novarocks_catalog::provider::CatalogProvider;
use novarocks_catalog::table::CatalogTable;

use crate::engine::query_planning::bindings::{
    QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::sql::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable,
};
use crate::sql::planner::table::TableDef;

/// Application materializer for connector-controlled table metadata.  The
/// interface is intentionally application-owned because it returns an exact
/// lease alongside planner facts.  It is not part of SQL's vocabulary.
pub(crate) trait QueryTableBindingLoader: Send + Sync {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<QueryTableBinding, String>;

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String>;
}

/// Application-owned catalog facade.  Its binding store is request-local and
/// retained by the caller as post-compile context; the SQL catalog trait does
/// not expose it.
pub(crate) struct CatalogServiceMaterializer<'a> {
    current_catalog: Option<&'a str>,
    service: &'a crate::sql::catalog::StandaloneCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
}

impl<'a> CatalogServiceMaterializer<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        service: &'a crate::sql::catalog::StandaloneCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self {
            current_catalog,
            service,
            bindings,
            loader,
        }
    }

    pub(crate) fn query_table_bindings(&self) -> Arc<QueryTableBindingStore> {
        Arc::clone(&self.bindings)
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
                let planner = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock")
                    .get(database, table)?;
                // COW writes register a query-local synthetic table in the
                // local catalog. Preserve that planner shape but materialize
                // exactly one physical connector lease under its real table
                // identity for later statistics and preparation.
                if let crate::sql::planner::table::ScanSource::IcebergDataFiles {
                    table: iceberg,
                    binding,
                    ..
                } = &planner.source
                {
                    let catalog = iceberg.catalog.clone();
                    let namespace = iceberg.namespace.clone();
                    let table_name = iceberg.table.clone();
                    if catalog != "default_catalog" {
                        let key = match binding {
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot => {
                                QueryTableBindingKey::strict_base(&catalog, &namespace, &table_name)
                            }
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles => {
                                let snapshot_id = iceberg.current_snapshot_id.ok_or_else(|| {
                                    format!(
                                        "explicit Iceberg input '{}.{}.{}' has no frozen snapshot identity",
                                        catalog, namespace, table_name
                                    )
                                })?;
                                QueryTableBindingKey::snapshot(
                                    &catalog,
                                    &namespace,
                                    &table_name,
                                    snapshot_id,
                                )
                            }
                        };
                        let token = self.bindings.resolve_or_insert(key, || {
                            let mut binding = self.loader.load_strict_base_table(
                                &catalog,
                                &namespace,
                                &table_name,
                            )?;
                            binding.resolved = ResolvedAnalyzerTable::from_planner(
                                Some("default_catalog"),
                                database,
                                planner,
                            );
                            Ok(binding)
                        })?;
                        return Ok(self.bindings.binding(token)?.resolved.clone());
                    }
                }
                let key = QueryTableBindingKey::analysis_lookup("default_catalog", database, table);
                let token = self.bindings.resolve_or_insert(key, || {
                    Ok(QueryTableBinding::local(
                        ResolvedAnalyzerTable::from_planner(
                            Some("default_catalog"),
                            database,
                            planner,
                        ),
                    ))
                })?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
            Some(catalog) => {
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = self.bindings.resolve_or_insert(key, || {
                    self.loader.load_strict_base_table(catalog, database, table)
                })?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
        }
    }

    fn metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self
                .service
                .local()
                .read()
                .expect("catalog service local read lock")
                .get(database, table),
            Some(catalog) => {
                self.loader
                    .load_metadata_table(catalog, database, table, metadata_table_type)
            }
        }
    }
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
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String> {
        self.metadata_table_def(catalog, database, table, metadata_table_type)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

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
}
