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

//! Query-scoped catalog materialization.
//!
//! The SQL layer owns the lookup identity and memoization boundary.  The
//! application supplies a provider-neutral loader which materializes external
//! tables and retains the exact connector planning lease that supplied their
//! metadata.  SQL catalog code never depends on a concrete connector
//! provider.

use crate::connector::backend::ResolvedTableStatisticsPin;
use crate::sql::catalog::{
    CatalogRuntimeMetadata, IcebergMetadataTableProvider, PlannerTableProvider,
    ResolvedAnalyzerTable, TableLookupMode,
};
use crate::sql::planner::table::TableDef;
use novarocks_catalog::partition::LegacyRangePartition;
use novarocks_catalog::provider::CatalogProvider;
use novarocks_catalog::service::CatalogService;
use novarocks_catalog::table::CatalogTable;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TableBindingKey {
    catalog: String,
    namespace: String,
    table: String,
    selector: TableBindingSelector,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum TableBindingSelector {
    StrictBaseTable,
    TimeTravelSnapshot(i64),
}

impl TableBindingKey {
    fn strict_base(catalog: &str, namespace: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_ascii_lowercase(),
            namespace: namespace.to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            selector: TableBindingSelector::StrictBaseTable,
        }
    }

    fn analysis_lookup(catalog: &str, namespace: &str, table: &str) -> Self {
        if let Some((base_table, snapshot_id)) = parse_time_travel_overlay_identity(table) {
            return Self {
                catalog: catalog.to_ascii_lowercase(),
                namespace: namespace.to_ascii_lowercase(),
                table: base_table.to_ascii_lowercase(),
                selector: TableBindingSelector::TimeTravelSnapshot(snapshot_id),
            };
        }
        Self::strict_base(catalog, namespace, table)
    }

    fn time_travel(catalog: &str, namespace: &str, table: &str, snapshot_id: i64) -> Self {
        Self {
            catalog: catalog.to_ascii_lowercase(),
            namespace: namespace.to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            selector: TableBindingSelector::TimeTravelSnapshot(snapshot_id),
        }
    }
}

fn parse_time_travel_overlay_identity(table: &str) -> Option<(&str, i64)> {
    let encoded = table.strip_prefix("__sqlx1_tt_")?;
    let (base_table, snapshot_id) = encoded.rsplit_once('_')?;
    (!base_table.is_empty())
        .then(|| snapshot_id.parse::<i64>().ok())
        .flatten()
        .map(|snapshot_id| (base_table, snapshot_id))
}

/// One external metadata result, pinned to the same connector control
/// generation used by later statistics and split preparation.
#[derive(Clone)]
pub(crate) struct QueryTableBinding {
    pub(crate) resolved: ResolvedAnalyzerTable,
    pub(crate) statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub(crate) planning_lease: Option<novarocks_spi::connector::ConnectorControlPlanningLease>,
}

impl QueryTableBinding {
    fn local(resolved: ResolvedAnalyzerTable) -> Self {
        Self {
            resolved,
            statistics_pin: None,
            planning_lease: None,
        }
    }
}

/// Application-owned external materializer.  It is intentionally small: SQL
/// can request a normalized table binding, but cannot name or downcast a
/// concrete connector provider.
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

/// Query-local memo of both successful and failed materializations.  Keeping
/// the lease in the value makes all subsequent consumers use the exact
/// metadata generation rather than acquiring `latest` again.
#[derive(Default)]
pub(crate) struct QueryTableBindingStore {
    entries: Mutex<HashMap<TableBindingKey, Result<Arc<QueryTableBinding>, String>>>,
}

impl QueryTableBindingStore {
    /// Canonical query-local snapshot used only to bind a prepared compiler
    /// artifact to the exact metadata facts it observed. Secrets and provider
    /// clients are excluded; keys are sorted before encoding.
    pub(crate) fn stable_digest_material(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let entries = self.entries.lock().expect("query table binding lock");
        let mut material = entries
            .iter()
            .map(|(key, value)| {
                let value = match value {
                    Ok(binding) => {
                        let mut digest = Sha256::new();
                        digest.update(b"ok\0");
                        if let Some(pin) = &binding.statistics_pin {
                            digest.update(pin.table.owner().as_str().as_bytes());
                            digest.update([0]);
                            digest.update(Sha256::digest(pin.table.payload().as_ref()));
                            digest.update(pin.data_version.as_bytes().as_ref());
                        }
                        if let Some(lease) = &binding.planning_lease {
                            digest.update(lease.binding().incarnation().to_bytes());
                            digest.update(
                                lease.binding().descriptor().provider_id.as_str().as_bytes(),
                            );
                            digest.update([0]);
                            digest.update(
                                lease.binding().descriptor().instance_id.as_str().as_bytes(),
                            );
                        }
                        format!("ok|{:x}", digest.finalize())
                    }
                    Err(error) => format!("err|{:x}", Sha256::digest(error.as_bytes())),
                };
                (format!("{key:?}"), value)
            })
            .collect::<Vec<_>>();
        material.sort_by(|left, right| left.0.cmp(&right.0));
        let mut bytes = Vec::new();
        for (key, value) in material {
            bytes.extend_from_slice(&(key.len() as u64).to_be_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes
    }

    fn resolve_or_insert(
        &self,
        key: TableBindingKey,
        load: impl FnOnce() -> Result<QueryTableBinding, String>,
    ) -> Result<Arc<QueryTableBinding>, String> {
        let mut entries = self.entries.lock().expect("query table binding lock");
        if let Some(entry) = entries.get(&key) {
            return entry.clone();
        }
        let entry = load().map(Arc::new);
        entries.insert(key, entry.clone());
        entry
    }

    pub(crate) fn strict_base_binding(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<Arc<QueryTableBinding>> {
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(&TableBindingKey::strict_base(catalog, namespace, table))
            .and_then(|entry| entry.clone().ok())
    }

    pub(crate) fn iceberg_data_file_binding(
        &self,
        table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
        binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding,
    ) -> Option<Arc<QueryTableBinding>> {
        let key = match binding {
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles => {
                table.current_snapshot_id.map(|snapshot_id| {
                    TableBindingKey::time_travel(
                        &table.catalog,
                        &table.namespace,
                        &table.table,
                        snapshot_id,
                    )
                })
            }
            _ => Some(TableBindingKey::strict_base(
                &table.catalog,
                &table.namespace,
                &table.table,
            )),
        }?;
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(&key)
            .and_then(|entry| entry.clone().ok())
    }

    #[cfg(test)]
    pub(crate) fn insert_strict_base_binding_for_test(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: QueryTableBinding,
    ) {
        self.entries
            .lock()
            .expect("query table binding lock")
            .insert(
                TableBindingKey::strict_base(catalog, namespace, table),
                Ok(Arc::new(binding)),
            );
    }
}

/// Transitional type name retained while application callers move from the
/// old mutable statistics-pin side channel to `QueryTableBindingStore`.
/// Unlike the old map, this is the immutable query binding authority.
pub(crate) type QueryStatisticsPins = Arc<QueryTableBindingStore>;

pub(crate) struct CatalogServiceProvider<'a> {
    current_catalog: Option<&'a str>,
    service: &'a CatalogService<TableDef, CatalogRuntimeMetadata>,
    bindings: QueryStatisticsPins,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
}

impl<'a> CatalogServiceProvider<'a> {
    pub(crate) fn with_query_table_bindings(
        current_catalog: Option<&'a str>,
        service: &'a CatalogService<TableDef, CatalogRuntimeMetadata>,
        _lookup_mode: TableLookupMode,
        bindings: QueryStatisticsPins,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self {
            current_catalog,
            service,
            bindings,
            loader,
        }
    }

    pub(crate) fn query_table_bindings(&self) -> QueryStatisticsPins {
        Arc::clone(&self.bindings)
    }

    /// Compatibility accessor for callers not yet moved to the explicit
    /// binding-store name.  It no longer exposes a mutable pin map.
    pub(crate) fn statistics_pins(&self) -> QueryStatisticsPins {
        self.query_table_bindings()
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
                // COW DML registers a query-local file input under the
                // internal catalog. The SQL surface name is synthetic, but
                // native scan preparation identifies the real Iceberg table.
                // Materialize the one connector lease under that physical
                // identity while preserving the synthetic TableDef used by
                // analysis; preparation can then consume the exact lease
                // without a current-generation reacquire.
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
                                TableBindingKey::strict_base(&catalog, &namespace, &table_name)
                            }
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles => {
                                let snapshot_id = iceberg.current_snapshot_id.ok_or_else(|| {
                                    format!(
                                        "explicit Iceberg input '{}.{}.{}' has no frozen snapshot identity",
                                        catalog, namespace, table_name
                                    )
                                })?;
                                TableBindingKey::time_travel(
                                    &catalog,
                                    &namespace,
                                    &table_name,
                                    snapshot_id,
                                )
                            }
                        };
                        return self
                            .bindings
                            .resolve_or_insert(key, || {
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
                            })
                            .map(|binding| binding.resolved.clone());
                    }
                }
                let key = TableBindingKey::analysis_lookup("default_catalog", database, table);
                self.bindings
                    .resolve_or_insert(key, || {
                        Ok(QueryTableBinding::local(
                            ResolvedAnalyzerTable::from_planner(
                                Some("default_catalog"),
                                database,
                                planner,
                            ),
                        ))
                    })
                    .map(|binding| binding.resolved.clone())
            }
            Some(catalog) => {
                let key = TableBindingKey::analysis_lookup(catalog, database, table);
                self.bindings
                    .resolve_or_insert(key, || {
                        self.loader.load_strict_base_table(catalog, database, table)
                    })
                    .map(|binding| binding.resolved.clone())
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

impl CatalogProvider for CatalogServiceProvider<'_> {
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

impl PlannerTableProvider for CatalogServiceProvider<'_> {
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

    fn query_table_bindings(&self) -> Option<QueryStatisticsPins> {
        Some(CatalogServiceProvider::query_table_bindings(self))
    }

    fn statistics_pins(&self) -> Option<QueryStatisticsPins> {
        Some(CatalogServiceProvider::statistics_pins(self))
    }
}

impl IcebergMetadataTableProvider for CatalogServiceProvider<'_> {
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
    fn sqlx1_resolution_error_is_memoized_by_canonical_identity() {
        let bindings = QueryTableBindingStore::default();
        let attempts = AtomicUsize::new(0);

        let first = bindings.resolve_or_insert(
            TableBindingKey::strict_base("ICEBERG", "DB", "TABLE"),
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err("missing table".to_string())
            },
        );
        let second = bindings.resolve_or_insert(
            TableBindingKey::strict_base("iceberg", "db", "table"),
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err("must not run".to_string())
            },
        );

        assert!(matches!(first, Err(error) if error == "missing table"));
        assert!(matches!(second, Err(error) if error == "missing table"));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sqlx1_resolution_time_travel_identity_uses_base_table_and_snapshot_selector() {
        let first = TableBindingKey::analysis_lookup("ice", "db", "__sqlx1_tt_orders_42");
        let same = TableBindingKey::time_travel("ICE", "DB", "orders", 42);
        let other_snapshot = TableBindingKey::analysis_lookup("ice", "db", "__sqlx1_tt_orders_43");

        assert_eq!(first, same);
        assert_ne!(first, other_snapshot);
    }

    #[test]
    fn sqlx1_resolution_source_has_no_concrete_provider_dependency() {
        let source = include_str!("provider.rs");
        assert!(source.contains("trait QueryTableBindingLoader"));
        let concrete_provider_path = ["connector::iceberg", "::provider::"].concat();
        assert!(!source.contains(&concrete_provider_path));
    }
}
