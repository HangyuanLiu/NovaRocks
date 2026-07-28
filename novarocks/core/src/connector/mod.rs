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
pub(crate) mod backend;
pub mod hdfs;
pub(crate) mod host;
pub mod iceberg;
pub mod jdbc;
pub(crate) mod runtime;
pub(crate) mod scan_model;
pub(crate) mod scan_planning;
pub mod schema;
#[cfg(feature = "compat")]
pub mod starrocks;
pub(crate) mod stats;

pub(crate) use backend::{CatalogBackend, MvBackend, TableSink, TableSource};
#[cfg(test)]
pub(crate) use iceberg::catalog::load_table as load_iceberg_table;
pub(crate) use iceberg::catalog::{
    IcebergCatalogRegistry, namespace_exists as iceberg_namespace_exists,
};
#[cfg(test)]
pub(crate) use iceberg::changes::plan_changes as plan_iceberg_changes;
#[cfg(not(test))]
pub(crate) use iceberg::compact::spawn_optimize_worker as spawn_iceberg_optimize_worker;
#[cfg(feature = "compat")]
pub(crate) use starrocks::table::{
    StarRocksTableCatalog, StarRocksTableConfig, register_starrocks_tables_in_catalog,
    runtime_registered,
};
#[cfg(not(feature = "compat"))]
pub(crate) use starrocks_table_stub::{
    StarRocksTableCatalog, StarRocksTableConfig, register_starrocks_tables_in_catalog,
    runtime_registered,
};

use scan_planning::ConnectorScanPlanner;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use novarocks_spi::connector::{ConnectorInstance, ConnectorInstanceId};

use self::host::{ConnectorHost, ConnectorHostError, ConnectorInstanceLease};

pub use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
use crate::exec::node::scan::{BoundScanRanges, ScanSource};

pub use crate::formats::FileFormatConfig;
pub use crate::formats::orc::OrcScanConfig;
pub use crate::formats::parquet::ParquetScanConfig;
pub use crate::fs::scan_context::FileScanRange;
pub use hdfs::{HdfsIcebergRuntimePruningConfig, HdfsScanConfig};
pub use iceberg::IcebergMetadataScanConfig;
pub use jdbc::JdbcScanConfig;
#[cfg(feature = "compat")]
pub use starrocks::{LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanOp, StarRocksScanRange};

#[cfg(test)]
mod backend_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod iceberg_provider_test;
#[cfg(test)]
mod runtime_test;

#[cfg(not(feature = "compat"))]
mod starrocks_table_stub {
    use crate::common::app_config::StandaloneStarRocksTableConfig as AppStarRocksTableConfig;
    use crate::meta::repository::starrocks_table::{
        StarRocksTableSnapshot, StoredStarRocksPartition, StoredStarRocksTable,
        StoredStarRocksTablet,
    };
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::sql::catalog::local::PlannerMemoryCatalog;

    #[derive(Clone, Debug)]
    pub(crate) struct StarRocksTableConfig {
        pub(crate) warehouse_uri: String,
        pub(crate) s3: S3StoreConfig,
        pub(crate) mv_default_storage_engine: String,
    }

    impl StarRocksTableConfig {
        pub(crate) fn from_app_config(config: AppStarRocksTableConfig) -> Result<Self, String> {
            let warehouse_uri = config
                .warehouse_uri
                .trim()
                .trim_end_matches('/')
                .to_string();
            if warehouse_uri.is_empty() {
                return Err("standalone StarRocks table warehouse_uri is empty".to_string());
            }
            let (bucket, _) =
                crate::fs::access::parse_object_store_path_parse_only(&warehouse_uri)?;
            let mv_default_storage_engine = config
                .mv_default_storage_engine
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("iceberg")
                .to_string();
            if mv_default_storage_engine != "iceberg" {
                return Err(format!(
                    "invalid mv_default_storage_engine `{mv_default_storage_engine}`; allowed: iceberg"
                ));
            }
            Ok(Self {
                warehouse_uri,
                s3: S3StoreConfig {
                    endpoint: config.endpoint.trim().to_string(),
                    bucket,
                    access_key_id: config.access_key_id.trim().to_string(),
                    access_key_secret: config.access_key_secret.trim().to_string(),
                    region: config.region.as_ref().map(|value| value.trim().to_string()),
                    enable_path_style_access: config.enable_path_style_access,
                },
                mv_default_storage_engine,
            })
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct StarRocksTableRuntime {
        pub(crate) database_name: String,
        pub(crate) table: StoredStarRocksTable,
        pub(crate) partitions: Vec<StoredStarRocksPartition>,
        pub(crate) tablets: Vec<StoredStarRocksTablet>,
    }

    #[derive(Clone, Debug, Default)]
    pub(crate) struct StarRocksTableCatalog {
        pub(crate) config: Option<StarRocksTableConfig>,
        pub(crate) snapshot: StarRocksTableSnapshot,
        runtimes: Vec<StarRocksTableRuntime>,
    }

    impl StarRocksTableCatalog {
        pub(crate) fn empty(config: Option<StarRocksTableConfig>) -> Self {
            Self {
                config,
                snapshot: StarRocksTableSnapshot::default(),
                runtimes: Vec::new(),
            }
        }

        pub(crate) fn rebuild_from_repository(
            config: Option<StarRocksTableConfig>,
            snapshot: StarRocksTableSnapshot,
        ) -> Result<Self, String> {
            Ok(Self {
                config,
                snapshot,
                runtimes: Vec::new(),
            })
        }

        pub(crate) fn table(
            &self,
            database_name: &str,
            table_name: &str,
        ) -> Result<&StarRocksTableRuntime, String> {
            let _ = (database_name, table_name);
            Err("standalone StarRocks tables require the compat feature".to_string())
        }

        pub(crate) fn runtime_by_table_id(&self, table_id: i64) -> Option<&StarRocksTableRuntime> {
            let _ = table_id;
            None
        }

        pub(crate) fn list_tables_in_database(
            &self,
            database_name: &str,
        ) -> Result<Vec<String>, String> {
            Ok(self
                .runtimes
                .iter()
                .filter(|runtime| runtime.database_name == database_name)
                .map(|runtime| runtime.table.name.clone())
                .collect())
        }
    }

    pub(crate) fn runtime_registered(tablet_id: i64) -> bool {
        let _ = tablet_id;
        false
    }

    pub(crate) fn register_starrocks_tables_in_catalog(
        catalog: &mut PlannerMemoryCatalog,
        starrocks: &StarRocksTableCatalog,
    ) -> Result<(), String> {
        let _ = (catalog, starrocks);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[test]
    fn standalone_catalog_service_keeps_internal_entry_after_backend_registration() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        super::register_standalone_backends(&state);

        let registry = state
            .catalog_service
            .registry()
            .read()
            .expect("catalog service registry");
        assert!(registry.get_catalog("default_catalog").is_ok());
    }
}

#[cfg(test)]
mod scan_planning_registry_tests {
    use std::sync::Arc;

    use super::ConnectorRegistry;
    use super::scan_planning::{
        BeginScanContext, ConnectorScanPlanner, ScanHandle, Split, SplitPlanningContext,
        TableHandle,
    };

    #[derive(Debug)]
    struct NoopPlanner;

    impl ConnectorScanPlanner for NoopPlanner {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn begin_scan(
            &self,
            _table: TableHandle,
            _ctx: BeginScanContext,
        ) -> Result<ScanHandle, String> {
            Err("not used".to_string())
        }

        fn plan_splits(
            &self,
            _scan: &ScanHandle,
            _ctx: SplitPlanningContext,
        ) -> Result<Vec<Split>, String> {
            Err("not used".to_string())
        }
    }

    #[test]
    fn connector_registry_returns_registered_scan_planner() {
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_planner(Arc::new(NoopPlanner));

        let planner = registry.scan_planner("noop").expect("registered planner");

        assert_eq!(planner.name(), "noop");
    }

    #[test]
    fn connector_registry_reports_unknown_scan_planner() {
        let registry = ConnectorRegistry::new();

        let err = registry
            .scan_planner("missing")
            .expect_err("unknown planner should fail");

        assert_eq!(err, "unknown scan planner: missing");
    }

    #[test]
    fn default_registry_does_not_register_standalone_scan_planners() {
        let registry = ConnectorRegistry::default();

        let err = registry
            .scan_planner("starrocks")
            .expect_err("standalone planners are registered with state, not Default");

        assert_eq!(err, "unknown scan planner: starrocks");
    }

    #[test]
    fn default_registry_does_not_register_standalone_iceberg_scan_planner() {
        let registry = ConnectorRegistry::default();

        let err = registry
            .scan_planner("iceberg")
            .expect_err("standalone planners are registered with state, not Default");

        assert_eq!(err, "unknown scan planner: iceberg");
    }

    #[test]
    fn default_connectors_register_stateful_iceberg_scan_planner() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        super::register_standalone_backends(&state);
        let connectors = state
            .connectors
            .read()
            .expect("connector registry read lock");
        let planner = connectors.scan_planner("iceberg").expect("iceberg planner");
        let handle =
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
                "missing_catalog",
                "db",
                "t",
                crate::connector::iceberg::scan_model::IcebergTableInfo {
                    catalog: "missing_catalog".to_string(),
                    namespace: "db".to_string(),
                    table: "t".to_string(),
                    table_uuid: None,
                    current_snapshot_id: None,
                    schema_id: 0,
                    location: "s3://bucket/t".to_string(),
                    schema: crate::connector::iceberg::scan_model::IcebergSchemaDef { fields: vec![] },
                    serialized_metadata: None,
                    serialized_metadata_rows: None,
                },
                vec!["id".to_string()],
            );
        let scan = planner
            .begin_scan(
                handle,
                crate::connector::scan_planning::BeginScanContext::default(),
            )
            .expect("begin scan");
        let err = planner
            .plan_splits(
                &scan,
                crate::connector::scan_planning::SplitPlanningContext::default(),
            )
            .expect_err("stateful planner should consult registry");
        assert!(err.contains("unknown catalog"), "{err}");
    }
}

#[derive(Clone, Debug)]
pub enum ScanConfig {
    Jdbc(JdbcScanConfig),
    Hdfs(Box<HdfsScanConfig>),
    IcebergMetadata(IcebergMetadataScanConfig),
    #[cfg(feature = "compat")]
    StarRocks(Box<StarRocksScanConfig>),
}

pub trait ScanConnector: Send + Sync {
    fn name(&self) -> &'static str;
    /// Split a decoder-built `ScanConfig` into a static `ScanSource` (for the
    /// plan node) plus the enriched `BoundScanRanges` (for the instance's scan
    /// assignment). Binding is deferred to execution time
    /// (`materialize_scan_bindings`); this no longer eagerly builds a `ScanOp`.
    fn create_scan_node(
        &self,
        cfg: ScanConfig,
    ) -> Result<(Arc<dyn ScanSource>, BoundScanRanges), String>;
}

#[derive(Clone)]
pub struct ConnectorRegistry {
    connector_host: Arc<RwLock<ConnectorHost>>,
    scan_connectors: HashMap<&'static str, Arc<dyn ScanConnector>>,
    catalog_backends: HashMap<&'static str, Arc<dyn CatalogBackend>>,
    table_sources: HashMap<&'static str, Arc<dyn TableSource>>,
    table_sinks: HashMap<&'static str, Arc<dyn TableSink>>,
    mv_backends: HashMap<&'static str, Arc<dyn MvBackend>>,
    scan_planners: HashMap<&'static str, Arc<dyn ConnectorScanPlanner>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            connector_host: Arc::new(RwLock::new(ConnectorHost::default())),
            scan_connectors: HashMap::new(),
            catalog_backends: HashMap::new(),
            table_sources: HashMap::new(),
            table_sinks: HashMap::new(),
            mv_backends: HashMap::new(),
            scan_planners: HashMap::new(),
        }
    }

    pub fn register_scan_connector(&mut self, connector: Arc<dyn ScanConnector>) {
        self.scan_connectors.insert(connector.name(), connector);
    }

    pub(crate) fn register_connector_instance(
        &self,
        instance: ConnectorInstance,
    ) -> Result<(), ConnectorHostError> {
        self.connector_host
            .write()
            .map_err(|_| ConnectorHostError::unavailable("connector host write lock poisoned"))?
            .register(instance)
    }

    pub(crate) fn unregister_connector_instance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.connector_host
            .write()
            .map_err(|_| ConnectorHostError::unavailable("connector host write lock poisoned"))?
            .unregister(instance_id)
    }

    pub(crate) fn connector_instance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.connector_host
            .read()
            .map_err(|_| ConnectorHostError::unavailable("connector host read lock poisoned"))?
            .resolve(instance_id)
    }

    /// Register a query-local instance and return a lease that removes it once
    /// the physical scan no longer retains it.
    pub(crate) fn register_ephemeral_connector_instance(
        &self,
        instance: ConnectorInstance,
    ) -> Result<(Arc<ConnectorInstance>, Arc<ConnectorInstanceLease>), ConnectorHostError> {
        let instance_id = instance.descriptor().instance_id.clone();
        self.register_connector_instance(instance)?;
        let lease = Arc::new(ConnectorInstanceLease::new(
            Arc::clone(&self.connector_host),
            instance_id.clone(),
        ));
        match self.connector_instance(&instance_id) {
            Ok(instance) => Ok((instance, lease)),
            Err(error) => {
                drop(lease);
                Err(error)
            }
        }
    }

    pub(crate) fn register_catalog_backend(&mut self, backend: Arc<dyn CatalogBackend>) {
        self.catalog_backends.insert(backend.name(), backend);
    }

    pub(crate) fn catalog_backend(&self, name: &str) -> Result<Arc<dyn CatalogBackend>, String> {
        self.catalog_backends
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown catalog backend: {name}"))
    }

    pub(crate) fn register_table_source(&mut self, source: Arc<dyn TableSource>) {
        self.table_sources.insert(source.name(), source);
    }

    pub(crate) fn table_source(&self, name: &str) -> Result<Arc<dyn TableSource>, String> {
        self.table_sources
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown table source: {name}"))
    }

    pub(crate) fn register_table_sink(&mut self, sink: Arc<dyn TableSink>) {
        self.table_sinks.insert(sink.name(), sink);
    }

    pub(crate) fn table_sink(&self, name: &str) -> Result<Arc<dyn TableSink>, String> {
        self.table_sinks
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown table sink: {name}"))
    }

    pub(crate) fn register_mv_backend(&mut self, backend: Arc<dyn MvBackend>) {
        self.mv_backends.insert(backend.name(), backend);
    }

    pub(crate) fn mv_backend(&self, name: &str) -> Result<Arc<dyn MvBackend>, String> {
        self.mv_backends
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown MV backend: {name}"))
    }

    pub(crate) fn mv_backends(&self) -> Vec<Arc<dyn MvBackend>> {
        let mut entries: Vec<_> = self.mv_backends.iter().collect();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        entries
            .into_iter()
            .map(|(_, backend)| Arc::clone(backend))
            .collect()
    }

    pub(crate) fn register_scan_planner(&mut self, planner: Arc<dyn ConnectorScanPlanner>) {
        self.scan_planners.insert(planner.name(), planner);
    }

    pub(crate) fn scan_planner(&self, name: &str) -> Result<Arc<dyn ConnectorScanPlanner>, String> {
        self.scan_planners
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown scan planner: {name}"))
    }

    /// Resolve the connector and split its `ScanConfig` into a static
    /// `ScanSource` (stored on the plan node) plus the enriched
    /// `BoundScanRanges` (routed into the instance's scan assignment). The
    /// per-instance `ScanOp` is materialized later by `materialize_scan_bindings`.
    pub fn create_scan_node(
        &self,
        connector_name: &str,
        cfg: ScanConfig,
    ) -> Result<(Arc<dyn ScanSource>, BoundScanRanges), String> {
        let Some(connector) = self.scan_connectors.get(connector_name) else {
            return Err(format!("unknown scan connector: {connector_name}"));
        };
        connector.create_scan_node(cfg)
    }
}

pub(crate) fn register_standalone_backends(state: &Arc<crate::engine::StandaloneState>) {
    let iceberg_catalogs = Arc::clone(&state.iceberg_catalogs);
    {
        let mut connectors = state
            .connectors
            .write()
            .expect("standalone connector registry write lock");
        connectors.register_catalog_backend(Arc::new(
            iceberg::catalog::IcebergCatalogBackend::new(Arc::clone(&iceberg_catalogs)),
        ));
        connectors.register_table_source(Arc::new(iceberg::catalog::IcebergTableSource::new(
            Arc::clone(&iceberg_catalogs),
        )));
        connectors.register_table_sink(Arc::new(iceberg::catalog::IcebergTableSink::new(
            Arc::clone(&iceberg_catalogs),
        )));
        connectors.register_scan_planner(Arc::new(
            iceberg::IcebergConnectorScanPlanner::with_catalog_registry(Arc::clone(
                &iceberg_catalogs,
            )),
        ));
        connectors.register_mv_backend(Arc::new(
            crate::engine::mv::iceberg_backend::IcebergMvBackend::new(state),
        ));
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        let mut reg = ConnectorRegistry::new();
        let jdbc = Arc::new(JdbcConnector { name: "jdbc" });
        let mysql = Arc::new(JdbcConnector { name: "mysql" });
        let hdfs = Arc::new(HdfsConnector { name: "hdfs" });
        let iceberg = Arc::new(IcebergConnector { name: "iceberg" });
        reg.register_scan_connector(jdbc);
        reg.register_scan_connector(mysql);
        reg.register_scan_connector(hdfs);
        reg.register_scan_connector(iceberg);
        #[cfg(feature = "compat")]
        let starrocks = Arc::new(StarRocksConnector { name: "starrocks" });
        #[cfg(feature = "compat")]
        reg.register_scan_connector(starrocks);
        reg
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut scan_connectors: Vec<_> = self.scan_connectors.keys().copied().collect();
        scan_connectors.sort();
        let mut catalog_backends: Vec<_> = self.catalog_backends.keys().copied().collect();
        catalog_backends.sort();
        let mut table_sources: Vec<_> = self.table_sources.keys().copied().collect();
        table_sources.sort();
        let mut table_sinks: Vec<_> = self.table_sinks.keys().copied().collect();
        table_sinks.sort();
        let mut mv_backends: Vec<_> = self.mv_backends.keys().copied().collect();
        mv_backends.sort();
        let mut scan_planners: Vec<_> = self.scan_planners.keys().copied().collect();
        scan_planners.sort();
        f.debug_struct("ConnectorRegistry")
            .field("scan_connectors", &scan_connectors)
            .field("catalog_backends", &catalog_backends)
            .field("table_sources", &table_sources)
            .field("table_sinks", &table_sinks)
            .field("mv_backends", &mv_backends)
            .field("scan_planners", &scan_planners)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct JdbcConnector {
    name: &'static str,
}

impl ScanConnector for JdbcConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(
        &self,
        cfg: ScanConfig,
    ) -> Result<(Arc<dyn ScanSource>, BoundScanRanges), String> {
        match cfg {
            ScanConfig::Jdbc(cfg) => {
                let source: Arc<dyn ScanSource> = Arc::new(jdbc::JdbcScanSource::new(cfg));
                Ok((source, BoundScanRanges::None))
            }
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct HdfsConnector {
    name: &'static str,
}

impl ScanConnector for HdfsConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(
        &self,
        cfg: ScanConfig,
    ) -> Result<(Arc<dyn ScanSource>, BoundScanRanges), String> {
        match cfg {
            ScanConfig::Hdfs(cfg) => {
                // Split the decoder-built config into a static source plus its
                // file ranges. The ranges travel to the instance assignment;
                // `bind` happens at materialize time.
                let HdfsScanConfig {
                    ranges,
                    // `original_range_count` is recomputed from `ranges` in
                    // `bind`; it equals `ranges.len()` at every decode site.
                    original_range_count: _,
                    has_more,
                    limit,
                    profile_label,
                    format,
                    object_store_config,
                    iceberg_table_locations,
                    query_global_dicts,
                    iceberg_runtime_pruning,
                } = *cfg;
                let source: Arc<dyn ScanSource> = Arc::new(hdfs::HdfsScanSource::new(
                    limit,
                    profile_label,
                    format,
                    object_store_config,
                    iceberg_table_locations,
                    query_global_dicts,
                    iceberg_runtime_pruning,
                ));
                Ok((source, BoundScanRanges::File { ranges, has_more }))
            }
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct IcebergConnector {
    name: &'static str,
}

impl ScanConnector for IcebergConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(
        &self,
        cfg: ScanConfig,
    ) -> Result<(Arc<dyn ScanSource>, BoundScanRanges), String> {
        match cfg {
            ScanConfig::IcebergMetadata(cfg) => {
                // Split the decoder-built config into a static source plus its
                // metadata split ranges. `bind` happens at materialize time.
                let IcebergMetadataScanConfig {
                    metadata_table_type,
                    serialized_table,
                    serialized_predicate,
                    load_column_stats,
                    ranges,
                    batch_size,
                    output_columns,
                    profile_label,
                } = cfg;
                let source: Arc<dyn ScanSource> =
                    Arc::new(iceberg::metadata::IcebergMetadataScanSource::new(
                        metadata_table_type,
                        serialized_table,
                        serialized_predicate,
                        load_column_stats,
                        batch_size,
                        output_columns,
                        profile_label,
                    ));
                Ok((source, BoundScanRanges::IcebergMetadata { ranges }))
            }
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "compat")]
struct StarRocksConnector {
    name: &'static str,
}

#[cfg(feature = "compat")]
impl ScanConnector for StarRocksConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(
        &self,
        cfg: ScanConfig,
    ) -> Result<(Arc<dyn ScanSource>, BoundScanRanges), String> {
        match cfg {
            ScanConfig::StarRocks(cfg) => {
                // Split the decoder-built config into a static source plus its
                // tablet ranges. `deferred_lake_resolution` is carried through
                // as-is; `bind` (at materialize time) never re-derives tablets.
                let StarRocksScanConfig {
                    db_name,
                    table_name,
                    properties,
                    ranges,
                    has_more,
                    required_chunk_schema,
                    output_chunk_schema,
                    query_global_dicts,
                    limit,
                    batch_size,
                    query_timeout,
                    mem_limit,
                    profile_label,
                    min_max_predicates,
                    lake_schema_meta,
                    deferred_lake_resolution,
                    topn_filter_column_map,
                } = *cfg;
                let source: Arc<dyn ScanSource> = Arc::new(starrocks::StarRocksScanSource {
                    db_name,
                    table_name,
                    properties,
                    required_chunk_schema,
                    output_chunk_schema,
                    query_global_dicts,
                    limit,
                    batch_size,
                    query_timeout,
                    mem_limit,
                    profile_label,
                    min_max_predicates,
                    lake_schema_meta,
                    deferred_lake_resolution,
                    topn_filter_column_map,
                });
                Ok((
                    source,
                    BoundScanRanges::StarRocksTablet { ranges, has_more },
                ))
            }
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}
