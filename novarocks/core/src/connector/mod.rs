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
pub(crate) mod file_execution;
pub mod hdfs;
pub(crate) mod host;
pub mod iceberg;
pub mod jdbc;
pub(crate) mod runtime;
pub(crate) mod scan_model;
pub mod schema;
#[cfg(feature = "compat")]
pub mod starrocks;
pub(crate) mod stats;

pub(crate) use backend::{CatalogBackend, MvBackend, TableSink};
#[cfg(test)]
pub(crate) use iceberg::catalog::load_table as load_iceberg_table;
pub(crate) use iceberg::catalog::{
    IcebergCatalogRegistry, namespace_exists as iceberg_namespace_exists,
};
#[cfg(test)]
pub(crate) use iceberg::changes::plan_changes as plan_iceberg_changes;
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorInstance, ConnectorInstanceDeclaration, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorInstanceInstaller, ConnectorProviderId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
};

use self::host::{ConnectorHost, ConnectorHostError, ConnectorInstanceLease};

struct RequestConnectorCancellation {
    signal: Arc<AtomicBool>,
}

impl ConnectorCancellation for RequestConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.signal.load(Ordering::SeqCst)
    }
}

struct QueryConnectorCancellation {
    cancellation: crate::query_execution::cancellation::QueryCancellationView,
}

impl ConnectorCancellation for QueryConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

fn build_connector_request_context(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<ConnectorRequestContext, String> {
    let (_, query_expire) = crate::runtime::query_options::query_expire_durations(query_options);
    ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        cancellation,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn connector_request_context(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    cancellation_signal: Arc<AtomicBool>,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(RequestConnectorCancellation {
            signal: cancellation_signal,
        }),
    )
}

pub(crate) fn connector_request_context_for_query(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
    cancellation: crate::query_execution::cancellation::QueryCancellationView,
) -> Result<ConnectorRequestContext, String> {
    build_connector_request_context(
        query_options,
        Arc::new(QueryConnectorCancellation { cancellation }),
    )
}

pub(crate) fn validate_request_context(context: &ConnectorRequestContext) -> Result<(), String> {
    if context.cancellation().is_cancelled() {
        return Err("connector request was cancelled".to_string());
    }
    if Instant::now() >= context.deadline() {
        return Err("connector request deadline elapsed".to_string());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_request_context() -> ConnectorRequestContext {
    connector_request_context(None, Arc::new(AtomicBool::new(false)))
        .expect("test connector request context")
}

fn metadata_instance(
    connectors: &ConnectorRegistry,
    catalog: &str,
) -> Result<Arc<ConnectorInstance>, String> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    connectors
        .connector_instance(&instance_id)
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_namespace_exists(
    connectors: &ConnectorRegistry,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
) -> Result<bool, String> {
    let instance = metadata_instance(connectors, catalog)?;
    let instance_id = instance.descriptor().instance_id.clone();
    instance
        .metadata()
        .ok_or_else(|| format!("connector instance {catalog} has no metadata capability"))?
        .namespace_exists(novarocks_spi::connector::ConnectorNamespaceRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from(namespace),
            },
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_table_exists(
    connectors: &ConnectorRegistry,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<bool, String> {
    let instance = metadata_instance(connectors, catalog)?;
    let instance_id = instance.descriptor().instance_id.clone();
    instance
        .metadata()
        .ok_or_else(|| format!("connector instance {catalog} has no metadata capability"))?
        .table_exists(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .map_err(|error| error.to_string())
}

pub(crate) fn metadata_load_table(
    connectors: &ConnectorRegistry,
    context: ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(backend::ResolvedTable, Option<i32>), String> {
    let instance = metadata_instance(connectors, catalog)?;
    let instance_id = instance.descriptor().instance_id.clone();
    let metadata = instance
        .metadata()
        .ok_or_else(|| format!("connector instance {catalog} has no metadata capability"))?
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        .map_err(|error| error.to_string())?;
    let columns = metadata
        .schema
        .fields()
        .iter()
        .map(|field| novarocks_catalog::schema::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect();
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    Ok((
        backend::ResolvedTable {
            catalog: metadata.identity.instance_id.as_str().to_string(),
            namespace: metadata.identity.namespace.to_string(),
            table: metadata.identity.table.to_string(),
            columns,
        },
        schema_id,
    ))
}

pub use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};

pub use crate::connector::file_execution::FileScanRange;
pub use crate::formats::FileFormatConfig;
pub use crate::formats::orc::OrcScanConfig;
pub use crate::formats::parquet::ParquetScanConfig;
pub use hdfs::{HdfsIcebergRuntimePruningConfig, HdfsScanConfig};
pub use jdbc::JdbcScanConfig;
#[cfg(feature = "compat")]
pub use starrocks::{LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanRange};

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
            let (bucket, _) = novarocks_fs::parse_object_store_path_parse_only(&warehouse_uri)
                .map_err(|error| error.to_string())?;
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

#[derive(Clone)]
pub struct ConnectorRegistry {
    connector_host: Arc<RwLock<ConnectorHost>>,
    catalog_backends: HashMap<&'static str, Arc<dyn CatalogBackend>>,
    table_sinks: HashMap<&'static str, Arc<dyn TableSink>>,
    mv_backends: HashMap<&'static str, Arc<dyn MvBackend>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            connector_host: Arc::new(RwLock::new(ConnectorHost::default())),
            catalog_backends: HashMap::new(),
            table_sinks: HashMap::new(),
            mv_backends: HashMap::new(),
        }
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

    pub(crate) fn register_connector_instance_installer(
        &self,
        installer: Arc<dyn ConnectorInstanceInstaller>,
    ) -> Result<(), ConnectorHostError> {
        self.connector_host
            .write()
            .map_err(|_| ConnectorHostError::unavailable("connector host write lock poisoned"))?
            .register_installer(installer)
    }

    pub(crate) fn install_connector_instance(
        &self,
        declaration: &ConnectorInstanceDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.connector_host
            .write()
            .map_err(|_| ConnectorHostError::unavailable("connector host write lock poisoned"))?
            .install(declaration, context)
    }

    pub(crate) fn retire_connector_instance(
        &self,
        instance_id: &ConnectorInstanceId,
        incarnation: ConnectorInstanceIncarnation,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.connector_host
            .write()
            .map_err(|_| ConnectorHostError::unavailable("connector host write lock poisoned"))?
            .retire(instance_id, incarnation)
    }

    /// Installs a startup-bound read-only instance received by the native
    /// control plane. This is the only composition-facing installation API;
    /// fragment decoding only resolves instances already present in this host.
    pub fn install_distributed_instance(
        &self,
        declaration: &ConnectorInstanceDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        self.install_connector_instance(declaration, context)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// Marks one distributed instance generation as retiring. Existing reader
    /// Arcs may drain, while subsequent fragment resolution is rejected.
    pub fn retire_distributed_instance(
        &self,
        instance_id: &ConnectorInstanceId,
        incarnation: ConnectorInstanceIncarnation,
    ) -> Result<(), String> {
        self.retire_connector_instance(instance_id, incarnation)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn materialize_transport_connector_instance(
        &self,
        provider_id: &ConnectorProviderId,
        instance_id: ConnectorInstanceId,
        scan_payload: bytes::Bytes,
        file_ranges: &[crate::connector::file_execution::FileScanRange],
        output_schema: crate::exec::chunk::ChunkSchemaRef,
    ) -> Result<ConnectorInstance, ConnectorHostError> {
        self.connector_host
            .read()
            .map_err(|_| ConnectorHostError::unavailable("connector host read lock poisoned"))?
            .materialize_transport_instance(
                provider_id,
                instance_id,
                scan_payload,
                file_ranges,
                output_schema,
            )
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
}

/// Registers the startup-bound installers that a BE process may use through
/// the connector binding control plane.  Callers supply the already parsed
/// local configuration; declarations only select the fixed `default` binding
/// and cannot replace its credentials or endpoint.
pub fn compose_backend_connector_installers(
    registry: &ConnectorRegistry,
    default_object_store: Option<novarocks_fs::ObjectStoreConfig>,
) -> Result<(), String> {
    let binding = iceberg::provider::IcebergReadBinding::default_binding(default_object_store);
    registry
        .register_connector_instance_installer(Arc::new(
            iceberg::provider::IcebergConnectorInstaller::new(binding.clone()),
        ))
        .map_err(|error| error.to_string())?;
    registry
        .register_connector_instance(
            iceberg::provider::compose_compat_read_instance(binding)
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())
}

pub(crate) fn register_standalone_backends(state: &Arc<crate::engine::StandaloneState>) {
    let iceberg_catalogs = Arc::clone(&state.iceberg_catalogs);
    {
        let mut connectors = state
            .connectors
            .write()
            .expect("standalone connector registry write lock");
        #[cfg(feature = "compat")]
        connectors
            .register_connector_instance(
                starrocks::table::provider::connector_instance(state)
                    .expect("create standalone StarRocks connector instance"),
            )
            .expect("register standalone StarRocks connector instance");
        connectors.register_catalog_backend(Arc::new(
            iceberg::catalog::IcebergCatalogBackend::new(Arc::clone(&iceberg_catalogs)),
        ));
        connectors.register_table_sink(Arc::new(iceberg::catalog::IcebergTableSink::new(
            Arc::clone(&iceberg_catalogs),
        )));
        connectors.register_mv_backend(Arc::new(
            crate::engine::mv::iceberg_backend::IcebergMvBackend::new(state),
        ));
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        let reg = ConnectorRegistry::new();
        reg.connector_host
            .write()
            .expect("connector host write lock poisoned")
            .register_transport_factory(Arc::new(hdfs::HdfsNativeTransportFactory::new()))
            .expect("register native HDFS transport factory");
        reg
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut catalog_backends: Vec<_> = self.catalog_backends.keys().copied().collect();
        catalog_backends.sort();
        let mut table_sinks: Vec<_> = self.table_sinks.keys().copied().collect();
        table_sinks.sort();
        let mut mv_backends: Vec<_> = self.mv_backends.keys().copied().collect();
        mv_backends.sort();
        f.debug_struct("ConnectorRegistry")
            .field("catalog_backends", &catalog_backends)
            .field("table_sinks", &table_sinks)
            .field("mv_backends", &mv_backends)
            .finish()
    }
}
