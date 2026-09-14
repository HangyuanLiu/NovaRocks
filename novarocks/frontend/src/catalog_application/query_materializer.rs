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
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::ConnectorControlResolver;

use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::catalog_application::query_catalog::{
    CatalogResolutionError, CatalogResolutionResult, ConnectorQueryTableMaterialization,
    QueryCatalogService, load_connector_table_alias_materialization_with_lease_typed,
    load_connector_table_materialization_with_lease_typed,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};

/// Project a provider-neutral SPI metadata materialization into the
/// request-local SQL binding. Provider aliases retain their separately frozen
/// provider-owned facts until their dedicated adapters run.
pub fn connector_query_binding_from_materialization(
    materialization: ConnectorQueryTableMaterialization,
    catalog: &str,
    namespace: &str,
    sql_table_name: &str,
    binding: SqlTableBindingId,
) -> Result<QueryTableBinding, String> {
    let sql_materialization = novarocks_sql::planning::catalog::materialize_connector_read_table(
        novarocks_sql::planning::catalog::ConnectorReadTableFacts {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: sql_table_name.to_string(),
            columns: materialization.columns,
            iceberg_row_lineage_metadata_columns: materialization.row_lineage_metadata_columns,
            schema: materialization.read_schema.clone(),
            binding,
            selector: materialization.read_selector,
            planning_facts: materialization.sql_planning_facts,
        },
    )?;
    let frozen_snapshot_materializations = sql_materialization
        .frozen_snapshot_id()
        .into_iter()
        .map(|snapshot_id| {
            (
                snapshot_id,
                QueryScanMaterialization {
                    table: materialization.read_table.clone(),
                    catalog_handle: materialization.catalog_handle.clone(),
                    schema: materialization.read_schema.clone(),
                    selector: novarocks_spi::connector::ConnectorReadSelector::SnapshotId(
                        snapshot_id,
                    ),
                    statistics_pin: materialization.statistics_pin.clone(),
                    planning_lease: materialization.planning_lease.clone(),
                },
            )
        })
        .collect();
    Ok(QueryTableBinding {
        resolved: sql_materialization.into_resolved_table(),
        statistics_pin: materialization.statistics_pin.clone(),
        admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
        scan_materialization: Some(QueryScanMaterialization {
            table: materialization.read_table,
            catalog_handle: materialization.catalog_handle,
            schema: materialization.read_schema,
            selector: materialization.read_selector,
            statistics_pin: materialization.statistics_pin,
            planning_lease: materialization.planning_lease,
        }),
        mv_target_read: None,
        write_target_admission: None,
        frozen_snapshot_materializations,
        admitted_change_scans: BTreeMap::new(),
    })
}

/// Admit one provider-owned change window while the caller holds the exact
/// table handle and planning lease. The returned sealed scan is the sole
/// physical authority retained by Core for later preparation.
/// Application materializer for connector-controlled table metadata.  The
/// interface is intentionally application-owned because it returns an exact
/// lease alongside planner facts.  It is not part of SQL's vocabulary.
pub trait QueryTableBindingLoader: Send + Sync {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding>;

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding>;
}

/// Application-owned catalog facade.  Its binding store is request-local and
/// retained by the caller as post-compile context; the SQL catalog trait does
/// not expose it.
pub struct CatalogServiceMaterializer<'a> {
    current_catalog: Option<&'a str>,
    service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
    /// Typed classification paired with binding-store failure memoization.
    /// The binding store owns the canonical at-most-once load; this sidecar
    /// retains only whether its string-compatible failure was absence or a
    /// hard catalog failure.
    resolution_errors: Mutex<HashMap<QueryTableBindingKey, CatalogResolutionError>>,
    /// Frontend-owned attachment admission. The loader still owns the exact
    /// connector lease; this gate preserves Absent versus Unavailable before
    /// Core can materialize an external table.
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
    /// Request-scoped synthetic relations used by application rewrite flows.
    /// They are intentionally kept next to the binding store instead of the
    /// shared memory catalog: SQL can only observe their projected tokenized
    /// scan after this materializer has admitted the exact connector lease.
    query_local_overlays: HashMap<(String, String), QueryLocalTableOverlay>,
}

/// One application-owned relation overlay for a generated query.
///
/// The overlay is a binding factory, not a planner table definition: generated COW and MV
/// reads must supply their frozen provider facts to the request-local store
/// before SQL sees the resulting tokenized table.  Keeping the factory here
/// prevents a synthetic relation from leaking into the shared catalog.
#[derive(Clone)]
pub struct QueryLocalTableOverlay {
    namespace: String,
    table: String,
    key: QueryTableBindingKey,
    materialize:
        Arc<dyn Fn(SqlTableBindingId) -> Result<QueryTableBinding, String> + Send + Sync + 'static>,
}

impl QueryLocalTableOverlay {
    pub fn new(
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
    pub fn new(
        current_catalog: Option<&'a str>,
        service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self::new_with_query_local_overlays(current_catalog, service, bindings, loader, Vec::new())
    }

    pub fn new_with_query_local_overlays(
        current_catalog: Option<&'a str>,
        service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
        overlays: Vec<QueryLocalTableOverlay>,
    ) -> Self {
        Self {
            current_catalog,
            service,
            bindings,
            loader,
            resolution_errors: Mutex::new(HashMap::new()),
            catalog_application: None,
            query_local_overlays: overlays
                .into_iter()
                .map(|overlay| (overlay.key(), overlay))
                .collect(),
        }
    }

    pub fn query_table_bindings(&self) -> Arc<QueryTableBindingStore> {
        Arc::clone(&self.bindings)
    }

    pub fn with_catalog_application(
        mut self,
        catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
    ) -> Self {
        self.catalog_application = catalog_application;
        self
    }

    fn require_catalog_admission(
        &self,
        catalog: &str,
    ) -> CatalogResolutionResult<Option<novarocks_catalog_application::CatalogRuntimeObservation>>
    {
        let Some(application) = self.catalog_application else {
            return Ok(None);
        };
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(catalog).map_err(|error| {
                CatalogResolutionError::failed(format!(
                    "invalid catalog instance `{catalog}`: {error}"
                ))
            })?;
        application
            .admit_catalog(&instance_id)
            .require_ready(&instance_id)
            .map(Some)
            .map_err(|error| CatalogResolutionError::failed(error.to_string()))
    }

    fn verify_catalog_admission(
        &self,
        catalog: &str,
        expected: Option<&novarocks_catalog_application::CatalogRuntimeObservation>,
    ) -> CatalogResolutionResult<()> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let current = self.require_catalog_admission(catalog)?.ok_or_else(|| {
            CatalogResolutionError::failed("catalog admission unexpectedly became legacy")
        })?;
        if &current != expected {
            return Err(CatalogResolutionError::failed(
                "catalog attachment generation changed while acquiring its planning lease",
            ));
        }
        Ok(())
    }

    /// Publish one application-resolved table only after its scan has been
    /// projected into the SQL vocabulary with the token allocated for this
    /// request.  Provider loaders may temporarily use a legacy carrier while
    /// decoding connector metadata, but that carrier must not escape this
    /// method into analysis or the compiler.
    fn bind_for_sql(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> CatalogResolutionResult<QueryTableBinding>,
    ) -> CatalogResolutionResult<SqlTableBindingId> {
        if let Some(error) = self
            .resolution_errors
            .lock()
            .expect("catalog resolution error lock")
            .get(&key)
            .cloned()
        {
            return Err(error);
        }
        let error_key = key.clone();
        let mut typed_error = None;
        let result = self.bindings.resolve_or_insert_with_id(key, |binding_id| {
            let binding = load(binding_id).map_err(|error| {
                let message = error.message().to_string();
                self.resolution_errors
                    .lock()
                    .expect("catalog resolution error lock")
                    .insert(error_key.clone(), error.clone());
                typed_error = Some(error);
                message
            })?;
            project_binding_for_sql(binding_id, binding)
        });
        result.map_err(|message| {
            let error = typed_error
                .or_else(|| {
                    self.resolution_errors
                        .lock()
                        .expect("catalog resolution error lock")
                        .get(&error_key)
                        .cloned()
                })
                .unwrap_or_else(|| CatalogResolutionError::failed(message));
            self.resolution_errors
                .lock()
                .expect("catalog resolution error lock")
                .insert(error_key, error.clone());
            error
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
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                if let Some(overlay) = self
                    .query_local_overlays
                    .get(&(database.to_ascii_lowercase(), table.to_ascii_lowercase()))
                    .cloned()
                {
                    return self.resolve_query_local_overlay(overlay);
                }
                let local = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock");
                let resolved = resolve_local_catalog_table_typed(&local, database, table)?;
                let key = QueryTableBindingKey::analysis_lookup("default_catalog", database, table);
                let token = self.bind_for_sql(key, |binding| {
                    Ok(QueryTableBinding::local(resolved, binding))
                })?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader
                        .load_strict_base_table(catalog, database, table, binding_id)
                })?;
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
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
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        let token = self.bind_for_sql(overlay.key, |binding_id| {
            (overlay.materialize)(binding_id).map_err(CatalogResolutionError::failed)
        })?;
        Ok(self
            .bindings
            .binding(token)
            .map_err(CatalogResolutionError::failed)?
            .resolved
            .clone())
    }

    fn metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                let local = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock");
                resolve_local_catalog_table_typed(&local, database, table)
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
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
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
            }
        }
    }

    /// Resolve an ordinary relation without erasing whether it is absent or
    /// whether its catalog generation failed. SQL completion maps only
    /// `Missing` into a `CatalogRelationFact::missing` response.
    pub fn resolve_table_for_analysis_typed(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        crate::preparation_diagnostics::observe_result_lazy(
            "metadata_observation",
            || {
                format!(
                    "resolve_table:{}.{database}.{table}",
                    self.effective_catalog(catalog).unwrap_or("default_catalog")
                )
            },
            "static",
            None,
            || self.resolve_table_for_analysis_once(catalog, database, table),
        )
    }

    /// Resolve an Iceberg metadata relation with the same typed absence
    /// contract as ordinary catalog lookup.
    pub fn resolve_metadata_table_typed(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        crate::preparation_diagnostics::observe_result_lazy(
            "metadata_observation",
            || {
                format!(
                    "resolve_metadata_table:{}.{database}.{table}:{metadata_table_type:?}",
                    self.effective_catalog(catalog).unwrap_or("default_catalog")
                )
            },
            "static",
            None,
            || self.metadata_table_def(catalog, database, table, metadata_table_type),
        )
    }
}

fn resolve_local_catalog_table_typed(
    local: &novarocks_sql::planning::catalog::PlannerMemoryCatalog,
    database: &str,
    table: &str,
) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
    let database_exists = local
        .database_exists(database)
        .map_err(CatalogResolutionError::failed)?;
    if !database_exists {
        return Err(CatalogResolutionError::missing(format!(
            "unknown database: {database}"
        )));
    }
    let normalized_table = novarocks_types::naming::normalize_identifier(table)
        .map_err(CatalogResolutionError::failed)?;
    if !local
        .table_names_in_database(database)
        .into_iter()
        .any(|name| name == normalized_table)
    {
        return Err(CatalogResolutionError::missing(format!(
            "unknown table: {table}"
        )));
    }
    novarocks_sql::planning::catalog::resolve_local_catalog_table(local, database, table)
        .map_err(CatalogResolutionError::failed)
}

fn project_binding_for_sql(
    binding_id: SqlTableBindingId,
    binding: QueryTableBinding,
) -> Result<QueryTableBinding, String> {
    binding.validate_sql_scan_binding(binding_id)?;
    Ok(binding)
}

impl PlannerTableProvider for CatalogServiceMaterializer<'_> {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis_typed(catalog, database, table)
            .map_err(CatalogResolutionError::into_message)
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl novarocks_sql::compiler::SqlCatalogSnapshot for CatalogServiceMaterializer<'_> {
    fn planner_table_provider(&self) -> &dyn PlannerTableProvider {
        self
    }
}

impl IcebergMetadataTableProvider for CatalogServiceMaterializer<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_metadata_table_typed(catalog, database, table, metadata_table_type)
            .map_err(CatalogResolutionError::into_message)
    }
}

/// Builds the request-local SQL materializer behind the Frontend-owned catalog
/// admission gate.
///
/// Every analyzer entry point passes the state's application port: an external
/// table can only be materialized while its attachment is `Ready` in this
/// process, and there is no ungated variant to fall back to.
pub fn build_catalog_service_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
) -> CatalogServiceMaterializer<'a> {
    build_catalog_service_provider_with_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        _lookup_mode,
        Vec::new(),
        catalog_application,
    )
}

/// Build the application catalog facade for one admitted query, optionally
/// supplying generated relations that are scoped to that request. These
/// overlays are projected into SQL binding tokens before analysis and never
/// enter the shared local catalog.
pub fn build_catalog_service_provider_with_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    overlays: Vec<QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
) -> CatalogServiceMaterializer<'a> {
    let bindings = Arc::new(
        QueryTableBindingStore::try_new()
            .expect("query table binding scope allocation must not fail"),
    );
    build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        bindings,
        overlays,
        catalog_application,
    )
}

pub fn build_catalog_service_provider_with_bindings_and_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    bindings: Arc<QueryTableBindingStore>,
    overlays: Vec<QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
) -> CatalogServiceMaterializer<'a> {
    let loader = iceberg_table_binding_loader(controls, connector_context);
    CatalogServiceMaterializer::new_with_query_local_overlays(
        current_catalog,
        catalog_service,
        bindings,
        loader,
        overlays,
    )
    .with_catalog_application(catalog_application)
}

/// Application adapter for the SQL catalog's provider-neutral materialization
/// seam. The resulting binding carries the exact planning lease acquired for
/// metadata; SQL itself never names the Iceberg provider.
pub fn iceberg_table_binding_loader<'a>(
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Box<dyn QueryTableBindingLoader + 'a> {
    Box::new(IcebergTableBindingLoader {
        controls,
        connector_context,
    })
}

struct IcebergTableBindingLoader<'a> {
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl QueryTableBindingLoader for IcebergTableBindingLoader<'_> {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding_id: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        let (base_table, snapshot_id) =
            crate::catalog_application::query_bindings::parse_time_travel_overlay_identity(table)
                .map(|(base_table, snapshot_id)| (base_table, Some(snapshot_id)))
                .unwrap_or((table, None));
        let mut materialization = load_connector_table_materialization_with_lease_typed(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            base_table,
        )?;
        if let Some(snapshot_id) = snapshot_id {
            materialization.read_selector =
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id);
        }
        connector_query_binding_from_materialization(
            materialization,
            catalog,
            namespace,
            table,
            binding_id,
        )
        .map_err(CatalogResolutionError::failed)
    }

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding_id: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        let alias = format!(
            "{table}${}",
            metadata_table_alias_suffix(metadata_table_type)
        );
        let materialization = load_connector_table_alias_materialization_with_lease_typed(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            &alias,
        )?;
        // The columns SQL analyzes must be the ones the typed reader produces.
        // The connector's own alias materialization still reports the retired
        // schema — `committed_at` as a bare `Int64`, `summary` as `Utf8` —
        // while both the frozen contract and the typed reader say
        // `TIMESTAMP WITH TIME ZONE` and `MAP`. Analyzing the retired shape
        // renders a raw epoch integer, and ordering by it fails at the
        // exchange with a schema mismatch.
        let columns =
            frozen_metadata_columns(metadata_table_type).unwrap_or(materialization.columns);
        Ok(QueryTableBinding {
            resolved: novarocks_sql::planning::catalog::resolved_metadata_table(
                catalog,
                namespace,
                table,
                metadata_table_type,
                columns,
                materialization.row_lineage_metadata_columns,
                binding_id,
            ),
            statistics_pin: materialization.statistics_pin.clone(),
            admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
            scan_materialization: Some(QueryScanMaterialization {
                table: materialization.read_table,
                catalog_handle: materialization.catalog_handle,
                schema: materialization.read_schema,
                selector: materialization.read_selector,
                statistics_pin: materialization.statistics_pin,
                planning_lease: materialization.planning_lease,
            }),
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        })
    }
}

/// The frozen column contract for one metadata relation, when it is fully
/// determined by the relation kind alone.
///
/// `$files`, `$entries` and `$partitions` are not: their `partition`,
/// `lower_bounds` and `upper_bounds` columns are derived from the table's own
/// partition spec and schema, and the contract entry point omits them rather
/// than guess. Those facts live in the connector and no typed column binding
/// carries a type today, so those three keep resolving through the connector's
/// alias materialization until the derived types can cross the control plane.
/// Returning `None` for them is that staged state, stated rather than hidden.
fn frozen_metadata_columns(
    kind: novarocks_sql::planning::catalog::MetadataTableKind,
) -> Option<Vec<novarocks_types::schema::ColumnDef>> {
    use novarocks_sql::planning::catalog::MetadataTableKind;

    match kind {
        MetadataTableKind::Files | MetadataTableKind::Entries | MetadataTableKind::Partitions => {
            None
        }
        MetadataTableKind::Snapshots
        | MetadataTableKind::History
        | MetadataTableKind::Refs
        | MetadataTableKind::Manifests => Some(
            novarocks_sql::planning::catalog::metadata_table_schema(kind)
                .into_iter()
                .map(|column| novarocks_types::schema::ColumnDef {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                    write_default: None,
                    logical_type: column.logical_type,
                })
                .collect(),
        ),
    }
}

/// Connector read-alias suffix for one metadata relation.
///
/// The suffix is the only thing that tells the connector which system relation
/// to materialize, so it must stay exactly the connector's own vocabulary. The
/// match is exhaustive on purpose: a new relation must fail to compile here
/// rather than resolve to some other relation's schema.
pub(crate) fn metadata_table_alias_suffix(
    kind: novarocks_sql::planning::catalog::MetadataTableKind,
) -> &'static str {
    use novarocks_sql::planning::catalog::MetadataTableKind;

    match kind {
        MetadataTableKind::Snapshots => "SNAPSHOTS",
        MetadataTableKind::History => "HISTORY",
        MetadataTableKind::Refs => "REFS",
        MetadataTableKind::Files => "FILES",
        MetadataTableKind::Manifests => "MANIFESTS",
        MetadataTableKind::Partitions => "PARTITIONS",
        // Was `LOGICAL_ICEBERG_METADATA`. The alias is retired; `$entries` is
        // the only spelling, and it maps to the wire's ENTRIES worker type.
        MetadataTableKind::Entries => "ENTRIES",
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use novarocks_sql::binding::SqlTableBindingAllocator;
    use novarocks_sql::planning::catalog::PlannerTableProvider;

    fn binding_id(scope: u64, ordinal: u32) -> SqlTableBindingId {
        let ordinal = NonZeroU32::new(ordinal).expect("non-zero ordinal");
        let mut allocator = SqlTableBindingAllocator::try_new_for_test(
            NonZeroU64::new(scope).expect("non-zero scope"),
        )
        .expect("test binding allocator");
        for _ in 1..ordinal.get() {
            allocator.allocate().expect("non-zero test binding ordinal");
        }
        allocator.allocate().expect("non-zero test binding ordinal")
    }

    fn local_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        QueryTableBinding::local(
            test_connector_read_materialization(
                "default_catalog",
                "orders",
                binding,
                novarocks_spi::connector::ConnectorReadSelector::Current,
            ),
            binding,
        )
    }

    fn frozen_overlay_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        QueryTableBinding {
            resolved: test_connector_read_materialization(
                "ice",
                "__nr_cow_orders",
                binding,
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(7),
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Local,
            scan_materialization: None,
            write_target_admission: None,
            mv_target_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        }
    }

    fn test_connector_read_materialization(
        catalog: &str,
        table: &str,
        binding: SqlTableBindingId,
        selector: novarocks_spi::connector::ConnectorReadSelector,
    ) -> ResolvedAnalyzerTable {
        novarocks_sql::planning::catalog::materialize_connector_read_table(
            novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                catalog: catalog.to_string(),
                namespace: "db".to_string(),
                table: table.to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                binding,
                selector,
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
            },
        )
        .expect("test catalog facts materialize")
        .into_resolved_table()
    }

    struct OverlayLoader;

    impl QueryTableBindingLoader for OverlayLoader {
        fn load_strict_base_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            Ok(local_binding(_binding))
        }

        fn load_metadata_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            Err(CatalogResolutionError::failed(
                "metadata is not part of this overlay fixture",
            ))
        }
    }

    #[derive(Clone, Copy)]
    enum TypedLoaderFailure {
        Missing,
        Failed,
    }

    struct TypedFailingLoader(TypedLoaderFailure);

    impl QueryTableBindingLoader for TypedFailingLoader {
        fn load_strict_base_table(
            &self,
            _catalog: &str,
            namespace: &str,
            table: &str,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            match self.0 {
                TypedLoaderFailure::Missing => Err(CatalogResolutionError::missing(format!(
                    "unknown table: {namespace}.{table}"
                ))),
                TypedLoaderFailure::Failed => {
                    Err(CatalogResolutionError::failed("catalog transport failed"))
                }
            }
        }

        fn load_metadata_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            Err(CatalogResolutionError::failed(
                "metadata is not part of this fixture",
            ))
        }
    }

    struct UnavailableCatalogApplication;

    struct ChangingCatalogApplication {
        admissions: AtomicUsize,
    }

    impl ChangingCatalogApplication {
        fn observation(
            generation: u64,
        ) -> novarocks_catalog_application::CatalogRuntimeObservation {
            novarocks_catalog_application::CatalogRuntimeObservation {
                attachment_id: if generation == 1 {
                    uuid::Uuid::from_u128(1)
                } else {
                    uuid::Uuid::from_u128(2)
                },
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                    .expect("instance ID"),
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .expect("provider ID"),
                generation,
            }
        }
    }

    impl novarocks_catalog_application::CatalogApplicationPort for ChangingCatalogApplication {
        fn create_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogCreateCommand,
        ) -> Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        > {
            unreachable!("create is not part of this fixture")
        }

        fn drop_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogDropCommand,
        ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
            unreachable!("drop is not part of this fixture")
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> novarocks_catalog_application::CatalogAdmission {
            let attempt = self.admissions.fetch_add(1, Ordering::SeqCst);
            novarocks_catalog_application::CatalogAdmission::Ready(Self::observation(
                if attempt == 0 { 1 } else { 2 },
            ))
        }
    }

    impl novarocks_catalog_application::CatalogApplicationPort for UnavailableCatalogApplication {
        fn create_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogCreateCommand,
        ) -> Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        > {
            Err(novarocks_catalog_application::CatalogApplicationError::new(
                novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
                "projection is stale",
            ))
        }

        fn drop_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogDropCommand,
        ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
            Err(novarocks_catalog_application::CatalogApplicationError::new(
                novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
                "projection is stale",
            ))
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> novarocks_catalog_application::CatalogAdmission {
            novarocks_catalog_application::CatalogAdmission::Unavailable {
                reason: "projection is stale".to_string(),
            }
        }
    }

    #[test]
    fn sqlx2_application_materializer_projects_local_scan_before_publication() {
        let binding =
            project_binding_for_sql(binding_id(101, 1), local_binding(binding_id(101, 1)))
                .expect("local scan must be tokenized before SQL receives it");

        assert!(matches!(
            novarocks_sql::planning::catalog::table_binding_id(&binding.resolved),
            token if token == binding_id(101, 1)
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
    fn catalog_application_admission_fails_closed_before_external_materialization() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let application = UnavailableCatalogApplication;
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings,
            Box::new(OverlayLoader),
        )
        .with_catalog_application(Some(&application));

        let error = materializer
            .resolve_table_for_analysis(None, "db", "orders")
            .expect_err("stale projection must not materialize a table");

        assert_eq!(
            error,
            "catalog `ice` is unavailable on this frontend: projection is stale"
        );
    }

    #[test]
    fn catalog_application_rejects_generation_change_while_acquiring_planning_binding() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let application = ChangingCatalogApplication {
            admissions: AtomicUsize::new(0),
        };
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings,
            Box::new(OverlayLoader),
        )
        .with_catalog_application(Some(&application));

        let error = materializer
            .resolve_table_for_analysis(None, "db", "orders")
            .expect_err("drop and recreate must not switch the request to a new generation");

        assert_eq!(
            error,
            "catalog attachment generation changed while acquiring its planning lease"
        );
    }

    #[test]
    fn typed_resolution_distinguishes_external_missing_from_failure() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let missing_materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("missing binding store")),
            Box::new(TypedFailingLoader(TypedLoaderFailure::Missing)),
        );
        let missing = missing_materializer
            .resolve_table_for_analysis_typed(None, "db", "orders")
            .expect_err("missing table must remain a typed absence");
        let repeated_missing = missing_materializer
            .resolve_table_for_analysis_typed(None, "db", "orders")
            .expect_err("memoized missing table must retain its typed absence");
        let failed = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("failure binding store")),
            Box::new(TypedFailingLoader(TypedLoaderFailure::Failed)),
        )
        .resolve_table_for_analysis_typed(None, "db", "orders")
        .expect_err("catalog failure must interrupt resolution");

        assert!(matches!(missing, CatalogResolutionError::Missing { .. }));
        assert_eq!(missing.message(), "unknown table: db.orders");
        assert!(matches!(
            repeated_missing,
            CatalogResolutionError::Missing { .. }
        ));
        assert!(matches!(failed, CatalogResolutionError::Failed { .. }));
        assert_eq!(failed.message(), "catalog transport failed");
    }

    #[test]
    fn typed_local_resolution_preserves_missing_and_invalid_name_failure() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let materializer = CatalogServiceMaterializer::new(
            Some("default_catalog"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("binding store")),
            Box::new(OverlayLoader),
        );

        let missing = materializer
            .resolve_table_for_analysis_typed(None, "default", "orders")
            .expect_err("absent local table must remain a typed absence");
        let invalid = materializer
            .resolve_table_for_analysis_typed(None, "default", "bad-name")
            .expect_err("invalid local name must remain a hard failure");

        assert_eq!(
            missing,
            CatalogResolutionError::Missing {
                reason: "unknown table: orders".to_string(),
            }
        );
        assert!(matches!(invalid, CatalogResolutionError::Failed { .. }));
        assert_eq!(invalid.message(), "unsupported identifier `bad-name`");
    }

    #[test]
    fn legacy_catalog_provider_stringifies_typed_missing_at_the_edge() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("binding store")),
            Box::new(TypedFailingLoader(TypedLoaderFailure::Missing)),
        );

        let error = materializer
            .resolve_table_for_analysis(None, "db", "orders")
            .expect_err("legacy provider still reports a string error");

        assert_eq!(error, "unknown table: db.orders");
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
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
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
        let binding_id = novarocks_sql::planning::catalog::table_binding_id(&resolved);
        assert!(binding_id.belongs_to(bindings.scope()));
        assert_eq!(
            novarocks_sql::planning::catalog::frozen_input_snapshot_id(&resolved),
            Some(7)
        );
        assert!(
            bindings
                .scan_materialization(binding_id)
                .expect("binding materialization")
                .is_none(),
            "analysis-only overlays do not manufacture a provider read handle"
        );
        let local = service.local().read().expect("catalog read");
        assert!(
            novarocks_sql::planning::catalog::local_catalog_table(&local, "db", "__nr_cow_orders")
                .is_err()
        );
    }
}
