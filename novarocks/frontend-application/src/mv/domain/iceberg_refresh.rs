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

//! Projection/filter materialized views backed by Iceberg target tables in the
//! current Iceberg catalog. Aggregate shapes are accepted at CREATE time for
//! target schema and contract persistence; refresh execution is gated later.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::DataType;

use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::mv::domain::analysis::refresh_property::{
    RefreshFragmentProperty, TargetIdentity, derive_fragment_property, derive_imv_refresh_contract,
};
use crate::mv::domain::analysis::{
    MvAnalysis, canonicalize_iceberg_mv_select_query, output_column_to_table_column,
    resolve_mv_name, validate_mv_partition_columns,
};
use crate::mv::domain::analysis_adapter::{
    BaseColumnDescriptor, BaseTableDescriptor, validate_ivm_primary_key,
};
use crate::mv::domain::application::MvCreateProjectionSeed;
use crate::mv::domain::application::StagedMvTarget;
use crate::mv::domain::application::{
    CreatedMvTarget, MvCreateProviderAdapter, MvCreateProviderError, MvCreateProviderErrorKind,
    MvCreateRefreshPolicy, MvCreateStatement, MvDropStatement, MvRefreshRequest,
    PrepareMvCreateRequest, PreparedMvCreate,
};
use crate::mv::domain::lifecycle::{
    BackendRefreshPlan, IcebergRefreshPlan, RefreshError, RefreshPlan,
};
use crate::mv::domain::model::{MvStorageEngine, RefreshMode};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::refresh::apply_key::ApplyKeyContract;
use crate::mv::domain::refresh::capabilities::{RefreshCapabilities, RefreshIdentity};
use crate::mv::domain::refresh::contract::ImvRefreshContract;
use crate::mv::domain::refresh::definition::{
    load_iceberg_mv_definition_by_target, mv_definition_fingerprint, parse_mv_select_query,
};
#[cfg(test)]
use crate::mv::domain::refresh::execution_policy::{
    explain_refresh_full_guard, non_join_incremental_write_mode,
    select_join_incremental_execution_mode, should_use_join_delta_append_only_fast_path,
};
use crate::mv::domain::refresh::observation::{
    observe_current_refresh_base, observe_schema_validation_for_table,
    rebind_mv_definition_before_refresh_derivation,
};
use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
use crate::mv::domain::refresh::planning::{
    RefreshPlanContract, RefreshPlanningInput, RefreshStateBaseline, RefreshStateBaselineSource,
    decide_refresh_plan,
};
#[cfg(test)]
use crate::mv::domain::refresh::repartition::{RepartitionShape, select_repartition_shape};
use crate::mv::domain::refresh::rewrite_context::observe_and_admit_change_window_for_table;
use crate::mv::domain::refresh::schema_contract::{
    MvOccurrenceFieldRebind, validate_aggregate_schema_contract_for_base,
    validate_aggregate_schema_contract_metadata, validate_projection_target,
};
use crate::mv::domain::refresh::snapshot::{
    BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision,
};
use crate::mv::domain::refresh::target::{
    IcebergMvTarget, load_iceberg_mv_target_binding_typed, resolve_refresh_target,
    validate_target_snapshot,
};
use crate::mv::domain::refresh::target_apply::{
    apply_key_table_column, branch_id_table_column, ensure_base_row_lineage_contract,
    join_apply_key_table_column,
};
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::refresh_io::acquire_mv_refresh_lock;
use crate::mv::domain::schema_validation::{
    validate_branch_id_field, validate_join_schema_contract, validate_schema_contract,
};
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_mv_application::persistence::codec::{ApplyKeyKind, RelationOccurrence};
use novarocks_mv_application::persistence::definition::CreateMvDefinitionRequest;
use novarocks_mv_application::persistence::definition::MvDesiredRefreshPolicy;
use novarocks_mv_application::persistence::dependency::CreateMvDependencyRequest;
use novarocks_mv_application::persistence::exact_revision::restore_exact_query_revision;
use novarocks_mv_application::persistence::projection::{MvPublicationState, StoredMvProjection};
use novarocks_mv_application::persistence::schema as mv_schema;
use novarocks_mv_application::persistence::schema::{
    APPLY_KEY_FIELD_ID_PROPERTY, APPLY_KEY_SOURCE_PROPERTY,
};
#[cfg(test)]
use novarocks_mv_application::product::{MvIncrementalJoinMode, MvIncrementalWriteMode};
use novarocks_parser::{Span, ast};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    CONNECTOR_MV_APPLY_KEY_COLUMN_PROPERTY as APPLY_KEY_COLUMN_PROPERTY,
    CONNECTOR_MV_HIDDEN_COLUMNS_PROPERTY as HIDDEN_COLUMNS_PROPERTY, ConnectorControlRegistry,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorTableObjectId,
};
use novarocks_sql::planning::mv::FULL_REFRESH_DISABLED_MESSAGE;
#[cfg(test)]
use novarocks_sql::planning::mv::MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME as GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_sql::planning::mv::UnionBranchKind;
use novarocks_sql::planning::mv::{
    MV_BRANCH_ID_COLUMN_NAME as BRANCH_ID_COLUMN_NAME,
    MV_HIDDEN_APPLY_KEY_COLUMN_NAME as HIDDEN_APPLY_KEY_COLUMN_NAME,
    MV_JOIN_APPLY_KEY_COLUMN_NAME as JOIN_APPLY_KEY_COLUMN_NAME, SqlMvAggregateCalls,
    SqlMvAggregateLayoutScope, SqlMvApplyKeySourceFacts, SqlMvJoinAliases, extract_join_aliases,
    mv_apply_key_source_from_column_name,
};
use novarocks_sql::semantic::{IcebergPartitionFieldExpr, ObjectName, TableColumnDef};
use novarocks_types::naming::{TableIdentity, normalize_identifier};

/// The explicit Core ports a refresh preparation may read while deriving its
/// immutable write artifact.  This deliberately names the individual
/// dependencies instead of admitting the aggregate application state into a
/// frontend-owned preparation path.
trait IcebergMvRefreshSource:
    crate::catalog_application::query_catalog::CatalogServiceSource + Send + Sync
{
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort>;
    fn connector_control(&self) -> &dyn ConnectorControlRegistry;
    fn storage_observation(&self) -> &dyn MvStorageObservationPort;
}

/// SQL-owned bridge for refresh planning.  It owns only analysis and immutable
/// facts; all durable intent, ref mutations, and writer execution are handed
/// to the frontend as `PreparedMvRefresh`.
/// The explicit Core inputs required by an Iceberg MV CREATE operation.
///
/// This is intentionally a narrow composition value: it owns the frozen
/// catalog source plus the provider and durable MV ports that CREATE needs.
/// It does not retain aggregate application state, and it has no state-based
/// constructor so a frontend composition must name every dependency.
#[derive(Clone)]
pub struct IcebergMvCorePorts {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Option<Arc<novarocks_catalog_application::ConnectorControlHost>>,
    readiness: Arc<MvReadinessPort>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    /// The one FE-process management authority every document-managed effect
    /// commits under. It is optional only so a composition root that does not
    /// own the typed control host can install it separately.
    management_entrance: Option<Arc<novarocks_mv_application::management::ManagementEntrance>>,
}

impl IcebergMvCorePorts {
    /// Construct the exact provider and durable-MV ports required by the
    /// Iceberg MV backend. Frontend composition must provide every leaf; this
    /// value deliberately has no application-facade constructor.
    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        readiness: Arc<MvReadinessPort>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control: None,
            readiness,
            storage_observation,
            management_entrance: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_management_entrance(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        readiness: Arc<MvReadinessPort>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
    ) -> Self {
        Self {
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control: Some(typed_connector_control),
            readiness,
            storage_observation,
            management_entrance: Some(management_entrance),
        }
    }

    /// Install the FE management authority on a port set composed without it.
    ///
    /// Composition roots that do not own the typed control host still have to
    /// publish, and publishing is a management effect. This is the one way to
    /// add the authority after construction; it never replaces one already
    /// installed, so a composition cannot quietly swap incarnations.
    pub(crate) fn with_management_entrance(
        mut self,
        management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
    ) -> Self {
        debug_assert!(
            self.management_entrance.is_none(),
            "MV core ports already own a management entrance"
        );
        self.management_entrance = Some(management_entrance);
        self
    }

    pub(crate) fn management_entrance(
        &self,
    ) -> Result<&Arc<novarocks_mv_application::management::ManagementEntrance>, String> {
        self.management_entrance.as_ref().ok_or_else(|| {
            "document-managed MV effects require the composed FE management entrance".to_string()
        })
    }

    pub(crate) fn typed_connector_control(
        &self,
    ) -> Result<&Arc<novarocks_catalog_application::ConnectorControlHost>, String> {
        self.typed_connector_control.as_ref().ok_or_else(|| {
            "document-managed MV CREATE requires the composed typed connector control host"
                .to_string()
        })
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    pub(crate) fn readiness(&self) -> &Arc<MvReadinessPort> {
        &self.readiness
    }

    pub(crate) fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }

    pub(crate) fn storage_observation(&self) -> &dyn MvStorageObservationPort {
        self.storage_observation.as_ref()
    }

    pub(crate) fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.catalog_application.as_deref()
    }
}

impl crate::catalog_application::query_catalog::CatalogServiceSource for IcebergMvCorePorts {
    fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }
}

impl IcebergMvRefreshSource for IcebergMvCorePorts {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.catalog_application.as_deref()
    }

    fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }

    fn storage_observation(&self) -> &dyn MvStorageObservationPort {
        self.storage_observation.as_ref()
    }
}

/// Core adapter used by the frontend-owned MV application service. It keeps
/// explicit connector/analyzer ports in core while exposing CREATE as
/// auditable, side-effect-sized primitives.
pub(crate) struct IcebergMvCreateProviderAdapter {
    ports: IcebergMvCorePorts,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    preparations: Mutex<HashMap<String, Arc<IcebergMvCreatePreparation>>>,
}

struct IcebergMvCreatePreparation {
    target: IcebergMvTarget,
    refresh_contract: ImvRefreshContract,
    property: RefreshFragmentProperty,
    base_refs: Vec<TableIdentity>,
    dependencies: Vec<CreateMvDependencyRequest>,
    /// The occurrence-qualified SQL facts D/L are bound against. Computed once
    /// with the analysis so the document build cannot re-derive a different
    /// projection of the same query.
    create_persistence_facts: novarocks_sql::planning::mv::SqlMvCreatePersistenceFacts,
    /// Present exactly when the target carries a UNION branch discriminator.
    branch_id_column_name: Option<String>,
    source_field_observations:
        Vec<novarocks_mv_application::persistence::aggregate_bindings::MvCreateRelationObservation>,
    created_at_ms: i64,
    /// Runtime state layout frozen during the same CREATE analysis as the
    /// physical request. Stateless shapes use the validated empty layout so
    /// D/L construction never invents an alternate aggregate carrier.
    aggregate_runtime_layout: novarocks_types::mv_aggregate_layout::MvAggregateRuntimeLayout,
    columns: Vec<TableColumnDef>,
    partition_fields: Vec<IcebergPartitionFieldExpr>,
    target_properties: Vec<(String, String)>,
    /// The invisible staged target this statement holds between `stage_target`
    /// and its single publish or abort.
    staged: Mutex<Option<crate::mv::domain::staged_create::StagedMvCreateTarget>>,
}

impl IcebergMvCreateProviderAdapter {
    pub(crate) fn new_with_ports(
        ports: IcebergMvCorePorts,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            ports,
            connector_context,
            preparations: Mutex::new(HashMap::new()),
        }
    }

    fn preparation_key(target: &MvTarget) -> String {
        format!(
            "{}.{}.{}",
            target.catalog.as_deref().unwrap_or_default(),
            target.database,
            target.name
        )
    }

    fn preparation(
        &self,
        plan: &PreparedMvCreate,
    ) -> Result<Arc<IcebergMvCreatePreparation>, MvCreateProviderError> {
        self.preparation_for_target(&plan.target)
    }

    fn preparation_for_target(
        &self,
        target: &MvTarget,
    ) -> Result<Arc<IcebergMvCreatePreparation>, MvCreateProviderError> {
        self.preparations
            .lock()
            .map_err(|error| {
                MvCreateProviderError::new(
                    MvCreateProviderErrorKind::TargetOperation,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .get(&Self::preparation_key(target))
            .cloned()
            .ok_or_else(|| {
                MvCreateProviderError::new(
                    MvCreateProviderErrorKind::InvalidRequest,
                    "MV CREATE plan was not prepared by this engine",
                )
            })
    }
}

impl IcebergMvCreateProviderAdapter {
    /// Take the one staged target this statement holds.
    ///
    /// A stage settles exactly once. Publishing or aborting twice, or under a
    /// different operation, is a programming error rather than a retry.
    fn take_staged_target(
        &self,
        prepared: &IcebergMvCreatePreparation,
        staged: &StagedMvTarget,
    ) -> Result<crate::mv::domain::staged_create::StagedMvCreateTarget, MvCreateProviderError> {
        let taken = prepared
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                engine_target_error(
                    "MV CREATE has no staged target to settle; it was never staged or already \
                     settled"
                        .to_string(),
                )
            })?;
        if taken.operation_id()
            != novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                *staged.staged_operation_id.as_bytes(),
            )
        {
            return Err(engine_target_error(
                "MV CREATE staged target belongs to a different provider operation".to_string(),
            ));
        }
        Ok(taken)
    }

    /// Seal the invisible staged target with the provider's own empty prepared
    /// set. MV CREATE writes no rows, so the published target has no snapshot.
    fn seal_empty_staged_write(
        &self,
        staged: &crate::mv::domain::staged_create::StagedMvCreateTarget,
        target: &IcebergMvTarget,
    ) -> Result<novarocks_spi::connector::ConnectorStagedWriteProof, MvCreateProviderError> {
        let host = self
            .ports
            .typed_connector_control()
            .map_err(engine_target_error)?;
        let planning_lease = staged.planning_lease();
        let binding = staged
            .staged_lease()
            .plan_write(
                novarocks_spi::connector::ConnectorStagedWritePlanningRequest {
                    handle: staged.handle().clone(),
                    context: self.connector_context.clone(),
                },
            )
            .map_err(|error| {
                engine_target_error(format!("plan MV CREATE staged write: {error}"))
            })?;
        let write_lease = planning_lease
            .derive_write_lease()
            .map_err(|error| engine_target_error(error.to_string()))?;
        let stack = crate::connector::write_target::derive_write_stack_lease(host, planning_lease)
            .map_err(engine_target_error)?;
        let session = crate::query_execution::write_session::begin_connector_write_session(
            stack,
            &write_lease,
            novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
                table: Arc::from(format!("{}.{}", target.namespace, target.table).as_str()),
                target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
                intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
                purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
                // The session writes no rows, but it still describes the
                // shape of the target it is opened on.
                input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                    fields: staged_target_write_fields(staged)?,
                },
                base: None,
                flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::StagedCreate(
                    binding.table().clone(),
                ),
                context: binding.context().clone(),
            },
        )
        .map_err(engine_target_error)?;
        let sealed =
            crate::query_execution::write_session::finish_empty_staged_create_write_for_following_terminal_action(
                session.as_ref(),
                self.connector_context.clone(),
            )
            .map_err(|error| {
                MvCreateProviderError::new(
                    MvCreateProviderErrorKind::KnownUncommitted,
                    format!("seal the empty MV CREATE staged write: {error}"),
                )
            })?;
        let (outcome, affected_rows, terminal_context) = sealed.into_parts();
        let novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted { receipt, .. } =
            outcome
        else {
            return Err(MvCreateProviderError::new(
                MvCreateProviderErrorKind::KnownUncommitted,
                "MV CREATE staged write was not sealed".to_string(),
            ));
        };
        if affected_rows != Some(0) {
            return Err(engine_target_error(
                "MV CREATE staged write reported rows; creation publishes no data".to_string(),
            ));
        }
        let proof = novarocks_spi::connector::ConnectorStagedWriteProof::try_new(receipt, 0)
            .map_err(|error| {
                engine_target_error(format!(
                    "MV CREATE write receipt is not publishable: {error}"
                ))
            })?;
        staged
            .staged_lease()
            .bind_write(staged.handle().clone(), proof.clone())
            .map_err(|error| {
                engine_target_error(format!(
                    "staged MV CREATE target refused its write: {error}"
                ))
            })?;
        drop(terminal_context);
        Ok(proof)
    }

    /// The marker the provider stamps on every object this deployment manages.
    fn managed_object_marker(
        &self,
    ) -> Result<
        novarocks_spi::connector::document_storage::ConnectorManagedObjectMarker,
        MvCreateProviderError,
    > {
        let entrance = self
            .ports
            .management_entrance()
            .map_err(engine_target_error)?;
        novarocks_spi::connector::document_storage::ConnectorManagedObjectMarker::try_new(
            "materialized-view",
            entrance.owner().as_str(),
            entrance.incarnation().as_str(),
        )
        .map_err(|error| {
            engine_target_error(format!("build the MV managed object marker: {error}"))
        })
    }
}

impl MvCreateProviderAdapter for IcebergMvCreateProviderAdapter {
    fn prepare_create(
        &self,
        request: PrepareMvCreateRequest<'_>,
    ) -> Result<PreparedMvCreate, MvCreateProviderError> {
        let prepared = prepare_iceberg_mv_create_with_ports(
            &self.ports,
            request.context.current_catalog,
            request.context.current_database,
            request.statement,
            &self.connector_context,
        )
        .map_err(engine_prepare_error)?;
        let target = MvTarget {
            catalog: Some(prepared.target.catalog.clone()),
            database: prepared.target.namespace.clone(),
            name: prepared.target.table.clone(),
        };
        let projection_seed = MvCreateProjectionSeed {
            definition: CreateMvDefinitionRequest {
                query_definition:
                    novarocks_query_application::persisted_query_definition::PersistedQueryDefinition::new(
                        request.statement.select_sql.clone(),
                        novarocks_query_application::persisted_query_definition::PersistedQueryDialect::StarRocks,
                        request.context.current_catalog.unwrap_or("default_catalog"),
                        request.context.current_database,
                    )
                    .map_err(engine_prepare_error)?,
                base_table_refs: prepared.base_refs.iter().map(TableIdentity::fqn).collect(),
                primary_key_columns: request.statement.primary_key.clone().unwrap_or_default(),
                storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
                target_catalog: Some(prepared.target.catalog.clone()),
                target_namespace: Some(prepared.target.namespace.clone()),
                target_table: Some(prepared.target.table.clone()),
                schema_contract: None,
                partition_spec: None,
                created_at_ms: prepared.created_at_ms,
            },
            refresh: initial_refresh_configuration_for_create(&request.statement.refresh_policy),
            dependencies: prepared.dependencies.clone(),
        };
        self.preparations
            .lock()
            .map_err(|error| {
                MvCreateProviderError::new(
                    MvCreateProviderErrorKind::TargetOperation,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .insert(Self::preparation_key(&target), Arc::new(prepared));
        Ok(PreparedMvCreate::new(target, projection_seed))
    }

    fn stage_target(
        &self,
        plan: &PreparedMvCreate,
        operation_id: uuid::Uuid,
    ) -> Result<StagedMvTarget, MvCreateProviderError> {
        let prepared = self.preparation(plan)?;
        let entrance = self
            .ports
            .management_entrance()
            .map_err(engine_target_error)?;
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&prepared.target.catalog)
                .map_err(|error| engine_target_error(error.to_string()))?;
        let planning_lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.ports.connector_control.as_ref(),
            &instance_id,
        )
        .map_err(|error| engine_target_error(error.to_string()))?;
        let table = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from(prepared.target.namespace.as_str()),
            table: Arc::from(prepared.target.table.as_str()),
        };
        let staged = crate::mv::domain::staged_create::stage_mv_create_target(
            entrance.as_ref(),
            crate::mv::domain::staged_create::StageMvCreateRequest {
                planning_lease,
                table,
                columns: prepared
                    .columns
                    .iter()
                    .map(crate::catalog_application::statement::connector_column)
                    .collect::<Result<_, _>>()
                    .map_err(engine_target_error)?,
                partitioning: prepared
                    .partition_fields
                    .iter()
                    .map(crate::catalog_application::statement::connector_partition_transform)
                    .collect(),
                properties: prepared
                    .target_properties
                    .iter()
                    .map(|(key, value)| (Arc::from(key.as_str()), Arc::from(value.as_str())))
                    .collect(),
                operation_id,
                context: &self.connector_context,
            },
        )
        .map_err(engine_target_error)?;
        *prepared
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(staged);
        Ok(StagedMvTarget {
            target: plan.target.clone(),
            staged_operation_id: operation_id,
        })
    }

    fn publish_staged_target(
        &self,
        plan: &PreparedMvCreate,
        staged: &StagedMvTarget,
    ) -> Result<CreatedMvTarget, MvCreateProviderError> {
        use crate::mv::domain::staged_create::StagedPublishOutcome;

        let prepared = self.preparation(plan)?;
        // Build the documents and seal the empty write while the stage is
        // still held, so a failure before the publish leaves it for the
        // product to abort rather than stranding it.
        let (documents, write) = {
            let guard = prepared
                .staged
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let target = guard.as_ref().ok_or_else(|| {
                engine_target_error("MV CREATE has no staged target to publish".to_string())
            })?;
            let prepared_target = target.handle().document_target().ok_or_else(|| {
                engine_target_error(
                    "staged MV CREATE target carries no prepared document binding".to_string(),
                )
            })?;
            let documents = build_create_documents_for_prepared_target(
                &prepared,
                &plan.projection_seed,
                prepared_target,
            )
            .map_err(engine_target_error)?;
            let write = self.seal_empty_staged_write(target, &prepared.target)?;
            (documents, write)
        };
        let target = self.take_staged_target(&prepared, staged)?;
        let marker = self.managed_object_marker()?;
        match target.publish(&documents, write, marker, &self.connector_context) {
            StagedPublishOutcome::Published(object_id) => Ok(CreatedMvTarget {
                target: plan.target.clone(),
                object_id,
            }),
            StagedPublishOutcome::NotPublished(message) => Err(MvCreateProviderError::new(
                MvCreateProviderErrorKind::TargetOperation,
                message,
            )),
            StagedPublishOutcome::Unknown(message) => Err(MvCreateProviderError::new(
                MvCreateProviderErrorKind::CommitUnknown,
                message,
            )),
        }
    }

    fn abort_staged_target(
        &self,
        plan: &PreparedMvCreate,
        staged: &StagedMvTarget,
    ) -> Result<(), MvCreateProviderError> {
        let prepared = self.preparation(plan)?;
        self.take_staged_target(&prepared, staged)?
            .abort(&self.connector_context)
            .map_err(engine_target_error)
    }

    fn install_created_projection(
        &self,
        target: &CreatedMvTarget,
        operation_id: uuid::Uuid,
    ) -> Result<(), MvCreateProviderError> {
        // The create is committed, so this may never re-create or delete the
        // target. It converges management onto the target this statement just
        // published and installs Current from that same sealed observation.
        let entrance = self
            .ports
            .management_entrance()
            .map_err(engine_target_error)?;
        let catalog_name =
            target.target.catalog.as_deref().ok_or_else(|| {
                engine_target_error("created MV target has no catalog".to_string())
            })?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog_name)
            .map_err(|error| engine_target_error(error.to_string()))?;
        let lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.ports.connector_control.as_ref(),
            &instance_id,
        )
        .map_err(|error| engine_target_error(error.to_string()))?;
        let catalog = lease
            .binding()
            .catalog_handle()
            .map_err(|error| engine_target_error(error.to_string()))?
            .clone();
        let product_target = novarocks_mv_application::product::MvTarget::try_new(
            target.target.catalog.clone(),
            target.target.database.clone(),
            target.target.name.clone(),
        )
        .map_err(|error| engine_target_error(error.to_string()))?;
        crate::mv::domain::staged_create::install_created_current_projection(
            entrance.as_ref(),
            self.ports.readiness().as_ref(),
            self.ports.connector_control.as_ref(),
            catalog,
            product_target,
            operation_id,
            // The create already happened; observe under a scope that cannot
            // be mistaken for part of the same effect.
            self.connector_context.clone().after_external_effect(),
        )
        .map_err(|error| {
            MvCreateProviderError::new(MvCreateProviderErrorKind::DescriptorSync, error)
        })
    }

    fn register_target(&self, target: &CreatedMvTarget) -> Result<(), MvCreateProviderError> {
        let preparation_key = Self::preparation_key(&target.target);
        let target = IcebergMvTarget {
            catalog: target.target.catalog.clone().ok_or_else(|| {
                MvCreateProviderError::new(
                    MvCreateProviderErrorKind::CatalogRegistration,
                    "Iceberg MV target has no catalog",
                )
            })?,
            namespace: target.target.database.clone(),
            table: target.target.name.clone(),
        };
        #[cfg(test)]
        if let Some(error) = take_catalog_registration_failure_for_test() {
            return Err(MvCreateProviderError::new(
                MvCreateProviderErrorKind::CatalogRegistration,
                error,
            ));
        }
        register_iceberg_mv_target_in_catalog(self.ports.connector_control.as_ref(), &target)
            .map_err(|error| {
                MvCreateProviderError::new(MvCreateProviderErrorKind::CatalogRegistration, error)
            })?;
        self.preparations
            .lock()
            .map_err(|error| {
                MvCreateProviderError::new(
                    MvCreateProviderErrorKind::CatalogRegistration,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .remove(&preparation_key);
        Ok(())
    }
}

fn engine_prepare_error(error: String) -> MvCreateProviderError {
    MvCreateProviderError::new(MvCreateProviderErrorKind::Analysis, error)
}

fn engine_target_error(error: String) -> MvCreateProviderError {
    MvCreateProviderError::new(MvCreateProviderErrorKind::TargetOperation, error)
}

fn initial_refresh_configuration_for_create(
    policy: &crate::mv::domain::application::MvCreateRefreshPolicy,
) -> novarocks_mv_application::repository::InitialMvRefreshConfiguration {
    let (policy, interval_ms) = match policy {
        crate::mv::domain::application::MvCreateRefreshPolicy::Manual => {
            (MvDesiredRefreshPolicy::Manual, None)
        }
        crate::mv::domain::application::MvCreateRefreshPolicy::AsyncOnChange => {
            (MvDesiredRefreshPolicy::AsyncOnChange, None)
        }
        crate::mv::domain::application::MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            (MvDesiredRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    };
    novarocks_mv_application::repository::InitialMvRefreshConfiguration {
        policy,
        paused: false,
        interval_ms,
        max_staleness_ms: None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn partition_fields_for_create(
    fields: Option<&Vec<crate::mv::domain::application::MvCreatePartitionField>>,
) -> Vec<IcebergPartitionFieldExpr> {
    fields
        .into_iter()
        .flatten()
        .map(|field| match field {
            crate::mv::domain::application::MvCreatePartitionField::Identity { column } => {
                IcebergPartitionFieldExpr::Identity {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Year { column } => {
                IcebergPartitionFieldExpr::Year {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Month { column } => {
                IcebergPartitionFieldExpr::Month {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Day { column } => {
                IcebergPartitionFieldExpr::Day {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Hour { column } => {
                IcebergPartitionFieldExpr::Hour {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Bucket {
                column,
                num_buckets,
            } => IcebergPartitionFieldExpr::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            crate::mv::domain::application::MvCreatePartitionField::Truncate { column, width } => {
                IcebergPartitionFieldExpr::Truncate {
                    column: column.clone(),
                    width: *width,
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Void { column } => {
                IcebergPartitionFieldExpr::Void {
                    column: column.clone(),
                }
            }
        })
        .collect()
}

fn prepare_iceberg_mv_create_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvCreateStatement,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<IcebergMvCreatePreparation, String> {
    crate::connector::validate_request_context(connector_context)?;
    let storage_engine = stmt
        .properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, value)| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "iceberg".to_string());
    match storage_engine.as_str() {
        "iceberg" => {}
        "starrocks" => return Err(
            "storage_engine='starrocks' is no longer supported for standalone materialized views; use storage_engine='iceberg'".to_string(),
        ),
        _ => return Err(format!("unknown materialized view storage_engine `{storage_engine}`")),
    }
    let current_catalog = current_catalog.ok_or_else(|| {
        "storage_engine='iceberg' requires current catalog to be an Iceberg catalog".to_string()
    })?;
    let (namespace, table) = match stmt.name_parts.as_slice() {
        [table] => (
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        ),
        [namespace, table] => (
            normalize_identifier(namespace)?,
            normalize_identifier(table)?,
        ),
        [catalog, namespace, table] if normalize_identifier(catalog)? == "default_catalog" => (
            normalize_identifier(namespace)?,
            normalize_identifier(table)?,
        ),
        [catalog, ..] => {
            return Err(format!(
                "materialized view name catalog must be `default_catalog`, got {}",
                normalize_identifier(catalog)?
            ));
        }
        _ => return Err("materialized view name must have one, two, or three parts".to_string()),
    };
    let target = IcebergMvTarget {
        catalog: normalize_identifier(current_catalog)?,
        namespace,
        table,
    };
    ensure_mv_create_target_absent_with_ports(ports, &target, connector_context)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &stmt.select_query,
        Some(current_catalog),
        current_database,
    );
    let catalog_service =
        crate::catalog_application::query_catalog::catalog_service_snapshot(ports);
    let provider = crate::catalog_application::query_materializer::build_catalog_service_provider(
        Some(current_catalog),
        &catalog_service,
        ports.connector_control.as_ref(),
        connector_context.clone(),
        novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
        ports.catalog_application.as_deref(),
    );
    let analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_provider(
        Some(current_catalog),
        &provider,
        current_database,
        &canonical_select_query,
        ports.function_catalog().as_ref(),
    )?;
    let refresh_contract = derive_imv_refresh_contract(&analysis)?;
    let partition_fields = partition_fields_for_create(stmt.partition_by.as_ref());
    validate_mv_partition_columns(Some(&partition_fields), &analysis.output_columns)?;
    let created_at_ms = now_ms();
    let resolved_dependencies =
        crate::mv::domain::dependency_resolver::resolve_create_mv_dependencies_with_readiness(
            ports.readiness.as_ref(),
            &analysis.resolved_refs,
            created_at_ms,
        )?;
    let dependency_target = novarocks_mv_application::dependency::iceberg_mv_dependency_ref(
        &target.catalog,
        &target.namespace,
        &target.table,
    );
    crate::mv::domain::dependency_resolver::validate_no_create_cycle_with_readiness(
        ports.readiness.as_ref(),
        &dependency_target,
        &resolved_dependencies.dependencies,
    )
    .map_err(|e| {
        format!(
            "cannot create materialized view {}.{}.{}: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;
    let property = derive_fragment_property(&analysis)?;
    let create_persistence_facts = analysis.refresh_input.create_persistence_facts()?;
    let source_field_observations =
        crate::mv::domain::persistence::source_bindings::observe_mv_create_source_bindings(
            ports.storage_observation.as_ref(),
            &create_persistence_facts,
            provider.query_table_bindings().as_ref(),
            connector_context.clone(),
        )?;
    let base_field_observations = observe_base_fields_for_refs_with_ports(
        ports,
        &resolved_dependencies.base_refs,
        connector_context,
    )?;
    for base_ref in &resolved_dependencies.base_refs {
        ensure_base_row_lineage_contract(
            observed_base(&base_field_observations, base_ref)?,
            &base_ref.fqn(),
        )?;
    }
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        match &property.identity {
            TargetIdentity::BaseRowId => {
                let base_ref = resolved_dependencies.base_refs.first().ok_or_else(|| {
                    "iceberg-backed materialized view has no resolved base table".to_string()
                })?;
                validate_ivm_primary_key(
                    pk_cols,
                    &base_table_descriptor_from_observation(observed_base(
                        &base_field_observations,
                        base_ref,
                    )?),
                )?;
            }
            TargetIdentity::JoinRowKey(_, _) => return Err("iceberg-backed join materialized views do not support PRIMARY KEY in this phase".to_string()),
            TargetIdentity::BranchScoped(_) => return Err("iceberg-backed UNION ALL materialized views do not support PRIMARY KEY in this phase".to_string()),
            TargetIdentity::GroupRowId(_) => return Err("iceberg-backed aggregate materialized views do not support PRIMARY KEY".to_string()),
        }
    }
    if !partition_fields.is_empty() && property.is_composed_aggregate_schema_contract_fallback() {
        return Err("partitioned composed aggregate Iceberg MV is not supported".to_string());
    }
    let apply_key_column_name = refresh_contract.apply_key.column_name;
    if analysis
        .output_columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
    {
        return Err(format!(
            "Iceberg MV output column name {apply_key_column_name} is reserved for internal apply key"
        ));
    }
    if identity_needs_branch_id_column(&property.identity)
        && analysis
            .output_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME))
    {
        return Err(format!(
            "Iceberg MV output column name {BRANCH_ID_COLUMN_NAME} is reserved for internal branch id"
        ));
    }
    let mut columns =
        create_target_columns_from_property(&property, &canonical_select_query, &analysis)?;
    if identity_needs_physical_apply_key_column(&property.identity) {
        columns.push(create_apply_key_table_column(&refresh_contract.apply_key)?);
    }
    let branch_id_column_name = identity_needs_branch_id_column(&property.identity).then(|| {
        columns.push(branch_id_table_column());
        BRANCH_ID_COLUMN_NAME.to_string()
    });
    let expected_apply_key_field_id = columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
        .and_then(|idx| i32::try_from(idx + 1).ok())
        .ok_or_else(|| {
            format!(
                "Iceberg MV target columns are missing apply-key column {apply_key_column_name}"
            )
        })?;
    let aggregate_state_hidden_columns = aggregate_state_hidden_columns_from_property(
        &property,
        &canonical_select_query,
        &analysis,
    )?;
    let aggregate_runtime_layout =
        representative_aggregate_layout(&property, &canonical_select_query, &analysis)?
            .map(|layout| layout.runtime_layout().clone())
            .unwrap_or_else(|| {
                novarocks_types::mv_aggregate_layout::MvAggregateRuntimeLayout::try_new(
                    "__row_id__".to_string(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
                .expect("empty aggregate layout is a valid stateless CREATE carrier")
            });
    let mut target_properties = vec![
        ("format-version".to_string(), "3".to_string()),
        ("write.row-lineage".to_string(), "true".to_string()),
        (
            APPLY_KEY_COLUMN_PROPERTY.to_string(),
            apply_key_column_name.to_string(),
        ),
        (
            APPLY_KEY_SOURCE_PROPERTY.to_string(),
            create_apply_key_source_property(&refresh_contract.apply_key).to_string(),
        ),
        (
            APPLY_KEY_FIELD_ID_PROPERTY.to_string(),
            expected_apply_key_field_id.to_string(),
        ),
    ];
    if !aggregate_state_hidden_columns.is_empty() {
        target_properties.push((
            HIDDEN_COLUMNS_PROPERTY.to_string(),
            aggregate_state_hidden_columns.join(","),
        ));
    }
    Ok(IcebergMvCreatePreparation {
        target,
        refresh_contract,
        property,
        base_refs: resolved_dependencies.base_refs,
        dependencies: resolved_dependencies.dependencies,
        create_persistence_facts,
        branch_id_column_name,
        source_field_observations,
        columns,
        aggregate_runtime_layout,
        partition_fields,
        target_properties,
        created_at_ms,
        staged: Mutex::new(None),
    })
}

/// The declared shape of an invisible staged target, from the provider's own
/// prepared field bindings.
fn staged_target_write_fields(
    staged: &crate::mv::domain::staged_create::StagedMvCreateTarget,
) -> Result<Vec<novarocks_spi::connector::ConnectorWriteFieldRequest>, MvCreateProviderError> {
    let prepared = staged.handle().document_target().ok_or_else(|| {
        engine_target_error(
            "staged MV CREATE target carries no prepared document binding".to_string(),
        )
    })?;
    prepared
        .fields()
        .iter()
        .map(|field| {
            let data_type =
                crate::mv::domain::rewrite::context::arrow_type_from_contract_signature(
                    field.type_signature(),
                )
                .map_err(engine_target_error)?;
            Ok(novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                arrow::datatypes::Field::new(field.name(), data_type, field.nullable()),
            ))
        })
        .collect()
}

/// Project the CREATE-time refresh configuration into its canonical document.
///
/// C is an independent document: it carries only the desired refresh policy
/// and never a publication or schema fact.
fn create_configuration_document(
    refresh: &novarocks_mv_application::repository::InitialMvRefreshConfiguration,
) -> Result<novarocks_mv_application::persistence::codec::ConfigurationDocument, String> {
    use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};

    fn non_negative(value: Option<i64>, what: &str) -> Result<Option<u64>, String> {
        value
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| format!("MV CREATE {what} must not be negative, got {value}"))
            })
            .transpose()
    }

    Ok(ConfigurationDocument {
        refresh_policy: match refresh.policy {
            MvDesiredRefreshPolicy::Manual => RefreshPolicy::Manual,
            MvDesiredRefreshPolicy::AsyncOnChange => RefreshPolicy::AsyncOnChange,
            MvDesiredRefreshPolicy::AsyncInterval => RefreshPolicy::AsyncInterval,
        },
        paused: refresh.paused,
        refresh_interval_ms: non_negative(refresh.interval_ms, "refresh interval")?,
        max_staleness_ms: non_negative(refresh.max_staleness_ms, "max staleness")?,
    })
}

/// Build this CREATE's canonical D/L/C from the facts frozen during
/// preparation and the provider's own prepared staged target.
///
/// The target facts come only from the staged-create handle. A post-create
/// reload is deliberately not accepted here: it would describe a generation
/// this statement did not stage.
fn build_create_documents_for_prepared_target(
    prepared: &IcebergMvCreatePreparation,
    seed: &crate::mv::domain::application::MvCreateProjectionSeed,
    prepared_target: &novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget,
) -> Result<novarocks_mv_application::persistence::create_documents::MvCreateDocuments, String> {
    let created_at_ms = u64::try_from(prepared.created_at_ms).map_err(|_| {
        format!(
            "MV CREATE time must be a non-negative epoch millisecond, got {}",
            prepared.created_at_ms
        )
    })?;
    novarocks_mv_application::persistence::create_documents::build_mv_create_documents(
        novarocks_mv_application::persistence::create_documents::MvCreateDocumentFacts {
            created_at_ms,
            query_definition: seed.definition.query_definition.clone(),
            sql_facts: &prepared.create_persistence_facts,
            runtime_layout: &prepared.aggregate_runtime_layout,
            source_observations: &prepared.source_field_observations,
            prepared_target,
            target_identity: &prepared.property.identity,
            apply_key_column_name: prepared.refresh_contract.apply_key.column_name,
            branch_column_name: prepared.branch_id_column_name.as_deref(),
            configuration: create_configuration_document(&seed.refresh)?,
        },
    )
}

fn ensure_mv_create_target_absent_with_ports(
    ports: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    ensure_mv_create_target_absent_with_connector_control(
        ports.connector_control.as_ref(),
        target,
        connector_context,
    )
}

fn ensure_mv_create_target_absent_with_connector_control(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &target.catalog)?;
    if lease.binding().descriptor().provider_id.as_str() != "iceberg" {
        return Err(
            "storage_engine='iceberg' requires current catalog to be an Iceberg catalog"
                .to_string(),
        );
    }
    let instance_id = lease.binding().descriptor().instance_id.clone();
    match lease
        .binding()
        .metadata()
        .load_table(novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context: connector_context.clone(),
        }) {
        Ok(_) => Err(format!(
            "Iceberg MV target table {}.{}.{} already exists",
            target.catalog, target.namespace, target.table
        )),
        Err(error) if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound => {
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

fn base_table_descriptor_from_observation(
    observation: &MvSchemaValidationObservation,
) -> BaseTableDescriptor {
    BaseTableDescriptor {
        format_version: if observation.is_format_v3() { 3 } else { 2 },
        columns: observation
            .fields()
            .iter()
            .map(|field| BaseColumnDescriptor {
                name: field.name.clone(),
                data_type: DataType::Null,
                sql_type: observed_iceberg_type_sql_head(&field.type_signature),
                nullable: field.nullable,
            })
            .collect(),
    }
}

fn observed_iceberg_type_sql_head(type_signature: &str) -> String {
    let lower = type_signature.trim().to_ascii_lowercase();
    let head = lower.split(['(', '<']).next().unwrap_or("").trim();
    match head {
        "long" => "BIGINT".to_string(),
        "int" => "INT".to_string(),
        "string" => "STRING".to_string(),
        "decimal" => "DECIMAL".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" | "timestamptz" => "DATETIME".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

/// Validate the resolved base-ref set for an aggregate-over-UNION-ALL fan-in MV.
///
/// The legacy invariant compared the classifier's `fan_in_bases` against the
/// analyzer-resolved `base_refs` and required exact equality. With the shape
/// retired, the resolved `base_refs` ARE the fan-in base set (the analyzer
/// resolved the union branches), so the "fan_in == resolved" comparison is
/// trivially satisfied by construction. The only remaining invariant to enforce
/// is the one the legacy check also enforced independently: the resolved base
/// refs must be distinct (a duplicate fan-in base is not supported in this
/// build). Each resolved base is further checked against the persisted schema
/// contract by `validate_aggregate_schema_contract_for_base`.
fn validate_aggregate_fan_in_base_refs(base_refs: &[TableIdentity]) -> Result<(), String> {
    let mut resolved_refs = BTreeSet::new();
    for base in base_refs {
        let fqn = base.fqn().to_ascii_lowercase();
        if !resolved_refs.insert(fqn.clone()) {
            return Err(format!(
                "aggregate-over-UNION-ALL MV duplicate resolved base ref {fqn} is not supported in this build"
            ));
        }
    }
    Ok(())
}

/// Validate the resolved base-ref set for a branch UNION ALL aggregate MV.
///
/// The legacy invariant (one distinct base per branch, branch_count ==
/// base_ref count, no fan-in branches) is incompatible with composed branch
/// unions: under the CREATE-time homogeneity gate every branch references the
/// SAME (possibly multi-table) base set, so the resolved base refs are exactly
/// that shared distinct set — not one-per-branch. The branch homogeneity itself
/// (same distinct base set / join structure / fan-in arity / group-key layout
/// across branches) is enforced at CREATE in `derive_from_set_operation`, and
/// every resolved base is independently checked against the persisted schema
/// contract by `validate_aggregate_schema_contract_for_base`. The only remaining
/// invariant to enforce here is that the resolved base refs are distinct: the
/// branch base set is a set, so a duplicate resolved ref would mean the resolved
/// refs and the branch base set cannot be in 1:1 correspondence.
fn validate_branch_union_aggregate_base_refs(base_refs: &[TableIdentity]) -> Result<(), String> {
    let mut resolved_refs = BTreeSet::new();
    for base_ref in base_refs {
        let fqn = base_ref.fqn().to_ascii_lowercase();
        if !resolved_refs.insert(fqn.clone()) {
            return Err(format!(
                "branch UNION ALL aggregate MV duplicate resolved base ref {fqn} is not supported in this build"
            ));
        }
    }
    Ok(())
}

/// Validate a branch UNION ALL aggregate definition against one exact target
/// observation. Branch count, inner apply-key kind and the branch
/// discriminator all come from L; there is no separate persisted contract
/// version to compare.
fn validate_branch_union_contract(
    target: &IcebergMvTarget,
    projection: &StoredMvProjection,
    query_branch_count: usize,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let interpretation = projection.facts.interpretation();
    if interpretation.aggregates.is_empty() {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} has no aggregate interpretation; recreate the MV",
            target.catalog, target.namespace, target.table
        ));
    }
    if interpretation.branches.len() != query_branch_count {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} interpretation has {} branches, query has {}",
            target.catalog,
            target.namespace,
            target.table,
            interpretation.branches.len(),
            query_branch_count
        ));
    }
    if interpretation.apply_key.kind != ApplyKeyKind::GroupRowId {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} must use GroupRowId inner apply keys",
            target.catalog, target.namespace, target.table
        ));
    }
    validate_branch_id_field(projection, target_observation).map_err(|error| {
        format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch binding is invalid: {error}",
            target.catalog, target.namespace, target.table
        )
    })
}

/// Validate one D relation occurrence of a UNION ALL projection/filter MV
/// against its exact source observation and the exact target generation.
///
/// `branch_count` is the query's own branch count; L's branch interpretations
/// are the persisted counterpart, so a mismatch is still an error.
fn validate_union_projection_schema_contract_for_base(
    iceberg_target: &IcebergMvTarget,
    projection: &StoredMvProjection,
    branch_count: usize,
    occurrence: &RelationOccurrence,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let interpretation = projection.facts.interpretation();
    if interpretation.branches.len() != branch_count {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} interpretation has {} branches, query has {}",
            iceberg_target.catalog,
            iceberg_target.namespace,
            iceberg_target.table,
            interpretation.branches.len(),
            branch_count
        ));
    }
    if interpretation.apply_key.kind != ApplyKeyKind::BaseRowId {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} must use BaseRowId inner apply keys",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ));
    }
    validate_branch_id_field(projection, target_observation).map_err(|error| {
        format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} branch binding is invalid: {error}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )
    })?;
    let renames =
        validate_schema_contract(projection, occurrence, base_observation, target_observation)?;
    if renames.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} requires schema rebind, which is not supported for UNION ALL refresh; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ))
    }
}
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn union_branch_inner_apply_key(
    branch_kind: UnionBranchKind,
) -> SqlMvApplyKeySourceFacts {
    match branch_kind {
        UnionBranchKind::Aggregate => SqlMvApplyKeySourceFacts::GroupRowId,
        UnionBranchKind::ProjectionFilter => SqlMvApplyKeySourceFacts::BaseRowId,
    }
}

/// Visible target columns for a new Iceberg MV, driven by the synthesized
/// identity. Stateless identities (projection/filter, join, and their UNION
/// ALL) keep the analyzed visible output columns verbatim; aggregate identities
/// derive their physical (state-shaped) layout from the representative
/// aggregate sub-query.
fn create_target_columns_from_property(
    property: &RefreshFragmentProperty,
    canonical_query: &ast::Query,
    analysis: &MvAnalysis,
) -> Result<Vec<TableColumnDef>, String> {
    match representative_aggregate_layout(property, canonical_query, analysis)? {
        None => analysis
            .output_columns
            .iter()
            .map(output_column_to_table_column)
            .collect::<Result<Vec<_>, _>>(),
        Some(layout) => iceberg_aggregate_target_columns_from_layout(&layout),
    }
}

/// Hidden aggregate-state column names for a new Iceberg MV (empty for
/// non-aggregate identities), driven by the synthesized identity.
fn aggregate_state_hidden_columns_from_property(
    property: &RefreshFragmentProperty,
    canonical_query: &ast::Query,
    analysis: &MvAnalysis,
) -> Result<Vec<String>, String> {
    let Some(layout) = representative_aggregate_layout(property, canonical_query, analysis)? else {
        return Ok(Vec::new());
    };
    Ok(layout
        .runtime_layout()
        .state_columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect())
}

/// The aggregate physical layout used for target-schema generation, or `None`
/// when the property's identity carries no aggregate state (projection/filter,
/// join, or their UNION ALL).
///
/// For a non-branch aggregate (`GroupRowId`) the layout is built from the whole
/// query. For a branch-union aggregate (`BranchScoped(GroupRowId)`) it is built
/// from the *first* branch — matching the legacy single-representative target
/// layout. The first branch may itself be a simple aggregate, an aggregate over
/// a join, or a fan-in aggregate; the FROM-agnostic `extract_aggregate_sql_calls`
/// extractor yields the right aggregate-call surface in every case.
fn representative_aggregate_layout(
    property: &RefreshFragmentProperty,
    canonical_query: &ast::Query,
    analysis: &MvAnalysis,
) -> Result<
    Option<novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout>,
    String,
> {
    match inner_row_identity(&property.identity) {
        TargetIdentity::BaseRowId | TargetIdentity::JoinRowKey(_, _) => Ok(None),
        TargetIdentity::GroupRowId(_) => {
            let scope = if matches!(property.identity, TargetIdentity::BranchScoped(_)) {
                SqlMvAggregateLayoutScope::FirstUnionBranch
            } else {
                SqlMvAggregateLayoutScope::WholeQuery
            };
            let facts = analysis
                .refresh_input
                .aggregate_layout_facts(canonical_query, scope)?;
            novarocks_sql::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(
                &facts,
            )
            .map(Some)
        }
        // `inner_row_identity` already peeled the branch wrapper; a nested
        // `BranchScoped` cannot occur (construction flattens it).
        TargetIdentity::BranchScoped(_) => Err(
            "Iceberg MV target layout internal error: unflattened branch-scoped identity"
                .to_string(),
        ),
    }
}

/// Observe neutral schema fields for every base, keyed by table FQN.
fn observe_base_fields_for_refs_with_ports(
    ports: &IcebergMvCorePorts,
    base_refs: &[TableIdentity],
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    std::collections::BTreeMap<
        String,
        crate::mv::domain::storage_observation::MvSchemaValidationObservation,
    >,
    String,
> {
    let mut observed = std::collections::BTreeMap::new();
    for base_ref in base_refs {
        let observation = observe_schema_validation_for_table(
            ports.connector_control.as_ref(),
            ports.storage_observation.as_ref(),
            base_ref,
            connector_context,
        )?;
        observed.insert(base_ref.fqn(), observation);
    }
    Ok(observed)
}

fn create_apply_key_source_property(apply_key: &ApplyKeyContract) -> &'static str {
    mv_apply_key_source_from_column_name(apply_key.column_name)
        .expect("known Iceberg MV apply-key column")
        .table_property_value()
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn refresh_policy_descriptor_json(
    policy: &MvCreateRefreshPolicy,
    paused: bool,
) -> serde_json::Value {
    match policy {
        MvCreateRefreshPolicy::Manual => serde_json::json!({
            "policy": "DEFERRED_MANUAL",
            "interval_ms": null,
            "paused": paused,
        }),
        MvCreateRefreshPolicy::AsyncOnChange => {
            serde_json::json!({
                "policy": "ASYNC_ON_CHANGE",
                "interval_ms": null,
                "paused": paused,
            })
        }
        MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            serde_json::json!({
                "policy": "ASYNC_INTERVAL",
                "interval_ms": interval_ms,
                "paused": paused,
            })
        }
    }
}

/// Refresh desired configuration is owned by the canonical configuration
/// document (C), not by a target-table descriptor property.
///
/// This path used to read-modify-write a legacy `MvDescriptorV3`: it copied the
/// retired numeric schema contract straight back out of the projection and
/// republished it beside a freshly encoded refresh block. Both halves are now
/// gone: the projection carries D/L/P/C only, and the CREATE-side descriptor
/// builder was deleted on purpose, so rebuilding a descriptor here would
/// reintroduce exactly the retired mapping. No canonical writer for C is wired
/// into this path yet, so `ALTER MATERIALIZED VIEW ... SET REFRESH`,
/// `PAUSE REFRESH` and `RESUME REFRESH` fail closed instead of degrading.
pub fn sync_iceberg_mv_descriptor_with_ports(
    _ports: &IcebergMvCorePorts,
    definition: &StoredMvProjection,
    _refresh_policy: &MvDesiredRefreshPolicy,
    _refresh_paused: bool,
    _refresh_interval_ms: Option<i64>,
    _expected_committed_partitioning: Option<
        novarocks_spi::connector::ConnectorCommittedPartitioning,
    >,
    _connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let target = definition.facts.target();
    Err(format!(
        "Iceberg MV {}.{}.{} refresh configuration is owned by its canonical configuration \
         document; the retired descriptor property writer was removed and no canonical \
         configuration writer is wired into this path",
        target.catalog().unwrap_or_default(),
        target.namespace(),
        target.name(),
    ))
}

/// Re-observe one target after a lake mutation and replace its Accelerator
/// projection from those authoritative facts.  This is intentionally a
/// post-commit operation: no caller may manufacture desired state from its
/// statement inputs after the provider has accepted a mutation.
pub fn reobserve_and_project_iceberg_mv_with_ports(
    ports: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    let lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
        ports.connector_control.as_ref(),
        &instance_id,
    )
    .map_err(|error| error.to_string())?;
    let table = novarocks_spi::connector::ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let metadata = lease
        .binding()
        .metadata()
        .load_table(novarocks_spi::connector::ConnectorTableRequest {
            table,
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context: connector_context.clone(),
        })
        .map_err(|error| error.to_string())?;
    let package = crate::mv::domain::storage_observation::observe_lake_package(
        ports.storage_observation.as_ref(),
        &lease,
        &metadata,
        connector_context,
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "Iceberg MV target is missing its lake descriptor after mutation".to_string())?;
    // The readiness port is the one place that adapts the async projection
    // store for this synchronous statement thread, and it is also what marks
    // the target consumable again once the post-commit observation lands.
    ports
        .readiness
        .project_observed(uuid::Uuid::now_v7(), &package)
        .map_err(|error| error.to_string())
}

/// Peel any top-level `BranchScoped` wrapper, returning the per-row inner
/// identity. `BranchScoped` construction already flattens nesting, so a single
/// peel is sufficient.
fn inner_row_identity(identity: &TargetIdentity) -> &TargetIdentity {
    match identity {
        TargetIdentity::BranchScoped(inner) => inner.as_ref(),
        other => other,
    }
}

/// A physical apply-key column is materialized iff each output row is
/// identified by a base or join row id (`BaseRowId` / `JoinRowKey`), whose
/// apply key is stored as a real target column. Group-row identities
/// (`GroupRowId`) derive their apply key from the group keys, so no physical
/// column is added. The `BranchScoped` wrapper is transparent here — what
/// matters is the per-row inner identity. This reproduces the legacy
/// strategy-based gating (ProjectionFilter / JoinProjectionFilter /
/// UnionProjectionFilter required the column; the aggregate strategies did
/// not).
fn identity_needs_physical_apply_key_column(identity: &TargetIdentity) -> bool {
    matches!(
        inner_row_identity(identity),
        TargetIdentity::BaseRowId | TargetIdentity::JoinRowKey(_, _)
    )
}

/// A `__branch_id__` discriminant column is materialized iff the output is a
/// UNION ALL (the identity top is `BranchScoped`). Reproduces the legacy gating
/// (UnionProjectionFilter / BranchUnionAggregate required it).
fn identity_needs_branch_id_column(identity: &TargetIdentity) -> bool {
    matches!(identity, TargetIdentity::BranchScoped(_))
}

fn create_apply_key_table_column(apply_key: &ApplyKeyContract) -> Result<TableColumnDef, String> {
    match apply_key.column_name {
        HIDDEN_APPLY_KEY_COLUMN_NAME => Ok(apply_key_table_column()),
        JOIN_APPLY_KEY_COLUMN_NAME => Ok(join_apply_key_table_column()),
        other => Err(format!(
            "Iceberg MV refresh contract apply-key column {other} is not a physical target apply-key column"
        )),
    }
}

fn base_snapshot_status_for_refresh(
    base_ref: &TableIdentity,
    previous_snapshot_id: Option<i64>,
    current_snapshot_id_before_pin: Option<i64>,
) -> BaseSnapshotStatus {
    BaseSnapshotStatus::new(
        base_ref.fqn(),
        previous_snapshot_id,
        current_snapshot_id_before_pin,
    )
}

fn iceberg_aggregate_target_columns_from_layout(
    layout: &novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout,
) -> Result<Vec<TableColumnDef>, String> {
    novarocks_sql::planning::mv_aggregate_layout::validate_unique_aggregate_physical_column_names(
        layout.physical_columns(),
    )?;
    Ok(layout
        .physical_columns()
        .iter()
        .map(|column| column.column().clone())
        .collect())
}

/// Whether an aggregate query's FROM clause is a fan-in UNION ALL subquery.
///
/// FROM-side complement to [`extract_aggregate_sql_calls`] for distinguishing a
/// fan-in aggregate from a single-scan aggregate. A fan-in FROM is exactly one
/// relation, no joins, a non-lateral derived subquery whose body is a `UNION ALL`
/// set operation.
fn from_clause_is_fan_in_union(query: &ast::Query) -> bool {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    if !from.joins.is_empty() {
        return false;
    }
    let ast::TableFactor::Derived {
        lateral, subquery, ..
    } = &from.relation
    else {
        return false;
    };
    if *lateral {
        return false;
    }
    matches!(subquery.body.as_ref(), ast::SetExpr::SetOperation(_))
}

fn validate_composed_aggregate_fallback_query(query: &ast::Query) -> Result<(), String> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("composed aggregate fallback requires a plain SELECT body".to_string());
    };
    if select.from.len() != 1 {
        return Err(
            "composed aggregate fallback requires a single direct FROM join tree".to_string(),
        );
    }
    let from = &select.from[0];
    if from.joins.is_empty() {
        return Err(
            "composed aggregate fallback requires the aggregate input to be a direct join tree"
                .to_string(),
        );
    }
    validate_composed_aggregate_table_factor(&from.relation)?;
    for join in &from.joins {
        validate_composed_aggregate_join(&join.operator, &join.constraint)?;
        validate_composed_aggregate_table_factor(&join.relation)?;
    }
    Ok(())
}

fn validate_composed_aggregate_table_factor(factor: &ast::TableFactor) -> Result<(), String> {
    match factor {
        ast::TableFactor::Table { .. } => Ok(()),
        ast::TableFactor::NestedJoin { table_with_joins, .. } => {
            validate_composed_aggregate_table_factor(&table_with_joins.relation)?;
            if table_with_joins.joins.is_empty() {
                return Err(
                    "composed aggregate fallback nested join must contain at least one join"
                        .to_string(),
                );
            }
            for join in &table_with_joins.joins {
                validate_composed_aggregate_join(&join.operator, &join.constraint)?;
                validate_composed_aggregate_table_factor(&join.relation)?;
            }
            Ok(())
        }
        _ => Err(
            "composed aggregate fallback supports only direct base-table joins, not subqueries or table functions"
                .to_string(),
        ),
    }
}

fn validate_composed_aggregate_join(
    operator: &ast::JoinOperator,
    constraint: &ast::JoinConstraint,
) -> Result<(), String> {
    match (operator, constraint) {
        (ast::JoinOperator::Inner | ast::JoinOperator::InnerExplicit, ast::JoinConstraint::On(_)) => Ok(()),
        (ast::JoinOperator::Cross, ast::JoinConstraint::None) => Ok(()),
        _ => Err(
            "composed aggregate fallback supports only direct INNER JOIN ... ON predicates or CROSS JOIN"
                .to_string(),
        ),
    }
}

/// Number of UNION ALL branches in `query`, counted off the AST so the build
/// does not depend on a top-level classified shape.
fn union_branch_count(query: &ast::Query) -> u32 {
    fn count(body: &ast::SetExpr) -> u32 {
        match body {
            ast::SetExpr::SetOperation(operation) => {
                count(&operation.left) + count(&operation.right)
            }
            ast::SetExpr::Query(inner) => count(inner.body.as_ref()),
            _ => 1,
        }
    }
    count(query.body.as_ref())
}

/// Neutral schema observation for `base_ref`.
///
/// Fails closed: a base the caller did not observe is a programming error, not
/// a reason to fall back to reading provider metadata.
fn observed_base<'a>(
    base_field_observations: &'a std::collections::BTreeMap<
        String,
        crate::mv::domain::storage_observation::MvSchemaValidationObservation,
    >,
    base_ref: &TableIdentity,
) -> Result<&'a crate::mv::domain::storage_observation::MvSchemaValidationObservation, String> {
    base_field_observations.get(&base_ref.fqn()).ok_or_else(|| {
        format!(
            "MV base {} was not observed before contract build",
            base_ref.fqn()
        )
    })
}

/// The state an Iceberg MV target restore reads, named explicitly rather than
/// reached through aggregate engine state.
///
/// Same shape and motive as the lake rebuild's context: two inputs, both already
/// reachable from a frontend composition, so restoring targets stops requiring
/// the engine.
pub struct MvTargetRestoreContext<'a> {
    pub connector_control: &'a dyn novarocks_spi::connector::ConnectorControlRegistry,
    pub readiness: &'a MvReadinessPort,
}

pub(crate) fn register_iceberg_mv_target_in_catalog(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    target: &IcebergMvTarget,
) -> Result<(), String> {
    // SQLX-2 keeps provider tables out of the process-wide planner catalog.
    // Every subsequent query resolves this target through its own admitted
    // binding store. Confirm the catalog's exact generation is published;
    // provider commits own their cache invalidation. Registering or mutating a
    // concrete Core catalog entry here would create a second runtime owner.
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("parse MV target connector identity: {error}"))?;
    novarocks_spi::connector::ConnectorControlResolver::acquire_current(
        connector_control,
        &instance_id,
    )
    .map_err(|error| format!("acquire MV target connector generation: {error}"))?;
    Ok(())
}

pub fn restore_iceberg_mv_targets(ctx: &MvTargetRestoreContext<'_>) -> Result<(), String> {
    // Restore every MV this process holds a projection for, not only the ones
    // it may manage. A projection rebuilt from the lake is a query candidate
    // immediately and stays closed to management until its readmission
    // completes; consuming the management-ready inventory here would leave a
    // restarted process unable to confirm the generation of its own MVs until
    // an operator intervened.
    let projections = match ctx
        .readiness
        .candidate_reader()
        .list_candidate_definitions()
    {
        Ok(projections) => projections,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "skipping Iceberg MV target restore because the Accelerator is unavailable"
            );
            return Ok(());
        }
    };
    for projection in projections {
        let source = projection.facts.source_revision();
        let target = IcebergMvTarget {
            catalog: source.target.instance_id.as_str().to_string(),
            namespace: source.target.namespace.to_string(),
            table: source.target.table.to_string(),
        };
        if let Err(error) = register_iceberg_mv_target_in_catalog(ctx.connector_control, &target) {
            tracing::warn!(
                mv_id = projection.mv_id,
                target = %format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                error = %error,
                "skipping failed Iceberg MV target registration during startup restore"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::refresh::apply_key::ApplyKeyValueType;
    use crate::mv::domain::refresh::capabilities::PartitionPruningPolicy;

    #[test]
    fn aggregate_incremental_inserts_use_row_delta() {
        assert!(matches!(
            non_join_incremental_write_mode(true, false),
            MvIncrementalWriteMode::RowDelta
        ));
        assert!(matches!(
            non_join_incremental_write_mode(false, false),
            MvIncrementalWriteMode::FastAppend
        ));
        assert!(matches!(
            non_join_incremental_write_mode(false, true),
            MvIncrementalWriteMode::RowDelta
        ));
    }
    #[test]
    fn explain_refresh_full_guard_rejects_full_with_disabled_message() {
        let err = super::explain_refresh_full_guard(true).unwrap_err();
        assert!(
            err.contains(concat!("currently disabled", " pending redesign")),
            "EXPLAIN REFRESH FULL must align with the exec-side disabled message, got: {err}"
        );
        assert!(
            !err.contains("not supported"),
            "stale 'not supported' wording must be gone: {err}"
        );
        assert!(super::explain_refresh_full_guard(false).is_ok());
    }

    #[test]
    fn imv_change_stream_effect_set_can_include_zero_row_route() {
        let effects = [
            novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
        ];
        assert!(effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Delete));
        assert!(effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Replace));
    }

    /// D's own effective SQL, not the order the caller happens to pass base
    /// refs in, decides which relation is the join's left side. The retired
    /// persisted join lineage is gone; the two-relation gate now comes from D's
    /// occurrence inventory.
    #[test]
    fn join_base_refs_for_definition_uses_the_definition_join_order() {
        let left = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "fact".to_string(),
        };
        let right = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "dim".to_string(),
        };
        let base_refs = vec![right.clone(), left.clone()];
        let projection = test_two_relation_projection(&left, &right);
        let query = parse_select_query(
            "SELECT l.order_id FROM ice.sales.fact l JOIN ice.sales.dim r ON l.order_id = r.order_id",
        );

        let (actual_left, actual_right) =
            join_base_refs_for_definition(&projection, &query, &base_refs)
                .expect("definition join base refs");

        assert_eq!(actual_left.fqn(), left.fqn());
        assert_eq!(actual_right.fqn(), right.fqn());
    }

    /// A self-join has two D occurrences of one relation. This locator-keyed
    /// path cannot tell them apart, so it must refuse instead of picking one.
    #[test]
    fn join_base_refs_for_definition_rejects_a_self_join() {
        let table = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "fact".to_string(),
        };
        let projection = test_two_relation_projection(&table, &table);
        let query = parse_select_query(
            "SELECT l.order_id FROM ice.sales.fact l JOIN ice.sales.fact r ON l.order_id = r.order_id",
        );

        let error = join_base_refs_for_definition(&projection, &query, &[table])
            .expect_err("a self join has no distinct left and right locator");

        assert!(error.contains("identical left/right relations"), "{error}");
    }

    /// Two canonical D occurrences bound to the given locators. Every other
    /// fact (outputs, interpretation, target binding) stays at the shared
    /// fixture so only the relation shape under test varies.
    fn test_two_relation_projection(
        left: &TableIdentity,
        right: &TableIdentity,
    ) -> StoredMvProjection {
        let mut fixture =
            novarocks_mv_application::persistence::test_support::ProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
                None,
            );
        for (occurrence, table) in fixture
            .definition
            .relation_occurrences
            .iter_mut()
            .zip([left, right])
        {
            occurrence.catalog_at_binding.clone_from(&table.catalog);
            occurrence.namespace_at_binding.clone_from(&table.namespace);
            occurrence.relation_at_binding.clone_from(&table.table);
        }
        StoredMvProjection {
            mv_id: 1,
            facts: fixture.build().expect("two-relation fixture projection"),
        }
    }

    fn parse_select_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(q)] = statements.as_slice() else {
            panic!("expected SELECT");
        };
        q.clone()
    }

    #[test]
    fn iceberg_join_mv_uses_join_apply_key_column() {
        let column = crate::mv::domain::refresh::target_apply::join_apply_key_table_column();
        assert_eq!(column.name, JOIN_APPLY_KEY_COLUMN_NAME);
    }

    #[test]
    fn create_apply_key_metadata_comes_from_refresh_contract() {
        use crate::mv::domain::refresh::apply_key::ApplyKeyContract;

        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::projection_filter()),
            SqlMvApplyKeySourceFacts::BaseRowId.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::join_projection_filter()),
            SqlMvApplyKeySourceFacts::JoinRowKey.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::aggregate_group_row()),
            SqlMvApplyKeySourceFacts::GroupRowId.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::join_aggregate_group_row()),
            SqlMvApplyKeySourceFacts::GroupRowId.table_property_value()
        );
    }

    #[test]
    fn repartition_support_accepts_projection_filter_and_aggregate() {
        let projection = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::SingleBase,
            has_agg_state: false,
            identity: RefreshIdentity::BaseRowId,
            apply_key_column: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Int64,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&projection).expect("projection/filter support"),
            RepartitionShape::ProjectionFilterSingleBase
        );

        let aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::SingleBase,
            has_agg_state: true,
            identity: RefreshIdentity::GroupRowId,
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&aggregate).expect("aggregate support"),
            RepartitionShape::AggregateSingleBase
        );
    }

    #[test]
    fn repartition_support_accepts_join_projection_filter() {
        let join = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            has_agg_state: false,
            identity: RefreshIdentity::JoinRowKey,
            apply_key_column: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&join).expect("join projection/filter support"),
            RepartitionShape::JoinProjectionFilter
        );
    }

    #[test]
    fn repartition_support_accepts_multi_base_shapes() {
        let join_aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            has_agg_state: true,
            identity: RefreshIdentity::GroupRowId,
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&join_aggregate).expect("join aggregate support"),
            RepartitionShape::JoinAggregate
        );

        let fan_in_aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: true,
            identity: RefreshIdentity::GroupRowId,
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&fan_in_aggregate).expect("fan-in aggregate support"),
            RepartitionShape::FanInAggregate
        );

        let union_projection = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: false,
            identity: RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::BaseRowId)),
            apply_key_column: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::BranchInt64,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&union_projection).expect("union projection support"),
            RepartitionShape::UnionProjectionFilter
        );
    }

    #[test]
    fn repartition_support_rejects_specific_unsupported_shape() {
        let invalid = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: false,
            identity: RefreshIdentity::JoinRowKey,
            apply_key_column: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };

        let err = select_repartition_shape(&invalid).expect_err("shape must be rejected");
        assert!(err.contains("UnsupportedRepartitionShape"));
        assert!(err.contains("JoinRowKey"));
        assert!(err.contains("AllBasesRequired"));
        assert!(err.contains("aggregate_state=false"));

        let branch_union_aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: true,
            identity: RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId)),
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::BranchUtf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        let err = select_repartition_shape(&branch_union_aggregate)
            .expect_err("branch UNION ALL aggregate repartition is unsupported");
        assert!(err.contains("UnsupportedRepartitionShape"));
        assert!(err.contains("BranchScoped"));
        assert!(err.contains("aggregate_state=true"));
    }

    #[test]
    fn identity_gating_matches_legacy_strategy_gating() {
        use crate::mv::domain::analysis::refresh_property::TargetIdentity;

        let base_row = TargetIdentity::BaseRowId;
        let join_row = TargetIdentity::JoinRowKey(
            Box::new(TargetIdentity::BaseRowId),
            Box::new(TargetIdentity::BaseRowId),
        );
        let group_row = TargetIdentity::GroupRowId(vec!["region".to_string()]);
        let union_proj = TargetIdentity::BranchScoped(Box::new(TargetIdentity::BaseRowId));
        let union_agg = TargetIdentity::BranchScoped(Box::new(group_row.clone()));

        // Physical apply-key column: required for base/join row identities
        // (ProjectionFilter / JoinProjectionFilter / UnionProjectionFilter),
        // not for group-row identities (the aggregate strategies).
        assert!(identity_needs_physical_apply_key_column(&base_row));
        assert!(identity_needs_physical_apply_key_column(&join_row));
        assert!(identity_needs_physical_apply_key_column(&union_proj));
        assert!(!identity_needs_physical_apply_key_column(&group_row));
        assert!(!identity_needs_physical_apply_key_column(&union_agg));

        // Branch id column: required iff the identity top is BranchScoped.
        assert!(!identity_needs_branch_id_column(&base_row));
        assert!(!identity_needs_branch_id_column(&join_row));
        assert!(!identity_needs_branch_id_column(&group_row));
        assert!(identity_needs_branch_id_column(&union_proj));
        assert!(identity_needs_branch_id_column(&union_agg));
    }

    #[test]
    fn refresh_status_uses_base_ref_fqn() {
        let base_ref = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        };

        let status = base_snapshot_status_for_refresh(&base_ref, Some(10), Some(11));

        assert_eq!(status.fqn, "ice.sales.orders");
        assert_eq!(status.previous_snapshot_id, Some(10));
        assert_eq!(status.current_snapshot_id_before_pin, Some(11));
    }
}

// A-family aggregate-union execution is orchestrated by the shared fan-in
// refresh path below. It pins every base, builds one refresh context, and
// drives the aggregate merge with one canonical change per base.

/// The per-shape payload distinguishing the two `AllBasesRequired` aggregate
/// refresh variants that share the wrapper [`refresh_fan_in_aggregate_iceberg_mv`].
///
/// This enum is the *identity gate* the folded wrapper dispatches on: the
/// `BranchUnion` variant corresponds to a `BranchScoped` row identity (UNION
/// ALL of aggregate branches), while `FanIn` corresponds to a plain
/// `GroupRowId` aggregate fanning in over a UNION ALL of scans. Both produce an
/// `AllBasesRequired` snapshot policy and an aggregate state contract; only the
/// branch-contract validation and the first-refresh strategy differ.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
enum AllBasesAggregateRefresh<'a> {
    /// Aggregate-over-UNION-ALL fan-in: one aggregate above a union of scans.
    /// The aggregate-call surface is sourced from the focused extractor
    /// (`extract_aggregate_sql_calls`), not the legacy classifier.
    FanIn {
        schema_contract: &'a mv_schema::MvSchemaContract,
        aggregate_calls: &'a SqlMvAggregateCalls,
    },
    /// Aggregate over a composed multi-base relation, such as a nested join or
    /// a zero-key CROSS JOIN. The change stream still uses the aggregate
    /// rewrite-merge path; the apply-key evidence decides whether join-delta
    /// proof is required.
    ComposedAggregate {
        schema_contract: &'a mv_schema::MvSchemaContract,
        aggregate_calls: &'a SqlMvAggregateCalls,
    },
    /// UNION ALL of aggregate branches (`BranchScoped` identity): the union sits
    /// above per-branch aggregates and the first refresh injects `__branch_id__`.
    /// The per-branch aggregate-call model is sourced from the focused extractor
    /// (not the legacy classifier), so a composed branch (`Agg(a JOIN b)` /
    /// `Agg(fan-in)`) is supported. `branch_count` is the persisted branch count;
    /// `first_branch_calls` is the first branch's aggregate-call surface, which is
    /// representative of every branch under the CREATE-time homogeneity gate.
    BranchUnion {
        branch_count: usize,
        first_branch_calls: &'a SqlMvAggregateCalls,
    },
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn target_fqn_string(target: &IcebergMvTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}

// Previous implementation of REFRESH FULL — `refresh_full_iceberg_mv` —
// was removed. It dropped the target table + deleted the MV definition +
// re-ran create_iceberg_mv (which leaves the new target empty), and the
// drop and the create were in separate transactions. The user-visible
// outcome was misleading ("MV is now empty" rather than "MV is fully
// repopulated") and the operation could leave behind an inconsistent
// state on partial failure. It also silently dropped partition_by.
//
// Re-introduce only after a redesign that clarifies:
//   - the keyword name (probably REBUILD rather than REFRESH FULL),
//   - atomic drop+create+populate semantics,
//   - a deterministic data-repopulation step,
//   - faithful preservation of the original DDL (partition_by,
//     distribution, properties).
// See the rejection in refresh_iceberg_mv for the user-facing error.

fn unknown_join_affected_partitions() -> crate::mv::domain::model::AffectedTargetPartitions {
    crate::mv::domain::model::AffectedTargetPartitions::not_derived(
        "join MV affected partition planning is not implemented",
    )
}

fn base_snapshot_statuses_for_plan(
    base_refs: &[TableIdentity],
    previous_snapshots: &BTreeMap<String, i64>,
    current_snapshots: &BTreeMap<String, Option<i64>>,
) -> Vec<BaseSnapshotStatus> {
    base_refs
        .iter()
        .map(|base_ref| {
            base_snapshot_status_for_refresh(
                base_ref,
                previous_snapshots.get(&base_ref.fqn()).copied(),
                current_snapshots.get(&base_ref.fqn()).copied().flatten(),
            )
        })
        .collect()
}

fn noop_affected_partitions(
    target_partition: &mv_schema::MvPartitionContract,
) -> crate::mv::domain::model::AffectedTargetPartitions {
    if is_unpartitioned_mv_target(target_partition) {
        crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
    } else {
        crate::mv::domain::model::AffectedTargetPartitions::known(std::iter::empty::<
            crate::mv::domain::model::MvPartitionKey,
        >())
    }
}

fn merge_affected_partition_results(
    context: &str,
    results: impl IntoIterator<Item = (String, crate::mv::domain::model::AffectedTargetPartitions)>,
) -> crate::mv::domain::model::AffectedTargetPartitions {
    let mut merged = BTreeSet::new();
    let mut saw_unpartitioned = false;

    for (base, result) in results {
        match result {
            crate::mv::domain::model::AffectedTargetPartitions::Known { partitions } => {
                merged.extend(partitions);
            }
            crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned => {
                saw_unpartitioned = true;
            }
            crate::mv::domain::model::AffectedTargetPartitions::NotDerived { reason } => {
                return crate::mv::domain::model::AffectedTargetPartitions::not_derived(format!(
                    "{context}: {base}: {reason}"
                ));
            }
        }
    }

    if saw_unpartitioned {
        if merged.is_empty() {
            crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
        } else {
            crate::mv::domain::model::AffectedTargetPartitions::not_derived(format!(
                "{context}: mixed unpartitioned and partitioned branch results"
            ))
        }
    } else {
        crate::mv::domain::model::AffectedTargetPartitions::known(merged)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Multi-base partition planning keeps each separately observed provider fact explicit."
)]
fn plan_multi_base_affected_partitions(
    projection: &StoredMvProjection,
    target_partition: &mv_schema::MvPartitionContract,
    mode: RefreshMode,
    base_refs: &[TableIdentity],
    previous_snapshots: &BTreeMap<String, i64>,
    current_snapshots: &BTreeMap<String, Option<i64>>,
    mut admit_for_base: impl FnMut(
        &TableIdentity,
        i64,
        i64,
    ) -> Result<
        (
            novarocks_spi::connector::ConnectorChangeWindowAdmission,
            MvSchemaValidationObservation,
        ),
        String,
    >,
    context: &str,
) -> crate::mv::domain::model::AffectedTargetPartitions {
    match mode {
        RefreshMode::Noop => noop_affected_partitions(target_partition),
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_target(target_partition) {
                crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
            } else {
                crate::mv::domain::model::AffectedTargetPartitions::not_derived(format!(
                    "{context}: full refresh affected partition planning is not implemented"
                ))
            }
        }
        RefreshMode::Incremental => {
            if is_unpartitioned_mv_target(target_partition) {
                return crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned;
            }

            let results = base_refs.iter().map(|base_ref| {
                let fqn = base_ref.fqn();
                let result = match (
                    previous_snapshots.get(&fqn).copied(),
                    current_snapshots.get(&fqn).copied().flatten(),
                ) {
                    (Some(previous), Some(current)) if previous == current => {
                        crate::mv::domain::model::AffectedTargetPartitions::known(
                            std::iter::empty::<crate::mv::domain::model::MvPartitionKey>(),
                        )
                    }
                    (Some(previous), Some(current)) => {
                        match admit_for_base(base_ref, previous, current) {
                            Ok((
                                novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly,
                                _,
                            )) => crate::mv::domain::model::AffectedTargetPartitions::known(
                                std::iter::empty::<crate::mv::domain::model::MvPartitionKey>(),
                            ),
                            Ok((
                                novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental {
                                    partition_impact,
                                    ..
                                },
                                observation,
                            )) => crate::mv::domain::partition::planner::plan_affected_partitions(
                                &crate::mv::domain::partition::planner::AffectedPartitionPlanInput {
                                    projection,
                                    partition_impact: Some(&partition_impact),
                                    schema_observation: Some(&observation),
                                },
                            ),
                            Ok((
                                novarocks_spi::connector::ConnectorChangeWindowAdmission::FullRebuild(_),
                                _,
                            )) => crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                                "connector change-window admission requires a full rebuild",
                            ),
                            Err(err) => crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                                format!("failed to admit connector changes for affected partitions: {err}"),
                            ),
                        }
                    }
                    (None, _) => crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        "incremental affected partition planning missing previous snapshot",
                    ),
                    (_, None) => crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        "incremental affected partition planning missing current snapshot",
                    ),
                };
                (fqn, result)
            });

            merge_affected_partition_results(context, results)
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Aggregate partition planning keeps each separately observed provider fact explicit."
)]
fn plan_aggregate_mv_affected_partitions(
    source: &dyn IcebergMvRefreshSource,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base_ref: &TableIdentity,
    projection: &StoredMvProjection,
    target_partition: &mv_schema::MvPartitionContract,
    mode: RefreshMode,
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> crate::mv::domain::model::AffectedTargetPartitions {
    match mode {
        RefreshMode::Noop => noop_affected_partitions(target_partition),
        RefreshMode::Incremental => {
            if is_unpartitioned_mv_target(target_partition) {
                crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
            } else {
                let Some(previous) = previous_snapshot_id else {
                    return crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        "incremental aggregate MV affected partition planning missing previous snapshot",
                    );
                };
                let Some(current) = current_snapshot_id else {
                    return crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        "incremental aggregate MV affected partition planning missing current snapshot",
                    );
                };
                match observe_and_admit_change_window_for_table(
                    source.connector_control(),
                    source.storage_observation(),
                    base_ref,
                    previous,
                    current,
                    connector_context,
                ) {
                    Ok((
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly,
                        _,
                    )) => crate::mv::domain::model::AffectedTargetPartitions::known(
                        std::iter::empty::<crate::mv::domain::model::MvPartitionKey>(),
                    ),
                    Ok((
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental {
                            partition_impact,
                            ..
                        },
                        observation,
                    )) => crate::mv::domain::partition::planner::plan_affected_partitions(
                        &crate::mv::domain::partition::planner::AffectedPartitionPlanInput {
                            projection,
                            partition_impact: Some(&partition_impact),
                            schema_observation: Some(&observation),
                        },
                    ),
                    Ok((
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::FullRebuild(_),
                        _,
                    )) => crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        "connector change-window admission requires a full rebuild",
                    ),
                    Err(err) => crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        format!("failed to admit connector changes for affected partitions: {err}"),
                    ),
                }
            }
        }
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_target(target_partition) {
                crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
            } else {
                crate::mv::domain::partition::planner::plan_affected_partitions(
                    &crate::mv::domain::partition::planner::AffectedPartitionPlanInput {
                        projection,
                        partition_impact: None,
                        schema_observation: None,
                    },
                )
            }
        }
    }
}

/// Whether the MV target is unpartitioned.
///
/// The retired schema contract carried its own copy of the target spec. The
/// canonical documents keep only the provider-opaque partition-spec version,
/// which must never be decoded, so this fact now comes from the typed
/// partition observation the exact target binding already carries.
fn is_unpartitioned_mv_target(target_partition: &mv_schema::MvPartitionContract) -> bool {
    target_partition.fields.is_empty()
}

fn log_planned_iceberg_mv_affected_partitions(
    iceberg_target: &IcebergMvTarget,
    affected_partitions: &crate::mv::domain::model::AffectedTargetPartitions,
) {
    tracing::info!(
        target = %format!(
            "{}.{}.{}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ),
        affected_partitions = ?affected_partitions,
        "planned iceberg MV affected partitions"
    );
}

pub(crate) fn build_refresh_state_baseline(
    mv_definition: &StoredMvProjection,
    target: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
) -> Result<RefreshStateBaseline, String> {
    let definition = mv_definition.facts.definition();
    let previous_sources = match mv_definition.facts.publication() {
        MvPublicationState::NeverPublished => Vec::new(),
        MvPublicationState::Published(publication) => {
            if publication.document().inputs.len() != definition.relation_occurrences.len() {
                return Err(
                    "MV publication inputs do not cover every D relation occurrence".into(),
                );
            }
            definition
                .relation_occurrences
                .iter()
                .zip(&publication.document().inputs)
                .map(|(occurrence, input)| {
                    if occurrence.occurrence_id != input.relation_occurrence_id {
                        return Err(format!(
                            "MV publication input order differs at D occurrence {}",
                            occurrence.occurrence_id,
                        ));
                    }
                    if occurrence.object_id != input.object_id {
                        return Err(format!(
                            "MV publication input object differs from D occurrence {}",
                            occurrence.occurrence_id,
                        ));
                    }
                    let semantic_revision =
                        restore_exact_query_revision(&input.object_id, &input.native_data_version)
                            .map_err(|error| {
                                format!(
                                    "restore exact MV publication input occurrence {}: {error}",
                                    occurrence.occurrence_id,
                                )
                            })?;
                    let table_object_id = ConnectorTableObjectId::try_new(
                        semantic_revision.object_identity().value().clone(),
                    )
                    .map_err(|error| {
                        format!(
                            "restore MV publication object occurrence {}: {error}",
                            occurrence.occurrence_id,
                        )
                    })?;
                    Ok(RefreshStateBaselineSource {
                        occurrence_id: novarocks_sql::compiler::SqlMvRelationOccurrenceId::new(
                            occurrence.occurrence_id,
                        ),
                        table: TableIdentity {
                            catalog: occurrence.catalog_at_binding.clone(),
                            namespace: occurrence.namespace_at_binding.clone(),
                            table: occurrence.relation_at_binding.clone(),
                        },
                        table_object_id,
                        semantic_revision,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?
        }
    };
    Ok(RefreshStateBaseline::SnapshotBacked {
        previous_sources,
        target_snapshot_id: target.current_snapshot_id(),
        target_table_uuid: target.table_uuid().to_string(),
        definition_fingerprint: mv_definition_fingerprint(&definition.query.effective_sql),
    })
}

/// D's binding-time locator for one relation occurrence.
fn occurrence_table(occurrence: &RelationOccurrence) -> TableIdentity {
    TableIdentity {
        catalog: occurrence.catalog_at_binding.clone(),
        namespace: occurrence.namespace_at_binding.clone(),
        table: occurrence.relation_at_binding.clone(),
    }
}

/// D's ordered relation occurrences, as the base locators refresh planning
/// speaks. The order is D occurrence order, which every canonical validator
/// requires. This locator-keyed planner cannot tell two occurrences of one
/// relation apart, so a repeated relation fails closed instead of collapsing
/// into a single key.
fn definition_base_refs(projection: &StoredMvProjection) -> Result<Vec<TableIdentity>, String> {
    let mut seen = BTreeSet::new();
    let mut base_refs = Vec::new();
    for occurrence in &projection.facts.definition().relation_occurrences {
        let table = occurrence_table(occurrence);
        if !seen.insert(table.fqn()) {
            return Err(format!(
                "MV definition references {} more than once; this path needs one occurrence \
                 per relation",
                table.fqn()
            ));
        }
        base_refs.push(table);
    }
    Ok(base_refs)
}

/// D's relation shape decides whether one empty source may still refresh: a
/// two-relation join pair may, a fan-in of independent relations may not.
fn definition_has_join(query: &ast::Query) -> bool {
    extract_join_aliases(query).is_ok()
}

/// D's occurrences in their own order, proven to describe exactly the base
/// locators this attempt resolved.
fn definition_occurrences_for_base_refs<'a>(
    projection: &'a StoredMvProjection,
    base_refs: &[TableIdentity],
) -> Result<Vec<&'a RelationOccurrence>, String> {
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != base_refs.len() {
        return Err("MV refresh base references do not retain every D occurrence".to_string());
    }
    for (occurrence, table) in occurrences.iter().zip(base_refs) {
        if occurrence.catalog_at_binding != table.catalog
            || occurrence.namespace_at_binding != table.namespace
            || occurrence.relation_at_binding != table.table
        {
            return Err(format!(
                "MV refresh base locator does not match D occurrence {}",
                occurrence.occurrence_id
            ));
        }
    }
    Ok(occurrences.iter().collect())
}

/// Applying a renamed source column needs the SQL owner's occurrence-aware
/// rewriter, which is not connected yet. A rename therefore fails closed here
/// exactly as it does in `refresh::observation`.
fn require_no_occurrence_rebind(renames: &[MvOccurrenceFieldRebind]) -> Result<(), String> {
    if renames.is_empty() {
        Ok(())
    } else {
        Err("MV refresh requires occurrence-aware SQL field rebinding".to_string())
    }
}

/// The locator-keyed predecessor facts this snapshot-oriented planner compares
/// against.
///
/// The keys are table names, which is the shape this planner still speaks and
/// which cannot express a self-join. That is not papered over: a baseline
/// naming one table twice is refused here rather than collapsed into a single
/// entry, and only an occurrence-keyed planning contract can lift the
/// restriction. Until then the refusal is the honest answer, and the same one
/// `unique_base_refs` reaches from the other side.
///
/// A baseline with no published predecessor yields empty maps, which is a
/// fact, not a fallback.
struct PreviousRefreshLocators {
    snapshots: BTreeMap<String, i64>,
    table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
}

fn previous_refresh_locators(
    baseline: &RefreshStateBaseline,
) -> Result<PreviousRefreshLocators, String> {
    let previous_sources = match baseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_sources, ..
        } => previous_sources.as_slice(),
        RefreshStateBaseline::Pinless => &[][..],
    };
    let predecessors =
        crate::mv::domain::refresh::planning::baseline_predecessors(previous_sources)?;
    Ok(PreviousRefreshLocators {
        snapshots: predecessors.snapshots,
        table_object_ids: predecessors.table_object_ids,
    })
}

/// Rebuild only the read-only candidate from sealed Current D/L/P/C.
///
/// This path deliberately cannot restore management readiness: refresh
/// planning does not carry the causal management observation/admission needed
/// to authorize Ready. The caller must fail closed after this succeeds and let
/// the management owner perform a separate Current readmission.
fn reconcile_published_lake_projection(
    source: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    binding: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let catalog = binding
        .lease()
        .binding()
        .catalog_handle()
        .map_err(|error| format!("bind recovery catalog generation: {error}"))?
        .clone();
    let canonical_target = novarocks_mv_application::product::MvTarget::try_new(
        Some(target.catalog.clone()),
        target.namespace.clone(),
        target.table.clone(),
    )
    .map_err(|error| format!("bind recovery MV target: {error}"))?;
    crate::mv::domain::lake_rebuild::rebuild_one_lake_package_if_missing_verified(
        source.readiness().as_ref(),
        source.connector_control(),
        catalog,
        canonical_target,
        connector_context.clone(),
    )
}

fn refresh_connector_preparation_error(error: ConnectorError) -> RefreshError {
    match error.kind() {
        ConnectorErrorKind::Unavailable
        | ConnectorErrorKind::DeadlineExceeded
        | ConnectorErrorKind::ResourceExhausted
        | ConnectorErrorKind::Cancelled => RefreshError::pre_commit(error.to_string()),
        ConnectorErrorKind::InvalidRequest
        | ConnectorErrorKind::NotFound
        | ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::Unsupported
        | ConnectorErrorKind::CorruptData
        | ConnectorErrorKind::Internal => RefreshError::user(error.to_string()),
    }
}

pub fn plan_iceberg_mv_refresh_with_connector_context(
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvRefreshRequest,
    target: MvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let iceberg_target =
        resolve_refresh_target(current_catalog, current_database, &stmt.name_parts)
            .map_err(RefreshError::user)?;
    if stmt.full {
        return Err(RefreshError::user(FULL_REFRESH_DISABLED_MESSAGE));
    }

    crate::connector::validate_request_context(connector_context)
        .map_err(RefreshError::pre_commit)?;
    // Preparation normally only observes the currently admitted catalog and
    // MV facts. A snapshot fence may rebuild sealed Current only as a
    // read-only candidate; this path has no authority to readmit management
    // readiness and therefore still fails the refresh.
    // Historical v1/v2 recovery stays in the legacy execution adapter; a
    // current frontend-owned attempt must never perform recovery before its
    // durable v3 intent exists.
    let mv_definition =
        load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &iceberg_target)
            .map_err(RefreshError::user)?;
    let target_binding = load_iceberg_mv_target_binding_typed(
        source.connector_control(),
        source.storage_observation(),
        &iceberg_target,
        connector_context,
    )
    .map_err(refresh_connector_preparation_error)?;
    if let Err(snapshot_error) =
        validate_target_snapshot(&iceberg_target, &mv_definition, &target_binding)
    {
        reconcile_published_lake_projection(
            source,
            &iceberg_target,
            &target_binding,
            connector_context,
        )
        .map_err(|recovery_error| {
            RefreshError::user(format!(
                "{snapshot_error}; sealed Current read-only recovery failed: {recovery_error}",
            ))
        })?;
        return Err(RefreshError::user(format!(
            "{snapshot_error}; sealed Current was installed as a read-only MV candidate; management Current readmission is required before refresh",
        )));
    }
    // D's own occurrence inventory is the base-locator authority now; the
    // retired projection copy of the resolved FQN list is gone.
    let base_refs = definition_base_refs(&mv_definition).map_err(RefreshError::user)?;
    let (mv_definition, refresh_query_source) = rebind_mv_definition_before_refresh_derivation(
        source.connector_control(),
        source.storage_observation(),
        &mv_definition,
        &base_refs,
        &iceberg_target,
        None,
        connector_context,
    )
    .map_err(RefreshError::user)?;
    // One exact target generation serves the whole plan: the snapshot fence
    // above, the state baseline, the L bindings, and the typed partition facts
    // every sub-planner reads. Re-resolving `latest` per fact could split one
    // attempt across two generations.
    let target_schema_observation =
        crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            target_binding.lease(),
            target_binding.metadata(),
            connector_context.clone(),
        )
        .map_err(|error| {
            RefreshError::user(format!(
                "observe exact MV target schema for {}.{}.{}: {error}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ))
        })?;
    let refresh_state_baseline = build_refresh_state_baseline(&mv_definition, &target_binding)
        .map_err(RefreshError::user)?;
    let resolution = &mv_definition.facts.definition().query.resolution;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&refresh_query_source).map_err(RefreshError::user)?,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
    );
    // Driver dispatch: plan-side dispatch is capability-driven, matching the
    // execute path. The capabilities come from L plus the exact target
    // observation L was validated against, never from a persisted contract.
    let runtime_bindings = validate_projection_target(&mv_definition, &target_schema_observation)
        .map_err(RefreshError::user)?;
    let caps = RefreshCapabilities::from_canonical_facts(
        mv_definition.facts.interpretation(),
        &runtime_bindings,
        mv_definition.facts.definition().relation_occurrences.len(),
        definition_has_join(&canonical_select_query),
    )
    .map_err(RefreshError::user)?;
    let is_join = matches!(
        caps.snapshot_policy,
        BaseSnapshotPolicy::JoinPairPartialInitialSkip
    );
    match (caps.has_agg_state, &caps.snapshot_policy, &caps.identity) {
        // UNION ALL of projection/filter branches.
        (false, BaseSnapshotPolicy::AllBasesRequired, _) => {
            // The query's own branch count; L's branch interpretations are the
            // persisted counterpart the per-base validator compares it against.
            let branch_count = union_branch_count(&canonical_select_query) as usize;
            return plan_iceberg_union_projection_mv_refresh(
                source,
                &iceberg_target,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                branch_count,
                &target_binding,
                &target_schema_observation,
                &refresh_state_baseline,
                connector_context,
            );
        }
        // Aggregate shapes: single-base, fan-in, branch-union, and join
        // aggregate all route through the aggregate planner, which selects the
        // per-shape plan by capability. The branch-union sub-path sources its
        // branch count + first-branch aggregate calls from the focused extractor
        // (not the union classifier), so composed branches are supported.
        (true, _, _) => {
            return plan_iceberg_aggregate_mv_refresh(
                source,
                &iceberg_target,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                &caps,
                &canonical_select_query,
                &target_binding,
                &target_schema_observation,
                &refresh_state_baseline,
                connector_context,
            );
        }
        // Join / single-base projection-filter: fall through to the inline
        // paths below.
        (false, BaseSnapshotPolicy::JoinPairPartialInitialSkip, _)
        | (false, BaseSnapshotPolicy::SingleBase, _) => {}
    }
    if is_join {
        // The join projection/filter plan path resolves left/right from D's own
        // effective SQL. There is no persisted join lineage and no contract
        // version to compare: the canonical documents carry their own revisions
        // and were already verified when the projection was reconstructed.
        let occurrences = definition_occurrences_for_base_refs(&mv_definition, &base_refs)
            .map_err(RefreshError::user)?;
        let (left_ref, right_ref) =
            join_base_refs_for_definition(&mv_definition, &canonical_select_query, &base_refs)
                .map_err(RefreshError::user)?;
        let left_refresh = observe_current_refresh_base(
            source.connector_control(),
            source.storage_observation(),
            left_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        let right_refresh = observe_current_refresh_base(
            source.connector_control(),
            source.storage_observation(),
            right_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        let left_observation = observe_schema_validation_for_table(
            source.connector_control(),
            source.storage_observation(),
            left_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        let right_observation = observe_schema_validation_for_table(
            source.connector_control(),
            source.storage_observation(),
            right_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        // The join validator demands D occurrence order, which is not the
        // join's own left/right order, so the observations are re-paired with
        // their occurrences rather than with the join sides.
        let bases = occurrences
            .iter()
            .copied()
            .map(|occurrence| {
                let table = occurrence_table(occurrence);
                if &table == left_ref {
                    Ok((occurrence, &left_observation))
                } else if &table == right_ref {
                    Ok((occurrence, &right_observation))
                } else {
                    Err(RefreshError::user(format!(
                        "iceberg join MV D occurrence {} is neither join side",
                        occurrence.occurrence_id
                    )))
                }
            })
            .collect::<Result<Vec<_>, RefreshError>>()?;
        let renames =
            validate_join_schema_contract(&mv_definition, &bases, &target_schema_observation)
                .map_err(RefreshError::user)?;
        require_no_occurrence_rebind(&renames).map_err(RefreshError::user)?;
        let left_current = left_refresh.current_snapshot_id();
        let right_current = right_refresh.current_snapshot_id();
        let mut snapshot_pins = BTreeMap::new();
        snapshot_pins.insert(left_ref.fqn(), left_current);
        snapshot_pins.insert(right_ref.fqn(), right_current);
        let mut current_snapshots = BTreeMap::new();
        current_snapshots.insert(left_ref.fqn(), left_current);
        current_snapshots.insert(right_ref.fqn(), right_current);
        let previous =
            previous_refresh_locators(&refresh_state_baseline).map_err(RefreshError::user)?;
        let previous_snapshots = &previous.snapshots;
        let refresh_label = format!(
            "iceberg join MV {}.{}.{}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        );
        let refresh_statuses =
            base_snapshot_statuses_for_plan(&base_refs, previous_snapshots, &current_snapshots);
        let decision = decide_refresh_plan(&RefreshPlanningInput {
            snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            base_snapshots: &refresh_statuses,
            label: &refresh_label,
        })
        .map_err(RefreshError::user)?;
        let has_previous = base_refs
            .iter()
            .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
        if has_previous {
            for base_ref in &base_refs {
                let fqn = base_ref.fqn();
                if previous_snapshots.contains_key(&fqn)
                    && current_snapshots.get(&fqn).copied().flatten().is_none()
                {
                    return Err(RefreshError::user(format!(
                        "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                    )));
                }
            }
            for base_ref in &base_refs {
                let fqn = base_ref.fqn();
                previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                    RefreshError::user(format!(
                        "iceberg join MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                    ))
                })?;
                current_snapshots
                    .get(&fqn)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        RefreshError::user(format!(
                            "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                        ))
                    })?;
            }
        }
        let affected_partitions = unknown_join_affected_partitions();
        log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
        return Ok(RefreshPlan {
            contract: RefreshPlanContract {
                mv_id: Some(mv_definition.mv_id),
                target,
                storage_engine: MvStorageEngine::Iceberg,
                decision: decision.refresh,
                state_baseline: refresh_state_baseline.clone(),
                base_refs: base_refs
                    .iter()
                    .map(|base_ref| TableIdentity {
                        catalog: base_ref.catalog.clone(),
                        namespace: base_ref.namespace.clone(),
                        table: base_ref.table.clone(),
                    })
                    .collect(),
                snapshot_pins,
                affected_partitions,
            },
            backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
                stmt: stmt.clone(),
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
            }),
        });
    }
    // The capability dispatch above (has_agg=false, SingleBase, non-join) routes only
    // single-base projection/filter MVs to this point. No classify guard needed.
    let [base_ref] = base_refs.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one base table reference",
        ));
    };
    let occurrences = definition_occurrences_for_base_refs(&mv_definition, &base_refs)
        .map_err(RefreshError::user)?;
    let [occurrence] = occurrences.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one D relation occurrence",
        ));
    };
    let current_snapshot_id_before_pin = observe_current_refresh_base(
        source.connector_control(),
        source.storage_observation(),
        base_ref,
        connector_context,
    )
    .map_err(RefreshError::user)?
    .current_snapshot_id();
    let previous =
        previous_refresh_locators(&refresh_state_baseline).map_err(RefreshError::user)?;
    let previous_snapshot_id = previous.snapshots.get(&base_ref.fqn()).copied();
    let refresh_label = format!(
        "iceberg materialized view {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let pre_pin_statuses = [base_snapshot_status_for_refresh(
        base_ref,
        previous_snapshot_id,
        current_snapshot_id_before_pin,
    )];
    let pre_pin_decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::SingleBase,
        base_snapshots: &pre_pin_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let base_observation = observe_schema_validation_for_table(
        source.connector_control(),
        source.storage_observation(),
        base_ref,
        connector_context,
    )
    .map_err(RefreshError::user)?;
    let renames = validate_schema_contract(
        &mv_definition,
        occurrence,
        &base_observation,
        &target_schema_observation,
    )
    .map_err(RefreshError::user)?;
    require_no_occurrence_rebind(&renames).map_err(RefreshError::user)?;
    match pre_pin_decision.refresh {
        ExecutableRefreshDecision::SkipEmpty => {
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_ref.fqn(), None);
            let affected_partitions = noop_affected_partitions(target_binding.partition());
            log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
            return Ok(RefreshPlan {
                contract: RefreshPlanContract {
                    mv_id: Some(mv_definition.mv_id),
                    target,
                    storage_engine: MvStorageEngine::Iceberg,
                    decision: pre_pin_decision.refresh,
                    state_baseline: refresh_state_baseline.clone(),
                    base_refs: vec![TableIdentity {
                        catalog: base_ref.catalog.clone(),
                        namespace: base_ref.namespace.clone(),
                        table: base_ref.table.clone(),
                    }],
                    snapshot_pins,
                    affected_partitions,
                },
                backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
                    stmt: stmt.clone(),
                    current_catalog: current_catalog.map(str::to_string),
                    current_database: current_database.to_string(),
                }),
            });
        }
        ExecutableRefreshDecision::FirstRefresh
        | ExecutableRefreshDecision::MetadataOnly
        | ExecutableRefreshDecision::Incremental => {}
    }

    let current_snapshot_id = current_snapshot_id_before_pin;

    let refresh_statuses = [base_snapshot_status_for_refresh(
        base_ref,
        previous_snapshot_id,
        current_snapshot_id,
    )];
    let decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::SingleBase,
        base_snapshots: &refresh_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let mode = decision.mode();
    let mut snapshot_pins = BTreeMap::new();
    snapshot_pins.insert(base_ref.fqn(), current_snapshot_id);
    let affected_partitions = plan_aggregate_mv_affected_partitions(
        source,
        connector_context,
        base_ref,
        &mv_definition,
        target_binding.partition(),
        mode,
        previous_snapshot_id,
        current_snapshot_id,
    );
    log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
    Ok(RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision: decision.refresh,
            state_baseline: refresh_state_baseline,
            base_refs: vec![TableIdentity {
                catalog: base_ref.catalog.clone(),
                namespace: base_ref.namespace.clone(),
                table: base_ref.table.clone(),
            }],
            snapshot_pins,
            affected_partitions,
        },
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Union projection planning keeps each frozen target, snapshot, and connector input explicit."
)]
fn plan_iceberg_union_projection_mv_refresh(
    source: &dyn IcebergMvRefreshSource,
    iceberg_target: &IcebergMvTarget,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvProjection,
    base_refs: &[TableIdentity],
    branch_count: usize,
    target_binding: &MvTargetBinding,
    target_observation: &MvSchemaValidationObservation,
    state_baseline: &RefreshStateBaseline,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    // The resolved bases ARE D's occurrences, in D's own order; the retired
    // contract base set that used to be compared here no longer exists.
    let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
        .map_err(RefreshError::user)?;

    let mut current_snapshots = BTreeMap::new();
    let mut current_table_object_ids = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for (occurrence, base_ref) in occurrences.iter().copied().zip(base_refs) {
        let refresh = observe_current_refresh_base(
            source.connector_control(),
            source.storage_observation(),
            base_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        let base_observation = observe_schema_validation_for_table(
            source.connector_control(),
            source.storage_observation(),
            base_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        validate_union_projection_schema_contract_for_base(
            iceberg_target,
            mv_definition,
            branch_count,
            occurrence,
            &base_observation,
            target_observation,
        )
        .map_err(RefreshError::user)?;
        let current = refresh.current_snapshot_id();
        let fqn = base_ref.fqn();
        snapshot_pins.insert(fqn.clone(), current);
        current_snapshots.insert(fqn.clone(), current);
        current_table_object_ids.insert(fqn, refresh.object_id().clone());
    }

    let previous = previous_refresh_locators(state_baseline).map_err(RefreshError::user)?;
    let previous_snapshots = &previous.snapshots;
    let previous_table_object_ids = &previous.table_object_ids;
    let has_previous_snapshots = base_refs
        .iter()
        .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
    let has_previous_table_object_ids = base_refs
        .iter()
        .any(|base_ref| previous_table_object_ids.contains_key(&base_ref.fqn()));
    let has_previous = has_previous_snapshots || has_previous_table_object_ids;
    let all_previous_snapshots = base_refs
        .iter()
        .all(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
    let all_previous_table_object_ids = base_refs
        .iter()
        .all(|base_ref| previous_table_object_ids.contains_key(&base_ref.fqn()));

    if has_previous && (!all_previous_snapshots || !all_previous_table_object_ids) {
        return Err(RefreshError::user(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} has partial previous refresh metadata; recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )));
    }
    let refresh_label = format!(
        "iceberg UNION ALL projection/filter MV {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let refresh_statuses =
        base_snapshot_statuses_for_plan(base_refs, previous_snapshots, &current_snapshots);
    let decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        base_snapshots: &refresh_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let mode = decision.mode();
    if has_previous {
        for base_ref in base_refs {
            let fqn = base_ref.fqn();
            if let Some(previous_object_id) = previous_table_object_ids.get(&fqn) {
                let current_object_id = current_table_object_ids.get(&fqn).ok_or_else(|| {
                    RefreshError::user(format!(
                        "refresh observation missing object ID for base {fqn} (this should not happen)"
                    ))
                })?;
                if previous_object_id != current_object_id {
                    return Err(RefreshError::user(format!(
                        "iceberg MV base table identity changed for {fqn}; incremental refresh is unsafe, rebuild or recreate the MV"
                    )));
                }
            }
            previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                RefreshError::user(format!(
                    "iceberg UNION ALL projection/filter MV {}.{}.{} has partial previous refresh metadata; recreate the MV",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                ))
            })?;
            current_snapshots.get(&fqn).copied().flatten().ok_or_else(|| {
                RefreshError::user(format!(
                    "cannot refresh iceberg UNION ALL projection/filter MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                ))
            })?;
        }
    }

    let affected_partitions = plan_multi_base_affected_partitions(
        mv_definition,
        target_binding.partition(),
        mode,
        base_refs,
        previous_snapshots,
        &current_snapshots,
        |base_ref, previous, current| {
            observe_and_admit_change_window_for_table(
                source.connector_control(),
                source.storage_observation(),
                base_ref,
                previous,
                current,
                connector_context,
            )
        },
        "UNION ALL MV affected partition planning",
    );
    log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
    Ok(RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision: decision.refresh,
            state_baseline: state_baseline.clone(),
            base_refs: base_refs
                .iter()
                .map(|base_ref| TableIdentity {
                    catalog: base_ref.catalog.clone(),
                    namespace: base_ref.namespace.clone(),
                    table: base_ref.table.clone(),
                })
                .collect(),
            snapshot_pins,
            affected_partitions,
        },
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    })
}

/// Plan the `AllBasesRequired` aggregate refresh variants (fan-in
/// `GroupRowId` and branch-union `BranchScoped`). Extracted from
/// `plan_iceberg_aggregate_mv_refresh` (I2) to mirror the execute-side
/// [`refresh_fan_in_aggregate_iceberg_mv`] fold: both `AllBasesRequired`
/// aggregate identities pin/validate every base, decide the refresh from the
/// combined base-snapshot statuses, and build one multi-base refresh plan; only
/// the up-front branch-contract vs fan-in base-ref validation and the log label
/// differ. Behavior is byte-for-byte identical to the inline block it replaced.
#[expect(
    clippy::too_many_arguments,
    reason = "All-bases aggregate planning preserves each independently validated base and connector fact."
)]
fn plan_iceberg_all_bases_aggregate_mv_refresh(
    source: &dyn IcebergMvRefreshSource,
    iceberg_target: &IcebergMvTarget,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvProjection,
    base_refs: &[TableIdentity],
    caps: &RefreshCapabilities,
    canonical_select_query: &ast::Query,
    target_binding: &MvTargetBinding,
    target_observation: &MvSchemaValidationObservation,
    state_baseline: &RefreshStateBaseline,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    // The branch-union variant validates the branch count (off the AST, so a
    // composed branch union is supported) + the resolved base-ref set; the
    // fan-in variant validates the resolved base-ref set directly (the resolved
    // bases ARE the fan-in base set now that the classifier is retired).
    let is_branch_union = matches!(caps.identity, RefreshIdentity::BranchScoped(_));
    let is_composed_join_aggregate =
        !is_branch_union && !from_clause_is_fan_in_union(canonical_select_query);
    let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
        .map_err(RefreshError::user)?;
    if is_branch_union {
        let branch_count = union_branch_count(canonical_select_query) as usize;
        validate_branch_union_contract(
            iceberg_target,
            mv_definition,
            branch_count,
            target_observation,
        )
        .map_err(RefreshError::user)?;
        validate_branch_union_aggregate_base_refs(base_refs).map_err(RefreshError::user)?;
    } else if is_composed_join_aggregate {
        validate_composed_aggregate_fallback_query(canonical_select_query)
            .map_err(RefreshError::user)?;
    } else {
        validate_aggregate_fan_in_base_refs(base_refs).map_err(RefreshError::user)?;
    }
    let mut current_snapshots = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for (occurrence, base_ref) in occurrences.iter().copied().zip(base_refs) {
        let refresh = observe_current_refresh_base(
            source.connector_control(),
            source.storage_observation(),
            base_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        let base_observation = observe_schema_validation_for_table(
            source.connector_control(),
            source.storage_observation(),
            base_ref,
            connector_context,
        )
        .map_err(RefreshError::user)?;
        let renames = validate_aggregate_schema_contract_for_base(
            mv_definition,
            occurrence,
            &base_observation,
            target_observation,
        )
        .map_err(RefreshError::user)?;
        require_no_occurrence_rebind(&renames).map_err(RefreshError::user)?;
        let current = refresh.current_snapshot_id();
        let fqn = base_ref.fqn();
        current_snapshots.insert(fqn.clone(), current);
        snapshot_pins.insert(fqn.clone(), current);
    }
    let previous = previous_refresh_locators(state_baseline).map_err(RefreshError::user)?;
    let previous_snapshots = &previous.snapshots;
    let refresh_kind_label = if is_branch_union {
        "branch UNION ALL aggregate"
    } else if is_composed_join_aggregate {
        "composed join aggregate"
    } else {
        "aggregate-over-UNION-ALL"
    };
    let refresh_label = format!(
        "iceberg {refresh_kind_label} MV {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let refresh_statuses =
        base_snapshot_statuses_for_plan(base_refs, previous_snapshots, &current_snapshots);
    let decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        base_snapshots: &refresh_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let mode = decision.mode();
    let has_previous = base_refs
        .iter()
        .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
    if has_previous {
        for base_ref in base_refs {
            let fqn = base_ref.fqn();
            previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                RefreshError::user(format!(
                    "iceberg {refresh_kind_label} MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                ))
            })?;
            current_snapshots.get(&fqn).copied().flatten().ok_or_else(|| {
                RefreshError::user(format!(
                    "cannot refresh iceberg {refresh_kind_label} MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                ))
            })?;
        }
    }
    let affected_partition_context =
        format!("iceberg {refresh_kind_label} MV affected partition planning");
    let affected_partitions = plan_multi_base_affected_partitions(
        mv_definition,
        target_binding.partition(),
        mode,
        base_refs,
        previous_snapshots,
        &current_snapshots,
        |base_ref, previous, current| {
            observe_and_admit_change_window_for_table(
                source.connector_control(),
                source.storage_observation(),
                base_ref,
                previous,
                current,
                connector_context,
            )
        },
        &affected_partition_context,
    );
    log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
    Ok(build_iceberg_refresh_plan(
        mv_definition,
        target,
        stmt,
        current_catalog,
        current_database,
        base_refs,
        snapshot_pins,
        decision.refresh,
        state_baseline.clone(),
        affected_partitions,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "Aggregate refresh planning must keep the independently frozen SQL, target, snapshot, and connector facts explicit."
)]
fn plan_iceberg_aggregate_mv_refresh(
    source: &dyn IcebergMvRefreshSource,
    iceberg_target: &IcebergMvTarget,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvProjection,
    base_refs: &[TableIdentity],
    caps: &RefreshCapabilities,
    canonical_select_query: &ast::Query,
    target_binding: &MvTargetBinding,
    target_observation: &MvSchemaValidationObservation,
    state_baseline: &RefreshStateBaseline,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    // L is the only aggregate authority; there is no persisted contract version
    // to compare against any more.
    validate_aggregate_schema_contract_metadata(iceberg_target, mv_definition)
        .map_err(RefreshError::user)?;
    // Aggregate plan dispatch (Phase 3 / B2): selected by capability.
    //   SingleBase                 -> single-base aggregate
    //   AllBasesRequired           -> fan-in (GroupRowId) or branch-union
    //                                 (BranchScoped), gated on the row identity
    //   JoinPairPartialInitialSkip -> join aggregate
    match &caps.snapshot_policy {
        BaseSnapshotPolicy::SingleBase | BaseSnapshotPolicy::AllBasesRequired => {
            // AllBasesRequired (fan-in `GroupRowId` / branch-union
            // `BranchScoped`) is planned by a dedicated helper (I2) mirroring
            // the execute-side `refresh_fan_in_aggregate_iceberg_mv` fold.
            if matches!(caps.snapshot_policy, BaseSnapshotPolicy::AllBasesRequired) {
                return plan_iceberg_all_bases_aggregate_mv_refresh(
                    source,
                    iceberg_target,
                    target,
                    stmt,
                    current_catalog,
                    current_database,
                    mv_definition,
                    base_refs,
                    caps,
                    canonical_select_query,
                    target_binding,
                    target_observation,
                    state_baseline,
                    connector_context,
                );
            }
            let [base_ref] = base_refs else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one base table reference",
                ));
            };
            let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
                .map_err(RefreshError::user)?;
            let [occurrence] = occurrences.as_slice() else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one D relation occurrence",
                ));
            };
            let refresh = observe_current_refresh_base(
                source.connector_control(),
                source.storage_observation(),
                base_ref,
                connector_context,
            )
            .map_err(RefreshError::user)?;
            let base_observation = observe_schema_validation_for_table(
                source.connector_control(),
                source.storage_observation(),
                base_ref,
                connector_context,
            )
            .map_err(RefreshError::user)?;
            let renames = validate_schema_contract(
                mv_definition,
                occurrence,
                &base_observation,
                target_observation,
            )
            .map_err(RefreshError::user)?;
            require_no_occurrence_rebind(&renames).map_err(RefreshError::user)?;
            let current = refresh.current_snapshot_id();
            let previous_locators =
                previous_refresh_locators(state_baseline).map_err(RefreshError::user)?;
            let previous = previous_locators.snapshots.get(&base_ref.fqn()).copied();
            let refresh_label = format!(
                "iceberg aggregate materialized view {}.{}.{}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            );
            let refresh_statuses = [base_snapshot_status_for_refresh(
                base_ref, previous, current,
            )];
            let decision = decide_refresh_plan(&RefreshPlanningInput {
                snapshot_policy: BaseSnapshotPolicy::SingleBase,
                base_snapshots: &refresh_statuses,
                label: &refresh_label,
            })
            .map_err(RefreshError::user)?;
            let mode = decision.mode();
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_ref.fqn(), current);
            let affected_partitions = plan_aggregate_mv_affected_partitions(
                source,
                connector_context,
                base_ref,
                mv_definition,
                target_binding.partition(),
                mode,
                previous,
                current,
            );
            log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                base_refs,
                snapshot_pins,
                decision.refresh,
                state_baseline.clone(),
                affected_partitions,
            ))
        }
        BaseSnapshotPolicy::JoinPairPartialInitialSkip => {
            // The join-aggregate plan resolves left/right from D's own effective
            // SQL (FROM-side only); the join ON keys are never read by the plan
            // path.
            let occurrences = definition_occurrences_for_base_refs(mv_definition, base_refs)
                .map_err(RefreshError::user)?;
            let (left_ref, right_ref) =
                join_base_refs_for_definition(mv_definition, canonical_select_query, base_refs)
                    .map_err(RefreshError::user)?;
            let left_refresh = observe_current_refresh_base(
                source.connector_control(),
                source.storage_observation(),
                left_ref,
                connector_context,
            )
            .map_err(RefreshError::user)?;
            let right_refresh = observe_current_refresh_base(
                source.connector_control(),
                source.storage_observation(),
                right_ref,
                connector_context,
            )
            .map_err(RefreshError::user)?;
            let left_observation = observe_schema_validation_for_table(
                source.connector_control(),
                source.storage_observation(),
                left_ref,
                connector_context,
            )
            .map_err(RefreshError::user)?;
            let right_observation = observe_schema_validation_for_table(
                source.connector_control(),
                source.storage_observation(),
                right_ref,
                connector_context,
            )
            .map_err(RefreshError::user)?;
            // The join validator demands D occurrence order, which is not the
            // join's own left/right order, so the observations are re-paired
            // with their occurrences rather than with the join sides.
            let bases = occurrences
                .iter()
                .copied()
                .map(|occurrence| {
                    let table = occurrence_table(occurrence);
                    if &table == left_ref {
                        Ok((occurrence, &left_observation))
                    } else if &table == right_ref {
                        Ok((occurrence, &right_observation))
                    } else {
                        Err(RefreshError::user(format!(
                            "iceberg join aggregate MV D occurrence {} is neither join side",
                            occurrence.occurrence_id
                        )))
                    }
                })
                .collect::<Result<Vec<_>, RefreshError>>()?;
            let renames = validate_join_schema_contract(mv_definition, &bases, target_observation)
                .map_err(RefreshError::user)?;
            require_no_occurrence_rebind(&renames).map_err(RefreshError::user)?;

            let mut snapshot_pins = BTreeMap::new();
            let mut current_snapshots = BTreeMap::new();
            current_snapshots.insert(left_ref.fqn(), left_refresh.current_snapshot_id());
            current_snapshots.insert(right_ref.fqn(), right_refresh.current_snapshot_id());
            for base_ref in base_refs {
                snapshot_pins.insert(
                    base_ref.fqn(),
                    current_snapshots.get(&base_ref.fqn()).copied().flatten(),
                );
            }
            let previous_locators =
                previous_refresh_locators(state_baseline).map_err(RefreshError::user)?;
            let previous_snapshots = &previous_locators.snapshots;
            let refresh_label = format!(
                "iceberg join aggregate MV {}.{}.{}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            );
            let refresh_statuses =
                base_snapshot_statuses_for_plan(base_refs, previous_snapshots, &current_snapshots);
            let decision = decide_refresh_plan(&RefreshPlanningInput {
                snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                base_snapshots: &refresh_statuses,
                label: &refresh_label,
            })
            .map_err(RefreshError::user)?;
            let has_previous = base_refs
                .iter()
                .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
            if has_previous {
                for base_ref in base_refs {
                    let fqn = base_ref.fqn();
                    previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                        RefreshError::user(format!(
                            "iceberg join aggregate MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                        ))
                    })?;
                    current_snapshots.get(&fqn).copied().flatten().ok_or_else(
                        || {
                            RefreshError::user(format!(
                                "cannot refresh iceberg join aggregate MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                                iceberg_target.catalog,
                                iceberg_target.namespace,
                                iceberg_target.table,
                                fqn
                            ))
                        },
                    )?;
                }
            }
            let affected_partitions = unknown_join_affected_partitions();
            log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                base_refs,
                snapshot_pins,
                decision.refresh,
                state_baseline.clone(),
                affected_partitions,
            ))
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The final plan constructor preserves each separately validated refresh contract fact."
)]
fn build_iceberg_refresh_plan(
    mv_definition: &StoredMvProjection,
    target: MvTarget,
    stmt: &MvRefreshRequest,
    current_catalog: Option<&str>,
    current_database: &str,
    base_refs: &[TableIdentity],
    snapshot_pins: BTreeMap<String, Option<i64>>,
    decision: ExecutableRefreshDecision,
    state_baseline: RefreshStateBaseline,
    affected_partitions: crate::mv::domain::model::AffectedTargetPartitions,
) -> RefreshPlan {
    RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision,
            state_baseline,
            base_refs: base_refs
                .iter()
                .map(|base_ref| TableIdentity {
                    catalog: base_ref.catalog.clone(),
                    namespace: base_ref.namespace.clone(),
                    table: base_ref.table.clone(),
                })
                .collect(),
            snapshot_pins,
            affected_partitions,
        },
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    }
}

#[cfg(test)]
thread_local! {
    static CATALOG_REGISTRATION_FAILURE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct CatalogRegistrationFailureGuard;

#[cfg(test)]
impl CatalogRegistrationFailureGuard {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    fn install(message: impl Into<String>) -> Self {
        CATALOG_REGISTRATION_FAILURE.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "catalog registration failure already installed"
            );
            *slot.borrow_mut() = Some(message.into());
        });
        Self
    }
}

#[cfg(test)]
impl Drop for CatalogRegistrationFailureGuard {
    fn drop(&mut self) {
        CATALOG_REGISTRATION_FAILURE.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
fn take_catalog_registration_failure_for_test() -> Option<String> {
    CATALOG_REGISTRATION_FAILURE.with(|slot| slot.borrow_mut().take())
}

#[cfg(test)]
thread_local! {
    static AFTER_CREATE_TARGET_HOOK: std::cell::RefCell<Option<Arc<dyn Fn() + Send + Sync>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct AfterCreateTargetHookGuard;

#[cfg(test)]
impl AfterCreateTargetHookGuard {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    fn install(hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        AFTER_CREATE_TARGET_HOOK.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "after-create target hook already installed"
            );
            *slot.borrow_mut() = Some(hook);
        });
        Self
    }
}

#[cfg(test)]
impl Drop for AfterCreateTargetHookGuard {
    fn drop(&mut self) {
        AFTER_CREATE_TARGET_HOOK.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
type AfterObserveBeforeCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct AfterObserveBeforeCaptureHookRegistration {
    owner: std::thread::ThreadId,
    hook: AfterObserveBeforeCaptureHook,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn after_observe_before_capture_hook_slot()
-> &'static std::sync::Mutex<Option<AfterObserveBeforeCaptureHookRegistration>> {
    static HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<AfterObserveBeforeCaptureHookRegistration>>,
    > = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn invoke_after_observe_before_capture_hook() {
    let current_thread = std::thread::current().id();
    let hook = after_observe_before_capture_hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .and_then(|registration| {
            (registration.owner == current_thread).then(|| Arc::clone(&registration.hook))
        });
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct AfterObserveBeforeCaptureHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl AfterObserveBeforeCaptureHookGuard {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    fn install(hook: AfterObserveBeforeCaptureHook) -> Self {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        let lock = LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *after_observe_before_capture_hook_slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(AfterObserveBeforeCaptureHookRegistration {
                owner: std::thread::current().id(),
                hook,
            });
        Self { _lock: lock }
    }
}

#[cfg(test)]
impl Drop for AfterObserveBeforeCaptureHookGuard {
    fn drop(&mut self) {
        *after_observe_before_capture_hook_slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn validate_refresh_pin_table_object_ids_against_baseline(
    baseline: &RefreshStateBaseline,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    let RefreshStateBaseline::SnapshotBacked {
        previous_sources, ..
    } = baseline
    else {
        return Err(
            "iceberg refresh execution requires a snapshot-backed state baseline".to_string(),
        );
    };
    for previous in previous_sources {
        if !base_refs.iter().any(|base_ref| base_ref == &previous.table) {
            return Err(format!(
                "refresh baseline occurrence {} refers to an unknown D locator {}",
                previous.occurrence_id.get(),
                previous.table.fqn(),
            ));
        }
        let current = pin.get(previous.occurrence_id).ok_or_else(|| {
            format!(
                "refresh pin missing D occurrence {} for base {}",
                previous.occurrence_id.get(),
                previous.table.fqn(),
            )
        })?;
        if current.table() != &previous.table {
            return Err(format!(
                "refresh pin locator differs at D occurrence {}",
                previous.occurrence_id.get(),
            ));
        }
        if previous.table_object_id != *current.table_object_id()
            || previous
                .semantic_revision
                .object_identity()
                .value()
                .as_ref()
                != previous.table_object_id.as_bytes().as_ref()
        {
            return Err(format!(
                "iceberg MV base table identity changed for {}; incremental refresh is unsafe, rebuild or recreate the MV",
                previous.table.fqn(),
            ));
        }
    }
    Ok(())
}

/// Resolves the left/right `base_refs` for a join MV by matching
/// `JoinAliases.{left_table,right_table}` (the `ObjectName.to_string()` FQN form)
/// against `base.fqn()`.
fn join_base_refs_for_aliases<'a>(
    aliases: &SqlMvJoinAliases,
    base_refs: &'a [TableIdentity],
) -> Result<(&'a TableIdentity, &'a TableIdentity), String> {
    let left_name = aliases.left_table.as_str();
    let right_name = aliases.right_table.as_str();
    let left = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(left_name))
        .ok_or_else(|| format!("join MV left base {left_name} was not resolved"))?;
    let right = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(right_name))
        .ok_or_else(|| format!("join MV right base {right_name} was not resolved"))?;
    Ok((left, right))
}

/// Resolve a join MV's left/right relations from D's own effective SQL.
///
/// The join order is a property of the definition, so it is reparsed from D
/// rather than read back from a persisted lineage copy. A self-join is
/// rejected here: this locator-keyed path cannot tell two occurrences of one
/// relation apart, and merging them would silently pick one.
pub fn join_base_refs_for_definition<'a>(
    projection: &StoredMvProjection,
    canonical_query: &ast::Query,
    base_refs: &'a [TableIdentity],
) -> Result<(&'a TableIdentity, &'a TableIdentity), String> {
    if projection.facts.definition().relation_occurrences.len() != 2 {
        return Err("join MV refresh requires exactly two D relation occurrences".to_string());
    }
    let aliases = extract_join_aliases(canonical_query)?;
    if aliases
        .left_table
        .eq_ignore_ascii_case(aliases.right_table.as_str())
    {
        return Err(format!(
            "join MV definition has identical left/right relations: {}",
            aliases.left_table
        ));
    }
    join_base_refs_for_aliases(&aliases, base_refs)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn rewrite_snapshot_table_factor(
    factor: &mut ast::TableFactor,
    base: &TableIdentity,
    snapshot_id: i64,
    default_alias: Option<&str>,
) -> Result<(), String> {
    let ast::TableFactor::Table {
        name,
        version,
        alias,
        ..
    } = factor
    else {
        return Err("join snapshot side must be a table".to_string());
    };
    if !object_name_matches_base(name, base)? {
        return Err(format!(
            "join snapshot rewrite expected base {}, got {}",
            base.fqn(),
            novarocks_parser::printer::print_object_name(name)
        ));
    }
    if let Some(version) = version {
        let rendered = novarocks_parser::printer::print_expr(&version.value);
        if !rendered.contains(&snapshot_id.to_string()) {
            return Err(format!(
                "join snapshot side {} has conflicting version {rendered}",
                base.fqn()
            ));
        }
    }
    *name = synthetic_snapshot_object_name(base, snapshot_id);
    *version = None;
    if alias.is_none()
        && let Some(default_alias) = default_alias
    {
        *alias = Some(ast::TableAlias {
            name: generated_ident(default_alias),
            columns: Vec::new(),
            explicit_as: true,
            span: Span::new(0, 0),
        });
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn object_name_matches_base(name: &ast::ObjectName, base: &TableIdentity) -> Result<bool, String> {
    let parts = object_name_identifier_parts(name);
    Ok(match parts.as_slice() {
        [table] => table.eq_ignore_ascii_case(&base.table),
        [namespace, table] => {
            namespace.eq_ignore_ascii_case(&base.namespace)
                && table.eq_ignore_ascii_case(&base.table)
        }
        [catalog, namespace, table] => {
            catalog.eq_ignore_ascii_case(&base.catalog)
                && namespace.eq_ignore_ascii_case(&base.namespace)
                && table.eq_ignore_ascii_case(&base.table)
        }
        _ => false,
    })
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn object_name_identifier_parts(name: &ast::ObjectName) -> Vec<String> {
    name.parts.iter().map(|ident| ident.value.clone()).collect()
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn synthetic_snapshot_table_name(base: &TableIdentity, snapshot_id: i64) -> String {
    format!("{}__at_{}", base.table, snapshot_id)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn synthetic_snapshot_object_name(base: &TableIdentity, snapshot_id: i64) -> ast::ObjectName {
    ast::ObjectName {
        parts: vec![
            generated_ident(&base.namespace),
            generated_ident(&synthetic_snapshot_table_name(base, snapshot_id)),
        ],
        span: Span::new(0, 0),
    }
}

fn generated_ident(value: &str) -> ast::Ident {
    ast::Ident {
        value: value.to_string(),
        quoted: false,
        quote_style: None,
        span: Span::new(0, 0),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn refresh_explain_rewrite_disabled_rules(
    is_aggregate_refresh: bool,
    optimizer_settings: &novarocks_sql::compiler::SessionOptimizerSettings,
) -> Vec<String> {
    let mut disabled_rules = optimizer_settings.disabled_rules.clone();
    if is_aggregate_refresh
        && !disabled_rules
            .iter()
            .any(|rule| rule == "RecordJoinRefreshDescriptor")
    {
        disabled_rules.push("RecordJoinRefreshDescriptor".to_string());
    }
    disabled_rules
}

#[cfg(test)]
mod partition_planning_tests {
    use super::*;
    use mv_schema::{MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract};
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;

    fn key(value: &str) -> crate::mv::domain::model::MvPartitionKey {
        crate::mv::domain::model::MvPartitionKey::new(
            7,
            vec![crate::mv::domain::model::MvPartitionKeyField::new(
                "region".to_string(),
                crate::mv::domain::model::MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    fn base_ref(table: &str) -> TableIdentity {
        TableIdentity {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
        }
    }

    /// The MV target's partitioning is the provider's own typed observation,
    /// carried by the exact target binding. The retired schema contract used to
    /// keep a second copy; it is gone.
    fn identity_partition() -> MvPartitionContract {
        MvPartitionContract {
            target_spec_id: 7,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 100,
                partition_field_name: "id".to_string(),
                source_target_field_id: 10,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Identity,
            }],
        }
    }

    fn projection() -> StoredMvProjection {
        StoredMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
                Some(11),
            )
            .build()
            .expect("fixture projection"),
        }
    }

    #[test]
    fn merge_affected_partition_results_unions_known_sets() {
        let merged = merge_affected_partition_results(
            "UNION ALL MV affected partition planning",
            vec![
                (
                    "ice.db.left".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::known([
                        key("west"),
                        key("east"),
                    ]),
                ),
                (
                    "ice.db.right".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::known([
                        key("east"),
                        key("north"),
                    ]),
                ),
            ],
        );

        assert_eq!(
            merged,
            crate::mv::domain::model::AffectedTargetPartitions::known([
                key("east"),
                key("north"),
                key("west"),
            ])
        );
    }

    #[test]
    fn merge_affected_partition_results_preserves_first_not_derived_reason() {
        let merged = merge_affected_partition_results(
            "UNION ALL MV affected partition planning",
            vec![
                (
                    "ice.db.left".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::known([key("west")]),
                ),
                (
                    "ice.db.right".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::not_derived(
                        "missing file partition metadata",
                    ),
                ),
            ],
        );

        assert_eq!(
            merged.not_derived_reason(),
            Some(
                "UNION ALL MV affected partition planning: ice.db.right: missing file partition metadata"
            )
        );
    }

    #[test]
    fn plan_multi_base_affected_partitions_unchanged_bases_return_empty_known_set() {
        let projection = projection();
        let partition = identity_partition();
        let base_refs = vec![base_ref("left"), base_ref("right")];
        let previous_snapshots = BTreeMap::from([
            ("ice.db.left".to_string(), 11_i64),
            ("ice.db.right".to_string(), 22_i64),
        ]);
        let current_snapshots = BTreeMap::from([
            ("ice.db.left".to_string(), Some(11_i64)),
            ("ice.db.right".to_string(), Some(22_i64)),
        ]);

        let planned = plan_multi_base_affected_partitions(
            &projection,
            &partition,
            RefreshMode::Incremental,
            &base_refs,
            &previous_snapshots,
            &current_snapshots,
            |_base_ref, _previous, _current| {
                panic!("unchanged bases should not require change-window admission")
            },
            "UNION ALL MV affected partition planning",
        );

        assert_eq!(
            planned,
            crate::mv::domain::model::AffectedTargetPartitions::known(std::iter::empty::<
                crate::mv::domain::model::MvPartitionKey,
            >(),)
        );
    }
}

#[cfg(test)]
mod join_delta_append_only_fast_path_tests {
    use super::*;

    #[test]
    fn join_incremental_refresh_plan_kind_uses_logical_cutover() {
        let mode = select_join_incremental_execution_mode(false, false);
        assert_eq!(mode, MvIncrementalJoinMode::AppendOnly);
        let mode = select_join_incremental_execution_mode(true, false);
        assert_eq!(mode, MvIncrementalJoinMode::Coalesce);
        let mode = select_join_incremental_execution_mode(false, true);
        assert_eq!(mode, MvIncrementalJoinMode::Coalesce);
    }

    #[test]
    fn aggregate_refresh_explain_disables_join_refresh_descriptor_recording() {
        let disabled_rules = refresh_explain_rewrite_disabled_rules(
            true,
            &novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );

        assert!(
            disabled_rules
                .iter()
                .any(|rule| rule == "RecordJoinRefreshDescriptor"),
            "aggregate refresh explain must not record a pure join refresh descriptor"
        );
    }

    #[test]
    fn join_delta_append_only_fast_path_requires_append_only_inner_or_cross_join() {
        assert!(should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"),
            false,
            false,
        ));
        assert!(should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l cross join ice.ns.right r"),
            false,
            false,
        ));

        assert!(!should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"),
            true,
            false,
        ));
        assert!(!should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"),
            false,
            true,
        ));
        assert!(!should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l left join ice.ns.right r on l.id = r.id"),
            false,
            false,
        ));
    }

    #[test]
    fn join_delta_coalesce_uses_normalized_snapshot_ctes() {
        let base_query = parse_query(
            "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
        );
        let left = base("left");
        let right = base("right");
        let branches = crate::mv::domain::iceberg_join_branch::plan_join_delta_branches(
            &left,
            &right,
            crate::mv::domain::iceberg_join_branch::SnapshotWindow { from: 10, to: 11 },
            crate::mv::domain::iceberg_join_branch::SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        let mut branch_queries = Vec::new();
        for branch in &branches {
            let mut branch_query =
                crate::mv::domain::iceberg_join_branch::rewrite_join_branch_query(
                    &base_query,
                    branch,
                    "l",
                    "r",
                )
                .expect("branch rewrite");
            normalize_join_branch_snapshot_tables(&mut branch_query, branch)
                .expect("snapshot normalization");
            branch_queries.push(branch_query);
        }

        let coalesced =
            crate::mv::domain::iceberg_join_branch::rewrite_join_delta_coalesce_query_with_branch_queries(
                &base_query,
                branch_queries,
                "left-uuid",
                "right-uuid",
            )
            .expect("coalesce rewrite");
        let rendered = novarocks_parser::printer::print_query(&coalesced);

        assert!(rendered.contains("right__at_20"), "sql={rendered}");
        assert!(rendered.contains("left__at_11"), "sql={rendered}");
        assert!(!rendered.contains("VERSION AS OF"), "sql={rendered}");
        assert!(
            rendered.contains("__nr_join_delta_branch_0"),
            "sql={rendered}"
        );
        assert!(
            rendered.contains("__nr_join_delta_branch_1"),
            "sql={rendered}"
        );
    }

    fn base(name: &str) -> TableIdentity {
        TableIdentity {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: name.to_string(),
        }
    }

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn normalize_join_branch_snapshot_tables(
    query: &mut ast::Query,
    branch: &crate::mv::domain::iceberg_join_branch::JoinDeltaBranchPlan,
) -> Result<(), String> {
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch snapshot normalization requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one JOIN".to_string());
    };
    if let crate::mv::domain::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.left {
        rewrite_snapshot_table_factor(&mut from.relation, &branch.left_base, snapshot_id, None)?;
    }
    if let crate::mv::domain::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.right
    {
        rewrite_snapshot_table_factor(&mut join.relation, &branch.right_base, snapshot_id, None)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct RewriteMergeRefreshOptions {
    apply_key: ApplyKeyContract,
}

/// Run the provider-specific effects for a product-owned DROP state machine.
/// SQL target resolution and Connector mutation remain outside the product;
/// the product fixes their admissible order and terminal meaning.
pub(crate) fn drop_iceberg_mv_with_product(
    product: &novarocks_mv_application::service::MvProductService,
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvDropStatement,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let target = resolve_drop_target(
        current_catalog,
        current_database,
        &ObjectName {
            parts: stmt.name_parts.clone(),
        },
    )?;
    let product_target = novarocks_mv_application::product::MvTarget::try_new(
        Some(target.catalog.clone()),
        target.namespace.clone(),
        target.table.clone(),
    )
    .map_err(|error| error.to_string())?;
    // This lock is an outer Iceberg effect capability. It remains held across
    // the complete product transition, exactly as the former direct route.
    let _refresh_guard = acquire_mv_refresh_lock()?;
    let projection = IcebergDropProjection {
        readiness: ports.readiness.as_ref(),
    };
    let effects = IcebergDropEffects {
        ports,
        connector_context,
    };
    match product
        .drop(
            novarocks_mv_application::product::MvOperationContext {
                operation_id: uuid::Uuid::now_v7(),
            },
            product_target,
            stmt.if_exists,
            &projection,
            &effects,
            &effects,
        )
        .map_err(|error| error.to_string())?
    {
        novarocks_mv_application::product::MvProductResult::Acknowledged => Ok(StatementResult::Ok),
        novarocks_mv_application::product::MvProductResult::Dropped => {
            tracing::info!(
                "iceberg mv {}.{}.{}: dropped successfully",
                target.catalog,
                target.namespace,
                target.table
            );
            Ok(StatementResult::Ok)
        }
        novarocks_mv_application::product::MvProductResult::Created(_)
        | novarocks_mv_application::product::MvProductResult::Listed(_) => {
            Err("MV DROP product returned a non-DROP result".to_string())
        }
    }
}

struct IcebergDropProjection<'a> {
    readiness: &'a MvReadinessPort,
}

impl novarocks_mv_application::ports::MvDropProjectionPort for IcebergDropProjection<'_> {
    fn prepare_drop(
        &self,
        _operation: novarocks_mv_application::product::MvOperationContext,
        target: &novarocks_mv_application::product::MvTarget,
        if_exists: bool,
    ) -> Result<
        novarocks_mv_application::readiness::MvDropReadiness,
        novarocks_mv_application::ports::MvProviderFailure,
    > {
        let target = sql_target_from_product(target);
        self.readiness
            .prepare_drop(&target, if_exists)
            .map_err(drop_preflight_projection_failure)
    }

    fn delete_after_provider_drop(
        &self,
        operation: novarocks_mv_application::product::MvOperationContext,
        guard: novarocks_mv_application::readiness::MvProjectionDeleteGuard,
    ) -> Result<(), novarocks_mv_application::ports::MvProviderFailure> {
        match self
            .readiness
            .delete_after_provider_drop(operation.operation_id, guard)
            .map_err(drop_provider_failure)?
        {
            novarocks_mv_application::readiness::MvProjectionInstallOutcome::Removed
            | novarocks_mv_application::readiness::MvProjectionInstallOutcome::AlreadyAbsent => {
                Ok(())
            }
            novarocks_mv_application::readiness::MvProjectionInstallOutcome::Superseded => {
                Err(novarocks_mv_application::ports::MvProviderFailure::new(
                    novarocks_mv_application::ports::MvProviderFailureKind::TargetReplaced,
                    "MV projection changed after the provider DROP committed",
                ))
            }
            novarocks_mv_application::readiness::MvProjectionInstallOutcome::Installed(_)
            | novarocks_mv_application::readiness::MvProjectionInstallOutcome::Unchanged(_)
            | novarocks_mv_application::readiness::MvProjectionInstallOutcome::AlreadyProjectedElsewhere(_) => {
                Err(novarocks_mv_application::ports::MvProviderFailure::new(
                    novarocks_mv_application::ports::MvProviderFailureKind::Corruption,
                    "MV DROP finalization returned an installation outcome",
                ))
            }
        }
    }
}

struct IcebergDropEffects<'a> {
    ports: &'a IcebergMvCorePorts,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
}

impl novarocks_mv_application::ports::MvDropProviderPort for IcebergDropEffects<'_> {
    fn drop_target(
        &self,
        _operation: novarocks_mv_application::product::MvOperationContext,
        target: &novarocks_mv_application::product::MvTarget,
    ) -> Result<(), novarocks_mv_application::ports::MvProviderFailure> {
        let catalog = target.catalog().ok_or_else(|| {
            novarocks_mv_application::ports::MvProviderFailure::new(
                novarocks_mv_application::ports::MvProviderFailureKind::InvalidRequest,
                "Iceberg MV DROP target has no catalog",
            )
        })?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
            .map_err(|error| drop_provider_failure(error.to_string()))?;
        crate::connector::mutation::execute_catalog_mutation(
            self.ports.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::DropTable {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.namespace()),
                    table: Arc::from(target.name()),
                },
                policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
                data_disposition:
                    novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
            },
            self.connector_context.clone(),
        )
        .map(|_| ())
        .map_err(drop_provider_failure)
    }
}

impl novarocks_mv_application::ports::MvDropCatalogRegistrationPort for IcebergDropEffects<'_> {
    fn unregister_target(
        &self,
        _operation: novarocks_mv_application::product::MvOperationContext,
        target: &novarocks_mv_application::product::MvTarget,
    ) -> Result<(), novarocks_mv_application::ports::MvProviderFailure> {
        crate::catalog_application::query_catalog::drop_local_table_registration_if_exists(
            self.ports,
            target.namespace(),
            target.name(),
        )
        .map_err(drop_provider_failure)
    }
}

fn sql_target_from_product(target: &novarocks_mv_application::product::MvTarget) -> MvTarget {
    MvTarget {
        catalog: target.catalog().map(str::to_owned),
        database: target.namespace().to_owned(),
        name: target.name().to_owned(),
    }
}

/// Map the readiness owner's typed projection failure onto the product's
/// provider-failure vocabulary.
///
/// DROP preflight no longer reads a repository row: it asks the readiness owner
/// for the Ready projection, so the disagreement categories are the document
/// owner's, not the repository's. A target-state disagreement (the MV is
/// absent, or its management admission closed) is the statement's own problem
/// and keeps the owner's message verbatim; every other category is reported
/// with its load context.
fn drop_preflight_projection_failure(
    error: novarocks_mv_application::readiness::MvProjectionError,
) -> novarocks_mv_application::ports::MvProviderFailure {
    use novarocks_mv_application::readiness::MvProjectionErrorKind;
    let kind = match error.kind() {
        MvProjectionErrorKind::SourceConflict | MvProjectionErrorKind::Unsupported => {
            novarocks_mv_application::ports::MvProviderFailureKind::InvalidRequest
        }
        MvProjectionErrorKind::CorruptDocument => {
            novarocks_mv_application::ports::MvProviderFailureKind::Corruption
        }
        MvProjectionErrorKind::Unavailable
        | MvProjectionErrorKind::BudgetExceeded
        | MvProjectionErrorKind::Cancelled
        | MvProjectionErrorKind::DeadlineExceeded
        | MvProjectionErrorKind::Repository => {
            novarocks_mv_application::ports::MvProviderFailureKind::Unavailable
        }
    };
    let message = if error.kind() == MvProjectionErrorKind::SourceConflict {
        error.to_string()
    } else {
        format!("load iceberg mv definition for drop failed: {error}")
    };
    novarocks_mv_application::ports::MvProviderFailure::new(kind, message)
}

fn drop_provider_failure(
    error: impl ToString,
) -> novarocks_mv_application::ports::MvProviderFailure {
    novarocks_mv_application::ports::MvProviderFailure::new(
        novarocks_mv_application::ports::MvProviderFailureKind::Unavailable,
        error.to_string(),
    )
}

fn resolve_drop_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "DROP MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(name, current_database)?;
    Ok(IcebergMvTarget {
        catalog: novarocks_types::naming::normalize_identifier(catalog)?,
        namespace,
        table,
    })
}
