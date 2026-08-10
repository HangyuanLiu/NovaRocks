// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Iceberg-only activation for the current frontend-owned MV refresh route.
//!
//! This module is the sole current-production owner of Iceberg provenance
//! encoding, first-refresh payload construction, and provider writer
//! registration. The application and frontend exchange typed MV values and
//! generic connector contracts only.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use novarocks_connector_iceberg::commit::{
    MV_PROVENANCE_VERSION, MvProvenanceV1, ProvenanceBase, RefreshTechnique,
};
use novarocks_connector_iceberg::iceberg::{NamespaceIdent, TableIdent};
use novarocks_spi::connector::{
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationIntent,
    ConnectorManagedPublicationTechnique, ConnectorRequestContext,
    ConnectorStagedPublicationBaseFact, ConnectorWriteActivationIntent, ConnectorWriteInputRequest,
    ConnectorWriteLease, ConnectorWriteOperationId,
};

use crate::connector::iceberg::commit::CommitOpKind;
use crate::connector::iceberg::write_control::IcebergFirstRefreshWritePlanPayloadV2;
use crate::engine::StandaloneState;
use crate::mv::application::{
    MvRefreshCommittedFacts, MvRefreshProviderActivation, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique, PreparedMvFirstRefreshWrite, PreparedMvRefreshWrite,
};
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::request_context::QueryExecutionContext;

/// Core-side provider adapter installed into the frontend composition. It
/// retains only a weak engine reference, preventing a direct all-in-one
/// lifecycle path or a runtime-liveness cycle.
pub(crate) struct StandaloneMvRefreshProviderActivation {
    state: Weak<StandaloneState>,
}

impl StandaloneMvRefreshProviderActivation {
    pub(crate) fn new(state: Weak<StandaloneState>) -> Self {
        Self { state }
    }
}

impl MvRefreshProviderActivation for StandaloneMvRefreshProviderActivation {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, String> {
        let state = self.state.upgrade().ok_or_else(|| {
            "MV refresh provider activation is unavailable during engine shutdown".to_string()
        })?;
        match prepared {
            PreparedMvRefreshWrite::FirstRefresh(prepared) => {
                crate::engine::mv_first_refresh_staging::bind_prepared_mv_first_refresh_staging(
                    &state,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
            PreparedMvRefreshWrite::Incremental(prepared) => {
                crate::engine::mv::iceberg_refresh::bind_prepared_mv_incremental_staging(
                    &state,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
        }
    }

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &novarocks_spi::connector::ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String> {
        MvRefreshCommittedFacts::from_write_receipt(intent, receipt)
    }
}

/// Encode application-owned intent through the existing Iceberg v1
/// provenance codec. The placeholder row count is replaced by the provider
/// commit path using the resulting table fact, exactly as before D2.
pub(crate) fn iceberg_publication_properties(
    intent: &MvRefreshPublicationIntent,
) -> Result<BTreeMap<String, String>, String> {
    let technique = match intent.technique() {
        MvRefreshPublicationTechnique::Full => RefreshTechnique::Full,
        MvRefreshPublicationTechnique::Incremental => RefreshTechnique::Incremental,
    };
    let bases = intent
        .bases()
        .iter()
        .map(|base| ProvenanceBase {
            table_fqn: base.table_fqn().to_string(),
            uuid: base.table_uuid().to_string(),
            from_snapshot: base.from_snapshot(),
            to_snapshot: base.to_snapshot(),
        })
        .collect();
    MvProvenanceV1 {
        provenance_version: MV_PROVENANCE_VERSION,
        refresh_id: intent.refresh_id(),
        mv_id: intent.mv_id(),
        token: intent.marker_token().to_string(),
        technique,
        bases,
        definition_fingerprint: intent.definition_fingerprint().to_string(),
        rows: 0,
    }
    .to_summary_properties()
}

/// Register a first-refresh writer from the exact C1 preparation. No caller
/// may provide an Iceberg payload or construct a second preparation.
pub(crate) fn activate_first_refresh_connector_write(
    state: &Arc<StandaloneState>,
    prepared: &PreparedMvFirstRefreshWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    if prepared.observed_binding() != exact_lease.binding_key() {
        return Err("MV first-refresh write lease drifted from prepared binding".to_string());
    }
    if prepared.target_table().owner() != &exact_lease.binding_key().instance_id {
        return Err(
            "MV first-refresh staging table belongs to a different connector instance".to_string(),
        );
    }
    let operation_id: ConnectorWriteOperationId = prepared.operation_id();
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    let entry = state
        .iceberg_catalogs
        .read()
        .map_err(|error| {
            format!("read Iceberg catalog registry for first-refresh activation: {error}")
        })?
        .get(&target.catalog)?;
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let target_table =
        crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)
            .map_err(|error| format!("reload MV first-refresh staging target: {error}"))?
            .into_table();
    validate_first_refresh_target_contract(&target_table, prepared.target_contract())?;
    let ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let collector = crate::mv::refresh::change_stream_write::new_iceberg_mv_commit_collector(
        &target_table,
        &ident,
        prepared.staging_branch(),
        match prepared.write_mode() {
            crate::mv::application::MvStagedRefreshWriteMode::Append => CommitOpKind::FastAppend,
            crate::mv::application::MvStagedRefreshWriteMode::FullOverwrite => {
                CommitOpKind::Overwrite
            }
        },
    );
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = Arc::new(
        crate::connector::iceberg::write_commit::IcebergWriteCommitExecutor {
            catalog,
            table: target_table.clone(),
            collector: Arc::clone(&collector),
            fs: abort_cleanup.fs,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_rewrite: None,
            target_ref: prepared.staging_branch().to_string(),
            snapshot_properties: BTreeMap::new(),
        },
    );
    let payload = IcebergFirstRefreshWritePlanPayloadV2 {
        version: 2,
        target: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
        target_ref: prepared.staging_branch().to_string(),
        expected_snapshot_id: prepared.expected_target_snapshot_id(),
        staging_path: collector.staging_dir.clone(),
        provenance_properties: iceberg_publication_properties(prepared.publication_intent())?,
    };
    let intent = match prepared.write_mode() {
        crate::mv::application::MvStagedRefreshWriteMode::Append => {
            novarocks_spi::connector::ConnectorWriteIntent::Append
        }
        crate::mv::application::MvStagedRefreshWriteMode::FullOverwrite => {
            novarocks_spi::connector::ConnectorWriteIntent::Overwrite
        }
    };
    let empty_input_policy = match prepared.write_mode() {
        crate::mv::application::MvStagedRefreshWriteMode::Append => {
            crate::connector::iceberg::write_service::IcebergMvPrimaryEmptyInputPolicy::AbortWithoutSnapshot
        }
        crate::mv::application::MvStagedRefreshWriteMode::FullOverwrite => {
            crate::connector::iceberg::write_service::IcebergMvPrimaryEmptyInputPolicy::CommitEmptyOverwrite
        }
    };
    let preparation = crate::engine::iceberg_writer::prepare_iceberg_connector_write(
        exact_lease,
        &target,
        prepared.staging_branch(),
        intent,
        ConnectorWriteInputRequest::Data {
            fields: prepared
                .target_contract()
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                        field.as_ref().clone(),
                    )
                })
                .collect(),
        },
        novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
        connector_context.clone(),
    )?;
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    crate::connector::iceberg::provider::register_iceberg_first_refresh_write_service_from_preparation(
        services,
        operation_id,
        &preparation,
        payload,
        &entry,
        commit_executor,
        empty_input_policy,
    )
    .map_err(|error| format!("activate Iceberg first-refresh writer from preparation: {error}"))?;
    let managed_publication = ConnectorManagedPublicationIntent::try_new(
        prepared.publication_intent().refresh_id(),
        prepared.publication_intent().mv_id(),
        prepared.publication_intent().marker_token(),
        match prepared.publication_intent().technique() {
            MvRefreshPublicationTechnique::Full => ConnectorManagedPublicationTechnique::Full,
            MvRefreshPublicationTechnique::Incremental => {
                ConnectorManagedPublicationTechnique::Incremental
            }
        },
        prepared
            .publication_intent()
            .bases()
            .iter()
            .map(|base| ConnectorStagedPublicationBaseFact {
                table: base.table_fqn().into(),
                uuid: base.table_uuid().into(),
                from_version: base.from_snapshot(),
                to_version: base.to_snapshot(),
            })
            .collect(),
        prepared.publication_intent().definition_fingerprint(),
        match empty_input_policy {
            crate::connector::iceberg::write_service::IcebergMvPrimaryEmptyInputPolicy::AbortWithoutSnapshot => {
                ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit
            }
            crate::connector::iceberg::write_service::IcebergMvPrimaryEmptyInputPolicy::CommitEmptyOverwrite => {
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
            }
        },
    )
    .map_err(|error| format!("build managed MV publication activation intent: {error}"))?;
    crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared_with_intent(
        operation_id,
        preparation,
        ConnectorWriteActivationIntent::ManagedPublication(managed_publication),
        connector_context,
        exact_lease.clone(),
    )
    .map_err(|error| format!("activate exact Iceberg MV write generation: {error}"))
}

fn validate_first_refresh_target_contract(
    target_table: &novarocks_connector_iceberg::iceberg::table::Table,
    contract: &crate::sql::mv_refresh::first_refresh::MvFirstRefreshTargetContract,
) -> Result<(), String> {
    let actual_schema = target_table.metadata().current_schema();
    let actual_arrow_schema =
        novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(actual_schema)
            .map_err(|error| {
                format!("convert MV first-refresh activation schema to Arrow: {error}")
            })?;
    let actual_field_ids = actual_schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    contract.validate_observed(
        &actual_arrow_schema,
        &actual_field_ids,
        target_table.metadata().default_partition_spec_id(),
    )
}
