// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Native, result-free consumer for a prepared MV first-refresh append.
//!
//! This module is deliberately not wired into the production REFRESH route.
//! MVX-2W exercises it through the native fixture; MVX-2 will make the route
//! switch only after that fixture proves the data plane.

use std::sync::Arc;

use iceberg::{NamespaceIdent, TableIdent};
use novarocks_spi::connector::{ConnectorWriteLease, ConnectorWriteOperationId};

use crate::connector::iceberg::commit::CommitOpKind;
use crate::connector::iceberg::write_control::IcebergFirstRefreshWritePlanPayloadV2;
use crate::engine::{
    IcebergWriteRootDistributionResolver, StandaloneState,
    execute_logical_plan_as_iceberg_staging_in_operation_with_connector_context,
    execute_query_as_iceberg_staging_in_operation_with_connector_context,
    iceberg_write_shuffle_by_output_name,
};
use crate::query_execution::contract::ConnectorWriteExecutionRegistration;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::query_execution::{ConnectorWriteCompletion, ConnectorWriteStagingSummary};
use crate::sql::mv_refresh::first_refresh::{
    MvFirstRefreshExecutionArtifact, PreparedMvFirstRefreshWrite,
};
use crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec;

/// Build the ordinary data-writer sink from the target frozen by the MV
/// refresh context.  This adapter owns concrete Iceberg table metadata; the
/// SQL prepared artifact itself remains provider-neutral and contains none of
/// these catalog handles.
pub(crate) fn build_mv_first_refresh_sink_spec(
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
) -> Result<IcebergWriteSinkSpec, String> {
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: ctx.rewrite.target.catalog.clone(),
        namespace: ctx.rewrite.target.namespace.clone(),
        table: ctx.rewrite.target.table.clone(),
    };
    let target_table = ctx.target_bindings.runtime().target_table();
    let columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
        target_table.metadata().current_schema(),
    )?;
    let resolved = crate::connector::backend::ResolvedTable {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        columns: columns.clone(),
        statistics_pin: None,
    };
    crate::engine::iceberg_writer::build_insert_write_sink_spec(
        &target,
        &resolved,
        target_table,
        ctx.target_bindings.runtime().target_entry(),
        &columns,
    )
}

/// Reserve the one primary first-refresh write cohort from facts frozen in an
/// MV refresh context. The staging branch must already exist and `exact_lease`
/// must have been derived from the retained target control binding. This is
/// the first point that mutates the provider write-service registry; SQL
/// artifact preparation remains side-effect free.
pub(crate) fn activate_mv_first_refresh_connector_write(
    state: &Arc<StandaloneState>,
    ctx: &crate::mv::refresh::execution_context::IcebergMvRefreshContext,
    prepared: &PreparedMvFirstRefreshWrite,
    staging_branch: &str,
    provenance_properties: std::collections::BTreeMap<String, String>,
    exact_lease: &ConnectorWriteLease,
) -> Result<
    (
        IcebergWriteSinkSpec,
        crate::query_execution::contract::ConnectorWritePlanningTemplate,
    ),
    String,
> {
    if prepared.observed_binding() != exact_lease.binding_key() {
        return Err("MV first-refresh write lease drifted from prepared binding".to_string());
    }
    let sink_spec = build_mv_first_refresh_sink_spec(ctx)?;
    let operation_id: ConnectorWriteOperationId = prepared.operation_id();
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: ctx.rewrite.target.catalog.clone(),
        namespace: ctx.rewrite.target.namespace.clone(),
        table: ctx.rewrite.target.table.clone(),
    };
    let target_table = ctx.target_bindings.runtime().target_table();
    let ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let collector = crate::mv::refresh::change_stream_write::new_iceberg_mv_commit_collector(
        target_table,
        &ident,
        staging_branch,
        CommitOpKind::FastAppend,
    );
    let entry = ctx.target_bindings.runtime().target_entry();
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(entry)?;
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(entry)?;
    let commit_executor = Arc::new(crate::engine::IcebergWriteCommitExecutor {
        state: Arc::downgrade(state),
        target: target.clone(),
        catalog,
        table: target_table.clone(),
        collector: Arc::clone(&collector),
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: staging_branch.to_string(),
        snapshot_properties: std::collections::BTreeMap::new(),
    });
    let payload = IcebergFirstRefreshWritePlanPayloadV2 {
        version: 2,
        target: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
        target_ref: staging_branch.to_string(),
        expected_snapshot_id: ctx.rewrite.target_snapshot_id,
        staging_path: collector.staging_dir.clone(),
        provenance_properties,
    };
    let writer_handle_payload =
        crate::connector::iceberg::write_contract::encode_data_sink_spec_handle_payload(
            &sink_spec,
        )?;
    let template = crate::engine::iceberg_writer::activate_iceberg_first_refresh_connector_write(
        state,
        &target,
        staging_branch,
        Arc::clone(prepared.target_contract().schema()),
        writer_handle_payload,
        payload,
        commit_executor,
        operation_id,
        prepared.connector_context().clone(),
        exact_lease,
    )?;
    Ok((sink_spec, template))
}

pub(crate) fn execute_prepared_mv_first_refresh_staging(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    prepared: PreparedMvFirstRefreshWrite,
    sink_spec: IcebergWriteSinkSpec,
    execution: &QueryExecutionContext,
    registration: ConnectorWriteExecutionRegistration,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
) -> Result<(ConnectorWriteCompletion, ConnectorWriteStagingSummary), String> {
    if registration.session().operation_id() != prepared.operation_id()
        || registration.cohort_id() != prepared.primary_cohort()
    {
        return Err("MV first-refresh staging registration identity mismatch".to_string());
    }
    if sink_spec.target_columns.len() != prepared.target_contract().schema().fields().len() {
        return Err(
            "MV first-refresh staging sink schema does not match target contract".to_string(),
        );
    }
    let connector_context = prepared.connector_context().clone();
    let root_hash_column = prepared.root_hash_column().to_string();
    match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let query = parse_query_from_sql(physical_sql.sql())?;
            let root_distribution: IcebergWriteRootDistributionResolver =
                iceberg_write_shuffle_by_output_name(root_hash_column);
            execute_query_as_iceberg_staging_in_operation_with_connector_context(
                state,
                current_catalog,
                current_database,
                &query,
                sink_spec,
                None,
                Some(root_distribution),
                Some(execution),
                &connector_context,
                registration,
            )
        }
        MvFirstRefreshExecutionArtifact::Logical(logical) => {
            let mv_refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "MV first-refresh logical staging requires its frozen refresh context".to_string()
            })?;
            let (logical_plan, factory) = logical.into_parts();
            execute_logical_plan_as_iceberg_staging_in_operation_with_connector_context(
                state,
                logical_plan,
                factory,
                sink_spec,
                iceberg_write_shuffle_by_output_name(root_hash_column),
                execution,
                &connector_context,
                mv_refresh_ctx,
                registration,
            )
        }
    }
}

fn parse_query_from_sql(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("MV first-refresh physical artifact is not a SELECT query".to_string());
    };
    Ok(*query)
}
