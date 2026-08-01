// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Native, result-free consumer for a prepared MV first-refresh append.
//!
//! This module is deliberately not wired into the production REFRESH route.
//! MVX-2W exercises it through the native fixture; MVX-2 will make the route
//! switch only after that fixture proves the data plane.

use std::sync::Arc;

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
