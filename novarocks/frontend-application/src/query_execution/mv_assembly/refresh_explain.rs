//! Frontend-owned assembly for `EXPLAIN REFRESH MATERIALIZED VIEW`.

use std::sync::Arc;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::mv::domain::application::MvRefreshRequest;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::refresh::target::{resolve_refresh_target, validate_target_snapshot};
use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;
use crate::query_execution::mv_assembly::query_local_bindings::{
    bind_imv_target_query_table_in_store_from_rewrite,
    freeze_imv_base_query_local_overlays_from_captured_inputs,
};

/// Compiles an EXPLAIN refresh plan from the exact frozen MV ports and
/// query-local bindings used by refresh preparation.
pub fn explain_iceberg_mv_refresh_rewrite_plan_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvRefreshRequest,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, String> {
    let (rewrite, target_planning_lease) =
        crate::query_execution::mv_assembly::refresh_preparation::freeze_statement_refresh_rewrite_context(
            ports,
            current_catalog,
            current_database,
            &stmt.name_parts,
            connector_context,
        )?;
    explain_iceberg_mv_refresh_rewrite_plan_from_rewrite(
        ports,
        current_catalog,
        current_database,
        stmt,
        rewrite,
        &target_planning_lease,
        level,
        connector_context,
    )
}

/// EXPLAIN from an already frozen rewrite context and its planning lease.
#[expect(
    clippy::too_many_arguments,
    reason = "EXPLAIN keeps the frozen rewrite, its lease, and the request context explicit."
)]
pub fn explain_iceberg_mv_refresh_rewrite_plan_from_rewrite(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvRefreshRequest,
    rewrite: Arc<IcebergMvRewriteContext>,
    target_planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, String> {
    let target = resolve_refresh_target(current_catalog, current_database, &stmt.name_parts)?;
    if rewrite.target.catalog != target.catalog
        || rewrite.target.namespace != target.namespace
        || rewrite.target.table != target.table
    {
        return Err(
            "EXPLAIN REFRESH target differs from its canonical rewrite context".to_string(),
        );
    }
    let target_binding =
        crate::mv::domain::refresh::target_binding::load_mv_target_binding_with_lease_and_ports(
            ports.storage_observation(),
            &rewrite.target,
            target_planning_lease.clone(),
            connector_context,
        )?;
    validate_target_snapshot(&target, &rewrite.mv_definition, &target_binding)?;
    if target_binding.table_uuid() != rewrite.target_table_uuid {
        return Err("EXPLAIN REFRESH target UUID differs from its rewrite context".to_string());
    }
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding_id = bind_imv_target_query_table_in_store_from_rewrite(
        &rewrite,
        &bindings,
        target_binding.lease(),
        connector_context,
        None,
    )?;
    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(ports);
    let overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        ports.connector_control(),
        connector_context,
        &rewrite,
    )?;
    let materializer = crate::catalog_application::query_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        rewrite.current_catalog.as_deref(),
        &catalog_service_snapshot,
        Arc::clone(&bindings),
        crate::catalog_application::query_materializer::iceberg_table_binding_loader(
            ports.connector_control(),
            connector_context.clone(),
        ),
        overlays,
    );
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
    novarocks_sql::compiler::compile_imv_refresh_explain_lines(
        novarocks_sql::compiler::SqlImvRefreshExplainContext {
            canonical_query: Box::new((*rewrite.canonical_select_query).clone()),
            imv_rewrite: novarocks_sql::compiler::SqlImvPlanningInput::new(
                rewrite.to_sql_rewrite_snapshot(target_binding_id)?,
                novarocks_sql::compiler::SqlImvRewriteValidation::None,
            ),
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings::default(),
            environment: novarocks_sql::compiler::SqlPlanningEnvironment::NotApplicable,
            catalog: &catalog,
            functions: ports.function_catalog().as_ref(),
            constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
            control: novarocks_sql::compiler::SqlCompileControl::new(
                Some(connector_context.deadline()),
                Arc::new(MvRefreshConnectorStopObservation {
                    stop: connector_context.stop().clone(),
                }),
            ),
            level,
        },
    )
    .map_err(|error| error.to_string())
}

struct MvRefreshConnectorStopObservation {
    stop: novarocks_spi::connector::ConnectorStopView,
}

impl novarocks_sql::compiler::SqlCancellationObservation for MvRefreshConnectorStopObservation {
    fn is_cancelled(&self) -> bool {
        self.stop.is_stopped()
    }
}
