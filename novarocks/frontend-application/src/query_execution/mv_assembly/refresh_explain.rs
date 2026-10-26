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
) -> Result<Vec<String>, novarocks_sql::compiler::SqlCompileError> {
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
pub fn explain_iceberg_mv_refresh_rewrite_plan_from_rewrite(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvRefreshRequest,
    rewrite: Arc<IcebergMvRewriteContext>,
    target_planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, novarocks_sql::compiler::SqlCompileError> {
    let target = resolve_refresh_target(current_catalog, current_database, &stmt.name_parts)?;
    if rewrite.target.catalog != target.catalog
        || rewrite.target.namespace != target.namespace
        || rewrite.target.table != target.table
    {
        return Err(
            "EXPLAIN REFRESH target differs from its canonical rewrite context"
                .to_string()
                .into(),
        );
    }
    let target_binding =
        crate::mv::domain::refresh::target_binding::load_mv_target_binding_with_lease_and_ports(
            ports.storage_observation(),
            &rewrite.target,
            target_planning_lease.clone(),
            connector_context,
        )?;
    validate_target_snapshot(
        &target,
        &rewrite.mv_definition,
        &target_binding,
        ports.management_entrance()?.as_ref(),
        connector_context,
    )?;
    if target_binding.table_uuid() != rewrite.target_table_uuid {
        return Err(
            "EXPLAIN REFRESH target UUID differs from its rewrite context"
                .to_string()
                .into(),
        );
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
    compile_captured_refresh_explain(
        ports,
        &rewrite,
        target_binding_id,
        &catalog,
        level,
        connector_context,
    )
}

/// The unique pure SQL explain author; its catalog cannot perform live I/O.
fn compile_captured_refresh_explain(
    ports: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target_binding_id: novarocks_sql::binding::SqlTableBindingId,
    catalog: &dyn novarocks_sql::compiler::SqlCatalogSnapshot,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, novarocks_sql::compiler::SqlCompileError> {
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
            catalog,
            functions: ports.function_catalog().as_ref(),
            constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
            constant_policy: ports.constant_policy(),
            control: crate::query_execution::planning::sql_compile_control_from_connector_request(
                connector_context,
            ),
            level,
        },
    )
}

/// Read-only inputs; no refresh identity, publication admission or writer.
pub(crate) struct MvExplainPreparationInput {
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) statement: MvRefreshRequest,
    pub(crate) level: novarocks_sql::compiler::ExplainLevel,
    pub(crate) context: novarocks_spi::connector::ConnectorRequestContext,
}

/// Suspends only at the existing named I/O gate. The original reader executes
/// the pure turns and retains all exact observations across every suspension.
pub(crate) async fn explain_original(
    input: MvExplainPreparationInput,
    io: &super::refresh_preparation::io_gate::MvRefreshPreparationIo,
) -> Result<Vec<String>, crate::mv::domain::application::MvApplicationError> {
    explain_original_inner(input, io)
        .await
        .map_err(|error| error.0)
}
async fn explain_original_inner(
    input: MvExplainPreparationInput,
    io: &super::refresh_preparation::io_gate::MvRefreshPreparationIo,
) -> Result<Vec<String>, super::refresh_preparation::async_facts::PreparationError> {
    use super::refresh_preparation::async_original::{
        MvCatalogCaptureInput, MvCatalogCaptureQuery, capture_catalog,
    };
    use crate::mv::domain::refresh::rewrite_context::RefreshRewriteInputs;
    let ports = &input.ports;
    let context = &input.context;
    let target = resolve_refresh_target(
        input.current_catalog.as_deref(),
        &input.current_database,
        &input.statement.name_parts,
    )?;
    let projection = io.definition(ports, &target).await?;
    let binding = io.target_binding(ports, &target, context).await?;
    let schema = crate::mv::domain::storage_observation::observe_schema_validation(
        ports.storage_observation(),
        binding.lease(),
        binding.metadata(),
        context.clone(),
    )?;
    let runtime_bindings = crate::mv::domain::refresh::schema_contract::validate_projection_target(
        &projection,
        &schema,
    )?;
    let pin = io.capture_pin(ports, &projection, context).await?;
    let baseline =
        crate::mv::domain::iceberg_refresh::build_refresh_state_baseline(&projection, &binding)?;
    let query = super::refresh_preparation::canonical_mv_select_query(&projection)?;
    let aggregate = super::refresh_preparation::pure_reader::frozen_refresh_aggregate_analysis(
        io,
        ports,
        &projection,
        &query,
        context,
    )
    .await?;
    let lease = binding.lease().clone();
    let rewrite =
        crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
            io,
            RefreshRewriteInputs {
                connector_control: ports.connector_control(),
                connector_context: context,
                projection: Arc::new(projection),
                pin: &pin,
                state_baseline: &baseline,
                target_binding: &binding,
                target_observation: &schema,
                runtime_bindings: &runtime_bindings,
                join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(&query)?,
                aggregate,
            },
        )
        .await?;
    let table = novarocks_types::naming::TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    let current_binding = io
        .target_binding_with_lease(ports.storage_observation(), &table, lease.clone(), context)
        .await?;
    crate::mv::domain::iceberg_refresh::async_preparation::validate_target_snapshot_original(
        io,
        ports,
        &target,
        &rewrite.mv_definition,
        &current_binding,
        context,
    )
    .await?;
    if current_binding.table_uuid() != rewrite.target_table_uuid {
        return Err("EXPLAIN REFRESH target UUID differs from its rewrite context".into());
    }
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding_id =
        super::query_local_bindings::bind_imv_target_query_table_in_store_from_rewrite_original(
            io, &rewrite, &bindings, &lease, context, None,
        )
        .await?;
    let overlays = super::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs_original(
        io, ports.connector_control(), context, &rewrite,
    ).await?;
    let captured = capture_catalog(
        MvCatalogCaptureInput {
            query: MvCatalogCaptureQuery::Query(Box::new(
                (*rewrite.canonical_select_query).clone(),
            )),
            ports: ports.clone(),
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            context: context.clone(),
            control: crate::query_execution::planning::sql_compile_control_from_connector_request(
                context,
            ),
            bindings,
            overlays,
        },
        io,
    )
    .await?;
    compile_captured_refresh_explain(
        ports,
        &rewrite,
        target_binding_id,
        &captured.catalog,
        input.level,
        context,
    )
    .map_err(Into::into)
}
