// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Iceberg-private adapter from one frozen rewrite cohort to the generic C1
//! distributed writer.  The only generic inputs are an opaque frozen source,
//! an exact rewrite lease, and a sealed writer registration.  File ownership,
//! Iceberg table metadata, and sink construction stay in this module.

use std::sync::Arc;

use novarocks_spi::connector::{ConnectorRequestContext, ConnectorWriteCohortId};

use crate::connector::backend::ResolvedTable;
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::query_execution::distributed_rewrite::{
    ConnectorDistributedRewriteSession, FrozenRewriteReadResolver,
    frozen_rewrite_scan_physical_plan, plan_frozen_rewrite_connector_read,
};
use crate::query_execution::outcome::{ConnectorWriteCompletion, ConnectorWriteStagingSummary};
use crate::query_execution::request_context::QueryExecutionContext;
use crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec;

use super::catalog::registry::load_table;
/// Stage one sealed frozen cohort.  The caller is responsible for recording
/// the returned completion as accepted or superseded before any aggregate
/// commit is attempted.
pub(crate) fn stage_frozen_rewrite_cohort(
    state: &Arc<StandaloneState>,
    session: &ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    execution: &QueryExecutionContext,
    context: &ConnectorRequestContext,
) -> Result<(ConnectorWriteCompletion, ConnectorWriteStagingSummary), String> {
    let cohort = session
        .plan()
        .cohorts()
        .iter()
        .find(|candidate| candidate.cohort_id() == cohort_id)
        .ok_or_else(|| "distributed rewrite execution names an unknown cohort".to_string())?;
    let read = plan_frozen_rewrite_connector_read(
        session.lease(),
        execution.topology(),
        cohort.source(),
        (0..cohort.input_schema().fields().len()).collect(),
        context.clone(),
    )
    .map_err(|error| format!("plan frozen rewrite source: {error}"))?;
    let resolver = FrozenRewriteReadResolver::new(read);
    let physical_plan = frozen_rewrite_scan_physical_plan(cohort.input_schema());
    let sink_spec = build_rewrite_sink_spec(state, session, cohort_id)?;
    let registration = session
        .execution_registration(cohort_id)
        .map_err(|error| format!("register frozen rewrite cohort: {error}"))?;
    crate::engine::execute_frozen_rewrite_physical_plan_as_iceberg_staging(
        state,
        physical_plan,
        sink_spec,
        Some(execution),
        context,
        &resolver,
        registration,
    )
}

fn build_rewrite_sink_spec(
    state: &Arc<StandaloneState>,
    session: &ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
) -> Result<IcebergWriteSinkSpec, String> {
    let (namespace, table_name) =
        super::provider::decode_data_mutation_table_target(session.plan().target())
            .map_err(|error| format!("decode distributed rewrite target: {error}"))?;
    let catalog = session
        .lease()
        .binding_key()
        .instance_id
        .as_str()
        .to_string();
    let entry = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("read Iceberg rewrite catalog registry: {error}"))?
        .get(&catalog)?;
    let loaded = load_table(&entry, &namespace, &table_name)
        .map_err(|error| format!("load distributed rewrite target table: {error}"))?;
    let target = TargetBackend {
        backend_name: "iceberg",
        catalog: catalog.clone(),
        namespace: namespace.clone(),
        table: table_name.clone(),
    };
    let columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
        loaded.table.metadata().current_schema(),
    )?;
    let resolved = ResolvedTable {
        catalog,
        namespace,
        table: table_name,
        columns: columns.clone(),
        statistics_pin: None,
    };
    match session.plan().operation_kind() {
        novarocks_spi::connector::REWRITE_DATA_FILES_KIND => {
            let cohort = session
                .plan()
                .cohorts()
                .iter()
                .find(|candidate| candidate.cohort_id() == cohort_id)
                .ok_or_else(|| {
                    "distributed rewrite execution names an unknown cohort".to_string()
                })?;
            if cohort.input_schema().fields().len() != columns.len() {
                return Err(
                    "frozen data rewrite input schema does not match target schema".to_string(),
                );
            }
            crate::engine::iceberg_writer::build_insert_write_sink_spec(
                &target,
                &resolved,
                &loaded.table,
                &entry,
                &columns,
            )
        }
        novarocks_spi::connector::REWRITE_POSITION_DELETES_KIND => {
            crate::engine::iceberg_writer::build_position_delete_sink_spec(
                &target,
                &resolved,
                &loaded.table,
                &entry,
            )
        }
        kind => Err(format!(
            "unsupported distributed rewrite operation kind `{kind}`"
        )),
    }
}
