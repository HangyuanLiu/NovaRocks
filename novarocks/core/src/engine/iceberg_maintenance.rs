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

//! Connector-facing table-maintenance execution.
//!
//! SQL parsing, application dispatch, and result encoding belong to
//! `novarocks-frontend`. Catalog, snapshot, file and commit truth belongs to
//! the Connector; this module only routes maintenance intents to it and shapes
//! the neutral outcome the frontend reports.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
};

pub(crate) fn execute_action(
    state: &Arc<StandaloneState>,
    request: MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    match request {
        MaintenanceActionRequest::RewriteDataFiles { .. }
        | MaintenanceActionRequest::RewritePositionDeleteFiles { .. } => Err(
            "distributed rewrite must be dispatched by the frontend table-maintenance owner"
                .to_string(),
        ),
        MaintenanceActionRequest::RewriteManifests {
            target,
            use_caching,
            spec_id,
        } => run_rewrite_manifests_action(state, target, use_caching, spec_id),
        MaintenanceActionRequest::ExpireSnapshots {
            target,
            older_than_ms,
            retain_last,
        } => run_expire_snapshots_action(state, target, older_than_ms, retain_last),
        MaintenanceActionRequest::RemoveOrphanFiles { .. } => Err(
            "remove orphan files must be dispatched by the frontend durable cleanup owner"
                .to_string(),
        ),
    }
}

pub(crate) fn current_snapshot_id(
    state: &Arc<StandaloneState>,
    target: &MaintenanceTarget,
) -> Result<i64, String> {
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let exact_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &target.catalog,
    )?;
    let facts = crate::connector::metadata_read_reference_facts_with_planning_lease(
        exact_lease,
        context,
        &target.namespace,
        &target.table,
    )?;
    facts.current_snapshot_id().ok_or_else(|| {
        format!(
            "iceberg table {} has no current snapshot",
            action_target(target)
        )
    })
}

fn run_rewrite_manifests_action(
    state: &Arc<StandaloneState>,
    target: MaintenanceTarget,
    use_caching: Option<bool>,
    spec_id: Option<i32>,
) -> Result<MaintenanceActionOutcome, String> {
    if use_caching.is_some() {
        return Err(
            "rewrite_manifests `use_caching` is not implemented in NovaRocks yet".to_string(),
        );
    }
    if spec_id.is_some() {
        return Err("rewrite_manifests `spec_id` is not implemented in NovaRocks yet".to_string());
    }
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    let completed = crate::connector::metadata_maintenance::execute_metadata_maintenance(
        state.connector_control.as_ref(),
        state.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        },
        crate::connector::metadata_maintenance::MetadataMaintenanceIntent::rewrite_metadata_layout(
        ),
        crate::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )?,
    )?;
    let summary = completed.receipt.summary();

    Ok(MaintenanceActionOutcome::RewriteManifests {
        rewritten_manifests_count: i32::try_from(summary.rewritten_items)
            .map_err(|_| "rewrite manifest count exceeds Spark result range".to_string())?,
        added_manifests_count: i32::try_from(summary.added_items)
            .map_err(|_| "added manifest count exceeds Spark result range".to_string())?,
    })
}

fn run_expire_snapshots_action(
    state: &Arc<StandaloneState>,
    target: MaintenanceTarget,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
) -> Result<MaintenanceActionOutcome, String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    crate::connector::metadata_maintenance::execute_metadata_maintenance(
        state.connector_control.as_ref(),
        state.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        },
        crate::connector::metadata_maintenance::MetadataMaintenanceIntent::expire_table_versions(
            older_than_ms,
            retain_last,
        ),
        crate::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )?,
    )?;

    tracing::info!(
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        "expire_snapshots: completed"
    );

    Ok(MaintenanceActionOutcome::ExpireSnapshots {
        deleted_data_files_count: None,
        deleted_position_delete_files_count: None,
        deleted_equality_delete_files_count: None,
        deleted_manifest_files_count: None,
        deleted_manifest_lists_count: None,
        deleted_statistics_files_count: None,
    })
}

fn action_target(target: &MaintenanceTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}
