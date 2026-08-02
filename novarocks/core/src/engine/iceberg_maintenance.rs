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

//! Connector-facing Iceberg table-maintenance execution.
//!
//! SQL parsing, application dispatch, and result encoding belong to
//! `novarocks-frontend`. This module retains live catalog, snapshot, file,
//! commit, cache, row-lineage, and MV snapshot-adoption truth.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::remove_orphan_files::run_remove_orphan_files;
use crate::engine::StandaloneState;
use crate::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
};
use novarocks_fs::ObjectStoreConfig;

/// Connector handles shared by synchronous and worker-driven maintenance.
pub(crate) type MaintenanceCatalogTriple =
    (Arc<dyn Catalog>, TableIdent, Option<ObjectStoreConfig>);

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
        MaintenanceActionRequest::RemoveOrphanFiles {
            target,
            older_than_ms,
        } => run_remove_orphan_files_action(state, target, older_than_ms),
    }
}

pub(crate) fn current_snapshot_id(
    state: &Arc<StandaloneState>,
    target: &MaintenanceTarget,
) -> Result<i64, String> {
    let (catalog, table_ident, _) =
        resolve_maintenance_catalog(state, &target.catalog, &target.namespace, &target.table)?;
    let table = block_on_iceberg(async move { catalog.load_table(&table_ident).await })?.map_err(
        |error| {
            format!(
                "load iceberg table {} for maintenance failed: {error}",
                action_target(target)
            )
        },
    )?;
    table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
        .ok_or_else(|| {
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

fn run_remove_orphan_files_action(
    state: &Arc<StandaloneState>,
    target: MaintenanceTarget,
    older_than_ms: i64,
) -> Result<MaintenanceActionOutcome, String> {
    let (catalog, table_ident, object_store_config) =
        resolve_maintenance_catalog(state, &target.catalog, &target.namespace, &target.table)?;
    let outcome = block_on_iceberg(async move {
        run_remove_orphan_files(
            catalog,
            table_ident,
            older_than_ms,
            object_store_config.as_ref(),
        )
        .await
    })?
    .map_err(|error| {
        format!(
            "REMOVE ORPHAN FILES failed for {}: {error}",
            action_target(&target)
        )
    })?;

    tracing::info!(
        deleted_count = outcome.deleted_count,
        scanned_count = outcome.scanned_count,
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        older_than_ms,
        "remove_orphan_files: completed"
    );

    Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
        orphan_file_locations: outcome.deleted_file_locations,
    })
}

/// Resolve a registered Iceberg catalog into an executable connector handle.
pub(crate) fn resolve_maintenance_catalog(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table: &str,
) -> Result<MaintenanceCatalogTriple, String> {
    let resolved_table = resolve_maintenance_table_name(state, catalog_name, namespace, table)?;
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?;
        registry.get(catalog_name)?
    };
    entry.invalidate_table_cache(namespace, table);
    if resolved_table != table {
        entry.invalidate_table_cache(namespace, &resolved_table);
    }
    let object_store_config = entry.object_store_config().cloned();
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(NamespaceIdent::new(namespace.to_string()), resolved_table);
    Ok((catalog, table_ident, object_store_config))
}

fn resolve_maintenance_table_name(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table: &str,
) -> Result<String, String> {
    let (resolved, _) = {
        crate::connector::metadata_load_table(
            state.connector_control.as_ref(),
            crate::connector::connector_request_context(
                None,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )?,
            catalog_name,
            namespace,
            table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?
    };
    Ok(resolved.table)
}

fn action_target(target: &MaintenanceTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}
