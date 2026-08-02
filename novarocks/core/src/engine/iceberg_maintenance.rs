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
use crate::connector::iceberg::commit::rewrite_position_delete_files::{
    RewritePositionDeleteOptions, run_rewrite_position_delete_files,
};
use crate::connector::iceberg::compact::{
    WholeTableRewriteResult, WholeTableRewriteTarget,
    execute_whole_table_rewrite_with_metrics_for_target,
};
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
        MaintenanceActionRequest::RewriteDataFiles {
            target,
            base_snapshot_id,
            job_id,
            options,
            branch,
            where_clause,
        } => run_rewrite_data_files_action(
            state,
            target,
            base_snapshot_id,
            job_id,
            options,
            branch,
            where_clause,
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
        MaintenanceActionRequest::RewritePositionDeleteFiles {
            target,
            options,
            where_clause,
        } => run_rewrite_position_delete_files_action(state, target, options, where_clause),
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

#[allow(clippy::too_many_arguments)]
fn run_rewrite_data_files_action(
    state: &Arc<StandaloneState>,
    target: MaintenanceTarget,
    base_snapshot_id: i64,
    job_id: Option<i64>,
    options: std::collections::BTreeMap<String, String>,
    branch: Option<String>,
    where_clause: Option<String>,
) -> Result<MaintenanceActionOutcome, String> {
    validate_rewrite_data_files_request(&options, branch.as_deref(), where_clause.as_deref())?;
    let resolved_table =
        resolve_maintenance_table_name(state, &target.catalog, &target.namespace, &target.table)?;
    let rewrite_target = WholeTableRewriteTarget {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: resolved_table,
        base_snapshot_id,
        job_id,
    };
    let rewrite_result =
        execute_whole_table_rewrite_with_metrics_for_target(state, &rewrite_target).map_err(
            |error| {
                format!(
                    "REWRITE DATA FILES failed for {}: {error}",
                    action_target(&target)
                )
            },
        )?;

    tracing::info!(
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        target_snapshot_id = ?rewrite_result.outcome.target_snapshot_id,
        rewritten_data_files_count = rewrite_result.outcome.rewritten_data_files,
        added_data_files_count = rewrite_result.outcome.added_data_files,
        removed_delete_files_count = rewrite_result.outcome.deleted_data_files,
        "rewrite_data_files: completed"
    );

    rewrite_data_files_outcome_from_result(&rewrite_result)
}

fn rewrite_data_files_outcome_from_result(
    result: &WholeTableRewriteResult,
) -> Result<MaintenanceActionOutcome, String> {
    Ok(MaintenanceActionOutcome::RewriteDataFiles {
        target_snapshot_id: result.outcome.target_snapshot_id,
        rewritten_data_files_count: checked_i32_metric(
            result.outcome.rewritten_data_files,
            "rewritten_data_files_count",
        )?,
        added_data_files_count: checked_i32_metric(
            result.outcome.added_data_files,
            "added_data_files_count",
        )?,
        rewritten_bytes_count: result.before_metrics.data_bytes,
        failed_data_files_count: 0,
        removed_delete_files_count: checked_i32_metric(
            result.outcome.deleted_data_files,
            "removed_delete_files_count",
        )?,
        output_record_count: result.outcome.output_record_count,
    })
}

fn run_rewrite_position_delete_files_action(
    state: &Arc<StandaloneState>,
    target: MaintenanceTarget,
    options: std::collections::BTreeMap<String, String>,
    where_clause: Option<String>,
) -> Result<MaintenanceActionOutcome, String> {
    if where_clause.is_some() {
        return Err(
            "rewrite_position_delete_files where is not supported in NovaRocks".to_string(),
        );
    }
    let options = RewritePositionDeleteOptions::from_map(&options)?;
    let (catalog, table_ident, _) =
        resolve_maintenance_catalog(state, &target.catalog, &target.namespace, &target.table)?;
    let outcome = block_on_iceberg(async move {
        run_rewrite_position_delete_files(catalog, table_ident, options).await
    })?
    .map_err(|error| {
        format!(
            "rewrite_position_delete_files failed for {}: {error}",
            action_target(&target)
        )
    })?;

    tracing::info!(
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        rewritten_delete_files_count = outcome.rewritten_delete_files_count,
        added_delete_files_count = outcome.added_delete_files_count,
        rewritten_bytes_count = outcome.rewritten_bytes_count,
        added_bytes_count = outcome.added_bytes_count,
        "rewrite_position_delete_files: completed"
    );

    Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
        rewritten_delete_files_count: outcome.rewritten_delete_files_count,
        added_delete_files_count: outcome.added_delete_files_count,
        rewritten_bytes_count: outcome.rewritten_bytes_count,
        added_bytes_count: outcome.added_bytes_count,
    })
}

fn validate_rewrite_data_files_request(
    options: &std::collections::BTreeMap<String, String>,
    branch: Option<&str>,
    where_clause: Option<&str>,
) -> Result<(), String> {
    if where_clause.is_some() {
        return Err("rewrite_data_files where is not supported in NovaRocks yet".to_string());
    }
    if branch.is_some() {
        return Err("rewrite_data_files branch is not supported in NovaRocks yet".to_string());
    }
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => {}
            "rewrite-all" => {
                return Err("rewrite_data_files option `rewrite-all` must be `true`".to_string());
            }
            "min-input-files" | "target-file-size-bytes" => {
                return Err(format!("unsupported rewrite_data_files option `{key}`"));
            }
            other => return Err(format!("unsupported rewrite_data_files option `{other}`")),
        }
    }
    Ok(())
}

fn checked_i32_metric(value: i64, name: &str) -> Result<i32, String> {
    i32::try_from(value).map_err(|_| format!("rewrite_data_files metric `{name}` overflow"))
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
