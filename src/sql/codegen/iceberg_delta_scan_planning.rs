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

use std::collections::{BTreeMap, HashMap};

use crate::exec::node::iceberg_delta_scan::{
    DeltaScanDeleteSidePayload, DeltaSourceFile, IcebergDeltaDataColumnPayload,
};

pub(crate) struct IcebergDeltaScanRuntimePlan {
    pub(crate) table_location: String,
    pub(crate) data_columns: Vec<IcebergDeltaDataColumnPayload>,
    pub(crate) cloud_properties: BTreeMap<String, String>,
    pub(crate) change_files: Vec<DeltaSourceFile>,
    pub(crate) delete_side: Option<DeltaScanDeleteSidePayload>,
}

pub(crate) fn build_iceberg_delta_scan_runtime_plan(
    table: &crate::sql::catalog::IcebergTableInfo,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<IcebergDeltaScanRuntimePlan, String> {
    let refresh_ctx = mv_refresh_ctx
        .ok_or_else(|| "Iceberg delta scan requires MV refresh context".to_string())?;
    let catalog_key = crate::engine::catalog::normalize_identifier(&table.catalog)?;
    let entry = refresh_ctx
        .base_catalog_entries
        .get(&catalog_key)
        .ok_or_else(|| {
            format!(
                "Iceberg delta scan requires base catalog {} in MV refresh context",
                table.catalog
            )
        })?;
    let ident = iceberg::TableIdent::from_strs([table.namespace.as_str(), table.table.as_str()])
        .map_err(|e| {
            format!(
                "build iceberg table ident for delta scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(entry)
        .map_err(|e| {
            format!(
                "build iceberg catalog for delta scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        catalog.load_table(&ident).await
    })
    .map_err(|e| format!("load iceberg table for delta scan runtime failed: {e}"))?
    .map_err(|e| {
        format!(
            "load iceberg table for delta scan {}.{}.{}: {e}",
            table.catalog, table.namespace, table.table
        )
    })?;

    let batch = crate::connector::iceberg::changes::plan_changes(
        &loaded,
        from_snapshot_id,
        Some(to_snapshot_id),
        &[],
    )
    .map_err(|e| {
        format!(
            "ivm-a1 codegen delta-scan: plan_changes failed for {}.{}.{} from_snapshot={} to_snapshot={}: {e}",
            table.catalog, table.namespace, table.table, from_snapshot_id, to_snapshot_id
        )
    })?;
    let equality_targets_by_delete_file =
        crate::connector::iceberg::changes::equality_delete_targets_at(
            &loaded,
            batch.current_snapshot_id,
            &batch.equality_deletes,
        )
        .map_err(|e| {
            format!(
                "ivm-a1 codegen delta-scan: plan equality-delete targets failed for {}.{}.{} at snapshot {}: {e}",
                table.catalog, table.namespace, table.table, batch.current_snapshot_id
            )
        })?;
    let change_files =
        crate::connector::iceberg::changes::delta_source_files_from_change_batch_with_equality_targets(
            &batch,
            &equality_targets_by_delete_file,
        )?;
    let has_delete = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let delete_side = if has_delete {
        let object_store_factory = crate::connector::iceberg::changes::build_factory_for_table(
            &loaded,
            entry.object_store_config(),
        )?;
        let object_store_factory = std::sync::Arc::new(object_store_factory);
        let expected_object_store_bucket =
            crate::connector::iceberg::changes::expected_object_store_bucket_for_table(&loaded)?;
        let base_data_file_lineage =
            crate::connector::iceberg::changes::base_data_file_lineage_index_at(
                &loaded,
                batch.current_snapshot_id,
            )?;
        let previous_data_file_lineage = if !batch.deleted_data_files.is_empty() {
            crate::connector::iceberg::changes::previous_snapshot_data_file_lineage_index(
                &loaded,
                batch.previous_snapshot_id,
            )?
        } else {
            HashMap::new()
        };
        let deleted_data_file_paths = batch
            .deleted_data_files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let touched_referenced_data_files: std::collections::HashSet<String> = batch
            .deletes
            .iter()
            .filter_map(|delete| delete.referenced_data_file.clone())
            .collect();
        let previously_deleted_positions_per_file = if !touched_referenced_data_files.is_empty() {
            crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
                &loaded,
                batch.previous_snapshot_id,
                object_store_factory.as_ref(),
                &|path: &str| {
                    crate::connector::iceberg::changes::normalize_delete_projection_path(
                        path,
                        entry.object_store_config(),
                        expected_object_store_bucket.as_deref(),
                    )
                },
                |data_file_path: &str| touched_referenced_data_files.contains(data_file_path),
            )
            .map_err(|e| {
                format!(
                    "ivm-a1 codegen delta-scan: preload previous deleted positions failed for {}.{}.{} at snapshot {}: {e}",
                    table.catalog, table.namespace, table.table, batch.previous_snapshot_id
                )
            })?
            .into_iter()
            .map(|(path, bitmap)| (path, bitmap.iter().collect::<Vec<_>>()))
            .collect()
        } else {
            HashMap::new()
        };
        let previous_delete_visibility_data_files =
            crate::connector::iceberg::changes::delete_visibility_data_files_at(
                &loaded,
                batch.previous_snapshot_id,
            )?;
        Some(DeltaScanDeleteSidePayload {
            base_data_file_lineage,
            previous_data_file_lineage,
            previous_delete_visibility_data_files,
            previously_deleted_positions_per_file,
            deleted_data_file_paths,
        })
    } else {
        None
    };
    let current_schema = loaded.metadata().current_schema();
    let data_columns = current_schema
        .as_ref()
        .as_struct()
        .fields()
        .iter()
        .map(|field| IcebergDeltaDataColumnPayload {
            name: field.name.clone(),
            field_id: field.id,
        })
        .collect();
    Ok(IcebergDeltaScanRuntimePlan {
        table_location: loaded.metadata().location().to_string(),
        data_columns,
        cloud_properties: entry.cloud_properties_map(),
        change_files,
        delete_side,
    })
}
