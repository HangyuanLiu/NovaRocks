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
    BaseDataFileLineage, DeletedFileVisibility, DeltaScanDeleteSidePayload, DeltaSourceFile,
    DeltaSourceRole, EqualityDeleteTargetData, IcebergDeltaDataColumnPayload,
    PositionDeleteFileFormat, PositionDeleteSourceData,
};
#[cfg(feature = "compat")]
use crate::thrift::plan_nodes;

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

#[cfg(feature = "compat")]
pub(crate) fn encode_iceberg_delta_scan_plan_thrift(
    plan: &IcebergDeltaScanRuntimePlan,
) -> Result<plan_nodes::TIcebergDeltaScanPlan, String> {
    Ok(plan_nodes::TIcebergDeltaScanPlan::new(
        plan.table_location.clone(),
        plan.data_columns
            .iter()
            .map(|column| {
                plan_nodes::TIcebergDeltaDataColumn::new(column.name.clone(), column.field_id)
            })
            .collect(),
        cloud_configuration_from_properties(&plan.cloud_properties),
        change_files_to_thrift(&plan.change_files)?,
        delete_side_to_thrift(plan.delete_side.as_ref())?,
    ))
}

pub(crate) fn encode_iceberg_delta_scan_plan_native(
    plan: &IcebergDeltaScanRuntimePlan,
) -> Result<crate::proto::plan::IcebergDeltaScanPlan, String> {
    Ok(crate::proto::plan::IcebergDeltaScanPlan {
        table_location: plan.table_location.clone(),
        data_columns: plan
            .data_columns
            .iter()
            .map(|column| crate::proto::plan::IcebergDeltaDataColumn {
                name: column.name.clone(),
                field_id: column.field_id,
            })
            .collect(),
        cloud_properties: plan.cloud_properties.clone().into_iter().collect(),
        change_files: plan
            .change_files
            .iter()
            .map(change_file_to_native)
            .collect::<Result<Vec<_>, _>>()?,
        delete_side: plan
            .delete_side
            .as_ref()
            .map(delete_side_to_native)
            .transpose()?,
    })
}

#[cfg(feature = "compat")]
fn cloud_configuration_from_properties(
    cloud_properties: &BTreeMap<String, String>,
) -> Option<crate::thrift::cloud_configuration::TCloudConfiguration> {
    if cloud_properties.is_empty() {
        return None;
    }
    Some(
        crate::thrift::cloud_configuration::TCloudConfiguration::new(
            None::<crate::thrift::cloud_configuration::TCloudType>,
            None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties.clone()),
            None::<bool>,
        ),
    )
}

fn change_file_to_native(
    file: &DeltaSourceFile,
) -> Result<crate::proto::plan::IcebergDeltaSourceFile, String> {
    let (role, position_deletes, equality_field_ids, equality_targets, deleted_file_visibility) =
        match &file.role {
            DeltaSourceRole::DataFile => (
                crate::proto::plan::IcebergDeltaSourceRole::DataFile,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
            ),
            DeltaSourceRole::PositionDelete { deletes } => (
                crate::proto::plan::IcebergDeltaSourceRole::PositionDelete,
                deletes
                    .iter()
                    .map(position_delete_source_to_native)
                    .collect::<Vec<_>>(),
                Vec::new(),
                Vec::new(),
                None,
            ),
            DeltaSourceRole::EqualityDelete {
                equality_field_ids,
                targets,
            } => (
                crate::proto::plan::IcebergDeltaSourceRole::EqualityDelete,
                Vec::new(),
                equality_field_ids.clone(),
                targets.iter().map(equality_target_to_native).collect(),
                None,
            ),
            DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility,
            } => (
                crate::proto::plan::IcebergDeltaSourceRole::DeletedDataFile,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                previous_data_file_visibility
                    .as_ref()
                    .map(deleted_file_visibility_to_native),
            ),
        };

    Ok(crate::proto::plan::IcebergDeltaSourceFile {
        path: file.path.clone(),
        size: file.size,
        role: role as i32,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        row_id_allow_list: file
            .row_id_allow_list
            .as_ref()
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default(),
        position_deletes,
        equality_field_ids,
        equality_targets,
        deleted_file_visibility,
    })
}

fn position_delete_source_to_native(
    delete: &PositionDeleteSourceData,
) -> crate::proto::plan::IcebergDeltaPositionDeleteSource {
    crate::proto::plan::IcebergDeltaPositionDeleteSource {
        delete_file_path: delete.delete_file_path.clone(),
        delete_file_size: delete.delete_file_size,
        referenced_data_file: delete.referenced_data_file.clone(),
        file_format: match delete.file_format {
            PositionDeleteFileFormat::Parquet => {
                crate::proto::plan::IcebergDeltaPositionDeleteFileFormat::Parquet
            }
            PositionDeleteFileFormat::Puffin => {
                crate::proto::plan::IcebergDeltaPositionDeleteFileFormat::Puffin
            }
        } as i32,
        content_offset: delete.content_offset,
        content_size_in_bytes: delete.content_size_in_bytes,
    }
}

fn equality_target_to_native(
    target: &EqualityDeleteTargetData,
) -> crate::proto::plan::IcebergDeltaEqualityDeleteTarget {
    crate::proto::plan::IcebergDeltaEqualityDeleteTarget {
        data_file_path: target.data_file_path.clone(),
        data_file_size: target.data_file_size,
        data_file_first_row_id: target.data_file_first_row_id,
        data_file_sequence_number: target.data_file_sequence_number,
    }
}

fn deleted_file_visibility_to_native(
    visibility: &DeletedFileVisibility,
) -> crate::proto::plan::IcebergDeltaDeletedFileVisibility {
    crate::proto::plan::IcebergDeltaDeletedFileVisibility {
        already_deleted_positions: visibility.already_deleted_positions.clone(),
    }
}

fn delete_side_to_native(
    payload: &DeltaScanDeleteSidePayload,
) -> Result<crate::proto::plan::IcebergDeltaDeleteSidePlan, String> {
    Ok(crate::proto::plan::IcebergDeltaDeleteSidePlan {
        base_data_file_lineage: lineage_map_to_native(&payload.base_data_file_lineage),
        previous_data_file_lineage: lineage_map_to_native(&payload.previous_data_file_lineage),
        previous_delete_visibility_data_files: payload
            .previous_delete_visibility_data_files
            .iter()
            .map(delete_visibility_data_file_to_native)
            .collect(),
        previously_deleted_positions_per_file: payload
            .previously_deleted_positions_per_file
            .iter()
            .map(|(path, positions)| {
                (
                    path.clone(),
                    crate::proto::plan::IcebergDeltaPositionList {
                        positions: positions.clone(),
                    },
                )
            })
            .collect(),
        deleted_data_file_paths: payload.deleted_data_file_paths.iter().cloned().collect(),
    })
}

fn lineage_map_to_native(
    input: &HashMap<String, BaseDataFileLineage>,
) -> HashMap<String, crate::proto::plan::IcebergDeltaBaseDataFileLineage> {
    input
        .iter()
        .map(|(path, lineage)| {
            (
                path.clone(),
                crate::proto::plan::IcebergDeltaBaseDataFileLineage {
                    first_row_id: lineage.first_row_id,
                    data_sequence_number: lineage.data_sequence_number,
                },
            )
        })
        .collect()
}

fn delete_visibility_data_file_to_native(
    file: &crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor,
) -> crate::proto::plan::IcebergDeltaDeleteVisibilityDataFile {
    crate::proto::plan::IcebergDeltaDeleteVisibilityDataFile {
        path: file.path.clone(),
        size: file.size,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        delete_files: file
            .delete_files
            .iter()
            .map(delete_visibility_delete_file_to_native)
            .collect(),
    }
}

fn delete_visibility_delete_file_to_native(
    file: &crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor,
) -> crate::proto::plan::IcebergDeltaDeleteVisibilityDeleteFile {
    crate::proto::plan::IcebergDeltaDeleteVisibilityDeleteFile {
        path: file.path.clone(),
        file_format: match file.file_format {
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Parquet => {
                crate::proto::plan::IcebergDeltaDeleteFileFormat::Parquet
            }
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Puffin => {
                crate::proto::plan::IcebergDeltaDeleteFileFormat::Puffin
            }
        } as i32,
        file_content: match file.file_content {
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Position => {
                crate::proto::plan::IcebergDeltaDeleteFileContent::Position
            }
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Equality => {
                crate::proto::plan::IcebergDeltaDeleteFileContent::Equality
            }
        } as i32,
        length: file.length,
        content_offset: file.content_offset,
        content_size_in_bytes: file.content_size_in_bytes,
    }
}

#[cfg(feature = "compat")]
fn change_files_to_thrift(
    files: &[DeltaSourceFile],
) -> Result<Vec<plan_nodes::TIcebergDeltaSourceFile>, String> {
    files.iter().map(change_file_to_thrift).collect()
}

#[cfg(feature = "compat")]
fn change_file_to_thrift(
    file: &DeltaSourceFile,
) -> Result<plan_nodes::TIcebergDeltaSourceFile, String> {
    let (role, position_deletes, equality_field_ids, equality_targets, deleted_file_visibility) =
        match &file.role {
            DeltaSourceRole::DataFile => (
                plan_nodes::TIcebergDeltaSourceRole::DATA_FILE,
                None,
                None,
                None,
                None,
            ),
            DeltaSourceRole::PositionDelete { deletes } => (
                plan_nodes::TIcebergDeltaSourceRole::POSITION_DELETE,
                Some(
                    deletes
                        .iter()
                        .map(position_delete_source_to_thrift)
                        .collect::<Vec<_>>(),
                ),
                None,
                None,
                None,
            ),
            DeltaSourceRole::EqualityDelete {
                equality_field_ids,
                targets,
            } => (
                plan_nodes::TIcebergDeltaSourceRole::EQUALITY_DELETE,
                None,
                Some(equality_field_ids.clone()),
                Some(targets.iter().map(equality_target_to_thrift).collect()),
                None,
            ),
            DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility,
            } => (
                plan_nodes::TIcebergDeltaSourceRole::DELETED_DATA_FILE,
                None,
                None,
                None,
                previous_data_file_visibility
                    .as_ref()
                    .map(deleted_file_visibility_to_thrift),
            ),
        };

    Ok(plan_nodes::TIcebergDeltaSourceFile::new(
        file.path.clone(),
        file.size,
        role,
        file.partition_spec_id,
        file.partition_key.clone(),
        file.first_row_id,
        file.data_sequence_number,
        file.row_id_allow_list.clone(),
        position_deletes,
        equality_field_ids,
        equality_targets,
        deleted_file_visibility,
    ))
}

#[cfg(feature = "compat")]
fn position_delete_source_to_thrift(
    delete: &PositionDeleteSourceData,
) -> plan_nodes::TIcebergDeltaPositionDeleteSource {
    plan_nodes::TIcebergDeltaPositionDeleteSource::new(
        delete.delete_file_path.clone(),
        delete.delete_file_size,
        delete.referenced_data_file.clone(),
        match delete.file_format {
            PositionDeleteFileFormat::Parquet => {
                plan_nodes::TIcebergDeltaPositionDeleteFileFormat::PARQUET
            }
            PositionDeleteFileFormat::Puffin => {
                plan_nodes::TIcebergDeltaPositionDeleteFileFormat::PUFFIN
            }
        },
        delete.content_offset,
        delete.content_size_in_bytes,
    )
}

#[cfg(feature = "compat")]
fn equality_target_to_thrift(
    target: &EqualityDeleteTargetData,
) -> plan_nodes::TIcebergDeltaEqualityDeleteTarget {
    plan_nodes::TIcebergDeltaEqualityDeleteTarget::new(
        target.data_file_path.clone(),
        target.data_file_size,
        target.data_file_first_row_id,
        target.data_file_sequence_number,
    )
}

#[cfg(feature = "compat")]
fn deleted_file_visibility_to_thrift(
    visibility: &DeletedFileVisibility,
) -> plan_nodes::TIcebergDeltaDeletedFileVisibility {
    plan_nodes::TIcebergDeltaDeletedFileVisibility::new(
        visibility.already_deleted_positions.clone(),
    )
}

#[cfg(feature = "compat")]
fn delete_side_to_thrift(
    payload: Option<&DeltaScanDeleteSidePayload>,
) -> Result<Option<plan_nodes::TIcebergDeltaDeleteSidePlan>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(Some(plan_nodes::TIcebergDeltaDeleteSidePlan::new(
        lineage_map_to_thrift(&payload.base_data_file_lineage),
        lineage_map_to_thrift(&payload.previous_data_file_lineage),
        payload
            .previous_delete_visibility_data_files
            .iter()
            .map(delete_visibility_data_file_to_thrift)
            .collect::<Vec<_>>(),
        previous_deleted_positions_to_thrift(&payload.previously_deleted_positions_per_file)?,
        payload.deleted_data_file_paths.iter().cloned().collect(),
    )))
}

#[cfg(feature = "compat")]
fn lineage_map_to_thrift(
    input: &HashMap<String, BaseDataFileLineage>,
) -> BTreeMap<String, plan_nodes::TIcebergDeltaBaseDataFileLineage> {
    input
        .iter()
        .map(|(path, lineage)| {
            (
                path.clone(),
                plan_nodes::TIcebergDeltaBaseDataFileLineage::new(
                    lineage.first_row_id,
                    lineage.data_sequence_number,
                ),
            )
        })
        .collect()
}

#[cfg(feature = "compat")]
fn previous_deleted_positions_to_thrift(
    input: &HashMap<String, Vec<u64>>,
) -> Result<BTreeMap<String, Vec<i64>>, String> {
    input
        .iter()
        .map(|(path, positions)| {
            let converted = positions
                .iter()
                .map(|position| {
                    i64::try_from(*position).map_err(|_| {
                        format!(
                            "iceberg delta scan previous deleted position for {} exceeds i64: {}",
                            path, position
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((path.clone(), converted))
        })
        .collect()
}

#[cfg(feature = "compat")]
fn delete_visibility_data_file_to_thrift(
    file: &crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor,
) -> plan_nodes::TIcebergDeltaDeleteVisibilityDataFile {
    plan_nodes::TIcebergDeltaDeleteVisibilityDataFile::new(
        file.path.clone(),
        file.size,
        file.first_row_id,
        file.data_sequence_number,
        file.delete_files
            .iter()
            .map(delete_visibility_delete_file_to_thrift)
            .collect(),
    )
}

#[cfg(feature = "compat")]
fn delete_visibility_delete_file_to_thrift(
    file: &crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor,
) -> plan_nodes::TIcebergDeltaDeleteVisibilityDeleteFile {
    plan_nodes::TIcebergDeltaDeleteVisibilityDeleteFile::new(
        file.path.clone(),
        match file.file_format {
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Parquet => {
                plan_nodes::TIcebergDeltaDeleteFileFormat::PARQUET
            }
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Puffin => {
                plan_nodes::TIcebergDeltaDeleteFileFormat::PUFFIN
            }
        },
        match file.file_content {
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Position => {
                plan_nodes::TIcebergDeltaDeleteFileContent::POSITION
            }
            crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Equality => {
                plan_nodes::TIcebergDeltaDeleteFileContent::EQUALITY
            }
        },
        file.length,
        file.content_offset,
        file.content_size_in_bytes,
    )
}
