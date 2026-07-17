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

//! Deterministic Iceberg delta scan-to-protobuf mapping for the native boundary.

use std::collections::HashMap;

use crate::coordinator::prepare::scan::IcebergDeltaScanRuntimePlan;
use crate::exec::node::iceberg_delta_scan::{
    BaseDataFileLineage, DeletedFileVisibility, DeltaScanDeleteSidePayload, DeltaSourceFile,
    DeltaSourceRole, EqualityDeleteTargetData, PositionDeleteFileFormat, PositionDeleteSourceData,
};

pub(super) fn encode_iceberg_delta_scan_plan_native(
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
