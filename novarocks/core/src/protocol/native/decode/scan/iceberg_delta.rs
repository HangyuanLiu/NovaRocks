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

use std::sync::Arc;

use super::super::layout::{chunk_schema_from_output_columns, layout_from_output_columns};
use super::super::node::DecodedNode;
use super::common::{lower_scan_predicate, resolve_cloud_object_store_config, scan_output_columns};
use super::delete_files::{
    build_delta_delete_side_from_payload, lower_delta_delete_side_payload_from_native,
    lower_equality_delete_target_from_native, lower_position_delete_source_from_native,
    reject_native_delta_role_payload,
};
use crate::exec::expr::ExprArena;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::iceberg_delta_scan::{
    ApplyKeySource, BaseTableIdent, DeletedFileVisibility, DeltaSourceFile, DeltaSourceRole,
    IcebergDeltaDataColumnPayload, IcebergDeltaScanNode, IcebergDeltaTablePayload,
    IcebergRuntimeHandles,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;

pub(super) fn lower_iceberg_delta_table_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergDeltaTable,
    arena: &mut ExprArena,
) -> Result<DecodedNode, String> {
    let output_columns = scan_output_columns(scan)?;
    let layout = layout_from_output_columns(&output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&output_columns)?;
    let table = source
        .table
        .as_ref()
        .ok_or_else(|| "IcebergDeltaTable table missing".to_string())?;
    if source.from_snapshot_id < 0 {
        return Err(format!(
            "IcebergDeltaTable node_id={} from_snapshot_id must be non-negative, got {}",
            node.node_id, source.from_snapshot_id
        ));
    }
    if source.to_snapshot_id < 0 {
        return Err(format!(
            "IcebergDeltaTable node_id={} to_snapshot_id must be non-negative, got {}",
            node.node_id, source.to_snapshot_id
        ));
    }
    let delta_plan = source
        .delta_plan
        .as_ref()
        .ok_or_else(|| "IcebergDeltaTable delta_plan missing".to_string())?;
    let table_payload = IcebergDeltaTablePayload {
        table_location: delta_plan.table_location.clone(),
        data_columns: delta_plan
            .data_columns
            .iter()
            .map(|column| IcebergDeltaDataColumnPayload {
                name: column.name.clone(),
                field_id: column.field_id,
            })
            .collect(),
    };
    let change_files = lower_delta_source_files_from_native(&delta_plan.change_files)?;
    let object_store_config = resolve_cloud_object_store_config(&delta_plan.cloud_properties)?;
    let object_store_factory = Arc::new(
        crate::connector::iceberg::changes::build_factory_for_table_location(
            &table_payload.table_location,
            object_store_config.as_ref(),
        )?,
    );
    let delete_side_payload =
        lower_delta_delete_side_payload_from_native(delta_plan.delete_side.as_ref())?;
    let delete_side =
        build_delta_delete_side_from_payload(delete_side_payload, object_store_config.as_ref())?;

    let mut exec_node = ExecNode {
        kind: ExecNodeKind::IcebergDeltaScan(IcebergDeltaScanNode {
            base_table_ident: BaseTableIdent {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            table_location: table_payload.table_location.clone(),
            from_snapshot_id: source.from_snapshot_id,
            to_snapshot_id: source.to_snapshot_id,
            output_chunk_schema: output_schema.clone(),
            apply_key_source: ApplyKeySource::BaseRowId,
            change_files,
            object_store_config,
            iceberg_runtime: Arc::new(IcebergRuntimeHandles {
                table: table_payload,
                object_store_factory,
                delete_side,
            }),
            node_id: node.node_id,
            native_runtime_filter_specs: Vec::new(),
        }),
    };
    if let Some(predicate) = lower_scan_predicate(scan, arena, &layout)? {
        exec_node = ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(exec_node),
                node_id: node.node_id,
                predicate,
            }),
        };
    }
    Ok(DecodedNode {
        node: exec_node,
        layout,
        output_schema,
    })
}

fn lower_delta_source_files_from_native(
    files: &[plan::IcebergDeltaSourceFile],
) -> Result<Vec<DeltaSourceFile>, String> {
    files
        .iter()
        .map(lower_delta_source_file_from_native)
        .collect()
}

fn lower_delta_source_file_from_native(
    file: &plan::IcebergDeltaSourceFile,
) -> Result<DeltaSourceFile, String> {
    let role = match plan::IcebergDeltaSourceRole::try_from(file.role).map_err(|_| {
        format!(
            "IcebergDeltaTable source file {} has unknown delta role {}",
            file.path, file.role
        )
    })? {
        plan::IcebergDeltaSourceRole::Unspecified => {
            return Err(format!(
                "IcebergDeltaTable source file {} has unspecified delta role",
                file.path
            ));
        }
        plan::IcebergDeltaSourceRole::DataFile => {
            reject_native_delta_role_payload(
                file,
                "DATA_FILE",
                &[
                    "position_deletes",
                    "equality_field_ids",
                    "equality_targets",
                    "deleted_file_visibility",
                ],
            )?;
            DeltaSourceRole::DataFile
        }
        plan::IcebergDeltaSourceRole::PositionDelete => {
            reject_native_delta_role_payload(
                file,
                "POSITION_DELETE",
                &[
                    "equality_field_ids",
                    "equality_targets",
                    "deleted_file_visibility",
                ],
            )?;
            if file.position_deletes.is_empty() {
                return Err(format!(
                    "IcebergDeltaTable source file {} role POSITION_DELETE requires position_deletes",
                    file.path
                ));
            }
            DeltaSourceRole::PositionDelete {
                deletes: file
                    .position_deletes
                    .iter()
                    .map(lower_position_delete_source_from_native)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        }
        plan::IcebergDeltaSourceRole::EqualityDelete => {
            reject_native_delta_role_payload(
                file,
                "EQUALITY_DELETE",
                &["position_deletes", "deleted_file_visibility"],
            )?;
            if file.equality_field_ids.is_empty() {
                return Err(format!(
                    "IcebergDeltaTable source file {} role EQUALITY_DELETE requires equality_field_ids",
                    file.path
                ));
            }
            if file.equality_targets.is_empty() {
                return Err(format!(
                    "IcebergDeltaTable source file {} role EQUALITY_DELETE requires equality_targets",
                    file.path
                ));
            }
            DeltaSourceRole::EqualityDelete {
                equality_field_ids: file.equality_field_ids.clone(),
                targets: file
                    .equality_targets
                    .iter()
                    .map(lower_equality_delete_target_from_native)
                    .collect(),
            }
        }
        plan::IcebergDeltaSourceRole::DeletedDataFile => {
            reject_native_delta_role_payload(
                file,
                "DELETED_DATA_FILE",
                &["position_deletes", "equality_field_ids", "equality_targets"],
            )?;
            DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility: file.deleted_file_visibility.as_ref().map(
                    |visibility| DeletedFileVisibility {
                        already_deleted_positions: visibility.already_deleted_positions.clone(),
                    },
                ),
            }
        }
    };

    Ok(DeltaSourceFile {
        path: file.path.clone(),
        size: file.size,
        role,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        row_id_allow_list: if file.row_id_allow_list.is_empty() {
            None
        } else {
            Some(file.row_id_allow_list.iter().copied().collect())
        },
    })
}
