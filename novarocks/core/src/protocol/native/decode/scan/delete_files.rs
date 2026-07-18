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

use std::collections::HashMap;

use crate::exec::node::iceberg_delta_scan::{
    BaseDataFileLineage, DeltaScanDeleteSidePayload, EqualityDeleteTargetData,
    PositionDeleteFileFormat, PositionDeleteSourceData,
};
use crate::proto::plan;
use crate::protocol::common::error::ProtocolErrorKind;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(super) fn reject_native_delta_role_payload(
    file: &plan::IcebergDeltaSourceFile,
    role_name: &str,
    fields: &[&'static str],
) -> Result<(), NativeFragmentLeafDecodeError> {
    for field in fields {
        let present = match *field {
            "position_deletes" => !file.position_deletes.is_empty(),
            "equality_field_ids" => !file.equality_field_ids.is_empty(),
            "equality_targets" => !file.equality_targets.is_empty(),
            "deleted_file_visibility" => file.deleted_file_visibility.is_some(),
            _ => false,
        };
        if present {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                field,
                format!(
                    "IcebergDeltaTable source file {} role {} must not carry {}",
                    file.path, role_name, field
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn lower_position_delete_source_from_native(
    delete: &plan::IcebergDeltaPositionDeleteSource,
) -> Result<PositionDeleteSourceData, NativeFragmentLeafDecodeError> {
    Ok(PositionDeleteSourceData {
        delete_file_path: delete.delete_file_path.clone(),
        delete_file_size: delete.delete_file_size,
        referenced_data_file: delete.referenced_data_file.clone(),
        file_format: lower_position_delete_format_from_native(delete.file_format)?,
        content_offset: delete.content_offset,
        content_size_in_bytes: delete.content_size_in_bytes,
    })
}

fn lower_position_delete_format_from_native(
    format: i32,
) -> Result<PositionDeleteFileFormat, NativeFragmentLeafDecodeError> {
    match plan::IcebergDeltaPositionDeleteFileFormat::try_from(format).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "file_format",
            format!("IcebergDeltaTable unsupported position-delete file format {format}"),
        )
    })? {
        plan::IcebergDeltaPositionDeleteFileFormat::Unspecified => {
            Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidEnum,
                "file_format",
                "IcebergDeltaTable position-delete file format is unspecified",
            ))
        }
        plan::IcebergDeltaPositionDeleteFileFormat::Parquet => {
            Ok(PositionDeleteFileFormat::Parquet)
        }
        plan::IcebergDeltaPositionDeleteFileFormat::Puffin => Ok(PositionDeleteFileFormat::Puffin),
    }
}

pub(super) fn lower_equality_delete_target_from_native(
    target: &plan::IcebergDeltaEqualityDeleteTarget,
) -> EqualityDeleteTargetData {
    EqualityDeleteTargetData {
        data_file_path: target.data_file_path.clone(),
        data_file_size: target.data_file_size,
        data_file_first_row_id: target.data_file_first_row_id,
        data_file_sequence_number: target.data_file_sequence_number,
    }
}

pub(super) fn lower_delta_delete_side_payload_from_native(
    payload: Option<&plan::IcebergDeltaDeleteSidePlan>,
) -> Result<Option<DeltaScanDeleteSidePayload>, NativeFragmentLeafDecodeError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(Some(DeltaScanDeleteSidePayload {
        base_data_file_lineage: lower_novarocks_base_lineage_map(&payload.base_data_file_lineage),
        previous_data_file_lineage: lower_novarocks_base_lineage_map(
            &payload.previous_data_file_lineage,
        ),
        previous_delete_visibility_data_files: payload
            .previous_delete_visibility_data_files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                lower_novarocks_delete_visibility_data_file(file).map_err(|error| {
                    error
                        .prepend_index(index)
                        .prepend_field("previous_delete_visibility_data_files")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        previously_deleted_positions_per_file: payload
            .previously_deleted_positions_per_file
            .iter()
            .map(|(path, positions)| (path.clone(), positions.positions.clone()))
            .collect(),
        deleted_data_file_paths: payload.deleted_data_file_paths.iter().cloned().collect(),
    }))
}

fn lower_novarocks_base_lineage_map(
    input: &HashMap<String, plan::IcebergDeltaBaseDataFileLineage>,
) -> HashMap<String, BaseDataFileLineage> {
    input
        .iter()
        .map(|(path, lineage)| {
            (
                path.clone(),
                BaseDataFileLineage {
                    first_row_id: lineage.first_row_id,
                    data_sequence_number: lineage.data_sequence_number,
                },
            )
        })
        .collect()
}

fn lower_novarocks_delete_visibility_data_file(
    file: &plan::IcebergDeltaDeleteVisibilityDataFile,
) -> Result<
    crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor,
    NativeFragmentLeafDecodeError,
> {
    Ok(
        crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor {
            path: file.path.clone(),
            size: file.size,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            delete_files: file
                .delete_files
                .iter()
                .enumerate()
                .map(|(index, delete)| {
                    lower_novarocks_delete_visibility_delete_file(delete)
                        .map_err(|error| error.prepend_index(index).prepend_field("delete_files"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        },
    )
}

fn lower_novarocks_delete_visibility_delete_file(
    file: &plan::IcebergDeltaDeleteVisibilityDeleteFile,
) -> Result<
    crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor,
    NativeFragmentLeafDecodeError,
> {
    Ok(
        crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor {
            path: file.path.clone(),
            file_format: lower_novarocks_delete_file_format(file.file_format)?,
            file_content: lower_novarocks_delete_file_content(file.file_content)?,
            length: file.length,
            content_offset: file.content_offset,
            content_size_in_bytes: file.content_size_in_bytes,
        },
    )
}

fn lower_novarocks_delete_file_format(
    format: i32,
) -> Result<
    crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat,
    NativeFragmentLeafDecodeError,
> {
    match plan::IcebergDeltaDeleteFileFormat::try_from(format).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "file_format",
            format!("IcebergDeltaTable unsupported delete file format {format}"),
        )
    })? {
        plan::IcebergDeltaDeleteFileFormat::Unspecified => {
            Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidEnum,
                "file_format",
                "IcebergDeltaTable delete file format is unspecified",
            ))
        }
        plan::IcebergDeltaDeleteFileFormat::Parquet => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Parquet)
        }
        plan::IcebergDeltaDeleteFileFormat::Puffin => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Puffin)
        }
    }
}

fn lower_novarocks_delete_file_content(
    content: i32,
) -> Result<
    crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent,
    NativeFragmentLeafDecodeError,
> {
    match plan::IcebergDeltaDeleteFileContent::try_from(content).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "file_content",
            format!("IcebergDeltaTable unsupported delete file content {content}"),
        )
    })? {
        plan::IcebergDeltaDeleteFileContent::Unspecified => {
            Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidEnum,
                "file_content",
                "IcebergDeltaTable delete file content is unspecified",
            ))
        }
        plan::IcebergDeltaDeleteFileContent::Position => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Position)
        }
        plan::IcebergDeltaDeleteFileContent::Equality => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Equality)
        }
    }
}
