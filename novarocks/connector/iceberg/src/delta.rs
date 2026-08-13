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

//! Provider-owned snapshot-delta split facts.
//!
//! Delta scan has a logical identity in the planner, but every physical
//! data, delete, and deletion-vector read belongs to the Iceberg provider.
//! These DTOs deliberately contain only serializable Iceberg facts; they do
//! not mention an execution node, `Chunk`, runtime filter, or core I/O type.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Provider-private delta membership carried by a frozen Iceberg split.
/// Core transports this value only inside the opaque provider split payload.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IcebergDeltaSplitPayload {
    pub source: DeltaSourceFile,
    #[serde(default)]
    pub delete_side: Option<DeltaScanDeleteSide>,
}

/// Provider-owned failure modes for Iceberg snapshot-lineage planning.
///
/// These are intentionally hard failures.  A generic caller may choose a
/// refresh policy from the signal below, but it must not reinterpret Iceberg
/// lineage or substitute a fallback read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeError {
    LineageBroken { previous_snapshot: i64 },
    UnsupportedOperation { snapshot_id: i64, op: String },
    SchemaEvolutionUnsupported { detail: String },
    ReplaceValidationFailed { snapshot_id: i64, reason: String },
    PrimaryKeyMissingFromBase { pk_col: String },
    PrimaryKeyNullable { pk_col: String },
    PrimaryKeyTypeUnsupported { pk_col: String, ty: String },
    PrimaryKeyValueNull { row_info: String },
    IcebergFormatUnsupported { format_version: i32 },
    InternalInconsistency(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergChangePolicySignal {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub fn policy_signal_from_change_error(err: &ChangeError) -> IcebergChangePolicySignal {
    match err {
        ChangeError::LineageBroken { .. } => IcebergChangePolicySignal::FullRefresh {
            reason: "previous snapshot is not reachable".to_string(),
        },
        ChangeError::ReplaceValidationFailed { reason, .. } => {
            IcebergChangePolicySignal::FullRefresh {
                reason: format!("replace snapshot is not a provably safe compaction: {reason}"),
            }
        }
        ChangeError::SchemaEvolutionUnsupported { detail } => {
            IcebergChangePolicySignal::Unsupported {
                reason: format!("schema evolution is not supported by IVM: {detail}"),
            }
        }
        other => IcebergChangePolicySignal::Unsupported {
            reason: other.to_string(),
        },
    }
}

impl std::fmt::Display for ChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LineageBroken { previous_snapshot } => write!(
                f,
                "iceberg lineage broken: previous snapshot {previous_snapshot} is unreachable from current snapshot"
            ),
            Self::UnsupportedOperation { snapshot_id, op } => {
                write!(
                    f,
                    "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
                )
            }
            Self::SchemaEvolutionUnsupported { detail } => {
                write!(f, "iceberg schema evolution not supported: {detail}")
            }
            Self::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => write!(
                f,
                "iceberg REPLACE snapshot {snapshot_id} failed compaction validation: {reason}"
            ),
            Self::PrimaryKeyMissingFromBase { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` does not exist on the iceberg base table"
            ),
            Self::PrimaryKeyNullable { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` must be NOT NULL on the iceberg base table"
            ),
            Self::PrimaryKeyTypeUnsupported { pk_col, ty } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` has unsupported type `{ty}`; only hashable scalar types are allowed"
            ),
            Self::PrimaryKeyValueNull { row_info } => {
                write!(f, "PRIMARY KEY value is NULL in base row: {row_info}")
            }
            Self::IcebergFormatUnsupported { format_version } => write!(
                f,
                "iceberg base table format-version {format_version} is not supported; IVM requires v2 or v3"
            ),
            Self::InternalInconsistency(detail) => write!(f, "internal inconsistency: {detail}"),
        }
    }
}

impl std::error::Error for ChangeError {}

/// Provider-native data file identity used while producing an incremental
/// change scan.  It is intentionally independent from execution batches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub row_id_allow_list: Option<BTreeSet<i64>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChangePartitionFieldValue {
    pub source_field_id: i32,
    pub source_column: Option<String>,
    pub field_name: String,
    pub transform: String,
    pub value: ChangePartitionValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChangePartitionValue {
    Null,
    Primitive(String),
    Unsupported(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeletedDataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,
    pub file_format: crate::iceberg::spec::DataFileFormat,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EqualityDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub equality_ids: Vec<i32>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
}

pub fn change_partition_field_values(
    metadata: &crate::iceberg::spec::TableMetadata,
    spec_id: i32,
    partition: &crate::iceberg::spec::Struct,
) -> Result<Vec<ChangePartitionFieldValue>, ChangeError> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(ChangeError::InternalInconsistency(format!(
            "iceberg table metadata missing partition spec id {spec_id}"
        )));
    };
    let schema = metadata.current_schema();
    spec.fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let literal = partition.fields().get(idx).ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "iceberg partition struct for spec id {spec_id} is missing field {} at index {idx}",
                    field.name
                ))
            })?;
            Ok(ChangePartitionFieldValue {
                source_field_id: field.source_id,
                source_column: schema.field_by_id(field.source_id).map(|source| source.name.clone()),
                field_name: field.name.clone(),
                transform: change_partition_transform_name(&field.transform),
                value: change_partition_value(literal.as_ref()),
            })
        })
        .collect()
}

pub fn change_partition_transform_name(transform: &crate::iceberg::spec::Transform) -> String {
    match transform {
        crate::iceberg::spec::Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn change_partition_value(literal: Option<&crate::iceberg::spec::Literal>) -> ChangePartitionValue {
    let Some(crate::iceberg::spec::Literal::Primitive(value)) = literal else {
        return match literal {
            None => ChangePartitionValue::Null,
            Some(_) => {
                ChangePartitionValue::Unsupported("non-primitive partition value".to_string())
            }
        };
    };
    let value = match value {
        crate::iceberg::spec::PrimitiveLiteral::Boolean(v) => v.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Int(v) => v.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Long(v) => v.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Float(v) => v.0.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Double(v) => v.0.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::String(v) => {
            return ChangePartitionValue::Primitive(v.clone());
        }
        crate::iceberg::spec::PrimitiveLiteral::Binary(_) => {
            return ChangePartitionValue::Unsupported("binary partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::Int128(_) => {
            return ChangePartitionValue::Unsupported("int128 partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::UInt128(_) => {
            return ChangePartitionValue::Unsupported("uint128 partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::AboveMax => {
            return ChangePartitionValue::Unsupported("above-max partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::BelowMin => {
            return ChangePartitionValue::Unsupported("below-min partition value".to_string());
        }
    };
    ChangePartitionValue::Primitive(value)
}

impl PositionDeleteRef {
    pub fn validate_invariants(&self) -> Result<(), ChangeError> {
        use crate::iceberg::spec::DataFileFormat;

        match self.file_format {
            DataFileFormat::Parquet
                if self.content_offset.is_none() && self.content_size_in_bytes.is_none() =>
            {
                Ok(())
            }
            DataFileFormat::Parquet => Err(ChangeError::InternalInconsistency(format!(
                "PositionDeleteRef {} has Parquet file_format but content_offset/size set",
                self.delete_file_path
            ))),
            DataFileFormat::Puffin if self.referenced_data_file.is_none() => {
                Err(ChangeError::InternalInconsistency(format!(
                    "Puffin DV {} missing referenced_data_file",
                    self.delete_file_path
                )))
            }
            DataFileFormat::Puffin if self.content_offset.is_none() => {
                Err(ChangeError::InternalInconsistency(format!(
                    "Puffin DV {} missing content_offset",
                    self.delete_file_path
                )))
            }
            DataFileFormat::Puffin if self.content_size_in_bytes.is_none() => {
                Err(ChangeError::InternalInconsistency(format!(
                    "Puffin DV {} missing content_size_in_bytes",
                    self.delete_file_path
                )))
            }
            DataFileFormat::Puffin => Ok(()),
            other => Err(ChangeError::InternalInconsistency(format!(
                "PositionDeleteRef {} has unsupported file_format {other:?}",
                self.delete_file_path
            ))),
        }
    }
}

/// Build a filesystem access handle for provider-owned delta files at a table
/// location. Callers must pass process-local object-store configuration; no
/// credential is carried by a scan or split payload.
pub fn build_factory_for_table_location(
    location: &str,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<novarocks_fs::FsAccessHandle, ChangeError> {
    crate::fs_io::reader_factory_for_table_location(location, object_store_config).map_err(
        |error| {
            ChangeError::InternalInconsistency(format!(
                "build iceberg table reader factory for {location}: {error}"
            ))
        },
    )
}

pub fn expected_object_store_bucket_from_location(
    location: &str,
) -> Result<Option<String>, ChangeError> {
    let location = novarocks_fs::FsAccessResolver::new()
        .parse_location(location)
        .map_err(|error| {
            ChangeError::InternalInconsistency(format!(
                "parse iceberg table location {location}: {error}"
            ))
        })?;
    if location.scheme() == novarocks_fs::FsScheme::ObjectStore {
        return location
            .authority()
            .map(|bucket| Some(bucket.to_string()))
            .ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "object-store iceberg table location missing bucket: {}",
                    location.original()
                ))
            });
    }
    Ok(None)
}

pub fn expected_object_store_bucket_for_table(
    table: &crate::iceberg::table::Table,
) -> Result<Option<String>, ChangeError> {
    expected_object_store_bucket_from_location(table.metadata().location())
}

pub fn build_factory_for_table(
    table: &crate::iceberg::table::Table,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<novarocks_fs::FsAccessHandle, ChangeError> {
    build_factory_for_table_location(table.metadata().location(), object_store_config)
}

pub fn normalize_delete_projection_path(
    path: &str,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<String, ChangeError> {
    let parsed = novarocks_fs::FsAccessResolver::new()
        .parse_location(path)
        .map_err(|error| {
            ChangeError::InternalInconsistency(format!(
                "parse iceberg delete reverse projection path {path}: {error}"
            ))
        })?;
    match parsed.scheme() {
        novarocks_fs::FsScheme::Local => Ok(parsed.path().to_string()),
        novarocks_fs::FsScheme::ObjectStore => {
            let access = crate::fs_io::resolve_access_for_location(path, object_store_config)
                .map_err(|error| {
                    ChangeError::InternalInconsistency(format!(
                        "normalize object-store delete reverse projection path {path}: {error}"
                    ))
                })?;
            let bucket = access.handle().authority().ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "object-store delete reverse projection path {path} missing bucket"
                ))
            })?;
            if let Some(expected) = expected_object_store_bucket
                && bucket != expected
            {
                return Err(ChangeError::InternalInconsistency(format!(
                    "bucket mismatch for object-store delete reverse projection path {path}: path bucket={bucket} expected bucket={expected}"
                )));
            }
            access
                .single_relative_path()
                .map(str::to_string)
                .map_err(|error| {
                    ChangeError::InternalInconsistency(format!(
                        "normalize object-store delete reverse projection path {path}: {error}"
                    ))
                })
        }
        novarocks_fs::FsScheme::Hdfs => {
            crate::fs_io::normalize_hdfs_path_parse_only(path).map_err(|error| {
                ChangeError::InternalInconsistency(format!(
                    "normalize hdfs delete reverse projection path {path}: {error}"
                ))
            })
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergChangeBatch {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: Vec<DataFileRef>,
    pub deletes: Vec<PositionDeleteRef>,
    pub equality_deletes: Vec<EqualityDeleteRef>,
    pub deleted_data_files: Vec<DeletedDataFileRef>,
}

pub fn delta_source_files_from_change_batch(
    batch: &IcebergChangeBatch,
    equality_targets_by_delete_file: &HashMap<String, Vec<EqualityDeleteTargetData>>,
) -> Result<Vec<DeltaSourceFile>, String> {
    let mut out = Vec::with_capacity(
        batch.inserts.len()
            + batch.deletes.len()
            + batch.equality_deletes.len()
            + batch.deleted_data_files.len(),
    );
    for file in &batch.inserts {
        out.push(DeltaSourceFile {
            path: file.path.clone(),
            size: file.size,
            role: DeltaSourceRole::DataFile,
            partition_spec_id: file.partition_spec_id,
            partition_key: file.partition_key.clone(),
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            row_id_allow_list: file.row_id_allow_list.clone(),
        });
    }
    let deletes = batch
        .deletes
        .iter()
        .map(|delete| {
            let file_format = match delete.file_format {
                crate::iceberg::spec::DataFileFormat::Parquet => PositionDeleteFileFormat::Parquet,
                crate::iceberg::spec::DataFileFormat::Puffin => PositionDeleteFileFormat::Puffin,
                other => return Err(format!(
                    "ivm delta-scan payload: position-delete file {} has unsupported file_format {other:?}; only Parquet and Puffin are supported",
                    delete.delete_file_path
                )),
            };
            Ok(PositionDeleteSourceData {
                delete_file_path: delete.delete_file_path.clone(),
                delete_file_size: delete.delete_file_size,
                referenced_data_file: delete.referenced_data_file.clone(),
                file_format,
                content_offset: delete.content_offset,
                content_size_in_bytes: delete.content_size_in_bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(first) = deletes.first() {
        out.push(DeltaSourceFile {
            path: first.delete_file_path.clone(),
            size: 0,
            role: DeltaSourceRole::PositionDelete { deletes },
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            row_id_allow_list: None,
        });
    }
    for file in &batch.equality_deletes {
        out.push(DeltaSourceFile {
            path: file.delete_file_path.clone(),
            size: file.delete_file_size,
            role: DeltaSourceRole::EqualityDelete {
                equality_field_ids: file.equality_ids.clone(),
                targets: equality_targets_by_delete_file
                    .get(&file.delete_file_path)
                    .cloned()
                    .unwrap_or_default(),
            },
            partition_spec_id: file.partition_spec_id,
            partition_key: file.partition_key.clone(),
            first_row_id: None,
            data_sequence_number: file.sequence_number,
            row_id_allow_list: None,
        });
    }
    for file in &batch.deleted_data_files {
        out.push(DeltaSourceFile {
            path: file.path.clone(),
            size: file.size,
            role: DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility: None,
            },
            partition_spec_id: file.partition_spec_id,
            partition_key: file.partition_key.clone(),
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            row_id_allow_list: None,
        });
    }
    Ok(out)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BaseDataFileLineage {
    pub first_row_id: i64,
    pub data_sequence_number: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaDataColumn {
    pub name: String,
    pub field_id: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub row_id_allow_list: Option<BTreeSet<i64>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DeltaSourceRole {
    DataFile,
    PositionDelete {
        deletes: Vec<PositionDeleteSourceData>,
    },
    EqualityDelete {
        equality_field_ids: Vec<i32>,
        targets: Vec<EqualityDeleteTargetData>,
    },
    DeletedDataFile {
        previous_data_file_visibility: Option<DeletedFileVisibility>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PositionDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PositionDeleteSourceData {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub referenced_data_file: Option<String>,
    pub file_format: PositionDeleteFileFormat,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EqualityDeleteTargetData {
    pub data_file_path: String,
    pub data_file_size: i64,
    pub data_file_first_row_id: Option<i64>,
    pub data_file_sequence_number: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeletedFileVisibility {
    pub already_deleted_positions: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum DeleteVisibilityDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum DeleteVisibilityDeleteFileContent {
    Position,
    Equality,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct DeleteVisibilityDeleteFileDescriptor {
    pub path: String,
    pub file_format: DeleteVisibilityDeleteFileFormat,
    pub file_content: DeleteVisibilityDeleteFileContent,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct DeleteVisibilityDataFileDescriptor {
    pub path: String,
    pub size: i64,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub delete_files: Vec<DeleteVisibilityDeleteFileDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaScanDeleteSide {
    pub base_data_file_lineage: HashMap<String, BaseDataFileLineage>,
    pub previous_data_file_lineage: HashMap<String, BaseDataFileLineage>,
    pub previous_delete_visibility_data_files: Vec<DeleteVisibilityDataFileDescriptor>,
    pub previously_deleted_positions_per_file: HashMap<String, Vec<u64>>,
    pub deleted_data_file_paths: HashSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_batch_keeps_provider_delete_roles_separate() {
        let batch = IcebergChangeBatch {
            previous_snapshot_id: 10,
            current_snapshot_id: 11,
            inserts: vec![DataFileRef {
                path: "s3://warehouse/data.parquet".to_string(),
                size: 8,
                record_count: Some(1),
                partition_spec_id: None,
                partition_key: None,
                partition_values: Vec::new(),
                first_row_id: Some(1),
                data_sequence_number: Some(2),
                row_id_allow_list: None,
            }],
            deletes: vec![PositionDeleteRef {
                delete_file_path: "s3://warehouse/delete.parquet".to_string(),
                delete_file_size: 4,
                record_count: Some(1),
                referenced_data_file: Some("s3://warehouse/data.parquet".to_string()),
                file_format: crate::iceberg::spec::DataFileFormat::Parquet,
                content_offset: None,
                content_size_in_bytes: None,
                partition_values: Vec::new(),
            }],
            equality_deletes: Vec::new(),
            deleted_data_files: Vec::new(),
        };

        let files = delta_source_files_from_change_batch(&batch, &HashMap::new())
            .expect("delta source facts");
        assert!(matches!(files[0].role, DeltaSourceRole::DataFile));
        assert!(matches!(
            files[1].role,
            DeltaSourceRole::PositionDelete { .. }
        ));
    }

    #[test]
    fn lineage_break_requests_a_full_refresh_without_runtime_fallback() {
        assert_eq!(
            policy_signal_from_change_error(&ChangeError::LineageBroken {
                previous_snapshot: 42,
            }),
            IcebergChangePolicySignal::FullRefresh {
                reason: "previous snapshot is not reachable".to_string(),
            }
        );
    }

    #[test]
    fn table_location_bucket_projection_is_provider_owned() {
        assert_eq!(
            expected_object_store_bucket_from_location("s3://lake/warehouse/db/orders")
                .expect("object-store location"),
            Some("lake".to_string())
        );
        assert_eq!(
            expected_object_store_bucket_from_location("hdfs://namenode:9000/warehouse/db/orders")
                .expect("HDFS location"),
            None
        );
    }

    #[test]
    fn local_delete_projection_keeps_the_exact_local_path() {
        assert_eq!(
            normalize_delete_projection_path("file:///tmp/orders.parquet", None, None)
                .expect("local path"),
            "/tmp/orders.parquet"
        );
    }
}
