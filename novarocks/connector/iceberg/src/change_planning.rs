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

//! Provider-owned Iceberg change-window admission and manifest planning.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
    ConnectorChangePartitionValue, ConnectorChangeWindowAdmission,
    ConnectorChangeWindowFullRebuildReason, ConnectorChangeWindowPartitionImpact,
    ConnectorChangeWindowReplaceFailure, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext,
};

use crate::delta::{
    BaseDataFileLineage, ChangePartitionFieldValue, ChangePartitionValue, DataFileRef,
    DeleteVisibilityDataFileDescriptor, DeleteVisibilityDeleteFileContent,
    DeleteVisibilityDeleteFileDescriptor, DeleteVisibilityDeleteFileFormat, DeletedDataFileRef,
    DeltaScanDeleteSide, DeltaSourceFile, EqualityDeleteRef, EqualityDeleteTargetData,
    IcebergChangeBatch, PositionDeleteRef, change_partition_field_values,
    delta_source_files_from_change_batch,
};
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, ManifestContentType, ManifestStatus, Operation,
    Snapshot, TableMetadata,
};
use crate::iceberg::table::Table;
use crate::resources::IcebergCatalogRuntime;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct IcebergDeltaScanPlan {
    pub(crate) sources: Vec<DeltaSourceFile>,
    pub(crate) delete_side: Option<DeltaScanDeleteSide>,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LineageAction {
    CollectInserts { snapshot_id: i64 },
    CollectDeletes { snapshot_id: i64 },
    CollectOverwriteDiff { snapshot_id: i64 },
}

#[derive(Debug)]
enum LineageAdmission {
    MetadataOnly,
    Incremental(Vec<LineageAction>),
    FullRebuild(ConnectorChangeWindowFullRebuildReason),
}

/// Plans one exact Iceberg snapshot window without leaking manifests, file
/// paths, or Iceberg field identities into SPI admission facts.
pub(crate) fn plan_change_window(
    table: &Table,
    from_exclusive: i64,
    to_inclusive: i64,
    runtime: &IcebergCatalogRuntime,
    context: &ConnectorRequestContext,
) -> Result<(ConnectorChangeWindowAdmission, IcebergChangeBatch), ConnectorError> {
    check_active(context)?;
    let metadata = table.metadata();
    if !matches!(
        metadata.format_version(),
        FormatVersion::V2 | FormatVersion::V3
    ) {
        return Err(unsupported(
            "Iceberg change-window scans require table format v2 or v3",
        ));
    }
    let lineage = classify_lineage(metadata, from_exclusive, to_inclusive)?;
    let actions = match lineage {
        LineageAdmission::MetadataOnly => {
            return Ok((
                ConnectorChangeWindowAdmission::MetadataOnly,
                empty_batch(from_exclusive, to_inclusive),
            ));
        }
        LineageAdmission::FullRebuild(reason) => {
            return Ok((
                ConnectorChangeWindowAdmission::FullRebuild(reason),
                empty_batch(from_exclusive, to_inclusive),
            ));
        }
        LineageAdmission::Incremental(actions) => actions,
    };

    let collect_metadata = metadata.clone();
    let file_io = table.file_io().clone();
    let collected = runtime
        .block_on(async move { collect_files(&collect_metadata, &file_io, &actions).await })
        .map_err(unavailable)??;
    check_active(context)?;
    let batch = IcebergChangeBatch {
        previous_snapshot_id: from_exclusive,
        current_snapshot_id: to_inclusive,
        inserts: collected.0,
        deletes: collected.1,
        equality_deletes: collected.2,
        deleted_data_files: collected.3,
    };
    if batch.inserts.is_empty()
        && batch.deletes.is_empty()
        && batch.equality_deletes.is_empty()
        && batch.deleted_data_files.is_empty()
    {
        return Ok((ConnectorChangeWindowAdmission::MetadataOnly, batch));
    }
    let admission = ConnectorChangeWindowAdmission::Incremental {
        has_inserts: !batch.inserts.is_empty(),
        has_deletes: !batch.deletes.is_empty()
            || !batch.equality_deletes.is_empty()
            || !batch.deleted_data_files.is_empty(),
        partition_impact: partition_impact(metadata, &batch, context)?,
    };
    Ok((admission, batch))
}

pub(crate) fn freeze_delta_scan_plan(
    table: &Table,
    batch: &IcebergChangeBatch,
    runtime: &IcebergCatalogRuntime,
    binding: &crate::access_binding::IcebergReadBinding,
    context: &ConnectorRequestContext,
) -> Result<IcebergDeltaScanPlan, ConnectorError> {
    check_active(context)?;
    let equality_targets = equality_delete_targets_at(
        table,
        batch.current_snapshot_id,
        &batch.equality_deletes,
        runtime,
    )?;
    let sources =
        delta_source_files_from_change_batch(batch, &equality_targets).map_err(corrupt)?;
    let has_deletes = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let delete_side = if has_deletes {
        let base_data_file_lineage =
            data_file_lineage_index_at(table, batch.current_snapshot_id, runtime)?;
        let previous_data_file_lineage = if batch.deleted_data_files.is_empty() {
            HashMap::new()
        } else {
            data_file_lineage_index_at(table, batch.previous_snapshot_id, runtime)?
        };
        let touched: HashSet<String> = batch
            .deletes
            .iter()
            .filter_map(|delete| delete.referenced_data_file.clone())
            .collect();
        let previously_deleted_positions_per_file = if touched.is_empty() {
            HashMap::new()
        } else {
            previously_deleted_positions(
                table,
                batch.previous_snapshot_id,
                &touched,
                runtime,
                binding,
                context,
            )?
        };
        Some(DeltaScanDeleteSide {
            base_data_file_lineage,
            previous_data_file_lineage,
            previous_delete_visibility_data_files: delete_visibility_data_files_at(
                table,
                batch.previous_snapshot_id,
                runtime,
            )?,
            previously_deleted_positions_per_file,
            deleted_data_file_paths: batch
                .deleted_data_files
                .iter()
                .map(|file| file.path.clone())
                .collect(),
        })
    } else {
        None
    };
    check_active(context)?;
    Ok(IcebergDeltaScanPlan {
        sources,
        delete_side,
    })
}

fn equality_delete_targets_at(
    table: &Table,
    snapshot_id: i64,
    deletes: &[EqualityDeleteRef],
    runtime: &IcebergCatalogRuntime,
) -> Result<HashMap<String, Vec<EqualityDeleteTargetData>>, ConnectorError> {
    if deletes.is_empty() {
        return Ok(HashMap::new());
    }
    let table = table.clone();
    let snapshot = runtime
        .block_on(
            async move { crate::read_snapshot::build_read_snapshot_at(&table, snapshot_id).await },
        )
        .map_err(unavailable)?
        .map_err(unavailable)?;
    Ok(deletes
        .iter()
        .map(|delete| {
            let read_delete = equality_read_delete(delete);
            let targets = crate::read_model::data_files_matching_delete(&snapshot, &read_delete)
                .into_iter()
                .map(|file| EqualityDeleteTargetData {
                    data_file_path: file.path.clone(),
                    data_file_size: file.size,
                    data_file_first_row_id: file.first_row_id,
                    data_file_sequence_number: file.data_sequence_number,
                })
                .collect();
            (delete.delete_file_path.clone(), targets)
        })
        .collect())
}

fn equality_read_delete(delete: &EqualityDeleteRef) -> crate::read_model::IcebergReadDeleteFile {
    crate::read_model::IcebergReadDeleteFile {
        path: delete.delete_file_path.clone(),
        file_format: crate::read_model::IcebergReadDeleteFormat::Parquet,
        kind: crate::read_model::IcebergReadDeleteKind::Equality {
            equality_field_ids: delete.equality_ids.clone(),
        },
        length: Some(delete.delete_file_size),
        content_offset: None,
        content_size_in_bytes: None,
        sequence_number: delete.sequence_number,
        partition_spec_id: delete.partition_spec_id,
        partition_key: delete.partition_key.clone(),
        referenced_data_file: None,
    }
}

fn data_file_lineage_index_at(
    table: &Table,
    snapshot_id: i64,
    runtime: &IcebergCatalogRuntime,
) -> Result<HashMap<String, BaseDataFileLineage>, ConnectorError> {
    let table = table.clone();
    let snapshot = runtime
        .block_on(
            async move { crate::read_snapshot::build_read_snapshot_at(&table, snapshot_id).await },
        )
        .map_err(unavailable)?
        .map_err(unavailable)?;
    snapshot
        .files
        .iter()
        .map(|file| {
            let first_row_id = file.first_row_id.ok_or_else(|| {
                unsupported(format!(
                    "Iceberg delta delete requires first_row_id for data file {}",
                    file.path
                ))
            })?;
            let data_sequence_number = file.data_sequence_number.ok_or_else(|| {
                corrupt(format!(
                    "Iceberg delta delete requires data_sequence_number for data file {}",
                    file.path
                ))
            })?;
            Ok((
                file.path.clone(),
                BaseDataFileLineage {
                    first_row_id,
                    data_sequence_number,
                },
            ))
        })
        .collect()
}

fn delete_visibility_data_files_at(
    table: &Table,
    snapshot_id: i64,
    runtime: &IcebergCatalogRuntime,
) -> Result<Vec<DeleteVisibilityDataFileDescriptor>, ConnectorError> {
    let table = table.clone();
    runtime
        .block_on(async move {
            crate::manifest::extract_data_files_with_stats_at(&table, snapshot_id).await
        })
        .map_err(unavailable)?
        .map_err(unavailable)?
        .into_iter()
        .map(|file| {
            let delete_files = file
                .delete_files
                .into_iter()
                .map(|delete| DeleteVisibilityDeleteFileDescriptor {
                    path: delete.path,
                    file_format: match delete.file_format {
                        crate::scan_model::IcebergDeleteFileFormat::Parquet => {
                            DeleteVisibilityDeleteFileFormat::Parquet
                        }
                        crate::scan_model::IcebergDeleteFileFormat::Puffin => {
                            DeleteVisibilityDeleteFileFormat::Puffin
                        }
                    },
                    file_content: match delete.file_content {
                        crate::scan_model::IcebergDeleteFileContent::Position => {
                            DeleteVisibilityDeleteFileContent::Position
                        }
                        crate::scan_model::IcebergDeleteFileContent::Equality => {
                            DeleteVisibilityDeleteFileContent::Equality
                        }
                    },
                    length: delete.length,
                    content_offset: delete.content_offset,
                    content_size_in_bytes: delete.content_size_in_bytes,
                })
                .collect();
            Ok(DeleteVisibilityDataFileDescriptor {
                path: file.path,
                size: file.size,
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files,
            })
        })
        .collect()
}

fn previously_deleted_positions(
    table: &Table,
    snapshot_id: i64,
    touched: &HashSet<String>,
    runtime: &IcebergCatalogRuntime,
    binding: &crate::access_binding::IcebergReadBinding,
    context: &ConnectorRequestContext,
) -> Result<HashMap<String, Vec<u64>>, ConnectorError> {
    let table = table.clone();
    let snapshot = runtime
        .block_on(
            async move { crate::read_snapshot::build_read_snapshot_at(&table, snapshot_id).await },
        )
        .map_err(unavailable)?
        .map_err(unavailable)?;
    let mut result = HashMap::new();
    for file in snapshot.files {
        if !touched.contains(&file.path) {
            continue;
        }
        check_active(context)?;
        let specs = file
            .deletes
            .iter()
            .filter_map(position_delete_spec)
            .collect::<Result<Vec<_>, _>>()?;
        if specs.is_empty() {
            continue;
        }
        let access = binding.resolve_access_for_locations(
            std::iter::once(file.path.as_str()).chain(specs.iter().map(|spec| spec.path.as_str())),
        )?;
        let read_context =
            binding.file_read_context(novarocks_fs::FileCancellation::new(), context.deadline())?;
        let positions = crate::position_delete::load_position_deletes_with_context(
            &specs,
            &file.path,
            &access,
            &read_context,
        )
        .map_err(unavailable)?;
        if !positions.is_empty() {
            result.insert(file.path, positions.iter().collect());
        }
    }
    Ok(result)
}

fn position_delete_spec(
    delete: &crate::read_model::IcebergReadDeleteFile,
) -> Option<Result<crate::delete_file::IcebergDeleteFileSpec, ConnectorError>> {
    if !matches!(
        delete.kind,
        crate::read_model::IcebergReadDeleteKind::Position
    ) {
        return None;
    }
    Some(match delete.file_format {
        crate::read_model::IcebergReadDeleteFormat::Parquet => Ok(
            crate::delete_file::IcebergDeleteFileSpec::parquet_position_delete(
                delete.path.clone(),
                delete.length.and_then(|value| u64::try_from(value).ok()),
            ),
        ),
        crate::read_model::IcebergReadDeleteFormat::Puffin => {
            let offset = delete
                .content_offset
                .ok_or_else(|| corrupt("Iceberg Puffin deletion vector has no offset"));
            let size = delete
                .content_size_in_bytes
                .ok_or_else(|| corrupt("Iceberg Puffin deletion vector has no size"));
            match (offset, size) {
                (Ok(offset), Ok(size)) => Ok(
                    crate::delete_file::IcebergDeleteFileSpec::puffin_position_delete(
                        delete.path.clone(),
                        delete.length.and_then(|value| u64::try_from(value).ok()),
                        offset,
                        size,
                    ),
                ),
                (Err(error), _) | (_, Err(error)) => Err(error),
            }
        }
    })
}

fn empty_batch(from_exclusive: i64, to_inclusive: i64) -> IcebergChangeBatch {
    IcebergChangeBatch {
        previous_snapshot_id: from_exclusive,
        current_snapshot_id: to_inclusive,
        inserts: Vec::new(),
        deletes: Vec::new(),
        equality_deletes: Vec::new(),
        deleted_data_files: Vec::new(),
    }
}

fn classify_lineage(
    metadata: &TableMetadata,
    from_exclusive: i64,
    to_inclusive: i64,
) -> Result<LineageAdmission, ConnectorError> {
    if from_exclusive == to_inclusive {
        return Ok(LineageAdmission::MetadataOnly);
    }
    let Some(mut current) = metadata.snapshot_by_id(to_inclusive) else {
        return Err(corrupt(format!(
            "Iceberg change-window upper snapshot {to_inclusive} is missing from metadata"
        )));
    };
    if metadata.snapshot_by_id(from_exclusive).is_none() {
        return Ok(LineageAdmission::FullRebuild(
            ConnectorChangeWindowFullRebuildReason::LineageBroken {
                from_snapshot_id: from_exclusive,
            },
        ));
    }

    let mut actions = Vec::new();
    loop {
        let snapshot = current.as_ref();
        let parent_id = snapshot.parent_snapshot_id();
        let parent = parent_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .map(|value| value.as_ref());
        match classify_snapshot(snapshot, parent)? {
            SnapshotDecision::Action(action) => actions.push(action),
            SnapshotDecision::MetadataOnly => {}
            SnapshotDecision::FullRebuild(reason) => {
                return Ok(LineageAdmission::FullRebuild(reason));
            }
        }
        if !matches!(snapshot.summary().operation, Operation::Replace)
            && parent.is_some_and(|parent| snapshot.schema_id() != parent.schema_id())
        {
            return Err(unsupported(format!(
                "Iceberg schema changed at snapshot {}; incremental change-window planning is not supported",
                snapshot.snapshot_id()
            )));
        }
        match parent_id {
            Some(id) if id == from_exclusive => break,
            Some(id) => {
                let Some(parent) = metadata.snapshot_by_id(id) else {
                    return Ok(LineageAdmission::FullRebuild(
                        ConnectorChangeWindowFullRebuildReason::LineageBroken {
                            from_snapshot_id: from_exclusive,
                        },
                    ));
                };
                current = parent;
            }
            None => {
                return Ok(LineageAdmission::FullRebuild(
                    ConnectorChangeWindowFullRebuildReason::LineageBroken {
                        from_snapshot_id: from_exclusive,
                    },
                ));
            }
        }
    }
    actions.reverse();
    Ok(if actions.is_empty() {
        LineageAdmission::MetadataOnly
    } else {
        LineageAdmission::Incremental(actions)
    })
}

enum SnapshotDecision {
    Action(LineageAction),
    MetadataOnly,
    FullRebuild(ConnectorChangeWindowFullRebuildReason),
}

fn classify_snapshot(
    snapshot: &Snapshot,
    parent: Option<&Snapshot>,
) -> Result<SnapshotDecision, ConnectorError> {
    let snapshot_id = snapshot.snapshot_id();
    Ok(match snapshot.summary().operation {
        Operation::Append => {
            SnapshotDecision::Action(LineageAction::CollectInserts { snapshot_id })
        }
        Operation::Delete => {
            SnapshotDecision::Action(LineageAction::CollectDeletes { snapshot_id })
        }
        Operation::Overwrite => {
            SnapshotDecision::Action(LineageAction::CollectOverwriteDiff { snapshot_id })
        }
        Operation::Replace => {
            let Some(parent) = parent else {
                return Ok(SnapshotDecision::FullRebuild(unproven_replace(
                    snapshot_id,
                    ConnectorChangeWindowReplaceFailure::MissingParent,
                )));
            };
            if let Some(failure) = validate_replace_snapshot(snapshot, parent)? {
                SnapshotDecision::FullRebuild(unproven_replace(snapshot_id, failure))
            } else {
                SnapshotDecision::MetadataOnly
            }
        }
    })
}

fn validate_replace_snapshot(
    snapshot: &Snapshot,
    parent: &Snapshot,
) -> Result<Option<ConnectorChangeWindowReplaceFailure>, ConnectorError> {
    let summary = &snapshot.summary().additional_properties;
    let parent_summary = &parent.summary().additional_properties;
    let records = parse_summary_i64(summary.get("total-records"))?;
    let parent_records = parse_summary_i64(parent_summary.get("total-records"))?;
    let (Some(records), Some(parent_records)) = (records, parent_records) else {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary,
        ));
    };
    if records != parent_records {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::RecordCountChanged,
        ));
    }
    let Some(added) = parse_summary_i64(summary.get("added-data-files"))? else {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary,
        ));
    };
    let Some(removed) = parse_summary_i64(summary.get("deleted-data-files"))? else {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary,
        ));
    };
    if added < 0 || removed < 0 {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts,
        ));
    }
    let valid = (added > 0 && removed > 0)
        || (added == 0 && removed == 0)
        || (records == 0 && added == 0 && removed > 0);
    if !valid {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts,
        ));
    }
    if snapshot.schema_id() != parent.schema_id() {
        return Ok(Some(ConnectorChangeWindowReplaceFailure::SchemaChanged));
    }
    Ok(None)
}

fn parse_summary_i64(value: Option<&String>) -> Result<Option<i64>, ConnectorError> {
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(value.parse::<i64>().ok())
}

fn unproven_replace(
    snapshot_id: i64,
    failure: ConnectorChangeWindowReplaceFailure,
) -> ConnectorChangeWindowFullRebuildReason {
    ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
        snapshot_id,
        failure,
    }
}

fn partition_impact(
    metadata: &TableMetadata,
    batch: &IcebergChangeBatch,
    context: &ConnectorRequestContext,
) -> Result<ConnectorChangeWindowPartitionImpact, ConnectorError> {
    if metadata.default_partition_spec().is_unpartitioned() {
        return Ok(ConnectorChangeWindowPartitionImpact::Unpartitioned);
    }
    let added = batch
        .inserts
        .iter()
        .map(|file| connector_partition(&file.partition_values))
        .collect::<Result<Option<Vec<_>>, _>>()?;
    let removed = batch
        .deleted_data_files
        .iter()
        .map(|file| connector_partition(&file.partition_values))
        .collect::<Result<Option<Vec<_>>, _>>()?;
    let (Some(added), Some(removed)) = (added, removed) else {
        return Ok(ConnectorChangeWindowPartitionImpact::Unavailable);
    };
    ConnectorChangeWindowPartitionImpact::try_exact(
        !batch.deletes.is_empty() || !batch.equality_deletes.is_empty(),
        added,
        removed,
        context,
    )
}

fn connector_partition(
    values: &[ChangePartitionFieldValue],
) -> Result<Option<ConnectorChangePartition>, ConnectorError> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(values.len());
    for value in values {
        let Some(source_column) = value.source_column.as_deref() else {
            return Ok(None);
        };
        let Some(transform) = connector_transform(&value.transform) else {
            return Ok(None);
        };
        let value = match &value.value {
            ChangePartitionValue::Null => ConnectorChangePartitionValue::Null,
            ChangePartitionValue::Primitive(value) => {
                ConnectorChangePartitionValue::String(Arc::from(value.as_str()))
            }
            ChangePartitionValue::Unsupported(_) => return Ok(None),
        };
        fields.push(ConnectorChangePartitionField::try_new(
            source_column,
            transform,
            value,
        )?);
    }
    ConnectorChangePartition::try_new(fields).map(Some)
}

fn connector_transform(value: &str) -> Option<ConnectorChangePartitionTransform> {
    match value.to_ascii_lowercase().as_str() {
        "identity" => Some(ConnectorChangePartitionTransform::Identity),
        "year" => Some(ConnectorChangePartitionTransform::Year),
        "month" => Some(ConnectorChangePartitionTransform::Month),
        "day" => Some(ConnectorChangePartitionTransform::Day),
        "hour" => Some(ConnectorChangePartitionTransform::Hour),
        value if value.starts_with("bucket(") && value.ends_with(')') => value[7..value.len() - 1]
            .parse::<u32>()
            .ok()
            .and_then(NonZeroU32::new)
            .map(|buckets| ConnectorChangePartitionTransform::Bucket { buckets }),
        value if value.starts_with("truncate(") && value.ends_with(')') => value
            [9..value.len() - 1]
            .parse::<u32>()
            .ok()
            .and_then(NonZeroU32::new)
            .map(|width| ConnectorChangePartitionTransform::Truncate { width }),
        _ => None,
    }
}

type CollectedFiles = (
    Vec<DataFileRef>,
    Vec<PositionDeleteRef>,
    Vec<EqualityDeleteRef>,
    Vec<DeletedDataFileRef>,
);

async fn collect_files(
    metadata: &TableMetadata,
    file_io: &crate::iceberg::io::FileIO,
    actions: &[LineageAction],
) -> Result<CollectedFiles, ConnectorError> {
    let mut inserts = Vec::new();
    let mut deletes = Vec::new();
    let mut equality_deletes = Vec::new();
    let mut deleted_data_files = Vec::new();
    for action in actions {
        let snapshot_id = match action {
            LineageAction::CollectInserts { snapshot_id }
            | LineageAction::CollectDeletes { snapshot_id }
            | LineageAction::CollectOverwriteDiff { snapshot_id } => *snapshot_id,
        };
        let snapshot = metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| corrupt(format!("Iceberg snapshot {snapshot_id} disappeared")))?;
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|error| unavailable(format!("load Iceberg manifest list: {error}")))?;
        match action {
            LineageAction::CollectInserts { .. } => {
                collect_added_data(metadata, snapshot_id, file_io, &manifest_list, &mut inserts)
                    .await?;
            }
            LineageAction::CollectDeletes { .. } => {
                collect_added_data(metadata, snapshot_id, file_io, &manifest_list, &mut inserts)
                    .await?;
                collect_added_deletes(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut deletes,
                    &mut equality_deletes,
                )
                .await?;
            }
            LineageAction::CollectOverwriteDiff { .. } => {
                collect_added_data(metadata, snapshot_id, file_io, &manifest_list, &mut inserts)
                    .await?;
                collect_deleted_data(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut deleted_data_files,
                )
                .await?;
            }
        }
    }
    Ok((inserts, deletes, equality_deletes, deleted_data_files))
}

async fn collect_added_data(
    metadata: &TableMetadata,
    snapshot_id: i64,
    file_io: &crate::iceberg::io::FileIO,
    manifest_list: &crate::iceberg::spec::ManifestList,
    out: &mut Vec<DataFileRef>,
) -> Result<(), ConnectorError> {
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data
            || manifest_file.added_snapshot_id != snapshot_id
        {
            continue;
        }
        let mut next_first_row_id = manifest_file
            .first_row_id
            .map(|value| {
                i64::try_from(value).map_err(|_| corrupt("Iceberg first_row_id overflows i64"))
            })
            .transpose()?;
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|error| unavailable(format!("load Iceberg data manifest: {error}")))?;
        for entry in manifest.entries() {
            if entry.status != ManifestStatus::Added
                || entry.snapshot_id() != Some(snapshot_id)
                || entry.data_file().content_type() != DataContentType::Data
            {
                continue;
            }
            let file = entry.data_file();
            let record_count = i64::try_from(file.record_count()).unwrap_or(i64::MAX);
            let first_row_id = file.first_row_id().or(next_first_row_id);
            if let Some(next) = next_first_row_id.as_mut() {
                *next = next
                    .checked_add(record_count)
                    .ok_or_else(|| corrupt("Iceberg first_row_id range overflows i64"))?;
            }
            out.push(DataFileRef {
                path: file.file_path().to_string(),
                size: i64::try_from(file.file_size_in_bytes()).unwrap_or(i64::MAX),
                record_count: Some(record_count),
                partition_spec_id: Some(manifest_file.partition_spec_id),
                partition_key: partition_key(file.partition()),
                partition_values: change_partition_field_values(
                    metadata,
                    manifest_file.partition_spec_id,
                    file.partition(),
                )
                .map_err(change_error)?,
                first_row_id,
                data_sequence_number: Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                ),
                row_id_allow_list: None,
            });
        }
    }
    Ok(())
}

async fn collect_deleted_data(
    metadata: &TableMetadata,
    snapshot_id: i64,
    file_io: &crate::iceberg::io::FileIO,
    manifest_list: &crate::iceberg::spec::ManifestList,
    out: &mut Vec<DeletedDataFileRef>,
) -> Result<(), ConnectorError> {
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data
            || manifest_file.added_snapshot_id != snapshot_id
        {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|error| unavailable(format!("load Iceberg overwrite manifest: {error}")))?;
        for entry in manifest.entries() {
            if entry.status != ManifestStatus::Deleted
                || entry.snapshot_id() != Some(snapshot_id)
                || entry.data_file().content_type() != DataContentType::Data
            {
                continue;
            }
            let file = entry.data_file();
            out.push(DeletedDataFileRef {
                path: file.file_path().to_string(),
                size: i64::try_from(file.file_size_in_bytes()).unwrap_or(i64::MAX),
                record_count: Some(i64::try_from(file.record_count()).unwrap_or(i64::MAX)),
                partition_spec_id: Some(manifest_file.partition_spec_id),
                partition_key: partition_key(file.partition()),
                partition_values: change_partition_field_values(
                    metadata,
                    manifest_file.partition_spec_id,
                    file.partition(),
                )
                .map_err(change_error)?,
                first_row_id: file.first_row_id(),
                data_sequence_number: Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                ),
            });
        }
    }
    Ok(())
}

async fn collect_added_deletes(
    metadata: &TableMetadata,
    snapshot_id: i64,
    file_io: &crate::iceberg::io::FileIO,
    manifest_list: &crate::iceberg::spec::ManifestList,
    positions: &mut Vec<PositionDeleteRef>,
    equalities: &mut Vec<EqualityDeleteRef>,
) -> Result<(), ConnectorError> {
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes
            || manifest_file.added_snapshot_id != snapshot_id
        {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|error| unavailable(format!("load Iceberg delete manifest: {error}")))?;
        for entry in manifest.entries() {
            if entry.status != ManifestStatus::Added || entry.snapshot_id() != Some(snapshot_id) {
                continue;
            }
            let file = entry.data_file();
            match file.content_type() {
                DataContentType::PositionDeletes => {
                    let (referenced, offset, size) = match file.file_format() {
                        DataFileFormat::Parquet => (file.referenced_data_file(), None, None),
                        DataFileFormat::Puffin => (
                            Some(file.referenced_data_file().ok_or_else(|| {
                                corrupt("Iceberg Puffin deletion vector has no referenced file")
                            })?),
                            Some(file.content_offset().ok_or_else(|| {
                                corrupt("Iceberg Puffin deletion vector has no offset")
                            })?),
                            Some(file.content_size_in_bytes().ok_or_else(|| {
                                corrupt("Iceberg Puffin deletion vector has no size")
                            })?),
                        ),
                        _ => {
                            return Err(corrupt(
                                "Iceberg position-delete manifest uses an unsupported format",
                            ));
                        }
                    };
                    let delete = PositionDeleteRef {
                        delete_file_path: file.file_path().to_string(),
                        delete_file_size: i64::try_from(file.file_size_in_bytes())
                            .unwrap_or(i64::MAX),
                        record_count: Some(i64::try_from(file.record_count()).unwrap_or(i64::MAX)),
                        referenced_data_file: referenced,
                        file_format: file.file_format(),
                        content_offset: offset,
                        content_size_in_bytes: size,
                        partition_values: change_partition_field_values(
                            metadata,
                            manifest_file.partition_spec_id,
                            file.partition(),
                        )
                        .map_err(change_error)?,
                    };
                    delete.validate_invariants().map_err(change_error)?;
                    positions.push(delete);
                }
                DataContentType::EqualityDeletes => {
                    if file.file_format() != DataFileFormat::Parquet {
                        return Err(corrupt(
                            "Iceberg equality-delete manifest uses an unsupported format",
                        ));
                    }
                    let equality_ids = file
                        .equality_ids()
                        .filter(|values| !values.is_empty())
                        .ok_or_else(|| corrupt("Iceberg equality-delete file has no field IDs"))?;
                    equalities.push(EqualityDeleteRef {
                        delete_file_path: file.file_path().to_string(),
                        delete_file_size: i64::try_from(file.file_size_in_bytes())
                            .unwrap_or(i64::MAX),
                        record_count: Some(i64::try_from(file.record_count()).unwrap_or(i64::MAX)),
                        equality_ids,
                        sequence_number: Some(
                            entry
                                .sequence_number()
                                .unwrap_or(manifest_file.sequence_number),
                        ),
                        partition_spec_id: Some(manifest_file.partition_spec_id),
                        partition_key: partition_key(file.partition()),
                        partition_values: change_partition_field_values(
                            metadata,
                            manifest_file.partition_spec_id,
                            file.partition(),
                        )
                        .map_err(change_error)?,
                    });
                }
                DataContentType::Data => {
                    return Err(corrupt("Iceberg delete manifest contains a data file"));
                }
            }
        }
    }
    Ok(())
}

fn partition_key(partition: &crate::iceberg::spec::Struct) -> Option<String> {
    (!partition.fields().is_empty()).then(|| format!("{partition:?}"))
}

fn check_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg change-window planning was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg change-window planning deadline elapsed",
        ));
    }
    Ok(())
}

fn change_error(error: crate::delta::ChangeError) -> ConnectorError {
    corrupt(error.to_string())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message).with_retryable_before_progress()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, Summary, TableMetadata, TableMetadataBuilder, Type,
    };

    use super::*;

    fn snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: properties
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect::<HashMap<_, _>>(),
            })
            .with_schema_id(schema_id)
            .build()
    }

    fn metadata_with_snapshots(snapshots: Vec<Snapshot>) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/change-window-test".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder");
        for snapshot in snapshots {
            builder = builder.add_snapshot(snapshot).expect("add snapshot");
        }
        builder.build().expect("metadata").metadata
    }

    fn replace_failure(
        parent: Option<&Snapshot>,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> ConnectorChangeWindowReplaceFailure {
        let replace = snapshot(
            2,
            parent.map(Snapshot::snapshot_id),
            Operation::Replace,
            properties,
            schema_id,
        );
        let SnapshotDecision::FullRebuild(
            ConnectorChangeWindowFullRebuildReason::UnprovenReplace { failure, .. },
        ) = classify_snapshot(&replace, parent).expect("replace admission")
        else {
            panic!("expected typed unproven REPLACE admission")
        };
        failure
    }

    #[test]
    fn replace_failures_are_typed_without_provider_reason_strings() {
        let parent = snapshot(1, None, Operation::Append, &[("total-records", "100")], 0);
        assert_eq!(
            replace_failure(None, &[("total-records", "100")], 0),
            ConnectorChangeWindowReplaceFailure::MissingParent
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[
                    ("total-records", "101"),
                    ("added-data-files", "1"),
                    ("deleted-data-files", "1"),
                ],
                0,
            ),
            ConnectorChangeWindowReplaceFailure::RecordCountChanged
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[("added-data-files", "1"), ("deleted-data-files", "1")],
                0,
            ),
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[
                    ("total-records", "100"),
                    ("added-data-files", "0"),
                    ("deleted-data-files", "1"),
                ],
                0,
            ),
            ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[
                    ("total-records", "100"),
                    ("added-data-files", "1"),
                    ("deleted-data-files", "1"),
                ],
                7,
            ),
            ConnectorChangeWindowReplaceFailure::SchemaChanged
        );
    }

    #[test]
    fn valid_replace_is_metadata_only() {
        let parent = snapshot(1, None, Operation::Append, &[("total-records", "100")], 0);
        let replace = snapshot(
            2,
            Some(1),
            Operation::Replace,
            &[
                ("total-records", "100"),
                ("added-data-files", "3"),
                ("deleted-data-files", "2"),
            ],
            0,
        );
        assert!(matches!(
            classify_snapshot(&replace, Some(&parent)).expect("replace admission"),
            SnapshotDecision::MetadataOnly
        ));
    }

    #[test]
    fn partition_transform_projection_is_typed_and_bounded() {
        assert_eq!(
            connector_transform("bucket(16)"),
            Some(ConnectorChangePartitionTransform::Bucket {
                buckets: NonZeroU32::new(16).expect("nonzero")
            })
        );
        assert_eq!(connector_transform("bucket(0)"), None);
        assert_eq!(connector_transform("void"), None);
    }

    #[test]
    fn equal_endpoints_are_metadata_only_without_ordering_snapshot_identities() {
        let metadata = metadata_with_snapshots(Vec::new());
        assert!(matches!(
            classify_lineage(&metadata, 41, 41).expect("equal endpoint admission"),
            LineageAdmission::MetadataOnly
        ));
    }

    #[test]
    fn missing_from_snapshot_is_a_typed_lineage_full_rebuild() {
        let current = snapshot(2, None, Operation::Append, &[], 0);
        let metadata = metadata_with_snapshots(vec![current]);
        assert!(matches!(
            classify_lineage(&metadata, 1, 2).expect("lineage admission"),
            LineageAdmission::FullRebuild(ConnectorChangeWindowFullRebuildReason::LineageBroken {
                from_snapshot_id: 1
            })
        ));
    }

    #[test]
    fn missing_upper_snapshot_remains_a_hard_corrupt_data_error() {
        let metadata = metadata_with_snapshots(Vec::new());
        let error = classify_lineage(&metadata, 1, 2).expect_err("missing upper snapshot");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}
