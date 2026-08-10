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

//! Provider-owned manifest walk that builds the Iceberg read view.

use std::collections::HashMap;

use crate::iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};
use crate::iceberg::table::Table;
use crate::read_model::{
    DeleteApplicabilityIndex, IcebergReadDeleteFile, IcebergReadDeleteFormat,
    IcebergReadDeleteKind, IcebergReadFile, IcebergReadSnapshot, iceberg_partition_key,
};
use crate::scan_model::IcebergColumnStats;

/// Build a read snapshot from one pinned Iceberg snapshot.
///
/// The walk resolves the snapshot schema before mapping manifest statistics,
/// then attaches only sequence/partition/referenced-file compatible deletes.
/// It is async so callers can choose the runtime owner appropriate to their
/// process role.
pub async fn build_read_snapshot_at(
    table: &Table,
    snapshot_id: i64,
) -> Result<IcebergReadSnapshot, String> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;

    // Use the schema associated with this snapshot for correct schema-evolution semantics.
    // Snapshot::schema() resolves the schema via schema_id if set, falling back to current.
    let schema = snapshot
        .schema(metadata)
        .map_err(|e| format!("resolve snapshot schema: {e}"))?;
    let field_id_to_name: HashMap<i32, String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field.name.clone()))
        .collect();

    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| format!("load manifest list: {e}"))?;

    let mut delete_index = DeleteApplicabilityIndex::default();

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest: {e}"))?;

        let partition_spec_id = manifest_file.partition_spec_id;
        for entry in manifest.entries() {
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let df = entry.data_file();
            let sequence_number = Some(
                entry
                    .sequence_number()
                    .unwrap_or(manifest_file.sequence_number),
            );

            match df.content_type() {
                DataContentType::PositionDeletes => {
                    let (file_format, content_offset, content_size_in_bytes) = match df
                        .file_format()
                    {
                        DataFileFormat::Parquet => (IcebergReadDeleteFormat::Parquet, None, None),
                        DataFileFormat::Puffin => {
                            let offset = df.content_offset().ok_or_else(|| {
                                format!("Puffin DV {} missing content_offset", df.file_path())
                            })?;
                            let length = df.content_size_in_bytes().ok_or_else(|| {
                                format!(
                                    "Puffin DV {} missing content_size_in_bytes",
                                    df.file_path()
                                )
                            })?;
                            (IcebergReadDeleteFormat::Puffin, Some(offset), Some(length))
                        }
                        other => {
                            return Err(format!(
                                "unsupported iceberg delete file format {:?}: {}",
                                other,
                                df.file_path()
                            ));
                        }
                    };

                    delete_index.push(IcebergReadDeleteFile {
                        path: df.file_path().to_string(),
                        file_format,
                        kind: IcebergReadDeleteKind::Position,
                        length: Some(
                            i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                                format!("delete file too large: {}", df.file_path())
                            })?,
                        ),
                        content_offset,
                        content_size_in_bytes,
                        sequence_number,
                        partition_spec_id: Some(partition_spec_id),
                        partition_key: iceberg_partition_key(df.partition()),
                        referenced_data_file: df.referenced_data_file(),
                    });
                }
                DataContentType::EqualityDeletes => {
                    if df.file_format() != DataFileFormat::Parquet {
                        return Err(format!(
                            "unsupported iceberg equality-delete file format {:?}: {}",
                            df.file_format(),
                            df.file_path()
                        ));
                    }
                    let equality_field_ids = df.equality_ids().ok_or_else(|| {
                        format!(
                            "iceberg equality-delete file {} missing equality_ids",
                            df.file_path()
                        )
                    })?;
                    if equality_field_ids.is_empty() {
                        return Err(format!(
                            "iceberg equality-delete file {} has empty equality_ids",
                            df.file_path()
                        ));
                    }

                    delete_index.push(IcebergReadDeleteFile {
                        path: df.file_path().to_string(),
                        file_format: IcebergReadDeleteFormat::Parquet,
                        kind: IcebergReadDeleteKind::Equality { equality_field_ids },
                        length: Some(
                            i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                                format!("delete file too large: {}", df.file_path())
                            })?,
                        ),
                        content_offset: None,
                        content_size_in_bytes: None,
                        sequence_number,
                        partition_spec_id: Some(partition_spec_id),
                        partition_key: iceberg_partition_key(df.partition()),
                        referenced_data_file: None,
                    });
                }
                DataContentType::Data => {}
            }
        }
    }

    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest: {e}"))?;

        let mut next_manifest_first_row_id = manifest_file
            .first_row_id
            .map(|value| {
                i64::try_from(value)
                    .map_err(|_| format!("manifest first_row_id too large: {value}"))
            })
            .transpose()?;

        for entry in manifest.entries() {
            if entry.status == ManifestStatus::Deleted {
                continue;
            }

            let df = entry.data_file();
            if df.content_type() != DataContentType::Data {
                continue;
            }

            let record_count_i64 = i64::try_from(df.record_count())
                .map_err(|_| format!("record_count too large for {}", df.file_path()))?;
            let first_row_id = df.first_row_id().or(next_manifest_first_row_id);
            if let Some(next) = next_manifest_first_row_id.as_mut() {
                *next = next.checked_add(record_count_i64).ok_or_else(|| {
                    format!(
                        "first_row_id overflow for manifest {}",
                        manifest_file.manifest_path
                    )
                })?;
            }

            let null_counts = df.null_value_counts();
            let value_counts = df.value_counts();
            let col_sizes = df.column_sizes();
            let lower = df.lower_bounds();
            let upper = df.upper_bounds();
            let has_any_stats = !null_counts.is_empty()
                || !value_counts.is_empty()
                || !col_sizes.is_empty()
                || !lower.is_empty()
                || !upper.is_empty();

            let column_stats = if has_any_stats {
                let mut all_ids = std::collections::HashSet::new();
                all_ids.extend(null_counts.keys());
                all_ids.extend(value_counts.keys());
                all_ids.extend(col_sizes.keys());
                all_ids.extend(lower.keys());
                all_ids.extend(upper.keys());

                let mut stats_map = HashMap::new();
                for &field_id in &all_ids {
                    if let Some(column_name) = field_id_to_name.get(&field_id) {
                        let lower_bound = lower
                            .get(&field_id)
                            .and_then(|datum| datum.to_bytes().ok())
                            .map(|bytes| bytes.to_vec());
                        let upper_bound = upper
                            .get(&field_id)
                            .and_then(|datum| datum.to_bytes().ok())
                            .map(|bytes| bytes.to_vec());
                        stats_map.insert(
                            column_name.clone(),
                            IcebergColumnStats {
                                null_count: null_counts
                                    .get(&field_id)
                                    .map(|&value| i64::try_from(value).unwrap_or(i64::MAX)),
                                value_count: value_counts
                                    .get(&field_id)
                                    .map(|&value| i64::try_from(value).unwrap_or(i64::MAX)),
                                column_size: col_sizes
                                    .get(&field_id)
                                    .map(|&value| i64::try_from(value).unwrap_or(i64::MAX)),
                                lower_bound,
                                upper_bound,
                            },
                        );
                    }
                }
                Some(stats_map)
            } else {
                None
            };

            let data_sequence_number = Some(
                entry
                    .sequence_number()
                    .unwrap_or(manifest_file.sequence_number),
            );
            let mut read_file = IcebergReadFile {
                path: df.file_path().to_string(),
                size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                record_count: Some(record_count_i64),
                column_stats,
                partition_spec_id: Some(manifest_file.partition_spec_id),
                partition_key: iceberg_partition_key(df.partition()),
                partition_values: Some(df.partition().clone()),
                manifest_path: Some(manifest_file.manifest_path.clone()),
                first_row_id,
                data_sequence_number,
                deletes: Vec::new(),
            };
            delete_index.attach_to(&mut read_file);
            files.push(read_file);
        }
    }

    Ok(IcebergReadSnapshot {
        snapshot_id: Some(snapshot_id),
        files,
    })
}

/// Build the read view for the current snapshot, if the table has one.
///
/// Runtime ownership remains with the caller: the provider exposes an async
/// manifest walk and never discovers a Tokio runtime itself.
pub async fn build_current_read_snapshot(table: &Table) -> Result<IcebergReadSnapshot, String> {
    match table.metadata().current_snapshot() {
        Some(snapshot) => build_read_snapshot_at(table, snapshot.snapshot_id()).await,
        None => Ok(IcebergReadSnapshot {
            snapshot_id: None,
            files: Vec::new(),
        }),
    }
}
