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

use arrow::array::RecordBatch;

use super::catalog::registry;

pub(crate) struct ReferencedDataFilePartition {
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_values: novarocks_connector_iceberg::iceberg::spec::Struct,
}

pub(crate) type ReferencedDataFilePartitions = HashMap<String, ReferencedDataFilePartition>;

/// Snapshot-aware version of [`load_referenced_data_file_partitions`].
///
/// Uses `snapshot_id` when `Some`, otherwise falls back to the current snapshot.
pub(crate) fn load_referenced_data_file_partitions_at(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    snapshot_id: Option<i64>,
) -> Result<ReferencedDataFilePartitions, String> {
    let data_files = match snapshot_id {
        Some(id) => registry::extract_data_files_with_stats_at(table, id)?,
        None => registry::extract_data_files_with_stats(table)?,
    };
    let mut out = HashMap::with_capacity(data_files.len());
    for data_file in data_files {
        let partition_spec_id = data_file.partition_spec_id.ok_or_else(|| {
            format!(
                "iceberg data file `{}` missing partition spec id",
                data_file.path
            )
        })?;
        let partition_values = data_file.partition_values.ok_or_else(|| {
            format!(
                "iceberg data file `{}` missing partition values",
                data_file.path
            )
        })?;
        let partition = ReferencedDataFilePartition {
            partition_spec_id,
            partition_values,
        };
        insert_referenced_data_file_partition(&mut out, data_file.path, partition)?;
    }
    Ok(out)
}

pub(crate) fn load_referenced_data_file_partitions(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<ReferencedDataFilePartitions, String> {
    load_referenced_data_file_partitions_at(table, None)
}

pub(crate) fn insert_referenced_data_file_partition(
    partitions: &mut ReferencedDataFilePartitions,
    path: String,
    partition: ReferencedDataFilePartition,
) -> Result<(), String> {
    match partitions.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            return Err(format!(
                "iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ExistingDeleteVisibility {
    pub(crate) deleted_positions: roaring::RoaringTreemap,
    pub(crate) equality_deletes: Vec<super::equality_delete::EqualityDeleteSet>,
}

pub(crate) type ExistingDeleteVisibilityByDataFile = HashMap<String, ExistingDeleteVisibility>;

/// Snapshot-aware version of [`load_existing_delete_visibility_by_data_file`].
///
/// Uses `snapshot_id` when `Some`, otherwise falls back to the current snapshot.
pub(crate) fn load_existing_delete_visibility_by_data_file_at(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    snapshot_id: Option<i64>,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    let data_files = match snapshot_id {
        Some(id) => registry::extract_data_files_with_stats_at(table, id)?,
        None => registry::extract_data_files_with_stats(table)?,
    };
    load_delete_visibility_from_data_files(data_files, object_store_config)
}

pub(crate) fn load_existing_delete_visibility_by_data_file(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    load_existing_delete_visibility_by_data_file_at(table, None, object_store_config)
}

pub(crate) fn load_existing_delete_visibility_from_descriptors(
    data_files: &[super::changes::DeleteVisibilityDataFileDescriptor],
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    let mut out = ExistingDeleteVisibilityByDataFile::new();
    for data_file in data_files {
        let delete_specs = data_file
            .delete_files
            .iter()
            .map(|file| DeleteFileInput {
                path: &file.path,
                length: file.length,
                content_offset: file.content_offset,
                content_size_in_bytes: file.content_size_in_bytes,
                file_format: match file.file_format {
                    super::changes::DeleteVisibilityDeleteFileFormat::Parquet => {
                        super::delete_file::IcebergFileFormat::Parquet
                    }
                    super::changes::DeleteVisibilityDeleteFileFormat::Puffin => {
                        super::delete_file::IcebergFileFormat::Puffin
                    }
                },
                file_content: match file.file_content {
                    super::changes::DeleteVisibilityDeleteFileContent::Position => {
                        super::delete_file::IcebergFileContent::PositionDeletes
                    }
                    super::changes::DeleteVisibilityDeleteFileContent::Equality => {
                        super::delete_file::IcebergFileContent::EqualityDeletes
                    }
                },
            })
            .collect::<Vec<_>>();
        load_visibility_for_data_file(
            &mut out,
            &data_file.path,
            data_file.size,
            &delete_specs,
            object_store_config,
        )?;
    }
    Ok(out)
}

fn load_delete_visibility_from_data_files(
    data_files: Vec<registry::DataFileWithStats>,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    let mut out = ExistingDeleteVisibilityByDataFile::new();
    for data_file in data_files {
        let delete_specs = data_file
            .delete_files
            .iter()
            .map(|file| DeleteFileInput {
                path: &file.path,
                length: file.length,
                content_offset: file.content_offset,
                content_size_in_bytes: file.content_size_in_bytes,
                file_format: match file.file_format {
                    super::scan_model::IcebergDeleteFileFormat::Parquet => {
                        super::delete_file::IcebergFileFormat::Parquet
                    }
                    super::scan_model::IcebergDeleteFileFormat::Puffin => {
                        super::delete_file::IcebergFileFormat::Puffin
                    }
                },
                file_content: match file.file_content {
                    super::scan_model::IcebergDeleteFileContent::Position => {
                        super::delete_file::IcebergFileContent::PositionDeletes
                    }
                    super::scan_model::IcebergDeleteFileContent::Equality => {
                        super::delete_file::IcebergFileContent::EqualityDeletes
                    }
                },
            })
            .collect::<Vec<_>>();
        load_visibility_for_data_file(
            &mut out,
            &data_file.path,
            data_file.size,
            &delete_specs,
            object_store_config,
        )?;
    }
    Ok(out)
}

struct DeleteFileInput<'a> {
    path: &'a str,
    length: Option<i64>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    file_format: super::delete_file::IcebergFileFormat,
    file_content: super::delete_file::IcebergFileContent,
}

fn load_visibility_for_data_file(
    out: &mut ExistingDeleteVisibilityByDataFile,
    data_file_path: &str,
    data_file_size: i64,
    delete_files: &[DeleteFileInput<'_>],
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<(), String> {
    if delete_files.is_empty() {
        return Ok(());
    }
    let data_file_len = u64::try_from(data_file_size)
        .map_err(|_| format!("iceberg data file size is negative: {data_file_path}"))?;
    let mut loader_ranges = Vec::with_capacity(1 + delete_files.len());
    loader_ranges.push(crate::connector::file_execution::FileScanRange {
        path: data_file_path.to_string(),
        file_len: data_file_len,
        offset: 0,
        length: data_file_len,
        scan_range_id: -1,
        external_datacache: None,
    });
    for delete_file in delete_files {
        let delete_len_i64 = delete_file.length.unwrap_or(0);
        let delete_len = u64::try_from(delete_len_i64)
            .map_err(|_| format!("iceberg delete file size is negative: {}", delete_file.path))?;
        loader_ranges.push(crate::connector::file_execution::FileScanRange {
            path: delete_file.path.to_string(),
            file_len: delete_len,
            offset: 0,
            length: delete_len,
            scan_range_id: -1,
            external_datacache: None,
        });
    }
    let ctx = crate::connector::file_execution::FileScanContext::build(
        loader_ranges,
        None,
        object_store_config,
    )?;
    let normalized_delete_specs = ctx
        .ranges
        .iter()
        .skip(1)
        .zip(delete_files)
        .map(|(resolved, original)| {
            Ok(super::delete_file::IcebergDeleteFileSpec {
                path: resolved.path.clone(),
                file_format: original.file_format,
                file_content: original.file_content,
                length: original
                    .length
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| {
                        format!("iceberg delete file size is negative: {}", original.path)
                    })?,
                content_offset: original.content_offset,
                content_size_in_bytes: original.content_size_in_bytes,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let deleted_positions = super::position_delete::load_position_deletes(
        &normalized_delete_specs,
        data_file_path,
        &ctx.access,
    )?;
    let equality_deletes =
        super::equality_delete::load_equality_delete_sets(&normalized_delete_specs, &ctx.access)?;
    if deleted_positions.is_empty() && equality_deletes.is_empty() {
        return Ok(());
    }
    let visibility = ExistingDeleteVisibility {
        deleted_positions,
        equality_deletes,
    };
    if let Some(resolved_data_file) = ctx.ranges.first()
        && resolved_data_file.path != data_file_path
    {
        out.insert(resolved_data_file.path.clone(), visibility.clone());
    }
    out.insert(data_file_path.to_string(), visibility);
    Ok(())
}

pub(crate) fn data_file_row_is_visible(
    batch: &RecordBatch,
    row: usize,
    file_path: &str,
    row_position: i64,
    existing_deletes_by_file: &ExistingDeleteVisibilityByDataFile,
) -> Result<bool, String> {
    let visibility = existing_deletes_by_file.get(file_path);
    if visibility
        .map(|state| state.deleted_positions.contains(row_position as u64))
        .unwrap_or(false)
    {
        return Ok(false);
    }
    let equality_deletes = visibility
        .map(|state| state.equality_deletes.as_slice())
        .unwrap_or(&[]);
    if super::equality_delete::equality_delete_row_is_deleted(batch, row, equality_deletes)? {
        return Ok(false);
    }
    Ok(true)
}
