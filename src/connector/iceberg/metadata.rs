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

use arrow::array::{
    ArrayRef, BinaryBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListArray,
    ListBuilder, MapBuilder, MapFieldNames, RecordBatch, RecordBatchOptions, StringArray,
    StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use base64::Engine;
use serde::Deserialize;

use super::{ensure_embedded_jvm_enabled, jvm::scan_metadata};
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp};
use crate::runtime::profile::RuntimeProfile;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergMetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
    Snapshots,
    History,
    Refs,
    Partitions,
}

impl IcebergMetadataTableType {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_uppercase().as_str() {
            "FILES" => Ok(Self::Files),
            "MANIFESTS" => Ok(Self::Manifests),
            "LOGICAL_ICEBERG_METADATA" => Ok(Self::LogicalIcebergMetadata),
            "SNAPSHOTS" => Ok(Self::Snapshots),
            "HISTORY" => Ok(Self::History),
            "REFS" => Ok(Self::Refs),
            "PARTITIONS" => Ok(Self::Partitions),
            other => Err(format!("unsupported iceberg metadata table type: {other}")),
        }
    }

    fn as_jvm_scanner_type(&self) -> &'static str {
        match self {
            Self::Files => "FILES",
            Self::Manifests => "MANIFESTS",
            Self::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
            Self::Snapshots => "SNAPSHOTS",
            Self::History => "HISTORY",
            Self::Refs => "REFS",
            Self::Partitions => "PARTITIONS",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataOutputColumn {
    pub name: String,
    pub slot_id: SlotId,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanRange {
    pub path: String,
    pub serialized_split: String,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanConfig {
    pub metadata_table_type: IcebergMetadataTableType,
    pub serialized_table: String,
    pub serialized_predicate: String,
    pub load_column_stats: bool,
    pub ranges: Vec<IcebergMetadataScanRange>,
    pub batch_size: usize,
    pub output_columns: Vec<IcebergMetadataOutputColumn>,
    pub profile_label: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanOp {
    cfg: IcebergMetadataScanConfig,
    output_schema: SchemaRef,
    output_chunk_schema: Arc<ChunkSchema>,
}

impl IcebergMetadataScanOp {
    pub fn new(cfg: IcebergMetadataScanConfig) -> Result<Self, String> {
        ensure_embedded_jvm_enabled("Iceberg metadata scan")?;
        let fields = cfg
            .output_columns
            .iter()
            .map(|col| {
                Arc::new(Field::new(
                    &col.name,
                    normalize_metadata_output_type(&col.data_type),
                    col.nullable,
                ))
            })
            .collect::<Vec<_>>();
        let chunk_schema = Arc::new(ChunkSchema::try_new(
            cfg.output_columns
                .iter()
                .zip(fields.iter())
                .map(|(col, field)| {
                    ChunkSlotSchema::new_with_field(col.slot_id, field.as_ref().clone(), None, None)
                })
                .collect(),
        )?);
        Ok(Self {
            output_schema: Arc::new(Schema::new(fields)),
            output_chunk_schema: chunk_schema,
            cfg,
        })
    }
}

fn normalize_metadata_output_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(item) => DataType::List(Arc::new(normalize_metadata_output_field(item))),
        DataType::LargeList(item) => {
            DataType::LargeList(Arc::new(normalize_metadata_output_field(item)))
        }
        DataType::FixedSizeList(item, len) => {
            DataType::FixedSizeList(Arc::new(normalize_metadata_output_field(item)), *len)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_metadata_output_field(field.as_ref()))
                .collect(),
        ),
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return data_type.clone();
            };
            if fields.len() != 2 {
                return data_type.clone();
            }
            let mut normalized_fields = fields.iter().cloned().collect::<Vec<_>>();
            normalized_fields[0] = Arc::new(
                normalized_fields[0]
                    .as_ref()
                    .clone()
                    .with_data_type(normalize_metadata_output_type(
                        normalized_fields[0].data_type(),
                    ))
                    .with_nullable(false),
            );
            normalized_fields[1] = Arc::new(normalized_fields[1].as_ref().clone().with_data_type(
                normalize_metadata_output_type(normalized_fields[1].data_type()),
            ));
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(normalized_fields.into()))
                        .with_nullable(false),
                ),
                *ordered,
            )
        }
        _ => data_type.clone(),
    }
}

fn normalize_metadata_output_field(field: &Field) -> Field {
    field
        .clone()
        .with_data_type(normalize_metadata_output_type(field.data_type()))
}

impl ScanOp for IcebergMetadataScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        ensure_embedded_jvm_enabled("Iceberg metadata scan")?;
        let ScanMorsel::IcebergMetadata { index } = morsel else {
            return Err("iceberg metadata scan received unexpected morsel".to_string());
        };
        // Indices come from build_morsels (0..ranges.len()), so .get(index) is
        // always Some. Table-level scans (snapshots/history/refs/partitions)
        // borrow `range` only for the optional profile annotation below.
        let range = self
            .cfg
            .ranges
            .get(index)
            .ok_or_else(|| format!("iceberg metadata range index out of bounds: {index}"))?;
        let chunks = match self.cfg.metadata_table_type {
            IcebergMetadataTableType::Files => {
                let rows = load_file_rows(&self.cfg, range, false)?;
                build_file_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                    false,
                    self.cfg.load_column_stats,
                )?
            }
            IcebergMetadataTableType::LogicalIcebergMetadata => {
                let rows = load_file_rows(&self.cfg, range, true)?;
                build_file_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                    true,
                    self.cfg.load_column_stats,
                )?
            }
            IcebergMetadataTableType::Manifests => {
                let rows = load_manifest_rows(&self.cfg, range)?;
                build_manifest_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Snapshots => {
                let rows = load_snapshot_rows(&self.cfg)?;
                build_snapshot_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::History => {
                let rows = load_history_rows(&self.cfg)?;
                build_history_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Refs => {
                let rows = load_ref_rows(&self.cfg)?;
                build_ref_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Partitions => {
                return Err(format!(
                    "iceberg metadata table {:?} is not implemented yet",
                    self.cfg.metadata_table_type
                ));
            }
        };

        if let Some(profile) = profile.as_ref() {
            profile.add_info_string(
                "IcebergMetadataTableType",
                format!("{:?}", self.cfg.metadata_table_type),
            );
            profile.add_info_string("RangeIndex", index.to_string());
            if !range.path.is_empty() {
                profile.add_info_string("RangePath", range.path.clone());
            }
        }

        Ok(Box::new(chunks.into_iter().map(Ok)))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let morsels = (0..self.cfg.ranges.len())
            .map(|index| ScanMorsel::IcebergMetadata { index })
            .collect();
        Ok(ScanMorsels::new(morsels, false))
    }

    fn profile_name(&self) -> Option<String> {
        let prefix = "ICEBERG_METADATA_SCAN";
        if let Some(label) = self.cfg.profile_label.as_deref() {
            return Some(format!("{prefix} ({label})"));
        }
        Some(prefix.to_string())
    }
}

#[derive(Clone, Debug)]
struct FileMetadataRow {
    content: i32,
    file_path: String,
    file_format: String,
    spec_id: i32,
    partition_data: Option<Vec<u8>>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<Vec<(i32, i64)>>,
    value_counts: Option<Vec<(i32, i64)>>,
    null_value_counts: Option<Vec<(i32, i64)>>,
    nan_value_counts: Option<Vec<(i32, i64)>>,
    lower_bounds: Option<Vec<(i32, String)>>,
    upper_bounds: Option<Vec<(i32, String)>>,
    split_offsets: Option<Vec<i64>>,
    sort_id: Option<i32>,
    equality_ids: Option<Vec<i32>>,
    file_sequence_number: Option<i64>,
    data_sequence_number: Option<i64>,
    column_stats: Option<Vec<u8>>,
    key_metadata: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct ManifestPartitionSummary {
    contains_null: Option<String>,
    contains_nan: Option<String>,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
}

#[derive(Clone, Debug)]
struct ManifestMetadataRow {
    path: String,
    length: i64,
    partition_spec_id: i32,
    added_snapshot_id: i64,
    added_data_files_count: i32,
    added_rows_count: i64,
    existing_data_files_count: i32,
    existing_rows_count: i64,
    deleted_data_files_count: i32,
    deleted_rows_count: i64,
    partitions: Option<Vec<ManifestPartitionSummary>>,
}

#[derive(Deserialize)]
struct RawLongEntry {
    key: i32,
    value: i64,
}

#[derive(Deserialize)]
struct RawStringEntry {
    key: i32,
    value: String,
}

#[derive(Deserialize)]
struct RawFileMetadataRow {
    content: i32,
    file_path: String,
    file_format: String,
    spec_id: i32,
    partition_data: Option<String>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<Vec<RawLongEntry>>,
    value_counts: Option<Vec<RawLongEntry>>,
    null_value_counts: Option<Vec<RawLongEntry>>,
    nan_value_counts: Option<Vec<RawLongEntry>>,
    lower_bounds: Option<Vec<RawStringEntry>>,
    upper_bounds: Option<Vec<RawStringEntry>>,
    split_offsets: Option<Vec<i64>>,
    sort_id: Option<i32>,
    equality_ids: Option<Vec<i32>>,
    file_sequence_number: Option<i64>,
    data_sequence_number: Option<i64>,
    column_stats: Option<String>,
    key_metadata: Option<String>,
}

#[derive(Deserialize)]
struct RawManifestPartitionSummary {
    contains_null: Option<String>,
    contains_nan: Option<String>,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
}

#[derive(Deserialize)]
struct RawManifestMetadataRow {
    path: String,
    length: i64,
    partition_spec_id: i32,
    added_snapshot_id: Option<i64>,
    added_data_files_count: Option<i32>,
    added_rows_count: Option<i64>,
    existing_data_files_count: Option<i32>,
    existing_rows_count: Option<i64>,
    deleted_data_files_count: Option<i32>,
    deleted_rows_count: Option<i64>,
    partitions: Option<Vec<RawManifestPartitionSummary>>,
}

fn load_file_rows(
    cfg: &IcebergMetadataScanConfig,
    range: &IcebergMetadataScanRange,
    logical_metadata: bool,
) -> Result<Vec<FileMetadataRow>, String> {
    let scanner_type = if logical_metadata {
        IcebergMetadataTableType::LogicalIcebergMetadata.as_jvm_scanner_type()
    } else {
        IcebergMetadataTableType::Files.as_jvm_scanner_type()
    };
    let payload = scan_metadata(
        scanner_type,
        &cfg.serialized_table,
        &range.serialized_split,
        &cfg.serialized_predicate,
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawFileMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg file metadata rows failed: {e}"))?;
    rows.into_iter().map(FileMetadataRow::try_from).collect()
}

fn load_manifest_rows(
    cfg: &IcebergMetadataScanConfig,
    range: &IcebergMetadataScanRange,
) -> Result<Vec<ManifestMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::Manifests.as_jvm_scanner_type(),
        &cfg.serialized_table,
        &range.serialized_split,
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawManifestMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg manifest metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(ManifestMetadataRow::from).collect())
}

impl TryFrom<RawFileMetadataRow> for FileMetadataRow {
    type Error = String;

    fn try_from(raw: RawFileMetadataRow) -> Result<Self, Self::Error> {
        Ok(Self {
            content: raw.content,
            file_path: raw.file_path,
            file_format: raw.file_format,
            spec_id: raw.spec_id,
            partition_data: decode_optional_bytes(raw.partition_data)?,
            record_count: raw.record_count,
            file_size_in_bytes: raw.file_size_in_bytes,
            column_sizes: convert_long_entries(raw.column_sizes),
            value_counts: convert_long_entries(raw.value_counts),
            null_value_counts: convert_long_entries(raw.null_value_counts),
            nan_value_counts: convert_long_entries(raw.nan_value_counts),
            lower_bounds: convert_string_entries(raw.lower_bounds),
            upper_bounds: convert_string_entries(raw.upper_bounds),
            split_offsets: raw.split_offsets,
            sort_id: raw.sort_id,
            equality_ids: raw.equality_ids,
            file_sequence_number: raw.file_sequence_number,
            data_sequence_number: raw.data_sequence_number,
            column_stats: decode_optional_bytes(raw.column_stats)?,
            key_metadata: decode_optional_bytes(raw.key_metadata)?,
        })
    }
}

impl From<RawManifestMetadataRow> for ManifestMetadataRow {
    fn from(raw: RawManifestMetadataRow) -> Self {
        Self {
            path: raw.path,
            length: raw.length,
            partition_spec_id: raw.partition_spec_id,
            added_snapshot_id: raw.added_snapshot_id.unwrap_or_default(),
            added_data_files_count: raw.added_data_files_count.unwrap_or_default(),
            added_rows_count: raw.added_rows_count.unwrap_or_default(),
            existing_data_files_count: raw.existing_data_files_count.unwrap_or_default(),
            existing_rows_count: raw.existing_rows_count.unwrap_or_default(),
            deleted_data_files_count: raw.deleted_data_files_count.unwrap_or_default(),
            deleted_rows_count: raw.deleted_rows_count.unwrap_or_default(),
            partitions: raw.partitions.map(|parts| {
                parts
                    .into_iter()
                    .map(|part| ManifestPartitionSummary {
                        contains_null: part.contains_null,
                        contains_nan: part.contains_nan,
                        lower_bound: part.lower_bound,
                        upper_bound: part.upper_bound,
                    })
                    .collect()
            }),
        }
    }
}

fn decode_optional_bytes(value: Option<String>) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map(Some)
        .map_err(|e| format!("decode JVM iceberg binary column failed: {e}"))
}

fn convert_long_entries(value: Option<Vec<RawLongEntry>>) -> Option<Vec<(i32, i64)>> {
    value.map(|entries| {
        entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect()
    })
}

fn convert_string_entries(value: Option<Vec<RawStringEntry>>) -> Option<Vec<(i32, String)>> {
    value.map(|entries| {
        entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect()
    })
}

fn build_file_chunks(
    rows: &[FileMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
    logical_metadata: bool,
    load_column_stats: bool,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let arrays = output_columns
        .iter()
        .map(|column| build_file_array(column, rows, logical_metadata, load_column_stats))
        .collect::<Result<Vec<_>, _>>()?;

    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

fn build_manifest_chunks(
    rows: &[ManifestMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let arrays = output_columns
        .iter()
        .map(|column| build_manifest_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;

    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

fn build_chunks(
    schema: &SchemaRef,
    chunk_schema: &Arc<ChunkSchema>,
    arrays: Vec<ArrayRef>,
    row_count: usize,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if row_count == 0 {
        return Ok(Vec::new());
    }

    let batch = if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(Arc::clone(schema), vec![], &options)
            .map_err(|e| format!("failed to build iceberg metadata empty batch: {}", e))?
    } else {
        RecordBatch::try_new(Arc::clone(schema), arrays)
            .map_err(|e| format!("failed to build iceberg metadata batch: {}", e))?
    };

    let batch_size = batch_size.max(1);
    if row_count <= batch_size {
        return Ok(vec![Chunk::new_with_chunk_schema(
            batch,
            Arc::clone(chunk_schema),
        )]);
    }

    let mut chunks = Vec::new();
    let mut offset = 0usize;
    while offset < row_count {
        let len = (row_count - offset).min(batch_size);
        chunks.push(Chunk::new_with_chunk_schema(
            batch.slice(offset, len),
            Arc::clone(chunk_schema),
        ));
        offset += len;
    }
    Ok(chunks)
}

fn build_file_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[FileMetadataRow],
    logical_metadata: bool,
    load_column_stats: bool,
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "content" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.content).collect::<Vec<_>>(),
        ))),
        "file_path" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.file_path.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "file_format" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.file_format.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "spec_id" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.spec_id).collect::<Vec<_>>(),
        ))),
        "partition_data" if logical_metadata => build_binary_array(
            rows.iter()
                .map(|row| row.partition_data.as_deref())
                .collect::<Vec<_>>(),
        ),
        "record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|row| row.record_count).collect::<Vec<_>>(),
        ))),
        "file_size_in_bytes" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.file_size_in_bytes)
                .collect::<Vec<_>>(),
        ))),
        "column_sizes" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.column_sizes.as_ref()))
        }
        "value_counts" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.value_counts.as_ref()))
        }
        "null_value_counts" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.null_value_counts.as_ref()))
        }
        "nan_value_counts" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.nan_value_counts.as_ref()))
        }
        "lower_bounds" if !logical_metadata => {
            build_i32_utf8_map_array(rows.iter().map(|row| row.lower_bounds.as_ref()))
        }
        "upper_bounds" if !logical_metadata => {
            build_i32_utf8_map_array(rows.iter().map(|row| row.upper_bounds.as_ref()))
        }
        "split_offsets" => build_i64_list_array(rows.iter().map(|row| row.split_offsets.as_ref())),
        "sort_id" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.sort_id).collect::<Vec<_>>(),
        ))),
        "equality_ids" => build_i32_list_array(rows.iter().map(|row| row.equality_ids.as_ref())),
        "file_sequence_number" if logical_metadata => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.file_sequence_number)
                .collect::<Vec<_>>(),
        ))),
        "data_sequence_number" if logical_metadata => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.data_sequence_number)
                .collect::<Vec<_>>(),
        ))),
        "column_stats" if logical_metadata => {
            let values = rows
                .iter()
                .map(|row| {
                    if load_column_stats {
                        row.column_stats.as_deref()
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            build_binary_array(values)
        }
        "key_metadata" => build_binary_array(
            rows.iter()
                .map(|row| row.key_metadata.as_deref())
                .collect::<Vec<_>>(),
        ),
        other => Err(format!(
            "unsupported iceberg {} metadata column: {}",
            if logical_metadata { "logical" } else { "files" },
            other
        )),
    }
}

fn build_manifest_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[ManifestMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "path" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.path.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "length" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|row| row.length).collect::<Vec<_>>(),
        ))),
        "partition_spec_id" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.partition_spec_id)
                .collect::<Vec<_>>(),
        ))),
        "added_snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.added_snapshot_id)
                .collect::<Vec<_>>(),
        ))),
        "added_data_files_count" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.added_data_files_count)
                .collect::<Vec<_>>(),
        ))),
        "added_rows_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.added_rows_count)
                .collect::<Vec<_>>(),
        ))),
        "existing_data_files_count" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.existing_data_files_count)
                .collect::<Vec<_>>(),
        ))),
        "existing_rows_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.existing_rows_count)
                .collect::<Vec<_>>(),
        ))),
        "deleted_data_files_count" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.deleted_data_files_count)
                .collect::<Vec<_>>(),
        ))),
        "deleted_rows_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.deleted_rows_count)
                .collect::<Vec<_>>(),
        ))),
        "partitions" => build_manifest_partitions_array(column, rows),
        other => Err(format!(
            "unsupported iceberg manifests metadata column: {}",
            other
        )),
    }
}

fn build_binary_array(values: Vec<Option<&[u8]>>) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for value in values {
        match value {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_i32_i64_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(i32, i64)>>>,
{
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        Int32Builder::new(),
        Int64Builder::new(),
    );
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(*key);
                    builder.values().append_value(*value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append map row failed: {}", e))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null map row failed: {}", e))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_i32_utf8_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(i32, String)>>>,
{
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        Int32Builder::new(),
        StringBuilder::new(),
    );
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(*key);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append map row failed: {}", e))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null map row failed: {}", e))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn iceberg_map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

fn build_i64_list_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<i64>>>,
{
    let mut builder = ListBuilder::new(Int64Builder::new());
    for row in rows {
        match row {
            Some(values) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            None => {
                builder.append(false);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_i32_list_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<i32>>>,
{
    let mut builder = ListBuilder::new(Int32Builder::new());
    for row in rows {
        match row {
            Some(values) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            None => {
                builder.append(false);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_manifest_partitions_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[ManifestMetadataRow],
) -> Result<ArrayRef, String> {
    let DataType::List(item_field) = &column.data_type else {
        return Err(format!(
            "iceberg manifests partitions expects List type, got {:?}",
            column.data_type
        ));
    };
    let DataType::Struct(struct_fields) = item_field.data_type() else {
        return Err(format!(
            "iceberg manifests partitions list item expects Struct type, got {:?}",
            item_field.data_type()
        ));
    };

    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0_i32);
    let mut list_nulls = NullBufferBuilder::new(rows.len());

    let mut contains_null = StringBuilder::new();
    let mut contains_nan = StringBuilder::new();
    let mut lower_bound = StringBuilder::new();
    let mut upper_bound = StringBuilder::new();

    let mut total = 0_i32;
    for row in rows {
        match row.partitions.as_ref() {
            Some(partitions) => {
                list_nulls.append(true);
                for part in partitions {
                    match part.contains_null.as_deref() {
                        Some(value) => contains_null.append_value(value),
                        None => contains_null.append_null(),
                    }
                    match part.contains_nan.as_deref() {
                        Some(value) => contains_nan.append_value(value),
                        None => contains_nan.append_null(),
                    }
                    match part.lower_bound.as_deref() {
                        Some(value) => lower_bound.append_value(value),
                        None => lower_bound.append_null(),
                    }
                    match part.upper_bound.as_deref() {
                        Some(value) => upper_bound.append_value(value),
                        None => upper_bound.append_null(),
                    }
                }
                total += i32::try_from(partitions.len())
                    .map_err(|_| "manifest partitions row count overflow".to_string())?;
            }
            None => list_nulls.append(false),
        }
        offsets.push(total);
    }

    let struct_array = StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(contains_null.finish()),
            Arc::new(contains_nan.finish()),
            Arc::new(lower_bound.finish()),
            Arc::new(upper_bound.finish()),
        ],
        None,
    );
    let list = ListArray::new(
        Arc::clone(item_field),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        list_nulls.finish(),
    );
    Ok(Arc::new(list))
}

#[derive(Clone, Debug)]
struct SnapshotMetadataRow {
    committed_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    operation: Option<String>,
    manifest_list: String,
    summary: Option<Vec<(String, String)>>,
}

#[derive(Deserialize)]
struct RawStringStringEntry {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct RawSnapshotMetadataRow {
    committed_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    operation: Option<String>,
    manifest_list: String,
    summary: Option<Vec<RawStringStringEntry>>,
}

impl From<RawSnapshotMetadataRow> for SnapshotMetadataRow {
    fn from(raw: RawSnapshotMetadataRow) -> Self {
        Self {
            committed_at_micros: raw.committed_at_micros,
            snapshot_id: raw.snapshot_id,
            parent_id: raw.parent_id,
            operation: raw.operation,
            manifest_list: raw.manifest_list,
            summary: raw.summary.map(|entries| {
                entries.into_iter().map(|e| (e.key, e.value)).collect()
            }),
        }
    }
}

fn load_snapshot_rows(
    cfg: &IcebergMetadataScanConfig,
) -> Result<Vec<SnapshotMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::Snapshots.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawSnapshotMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg snapshots metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(SnapshotMetadataRow::from).collect())
}

fn build_snapshot_chunks(
    rows: &[SnapshotMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_snapshot_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_snapshot_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[SnapshotMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "committed_at" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.committed_at_micros).collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "operation" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| r.operation.as_deref()).collect::<Vec<_>>(),
        ))),
        "manifest_list" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.manifest_list.as_str())).collect::<Vec<_>>(),
        ))),
        "summary" => build_string_string_map_array(rows.iter().map(|r| r.summary.as_ref())),
        other => Err(format!("unsupported iceberg snapshots metadata column: {}", other)),
    }
}

fn build_string_string_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(String, String)>>>,
{
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(key);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append map row failed: {}", e))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null map row failed: {}", e))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[derive(Clone, Debug)]
struct HistoryMetadataRow {
    made_current_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    is_current_ancestor: bool,
}

#[derive(Deserialize)]
struct RawHistoryMetadataRow {
    made_current_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    is_current_ancestor: bool,
}

impl From<RawHistoryMetadataRow> for HistoryMetadataRow {
    fn from(raw: RawHistoryMetadataRow) -> Self {
        Self {
            made_current_at_micros: raw.made_current_at_micros,
            snapshot_id: raw.snapshot_id,
            parent_id: raw.parent_id,
            is_current_ancestor: raw.is_current_ancestor,
        }
    }
}

fn load_history_rows(
    cfg: &IcebergMetadataScanConfig,
) -> Result<Vec<HistoryMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::History.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawHistoryMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg history metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(HistoryMetadataRow::from).collect())
}

fn build_history_chunks(
    rows: &[HistoryMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_history_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_history_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[HistoryMetadataRow],
) -> Result<ArrayRef, String> {
    use arrow::array::BooleanArray;
    match column.name.as_str() {
        "made_current_at" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.made_current_at_micros).collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "is_current_ancestor" => Ok(Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.is_current_ancestor).collect::<Vec<_>>(),
        ))),
        other => Err(format!("unsupported iceberg history metadata column: {}", other)),
    }
}

#[derive(Clone, Debug)]
struct RefMetadataRow {
    name: String,
    type_: String,
    snapshot_id: i64,
    max_reference_age_in_ms: Option<i64>,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_in_ms: Option<i64>,
}

#[derive(Deserialize)]
struct RawRefMetadataRow {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    snapshot_id: i64,
    max_reference_age_in_ms: Option<i64>,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_in_ms: Option<i64>,
}

impl From<RawRefMetadataRow> for RefMetadataRow {
    fn from(raw: RawRefMetadataRow) -> Self {
        Self {
            name: raw.name,
            type_: raw.type_,
            snapshot_id: raw.snapshot_id,
            max_reference_age_in_ms: raw.max_reference_age_in_ms,
            min_snapshots_to_keep: raw.min_snapshots_to_keep,
            max_snapshot_age_in_ms: raw.max_snapshot_age_in_ms,
        }
    }
}

fn load_ref_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<RefMetadataRow>, String> {
    let payload = scan_metadata(
        IcebergMetadataTableType::Refs.as_jvm_scanner_type(),
        &cfg.serialized_table,
        "",
        "",
        cfg.load_column_stats,
    )?;
    let rows: Vec<RawRefMetadataRow> = serde_json::from_slice(&payload)
        .map_err(|e| format!("parse JVM iceberg refs metadata rows failed: {e}"))?;
    Ok(rows.into_iter().map(RefMetadataRow::from).collect())
}

fn build_ref_chunks(
    rows: &[RefMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_ref_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, output_chunk_schema, arrays, rows.len(), batch_size)
}

fn build_ref_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[RefMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "name" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.name.as_str())).collect::<Vec<_>>(),
        ))),
        "type" => Ok(Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.type_.as_str())).collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "max_reference_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.max_reference_age_in_ms).collect::<Vec<_>>(),
        ))),
        "min_snapshots_to_keep" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|r| r.min_snapshots_to_keep).collect::<Vec<_>>(),
        ))),
        "max_snapshot_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.max_snapshot_age_in_ms).collect::<Vec<_>>(),
        ))),
        other => Err(format!("unsupported iceberg refs metadata column: {}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IcebergMetadataScanConfig, IcebergMetadataScanOp, IcebergMetadataTableType,
        build_i32_i64_map_array, build_i32_utf8_map_array, normalize_metadata_output_type,
    };
    use crate::common::ids::SlotId;
    use arrow::array::{Array, MapArray};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_build_i32_i64_map_array_uses_iceberg_field_names() {
        let rows = [Some(vec![(1, 10_i64)]), None];
        let array = build_i32_i64_map_array(rows.iter().map(|row| row.as_ref())).unwrap();
        let map = array.as_any().downcast_ref::<MapArray>().unwrap();
        let (key_field, value_field) = map.entries_fields();
        assert_eq!(key_field.name(), "key");
        assert_eq!(value_field.name(), "value");
    }

    #[test]
    fn test_build_i32_utf8_map_array_uses_iceberg_field_names() {
        let rows = [Some(vec![(2, "abc".to_string())]), None];
        let array = build_i32_utf8_map_array(rows.iter().map(|row| row.as_ref())).unwrap();
        let map = array.as_any().downcast_ref::<MapArray>().unwrap();
        let (key_field, value_field) = map.entries_fields();
        assert_eq!(key_field.name(), "key");
        assert_eq!(value_field.name(), "value");
    }

    #[test]
    fn test_normalize_metadata_output_type_makes_map_keys_non_nullable() {
        let ty = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, true)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let normalized = normalize_metadata_output_type(&ty);
        let DataType::Map(entries, _) = normalized else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries struct");
        };
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn test_metadata_scan_requires_embedded_jvm_config() {
        let err = IcebergMetadataScanOp::new(IcebergMetadataScanConfig {
            metadata_table_type: IcebergMetadataTableType::Files,
            serialized_table: String::new(),
            serialized_predicate: String::new(),
            load_column_stats: false,
            ranges: Vec::new(),
            batch_size: 1,
            output_columns: vec![super::IcebergMetadataOutputColumn {
                name: "content".to_string(),
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
                nullable: false,
            }],
            profile_label: None,
        })
        .expect_err("embedded JVM gate should reject metadata scans by default");
        if cfg!(feature = "embedded-jvm") {
            assert!(err.contains("[iceberg].enable_embedded_jvm"));
        } else {
            assert!(err.contains("built without the `embedded-jvm` feature"));
        }
    }

    #[test]
    fn test_parse_snapshots_history_refs_partitions() {
        assert_eq!(
            IcebergMetadataTableType::parse("SNAPSHOTS").unwrap(),
            IcebergMetadataTableType::Snapshots
        );
        assert_eq!(
            IcebergMetadataTableType::parse("history").unwrap(),
            IcebergMetadataTableType::History
        );
        assert_eq!(
            IcebergMetadataTableType::parse("Refs").unwrap(),
            IcebergMetadataTableType::Refs
        );
        assert_eq!(
            IcebergMetadataTableType::parse("partitions").unwrap(),
            IcebergMetadataTableType::Partitions
        );
    }

    #[test]
    fn test_build_snapshot_arrays_basic_shapes() {
        use super::SnapshotMetadataRow;
        let rows = vec![SnapshotMetadataRow {
            committed_at_micros: 1_700_000_000_000_000,
            snapshot_id: 42,
            parent_id: Some(41),
            operation: Some("append".into()),
            manifest_list: "s3://bucket/manifest-list.avro".into(),
            summary: Some(vec![("added-records".into(), "10".into())]),
        }];
        let columns = [
            ("snapshot_id", DataType::Int64),
            ("operation", DataType::Utf8),
        ];
        for (name, ty) in &columns {
            let col = super::IcebergMetadataOutputColumn {
                name: (*name).into(),
                slot_id: SlotId::new(1),
                data_type: ty.clone(),
                nullable: true,
            };
            let arr = super::build_snapshot_array(&col, &rows).unwrap();
            assert_eq!(arr.len(), 1);
        }
    }

    #[test]
    fn test_build_snapshot_summary_map_uses_iceberg_field_names() {
        use super::SnapshotMetadataRow;
        let rows = vec![SnapshotMetadataRow {
            committed_at_micros: 0,
            snapshot_id: 1,
            parent_id: None,
            operation: None,
            manifest_list: "x".into(),
            summary: Some(vec![("added-records".into(), "10".into())]),
        }];
        // The Map type passed in matches what FE will declare for the summary column.
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let col = super::IcebergMetadataOutputColumn {
            name: "summary".into(),
            slot_id: SlotId::new(1),
            data_type: map_type,
            nullable: true,
        };
        let arr = super::build_snapshot_array(&col, &rows).unwrap();
        let map = arr.as_any().downcast_ref::<MapArray>().expect("MapArray");
        assert_eq!(map.len(), 1);
        let (key_field, value_field) = map.entries_fields();
        assert_eq!(key_field.name(), "key");
        assert_eq!(value_field.name(), "value");
    }

    #[test]
    fn test_build_history_arrays_basic_shapes() {
        use super::HistoryMetadataRow;
        use arrow::array::BooleanArray;
        let rows = vec![
            HistoryMetadataRow {
                made_current_at_micros: 1_700_000_000_000_000,
                snapshot_id: 1,
                parent_id: None,
                is_current_ancestor: true,
            },
            HistoryMetadataRow {
                made_current_at_micros: 1_700_000_000_000_001,
                snapshot_id: 2,
                parent_id: Some(1),
                is_current_ancestor: false,
            },
        ];
        let bool_col = super::IcebergMetadataOutputColumn {
            name: "is_current_ancestor".into(),
            slot_id: SlotId::new(1),
            data_type: DataType::Boolean,
            nullable: false,
        };
        let arr = super::build_history_array(&bool_col, &rows).unwrap();
        let bools = arr.as_any().downcast_ref::<BooleanArray>().expect("BooleanArray");
        assert_eq!(bools.len(), 2);
        assert!(bools.value(0));
        assert!(!bools.value(1));

        let parent_col = super::IcebergMetadataOutputColumn {
            name: "parent_id".into(),
            slot_id: SlotId::new(2),
            data_type: DataType::Int64,
            nullable: true,
        };
        let arr = super::build_history_array(&parent_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr.is_null(0));
        assert!(!arr.is_null(1));
    }

    #[test]
    fn test_build_ref_arrays_basic_shapes() {
        use super::RefMetadataRow;
        let rows = vec![
            RefMetadataRow {
                name: "main".into(),
                type_: "BRANCH".into(),
                snapshot_id: 1,
                max_reference_age_in_ms: None,
                min_snapshots_to_keep: None,
                max_snapshot_age_in_ms: None,
            },
            RefMetadataRow {
                name: "release-2026-q1".into(),
                type_: "TAG".into(),
                snapshot_id: 2,
                max_reference_age_in_ms: Some(86_400_000),
                min_snapshots_to_keep: Some(3),
                max_snapshot_age_in_ms: Some(31_536_000_000),
            },
        ];

        let type_col = super::IcebergMetadataOutputColumn {
            name: "type".into(),
            slot_id: SlotId::new(1),
            data_type: DataType::Utf8,
            nullable: false,
        };
        let arr = super::build_ref_array(&type_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        let strs = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("StringArray");
        assert_eq!(strs.value(0), "BRANCH");
        assert_eq!(strs.value(1), "TAG");

        let min_col = super::IcebergMetadataOutputColumn {
            name: "min_snapshots_to_keep".into(),
            slot_id: SlotId::new(2),
            data_type: DataType::Int32,
            nullable: true,
        };
        let arr = super::build_ref_array(&min_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr.is_null(0));
        assert!(!arr.is_null(1));
    }

    #[test]
    fn test_jvm_scanner_type_for_new_variants() {
        assert_eq!(
            IcebergMetadataTableType::Snapshots.as_jvm_scanner_type(),
            "SNAPSHOTS"
        );
        assert_eq!(
            IcebergMetadataTableType::History.as_jvm_scanner_type(),
            "HISTORY"
        );
        assert_eq!(
            IcebergMetadataTableType::Refs.as_jvm_scanner_type(),
            "REFS"
        );
        assert_eq!(
            IcebergMetadataTableType::Partitions.as_jvm_scanner_type(),
            "PARTITIONS"
        );
    }
}
