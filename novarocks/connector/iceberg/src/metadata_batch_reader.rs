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

//! Provider-owned Iceberg metadata-table row decoding and Arrow batch building.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, MapBuilder, MapFieldNames, RecordBatch, RecordBatchOptions,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext,
};

use crate::iceberg::spec::{SnapshotRetention, TableMetadata};

fn is_provider_private_ref(name: &str) -> bool {
    crate::commit::write_fence::is_fence_ref(name)
        || name == crate::commit::mv_publication_fence::MV_PUBLICATION_FENCE_REF
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum MetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
    Snapshots,
    History,
    Refs,
    Partitions,
}

#[derive(Clone, Debug)]
pub struct MetadataOutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Provider-owned schema for one Iceberg metadata-table alias.
///
/// The table metadata is required for the dynamic partition struct exposed by
/// `$files` and `$entries`; every other alias has a fixed provider schema.
pub fn metadata_table_output_columns(
    metadata_table_type: MetadataTableType,
    metadata: &TableMetadata,
) -> Result<Vec<MetadataOutputColumn>, String> {
    let column = |name: &str, data_type: DataType, nullable: bool| MetadataOutputColumn {
        name: name.to_string(),
        data_type,
        nullable,
    };
    let map_int_to = |value: DataType| {
        let entries = DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Field::new("value", value, true)),
            ]
            .into(),
        );
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
    };
    let list_of = |value: DataType| DataType::List(Arc::new(Field::new("item", value, true)));
    let files = || -> Result<Vec<MetadataOutputColumn>, String> {
        Ok(vec![
            column("content", DataType::Int32, false),
            column("file_path", DataType::Utf8, false),
            column("file_format", DataType::Utf8, false),
            column("spec_id", DataType::Int32, false),
            column("record_count", DataType::Int64, false),
            column("file_size_in_bytes", DataType::Int64, false),
            column("column_sizes", map_int_to(DataType::Int64), true),
            column("value_counts", map_int_to(DataType::Int64), true),
            column("null_value_counts", map_int_to(DataType::Int64), true),
            column("nan_value_counts", map_int_to(DataType::Int64), true),
            column("lower_bounds", map_int_to(DataType::Binary), true),
            column("upper_bounds", map_int_to(DataType::Binary), true),
            column("split_offsets", list_of(DataType::Int64), true),
            column("equality_ids", list_of(DataType::Int32), true),
            column("sort_order_id", DataType::Int32, true),
            column("key_metadata", DataType::Binary, true),
            column("first_row_id", DataType::Int64, true),
            column("partition", partition_struct_type(metadata)?, true),
        ])
    };
    match metadata_table_type {
        MetadataTableType::Snapshots => Ok(vec![
            column("committed_at", DataType::Int64, false),
            column("snapshot_id", DataType::Int64, false),
            column("parent_id", DataType::Int64, true),
            column("operation", DataType::Utf8, true),
            column("manifest_list", DataType::Utf8, false),
            column("summary", DataType::Utf8, false),
        ]),
        MetadataTableType::History => Ok(vec![
            column("made_current_at", DataType::Int64, false),
            column("snapshot_id", DataType::Int64, false),
            column("parent_id", DataType::Int64, true),
            column("is_current_ancestor", DataType::Boolean, false),
        ]),
        MetadataTableType::Refs => Ok(vec![
            column("name", DataType::Utf8, false),
            column("type", DataType::Utf8, false),
            column("snapshot_id", DataType::Int64, false),
            column("max_reference_age_in_ms", DataType::Int64, true),
            column("min_snapshots_to_keep", DataType::Int32, true),
            column("max_snapshot_age_in_ms", DataType::Int64, true),
        ]),
        MetadataTableType::Partitions => Ok(vec![
            column("record_count", DataType::Int64, false),
            column("file_count", DataType::Int64, false),
            column("position_delete_file_count", DataType::Int64, true),
            column("equality_delete_file_count", DataType::Int64, true),
        ]),
        MetadataTableType::Files => files(),
        MetadataTableType::Manifests => Ok(vec![
            column("content", DataType::Int32, false),
            column("path", DataType::Utf8, false),
            column("length", DataType::Int64, false),
            column("partition_spec_id", DataType::Int32, false),
            column("added_snapshot_id", DataType::Int64, true),
            column("added_data_files_count", DataType::Int32, false),
            column("existing_data_files_count", DataType::Int32, false),
            column("deleted_data_files_count", DataType::Int32, false),
            column("added_rows_count", DataType::Int64, false),
            column("existing_rows_count", DataType::Int64, false),
            column("deleted_rows_count", DataType::Int64, false),
            column(
                "partition_summaries",
                list_of(DataType::Struct(
                    vec![
                        Arc::new(Field::new("contains_null", DataType::Boolean, true)),
                        Arc::new(Field::new("contains_nan", DataType::Boolean, true)),
                        Arc::new(Field::new("lower_bound", DataType::Utf8, true)),
                        Arc::new(Field::new("upper_bound", DataType::Utf8, true)),
                    ]
                    .into(),
                )),
                true,
            ),
        ]),
        MetadataTableType::LogicalIcebergMetadata => {
            let mut columns = vec![
                column("status", DataType::Int32, false),
                column("snapshot_id", DataType::Int64, true),
                column("sequence_number", DataType::Int64, true),
                column("file_sequence_number", DataType::Int64, true),
            ];
            columns.extend(files()?);
            Ok(columns)
        }
    }
}

fn partition_source_type(
    metadata: &TableMetadata,
    source_id: i32,
) -> Option<&crate::iceberg::spec::Type> {
    metadata
        .current_schema()
        .field_by_id(source_id)
        .map(|field| field.field_type.as_ref())
        .or_else(|| {
            metadata.schemas_iter().find_map(|schema| {
                schema
                    .field_by_id(source_id)
                    .map(|field| field.field_type.as_ref())
            })
        })
}

fn partition_struct_type(metadata: &TableMetadata) -> Result<DataType, String> {
    let mut specs = metadata.partition_specs_iter().cloned().collect::<Vec<_>>();
    specs.sort_by_key(|spec| spec.spec_id());
    let mut fields: Vec<Arc<Field>> = Vec::new();
    for spec in specs {
        for partition_field in spec.fields() {
            let source_type = partition_source_type(metadata, partition_field.source_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg partition field {} references missing source field id {}",
                        partition_field.name, partition_field.source_id
                    )
                })?;
            let result_type =
                partition_field
                    .transform
                    .result_type(source_type)
                    .map_err(|error| {
                        format!(
                            "infer iceberg partition field {} type: {error}",
                            partition_field.name
                        )
                    })?;
            let arrow_type = iceberg_type_to_arrow_type(&result_type)?;
            if let Some(existing) = fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(&partition_field.name))
            {
                if existing.data_type() != &arrow_type {
                    return Err(format!(
                        "iceberg partition field {} has incompatible types across specs: {:?} vs {:?}",
                        partition_field.name,
                        existing.data_type(),
                        arrow_type
                    ));
                }
                continue;
            }
            fields.push(Arc::new(Field::new(
                partition_field.name.clone(),
                arrow_type,
                true,
            )));
        }
    }
    Ok(DataType::Struct(Fields::from(fields)))
}

pub(crate) fn iceberg_type_to_arrow_type(
    ty: &crate::iceberg::spec::Type,
) -> Result<DataType, String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    match ty {
        Type::Primitive(primitive) => Ok(match primitive {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
                u8::try_from(*precision)
                    .map_err(|_| format!("iceberg decimal precision out of range: {precision}"))?,
                i8::try_from(*scale)
                    .map_err(|_| format!("iceberg decimal scale out of range: {scale}"))?,
            ),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            PrimitiveType::String | PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
                i32::try_from(*width)
                    .map_err(|_| format!("iceberg fixed width out of range: {width}"))?,
            ),
            PrimitiveType::Binary | PrimitiveType::Variant => DataType::Binary,
        }),
        other => Err(format!(
            "iceberg metadata partition field must be primitive, got {other:?}"
        )),
    }
}

#[derive(Clone, Debug)]
struct MetadataBatchReaderConfig {
    metadata_table_type: MetadataTableType,
    serialized_table: String,
    serialized_payload: String,
    batch_size: usize,
    output_columns: Vec<MetadataOutputColumn>,
}

fn parse_table_metadata(serialized: &str) -> Result<TableMetadata, String> {
    serde_json::from_str::<TableMetadata>(serialized)
        .map_err(|e| format!("parse iceberg table metadata for metadata-scan failed: {e}"))
}

struct MetadataBatchReader {
    batches: std::vec::IntoIter<RecordBatch>,
    context: ConnectorRequestContext,
    closed: bool,
}

/// Opens a provider-owned metadata reader for a frozen Iceberg metadata scan.
///
/// The carrier-provided expected schema is authoritative; the provider derives
/// a schema-only projection from it and validates that its native batch builder
/// emits exactly the same Arrow schema.
pub fn open_metadata_connector_reader(
    metadata_table_type: MetadataTableType,
    serialized_table: String,
    serialized_payload: String,
    expected_schema: SchemaRef,
    batch: ConnectorBatchBudget,
    context: ConnectorRequestContext,
) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
    let output_columns = expected_schema
        .fields()
        .iter()
        .map(|field| MetadataOutputColumn {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
        .collect();
    let reader = MetadataBatchReaderBuilder::new(MetadataBatchReaderConfig {
        metadata_table_type,
        serialized_table,
        serialized_payload,
        batch_size: batch.max_rows.get(),
        output_columns,
    })
    .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
    if reader.output_schema.as_ref() != expected_schema.as_ref() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg metadata reader schema differs from connector expected schema",
        ));
    }
    let batches = reader
        .read_batches()
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
    Ok(Box::new(MetadataBatchReader {
        batches: batches.into_iter(),
        context,
        closed: false,
    }))
}

impl ConnectorBatchReader for MetadataBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        if self.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        Ok(self.batches.next())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

pub fn metadata_output_schema(
    output_columns: &[MetadataOutputColumn],
) -> Result<SchemaRef, String> {
    MetadataBatchReaderBuilder::new(MetadataBatchReaderConfig {
        metadata_table_type: MetadataTableType::Files,
        serialized_table: String::new(),
        serialized_payload: String::new(),
        batch_size: 1,
        output_columns: output_columns.to_vec(),
    })
    .map(|reader| reader.output_schema)
}

#[derive(Clone, Debug)]
struct MetadataBatchReaderBuilder {
    cfg: MetadataBatchReaderConfig,
    output_schema: SchemaRef,
}

impl MetadataBatchReaderBuilder {
    fn new(cfg: MetadataBatchReaderConfig) -> Result<Self, String> {
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
        Ok(Self {
            output_schema: Arc::new(Schema::new(fields)),
            cfg,
        })
    }

    fn read_batches(&self) -> Result<Vec<RecordBatch>, String> {
        match self.cfg.metadata_table_type {
            MetadataTableType::Files => build_files_chunks(
                &load_files_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
            MetadataTableType::Manifests => build_manifests_chunks(
                &load_manifests_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
            MetadataTableType::LogicalIcebergMetadata => build_entries_chunks(
                &load_entries_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
            MetadataTableType::Snapshots => build_snapshot_chunks(
                &load_snapshot_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
            MetadataTableType::History => build_history_chunks(
                &load_history_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
            MetadataTableType::Refs => build_ref_chunks(
                &load_ref_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
            MetadataTableType::Partitions => build_partition_chunks(
                &load_partition_rows(&self.cfg)?,
                &self.cfg.output_columns,
                &self.output_schema,
                self.cfg.batch_size,
            ),
        }
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

fn build_chunks(
    schema: &SchemaRef,
    arrays: Vec<ArrayRef>,
    row_count: usize,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
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
        return Ok(vec![batch]);
    }

    let mut chunks = Vec::new();
    let mut offset = 0usize;
    while offset < row_count {
        let len = (row_count - offset).min(batch_size);
        chunks.push(batch.slice(offset, len));
        offset += len;
    }
    Ok(chunks)
}

fn iceberg_map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
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

fn load_snapshot_rows(cfg: &MetadataBatchReaderConfig) -> Result<Vec<SnapshotMetadataRow>, String> {
    let metadata = parse_table_metadata(&cfg.serialized_table)?;
    let mut rows = Vec::with_capacity(metadata.snapshots().len());
    for snapshot in metadata.snapshots() {
        let summary = snapshot.summary();
        // External write fence markers are provider bookkeeping, not table
        // history. They carry no data and describe no user write, so they must
        // not appear in `$snapshots` alongside the snapshots a user actually
        // produced. (They remain present in the raw Iceberg metadata, which is
        // an accepted cost of the carrier -- see ADR-0068.)
        if crate::commit::write_fence::is_fence_marker_snapshot(summary) {
            continue;
        }
        let summary_pairs = if summary.additional_properties.is_empty() {
            None
        } else {
            let mut pairs: Vec<(String, String)> = summary
                .additional_properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            // Stable key order so chunked output is deterministic across runs.
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Some(pairs)
        };
        rows.push(SnapshotMetadataRow {
            // Iceberg snapshot timestamps are millisecond-resolution; the
            // analyzer surfaces this column as Int64 microseconds.
            committed_at_micros: snapshot.timestamp_ms().saturating_mul(1_000),
            snapshot_id: snapshot.snapshot_id(),
            parent_id: snapshot.parent_snapshot_id(),
            operation: Some(summary.operation.as_str().to_string()),
            manifest_list: snapshot.manifest_list().to_string(),
            summary: summary_pairs,
        });
    }
    Ok(rows)
}

fn build_snapshot_chunks(
    rows: &[SnapshotMetadataRow],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_snapshot_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

fn build_snapshot_array(
    column: &MetadataOutputColumn,
    rows: &[SnapshotMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "committed_at" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.committed_at_micros)
                .collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "operation" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.operation.as_deref())
                .collect::<Vec<_>>(),
        ))),
        "manifest_list" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.manifest_list.as_str()))
                .collect::<Vec<_>>(),
        ))),
        // Serialize as a JSON object string so that the column matches the
        // Utf8 type the analyzer advertises, enabling LIKE / string operations.
        // Example: {"added-data-files":"1","engine-name":"novarocks",...}
        // Keys/values are escaped via serde_json to handle embedded quotes or
        // backslashes in arbitrary summary values.
        "summary" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| {
                    r.summary.as_ref().map(|pairs| {
                        let map: serde_json::Map<String, serde_json::Value> = pairs
                            .iter()
                            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                            .collect();
                        serde_json::Value::Object(map).to_string()
                    })
                })
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg snapshots metadata column: {}",
            other
        )),
    }
}

// Reference implementation for the `Map`-typed metadata columns; the
// `$files`/`$entries` int-keyed map builders mirror its `MapFieldNames` usage.
// Currently unused in production (the `summary` column is surfaced as Utf8),
// but kept as the canonical pattern.
#[allow(dead_code)]
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

fn load_history_rows(cfg: &MetadataBatchReaderConfig) -> Result<Vec<HistoryMetadataRow>, String> {
    let metadata = parse_table_metadata(&cfg.serialized_table)?;
    // `is_current_ancestor` is true for any snapshot reachable from the
    // current head by walking parent_snapshot_id pointers. Build the set
    // up front so each history row can be tagged in O(1).
    let mut current_ancestors: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut walker = metadata.current_snapshot_id();
    while let Some(id) = walker {
        if !current_ancestors.insert(id) {
            // Defensive: stop on any cycle in parent pointers.
            break;
        }
        walker = metadata
            .snapshot_by_id(id)
            .and_then(|snap| snap.parent_snapshot_id());
    }

    let history = metadata.history();
    let mut rows = Vec::with_capacity(history.len());
    for entry in history {
        // Resolve parent_snapshot_id by looking the snapshot up; the
        // history log itself only carries (snapshot_id, timestamp_ms).
        let parent_id = metadata
            .snapshot_by_id(entry.snapshot_id)
            .and_then(|snap| snap.parent_snapshot_id());
        rows.push(HistoryMetadataRow {
            made_current_at_micros: entry.timestamp_ms.saturating_mul(1_000),
            snapshot_id: entry.snapshot_id,
            parent_id,
            is_current_ancestor: current_ancestors.contains(&entry.snapshot_id),
        });
    }
    Ok(rows)
}

fn build_history_chunks(
    rows: &[HistoryMetadataRow],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_history_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

fn build_history_array(
    column: &MetadataOutputColumn,
    rows: &[HistoryMetadataRow],
) -> Result<ArrayRef, String> {
    use arrow::array::BooleanArray;
    match column.name.as_str() {
        "made_current_at" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.made_current_at_micros)
                .collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "is_current_ancestor" => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|r| r.is_current_ancestor)
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg history metadata column: {}",
            other
        )),
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

fn load_ref_rows(cfg: &MetadataBatchReaderConfig) -> Result<Vec<RefMetadataRow>, String> {
    let metadata = parse_table_metadata(&cfg.serialized_table)?;
    let refs = metadata.refs();
    let mut rows: Vec<RefMetadataRow> = refs
        .iter()
        .filter(|(name, _)| !is_provider_private_ref(name))
        .map(|(name, reference)| {
            let (type_, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms) =
                match &reference.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep,
                        max_snapshot_age_ms,
                        max_ref_age_ms,
                    } => (
                        "BRANCH",
                        *max_ref_age_ms,
                        *min_snapshots_to_keep,
                        *max_snapshot_age_ms,
                    ),
                    SnapshotRetention::Tag { max_ref_age_ms } => {
                        ("TAG", *max_ref_age_ms, None, None)
                    }
                };
            RefMetadataRow {
                name: name.clone(),
                type_: type_.to_string(),
                snapshot_id: reference.snapshot_id,
                max_reference_age_in_ms,
                min_snapshots_to_keep,
                max_snapshot_age_in_ms,
            }
        })
        .collect();
    // Stable name order so output is deterministic across runs.
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rows)
}

fn build_ref_chunks(
    rows: &[RefMetadataRow],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_ref_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

fn build_ref_array(
    column: &MetadataOutputColumn,
    rows: &[RefMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "name" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.name.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "type" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.type_.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "max_reference_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.max_reference_age_in_ms)
                .collect::<Vec<_>>(),
        ))),
        "min_snapshots_to_keep" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|r| r.min_snapshots_to_keep)
                .collect::<Vec<_>>(),
        ))),
        "max_snapshot_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.max_snapshot_age_in_ms)
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg refs metadata column: {}",
            other
        )),
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct PartitionMetadataPayload {
    version: i32,
    rows: Vec<PartitionMetadataRow>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct PartitionMetadataRow {
    record_count: i64,
    file_count: i64,
    position_delete_file_count: Option<i64>,
    equality_delete_file_count: Option<i64>,
}

fn load_partition_rows(
    cfg: &MetadataBatchReaderConfig,
) -> Result<Vec<PartitionMetadataRow>, String> {
    if cfg.serialized_payload.trim().is_empty() {
        return Err(
            "iceberg partitions metadata scan missing partition aggregate payload".to_string(),
        );
    }
    let payload: PartitionMetadataPayload = serde_json::from_str(&cfg.serialized_payload)
        .map_err(|e| format!("parse iceberg partitions metadata payload failed: {e}"))?;
    if payload.version != 1 {
        return Err(format!(
            "unsupported iceberg partitions metadata payload version {}",
            payload.version
        ));
    }
    Ok(payload.rows)
}

fn build_partition_chunks(
    rows: &[PartitionMetadataRow],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_partition_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

fn build_partition_array(
    column: &MetadataOutputColumn,
    rows: &[PartitionMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.record_count).collect::<Vec<_>>(),
        ))),
        "file_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.file_count).collect::<Vec<_>>(),
        ))),
        "position_delete_file_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.position_delete_file_count)
                .collect::<Vec<_>>(),
        ))),
        "equality_delete_file_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.equality_delete_file_count)
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg partitions metadata column: {}",
            other
        )),
    }
}

/// `{version,rows}` envelope shared by the `$files` / `$manifests` /
/// `$entries` metadata tables. The resolution-time manifest walk
/// (`metadata_read.rs`) produces this exact shape; here we decode the row
/// objects back out so the per-table builders can materialise Arrow columns.
#[derive(Clone, Debug, serde::Deserialize)]
struct JsonRowsPayload {
    version: i32,
    rows: Vec<serde_json::Value>,
}

/// Decode the `{version,rows}` payload carried on
/// `MetadataBatchReaderConfig::serialized_predicate` into its row objects.
/// `label` names the metadata table for error messages.
fn load_json_rows(
    cfg: &MetadataBatchReaderConfig,
    label: &str,
) -> Result<Vec<serde_json::Value>, String> {
    if cfg.serialized_payload.trim().is_empty() {
        return Err(format!("iceberg {label} metadata scan missing payload"));
    }
    let payload: JsonRowsPayload = serde_json::from_str(&cfg.serialized_payload)
        .map_err(|e| format!("parse iceberg {label} metadata payload failed: {e}"))?;
    if payload.version != 1 {
        return Err(format!(
            "unsupported iceberg {label} metadata payload version {}",
            payload.version
        ));
    }
    Ok(payload.rows)
}

fn load_files_rows(cfg: &MetadataBatchReaderConfig) -> Result<Vec<serde_json::Value>, String> {
    load_json_rows(cfg, "files")
}

fn load_manifests_rows(cfg: &MetadataBatchReaderConfig) -> Result<Vec<serde_json::Value>, String> {
    load_json_rows(cfg, "manifests")
}

fn load_entries_rows(cfg: &MetadataBatchReaderConfig) -> Result<Vec<serde_json::Value>, String> {
    load_json_rows(cfg, "entries")
}

/// Convert a JSON array of small non-negative integers into a `Vec<u8>`,
/// rejecting any element that is not an in-range byte. Used for `key_metadata`
/// and the `lower_bounds`/`upper_bounds` map values (the walk serialises bytes
/// as a JSON array of `u8`).
fn json_u8_array(items: &[serde_json::Value]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        let v = it
            .as_u64()
            .ok_or_else(|| "expected byte value in JSON array".to_string())?;
        if v > u8::MAX as u64 {
            return Err(format!("byte value out of range: {v}"));
        }
        out.push(v as u8);
    }
    Ok(out)
}

/// Build a `Map<Int32, Int64>` column from rows whose `name` field is a JSON
/// array of `[key, value]` pairs. Map field names mirror
/// `build_string_string_map_array` (via `iceberg_map_field_names`) so the
/// produced type matches the analyzer's `map_int_to(Int64)` declaration.
fn build_int_int_map_array(rows: &[serde_json::Value], name: &str) -> Result<ArrayRef, String> {
    use arrow::array::{Int32Builder, Int64Builder};
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        Int32Builder::new(),
        Int64Builder::new(),
    );
    for row in rows {
        match row.get(name).and_then(|v| v.as_array()) {
            Some(pairs) => {
                for pair in pairs {
                    let entry = pair
                        .as_array()
                        .ok_or_else(|| format!("{name} entry must be a [key,value] array"))?;
                    let key = entry
                        .first()
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| format!("{name} key must be an integer"))?;
                    let value = entry
                        .get(1)
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| format!("{name} value must be an integer"))?;
                    builder.keys().append_value(key as i32);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append {name} map row failed: {e}"))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null {name} map row failed: {e}"))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build a `Map<Int32, Binary>` column from rows whose `name` field is a JSON
/// array of `[key, [bytes...]]` pairs. Map field names mirror
/// `build_string_string_map_array` so the produced type matches the analyzer's
/// `map_int_to(Binary)` declaration.
fn build_int_binary_map_array(rows: &[serde_json::Value], name: &str) -> Result<ArrayRef, String> {
    use arrow::array::{BinaryBuilder, Int32Builder};
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        Int32Builder::new(),
        BinaryBuilder::new(),
    );
    for row in rows {
        match row.get(name).and_then(|v| v.as_array()) {
            Some(pairs) => {
                for pair in pairs {
                    let entry = pair
                        .as_array()
                        .ok_or_else(|| format!("{name} entry must be a [key,value] array"))?;
                    let key = entry
                        .first()
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| format!("{name} key must be an integer"))?;
                    let bytes = entry
                        .get(1)
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| format!("{name} value must be a byte array"))?;
                    builder.keys().append_value(key as i32);
                    builder.values().append_value(json_u8_array(bytes)?);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append {name} map row failed: {e}"))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null {name} map row failed: {e}"))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn partition_field_value<'a>(
    row: &'a serde_json::Value,
    field: &Field,
) -> Option<&'a serde_json::Value> {
    row.get("partition")
        .and_then(|value| value.as_object())
        .and_then(|partition| partition.get(field.name()))
        .filter(|value| !value.is_null())
}

fn build_partition_child_array(
    field: &Field,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{
        BinaryBuilder, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };

    match field.data_type() {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| partition_field_value(row, field).and_then(|value| value.as_bool()))
                .collect::<Vec<_>>(),
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| {
                    partition_field_value(row, field)
                        .and_then(|value| value.as_i64())
                        .map(|value| value as i32)
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| partition_field_value(row, field).and_then(|value| value.as_i64()))
                .collect::<Vec<_>>(),
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(
            rows.iter()
                .map(|row| {
                    partition_field_value(row, field)
                        .and_then(|value| value.as_f64())
                        .map(|value| value as f32)
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| partition_field_value(row, field).and_then(|value| value.as_f64()))
                .collect::<Vec<_>>(),
        ))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| partition_field_value(row, field).and_then(|value| value.as_str()))
                .collect::<Vec<_>>(),
        ))),
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match partition_field_value(row, field).and_then(|value| value.as_array()) {
                    Some(bytes) => builder.append_value(json_u8_array(bytes)?),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            rows.iter()
                .map(|row| {
                    partition_field_value(row, field)
                        .and_then(|value| value.as_i64())
                        .map(|value| value as i32)
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Time64(TimeUnit::Microsecond) => Ok(Arc::new(Time64MicrosecondArray::from(
            rows.iter()
                .map(|row| partition_field_value(row, field).and_then(|value| value.as_i64()))
                .collect::<Vec<_>>(),
        ))),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(Arc::new(Time64NanosecondArray::from(
            rows.iter()
                .map(|row| partition_field_value(row, field).and_then(|value| value.as_i64()))
                .collect::<Vec<_>>(),
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(
                rows.iter()
                    .map(|row| partition_field_value(row, field).and_then(|value| value.as_i64()))
                    .collect::<Vec<_>>(),
            )))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Ok(Arc::new(TimestampNanosecondArray::from(
                rows.iter()
                    .map(|row| partition_field_value(row, field).and_then(|value| value.as_i64()))
                    .collect::<Vec<_>>(),
            )))
        }
        other => Err(format!(
            "unsupported iceberg files partition field {} type {:?}",
            field.name(),
            other
        )),
    }
}

fn build_partition_struct_array(
    column: &MetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::StructArray;
    let DataType::Struct(fields) = &column.data_type else {
        return Err(format!(
            "iceberg files partition column must be STRUCT, got {:?}",
            column.data_type
        ));
    };
    if fields.is_empty() {
        return Ok(Arc::new(
            StructArray::try_new_with_length(fields.clone(), vec![], None, rows.len())
                .map_err(|e| format!("build empty iceberg partition struct failed: {e}"))?,
        ));
    }
    let arrays = fields
        .iter()
        .map(|field| build_partition_child_array(field.as_ref(), rows))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), arrays, None)
            .map_err(|e| format!("build iceberg partition struct failed: {e}"))?,
    ))
}

/// Build the Arrow array for a single `$files` column from the JSON rows. The
/// produced array type EXACTLY matches the analyzer's `files_columns()`
/// declaration for that column (scalars, field-id maps, lists). Non-nullable
/// columns (`content`, `file_path`, `file_format`, `spec_id`, `record_count`,
/// `file_size_in_bytes`) always receive a value; the rest use `append_option`.
fn build_files_array(
    column: &MetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder};
    match column.name.as_str() {
        // Non-nullable Int32 scalar.
        "content" | "spec_id" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int32 scalar.
        "sort_order_id" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_option(
                    r.get(&column.name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32),
                );
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int64 scalar.
        "record_count" | "file_size_in_bytes" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0));
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int64 scalar.
        "first_row_id" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(r.get(&column.name).and_then(|v| v.as_i64()));
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Utf8 scalar.
        "file_path" | "file_format" => {
            let mut b = StringBuilder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_str()).unwrap_or(""));
            }
            Ok(Arc::new(b.finish()))
        }
        "partition" => build_partition_struct_array(column, rows),
        // Nullable Binary scalar.
        "key_metadata" => {
            let mut b = BinaryBuilder::new();
            for r in rows {
                match r.get("key_metadata").and_then(|v| v.as_array()) {
                    Some(bytes) => b.append_value(json_u8_array(bytes)?),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        "column_sizes" | "value_counts" | "null_value_counts" | "nan_value_counts" => {
            build_int_int_map_array(rows, &column.name)
        }
        "lower_bounds" | "upper_bounds" => build_int_binary_map_array(rows, &column.name),
        "split_offsets" => {
            let mut b = ListBuilder::new(Int64Builder::new());
            for r in rows {
                match r.get("split_offsets").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            b.values().append_option(it.as_i64());
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        "equality_ids" => {
            let mut b = ListBuilder::new(Int32Builder::new());
            for r in rows {
                match r.get("equality_ids").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            b.values().append_option(it.as_i64().map(|x| x as i32));
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(format!(
            "unsupported iceberg files metadata column: {other}"
        )),
    }
}

fn build_files_chunks(
    rows: &[serde_json::Value],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_files_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

/// Build the Arrow array for a single `$manifests` column. Scalars follow the
/// `$files` pattern; the non-nullable count columns coerce a missing/null
/// source value to `0` (they are declared NON-nullable but the walk emits
/// `null` when the underlying `Option` is absent); `partition_summaries` is a
/// `List<Struct<...>>` whose struct field order/names are derived from the
/// analyzer-declared `column.data_type` so the produced type matches exactly.
fn build_manifests_array(
    column: &MetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{
        BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    };
    match column.name.as_str() {
        // Non-nullable Int32 scalar.
        "content" | "partition_spec_id" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int32 counts: coerce missing/null to 0.
        "added_data_files_count" | "existing_data_files_count" | "deleted_data_files_count" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int64 scalar.
        "length" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_value(r.get("length").and_then(|v| v.as_i64()).unwrap_or(0));
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int64 counts: coerce missing/null to 0.
        "added_rows_count" | "existing_rows_count" | "deleted_rows_count" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0));
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int64 scalar.
        "added_snapshot_id" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(r.get("added_snapshot_id").and_then(|v| v.as_i64()));
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Utf8 scalar.
        "path" => {
            let mut b = StringBuilder::new();
            for r in rows {
                b.append_value(r.get("path").and_then(|v| v.as_str()).unwrap_or(""));
            }
            Ok(Arc::new(b.finish()))
        }
        "partition_summaries" => {
            // Derive the struct fields from the analyzer-declared List<Struct>
            // type so names/nullability match exactly at RecordBatch::try_new.
            let fields = match &column.data_type {
                DataType::List(f) => match f.data_type() {
                    DataType::Struct(fs) => fs.clone(),
                    _ => return Err("partition_summaries inner type is not a struct".into()),
                },
                _ => return Err("partition_summaries type is not a list".into()),
            };
            let mut b = ListBuilder::new(StructBuilder::from_fields(fields.clone(), 0));
            for r in rows {
                match r.get("partition_summaries").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            let sb = b.values();
                            sb.field_builder::<BooleanBuilder>(0)
                                .ok_or("partition_summaries field 0 builder")?
                                .append_option(it.get("contains_null").and_then(|v| v.as_bool()));
                            sb.field_builder::<BooleanBuilder>(1)
                                .ok_or("partition_summaries field 1 builder")?
                                .append_option(it.get("contains_nan").and_then(|v| v.as_bool()));
                            sb.field_builder::<StringBuilder>(2)
                                .ok_or("partition_summaries field 2 builder")?
                                .append_option(it.get("lower_bound").and_then(|v| v.as_str()));
                            sb.field_builder::<StringBuilder>(3)
                                .ok_or("partition_summaries field 3 builder")?
                                .append_option(it.get("upper_bound").and_then(|v| v.as_str()));
                            sb.append(true);
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(format!(
            "unsupported iceberg manifests metadata column: {other}"
        )),
    }
}

fn build_manifests_chunks(
    rows: &[serde_json::Value],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_manifests_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

/// Build the Arrow array for a single `$entries` column. The entry-level
/// columns (`status` non-nullable Int32; `snapshot_id` / `sequence_number` /
/// `file_sequence_number` nullable Int64) are built here; every other column
/// (including `first_row_id`) is a file property and delegates to
/// `build_files_array`, since the JSON row carries them under identical names.
fn build_entries_array(
    column: &MetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{Int32Builder, Int64Builder};
    match column.name.as_str() {
        // Non-nullable Int32 scalar.
        "status" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get("status").and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int64 entry scalars.
        "snapshot_id" | "sequence_number" | "file_sequence_number" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(r.get(&column.name).and_then(|v| v.as_i64()));
            }
            Ok(Arc::new(b.finish()))
        }
        // `first_row_id` + every $files column reuse the files builder.
        _ => build_files_array(column, rows),
    }
}

fn build_entries_chunks(
    rows: &[serde_json::Value],
    output_columns: &[MetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_entries_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

#[cfg(test)]
mod tests {
    use super::{
        MetadataOutputColumn, MetadataTableType, build_files_array, build_files_chunks,
        metadata_output_schema, metadata_table_output_columns, normalize_metadata_output_type,
    };
    use arrow::array::{Array, Int32Array, Int64Array, MapArray};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn table_metadata() -> crate::iceberg::spec::TableMetadata {
        use std::collections::HashMap;

        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///metadata-schema-test".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn provider_owns_metadata_alias_schemas() {
        let metadata = table_metadata();
        let files = metadata_table_output_columns(MetadataTableType::Files, &metadata)
            .expect("files columns");
        assert_eq!(
            files.first().map(|column| column.name.as_str()),
            Some("content")
        );
        assert_eq!(
            files.last().map(|column| column.name.as_str()),
            Some("partition")
        );
        assert!(matches!(
            files.last().map(|column| &column.data_type),
            Some(DataType::Struct(fields)) if fields.is_empty()
        ));
        let snapshots = metadata_table_output_columns(MetadataTableType::Snapshots, &metadata)
            .expect("snapshots columns");
        assert_eq!(
            snapshots
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary"
            ]
        );
    }

    #[test]
    fn normalizes_metadata_map_key_nullability() {
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
        let DataType::Map(entries, _) = normalize_metadata_output_type(&ty) else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries struct");
        };
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn files_builder_emits_declared_map_and_record_batch_schema() {
        let columns = vec![
            MetadataOutputColumn {
                name: "content".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            MetadataOutputColumn {
                name: "record_count".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            MetadataOutputColumn {
                name: "column_sizes".into(),
                data_type: DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Int32, false)),
                                Arc::new(Field::new("value", DataType::Int64, true)),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                nullable: true,
            },
        ];
        let rows = vec![serde_json::json!({
            "content": 0,
            "record_count": 3,
            "column_sizes": [[1, 100]],
        })];
        let map = build_files_array(&columns[2], &rows).unwrap();
        let map = map.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map.value(0);
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            entries
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            100
        );

        let schema = metadata_output_schema(&columns).unwrap();
        let batches = build_files_chunks(&rows, &columns, &schema, 1).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), schema);
    }
}
