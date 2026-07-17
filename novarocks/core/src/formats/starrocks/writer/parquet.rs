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

use std::collections::{HashMap, hash_map::Entry};
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;

use arrow::array::{
    Array, ArrayRef, ListArray, MapArray, StructArray, UInt32Array, new_null_array,
};
use arrow::compute::{cast, concat, take};
use arrow::datatypes::{DataType, Field, Fields, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::formats::starrocks::fs_access::resolve_format_path;
use crate::formats::starrocks::metadata::{StarRocksSegmentFile, StarRocksTabletSnapshot};
use crate::formats::starrocks::plan::{
    StarRocksOutputColumnHint, StarRocksPhysicalColumnBinding,
    validate_physical_schema_to_output_type,
};
use crate::fs::access::FsScheme;
use crate::service::grpc_client::proto::starrocks::{ColumnPb, TabletSchemaPb};

pub fn read_bundle_parquet_snapshot_if_any(
    snapshot: &StarRocksTabletSnapshot,
    output_schema: SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    read_bundle_parquet_snapshot_impl(snapshot, output_schema, None, &snapshot.tablet_schema)
}

pub fn read_bundle_parquet_snapshot_with_output_hints_if_any(
    snapshot: &StarRocksTabletSnapshot,
    output_schema: SchemaRef,
    output_hints: &[StarRocksOutputColumnHint],
) -> Result<Option<RecordBatch>, String> {
    read_bundle_parquet_snapshot_impl(
        snapshot,
        output_schema,
        Some(output_hints),
        &snapshot.tablet_schema,
    )
}

pub fn read_bundle_parquet_snapshot_with_output_hints_and_physical_schema_if_any(
    snapshot: &StarRocksTabletSnapshot,
    output_schema: SchemaRef,
    output_hints: &[StarRocksOutputColumnHint],
    physical_fallback_schema: &TabletSchemaPb,
) -> Result<Option<RecordBatch>, String> {
    read_bundle_parquet_snapshot_impl(
        snapshot,
        output_schema,
        Some(output_hints),
        physical_fallback_schema,
    )
}

fn read_bundle_parquet_snapshot_impl(
    snapshot: &StarRocksTabletSnapshot,
    output_schema: SchemaRef,
    output_hints: Option<&[StarRocksOutputColumnHint]>,
    physical_fallback_schema: &TabletSchemaPb,
) -> Result<Option<RecordBatch>, String> {
    if snapshot.segment_files.is_empty() {
        return Ok(None);
    }
    if snapshot
        .segment_files
        .iter()
        .any(|seg| seg.bundle_file_offset.is_some())
    {
        return Ok(None);
    }
    if snapshot
        .segment_files
        .iter()
        .any(|seg| !seg.name.to_ascii_lowercase().ends_with(".parquet"))
    {
        return Ok(None);
    }

    let mut batches = Vec::new();
    for seg in &snapshot.segment_files {
        let physical_schema =
            resolve_segment_source_schema(snapshot, seg, physical_fallback_schema)?;
        let parquet_file = read_parquet_file_with_schema(&seg.path)?;
        let segment_batches = if parquet_file.batches.is_empty() {
            vec![RecordBatch::new_empty(parquet_file.schema)]
        } else {
            parquet_file.batches
        };
        for batch in segment_batches {
            let aligned = match output_hints {
                Some(hints) => align_batch_to_output_schema_with_hints_and_current_schema(
                    batch,
                    &output_schema,
                    hints,
                    &snapshot.tablet_schema,
                    physical_schema,
                )?,
                None => align_batch_to_output_schema(batch, &output_schema)?,
            };
            if aligned.num_rows() > 0 {
                batches.push(aligned);
            }
        }
    }
    if batches.is_empty() {
        return Ok(Some(RecordBatch::new_empty(output_schema)));
    }
    concat_batches(output_schema, batches)
}

fn resolve_segment_source_schema<'a>(
    snapshot: &'a StarRocksTabletSnapshot,
    segment: &StarRocksSegmentFile,
    physical_fallback_schema: &'a TabletSchemaPb,
) -> Result<&'a TabletSchemaPb, String> {
    let schema_id = match segment.schema_id {
        None => return Ok(physical_fallback_schema),
        Some(schema_id) if schema_id <= 0 => {
            return Err(format!(
                "segment rowset schema id must be positive when present: tablet_id={}, version={}, segment_path={}, rowset_version={}, schema_id={}",
                snapshot.tablet_id,
                snapshot.version,
                segment.path,
                segment.rowset_version,
                schema_id
            ));
        }
        Some(schema_id) => schema_id,
    };
    if let Some(schema) = snapshot.historical_schemas.get(&schema_id) {
        if schema.id != Some(schema_id) {
            return Err(format!(
                "segment rowset resolved tablet schema id mismatch: tablet_id={}, version={}, segment_path={}, rowset_version={}, schema_id={}, resolved_schema_id={:?}",
                snapshot.tablet_id,
                snapshot.version,
                segment.path,
                segment.rowset_version,
                schema_id,
                schema.id
            ));
        }
        return Ok(schema);
    }
    if snapshot.tablet_schema.id == Some(schema_id) {
        return Ok(&snapshot.tablet_schema);
    }
    Err(format!(
        "segment rowset schema id is missing from snapshot historical schemas: tablet_id={}, version={}, segment_path={}, rowset_version={}, schema_id={}",
        snapshot.tablet_id, snapshot.version, segment.path, segment.rowset_version, schema_id
    ))
}

pub fn write_parquet_file(path: &str, batch: &RecordBatch) -> Result<u64, String> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    reject_hdfs_path(path, "write_parquet_file")?;
    let access = resolve_format_path(path)?;

    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if let Some(parent) = path_buf.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("create parquet dir failed: {}", e))?;
            }
            let file = fs::File::create(&path_buf)
                .map_err(|e| format!("create parquet file failed: {}", e))?;
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
                .map_err(|e| format!("create parquet writer failed: {}", e))?;
            writer
                .write(batch)
                .map_err(|e| format!("write parquet batch failed: {}", e))?;
            writer
                .close()
                .map_err(|e| format!("close parquet writer failed: {}", e))?;
            let meta =
                fs::metadata(&path_buf).map_err(|e| format!("stat parquet failed: {}", e))?;
            Ok(meta.len())
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let mut bytes = Vec::new();
            {
                let cursor = Cursor::new(&mut bytes);
                let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
                    .map_err(|e| format!("create parquet writer failed: {}", e))?;
                writer
                    .write(batch)
                    .map_err(|e| format!("write parquet batch failed: {}", e))?;
                writer
                    .close()
                    .map_err(|e| format!("close parquet writer failed: {}", e))?;
            }
            let size = bytes.len() as u64;
            let write_result =
                crate::fs::object_store::oss_block_on(access.operator().write(&rel, bytes))?;
            write_result.map_err(|e| format!("write parquet object failed: {}", e))?;
            Ok(size)
        }
        FsScheme::Hdfs => Err(format!(
            "write_parquet_file does not support hdfs path yet: {}",
            path
        )),
    }
}

pub fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>, String> {
    Ok(read_parquet_file_with_schema(path)?.batches)
}

struct ParquetFileRead {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

fn read_parquet_file_with_schema(path: &str) -> Result<ParquetFileRead, String> {
    reject_hdfs_path(path, "read_parquet_file")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let file = fs::File::open(path).map_err(|e| format!("open parquet failed: {}", e))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| format!("create parquet reader failed: {}", e))?;
            let schema = builder.schema().clone();
            let reader = builder
                .build()
                .map_err(|e| format!("build parquet reader failed: {}", e))?;
            let mut out = Vec::new();
            for batch in reader {
                out.push(batch.map_err(|e| format!("read parquet batch failed: {}", e))?);
            }
            Ok(ParquetFileRead {
                schema,
                batches: out,
            })
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let read_result = crate::fs::object_store::oss_block_on(access.operator().read(&rel))?;
            let bytes = read_result.map_err(|e| format!("read parquet object failed: {}", e))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.to_bytes())
                .map_err(|e| format!("create parquet reader failed: {}", e))?;
            let schema = builder.schema().clone();
            let reader = builder
                .build()
                .map_err(|e| format!("build parquet reader failed: {}", e))?;
            let mut out = Vec::new();
            for batch in reader {
                out.push(batch.map_err(|e| format!("read parquet batch failed: {}", e))?);
            }
            Ok(ParquetFileRead {
                schema,
                batches: out,
            })
        }
        FsScheme::Hdfs => Err(format!(
            "read_parquet_file does not support hdfs path yet: {}",
            path
        )),
    }
}

fn reject_hdfs_path(path: &str, function_name: &str) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("hdfs"))
    {
        return Err(format!(
            "{function_name} does not support hdfs path yet: {path}"
        ));
    }
    Ok(())
}

fn align_batch_to_output_schema(
    batch: RecordBatch,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    align_batch_to_output_schema_inner(batch, output_schema, None)
}

fn align_batch_to_output_schema_with_hints_and_current_schema(
    batch: RecordBatch,
    output_schema: &SchemaRef,
    output_hints: &[StarRocksOutputColumnHint],
    current_schema: &TabletSchemaPb,
    physical_schema: &TabletSchemaPb,
) -> Result<RecordBatch, String> {
    if output_hints.len() != output_schema.fields().len() {
        return Err(format!(
            "parquet output hint count mismatch: hints={} fields={}",
            output_hints.len(),
            output_schema.fields().len()
        ));
    }
    align_batch_to_output_schema_inner(
        batch,
        output_schema,
        Some((output_hints, current_schema, physical_schema)),
    )
}

fn align_batch_to_output_schema_inner(
    batch: RecordBatch,
    output_schema: &SchemaRef,
    output_hints: Option<(
        &[StarRocksOutputColumnHint],
        &TabletSchemaPb,
        &TabletSchemaPb,
    )>,
) -> Result<RecordBatch, String> {
    let physical_names_by_unique_id = output_hints
        .filter(|(hints, _, _)| hints.iter().any(|hint| hint.schema_unique_id.is_some()))
        .map(|(_, _, physical_schema)| build_physical_names_by_unique_id(physical_schema))
        .transpose()?;
    let mut name_to_index = HashMap::<String, usize>::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized_name = normalize_column_name(field.name());
        if !normalized_name.is_empty() {
            match name_to_index.entry(normalized_name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(idx);
                }
                Entry::Occupied(_) => {
                    return Err(format!(
                        "duplicated parquet Arrow field name after normalization: column_name={normalized_name}"
                    ));
                }
            }
        }
    }

    let mut arrays = Vec::with_capacity(output_schema.fields().len());
    for (idx, field) in output_schema.fields().iter().enumerate() {
        let hint = output_hints.and_then(|(hints, _, _)| hints.get(idx));
        let authoritative_columns = match (hint, output_hints) {
            (
                Some(StarRocksOutputColumnHint {
                    physical_binding:
                        StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(unique_id),
                    ..
                }),
                Some((_, current_schema, physical_schema)),
            ) => {
                let current_column = find_schema_column_by_unique_id(current_schema, *unique_id)
                    .ok_or_else(|| {
                        format!(
                            "current parquet schema column is missing by authoritative identity: output_idx={} output_name={} unique_id={}",
                            idx,
                            field.name(),
                            unique_id
                        )
                    })?;
                validate_authoritative_current_schema_for_output(
                    current_column,
                    field.as_ref(),
                    field.name(),
                )
                .map_err(|err| {
                    format!(
                        "reject parquet schema evolution: output_idx={} output_name={} error={}",
                        idx,
                        field.name(),
                        err
                    )
                })?;
                Some((
                    *unique_id,
                    current_column,
                    find_schema_column_by_unique_id(physical_schema, *unique_id),
                ))
            }
            _ => None,
        };
        let source_idx = match hint.map(|hint| &hint.physical_binding) {
            Some(StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(unique_id)) => {
                physical_names_by_unique_id
                    .as_ref()
                    .and_then(|names| names.get(unique_id))
                    .and_then(|name| name_to_index.get(name))
                    .copied()
            }
            Some(StarRocksPhysicalColumnBinding::LegacyName) => hint
                .and_then(|hint| hint.schema_unique_id)
                .and_then(|unique_id| {
                    physical_names_by_unique_id
                        .as_ref()
                        .and_then(|names| names.get(&unique_id))
                })
                .and_then(|name| name_to_index.get(name))
                .copied()
                .or_else(|| {
                    name_to_index
                        .get(&normalize_column_name(field.name()))
                        .copied()
                }),
            None => name_to_index
                .get(&normalize_column_name(field.name()))
                .copied(),
        };
        let Some(source_idx) = source_idx else {
            let filled = if let Some((unique_id, current_column, physical_column)) =
                authoritative_columns
            {
                if let Some(physical_column) = physical_column {
                    return Err(format!(
                        "physical parquet Arrow column is missing for declared schema column: output_idx={} output_name={} unique_id={} physical_name={:?}",
                        idx,
                        field.name(),
                        unique_id,
                        physical_column.name
                    ));
                }
                build_missing_authoritative_column(
                    current_column,
                    field.data_type(),
                    batch.num_rows(),
                    field.name(),
                )?
            } else if let Some(default_literal) =
                hint.and_then(|hint| hint.fallback_default_literal.as_deref())
            {
                let singleton = crate::connector::starrocks::lake::txn_log::parse_default_literal_to_singleton_array(
                    field.data_type(),
                    default_literal,
                )?;
                let indices = UInt32Array::from(vec![0; batch.num_rows()]);
                take(singleton.as_ref(), &indices, None).map_err(|err| {
                    format!(
                        "repeat parquet default column failed: output_idx={idx} output_name={} error={err}",
                        field.name()
                    )
                })?
            } else if field.is_nullable() {
                new_null_array(field.data_type(), batch.num_rows())
            } else {
                return Err(format!(
                    "parquet output column '{}' not found in source schema and has no default; source_fields={}",
                    field.name(),
                    debug_schema_fields(batch.schema().as_ref())
                ));
            };
            arrays.push(filled);
            continue;
        };
        let src = batch.column(source_idx).clone();
        let out = if let Some((_, current_schema, physical_schema)) = output_hints {
            let (physical_column, current_column) = if let Some((
                _,
                current_column,
                physical_column,
            )) = authoritative_columns
            {
                let physical_column = physical_column.ok_or_else(|| {
                    format!(
                        "physical parquet schema column is missing for alignment: output_idx={} output_name={} source_idx={} source_name={}",
                        idx,
                        field.name(),
                        source_idx,
                        batch.schema().field(source_idx).name()
                    )
                })?;
                (physical_column, current_column)
            } else {
                let physical_column = find_physical_schema_column(
                    hint,
                    batch.schema().field(source_idx).name(),
                    physical_schema,
                )
                .ok_or_else(|| {
                    format!(
                        "physical parquet schema column is missing for alignment: output_idx={} output_name={} source_idx={} source_name={}",
                        idx,
                        field.name(),
                        source_idx,
                        batch.schema().field(source_idx).name()
                    )
                })?;
                let current_column = find_physical_schema_column(hint, field.name(), current_schema)
                    .ok_or_else(|| {
                        format!(
                            "current parquet schema column is missing for alignment: output_idx={} output_name={} source_idx={} source_name={}",
                            idx,
                            field.name(),
                            source_idx,
                            batch.schema().field(source_idx).name()
                        )
                    })?;
                (physical_column, current_column)
            };
            let physical_data_type = derive_physical_arrow_type_for_output(
                physical_column,
                current_column,
                field.data_type(),
                field.name(),
            )
            .map_err(|err| {
                format!(
                    "reject parquet schema evolution: output_idx={} output_name={} source_idx={} error={}",
                    idx,
                    field.name(),
                    source_idx,
                    err
                )
            })?;
            validate_physical_schema_nullability_for_arrow_field(
                physical_column,
                batch.schema().field(source_idx),
                field.name(),
            )
            .map_err(|err| {
                format!(
                    "reject parquet physical schema: output_idx={} output_name={} source_idx={} error={}",
                    idx,
                    field.name(),
                    source_idx,
                    err
                )
            })?;
            if src.data_type() != &physical_data_type {
                return Err(format!(
                    "physical parquet Arrow type does not match tablet schema: output_idx={} output_name={} source_idx={} arrow_type={:?} schema_type={:?}",
                    idx,
                    field.name(),
                    source_idx,
                    src.data_type(),
                    physical_data_type
                ));
            }
            cast_parquet_array_to_output(
                src,
                physical_column,
                current_column,
                field.data_type(),
                field.name(),
            )
            .map_err(|e| {
                format!(
                    "cast parquet column failed: output_idx={} output_name={} source_idx={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    source_idx,
                    physical_data_type,
                    field.data_type(),
                    e
                )
            })?
        } else if src.data_type() == field.data_type() {
            src
        } else {
            cast(src.as_ref(), field.data_type()).map_err(|e| {
                format!(
                    "cast parquet column failed: output_idx={} output_name={} source_idx={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    source_idx,
                    src.data_type(),
                    field.data_type(),
                    e
                )
            })?
        };
        arrays.push(out);
    }
    RecordBatch::try_new(output_schema.clone(), arrays)
        .map_err(|e| format!("build aligned batch failed: {}", e))
}

fn find_schema_column_by_unique_id(schema: &TabletSchemaPb, unique_id: u32) -> Option<&ColumnPb> {
    schema
        .column
        .iter()
        .find(|column| u32::try_from(column.unique_id).ok() == Some(unique_id))
}

fn validate_authoritative_current_schema_for_output(
    current_column: &ColumnPb,
    output_field: &Field,
    output_path: &str,
) -> Result<(), String> {
    derive_physical_arrow_type_for_output(
        current_column,
        current_column,
        output_field.data_type(),
        output_path,
    )?;
    validate_schema_column_nullability_against_arrow_field(
        current_column,
        output_field,
        output_path,
        ParquetNullabilityBoundary::CurrentToOutput,
    )
}

fn validate_physical_schema_nullability_for_arrow_field(
    physical_column: &ColumnPb,
    arrow_field: &Field,
    output_path: &str,
) -> Result<(), String> {
    validate_schema_column_nullability_against_arrow_field(
        physical_column,
        arrow_field,
        output_path,
        ParquetNullabilityBoundary::PhysicalToArrow,
    )
}

#[derive(Clone, Copy)]
enum ParquetNullabilityBoundary {
    CurrentToOutput,
    PhysicalToArrow,
}

fn validate_schema_column_nullability_against_arrow_field(
    schema_column: &ColumnPb,
    arrow_field: &Field,
    output_path: &str,
    boundary: ParquetNullabilityBoundary,
) -> Result<(), String> {
    let schema_nullable = match boundary {
        ParquetNullabilityBoundary::CurrentToOutput => schema_column.is_nullable,
        ParquetNullabilityBoundary::PhysicalToArrow => Some(required_physical_column_nullability(
            schema_column,
            output_path,
        )?),
    };
    if let Some(schema_nullable) = schema_nullable
        && schema_nullable != arrow_field.is_nullable()
    {
        return Err(match boundary {
            ParquetNullabilityBoundary::CurrentToOutput => format!(
                "authoritative current parquet schema nullability does not match output: output_path={output_path}, current_nullable={schema_nullable}, output_nullable={}",
                arrow_field.is_nullable()
            ),
            ParquetNullabilityBoundary::PhysicalToArrow => format!(
                "physical parquet schema nullability does not match Arrow field: output_path={output_path}, physical_nullable={schema_nullable}, arrow_nullable={}",
                arrow_field.is_nullable()
            ),
        });
    }

    match arrow_field.data_type() {
        DataType::Struct(fields) if schema_column.children_columns.len() == fields.len() => {
            for (field, schema_child) in fields.iter().zip(schema_column.children_columns.iter()) {
                validate_schema_column_nullability_against_arrow_field(
                    schema_child,
                    field,
                    &format!("{output_path}.{}", field.name()),
                    boundary,
                )?;
            }
        }
        DataType::List(item_field) if schema_column.children_columns.len() == 1 => {
            validate_schema_column_nullability_against_arrow_field(
                &schema_column.children_columns[0],
                item_field,
                &format!("{output_path}.item"),
                boundary,
            )?;
        }
        DataType::Map(entries_field, _) if schema_column.children_columns.len() == 2 => {
            if let DataType::Struct(entry_fields) = entries_field.data_type()
                && entry_fields.len() == 2
            {
                for (idx, child_name) in ["key", "value"].into_iter().enumerate() {
                    validate_schema_column_nullability_against_arrow_field(
                        &schema_column.children_columns[idx],
                        &entry_fields[idx],
                        &format!("{output_path}.{child_name}"),
                        boundary,
                    )?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn required_physical_column_nullability(
    physical_column: &ColumnPb,
    output_path: &str,
) -> Result<bool, String> {
    physical_column.is_nullable.ok_or_else(|| {
        format!(
            "physical parquet schema nullability is missing: output_path={output_path}, unique_id={}, physical_name={:?}",
            physical_column.unique_id, physical_column.name
        )
    })
}

fn build_missing_authoritative_column(
    current_column: &ColumnPb,
    output_type: &DataType,
    row_count: usize,
    output_name: &str,
) -> Result<ArrayRef, String> {
    if let Some(default_value) = current_column.default_value.as_deref() {
        let default_literal = String::from_utf8_lossy(default_value);
        let singleton =
            crate::connector::starrocks::lake::txn_log::parse_default_literal_to_singleton_array(
                output_type,
                default_literal.as_ref(),
            )?;
        let indices = UInt32Array::from(vec![0; row_count]);
        return take(singleton.as_ref(), &indices, None).map_err(|err| {
            format!("repeat parquet default column failed: output_name={output_name} error={err}")
        });
    }
    match current_column.is_nullable {
        Some(true) => Ok(new_null_array(output_type, row_count)),
        Some(false) => Err(format!(
            "parquet output column '{output_name}' is missing from physical schema and authoritative current schema is non-nullable without default"
        )),
        None => Err(format!(
            "authoritative current parquet schema nullability is missing for backfill: output_name={output_name}, unique_id={}",
            current_column.unique_id
        )),
    }
}

fn find_physical_schema_column<'a>(
    hint: Option<&StarRocksOutputColumnHint>,
    source_name: &str,
    physical_schema: &'a TabletSchemaPb,
) -> Option<&'a ColumnPb> {
    let hinted_unique_id = hint.and_then(|hint| match hint.physical_binding {
        StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(unique_id) => Some(unique_id),
        StarRocksPhysicalColumnBinding::LegacyName => hint.schema_unique_id,
    });
    hinted_unique_id
        .and_then(|unique_id| {
            physical_schema
                .column
                .iter()
                .find(|column| u32::try_from(column.unique_id).ok() == Some(unique_id))
        })
        .or_else(|| {
            let normalized_source_name = normalize_column_name(source_name);
            physical_schema.column.iter().find(|column| {
                column
                    .name
                    .as_deref()
                    .is_some_and(|name| normalize_column_name(name) == normalized_source_name)
            })
        })
}

fn build_physical_names_by_unique_id(
    physical_schema: &TabletSchemaPb,
) -> Result<HashMap<u32, String>, String> {
    let mut names = HashMap::new();
    let mut seen_unique_ids = HashMap::<u32, ()>::new();
    let mut seen_names = HashMap::<String, ()>::new();
    for column in &physical_schema.column {
        let unique_id = u32::try_from(column.unique_id).ok();
        if let Some(unique_id) = unique_id {
            match seen_unique_ids.entry(unique_id) {
                Entry::Vacant(entry) => {
                    entry.insert(());
                }
                Entry::Occupied(_) => {
                    return Err(format!(
                        "duplicated physical parquet column unique_id: unique_id={unique_id}"
                    ));
                }
            }
        }
        let normalized_name = column
            .name
            .as_deref()
            .map(normalize_column_name)
            .filter(|name| !name.is_empty());
        if let Some(normalized_name) = normalized_name.as_ref() {
            match seen_names.entry(normalized_name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(());
                }
                Entry::Occupied(_) => {
                    return Err(format!(
                        "duplicated physical parquet column name: column_name={normalized_name}"
                    ));
                }
            }
        }
        if let (Some(unique_id), Some(normalized_name)) = (unique_id, normalized_name) {
            names.insert(unique_id, normalized_name);
        }
    }
    Ok(names)
}

fn derive_physical_arrow_type_for_output(
    physical_column: &ColumnPb,
    current_column: &ColumnPb,
    output_type: &DataType,
    output_path: &str,
) -> Result<DataType, String> {
    match output_type {
        DataType::Struct(output_fields) => {
            validate_complex_schema_type(physical_column, current_column, "STRUCT", output_type)?;
            let aligned = align_struct_physical_children(
                physical_column,
                current_column,
                output_fields,
                output_path,
            )?;
            let mut physical_fields = vec![None; physical_column.children_columns.len()];
            for ((output_field, current_child), (physical_idx, physical_child)) in output_fields
                .iter()
                .zip(current_column.children_columns.iter())
                .zip(aligned.into_iter())
            {
                let child_path = format!("{output_path}.{}", output_field.name());
                let child_type = derive_physical_arrow_type_for_output(
                    physical_child,
                    current_child,
                    output_field.data_type(),
                    &child_path,
                )?;
                let physical_name = physical_child
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("field_{physical_idx}"));
                physical_fields[physical_idx] = Some(Field::new(
                    physical_name,
                    child_type,
                    required_physical_column_nullability(physical_child, &child_path)?,
                ));
            }
            let physical_fields = physical_fields
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    format!(
                        "STRUCT physical child was not aligned for parquet schema evolution: output_path={output_path}"
                    )
                })?;
            Ok(DataType::Struct(Fields::from(physical_fields)))
        }
        DataType::List(output_item) => {
            validate_complex_schema_type(physical_column, current_column, "ARRAY", output_type)?;
            validate_complex_child_count(physical_column, current_column, 1, output_path)?;
            let child_type = derive_physical_arrow_type_for_output(
                &physical_column.children_columns[0],
                &current_column.children_columns[0],
                output_item.data_type(),
                &format!("{output_path}.item"),
            )?;
            Ok(DataType::List(std::sync::Arc::new(Field::new(
                output_item.name(),
                child_type,
                required_physical_column_nullability(
                    &physical_column.children_columns[0],
                    &format!("{output_path}.item"),
                )?,
            ))))
        }
        DataType::Map(output_entries, ordered) => {
            validate_complex_schema_type(physical_column, current_column, "MAP", output_type)?;
            validate_complex_child_count(physical_column, current_column, 2, output_path)?;
            let DataType::Struct(output_entry_fields) = output_entries.data_type() else {
                return Err(format!(
                    "MAP output entries must be a struct for parquet schema evolution: output_path={output_path}, output_type={output_type:?}"
                ));
            };
            if output_entry_fields.len() != 2 {
                return Err(format!(
                    "MAP output entry count mismatch for parquet schema evolution: output_path={output_path}, output_entries={}, expected=2",
                    output_entry_fields.len()
                ));
            }
            let mut physical_entry_fields = Vec::with_capacity(2);
            for (idx, child_name) in ["key", "value"].into_iter().enumerate() {
                let child_type = derive_physical_arrow_type_for_output(
                    &physical_column.children_columns[idx],
                    &current_column.children_columns[idx],
                    output_entry_fields[idx].data_type(),
                    &format!("{output_path}.{child_name}"),
                )?;
                physical_entry_fields.push(Field::new(
                    output_entry_fields[idx].name(),
                    child_type,
                    required_physical_column_nullability(
                        &physical_column.children_columns[idx],
                        &format!("{output_path}.{child_name}"),
                    )?,
                ));
            }
            Ok(DataType::Map(
                std::sync::Arc::new(Field::new(
                    output_entries.name(),
                    DataType::Struct(Fields::from(physical_entry_fields)),
                    output_entries.is_nullable(),
                )),
                *ordered,
            ))
        }
        _ => {
            if !physical_column.children_columns.is_empty()
                || !current_column.children_columns.is_empty()
            {
                return Err(format!(
                    "scalar parquet schema column has children: output_path={output_path}, physical_children={}, current_children={}",
                    physical_column.children_columns.len(),
                    current_column.children_columns.len()
                ));
            }
            let current_type = validate_current_schema_matches_output_type(
                current_column,
                output_type,
                output_path,
            )?;
            validate_physical_schema_to_output_type(physical_column, &current_type)
                .map_err(|err| format!("output_path={output_path}: {err}"))
        }
    }
}

fn validate_current_schema_matches_output_type(
    current_column: &ColumnPb,
    output_type: &DataType,
    output_path: &str,
) -> Result<DataType, String> {
    let current_type = validate_physical_schema_to_output_type(current_column, output_type)
        .map_err(|_| {
            format!(
                "authoritative current parquet schema does not match output: output_path={output_path}, current_type={}, output_type={output_type:?}",
                current_column.r#type.trim().to_ascii_uppercase()
            )
        })?;
    if &current_type != output_type {
        return Err(format!(
            "authoritative current parquet schema does not match output: output_path={output_path}, current_type={}, output_type={output_type:?}",
            current_column.r#type.trim().to_ascii_uppercase()
        ));
    }
    Ok(current_type)
}

fn validate_complex_schema_type(
    physical_column: &ColumnPb,
    current_column: &ColumnPb,
    expected_type: &str,
    output_type: &DataType,
) -> Result<(), String> {
    let physical_type = physical_column.r#type.trim().to_ascii_uppercase();
    let current_type = current_column.r#type.trim().to_ascii_uppercase();
    if physical_type != expected_type || current_type != expected_type {
        return Err(format!(
            "unsupported StarRocks schema evolution: physical_type={physical_type}, current_type={current_type}, output_type={output_type:?}; supported=same complex type with same leaf type or signed integer widening"
        ));
    }
    Ok(())
}

fn validate_complex_child_count(
    physical_column: &ColumnPb,
    current_column: &ColumnPb,
    expected: usize,
    output_path: &str,
) -> Result<(), String> {
    if physical_column.children_columns.len() != expected
        || current_column.children_columns.len() != expected
    {
        return Err(format!(
            "complex parquet schema child count mismatch: output_path={output_path}, physical_children={}, current_children={}, expected={expected}",
            physical_column.children_columns.len(),
            current_column.children_columns.len()
        ));
    }
    Ok(())
}

fn nonnegative_schema_column_unique_id(column: &ColumnPb) -> Option<u32> {
    u32::try_from(column.unique_id).ok()
}

fn build_complex_children_by_unique_id<'a>(
    children: &'a [ColumnPb],
    schema_role: &str,
    output_path: &str,
) -> Result<std::collections::HashMap<u32, (usize, &'a ColumnPb)>, String> {
    let mut by_unique_id = std::collections::HashMap::new();
    for (idx, child) in children.iter().enumerate() {
        let Some(unique_id) = nonnegative_schema_column_unique_id(child) else {
            continue;
        };
        if by_unique_id.insert(unique_id, (idx, child)).is_some() {
            return Err(format!(
                "duplicated {schema_role} parquet STRUCT child unique_id: output_path={output_path}, unique_id={unique_id}"
            ));
        }
    }
    Ok(by_unique_id)
}

fn align_struct_physical_children<'a>(
    physical_column: &'a ColumnPb,
    current_column: &ColumnPb,
    output_fields: &Fields,
    output_path: &str,
) -> Result<Vec<(usize, &'a ColumnPb)>, String> {
    if current_column.children_columns.len() != output_fields.len()
        || physical_column.children_columns.len() != current_column.children_columns.len()
    {
        return Err(format!(
            "STRUCT parquet schema child count mismatch: output_path={output_path}, physical_children={}, current_children={}, output_fields={}",
            physical_column.children_columns.len(),
            current_column.children_columns.len(),
            output_fields.len()
        ));
    }
    let physical_by_unique_id = build_complex_children_by_unique_id(
        &physical_column.children_columns,
        "physical",
        output_path,
    )?;
    build_complex_children_by_unique_id(&current_column.children_columns, "current", output_path)?;

    let mut matched_physical_indexes = vec![false; physical_column.children_columns.len()];
    let mut aligned = Vec::with_capacity(output_fields.len());
    for (output_field, current_child) in output_fields
        .iter()
        .zip(current_column.children_columns.iter())
    {
        if let Some(current_name) = current_child
            .name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            && normalize_column_name(current_name) != normalize_column_name(output_field.name())
        {
            return Err(format!(
                "current parquet STRUCT child name does not match output field: output_path={output_path}, current_name={current_name}, output_name={}",
                output_field.name()
            ));
        }

        let matched = if let Some(unique_id) = nonnegative_schema_column_unique_id(current_child) {
            physical_by_unique_id.get(&unique_id).copied()
        } else {
            let current_name = current_child
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| output_field.name());
            let normalized_current_name = normalize_column_name(current_name);
            let mut matches = physical_column.children_columns.iter().enumerate().filter(
                |(idx, physical_child)| {
                    !matched_physical_indexes[*idx]
                        && physical_child.name.as_deref().is_some_and(|name| {
                            normalize_column_name(name) == normalized_current_name
                        })
                },
            );
            let matched = matches.next();
            if matches.next().is_some() {
                return Err(format!(
                    "ambiguous legacy parquet STRUCT child name: output_path={output_path}, child_name={current_name}"
                ));
            }
            matched
        };
        let Some((physical_idx, physical_child)) = matched else {
            return Err(format!(
                "physical parquet STRUCT child is missing for current output: output_path={output_path}, output_child={}, current_unique_id={}",
                output_field.name(),
                current_child.unique_id
            ));
        };
        if matched_physical_indexes[physical_idx] {
            return Err(format!(
                "physical parquet STRUCT child matched more than once: output_path={output_path}, physical_index={physical_idx}"
            ));
        }
        matched_physical_indexes[physical_idx] = true;
        aligned.push((physical_idx, physical_child));
    }
    Ok(aligned)
}

fn cast_parquet_array_to_output(
    source: ArrayRef,
    physical_column: &ColumnPb,
    current_column: &ColumnPb,
    output_type: &DataType,
    output_path: &str,
) -> Result<ArrayRef, String> {
    if source.data_type() == output_type {
        return Ok(source);
    }
    match output_type {
        DataType::Struct(output_fields) => {
            let source_struct = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| format!("expected STRUCT array at {output_path}"))?;
            let aligned = align_struct_physical_children(
                physical_column,
                current_column,
                output_fields,
                output_path,
            )?;
            let mut output_children = Vec::with_capacity(output_fields.len());
            for ((output_field, current_child), (physical_idx, physical_child)) in output_fields
                .iter()
                .zip(current_column.children_columns.iter())
                .zip(aligned.into_iter())
            {
                output_children.push(cast_parquet_array_to_output(
                    source_struct.column(physical_idx).clone(),
                    physical_child,
                    current_child,
                    output_field.data_type(),
                    &format!("{output_path}.{}", output_field.name()),
                )?);
            }
            let output = StructArray::try_new(
                output_fields.clone(),
                output_children,
                source_struct.nulls().cloned(),
            )
            .map_err(|err| format!("rebuild STRUCT array failed at {output_path}: {err}"))?;
            Ok(std::sync::Arc::new(output))
        }
        DataType::List(output_item) => {
            let source_list = source
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| format!("expected ARRAY list at {output_path}"))?;
            let output_values = cast_parquet_array_to_output(
                source_list.values().clone(),
                &physical_column.children_columns[0],
                &current_column.children_columns[0],
                output_item.data_type(),
                &format!("{output_path}.item"),
            )?;
            let output = ListArray::try_new(
                output_item.clone(),
                source_list.offsets().clone(),
                output_values,
                source_list.nulls().cloned(),
            )
            .map_err(|err| format!("rebuild ARRAY list failed at {output_path}: {err}"))?;
            Ok(std::sync::Arc::new(output))
        }
        DataType::Map(output_entries, ordered) => {
            let source_map = source
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| format!("expected MAP array at {output_path}"))?;
            let DataType::Struct(output_entry_fields) = output_entries.data_type() else {
                return Err(format!("expected MAP entry struct at {output_path}"));
            };
            let mut output_children = Vec::with_capacity(2);
            for (idx, child_name) in ["key", "value"].into_iter().enumerate() {
                output_children.push(cast_parquet_array_to_output(
                    source_map.entries().column(idx).clone(),
                    &physical_column.children_columns[idx],
                    &current_column.children_columns[idx],
                    output_entry_fields[idx].data_type(),
                    &format!("{output_path}.{child_name}"),
                )?);
            }
            let output_entry_array = StructArray::try_new(
                output_entry_fields.clone(),
                output_children,
                source_map.entries().nulls().cloned(),
            )
            .map_err(|err| format!("rebuild MAP entries failed at {output_path}: {err}"))?;
            let output = MapArray::try_new(
                output_entries.clone(),
                source_map.offsets().clone(),
                output_entry_array,
                source_map.nulls().cloned(),
                *ordered,
            )
            .map_err(|err| format!("rebuild MAP array failed at {output_path}: {err}"))?;
            Ok(std::sync::Arc::new(output))
        }
        _ => cast(source.as_ref(), output_type)
            .map_err(|err| format!("cast scalar array failed at {output_path}: {err}")),
    }
}

fn normalize_column_name(name: &str) -> String {
    name.trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn debug_schema_fields(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| format!("#{idx}:{}:{:?}", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn concat_batches(
    output_schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<Option<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(None);
    }
    let num_cols = output_schema.fields().len();
    let mut by_col: Vec<Vec<ArrayRef>> = (0..num_cols).map(|_| Vec::new()).collect();
    let mut total_rows = 0usize;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows = total_rows.saturating_add(batch.num_rows());
        for (col_idx, columns) in by_col.iter_mut().enumerate().take(num_cols) {
            columns.push(batch.column(col_idx).clone());
        }
    }
    if total_rows == 0 {
        return Ok(None);
    }

    let mut merged = Vec::with_capacity(num_cols);
    for arrays in by_col {
        if arrays.is_empty() {
            return Err("empty column arrays while concatenating".to_string());
        }
        if arrays.len() == 1 {
            merged.push(arrays[0].clone());
            continue;
        }
        let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let arr = concat(&refs).map_err(|e| format!("concat arrays failed: {}", e))?;
        merged.push(arr);
    }

    let out = RecordBatch::try_new(output_schema, merged)
        .map_err(|e| format!("build batch failed: {}", e))?;
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::formats::starrocks::metadata::{
        StarRocksDelvecMetaRaw, StarRocksSegmentFile, StarRocksTabletSnapshot,
    };
    use crate::formats::starrocks::plan::{
        StarRocksOutputColumnHint, StarRocksPhysicalColumnBinding,
    };
    use crate::service::grpc_client::proto::starrocks::{ColumnPb, TabletSchemaPb};
    use arrow::array::{
        BooleanArray, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    fn parquet_provenance_snapshot(current_schema_id: i64) -> StarRocksTabletSnapshot {
        StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: TabletSchemaPb {
                id: Some(current_schema_id),
                ..Default::default()
            },
            historical_schemas: std::collections::BTreeMap::new(),
            total_num_rows: 0,
            rowset_count: 1,
            segment_files: Vec::new(),
            delete_predicates: Vec::new(),
            delvec_meta: Default::default(),
        }
    }

    fn parquet_provenance_segment(schema_id: Option<i64>) -> StarRocksSegmentFile {
        StarRocksSegmentFile {
            name: "segment.parquet".to_string(),
            relative_path: "data/segment.parquet".to_string(),
            path: "/tmp/segment.parquet".to_string(),
            rowset_version: 7,
            schema_id,
            segment_id: Some(0),
            bundle_file_offset: None,
            segment_size: None,
        }
    }

    #[test]
    fn schema_provenance_parquet_rejects_nonpositive_ids() {
        let snapshot = parquet_provenance_snapshot(30);
        let physical_fallback = TabletSchemaPb {
            id: Some(29),
            ..Default::default()
        };

        for schema_id in [0, -1] {
            let err = resolve_segment_source_schema(
                &snapshot,
                &parquet_provenance_segment(Some(schema_id)),
                &physical_fallback,
            )
            .expect_err("explicit nonpositive schema ID must fail");
            assert!(
                err.contains("segment rowset schema id must be positive")
                    && err.contains(&format!("schema_id={schema_id}")),
                "{err}"
            );
        }
    }

    #[test]
    fn schema_provenance_parquet_rejects_historical_embedded_id_mismatch() {
        let mut snapshot = parquet_provenance_snapshot(30);
        snapshot.historical_schemas.insert(
            29,
            TabletSchemaPb {
                id: Some(28),
                ..Default::default()
            },
        );
        let physical_fallback = TabletSchemaPb {
            id: Some(29),
            ..Default::default()
        };

        let err = resolve_segment_source_schema(
            &snapshot,
            &parquet_provenance_segment(Some(29)),
            &physical_fallback,
        )
        .expect_err("historical map key and embedded schema ID must agree");

        assert!(
            err.contains("resolved tablet schema id mismatch")
                && err.contains("schema_id=29")
                && err.contains("resolved_schema_id=Some(28)"),
            "{err}"
        );
    }

    #[test]
    fn schema_provenance_parquet_accepts_positive_refreshed_current_id() {
        let snapshot = parquet_provenance_snapshot(30);
        let physical_fallback = TabletSchemaPb {
            id: Some(29),
            ..Default::default()
        };

        let resolved = resolve_segment_source_schema(
            &snapshot,
            &parquet_provenance_segment(Some(30)),
            &physical_fallback,
        )
        .expect("positive refreshed-current schema ID must resolve");

        assert!(std::ptr::eq(resolved, &snapshot.tablet_schema));
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ],
        )
        .expect("build sample batch")
    }

    fn single_boolean_parquet_snapshot(
        physical_name: &str,
        unique_id: i32,
        values: Vec<bool>,
    ) -> (tempfile::TempDir, StarRocksTabletSnapshot) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("old.parquet");
        let path = path.to_string_lossy().to_string();
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            physical_name,
            DataType::Boolean,
            false,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(BooleanArray::from(values)) as ArrayRef],
        )
        .expect("old parquet batch");
        write_parquet_file(&path, &source).expect("write old parquet");
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: TabletSchemaPb {
                column: vec![ColumnPb {
                    unique_id,
                    name: Some(physical_name.to_string()),
                    r#type: "BOOLEAN".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            historical_schemas: Default::default(),
            total_num_rows: 2,
            rowset_count: 1,
            segment_files: vec![StarRocksSegmentFile {
                name: "old.parquet".to_string(),
                relative_path: "old.parquet".to_string(),
                path,
                rowset_version: 20,
                schema_id: None,
                segment_id: Some(0),
                bundle_file_offset: None,
                segment_size: None,
            }],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        (temp_dir, snapshot)
    }

    fn boolean_tablet_schema(schema_id: i64, name: &str, unique_id: i32) -> TabletSchemaPb {
        TabletSchemaPb {
            id: Some(schema_id),
            column: vec![ColumnPb {
                unique_id,
                name: Some(name.to_string()),
                r#type: "BOOLEAN".to_string(),
                is_nullable: Some(false),
                visible: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn scalar_tablet_schema(unique_id: i32, name: &str, schema_type: &str) -> TabletSchemaPb {
        TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id,
                name: Some(name.to_string()),
                r#type: schema_type.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn real_parquet_snapshot_with_schemas(
        batch: RecordBatch,
        current_schema: TabletSchemaPb,
        physical_schema: TabletSchemaPb,
    ) -> (tempfile::TempDir, StarRocksTabletSnapshot) {
        let temp_dir = tempfile::tempdir().expect("create real parquet temp dir");
        let path = temp_dir.path().join("identity.parquet");
        let path = path.to_string_lossy().to_string();
        write_parquet_file(&path, &batch).expect("write real parquet identity fixture");
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: current_schema,
            historical_schemas: std::collections::BTreeMap::from([(10, physical_schema)]),
            total_num_rows: batch.num_rows() as u64,
            rowset_count: 1,
            segment_files: vec![StarRocksSegmentFile {
                name: "identity.parquet".to_string(),
                relative_path: "identity.parquet".to_string(),
                path,
                rowset_version: 20,
                schema_id: Some(10),
                segment_id: Some(0),
                bundle_file_offset: None,
                segment_size: None,
            }],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        (temp_dir, snapshot)
    }

    fn align_real_scalar_parquet_nullability(
        arrow_nullable: bool,
        physical_nullable: Option<bool>,
        current_nullable: bool,
        output_nullable: bool,
    ) -> Result<RecordBatch, String> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::Int32,
                arrow_nullable,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("real scalar nullability parquet batch");
        let current_schema = TabletSchemaPb {
            id: Some(20),
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(current_nullable),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: physical_nullable,
                ..Default::default()
            }],
            ..Default::default()
        };
        let (_temp_dir, snapshot) =
            real_parquet_snapshot_with_schemas(batch, current_schema, physical_schema);
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];

        read_bundle_parquet_snapshot_with_output_hints_if_any(
            &snapshot,
            Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::Int32,
                output_nullable,
            )])),
            &hints,
        )?
        .ok_or_else(|| "real scalar nullability parquet batch is missing".to_string())
    }

    fn read_real_parquet_with_schema_id_none_after_refresh(
        physical_schema_id: i64,
        segment_schema_id: Option<i64>,
        empty: bool,
    ) -> Result<Option<RecordBatch>, String> {
        let temp_dir = tempfile::tempdir().expect("create schema-id-none parquet temp dir");
        let path = temp_dir.path().join("legacy-int.parquet");
        let path = path.to_string_lossy().to_string();
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = if empty {
            RecordBatch::new_empty(arrow_schema)
        } else {
            RecordBatch::try_new(
                arrow_schema,
                vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
            )
            .expect("legacy INT parquet batch")
        };
        write_parquet_file(&path, &batch).expect("write schema-id-none legacy parquet");

        let physical_schema = TabletSchemaPb {
            id: Some(physical_schema_id),
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(false),
                ..Default::default()
            }],
            ..Default::default()
        };
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: TabletSchemaPb {
                id: Some(30),
                column: vec![ColumnPb {
                    unique_id: 11,
                    name: Some("v".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(false),
                    ..Default::default()
                }],
                ..Default::default()
            },
            historical_schemas: std::collections::BTreeMap::new(),
            total_num_rows: batch.num_rows() as u64,
            rowset_count: 1,
            segment_files: vec![StarRocksSegmentFile {
                name: "legacy-int.parquet".to_string(),
                relative_path: "legacy-int.parquet".to_string(),
                path,
                rowset_version: 20,
                schema_id: segment_schema_id,
                segment_id: Some(0),
                bundle_file_offset: None,
                segment_size: None,
            }],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];

        read_bundle_parquet_snapshot_with_output_hints_and_physical_schema_if_any(
            &snapshot,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            &hints,
            &physical_schema,
        )
    }

    #[derive(Clone, Copy, Debug)]
    enum NestedShape {
        Struct,
        Array,
        Map,
    }

    fn nested_column(
        shape: NestedShape,
        leaf_name: &str,
        leaf_unique_id: i32,
        leaf_type: &str,
    ) -> ColumnPb {
        nested_column_with_leaf_nullability(
            shape,
            leaf_name,
            leaf_unique_id,
            leaf_type,
            Some(false),
        )
    }

    fn nested_column_with_leaf_nullability(
        shape: NestedShape,
        leaf_name: &str,
        leaf_unique_id: i32,
        leaf_type: &str,
        leaf_nullable: Option<bool>,
    ) -> ColumnPb {
        let leaf = ColumnPb {
            unique_id: leaf_unique_id,
            name: Some(leaf_name.to_string()),
            r#type: leaf_type.to_string(),
            is_nullable: leaf_nullable,
            ..Default::default()
        };
        let (root_type, children_columns) = match shape {
            NestedShape::Struct => ("STRUCT", vec![leaf]),
            NestedShape::Array => ("ARRAY", vec![leaf]),
            NestedShape::Map => (
                "MAP",
                vec![
                    ColumnPb {
                        unique_id: 100,
                        name: Some("key".to_string()),
                        r#type: "INT".to_string(),
                        is_nullable: Some(false),
                        ..Default::default()
                    },
                    leaf,
                ],
            ),
        };
        ColumnPb {
            unique_id: 11,
            name: Some("payload".to_string()),
            r#type: root_type.to_string(),
            is_nullable: Some(false),
            children_columns,
            ..Default::default()
        }
    }

    fn nested_data_type(shape: NestedShape, leaf_name: &str, leaf_type: DataType) -> DataType {
        nested_data_type_with_leaf_nullability(shape, leaf_name, leaf_type, false)
    }

    fn nested_data_type_with_leaf_nullability(
        shape: NestedShape,
        leaf_name: &str,
        leaf_type: DataType,
        leaf_nullable: bool,
    ) -> DataType {
        match shape {
            NestedShape::Struct => DataType::Struct(Fields::from(vec![Field::new(
                leaf_name,
                leaf_type,
                leaf_nullable,
            )])),
            NestedShape::Array => {
                DataType::List(Arc::new(Field::new("item", leaf_type, leaf_nullable)))
            }
            NestedShape::Map => {
                let entries = Fields::from(vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", leaf_type, leaf_nullable),
                ]);
                DataType::Map(
                    Arc::new(Field::new("entries", DataType::Struct(entries), false)),
                    false,
                )
            }
        }
    }

    fn nested_leaf_array(data_type: &DataType) -> ArrayRef {
        match data_type {
            DataType::Int32 => Arc::new(Int32Array::from(vec![1_i32, 2_i32])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            DataType::Utf8 => Arc::new(StringArray::from(vec!["1", "2"])),
            other => panic!("unsupported nested test leaf type: {other:?}"),
        }
    }

    fn nested_array(data_type: &DataType) -> ArrayRef {
        match data_type {
            DataType::Struct(fields) => Arc::new(
                StructArray::try_new(
                    fields.clone(),
                    fields
                        .iter()
                        .map(|field| nested_leaf_array(field.data_type()))
                        .collect(),
                    None,
                )
                .expect("nested STRUCT array"),
            ),
            DataType::List(item_field) => Arc::new(
                ListArray::try_new(
                    item_field.clone(),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2_i32])),
                    nested_leaf_array(item_field.data_type()),
                    None,
                )
                .expect("nested ARRAY array"),
            ),
            DataType::Map(entries_field, ordered) => {
                let DataType::Struct(entry_fields) = entries_field.data_type() else {
                    panic!("nested MAP entries must be a struct")
                };
                let entries = StructArray::try_new(
                    entry_fields.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![10_i32, 20_i32])) as ArrayRef,
                        nested_leaf_array(entry_fields[1].data_type()),
                    ],
                    None,
                )
                .expect("nested MAP entries");
                Arc::new(
                    MapArray::try_new(
                        entries_field.clone(),
                        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2_i32])),
                        entries,
                        None,
                        *ordered,
                    )
                    .expect("nested MAP array"),
                )
            }
            other => panic!("unsupported nested test type: {other:?}"),
        }
    }

    fn align_nested_parquet(
        source_type: DataType,
        physical_column: ColumnPb,
        output_type: DataType,
        current_column: ColumnPb,
    ) -> Result<RecordBatch, String> {
        let temp_dir = tempfile::tempdir().expect("create nested parquet temp dir");
        let path = temp_dir.path().join("nested.parquet");
        let path = path.to_string_lossy().to_string();
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                source_type.clone(),
                false,
            )])),
            vec![nested_array(&source_type)],
        )
        .expect("nested parquet source batch");
        write_parquet_file(&path, &source).expect("write nested parquet source");

        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: TabletSchemaPb {
                id: Some(20),
                column: vec![current_column],
                ..Default::default()
            },
            historical_schemas: std::collections::BTreeMap::from([(
                10,
                TabletSchemaPb {
                    id: Some(10),
                    column: vec![physical_column],
                    ..Default::default()
                },
            )]),
            total_num_rows: 1,
            rowset_count: 1,
            segment_files: vec![StarRocksSegmentFile {
                name: "nested.parquet".to_string(),
                relative_path: "nested.parquet".to_string(),
                path,
                rowset_version: 20,
                schema_id: Some(10),
                segment_id: Some(0),
                bundle_file_offset: None,
                segment_size: None,
            }],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];

        read_bundle_parquet_snapshot_with_output_hints_if_any(
            &snapshot,
            Arc::new(Schema::new(vec![Field::new("payload", output_type, false)])),
            &hints,
        )?
        .ok_or_else(|| "nested parquet batch is missing".to_string())
    }

    #[test]
    fn parquet_helpers_round_trip_local_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir
            .path()
            .join("nested")
            .join("data.parquet")
            .to_string_lossy()
            .to_string();
        let batch = sample_batch();

        let size = write_parquet_file(&path, &batch).expect("write parquet");
        let batches = read_parquet_file(&path).expect("read parquet");

        assert!(size > 0);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let keys = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        assert_eq!(keys.value(0), 1);
        assert_eq!(keys.value(1), 2);
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        assert_eq!(values.value(0), "a");
        assert!(values.is_null(1));
    }

    #[test]
    fn parquet_helpers_use_format_path_resolver_for_object_store_credentials() {
        let _guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let path = "s3://missing-bucket/warehouse/tablet-1/data.parquet";

        let read_err = read_parquet_file(path).expect_err("missing runtime S3 config must fail");
        assert!(
            read_err.contains("missing S3 config for StarRocks object-store path="),
            "{read_err}"
        );

        let batch = sample_batch();
        let write_err =
            write_parquet_file(path, &batch).expect_err("missing runtime S3 config must fail");
        assert!(
            write_err.contains("missing S3 config for StarRocks object-store path="),
            "{write_err}"
        );
    }

    #[test]
    fn parquet_helpers_reject_malformed_hdfs_with_function_specific_errors() {
        let path = "hdfs://nn:9000";

        let batch = sample_batch();
        let write_err = write_parquet_file(path, &batch).expect_err("hdfs parquet write must fail");
        assert!(
            write_err.contains("write_parquet_file does not support hdfs path yet"),
            "{write_err}"
        );

        let read_err = read_parquet_file(path).expect_err("hdfs parquet read must fail");
        assert!(
            read_err.contains("read_parquet_file does not support hdfs path yet"),
            "{read_err}"
        );
    }

    #[test]
    fn parquet_alignment_backfills_missing_current_column_from_native_hint() {
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let hints = vec![
            StarRocksOutputColumnHint {
                schema_unique_id: None,
                physical_binding: StarRocksPhysicalColumnBinding::LegacyName,
                fallback_default_literal: None,
            },
            StarRocksOutputColumnHint {
                schema_unique_id: Some(0),
                physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(0),
                fallback_default_literal: Some("false".to_string()),
            },
        ];

        let current_schema = TabletSchemaPb {
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k".to_string()),
                    r#type: "INT".to_string(),
                    is_nullable: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 0,
                    name: Some("flag".to_string()),
                    r#type: "BOOLEAN".to_string(),
                    is_nullable: Some(false),
                    default_value: Some(b"false".to_vec()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("k".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(false),
                ..Default::default()
            }],
            ..Default::default()
        };
        let aligned = align_batch_to_output_schema_with_hints_and_current_schema(
            sample_batch(),
            &output_schema,
            &hints,
            &current_schema,
            &physical_schema,
        )
        .expect("native hint must backfill missing parquet column");
        let flags = aligned
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean default column");
        assert_eq!(flags.len(), 2);
        assert!(!flags.value(0));
        assert!(!flags.value(1));
    }

    #[test]
    fn parquet_snapshot_does_not_bind_same_named_historical_unique_id() {
        let (_temp_dir, mut snapshot) =
            single_boolean_parquet_snapshot("flag", 11, vec![true, true]);
        snapshot.tablet_schema = boolean_tablet_schema(20, "flag", 12);
        snapshot.tablet_schema.column[0].default_value = Some(b"false".to_vec());
        snapshot
            .historical_schemas
            .insert(10, boolean_tablet_schema(10, "flag", 11));
        snapshot.segment_files[0].schema_id = Some(10);
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: Some("false".to_string()),
        }];

        let aligned =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect("new authoritative column must be backfilled")
                .expect("parquet batch");
        let flags = aligned
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean default column");
        assert!(!flags.value(0));
        assert!(!flags.value(1));
    }

    #[test]
    fn parquet_snapshot_rejects_missing_non_nullable_authoritative_column_without_current_default()
    {
        let (_temp_dir, mut snapshot) =
            single_boolean_parquet_snapshot("old_flag", 11, vec![true, false]);
        snapshot.tablet_schema = boolean_tablet_schema(20, "flag", 12);
        snapshot
            .historical_schemas
            .insert(10, boolean_tablet_schema(10, "old_flag", 11));
        snapshot.segment_files[0].schema_id = Some(10);
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];

        let err =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect_err("missing non-nullable current column without default must fail fast");

        assert!(
            err.contains("authoritative current schema is non-nullable without default"),
            "{err}"
        );
    }

    #[test]
    fn parquet_snapshot_binds_historical_renamed_column_by_authoritative_unique_id() {
        let (_temp_dir, mut snapshot) =
            single_boolean_parquet_snapshot("old_flag", 11, vec![true, false]);
        snapshot.tablet_schema = boolean_tablet_schema(20, "new_flag", 11);
        snapshot
            .historical_schemas
            .insert(10, boolean_tablet_schema(10, "old_flag", 11));
        snapshot.segment_files[0].schema_id = Some(10);
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "new_flag",
            DataType::Boolean,
            false,
        )]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];

        let aligned =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect("renamed column must bind by authoritative unique id")
                .expect("parquet batch");
        let flags = aligned
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("renamed boolean column");
        assert!(flags.value(0));
        assert!(!flags.value(1));
    }

    #[test]
    fn parquet_alignment_rejects_bigint_to_int_narrowing_from_physical_schema() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef],
        )
        .expect("BIGINT parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "BIGINT".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &scalar_tablet_schema(11, "v", "INT"),
            &physical_schema,
        )
        .expect_err("BIGINT to INT parquet narrowing must fail at the schema boundary");

        assert!(err.contains("signed integer widening"), "{err}");
    }

    #[test]
    fn parquet_alignment_rejects_varchar_to_int_cross_family_cast() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec!["1", "2"])) as ArrayRef],
        )
        .expect("VARCHAR parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "VARCHAR".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &scalar_tablet_schema(11, "v", "INT"),
            &physical_schema,
        )
        .expect_err("VARCHAR to INT parquet cast must fail at the schema boundary");

        assert!(err.contains("signed integer widening"), "{err}");
    }

    #[test]
    fn parquet_alignment_rejects_int_fast_path_when_physical_schema_is_bigint() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("INT parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "BIGINT".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &scalar_tablet_schema(11, "v", "INT"),
            &physical_schema,
        )
        .expect_err("physical BIGINT must reject an INT Arrow fast path");

        assert!(err.contains("signed integer widening"), "{err}");
    }

    #[test]
    fn parquet_alignment_rejects_int_fast_path_when_physical_schema_is_varchar() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("INT parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "VARCHAR".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &scalar_tablet_schema(11, "v", "INT"),
            &physical_schema,
        )
        .expect_err("physical VARCHAR must reject an INT Arrow fast path");

        assert!(err.contains("signed integer widening"), "{err}");
    }

    #[test]
    fn parquet_alignment_allows_int_to_bigint_widening_from_physical_schema() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("INT parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(false),
                ..Default::default()
            }],
            ..Default::default()
        };

        let aligned = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &scalar_tablet_schema(11, "v", "BIGINT"),
            &physical_schema,
        )
        .expect("physical INT must widen to BIGINT");
        let values = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT output column");

        assert_eq!(values.values(), &[1_i64, 2_i64]);
    }

    #[test]
    fn parquet_alignment_rejects_current_bigint_with_stale_int_output() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("historical INT parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let current_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "BIGINT".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &current_schema,
            &physical_schema,
        )
        .expect_err("authoritative current BIGINT must reject stale INT output metadata");

        assert!(
            err.contains("authoritative current parquet schema does not match output")
                && err.contains("current_type=BIGINT")
                && err.contains("output_type=Int32"),
            "{err}"
        );
    }

    #[test]
    fn parquet_alignment_rejects_current_varchar_with_int_output() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("historical INT parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];
        let physical_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let current_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "VARCHAR".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &current_schema,
            &physical_schema,
        )
        .expect_err("authoritative current/output cross-family mismatch must fail fast");

        assert!(
            err.contains("authoritative current parquet schema does not match output")
                && err.contains("current_type=VARCHAR")
                && err.contains("output_type=Int32"),
            "{err}"
        );
    }

    #[test]
    fn parquet_alignment_missing_source_rejects_stale_current_bigint_output_int() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "old_v",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("historical parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];
        let current_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "BIGINT".to_string(),
                is_nullable: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = scalar_tablet_schema(11, "old_v", "INT");

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &current_schema,
            &physical_schema,
        )
        .expect_err("missing physical UID must not bypass stale current/output validation");

        assert!(
            err.contains("authoritative current parquet schema does not match output")
                && err.contains("current_type=BIGINT")
                && err.contains("output_type=Int32"),
            "{err}"
        );
    }

    #[test]
    fn parquet_alignment_missing_source_rejects_current_varchar_output_int() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "old_v",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("historical parquet batch");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];
        let current_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "VARCHAR".to_string(),
                is_nullable: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = scalar_tablet_schema(11, "old_v", "INT");

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &current_schema,
            &physical_schema,
        )
        .expect_err("missing physical UID must not bypass current/output family validation");

        assert!(
            err.contains("authoritative current parquet schema does not match output")
                && err.contains("current_type=VARCHAR")
                && err.contains("output_type=Int32"),
            "{err}"
        );
    }

    #[test]
    fn parquet_alignment_missing_source_rejects_declared_physical_uid_without_arrow_field() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("parquet batch without declared physical field");
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];
        let current_schema = TabletSchemaPb {
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = scalar_tablet_schema(12, "declared_v", "INT");

        let err = align_batch_to_output_schema_with_hints_and_current_schema(
            batch,
            &output_schema,
            &hints,
            &current_schema,
            &physical_schema,
        )
        .expect_err("declared physical UID without Arrow field is metadata drift, not backfill");

        assert!(
            err.contains("physical parquet Arrow column is missing for declared schema column")
                && err.contains("unique_id=12"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_nullable_output_for_non_nullable_current_column() {
        let err = align_real_scalar_parquet_nullability(false, Some(false), false, true)
            .expect_err("nullable output must not widen non-nullable current metadata");

        assert!(
            err.contains("authoritative current parquet schema nullability does not match output")
                && err.contains("current_nullable=false")
                && err.contains("output_nullable=true"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_non_nullable_output_for_nullable_current_column() {
        let err = align_real_scalar_parquet_nullability(true, Some(true), true, false)
            .expect_err("non-nullable output must not narrow nullable current metadata");

        assert!(
            err.contains("authoritative current parquet schema nullability does not match output")
                && err.contains("current_nullable=true")
                && err.contains("output_nullable=false"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_physical_arrow_top_level_nullability_drift() {
        let err = align_real_scalar_parquet_nullability(true, Some(false), false, false)
            .expect_err("physical metadata/Arrow field nullability drift must fail");

        assert!(
            err.contains("physical parquet schema nullability does not match Arrow field")
                && err.contains("physical_nullable=false")
                && err.contains("arrow_nullable=true"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_missing_top_level_physical_nullability() {
        let err = align_real_scalar_parquet_nullability(false, None, false, false)
            .expect_err("missing physical nullability must fail before alignment");

        assert!(
            err.contains("physical parquet schema nullability is missing")
                && err.contains("output_path=v")
                && err.contains("unique_id=11"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_duplicate_physical_uid_zero_when_first_name_is_missing() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef],
        )
        .expect("real parquet UID0 batch");
        let current_schema = scalar_tablet_schema(0, "v", "INT");
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![
                ColumnPb {
                    unique_id: 0,
                    name: None,
                    r#type: "INT".to_string(),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 0,
                    name: Some("v".to_string()),
                    r#type: "INT".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let (_temp_dir, snapshot) =
            real_parquet_snapshot_with_schemas(batch, current_schema, physical_schema);
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(0),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(0),
            fallback_default_literal: None,
        }];

        let err =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect_err(
                    "all nonnegative physical UIDs must be registered before name filtering",
                );

        assert!(
            err.contains("duplicated physical parquet column unique_id")
                && err.contains("unique_id=0"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_duplicate_normalized_physical_schema_names() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("V", DataType::Int32, false),
                Field::new(" v ", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![100_i32, 200_i32])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef,
            ],
        )
        .expect("real parquet duplicate normalized physical-name batch");
        let current_schema = scalar_tablet_schema(1, "v", "INT");
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![
                ColumnPb {
                    unique_id: 0,
                    name: Some("V".to_string()),
                    r#type: "INT".to_string(),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 1,
                    name: Some(" v ".to_string()),
                    r#type: "INT".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let (_temp_dir, snapshot) =
            real_parquet_snapshot_with_schemas(batch, current_schema, physical_schema);
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(1),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(1),
            fallback_default_literal: None,
        }];

        let err =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect_err("different physical UIDs must not share one normalized name");

        assert!(
            err.contains("duplicated physical parquet column name")
                && err.contains("column_name=v"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_rejects_duplicate_normalized_arrow_fields_before_uid_binding() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("V", DataType::Int32, false),
                Field::new(" v ", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![100_i32, 200_i32])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef,
            ],
        )
        .expect("real parquet duplicate normalized Arrow-name batch");
        let current_schema = scalar_tablet_schema(1, "v", "INT");
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let (_temp_dir, snapshot) =
            real_parquet_snapshot_with_schemas(batch, current_schema, physical_schema);
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(1),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(1),
            fallback_default_literal: None,
        }];

        let err =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect_err("ambiguous normalized Arrow fields must not bind UID1 to UID0 data");

        assert!(
            err.contains("duplicated parquet Arrow field name after normalization")
                && err.contains("column_name=v"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_empty_segment_still_validates_declared_physical_schema() {
        let temp_dir = tempfile::tempdir().expect("create empty segment temp dir");
        let empty_path = temp_dir.path().join("empty-drift.parquet");
        let empty_path = empty_path.to_string_lossy().to_string();
        let legal_path = temp_dir.path().join("legal.parquet");
        let legal_path = legal_path.to_string_lossy().to_string();

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )])));
        write_parquet_file(&empty_path, &empty_batch).expect("write empty drift parquet");
        assert!(
            read_parquet_file(&empty_path)
                .expect("read empty drift parquet")
                .is_empty(),
            "fixture must exercise the zero-batch parquet path"
        );

        let legal_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![Some(7_i32)])) as ArrayRef],
        )
        .expect("legal parquet batch");
        write_parquet_file(&legal_path, &legal_batch).expect("write legal parquet");

        let current_schema = TabletSchemaPb {
            id: Some(20),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("declared_v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: current_schema,
            historical_schemas: std::collections::BTreeMap::from([(10, physical_schema)]),
            total_num_rows: 1,
            rowset_count: 2,
            segment_files: vec![
                StarRocksSegmentFile {
                    name: "empty-drift.parquet".to_string(),
                    relative_path: "empty-drift.parquet".to_string(),
                    path: empty_path,
                    rowset_version: 10,
                    schema_id: Some(10),
                    segment_id: Some(0),
                    bundle_file_offset: None,
                    segment_size: None,
                },
                StarRocksSegmentFile {
                    name: "legal.parquet".to_string(),
                    relative_path: "legal.parquet".to_string(),
                    path: legal_path,
                    rowset_version: 20,
                    schema_id: Some(20),
                    segment_id: Some(0),
                    bundle_file_offset: None,
                    segment_size: None,
                },
            ],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];

        let err =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect_err("empty parquet segment metadata drift must fail before concatenation");

        assert!(
            err.contains("physical parquet Arrow column is missing for declared schema column")
                && err.contains("unique_id=12"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_empty_segment_still_validates_physical_arrow_nullability() {
        let temp_dir = tempfile::tempdir().expect("create empty nullability segment temp dir");
        let empty_path = temp_dir.path().join("empty-nullability-drift.parquet");
        let empty_path = empty_path.to_string_lossy().to_string();
        let legal_path = temp_dir.path().join("legal-nullability.parquet");
        let legal_path = legal_path.to_string_lossy().to_string();

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int32,
            true,
        )])));
        write_parquet_file(&empty_path, &empty_batch)
            .expect("write empty nullability drift parquet");
        assert!(
            read_parquet_file(&empty_path)
                .expect("read empty nullability drift parquet")
                .is_empty(),
            "fixture must exercise the zero-batch parquet path"
        );

        let legal_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![7_i32])) as ArrayRef],
        )
        .expect("legal nullability parquet batch");
        write_parquet_file(&legal_path, &legal_batch).expect("write legal nullability parquet");

        let current_schema = TabletSchemaPb {
            id: Some(20),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(false),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(false),
                ..Default::default()
            }],
            ..Default::default()
        };
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: current_schema,
            historical_schemas: std::collections::BTreeMap::from([(10, physical_schema)]),
            total_num_rows: 1,
            rowset_count: 2,
            segment_files: vec![
                StarRocksSegmentFile {
                    name: "empty-nullability-drift.parquet".to_string(),
                    relative_path: "empty-nullability-drift.parquet".to_string(),
                    path: empty_path,
                    rowset_version: 10,
                    schema_id: Some(10),
                    segment_id: Some(0),
                    bundle_file_offset: None,
                    segment_size: None,
                },
                StarRocksSegmentFile {
                    name: "legal-nullability.parquet".to_string(),
                    relative_path: "legal-nullability.parquet".to_string(),
                    path: legal_path,
                    rowset_version: 20,
                    schema_id: Some(20),
                    segment_id: Some(0),
                    bundle_file_offset: None,
                    segment_size: None,
                },
            ],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];

        let err = read_bundle_parquet_snapshot_with_output_hints_if_any(
            &snapshot,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            &hints,
        )
        .expect_err("empty parquet nullability drift must fail before concatenation");

        assert!(
            err.contains("physical parquet schema nullability does not match Arrow field")
                && err.contains("physical_nullable=false")
                && err.contains("arrow_nullable=true"),
            "{err}"
        );
    }

    #[test]
    fn real_parquet_empty_segment_rejects_missing_physical_nullability_before_legal_segment() {
        let temp_dir = tempfile::tempdir().expect("create missing nullability segment temp dir");
        let empty_path = temp_dir.path().join("empty-missing-nullability.parquet");
        let empty_path = empty_path.to_string_lossy().to_string();
        let legal_path = temp_dir.path().join("legal-present-nullability.parquet");
        let legal_path = legal_path.to_string_lossy().to_string();

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int32,
            true,
        )])));
        write_parquet_file(&empty_path, &empty_batch)
            .expect("write empty missing-nullability parquet");
        assert!(
            read_parquet_file(&empty_path)
                .expect("read empty missing-nullability parquet")
                .is_empty(),
            "fixture must exercise the zero-batch parquet path"
        );

        let legal_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![Some(7_i32)])) as ArrayRef],
        )
        .expect("legal present-nullability parquet batch");
        write_parquet_file(&legal_path, &legal_batch)
            .expect("write legal present-nullability parquet");

        let current_schema = TabletSchemaPb {
            id: Some(20),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let physical_schema = TabletSchemaPb {
            id: Some(10),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("v".to_string()),
                r#type: "INT".to_string(),
                is_nullable: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: current_schema,
            historical_schemas: std::collections::BTreeMap::from([(10, physical_schema)]),
            total_num_rows: 1,
            rowset_count: 2,
            segment_files: vec![
                StarRocksSegmentFile {
                    name: "empty-missing-nullability.parquet".to_string(),
                    relative_path: "empty-missing-nullability.parquet".to_string(),
                    path: empty_path,
                    rowset_version: 10,
                    schema_id: Some(10),
                    segment_id: Some(0),
                    bundle_file_offset: None,
                    segment_size: None,
                },
                StarRocksSegmentFile {
                    name: "legal-present-nullability.parquet".to_string(),
                    relative_path: "legal-present-nullability.parquet".to_string(),
                    path: legal_path,
                    rowset_version: 20,
                    schema_id: Some(20),
                    segment_id: Some(0),
                    bundle_file_offset: None,
                    segment_size: None,
                },
            ],
            delete_predicates: Vec::new(),
            delvec_meta: StarRocksDelvecMetaRaw::default(),
        };
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(12),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12),
            fallback_default_literal: None,
        }];

        let err = read_bundle_parquet_snapshot_with_output_hints_if_any(
            &snapshot,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            &hints,
        )
        .expect_err("empty missing-nullability segment must fail before concatenation");

        assert!(
            err.contains("physical parquet schema nullability is missing")
                && err.contains("output_path=v")
                && err.contains("unique_id=12"),
            "{err}"
        );
    }

    #[test]
    fn parquet_struct_alignment_widens_renamed_uid_zero_child() {
        let source_type = nested_data_type(NestedShape::Struct, "old_value", DataType::Int32);
        let output_type = nested_data_type(NestedShape::Struct, "new_value", DataType::Int64);
        let aligned = align_nested_parquet(
            source_type,
            nested_column(NestedShape::Struct, "old_value", 0, "INT"),
            output_type.clone(),
            nested_column(NestedShape::Struct, "new_value", 0, "BIGINT"),
        )
        .expect("renamed STRUCT child with uid 0 must widen by identity");

        assert_eq!(aligned.column(0).data_type(), &output_type);
        let values = aligned
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("STRUCT output")
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT STRUCT child");
        assert_eq!(values.values(), &[1_i64, 2_i64]);
    }

    #[test]
    fn parquet_array_alignment_widens_nested_int_to_bigint() {
        let source_type = nested_data_type(NestedShape::Array, "item", DataType::Int32);
        let output_type = nested_data_type(NestedShape::Array, "item", DataType::Int64);
        let aligned = align_nested_parquet(
            source_type,
            nested_column(NestedShape::Array, "item", 0, "INT"),
            output_type.clone(),
            nested_column(NestedShape::Array, "item", 0, "BIGINT"),
        )
        .expect("ARRAY item INT must widen to BIGINT");

        assert_eq!(aligned.column(0).data_type(), &output_type);
        let values = aligned
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ARRAY output")
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT ARRAY item");
        assert_eq!(values.values(), &[1_i64, 2_i64]);
    }

    #[test]
    fn parquet_map_alignment_widens_nested_int_to_bigint() {
        let source_type = nested_data_type(NestedShape::Map, "value", DataType::Int32);
        let output_type = nested_data_type(NestedShape::Map, "value", DataType::Int64);
        let aligned = align_nested_parquet(
            source_type,
            nested_column(NestedShape::Map, "value", 0, "INT"),
            output_type.clone(),
            nested_column(NestedShape::Map, "value", 0, "BIGINT"),
        )
        .expect("MAP value INT must widen to BIGINT");

        assert_eq!(aligned.column(0).data_type(), &output_type);
        let values = aligned
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MAP output")
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT MAP value");
        assert_eq!(values.values(), &[1_i64, 2_i64]);
    }

    #[test]
    fn parquet_nested_alignment_rejects_narrowing_for_all_complex_shapes() {
        for shape in [NestedShape::Struct, NestedShape::Array, NestedShape::Map] {
            let err = align_nested_parquet(
                nested_data_type(shape, "value", DataType::Int64),
                nested_column(shape, "value", 0, "BIGINT"),
                nested_data_type(shape, "value", DataType::Int32),
                nested_column(shape, "value", 0, "INT"),
            )
            .expect_err("nested BIGINT to INT narrowing must fail");

            assert!(err.contains("signed integer widening"), "{shape:?}: {err}");
        }
    }

    #[test]
    fn parquet_nested_alignment_rejects_cross_family_for_all_complex_shapes() {
        for shape in [NestedShape::Struct, NestedShape::Array, NestedShape::Map] {
            let err = align_nested_parquet(
                nested_data_type(shape, "value", DataType::Utf8),
                nested_column(shape, "value", 0, "VARCHAR"),
                nested_data_type(shape, "value", DataType::Int64),
                nested_column(shape, "value", 0, "BIGINT"),
            )
            .expect_err("nested VARCHAR to BIGINT cast must fail");

            assert!(err.contains("signed integer widening"), "{shape:?}: {err}");
        }
    }

    #[test]
    fn parquet_nested_alignment_rejects_arrow_metadata_mismatch_for_all_complex_shapes() {
        for shape in [NestedShape::Struct, NestedShape::Array, NestedShape::Map] {
            let err = align_nested_parquet(
                nested_data_type(shape, "value", DataType::Int64),
                nested_column(shape, "value", 0, "INT"),
                nested_data_type(shape, "value", DataType::Int64),
                nested_column(shape, "value", 0, "BIGINT"),
            )
            .expect_err("actual nested BIGINT must not masquerade as physical INT metadata");

            assert!(
                err.contains("physical parquet Arrow type does not match tablet schema"),
                "{shape:?}: {err}"
            );
        }
    }

    #[test]
    fn real_parquet_rejects_struct_child_current_output_nullability_drift() {
        assert_nested_current_output_nullability_drift(NestedShape::Struct);
    }

    #[test]
    fn real_parquet_rejects_array_child_current_output_nullability_drift() {
        assert_nested_current_output_nullability_drift(NestedShape::Array);
    }

    #[test]
    fn real_parquet_rejects_map_child_current_output_nullability_drift() {
        assert_nested_current_output_nullability_drift(NestedShape::Map);
    }

    #[test]
    fn real_parquet_rejects_missing_struct_child_physical_nullability() {
        assert_missing_nested_physical_nullability(NestedShape::Struct);
    }

    #[test]
    fn real_parquet_rejects_missing_array_child_physical_nullability() {
        assert_missing_nested_physical_nullability(NestedShape::Array);
    }

    #[test]
    fn real_parquet_rejects_missing_map_child_physical_nullability() {
        assert_missing_nested_physical_nullability(NestedShape::Map);
    }

    fn assert_missing_nested_physical_nullability(shape: NestedShape) {
        let physical = nested_column_with_leaf_nullability(shape, "value", 0, "INT", None);
        let current = nested_column_with_leaf_nullability(shape, "value", 0, "INT", Some(true));
        let nullable_type =
            nested_data_type_with_leaf_nullability(shape, "value", DataType::Int32, true);

        let err = align_nested_parquet(nullable_type.clone(), physical, nullable_type, current)
            .expect_err("missing nested physical nullability must fail before alignment");

        assert!(
            err.contains("physical parquet schema nullability is missing")
                && err.contains("unique_id=0"),
            "{shape:?}: {err}"
        );
    }

    fn assert_nested_current_output_nullability_drift(shape: NestedShape) {
        let physical = nested_column(shape, "value", 0, "INT");
        let mut current = nested_column(shape, "value", 0, "INT");
        let current_leaf_index = match shape {
            NestedShape::Map => 1,
            NestedShape::Struct | NestedShape::Array => 0,
        };
        current.children_columns[current_leaf_index].is_nullable = Some(true);

        let err = align_nested_parquet(
            nested_data_type(shape, "value", DataType::Int32),
            physical,
            nested_data_type(shape, "value", DataType::Int32),
            current,
        )
        .expect_err("nested authoritative current/output nullability drift must fail");

        assert!(
            err.contains("authoritative current parquet schema nullability does not match output")
                && err.contains("current_nullable=true")
                && err.contains("output_nullable=false"),
            "{shape:?}: {err}"
        );
    }

    #[test]
    fn parquet_struct_alignment_rejects_duplicate_nonnegative_child_uids() {
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let mut physical = nested_column(NestedShape::Struct, "a", 0, "INT");
        physical.children_columns.push(ColumnPb {
            unique_id: 0,
            name: Some("b".to_string()),
            r#type: "INT".to_string(),
            is_nullable: Some(false),
            ..Default::default()
        });
        let mut current = nested_column(NestedShape::Struct, "a", 0, "INT");
        current.children_columns.push(ColumnPb {
            unique_id: 1,
            name: Some("b".to_string()),
            r#type: "INT".to_string(),
            is_nullable: Some(false),
            ..Default::default()
        });

        let err = align_nested_parquet(struct_type.clone(), physical, struct_type, current)
            .expect_err("duplicate nested physical uid 0 must fail before alignment");

        assert!(err.contains("duplicated"), "{err}");
        assert!(err.contains("unique_id=0"), "{err}");
    }

    #[test]
    fn parquet_snapshot_rejects_unknown_historical_segment_schema_id() {
        let (_temp_dir, mut snapshot) =
            single_boolean_parquet_snapshot("flag", 11, vec![true, false]);
        snapshot.tablet_schema = boolean_tablet_schema(20, "flag", 11);
        snapshot.segment_files[0].schema_id = Some(999);
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(11),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11),
            fallback_default_literal: None,
        }];

        let err =
            read_bundle_parquet_snapshot_with_output_hints_if_any(&snapshot, output_schema, &hints)
                .expect_err("unknown historical segment schema id must fail fast");
        assert!(
            err.contains(
                "segment rowset schema id is missing from snapshot historical schemas: \
                 tablet_id=10, version=20"
            ),
            "{err}"
        );
        assert!(err.contains("schema_id=999"), "{err}");
    }

    #[test]
    fn parquet_schema_id_none_uses_pre_refresh_physical_schema_when_ids_differ() {
        let batch = read_real_parquet_with_schema_id_none_after_refresh(29, None, false)
            .expect("legacy INT parquet must widen with pre-refresh physical schema")
            .expect("non-empty legacy parquet batch");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT widened output");
        assert_eq!(values.values(), &[1_i64, 2_i64]);
    }

    #[test]
    fn parquet_schema_id_none_uses_pre_refresh_physical_schema_when_ids_match() {
        let batch = read_real_parquet_with_schema_id_none_after_refresh(30, None, false)
            .expect("same-ID legacy INT parquet must retain pre-refresh physical semantics")
            .expect("non-empty same-ID legacy parquet batch");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT widened output");
        assert_eq!(values.values(), &[1_i64, 2_i64]);
    }

    #[test]
    fn parquet_schema_id_none_zero_batch_uses_pre_refresh_physical_schema() {
        let batch = read_real_parquet_with_schema_id_none_after_refresh(29, None, true)
            .expect("zero-batch legacy INT parquet must validate against physical fallback")
            .expect("identified zero-batch parquet must be handled as an empty batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn parquet_positive_schema_id_does_not_use_pre_refresh_physical_fallback() {
        let err = read_real_parquet_with_schema_id_none_after_refresh(29, Some(29), false)
            .expect_err("positive schema ID must resolve through historical/current schemas");

        assert!(
            err.contains("segment rowset schema id is missing from snapshot historical schemas")
                && err.contains("schema_id=29"),
            "{err}"
        );
    }
}
