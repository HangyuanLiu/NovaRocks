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

use crate::connector::starrocks::schema::{StarRocksColumnSchema, StarRocksTabletSchema};
use crate::formats::starrocks::fs_access::resolve_format_path;
use crate::formats::starrocks::metadata::{StarRocksSegmentFile, StarRocksTabletSnapshot};
use crate::formats::starrocks::plan::{
    StarRocksOutputColumnHint, StarRocksPhysicalColumnBinding,
    validate_physical_schema_to_output_type,
};
use crate::fs::access::FsScheme;

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
    physical_fallback_schema: &StarRocksTabletSchema,
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
    physical_fallback_schema: &StarRocksTabletSchema,
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
    physical_fallback_schema: &'a StarRocksTabletSchema,
) -> Result<&'a StarRocksTabletSchema, String> {
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
    current_schema: &StarRocksTabletSchema,
    physical_schema: &StarRocksTabletSchema,
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
        &StarRocksTabletSchema,
        &StarRocksTabletSchema,
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

fn find_schema_column_by_unique_id(
    schema: &StarRocksTabletSchema,
    unique_id: u32,
) -> Option<&StarRocksColumnSchema> {
    schema
        .column
        .iter()
        .find(|column| u32::try_from(column.unique_id).ok() == Some(unique_id))
}

fn validate_authoritative_current_schema_for_output(
    current_column: &StarRocksColumnSchema,
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
    physical_column: &StarRocksColumnSchema,
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
    schema_column: &StarRocksColumnSchema,
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
    physical_column: &StarRocksColumnSchema,
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
    current_column: &StarRocksColumnSchema,
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
    physical_schema: &'a StarRocksTabletSchema,
) -> Option<&'a StarRocksColumnSchema> {
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
    physical_schema: &StarRocksTabletSchema,
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
    physical_column: &StarRocksColumnSchema,
    current_column: &StarRocksColumnSchema,
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
    current_column: &StarRocksColumnSchema,
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
    physical_column: &StarRocksColumnSchema,
    current_column: &StarRocksColumnSchema,
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
    physical_column: &StarRocksColumnSchema,
    current_column: &StarRocksColumnSchema,
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

fn nonnegative_schema_column_unique_id(column: &StarRocksColumnSchema) -> Option<u32> {
    u32::try_from(column.unique_id).ok()
}

fn build_complex_children_by_unique_id<'a>(
    children: &'a [StarRocksColumnSchema],
    schema_role: &str,
    output_path: &str,
) -> Result<std::collections::HashMap<u32, (usize, &'a StarRocksColumnSchema)>, String> {
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
    physical_column: &'a StarRocksColumnSchema,
    current_column: &StarRocksColumnSchema,
    output_fields: &Fields,
    output_path: &str,
) -> Result<Vec<(usize, &'a StarRocksColumnSchema)>, String> {
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
    physical_column: &StarRocksColumnSchema,
    current_column: &StarRocksColumnSchema,
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
