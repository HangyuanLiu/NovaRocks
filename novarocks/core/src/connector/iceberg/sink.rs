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
//! Iceberg provider-side staged-file helpers.
//!
//! Responsibilities:
//! - Build bounded Iceberg staged-writer metadata and object-store access.
//! - Encode Parquet files and provider-private statistics used by the common
//!   connector writer execution adapter.

use std::collections::{BTreeMap, HashMap};

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use novarocks_connector_iceberg::iceberg::spec::TableMetadata;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::connector::iceberg::sink_plan::{
    IcebergSinkObjectStoreConfig, PositionDeleteDataFilePartition,
};
use crate::runtime::global_async_runtime::data_block_on;
use novarocks_connector_iceberg::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::expr::{ExprArena, ExprId, cast_with_special_rules};

pub(crate) fn build_position_delete_data_file_partition_index(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
    table_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<HashMap<String, PositionDeleteDataFilePartition>, String> {
    use novarocks_connector_iceberg::iceberg::spec::{
        DataContentType, ManifestContentType, ManifestStatus,
    };

    let Some(snapshot_id) = target_snapshot_id.or_else(|| metadata.current_snapshot_id()) else {
        return Ok(HashMap::new());
    };
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        format!("Iceberg delete sink target snapshot id {snapshot_id} not found in table metadata")
    })?;
    let file_io = build_core_staged_file_io(table_location, s3_config)?;
    data_block_on(async {
        let manifest_list = snapshot
            .load_manifest_list(&file_io, metadata)
            .await
            .map_err(|e| format!("load Iceberg position-delete target manifest list: {e}"))?;
        let mut index = HashMap::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(&file_io).await.map_err(|e| {
                format!(
                    "load Iceberg position-delete data manifest {} failed: {e}",
                    manifest_file.manifest_path
                )
            })?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    continue;
                }
                let partition = PositionDeleteDataFilePartition {
                    partition_spec_id: manifest_file.partition_spec_id,
                    partition_values: data_file.partition().clone(),
                };
                insert_position_delete_data_file_partition(
                    &mut index,
                    data_file.file_path().to_string(),
                    partition,
                )?;
            }
        }
        Ok(index)
    })?
}

fn insert_position_delete_data_file_partition(
    index: &mut HashMap<String, PositionDeleteDataFilePartition>,
    path: String,
    partition: PositionDeleteDataFilePartition,
) -> Result<(), String> {
    match index.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
            Ok(())
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            Err(format!(
                "Iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ))
        }
    }
}

fn group_positions_by_file(
    batch: &RecordBatch,
    sink_label: &str,
) -> Result<BTreeMap<String, Vec<u64>>, String> {
    let file_path_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("iceberg {sink_label} sink: file_path array expected as Utf8"))?;
    let pos_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("iceberg {sink_label} sink: pos array expected as Int64"))?;
    if file_path_col.null_count() > 0 || pos_col.null_count() > 0 {
        return Err(format!(
            "iceberg {sink_label} sink rejects NULL file_path or pos"
        ));
    }

    let mut out: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let pos = pos_col.value(row);
        if pos < 0 {
            return Err(format!(
                "iceberg {sink_label} sink pos must be non-negative: {pos}"
            ));
        }
        out.entry(file_path_col.value(row).to_string())
            .or_default()
            .push(pos as u64);
    }
    Ok(out)
}

fn merge_deletion_vectors_by_file(
    mut existing: HashMap<String, crate::connector::iceberg::commit::DeletionVector>,
    positions_by_file: &BTreeMap<String, Vec<u64>>,
) -> Result<BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>, String> {
    let mut out = BTreeMap::new();
    for (file, positions) in positions_by_file {
        let mut dv = existing.remove(file).unwrap_or_default();
        for pos in positions {
            dv.insert(*pos).map_err(|e| {
                format!(
                    "iceberg deletion-vector sink insert position {pos} for `{file}` failed: {e}"
                )
            })?;
        }
        out.insert(file.clone(), dv);
    }
    Ok(out)
}

fn merge_existing_with_pending_deletion_vectors(
    mut existing: HashMap<String, crate::connector::iceberg::commit::DeletionVector>,
    pending: &BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>,
) -> BTreeMap<String, crate::connector::iceberg::commit::DeletionVector> {
    let mut out = BTreeMap::new();
    for (file, pending_dv) in pending {
        let mut dv = existing.remove(file).unwrap_or_default();
        dv.merge(pending_dv);
        out.insert(file.clone(), dv);
    }
    out
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    partition_spec_id: i32,
    path: String,
    null_fingerprint: String,
    partition_key: String,
}

#[derive(Debug)]
struct PartitionGroup {
    indices: Vec<u32>,
    partition_spec_id: i32,
    partition_values: novarocks_connector_iceberg::iceberg::spec::Struct,
}

fn eval_exprs(arena: &ExprArena, exprs: &[ExprId], chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(arena.eval(*expr, chunk)?);
    }
    Ok(out)
}

fn align_arrays_to_schema(
    arrays: Vec<ArrayRef>,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>, String> {
    if arrays.len() != schema.fields().len() {
        return Err(format!(
            "iceberg sink column count mismatch while aligning arrays: arrays={} schema={}",
            arrays.len(),
            schema.fields().len()
        ));
    }

    arrays
        .into_iter()
        .zip(schema.fields().iter())
        .enumerate()
        .map(|(idx, (array, field))| {
            let target_type = field.data_type();
            if array.data_type() == target_type {
                return Ok(array);
            }

            let casted = if data_type_contains_largeint(target_type) {
                cast_with_special_rules(&array, target_type)
            } else {
                cast(array.as_ref(), target_type).map_err(|e| e.to_string())
            }
            .map_err(|e| {
                format!(
                    "iceberg sink cast failed at column index {} name={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type,
                    e
                )
            })?;

            if !matches!(array.data_type(), DataType::Null)
                && casted.null_count() > array.null_count()
            {
                return Err(format!(
                    "iceberg sink cast introduced nulls at column index {} name={} from {:?} to {:?}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type
                ));
            }
            Ok(casted)
        })
        .collect()
}

fn data_type_contains_largeint(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeBinary(width) => {
            *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            data_type_contains_largeint(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_contains_largeint(field.data_type())),
        DataType::Map(entries, _) => data_type_contains_largeint(entries.data_type()),
        _ => false,
    }
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "iceberg sink field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|e| {
        format!(
            "iceberg sink field {} has invalid parquet field id {raw}: {e}",
            field.name()
        )
    })
}

fn schema_has_reserved_row_lineage_columns(schema: &Schema) -> Result<bool, String> {
    let mut has_row_id = false;
    let mut has_last_updated = false;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
            has_row_id = matches!(arrow_field_id(field), Ok(ICEBERG_RESERVED_FIELD_ID_ROW_ID));
        } else if field
            .name()
            .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        {
            has_last_updated = matches!(
                arrow_field_id(field),
                Ok(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
            );
        }
    }
    Ok(has_row_id && has_last_updated)
}

fn row_lineage_row_id_index(schema: &Schema) -> Result<usize, String> {
    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
            && arrow_field_id(field)? == ICEBERG_RESERVED_FIELD_ID_ROW_ID
        {
            return Ok(idx);
        }
    }
    Err("iceberg row-lineage sink missing reserved _row_id field".to_string())
}

/// Core-only planning/manifest FileIO builder. BE write execution must use
/// `novarocks_connector_iceberg::commit::write_io::build_staged_file_io`.
fn build_core_staged_file_io(
    data_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<novarocks_connector_iceberg::iceberg::io::FileIO, String> {
    if novarocks_fs::is_object_store_location_parse_only(data_location)
        .map_err(|e| format!("parse staged iceberg data_location {data_location}: {e}"))?
    {
        let s3 = s3_config.ok_or_else(|| {
            format!(
                "iceberg sink missing S3 config for staged writer data_location={data_location}"
            )
        })?;
        let object_store_config = s3.to_object_store_config();
        return Ok(
            novarocks_connector_iceberg::fs_io::build_file_io_for_location(
                data_location,
                Some(&object_store_config),
            ),
        );
    }
    Ok(novarocks_connector_iceberg::fs_io::build_file_io_for_location(data_location, None))
}

fn iceberg_partition_key_for_row(
    partition_column_names: &[String],
    transform_exprs: &[String],
    partition_arrays: &[ArrayRef],
    row: usize,
) -> Result<
    (
        String,
        String,
        novarocks_connector_iceberg::iceberg::spec::Struct,
    ),
    String,
> {
    if partition_column_names.len() != transform_exprs.len()
        || partition_arrays.len() != partition_column_names.len()
    {
        return Err("partition arrays mismatch for iceberg sink".to_string());
    }
    let mut path = String::new();
    let mut nulls = String::with_capacity(partition_column_names.len());
    let mut partition_values = Vec::with_capacity(partition_column_names.len());
    for i in 0..partition_column_names.len() {
        let transform = transform_exprs[i].to_lowercase();
        let base = transform.split('[').next().unwrap_or(transform.as_str());
        let is_null = base == "void" || partition_arrays[i].is_null(row);
        let value = iceberg_partition_value(base, &partition_arrays[i], row)?;
        let literal = if is_null {
            None
        } else {
            Some(iceberg_partition_literal(base, &partition_arrays[i], row)?)
        };
        nulls.push(if is_null { '1' } else { '0' });
        partition_values.push(literal);
        path.push_str(&partition_column_names[i]);
        path.push('=');
        path.push_str(&value);
        path.push('/');
    }
    Ok((
        path,
        nulls,
        novarocks_connector_iceberg::iceberg::spec::Struct::from_iter(partition_values),
    ))
}

fn iceberg_partition_literal(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Literal, String> {
    match transform {
        "year" | "month" | "hour" | "bucket" => {
            let value = array_value_as_i64(array, row)?;
            let value = i32::try_from(value)
                .map_err(|_| format!("{transform} transform value out of INT range"))?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                value,
            ))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::date(
                days,
            ))
        }
        "truncate" | "identity" => column_literal(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn iceberg_partition_value(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<String, String> {
    if array.is_null(row) || transform == "void" {
        return Ok("null".to_string());
    }
    match transform {
        "year" => {
            let value = array_value_as_i64(array, row)?;
            Ok((value + 1970).to_string())
        }
        "month" => {
            let value = array_value_as_i64(array, row)?;
            let year = 1970 + (value / 12);
            let month = value % 12 + 1;
            Ok(format!("{:04}-{:02}", year, month))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid day transform value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        "hour" => {
            let value = array_value_as_i64(array, row)?;
            let seconds = value * 3600;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
                .ok_or_else(|| "invalid hour transform value".to_string())?
                .naive_utc();
            Ok(dt.format("%Y-%m-%d-%H").to_string())
        }
        "truncate" | "bucket" | "identity" => column_value(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn array_value_as_i64(array: &ArrayRef, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(i64::from(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row))
        }
        other => Err(format!(
            "iceberg partition transform expects INT/BIGINT, got {other:?}"
        )),
    }
}

fn column_value(array: &ArrayRef, row: usize) -> Result<String, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(if arr.value(row) { "true" } else { "false" }.to_string())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid Date32 value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            let micros = arr.value(row);
            let secs = micros.div_euclid(1_000_000);
            let rem = micros.rem_euclid(1_000_000);
            let nanos = (rem as u32) * 1000;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                .ok_or_else(|| "invalid DATETIME value".to_string())?
                .naive_utc();
            Ok(url_encode(&format_datetime(dt)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(url_encode(arr.value(row)))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            let bytes = arr.value(row);
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(url_encode(&encoded))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(arr.value_as_string(row))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn column_literal(
    array: &ArrayRef,
    row: usize,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Literal, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::bool(
                arr.value(row),
            ))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                i32::from(arr.value(row)),
            ))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                i32::from(arr.value(row)),
            ))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                arr.value(row),
            ))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::long(
                arr.value(row),
            ))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "expected FLOAT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::float(
                arr.value(row),
            ))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "expected DOUBLE array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::double(
                arr.value(row),
            ))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::date(
                arr.value(row),
            ))
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
                .ok_or_else(|| "expected TIME array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::time(
                arr.value(row),
            ))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::timestamp(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected TIMESTAMP array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::timestamptz(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| "expected TIMESTAMP_NS array".to_string())?;
            Ok(
                novarocks_connector_iceberg::iceberg::spec::Literal::Primitive(
                    novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Long(
                        arr.value(row),
                    ),
                ),
            )
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::string(
                arr.value(row),
            ))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::binary(
                arr.value(row).iter().copied(),
            ))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .ok_or_else(|| "expected FIXED array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::fixed(
                arr.value(row).iter().copied(),
            ))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::decimal(arr.value(row)))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn url_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

fn format_datetime(dt: chrono::NaiveDateTime) -> String {
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}
