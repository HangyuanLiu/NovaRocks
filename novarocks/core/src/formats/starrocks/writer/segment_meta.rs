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

//! StarRocks native write-side helpers.
//!
//! This module keeps format-level logic close to native format code:
//! - Sort rows by tablet sort key before segment encoding.
//! - Build minimal `SegmentMetadataPB` (`sort_key_min/max`, `num_rows`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;
use chrono::{DateTime, NaiveDate};

use crate::common::largeint;
use crate::connector::starrocks::schema::{StarRocksColumnSchema, StarRocksTabletSchema};
use crate::service::grpc_client::proto::starrocks::{
    PScalarType, PTypeDesc, PTypeNode, SegmentMetadataPb, TuplePb, VariantPb, VariantTypePb,
};

const TYPE_NODE_SCALAR: i32 = 0;
const DATE32_UNIX_EPOCH_DAY_OFFSET: i32 = 719_163; // 1970-01-01 in proleptic Gregorian days

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortKeyValueType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Boolean,
    Date,
    Datetime,
    LargeInt,
    Varchar,
    Decimal {
        wire_type: StarRocksSegmentWireType,
        scale: i8,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StarRocksSegmentWireType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Date,
    Datetime,
    Binary,
    Decimal,
    Char,
    LargeInt,
    Varchar,
    DecimalV2,
    Decimal32,
    Decimal64,
    Decimal128,
    VarBinary,
    Decimal256,
}

impl StarRocksSegmentWireType {
    fn primitive_code(self) -> i32 {
        match self {
            Self::Boolean => 2,
            Self::TinyInt => 3,
            Self::SmallInt => 4,
            Self::Int => 5,
            Self::BigInt => 6,
            Self::Float => 7,
            Self::Double => 8,
            Self::Date => 9,
            Self::Datetime => 10,
            Self::Binary => 11,
            Self::Decimal => 12,
            Self::Char => 13,
            Self::LargeInt => 14,
            Self::Varchar => 15,
            Self::DecimalV2 => 17,
            Self::Decimal32 => 21,
            Self::Decimal64 => 22,
            Self::Decimal128 => 23,
            Self::VarBinary => 26,
            Self::Decimal256 => 27,
        }
    }
}

pub fn sort_batch_for_native_write(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<RecordBatch, String> {
    let aligned_batch = align_batch_columns_to_schema(batch, tablet_schema)?;
    if aligned_batch.num_rows() <= 1 {
        return Ok(aligned_batch);
    }
    validate_keys_type_for_native_write(tablet_schema)?;
    let sort_key_indexes = resolve_sort_key_indexes(tablet_schema, aligned_batch.num_columns())?;

    let mut columns = Vec::with_capacity(sort_key_indexes.len() + 1);
    for col_idx in sort_key_indexes {
        columns.push(SortColumn {
            values: aligned_batch.column(col_idx).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
    }

    // Append row ordinal to enforce deterministic stable ordering for equal sort keys.
    let row_ordinal =
        UInt64Array::from_iter_values((0..aligned_batch.num_rows()).map(|v| v as u64));
    columns.push(SortColumn {
        values: Arc::new(row_ordinal),
        options: Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
    });

    let indices = lexsort_to_indices(&columns, None)
        .map_err(|e| format!("sort batch by sort key failed: {e}"))?;
    let mut sorted_columns = Vec::with_capacity(aligned_batch.num_columns());
    for col_idx in 0..aligned_batch.num_columns() {
        let sorted = take(aligned_batch.column(col_idx).as_ref(), &indices, None).map_err(|e| {
            format!(
                "reorder column by sorted indices failed: column_index={}, error={}",
                col_idx, e
            )
        })?;
        sorted_columns.push(sorted);
    }
    RecordBatch::try_new(aligned_batch.schema(), sorted_columns)
        .map_err(|e| format!("build sorted record batch failed: {e}"))
}

pub fn build_single_segment_metadata(
    sorted_batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<SegmentMetadataPb, String> {
    let sorted_batch = align_batch_columns_to_schema(sorted_batch, tablet_schema)?;
    if sorted_batch.num_rows() == 0 {
        return Err("cannot build segment metadata from empty batch".to_string());
    }
    let sort_key_indexes = resolve_sort_key_indexes(tablet_schema, sorted_batch.num_columns())?;
    let sort_key_min = build_sort_key_tuple(&sorted_batch, tablet_schema, &sort_key_indexes, 0)?;
    let sort_key_max = build_sort_key_tuple(
        &sorted_batch,
        tablet_schema,
        &sort_key_indexes,
        sorted_batch.num_rows() - 1,
    )?;
    Ok(SegmentMetadataPb {
        sort_key_min: Some(sort_key_min),
        sort_key_max: Some(sort_key_max),
        num_rows: Some(sorted_batch.num_rows() as i64),
    })
}

fn is_positional_generated_field_name(field_name: &str, index: usize) -> bool {
    field_name
        .strip_prefix("col_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .is_some_and(|generated_index| generated_index == index)
}

fn align_batch_columns_to_schema(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<RecordBatch, String> {
    if tablet_schema.column.is_empty() {
        return Ok(batch.clone());
    }
    if batch.num_columns() < tablet_schema.column.len() {
        return Err(format!(
            "batch/schema column mismatch for native writer: batch_columns={} schema_columns={}",
            batch.num_columns(),
            tablet_schema.column.len()
        ));
    }

    let mut name_to_batch_idx = HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        name_to_batch_idx.insert(field.name().to_ascii_lowercase(), idx);
    }

    let mut selected_indices = Vec::with_capacity(tablet_schema.column.len());
    for (schema_idx, schema_col) in tablet_schema.column.iter().enumerate() {
        let schema_name = schema_col
            .name
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();

        let batch_schema = batch.schema();
        let indexed_field = batch_schema.fields().get(schema_idx);
        let index_matches_name =
            indexed_field.is_some_and(|field| field.name().eq_ignore_ascii_case(&schema_name));
        let index_is_generated = indexed_field
            .is_some_and(|field| is_positional_generated_field_name(field.name(), schema_idx));

        let batch_idx = if index_matches_name || schema_name.is_empty() || index_is_generated {
            schema_idx
        } else if let Some(idx) = name_to_batch_idx.get(&schema_name) {
            *idx
        } else if schema_idx < batch.num_columns() {
            schema_idx
        } else {
            return Err(format!(
                "schema column not found in batch for native writer: schema_index={}, schema_name={}",
                schema_idx, schema_name
            ));
        };
        selected_indices.push(batch_idx);
    }

    let identity = selected_indices
        .iter()
        .enumerate()
        .all(|(idx, selected)| idx == *selected);
    let names_aligned = selected_indices
        .iter()
        .enumerate()
        .all(|(schema_idx, batch_idx)| {
            let schema_name = tablet_schema.column[schema_idx]
                .name
                .as_deref()
                .unwrap_or("")
                .trim();
            schema_name.is_empty()
                || batch
                    .schema()
                    .field(*batch_idx)
                    .name()
                    .eq_ignore_ascii_case(schema_name)
        });
    if identity && names_aligned {
        return Ok(batch.clone());
    }

    let mut aligned_columns = Vec::with_capacity(selected_indices.len());
    let mut aligned_fields = Vec::with_capacity(selected_indices.len());
    for (schema_idx, batch_idx) in selected_indices.iter().enumerate() {
        aligned_columns.push(batch.column(*batch_idx).clone());
        let source_field = batch.schema().field(*batch_idx).as_ref().clone();
        let schema_name = tablet_schema.column[schema_idx]
            .name
            .as_deref()
            .unwrap_or("")
            .trim();
        let aligned_field = if schema_name.is_empty() {
            source_field
        } else {
            source_field.with_name(schema_name.to_string())
        };
        aligned_fields.push(Arc::new(aligned_field));
    }

    let aligned_schema = Arc::new(Schema::new(aligned_fields));
    RecordBatch::try_new(aligned_schema, aligned_columns)
        .map_err(|e| format!("build schema-aligned record batch failed: {e}"))
}

fn validate_keys_type_for_native_write(
    tablet_schema: &StarRocksTabletSchema,
) -> Result<(), String> {
    let keys_type = tablet_schema
        .keys_type
        .ok_or_else(|| "tablet schema missing keys_type for native write".to_string())?;
    let _ = keys_type;
    Ok(())
}

fn resolve_sort_key_indexes(
    tablet_schema: &StarRocksTabletSchema,
    output_columns: usize,
) -> Result<Vec<usize>, String> {
    if tablet_schema.sort_key_idxes.is_empty() {
        return Err("tablet schema missing sort_key_idxes for native write".to_string());
    }
    let mut indexes = Vec::with_capacity(tablet_schema.sort_key_idxes.len());
    for idx in &tablet_schema.sort_key_idxes {
        let idx_usize = usize::try_from(*idx)
            .map_err(|_| format!("invalid sort_key_idx in tablet schema: {}", idx))?;
        if idx_usize >= output_columns {
            return Err(format!(
                "sort_key_idx out of range in tablet schema: idx={} output_columns={}",
                idx_usize, output_columns
            ));
        }
        indexes.push(idx_usize);
    }
    Ok(indexes)
}

fn build_sort_key_tuple(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
    sort_key_indexes: &[usize],
    row_idx: usize,
) -> Result<TuplePb, String> {
    let mut values = Vec::with_capacity(sort_key_indexes.len());
    for col_idx in sort_key_indexes {
        let schema_col = tablet_schema.column.get(*col_idx).ok_or_else(|| {
            format!(
                "sort key column index out of range in tablet schema: idx={} columns={}",
                col_idx,
                tablet_schema.column.len()
            )
        })?;
        let array = batch.column(*col_idx);
        let value_type = parse_sort_key_value_type(schema_col)?;
        let variant = build_variant_for_value(array, schema_col, value_type, *col_idx, row_idx)?;
        values.push(variant);
    }
    Ok(TuplePb { values })
}

fn build_variant_for_value(
    array: &ArrayRef,
    schema_col: &StarRocksColumnSchema,
    value_type: SortKeyValueType,
    col_idx: usize,
    row_idx: usize,
) -> Result<VariantPb, String> {
    let type_desc = build_scalar_type_desc(schema_col, value_type)?;
    if array.is_null(row_idx) {
        return Ok(VariantPb {
            r#type: Some(type_desc),
            value: None,
            variant_type: Some(VariantTypePb::NullValue as i32),
        });
    }
    let value = match value_type {
        SortKeyValueType::TinyInt
        | SortKeyValueType::SmallInt
        | SortKeyValueType::Int
        | SortKeyValueType::BigInt
        | SortKeyValueType::LargeInt => {
            extract_integral_sort_key_value(array, row_idx, col_idx)?.to_string()
        }
        SortKeyValueType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Boolean array at column {}",
                        col_idx
                    )
                })?;
            if typed.value(row_idx) {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        SortKeyValueType::Float => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Float32 array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).to_string()
        }
        SortKeyValueType::Double => {
            let typed = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Float64 array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).to_string()
        }
        SortKeyValueType::Varchar => extract_varchar_sort_key_value(array, row_idx, col_idx)?,
        SortKeyValueType::Date => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Date32 array at column {}",
                        col_idx
                    )
                })?;
            format_date32_sort_key_value(typed.value(row_idx))?
        }
        SortKeyValueType::Datetime => extract_datetime_sort_key_value(array, row_idx, col_idx)?,
        SortKeyValueType::Decimal { wire_type, scale } => {
            if wire_type == StarRocksSegmentWireType::Decimal256 {
                let typed = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| {
                        format!(
                            "sort-key type mismatch: expected Decimal256 array at column {}",
                            col_idx
                        )
                    })?;
                format_decimal256_sort_key_value(typed.value(row_idx), scale)
            } else {
                let typed = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        format!(
                            "sort-key type mismatch: expected Decimal128 array at column {}",
                            col_idx
                        )
                    })?;
                format_decimal_sort_key_value(typed.value(row_idx), scale)
            }
        }
    };

    Ok(VariantPb {
        r#type: Some(type_desc),
        value: Some(value),
        variant_type: Some(VariantTypePb::NormalValue as i32),
    })
}

fn parse_sort_key_value_type(col: &StarRocksColumnSchema) -> Result<SortKeyValueType, String> {
    let type_name = col.r#type.trim().to_ascii_uppercase();
    let base_type = type_name.split('(').next().unwrap_or(type_name.as_str());
    match base_type {
        "TINYINT" => Ok(SortKeyValueType::TinyInt),
        "SMALLINT" => Ok(SortKeyValueType::SmallInt),
        "INT" => Ok(SortKeyValueType::Int),
        "BIGINT" => Ok(SortKeyValueType::BigInt),
        "LARGEINT" => Ok(SortKeyValueType::LargeInt),
        "FLOAT" => Ok(SortKeyValueType::Float),
        "DOUBLE" => Ok(SortKeyValueType::Double),
        "BOOLEAN" => Ok(SortKeyValueType::Boolean),
        "DATE" | "DATE_V2" => Ok(SortKeyValueType::Date),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => Ok(SortKeyValueType::Datetime),
        "CHAR" | "VARCHAR" | "STRING" | "BINARY" | "VARBINARY" => Ok(SortKeyValueType::Varchar),
        "DECIMAL32" => parse_decimal_sort_key_value_type(col, StarRocksSegmentWireType::Decimal32),
        "DECIMAL64" => parse_decimal_sort_key_value_type(col, StarRocksSegmentWireType::Decimal64),
        "DECIMAL128" => {
            parse_decimal_sort_key_value_type(col, StarRocksSegmentWireType::Decimal128)
        }
        "DECIMAL256" => {
            parse_decimal_sort_key_value_type(col, StarRocksSegmentWireType::Decimal256)
        }
        "DECIMAL" => parse_decimal_sort_key_value_type(col, StarRocksSegmentWireType::Decimal),
        "DECIMALV2" => parse_decimal_sort_key_value_type(col, StarRocksSegmentWireType::DecimalV2),
        other => Err(format!(
            "unsupported sort-key schema type for segment metadata writer: {}",
            other
        )),
    }
}

fn build_scalar_type_desc(
    col: &StarRocksColumnSchema,
    value_type: SortKeyValueType,
) -> Result<PTypeDesc, String> {
    let primitive = match value_type {
        SortKeyValueType::Boolean => StarRocksSegmentWireType::Boolean.primitive_code(),
        SortKeyValueType::TinyInt => StarRocksSegmentWireType::TinyInt.primitive_code(),
        SortKeyValueType::SmallInt => StarRocksSegmentWireType::SmallInt.primitive_code(),
        SortKeyValueType::Int => StarRocksSegmentWireType::Int.primitive_code(),
        SortKeyValueType::BigInt => StarRocksSegmentWireType::BigInt.primitive_code(),
        SortKeyValueType::LargeInt => StarRocksSegmentWireType::LargeInt.primitive_code(),
        SortKeyValueType::Float => StarRocksSegmentWireType::Float.primitive_code(),
        SortKeyValueType::Double => StarRocksSegmentWireType::Double.primitive_code(),
        SortKeyValueType::Date => StarRocksSegmentWireType::Date.primitive_code(),
        SortKeyValueType::Datetime => StarRocksSegmentWireType::Datetime.primitive_code(),
        SortKeyValueType::Decimal { wire_type, .. } => wire_type.primitive_code(),
        SortKeyValueType::Varchar => {
            let type_name = col.r#type.trim().to_ascii_uppercase();
            let base_type = type_name.split('(').next().unwrap_or(type_name.as_str());
            match base_type {
                "CHAR" => StarRocksSegmentWireType::Char.primitive_code(),
                "STRING" => StarRocksSegmentWireType::Varchar.primitive_code(),
                "VARCHAR" => StarRocksSegmentWireType::Varchar.primitive_code(),
                "BINARY" => StarRocksSegmentWireType::Binary.primitive_code(),
                "VARBINARY" => StarRocksSegmentWireType::VarBinary.primitive_code(),
                other => {
                    return Err(format!(
                        "unsupported textual schema type for segment metadata writer: {}",
                        other
                    ));
                }
            }
        }
    };
    Ok(PTypeDesc {
        types: vec![PTypeNode {
            r#type: TYPE_NODE_SCALAR,
            scalar_type: Some(PScalarType {
                r#type: primitive,
                len: col.length,
                precision: col.precision,
                scale: col.frac,
            }),
            struct_fields: Vec::new(),
        }],
    })
}

fn parse_decimal_sort_key_value_type(
    col: &StarRocksColumnSchema,
    wire_type: StarRocksSegmentWireType,
) -> Result<SortKeyValueType, String> {
    let raw_scale = col
        .frac
        .ok_or_else(|| format!("decimal sort-key column missing scale: {}", col.r#type))?;
    let scale = i8::try_from(raw_scale).map_err(|_| {
        format!(
            "decimal sort-key column scale overflows i8: type={} scale={}",
            col.r#type, raw_scale
        )
    })?;
    if scale < 0 {
        return Err(format!(
            "decimal sort-key column has negative scale: type={} scale={}",
            col.r#type, scale
        ));
    }
    Ok(SortKeyValueType::Decimal { wire_type, scale })
}

fn extract_integral_sort_key_value(
    array: &ArrayRef,
    row_idx: usize,
    col_idx: usize,
) -> Result<i128, String> {
    match array.data_type() {
        DataType::Int8 => {
            let typed = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::Int16 => {
            let typed = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::Int32 => {
            let typed = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::Int64 => {
            let typed = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt8 => {
            let typed = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt16 => {
            let typed = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt32 => {
            let typed = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt64 => {
            let typed = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let typed = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected LARGEINT FixedSizeBinary array at column {}",
                        col_idx
                    )
                })?;
            largeint::i128_from_be_bytes(typed.value(row_idx)).map_err(|e| {
                format!(
                    "decode LARGEINT sort-key value failed: column={}, row={}, error={}",
                    col_idx, row_idx, e
                )
            })
        }
        other => Err(format!(
            "sort-key type mismatch: expected integral/LARGEINT array at column {} (actual={:?})",
            col_idx, other
        )),
    }
}

fn extract_varchar_sort_key_value(
    array: &ArrayRef,
    row_idx: usize,
    col_idx: usize,
) -> Result<String, String> {
    if let Some(typed) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(typed.value(row_idx).to_string());
    }
    if let Some(typed) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(typed.value(row_idx).to_string());
    }
    if let Some(typed) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(String::from_utf8_lossy(typed.value(row_idx)).to_string());
    }
    if let Some(typed) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(String::from_utf8_lossy(typed.value(row_idx)).to_string());
    }
    Err(format!(
        "sort-key type mismatch: expected textual array at column {} (actual={:?})",
        col_idx,
        array.data_type()
    ))
}

fn extract_datetime_sort_key_value(
    array: &ArrayRef,
    row_idx: usize,
    col_idx: usize,
) -> Result<String, String> {
    let micros = match array.data_type() {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).saturating_mul(1_000)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).saturating_mul(1_000_000)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx) / 1_000
        }
        other => {
            return Err(format!(
                "sort-key type mismatch: expected timestamp array at column {} (actual={:?})",
                col_idx, other
            ));
        }
    };
    format_datetime_sort_key_value(micros)
}

fn format_date32_sort_key_value(days_since_epoch: i32) -> Result<String, String> {
    let days_from_ce = DATE32_UNIX_EPOCH_DAY_OFFSET
        .checked_add(days_since_epoch)
        .ok_or_else(|| {
            format!(
                "date32 day overflow when formatting sort-key value: {}",
                days_since_epoch
            )
        })?;
    let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce).ok_or_else(|| {
        format!(
            "invalid date32 value for sort-key formatting: {}",
            days_since_epoch
        )
    })?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn format_datetime_sort_key_value(unix_micros: i64) -> Result<String, String> {
    let dt = DateTime::from_timestamp_micros(unix_micros).ok_or_else(|| {
        format!(
            "invalid unix micros for datetime sort-key formatting: {}",
            unix_micros
        )
    })?;
    Ok(dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

fn format_decimal_sort_key_value(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let mut digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        let mut padded = String::with_capacity(scale + 1);
        for _ in 0..=(scale - digits.len()) {
            padded.push('0');
        }
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - scale;
    let sign = if unscaled < 0 { "-" } else { "" };
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
}

fn format_decimal256_sort_key_value(unscaled: i256, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let mut digits = abs.to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        let mut padded = String::with_capacity(scale + 1);
        for _ in 0..=(scale - digits.len()) {
            padded.push('0');
        }
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - scale;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
}
