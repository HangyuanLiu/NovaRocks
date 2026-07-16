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

use crate::cache::ExternalDataCacheRangeOptions;
use crate::connector::iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use crate::connector::iceberg::file_pruning::IcebergFilePruningMetadata;
use crate::fs::scan_context::FileScanRange;
use crate::proto::{novarocks, plan};
use crate::sql::planner::table::IcebergColumnStats;

pub(super) fn decode_file_scan_ranges(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    ranges: &[novarocks::ScanRangeParams],
) -> Result<Vec<FileScanRange>, String> {
    ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            if range.has_more.unwrap_or(false) {
                return Err(format!(
                    "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
                ));
            }
            if range.empty.unwrap_or(false) {
                Ok(None)
            } else {
                decode_file_scan_range(node_id, table, idx, range).map(Some)
            }
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|ranges| ranges.into_iter().flatten().collect())
}

fn decode_file_scan_range(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    idx: usize,
    range: &novarocks::ScanRangeParams,
) -> Result<FileScanRange, String> {
    if range.has_more.unwrap_or(false) {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
        ));
    }
    let Some(novarocks::scan_range::Kind::File(file)) =
        range.range.as_ref().and_then(|range| range.kind.as_ref())
    else {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} expected file range"
        ));
    };
    if !file.file_format.eq_ignore_ascii_case("PARQUET") {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} unsupported file_format {}; only PARQUET is supported",
            file.file_format
        ));
    }
    let path = file_range_path(table, file)?;
    let file_len = nonnegative_u64(file.file_length, "file_length")?;
    let offset = nonnegative_u64(file.offset, "offset")?;
    if offset > file_len {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} offset {} exceeds file_length {}",
            file.offset, file.file_length
        ));
    }
    let length = if file.length > 0 {
        nonnegative_u64(file.length, "length")?
    } else {
        file_len - offset
    };
    let mut delete_files = decode_delete_files(node_id, idx, &file.delete_files)?;
    if let Some(dv) = file.deletion_vector_descriptor.as_ref() {
        delete_files.push(decode_deletion_vector_descriptor(node_id, idx, dv)?);
    }
    Ok(FileScanRange {
        path,
        file_len,
        offset,
        length,
        scan_range_id: i32::try_from(idx)
            .map_err(|_| format!("ScanNode node_id={node_id} range index overflow"))?,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: decode_change_op(node_id, idx, file.change_op)?,
        included_positions: if file.included_positions.is_empty() {
            None
        } else {
            Some(file.included_positions.clone())
        },
        external_datacache: file_external_datacache(file),
        delete_files,
        iceberg_file_pruning: file_pruning_metadata_from_native(
            node_id,
            idx,
            table,
            &file.file_pruning_min_max_values,
        )?,
    })
}

fn decode_change_op(node_id: i32, idx: usize, value: Option<i32>) -> Result<Option<i8>, String> {
    value
        .map(|value| {
            let change_op = i8::try_from(value).map_err(|_| {
                format!("ScanNode node_id={node_id} range {idx} change_op {value} exceeds i8 range")
            })?;
            crate::exec::change_op::validate_change_op_value(change_op)?;
            Ok(change_op)
        })
        .transpose()
}

fn file_pruning_metadata_from_native(
    node_id: i32,
    range_idx: usize,
    table: &plan::IcebergTableInfo,
    values: &HashMap<i32, novarocks::FilePruningMinMaxValue>,
) -> Result<Option<IcebergFilePruningMetadata>, String> {
    if values.is_empty() {
        return Ok(None);
    }
    let Some(schema) = table.schema.as_ref() else {
        return Ok(None);
    };
    let mut columns = HashMap::new();
    for (ordinal, value) in values {
        let ordinal_usize = usize::try_from(*ordinal).map_err(|_| {
            format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning ordinal {ordinal} must be non-negative"
            )
        })?;
        let Some(field) = schema.fields.get(ordinal_usize) else {
            return Err(format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning ordinal {ordinal} exceeds Iceberg schema field count {}",
                schema.fields.len()
            ));
        };
        let Some(stats) = column_stats_from_native_min_max_value(node_id, range_idx, value)? else {
            continue;
        };
        columns.insert(field.name.clone(), stats);
    }
    if columns.is_empty() {
        Ok(None)
    } else {
        Ok(Some(IcebergFilePruningMetadata { columns }))
    }
}

fn column_stats_from_native_min_max_value(
    node_id: i32,
    range_idx: usize,
    value: &novarocks::FilePruningMinMaxValue,
) -> Result<Option<IcebergColumnStats>, String> {
    let (lower_bound, upper_bound) = match value.value_kind {
        1 => {
            let lower = bool_bound_to_byte(value.min_int_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} bool file pruning min_int_value missing"
                )
            })?)?;
            let upper = bool_bound_to_byte(value.max_int_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} bool file pruning max_int_value missing"
                )
            })?)?;
            (vec![lower], vec![upper])
        }
        2 => (
            value
                .min_int_value
                .ok_or_else(|| {
                    format!(
                        "ScanNode node_id={node_id} range {range_idx} int file pruning min_int_value missing"
                    )
                })?
                .to_le_bytes()
                .to_vec(),
            value
                .max_int_value
                .ok_or_else(|| {
                    format!(
                        "ScanNode node_id={node_id} range {range_idx} int file pruning max_int_value missing"
                    )
                })?
                .to_le_bytes()
                .to_vec(),
        ),
        3 => {
            let lower = value.min_float_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} float file pruning min_float_value missing"
                )
            })?;
            let upper = value.max_float_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} float file pruning max_float_value missing"
                )
            })?;
            if lower.is_nan() || upper.is_nan() {
                return Ok(None);
            }
            (lower.to_le_bytes().to_vec(), upper.to_le_bytes().to_vec())
        }
        0 => {
            return Err(format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning value_kind is unspecified"
            ));
        }
        other => {
            return Err(format!(
                "ScanNode node_id={node_id} range {range_idx} unsupported file pruning value_kind {other}"
            ));
        }
    };

    Ok(Some(IcebergColumnStats {
        null_count: Some(if value.has_null { 1 } else { 0 }),
        value_count: None,
        column_size: None,
        lower_bound: Some(lower_bound),
        upper_bound: Some(upper_bound),
    }))
}

fn bool_bound_to_byte(value: i64) -> Result<u8, String> {
    match value {
        0 => Ok(0),
        1 => Ok(1),
        _ => Err(format!(
            "bool file pruning bound must be 0 or 1, got {value}"
        )),
    }
}

fn file_range_path(
    table: &plan::IcebergTableInfo,
    file: &novarocks::FileScanRange,
) -> Result<String, String> {
    if let Some(path) = file.full_path.as_deref()
        && !path.is_empty()
    {
        return Ok(path.to_string());
    }
    let Some(relative_path) = file
        .relative_path
        .as_deref()
        .filter(|path| !path.is_empty())
    else {
        return Err("file range missing full_path/relative_path".to_string());
    };
    if table.location.is_empty() {
        return Err("HDFS relative_path requires Iceberg table location".to_string());
    }
    Ok(format!(
        "{}/{}",
        table.location.trim_end_matches('/'),
        relative_path.trim_start_matches('/')
    ))
}

fn nonnegative_u64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value)
        .map_err(|_| format!("file range {field} must be non-negative, got {value}"))
}

fn file_external_datacache(
    file: &novarocks::FileScanRange,
) -> Option<ExternalDataCacheRangeOptions> {
    file.datacache_options
        .as_ref()
        .map(|opts| ExternalDataCacheRangeOptions {
            modification_time: file.modification_time,
            enable_populate_datacache: opts.enable_populate_datacache,
            datacache_priority: opts.priority,
            candidate_node: None,
        })
}

fn decode_delete_files(
    node_id: i32,
    range_idx: usize,
    delete_files: &[novarocks::IcebergDeleteFile],
) -> Result<Vec<IcebergDeleteFileSpec>, String> {
    delete_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let path = file.full_path.clone().ok_or_else(|| {
                format!("ScanNode node_id={node_id} range {range_idx} delete file {idx} full_path missing")
            })?;
            let file_format = match file.file_format.to_ascii_uppercase().as_str() {
                "PARQUET" => IcebergFileFormat::Parquet,
                other => {
                    return Err(format!(
                        "ScanNode node_id={node_id} range {range_idx} delete file {idx} unsupported file_format {other}"
                    ));
                }
            };
            let file_content = match file.file_content.to_ascii_uppercase().as_str() {
                "POSITION_DELETES" => IcebergFileContent::PositionDeletes,
                "EQUALITY_DELETES" => IcebergFileContent::EqualityDeletes,
                other => {
                    return Err(format!(
                        "ScanNode node_id={node_id} range {range_idx} delete file {idx} unsupported file_content {other}"
                    ));
                }
            };
            let length = file
                .length
                .map(|value| nonnegative_u64(value, "delete_file.length"))
                .transpose()?;
            Ok(IcebergDeleteFileSpec {
                path,
                file_format,
                file_content,
                length,
                content_offset: None,
                content_size_in_bytes: None,
            })
        })
        .collect()
}

fn decode_deletion_vector_descriptor(
    node_id: i32,
    range_idx: usize,
    dv: &novarocks::DeletionVectorDescriptor,
) -> Result<IcebergDeleteFileSpec, String> {
    let path = dv
        .path_or_inline_dv
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            format!(
                "ScanNode node_id={node_id} range {range_idx} deletion vector is missing path_or_inline_dv"
            )
        })?
        .to_string();
    let offset = dv.offset.ok_or_else(|| {
        format!(
            "ScanNode node_id={node_id} range {range_idx} deletion vector {path} is missing offset"
        )
    })?;
    let size = dv.size_in_bytes.ok_or_else(|| {
        format!(
            "ScanNode node_id={node_id} range {range_idx} deletion vector {path} is missing size_in_bytes"
        )
    })?;
    Ok(IcebergDeleteFileSpec::puffin_position_delete(
        path, None, offset, size,
    ))
}
