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

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::DataType;

use super::common::column_def_data_type;
use crate::cache::ExternalDataCacheRangeOptions;
use crate::connector::iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use crate::connector::iceberg::file_pruning::IcebergFilePruningMetadata;
use crate::connector::iceberg::scan_model::IcebergColumnStats;
use crate::fs::scan_context::FileScanRange;
use crate::proto::plan;
use crate::protocol::common::error::ProtocolErrorKind;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;
use crate::runtime::scan_range::{
    DeletionVectorDescriptor, FileFormat, FilePruningMinMaxValue, FilePruningValueKind,
    FileScanRange as RuntimeFileScanRange, IcebergDeleteFile,
    IcebergFileContent as RuntimeIcebergFileContent, IcebergFileFormat as RuntimeIcebergFileFormat,
    ScanRange, ScanRangeParams,
};

pub(super) fn decode_file_scan_ranges(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    table_columns: &[plan::ColumnDef],
    ranges: &[ScanRangeParams],
) -> Result<Vec<FileScanRange>, NativeFragmentLeafDecodeError> {
    Ok(ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            if range.has_more.unwrap_or(false) {
                return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "has_more", format!(
                    "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
                )).prepend_index(idx).prepend_field("ranges"));
            }
            if range.empty.unwrap_or(false) {
                Ok(None)
            } else {
                decode_file_scan_range(node_id, table, table_columns, idx, range)
                    .map_err(|error| error.prepend_index(idx).prepend_field("ranges"))
                    .map(Some)
            }
        })
        .collect::<Result<Vec<_>, NativeFragmentLeafDecodeError>>()
        .map(|ranges| ranges.into_iter().flatten().collect())?)
}

fn decode_file_scan_range(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    table_columns: &[plan::ColumnDef],
    idx: usize,
    range: &ScanRangeParams,
) -> Result<FileScanRange, NativeFragmentLeafDecodeError> {
    if range.has_more.unwrap_or(false) {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "has_more",
            format!(
                "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
            ),
        ));
    }
    let ScanRange::File(file) = &range.range else {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "range",
            format!("ScanNode node_id={node_id} range {idx} expected file range"),
        ));
    };
    if file.file_format != FileFormat::Parquet {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "file_format",
            format!(
                "ScanNode node_id={node_id} range {idx} unsupported file_format {}; only PARQUET is supported",
                file.file_format.as_native_name()
            ),
        ));
    }
    let path = file_range_path(table, file)?;
    let file_len = nonnegative_u64(file.file_length, "file_length")?;
    let offset = nonnegative_u64(file.offset, "offset")?;
    if offset > file_len {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "offset",
            format!(
                "ScanNode node_id={node_id} range {idx} offset {} exceeds file_length {}",
                file.offset, file.file_length
            ),
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
        scan_range_id: i32::try_from(idx).map_err(|_| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::OutOfRange,
                "range",
                format!("ScanNode node_id={node_id} range index overflow"),
            )
        })?,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: decode_change_op(node_id, idx, file.ivm_change_op).map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "ivm_change_op",
                error,
            )
        })?,
        included_positions: if file.included_positions.is_empty() {
            None
        } else {
            Some(file.included_positions.clone())
        },
        external_datacache: file_external_datacache(file),
        delete_files,
        iceberg_file_pruning: file_pruning_metadata_from_assignment(
            node_id,
            idx,
            table,
            table_columns,
            file.file_pruning_min_max_values.as_ref(),
        )?,
    })
}

fn decode_change_op(node_id: i32, idx: usize, value: Option<i8>) -> Result<Option<i8>, String> {
    if let Some(value) = value {
        crate::exec::change_op::validate_change_op_value(value).map_err(|error| {
            format!("ScanNode node_id={node_id} range {idx} change_op: {error}")
        })?;
    }
    Ok(value)
}

fn file_pruning_metadata_from_assignment(
    node_id: i32,
    range_idx: usize,
    table: &plan::IcebergTableInfo,
    table_columns: &[plan::ColumnDef],
    values: Option<&BTreeMap<i32, FilePruningMinMaxValue>>,
) -> Result<Option<IcebergFilePruningMetadata>, NativeFragmentLeafDecodeError> {
    let Some(values) = values else {
        return Ok(None);
    };
    let Some(schema) = table.schema.as_ref() else {
        return Ok(None);
    };
    let mut columns = HashMap::new();
    for (ordinal, value) in values {
        let ordinal_usize = usize::try_from(*ordinal).map_err(|_| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "file_pruning_min_max_values", format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning ordinal {ordinal} must be non-negative"
            ))
        })?;
        let Some(field) = schema.fields.get(ordinal_usize) else {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::OutOfRange,
                "file_pruning_min_max_values",
                format!(
                    "ScanNode node_id={node_id} range {range_idx} file pruning ordinal {ordinal} exceeds Iceberg schema field count {}",
                    schema.fields.len()
                ),
            ));
        };
        let Some(column) = table_columns
            .iter()
            .find(|column| column.name == field.name)
        else {
            continue;
        };
        // The wire kind deliberately omits integer/float width. Restore it only
        // from the authoritative ScanNode table type; missing or incompatible
        // type evidence omits optional pruning metadata and keeps the file.
        let Ok(data_type) = column_def_data_type(column) else {
            continue;
        };
        let Some(stats) = column_stats_from_min_max_value(node_id, range_idx, value, &data_type)
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "file_pruning_min_max_values",
                    error,
                )
            })?
        else {
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

fn column_stats_from_min_max_value(
    node_id: i32,
    range_idx: usize,
    value: &FilePruningMinMaxValue,
    data_type: &DataType,
) -> Result<Option<IcebergColumnStats>, String> {
    let (lower_bound, upper_bound) = match value.value_kind {
        FilePruningValueKind::Bool => {
            if data_type != &DataType::Boolean {
                return Ok(None);
            }
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
        FilePruningValueKind::Int => {
            let lower = value.min_int_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} int file pruning min_int_value missing"
                )
            })?;
            let upper = value.max_int_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} int file pruning max_int_value missing"
                )
            })?;
            let Some(lower) = int_bound_bytes(lower, data_type) else {
                return Ok(None);
            };
            let Some(upper) = int_bound_bytes(upper, data_type) else {
                return Ok(None);
            };
            (lower, upper)
        }
        FilePruningValueKind::Float => {
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
            let Some(lower) = float_bound_bytes(lower, data_type) else {
                return Ok(None);
            };
            let Some(upper) = float_bound_bytes(upper, data_type) else {
                return Ok(None);
            };
            (lower, upper)
        }
    };

    // The native assignment carries exact null-state booleans rather than the
    // original Iceberg row counts. Preserve that evidence with normalized
    // counts so late ordered pruning can distinguish non-null, nullable, and
    // all-null files without guessing an actual file row count.
    let (value_count, null_count) = if value.all_null {
        (1, 1)
    } else if value.has_null {
        (2, 1)
    } else {
        (1, 0)
    };
    Ok(Some(IcebergColumnStats {
        null_count: Some(null_count),
        value_count: Some(value_count),
        column_size: None,
        lower_bound: Some(lower_bound),
        upper_bound: Some(upper_bound),
    }))
}

fn int_bound_bytes(value: i64, data_type: &DataType) -> Option<Vec<u8>> {
    match data_type {
        DataType::Int8 => i8::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .ok(),
        DataType::Int16 => i16::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .ok(),
        DataType::Int32 => i32::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .ok(),
        DataType::Int64 => Some(value.to_le_bytes().to_vec()),
        _ => None,
    }
}

fn float_bound_bytes(value: f64, data_type: &DataType) -> Option<Vec<u8>> {
    match data_type {
        DataType::Float32 => {
            let narrowed = value as f32;
            if f64::from(narrowed) != value {
                return None;
            }
            Some(narrowed.to_le_bytes().to_vec())
        }
        DataType::Float64 => Some(value.to_le_bytes().to_vec()),
        _ => None,
    }
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
    file: &RuntimeFileScanRange,
) -> Result<String, NativeFragmentLeafDecodeError> {
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
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "full_path",
            "file range missing full_path/relative_path",
        ));
    };
    if table.location.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "relative_path",
            "HDFS relative_path requires Iceberg table location",
        ));
    }
    Ok(format!(
        "{}/{}",
        table.location.trim_end_matches('/'),
        relative_path.trim_start_matches('/')
    ))
}

fn nonnegative_u64(value: i64, field: &'static str) -> Result<u64, NativeFragmentLeafDecodeError> {
    u64::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("file range {field} must be non-negative, got {value}"),
        )
    })
}

fn file_external_datacache(file: &RuntimeFileScanRange) -> Option<ExternalDataCacheRangeOptions> {
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
    delete_files: &[IcebergDeleteFile],
) -> Result<Vec<IcebergDeleteFileSpec>, NativeFragmentLeafDecodeError> {
    delete_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let path = file.full_path.clone().ok_or_else(|| NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "full_path",
                format!("ScanNode node_id={node_id} range {range_idx} delete file {idx} full_path missing"),
            ).prepend_index(idx).prepend_field("delete_files"))?;
            let file_format = match file.file_format {
                RuntimeIcebergFileFormat::Parquet => IcebergFileFormat::Parquet,
            };
            let file_content = match file.file_content {
                RuntimeIcebergFileContent::PositionDeletes => IcebergFileContent::PositionDeletes,
                RuntimeIcebergFileContent::EqualityDeletes => IcebergFileContent::EqualityDeletes,
            };
            let length = file
                .length
                .map(|value| nonnegative_u64(value, "length").map_err(|error| error.prepend_index(idx).prepend_field("delete_files")))
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
    dv: &DeletionVectorDescriptor,
) -> Result<IcebergDeleteFileSpec, NativeFragmentLeafDecodeError> {
    let path = dv
        .path_or_inline_dv
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "path_or_inline_dv", format!(
                "ScanNode node_id={node_id} range {range_idx} deletion vector is missing path_or_inline_dv"
            )).prepend_field("deletion_vector_descriptor")
        })?
        .to_string();
    let offset = dv.offset.ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "offset", format!(
            "ScanNode node_id={node_id} range {range_idx} deletion vector {path} is missing offset"
        )).prepend_field("deletion_vector_descriptor")
    })?;
    let size = dv.size_in_bytes.ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::MissingField, "size_in_bytes", format!(
            "ScanNode node_id={node_id} range {range_idx} deletion vector {path} is missing size_in_bytes"
        )).prepend_field("deletion_vector_descriptor")
    })?;
    Ok(IcebergDeleteFileSpec::puffin_position_delete(
        path, None, offset, size,
    ))
}
