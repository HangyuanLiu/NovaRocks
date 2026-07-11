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

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::connector::iceberg::scan_planner::{
    IcebergScanHandle, iceberg_scan_handle, iceberg_split,
};
use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
use crate::runtime::scan_range;
use crate::sql::catalog::{
    ColumnDef, IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
    IcebergDeleteFileInfo,
};
use arrow::datatypes::DataType;

const ICEBERG_SCAN_SPLIT_TARGET_BYTES: i64 = 128 * 1024 * 1024;
const ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE: usize = 1024;
const ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE: i64 = 512 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectorScanContext {
    pub(crate) min_max_predicates: Vec<MinMaxPredicate>,
    pub(crate) columns: Vec<ColumnDef>,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeFileScanPlan {
    pub(crate) scan_ranges: Vec<scan_range::ScanRangeParams>,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedNativeStarRocksScan {
    pub(crate) ranges: Vec<scan_range::ScanRangeParams>,
    pub(crate) source: crate::sql::codegen::proto_encode::plan::StarRocksScanSourceDescriptor,
}

pub(crate) fn plan_native_starrocks_scan_node(
    scan_node_id: i32,
    scan: &crate::sql::planner::payload::PlanScanNode,
    connectors: &crate::connector::ConnectorRegistry,
) -> Result<PlannedNativeStarRocksScan, String> {
    #[cfg(not(feature = "compat"))]
    {
        let _ = (scan_node_id, scan, connectors);
        Err("StarRocks native scan planning requires feature compat".to_string())
    }
    #[cfg(feature = "compat")]
    {
        use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext};
        use crate::connector::starrocks::table::scan_planner::{
            StarRocksTableScanPlanner, starrocks_scan_handle, starrocks_split,
        };
        use crate::sql::catalog::ScanSource;

        let ScanSource::StarRocks { db_id, table_id } = &scan.table.source else {
            return Err(format!(
                "native StarRocks scan planning node_id={scan_node_id} received non-StarRocks source"
            ));
        };
        for (field, value) in [("db_id", *db_id), ("table_id", *table_id)] {
            if value <= 0 {
                return Err(format!(
                    "StarRocks ScanNode node_id={scan_node_id} {field} must be positive, got {value}"
                ));
            }
        }
        let planner = connectors.scan_planner("starrocks")?;
        let table_handle = StarRocksTableScanPlanner::table_handle_from_source(
            &scan.database,
            &scan.table.name,
            *db_id,
            *table_id,
        );
        let scan_handle = planner.begin_scan(table_handle, BeginScanContext::default())?;
        let handle = starrocks_scan_handle(&scan_handle)?;
        if handle.table.db_id != *db_id || handle.table.table_id != *table_id {
            return Err(format!(
                "StarRocks ScanNode node_id={scan_node_id} planned scan handle identity mismatch: source=({db_id}, {table_id}) handle=({}, {})",
                handle.table.db_id, handle.table.table_id
            ));
        }
        let native_source = handle.native_source();
        let source = crate::sql::codegen::proto_encode::plan::StarRocksScanSourceDescriptor {
            catalog_name: native_source.catalog_name,
            db_id: native_source.db_id,
            table_id: native_source.table_id,
            schema_id: native_source.schema_id,
            storage_columns: native_source
                .storage_columns
                .into_iter()
                .map(|column| {
                    crate::sql::codegen::proto_encode::plan::StarRocksStorageColumnDescriptor {
                        name: column.name,
                        unique_id: column.unique_id,
                        default_value: column.default_value,
                    }
                })
                .collect(),
            tablet_schema:
                crate::sql::codegen::proto_encode::plan::starrocks_tablet_schema_descriptor(
                    native_source.tablet_schema,
                ),
        };
        let splits = planner.plan_splits(&scan_handle, SplitPlanningContext::default())?;
        if splits.is_empty() {
            return Err(format!(
                "StarRocks table {}.{} has no selected tablet splits",
                scan.database, scan.table.name
            ));
        }
        let mut tablets = std::collections::HashSet::new();
        let mut ranges = Vec::with_capacity(splits.len());
        for split in &splits {
            let split = starrocks_split(split)?;
            if !tablets.insert(split.tablet_id) {
                return Err(format!(
                    "StarRocks ScanNode node_id={scan_node_id} has duplicate tablet_id={}",
                    split.tablet_id
                ));
            }
            ranges.push(scan_range::ScanRangeParams::starrocks_tablet(
                split.tablet_id,
                split.partition_id,
                split.version,
            )?);
        }
        Ok(PlannedNativeStarRocksScan { ranges, source })
    }
}

pub(crate) fn to_native_file_scan(
    connector_id: &str,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ConnectorScanContext,
) -> Result<NativeFileScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    match connector_id {
        "iceberg" => iceberg_to_native_file_scan(scan, splits, ctx),
        other => Err(format!(
            "unsupported connector native file scan emitter: {other}"
        )),
    }
}

fn iceberg_to_native_file_scan(
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ConnectorScanContext,
) -> Result<NativeFileScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan = iceberg_scan_handle(scan)?;
    let scan_ranges = build_iceberg_native_scan_ranges(scan, splits, &ctx)?;
    Ok(NativeFileScanPlan { scan_ranges })
}

fn build_iceberg_native_scan_ranges(
    scan: &IcebergScanHandle,
    splits: &[Split],
    ctx: &ConnectorScanContext,
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    let mut ranges = Vec::new();
    let scan_predicates =
        crate::connector::iceberg::file_pruning::min_max_predicates_to_scan_predicates(
            &ctx.min_max_predicates,
        );
    let mut pruning_counters =
        crate::connector::iceberg::file_pruning::IcebergFilePruningCounters::default();
    let pruning_columns = pruning_columns_for_scan(scan, &ctx.columns)?;
    for split in splits {
        let file = &iceberg_split(split)?.data_file;
        if !crate::connector::iceberg::file_pruning::file_may_satisfy_scan_predicates(
            file,
            &scan_predicates,
            &mut pruning_counters,
        ) {
            continue;
        }
        ranges.extend(build_native_file_scan_range_params_for_file(
            file,
            &pruning_columns,
        )?);
    }
    Ok(ranges)
}

#[derive(Clone, Debug)]
struct PruningColumn {
    schema_ordinal: i32,
    column: ColumnDef,
}

fn pruning_columns_for_scan(
    scan: &IcebergScanHandle,
    columns: &[ColumnDef],
) -> Result<Vec<PruningColumn>, String> {
    scan.table
        .table_info
        .schema
        .fields
        .iter()
        .enumerate()
        .filter_map(|(schema_ordinal, field)| {
            scan.table
                .column_names
                .iter()
                .any(|column_name| column_name.eq_ignore_ascii_case(&field.name))
                .then_some((schema_ordinal, field))
        })
        .map(|(schema_ordinal, field)| {
            let schema_ordinal = i32::try_from(schema_ordinal).map_err(|_| {
                format!(
                    "Iceberg table {}.{} schema field ordinal overflow for {}",
                    scan.table.namespace, scan.table.table, field.name
                )
            })?;
            let column = columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&field.name))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "Iceberg table {}.{} scan column {} missing from resolved table columns",
                        scan.table.namespace, scan.table.table, field.name
                    )
                })?;
            Ok(PruningColumn {
                schema_ordinal,
                column,
            })
        })
        .collect()
}

fn build_native_file_scan_range_params_for_file(
    file: &IcebergDataFileInfo,
    columns: &[PruningColumn],
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    validate_iceberg_delete_apply_cost(&file.path, &file.delete_files)?;
    let splits = plan_hdfs_file_splits(file);
    let file_pruning_min_max_values = native_file_pruning_min_max_values(file, columns);
    splits
        .into_iter()
        .map(|(offset, length)| {
            build_native_file_scan_range_params(
                &file.path,
                file.size,
                offset,
                length,
                file.first_row_id,
                file.data_sequence_number,
                file.ivm_change_op,
                file.included_positions.as_ref(),
                &file.delete_files,
                file_pruning_min_max_values.clone(),
            )
        })
        .collect()
}

fn native_file_pruning_min_max_values(
    file: &IcebergDataFileInfo,
    columns: &[PruningColumn],
) -> Option<BTreeMap<i32, scan_range::FilePruningMinMaxValue>> {
    let stats = file.column_stats.as_ref()?;
    if stats.is_empty() || columns.is_empty() {
        return None;
    }

    let mut out = BTreeMap::new();
    for column in columns {
        let Some(stat) = find_column_stats(stats, &column.column.name) else {
            continue;
        };
        let Some(value) = native_min_max_value_from_stats(stat, &column.column.data_type) else {
            continue;
        };
        out.insert(column.schema_ordinal, value);
    }

    if out.is_empty() { None } else { Some(out) }
}

fn find_column_stats<'a>(
    stats: &'a HashMap<String, crate::sql::catalog::IcebergColumnStats>,
    column: &str,
) -> Option<&'a crate::sql::catalog::IcebergColumnStats> {
    stats.get(column).or_else(|| {
        stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, stats)| stats)
    })
}

fn native_min_max_value_from_stats(
    stats: &crate::sql::catalog::IcebergColumnStats,
    data_type: &DataType,
) -> Option<scan_range::FilePruningMinMaxValue> {
    let has_null = stats.null_count.unwrap_or(0) > 0;
    let all_null = stats
        .value_count
        .zip(stats.null_count)
        .is_some_and(|(value_count, null_count)| value_count > 0 && value_count == null_count);

    match data_type {
        DataType::Boolean => {
            let lower = stats.lower_bound.as_deref().and_then(decode_bool_bound)?;
            let upper = stats.upper_bound.as_deref().and_then(decode_bool_bound)?;
            Some(scan_range::FilePruningMinMaxValue {
                value_kind: scan_range::FilePruningValueKind::Bool,
                has_null,
                all_null,
                min_int_value: Some(i64::from(lower)),
                max_int_value: Some(i64::from(upper)),
                min_float_value: None,
                max_float_value: None,
            })
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let lower = stats
                .lower_bound
                .as_deref()
                .and_then(|bytes| decode_int_bound_for_type(bytes, data_type))?;
            let upper = stats
                .upper_bound
                .as_deref()
                .and_then(|bytes| decode_int_bound_for_type(bytes, data_type))?;
            Some(scan_range::FilePruningMinMaxValue {
                value_kind: scan_range::FilePruningValueKind::Int,
                has_null,
                all_null,
                min_int_value: Some(lower),
                max_int_value: Some(upper),
                min_float_value: None,
                max_float_value: None,
            })
        }
        DataType::Float32 | DataType::Float64 => {
            let lower = stats
                .lower_bound
                .as_deref()
                .and_then(|bytes| decode_float_bound_for_type(bytes, data_type))?;
            let upper = stats
                .upper_bound
                .as_deref()
                .and_then(|bytes| decode_float_bound_for_type(bytes, data_type))?;
            if lower.is_nan() || upper.is_nan() {
                return None;
            }
            Some(scan_range::FilePruningMinMaxValue {
                value_kind: scan_range::FilePruningValueKind::Float,
                has_null,
                all_null,
                min_int_value: None,
                max_int_value: None,
                min_float_value: Some(lower),
                max_float_value: Some(upper),
            })
        }
        _ => None,
    }
}

fn decode_bool_bound(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [0] => Some(false),
        [1] => Some(true),
        _ => None,
    }
}

fn decode_int_bound_for_type(bytes: &[u8], data_type: &DataType) -> Option<i64> {
    match data_type {
        DataType::Int8 => {
            let arr: [u8; 1] = bytes.try_into().ok()?;
            Some(i64::from(i8::from_le_bytes(arr)))
        }
        DataType::Int16 => {
            let arr: [u8; 2] = bytes.try_into().ok()?;
            Some(i64::from(i16::from_le_bytes(arr)))
        }
        DataType::Int32 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(i64::from(i32::from_le_bytes(arr)))
        }
        DataType::Int64 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(i64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn decode_float_bound_for_type(bytes: &[u8], data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Float32 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(f64::from(f32::from_le_bytes(arr)))
        }
        DataType::Float64 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(f64::from_le_bytes(arr))
        }
        _ => None,
    }
}

#[cfg(test)]
fn pruning_columns_from_column_order_for_test(
    columns: &[ColumnDef],
) -> Result<Vec<PruningColumn>, String> {
    columns
        .iter()
        .enumerate()
        .map(|(schema_ordinal, column)| {
            Ok(PruningColumn {
                schema_ordinal: i32::try_from(schema_ordinal)
                    .map_err(|_| "test schema ordinal overflow".to_string())?,
                column: column.clone(),
            })
        })
        .collect()
}

fn plan_hdfs_file_splits(file: &IcebergDataFileInfo) -> Vec<(i64, i64)> {
    let file_len = file.size.max(0);
    if file_len <= ICEBERG_SCAN_SPLIT_TARGET_BYTES
        || file.first_row_id.is_some()
        || !file.delete_files.is_empty()
        || file.included_positions.is_some()
    {
        return vec![(0, file_len)];
    }

    let mut out = Vec::new();
    let mut offset = 0_i64;
    while offset < file_len {
        let remaining = file_len - offset;
        let length = remaining.min(ICEBERG_SCAN_SPLIT_TARGET_BYTES);
        out.push((offset, length));
        offset += length;
    }
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

fn validate_iceberg_delete_apply_cost(
    data_path: &str,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<(), String> {
    if delete_files.len() > ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE {
        return Err(format!(
            "too many Iceberg delete files attached to data file {data_path}: count={} max={}",
            delete_files.len(),
            ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE
        ));
    }
    let total_bytes = delete_files.iter().try_fold(0_i64, |acc, delete_file| {
        let Some(length) = delete_file.length else {
            return Ok(acc);
        };
        acc.checked_add(length.max(0))
            .ok_or_else(|| format!("Iceberg delete file length overflow for data file {data_path}"))
    })?;
    if total_bytes > ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE {
        return Err(format!(
            "Iceberg delete files attached to data file {data_path} are too large: bytes={total_bytes} max={ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE}"
        ));
    }
    Ok(())
}

pub(crate) fn build_native_file_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    included_positions: Option<&Vec<i64>>,
    delete_files: &[IcebergDeleteFileInfo],
    file_pruning_min_max_values: Option<BTreeMap<i32, scan_range::FilePruningMinMaxValue>>,
) -> Result<scan_range::ScanRangeParams, String> {
    let mut parquet_delete_files = Vec::new();
    let mut deletion_vector_descriptor = None;
    for delete_file in delete_files {
        match delete_file.file_format {
            IcebergDeleteFileFormat::Parquet => {
                let file_content = match delete_file.file_content {
                    IcebergDeleteFileContent::Position => {
                        scan_range::IcebergFileContent::PositionDeletes
                    }
                    IcebergDeleteFileContent::Equality => {
                        // Equality field IDs are read from the equality-delete Parquet schema by
                        // the Rust scan runner. The scan range only needs to identify the
                        // delete file as an equality-delete file.
                        scan_range::IcebergFileContent::EqualityDeletes
                    }
                };
                parquet_delete_files.push(scan_range::IcebergDeleteFile {
                    full_path: Some(delete_file.path.clone()),
                    file_format: scan_range::IcebergFileFormat::Parquet,
                    file_content,
                    length: delete_file.length,
                });
            }
            IcebergDeleteFileFormat::Puffin => {
                if deletion_vector_descriptor.is_some() {
                    return Err(format!(
                        "multiple Puffin deletion vectors are attached to data file {}",
                        full_path
                    ));
                }
                let offset = delete_file.content_offset.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_offset",
                        delete_file.path, full_path
                    )
                })?;
                let size = delete_file.content_size_in_bytes.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_size_in_bytes",
                        delete_file.path, full_path
                    )
                })?;
                deletion_vector_descriptor = Some(scan_range::DeletionVectorDescriptor {
                    storage_type: Some("PUFFIN".to_string()),
                    path_or_inline_dv: Some(delete_file.path.clone()),
                    offset: Some(offset),
                    size_in_bytes: Some(size),
                    cardinality: None,
                });
            }
        }
    }
    if let Some(op) = ivm_change_op {
        crate::exec::change_op::validate_change_op_value(op)?;
    }
    Ok(scan_range::ScanRangeParams::file(
        scan_range::FileScanRange {
            file_format: scan_range::FileFormat::Parquet,
            full_path: Some(full_path.to_string()),
            relative_path: None,
            table_id: None,
            offset,
            length,
            file_length: file_len,
            delete_files: parquet_delete_files,
            deletion_vector_descriptor,
            first_row_id,
            data_sequence_number,
            modification_time: None,
            datacache_options: None,
            included_positions: included_positions.cloned().unwrap_or_default(),
            serialized_split: None,
            use_iceberg_jni_metadata_reader: false,
            ivm_change_op,
            file_pruning_min_max_values,
        },
    ))
}
