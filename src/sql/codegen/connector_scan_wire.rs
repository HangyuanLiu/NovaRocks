use std::collections::{BTreeMap, HashMap};

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::connector::iceberg::scan_planner::{
    IcebergScanHandle, iceberg_scan_handle, iceberg_split,
};
use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
use crate::connector::starrocks::table::scan_planner::{
    StarRocksScanHandle, StarRocksSplit, starrocks_scan_handle, starrocks_split,
};
use crate::runtime::scan_range;
use crate::sql::catalog::{
    ColumnDef, IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
    IcebergDeleteFileInfo,
};
use crate::thrift::{descriptors, exprs, internal_service, partitions, plan_nodes, types};
use arrow::datatypes::DataType;

const DEFAULT_FE_CATALOG: &str = "default_catalog";
const ICEBERG_SCAN_SPLIT_TARGET_BYTES: i64 = 128 * 1024 * 1024;
const ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE: usize = 1024;
const ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE: i64 = 512 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct ThriftScanContext {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) node_id: i32,
    pub(crate) scan_tuple_id: types::TTupleId,
    pub(crate) conjuncts: Vec<exprs::TExpr>,
    pub(crate) min_max_predicates: Vec<MinMaxPredicate>,
    pub(crate) change_op_slot: Option<types::TSlotId>,
    pub(crate) cloud_properties: BTreeMap<String, String>,
    pub(crate) columns: Vec<ColumnDef>,
}

#[derive(Clone, Debug)]
pub(crate) struct ThriftScanPlan {
    pub(crate) node: Option<plan_nodes::TPlanNode>,
    pub(crate) scan_ranges: Vec<internal_service::TScanRangeParams>,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeFileScanPlan {
    pub(crate) node: Option<plan_nodes::TPlanNode>,
    pub(crate) scan_ranges: Vec<scan_range::ScanRangeParams>,
}

pub(crate) fn to_thrift_scan(
    connector_id: &str,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    match connector_id {
        "iceberg" => iceberg_to_thrift_scan(scan, splits, ctx),
        "starrocks" => starrocks_to_thrift_scan(scan, splits, ctx),
        other => Err(format!(
            "unsupported connector scan thrift emitter: {other}"
        )),
    }
}

pub(crate) fn to_native_file_scan(
    connector_id: &str,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<NativeFileScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    match connector_id {
        "iceberg" => iceberg_to_native_file_scan(scan, splits, ctx),
        other => Err(format!(
            "unsupported connector native file scan emitter: {other}"
        )),
    }
}

fn iceberg_to_thrift_scan(
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    let native = iceberg_to_native_file_scan(scan, splits, ctx)?;
    let scan_ranges = native
        .scan_ranges
        .iter()
        .map(scan_range::thrift_scan_range_params_from_native)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ThriftScanPlan {
        node: native.node,
        scan_ranges,
    })
}

fn iceberg_to_native_file_scan(
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<NativeFileScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan = iceberg_scan_handle(scan)?;
    let scan_ranges = build_iceberg_native_scan_ranges(scan, splits, &ctx)?;
    let node = build_iceberg_hdfs_scan_node(scan, &ctx);
    Ok(NativeFileScanPlan {
        node: Some(node),
        scan_ranges,
    })
}

fn build_iceberg_native_scan_ranges(
    scan: &IcebergScanHandle,
    splits: &[Split],
    ctx: &ThriftScanContext,
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
            ctx.change_op_slot,
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

fn build_iceberg_hdfs_scan_node(
    scan: &IcebergScanHandle,
    ctx: &ThriftScanContext,
) -> plan_nodes::TPlanNode {
    let mut node = crate::sql::codegen::nodes::default_plan_node();
    node.node_id = ctx.node_id;
    node.node_type = plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![ctx.scan_tuple_id];
    node.nullable_tuples = vec![];
    let min_max_conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    let min_max_tuple_id = min_max_conjuncts.as_ref().map(|_| ctx.scan_tuple_id);
    node.conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    node.compact_data = true;
    node.hdfs_scan_node = Some(plan_nodes::THdfsScanNode::new(
        Some(ctx.scan_tuple_id),
        None::<BTreeMap<types::TTupleId, Vec<exprs::TExpr>>>,
        min_max_conjuncts,
        min_max_tuple_id,
        None::<BTreeMap<types::TSlotId, Vec<i32>>>,
        None::<Vec<exprs::TExpr>>,
        Some(scan.table.column_names.clone()),
        Some(ctx.table.clone()),
        None::<String>,
        None::<String>,
        None::<String>,
        Some(true),
        Some(
            crate::thrift::cloud_configuration::TCloudConfiguration::new(
                None::<crate::thrift::cloud_configuration::TCloudType>,
                None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
                Some(ctx.cloud_properties.clone()),
                None::<bool>,
            ),
        ),
        None::<bool>,
        None::<bool>,
        None::<bool>,
        None::<types::TTupleId>,
        None::<String>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<crate::thrift::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<Vec<partitions::TBucketProperty>>,
        None::<bool>,
        None::<i64>,
        None::<Vec<plan_nodes::TColumnAccessPath>>,
        None::<Vec<plan_nodes::TVariantPathColumn>>,
    ));
    node
}

fn build_native_file_scan_range_params_for_file(
    file: &IcebergDataFileInfo,
    change_op_slot: Option<types::TSlotId>,
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
                change_op_slot,
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
fn build_hdfs_scan_range_params_for_file(
    file: &IcebergDataFileInfo,
    change_op_slot: Option<types::TSlotId>,
    columns: &[ColumnDef],
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    let pruning_columns = pruning_columns_from_column_order_for_test(columns)?;
    build_native_file_scan_range_params_for_file(file, change_op_slot, &pruning_columns)?
        .iter()
        .map(scan_range::thrift_scan_range_params_from_native)
        .collect()
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

fn int_literal_expr(value: i64) -> exprs::TExpr {
    exprs::TExpr::new(vec![crate::sql::codegen::expr_compiler::int_literal_node(
        value,
    )])
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
    change_op_slot: Option<types::TSlotId>,
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
                        types::TIcebergFileContent::POSITION_DELETES
                    }
                    IcebergDeleteFileContent::Equality => {
                        // Equality field IDs are read from the equality-delete Parquet schema by
                        // the Rust scan runner. The Thrift scan range only needs to identify the
                        // delete file as an equality-delete file.
                        types::TIcebergFileContent::EQUALITY_DELETES
                    }
                };
                parquet_delete_files.push(scan_range::IcebergDeleteFile {
                    full_path: Some(delete_file.path.clone()),
                    file_format: scan_range::IcebergFileFormat::Parquet,
                    file_content: match file_content {
                        types::TIcebergFileContent::POSITION_DELETES => {
                            scan_range::IcebergFileContent::PositionDeletes
                        }
                        types::TIcebergFileContent::EQUALITY_DELETES => {
                            scan_range::IcebergFileContent::EqualityDeletes
                        }
                        other => {
                            return Err(format!(
                                "unsupported Iceberg delete file content for native scan range: {other:?}"
                            ));
                        }
                    },
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
    let extended_columns = match (ivm_change_op, change_op_slot) {
        (Some(op), Some(slot_id)) => {
            crate::exec::change_op::validate_change_op_value(op)?;
            Some(BTreeMap::from([(slot_id, int_literal_expr(op as i64))]))
        }
        _ => None,
    };
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
            extended_columns,
        },
    ))
}

#[cfg(test)]
pub(crate) fn build_hdfs_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    included_positions: Option<&Vec<i64>>,
    change_op_slot: Option<types::TSlotId>,
    delete_files: &[IcebergDeleteFileInfo],
    file_pruning_min_max_values: Option<BTreeMap<i32, scan_range::FilePruningMinMaxValue>>,
) -> Result<internal_service::TScanRangeParams, String> {
    let native = build_native_file_scan_range_params(
        full_path,
        file_len,
        offset,
        length,
        first_row_id,
        data_sequence_number,
        ivm_change_op,
        included_positions,
        change_op_slot,
        delete_files,
        file_pruning_min_max_values,
    )?;
    scan_range::thrift_scan_range_params_from_native(&native)
}

fn starrocks_to_thrift_scan(
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    let scan = starrocks_scan_handle(scan)?;
    let scan_ranges = splits
        .iter()
        .map(|split| {
            let split = starrocks_split(split)?;
            Ok(build_starrocks_internal_scan_range_params(
                &ctx.database,
                &ctx.table,
                scan.schema_id,
                split,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let node = build_starrocks_lake_scan_node(scan, &ctx);
    Ok(ThriftScanPlan {
        node: Some(node),
        scan_ranges,
    })
}

fn build_starrocks_internal_scan_range_params(
    database: &str,
    table: &str,
    schema_id: i64,
    split: &StarRocksSplit,
) -> internal_service::TScanRangeParams {
    let internal_scan_range = plan_nodes::TInternalScanRange::new(
        vec![],
        schema_id.to_string(),
        split.version.to_string(),
        split.version.to_string(),
        split.tablet_id,
        database.to_string(),
        None::<Vec<plan_nodes::TKeyRange>>,
        None::<String>,
        Some(table.to_string()),
        Some(split.partition_id),
        None::<i64>,
        Some(true),
        None::<i32>,
        Some(false),
        Some(false),
        None::<i64>,
        Some(DEFAULT_FE_CATALOG.to_string()),
    );

    internal_service::TScanRangeParams::new(
        plan_nodes::TScanRange::new(
            Some(internal_scan_range),
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            None::<plan_nodes::THdfsScanRange>,
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    )
}

fn build_starrocks_lake_scan_node(
    scan: &StarRocksScanHandle,
    ctx: &ThriftScanContext,
) -> plan_nodes::TPlanNode {
    let mut node = crate::sql::codegen::nodes::default_plan_node();
    node.node_id = ctx.node_id;
    node.node_type = plan_nodes::TPlanNodeType::LAKE_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![ctx.scan_tuple_id];
    node.nullable_tuples = vec![];
    node.conjuncts = if ctx.conjuncts.is_empty() {
        None
    } else {
        Some(ctx.conjuncts.clone())
    };
    node.compact_data = true;
    node.lake_scan_node = Some(plan_nodes::TLakeScanNode {
        tuple_id: ctx.scan_tuple_id,
        key_column_name: vec![],
        key_column_type: vec![],
        is_preaggregation: false,
        sort_column: None,
        rollup_name: None,
        sql_predicates: None,
        enable_column_expr_predicate: None,
        dict_string_id_to_int_ids: None,
        unused_output_column_name: None,
        sort_key_column_names: None,
        bucket_exprs: None,
        column_access_paths: None,
        sorted_by_keys_per_tablet: None,
        output_chunk_by_bucket: None,
        output_asc_hint: None,
        partition_order_hint: None,
        enable_topn_filter_back_pressure: None,
        back_pressure_max_rounds: None,
        back_pressure_throttle_time: None,
        back_pressure_throttle_time_upper_bound: None,
        back_pressure_num_rows: None,
        schema_key: Some(descriptors::TTableSchemaKey::new(
            Some(scan.table.db_id),
            Some(scan.table.table_id),
            Some(scan.schema_id),
        )),
        enable_prune_column_after_index_filter: None,
        enable_gin_filter: None,
        next_uniq_id: None,
        enable_global_late_materialization: None,
    });
    node
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::iceberg::IcebergConnectorScanPlanner;
    use crate::connector::scan_planning::{
        ConnectorScanHandle, ConnectorScanPlanner, ConnectorSplit,
    };
    use crate::connector::starrocks::table::scan_planner::{
        StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle,
    };
    use crate::sql::catalog::{ColumnDef, IcebergColumnStats, IcebergSchemaDef, IcebergTableInfo};

    #[derive(Debug)]
    struct DummyScanHandle;

    impl ConnectorScanHandle for DummyScanHandle {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct DummySplit;

    impl ConnectorSplit for DummySplit {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn dummy_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 1,
            location: String::new(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn dummy_iceberg_file() -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: "s3://bucket/data/file.parquet".to_string(),
            size: 1024,
            row_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn column(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn stats(lower: Vec<u8>, upper: Vec<u8>) -> IcebergColumnStats {
        IcebergColumnStats {
            null_count: None,
            value_count: None,
            column_size: None,
            lower_bound: Some(lower),
            upper_bound: Some(upper),
        }
    }

    #[test]
    fn to_thrift_scan_rejects_mismatched_splits_before_dispatch() {
        let scan = ScanHandle::new("starrocks", DummyScanHandle);
        let splits = vec![Split::new("iceberg", DummySplit)];

        let err = to_thrift_scan("missing", &scan, &splits, ThriftScanContext::default())
            .expect_err("split validation must run before connector dispatch");

        assert!(
            err.contains("split connector mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn to_thrift_scan_rejects_unknown_connector_id() {
        let scan = ScanHandle::new("missing", DummyScanHandle);

        let err = to_thrift_scan("missing", &scan, &[], ThriftScanContext::default())
            .expect_err("unknown connector must fail");

        assert!(
            err.contains("unsupported connector scan thrift emitter: missing"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn to_thrift_scan_dispatches_starrocks_wire_emission() {
        let scan = ScanHandle::new(
            "starrocks",
            StarRocksScanHandle {
                table: StarRocksTableHandle {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    db_id: 10,
                    table_id: 20,
                },
                schema_id: 30,
            },
        );
        let splits = vec![Split::new(
            "starrocks",
            StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            },
        )];

        let plan = to_thrift_scan(
            "starrocks",
            &scan,
            &splits,
            ThriftScanContext {
                database: "default".to_string(),
                table: "orders".to_string(),
                node_id: 11,
                scan_tuple_id: 1,
                ..ThriftScanContext::default()
            },
        )
        .expect("starrocks thrift scan");

        let node = plan.node.expect("lake scan node");
        assert_eq!(node.node_id, 11);
        assert_eq!(node.node_type, plan_nodes::TPlanNodeType::LAKE_SCAN_NODE);
        let lake = node.lake_scan_node.expect("lake scan payload");
        assert_eq!(lake.tuple_id, 1);
        let schema_key = lake.schema_key.expect("schema key");
        assert_eq!(schema_key.db_id, Some(10));
        assert_eq!(schema_key.table_id, Some(20));
        assert_eq!(schema_key.schema_id, Some(30));
        assert_eq!(plan.scan_ranges.len(), 1);
        let internal = plan.scan_ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");
        assert_eq!(internal.tablet_id, 300);
    }

    #[test]
    fn position_bound_iceberg_file_carries_included_positions_without_splitting() {
        let mut file = dummy_iceberg_file();
        file.size = ICEBERG_SCAN_SPLIT_TARGET_BYTES + 1;
        file.included_positions = Some(vec![3, 9, 11]);

        let ranges = build_hdfs_scan_range_params_for_file(&file, None, &[]).expect("scan ranges");

        assert_eq!(ranges.len(), 1);
        let hdfs_range = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range");
        assert_eq!(hdfs_range.offset, Some(0));
        assert_eq!(hdfs_range.length, Some(file.size));
        assert_eq!(hdfs_range.included_positions, Some(vec![3, 9, 11]));
    }

    #[test]
    fn iceberg_scan_range_min_max_values_bridge_numeric_stats_only() {
        let mut file = dummy_iceberg_file();
        file.column_stats = Some(std::collections::HashMap::from([
            ("flag".to_string(), stats(vec![0], vec![1])),
            (
                "id".to_string(),
                stats(10_i64.to_le_bytes().to_vec(), 20_i64.to_le_bytes().to_vec()),
            ),
            (
                "score".to_string(),
                stats(
                    1.5_f64.to_le_bytes().to_vec(),
                    9.25_f64.to_le_bytes().to_vec(),
                ),
            ),
            ("name".to_string(), stats(b"a".to_vec(), b"z".to_vec())),
        ]));
        let columns = vec![
            column("flag", DataType::Boolean),
            column("id", DataType::Int64),
            column("score", DataType::Float64),
            column("name", DataType::Utf8),
        ];
        let pruning_columns =
            pruning_columns_from_column_order_for_test(&columns).expect("test pruning columns");

        let native_ranges =
            build_native_file_scan_range_params_for_file(&file, None, &pruning_columns)
                .expect("native scan ranges");
        let native_file = match &native_ranges[0].range {
            scan_range::ScanRange::File(file) => file,
        };
        let native_values = native_file
            .file_pruning_min_max_values
            .as_ref()
            .expect("native min max values");
        assert_eq!(native_values.len(), 3);
        assert_eq!(
            native_values.get(&0).expect("flag stats").value_kind,
            scan_range::FilePruningValueKind::Bool
        );
        assert_eq!(
            native_values.get(&1).expect("id stats").value_kind,
            scan_range::FilePruningValueKind::Int
        );
        assert_eq!(
            native_values.get(&2).expect("score stats").value_kind,
            scan_range::FilePruningValueKind::Float
        );

        let ranges =
            build_hdfs_scan_range_params_for_file(&file, None, &columns).expect("scan ranges");
        let hdfs = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range");
        let values = hdfs.min_max_values.as_ref().expect("min max values");

        assert_eq!(values.len(), 3);
        let flag = values.get(&0).expect("flag stats");
        assert_eq!(flag.type_, exprs::TExprNodeType::BOOL_LITERAL);
        assert_eq!(flag.min_int_value, Some(0));
        assert_eq!(flag.max_int_value, Some(1));

        let id = values.get(&1).expect("id stats");
        assert_eq!(id.type_, exprs::TExprNodeType::INT_LITERAL);
        assert_eq!(id.min_int_value, Some(10));
        assert_eq!(id.max_int_value, Some(20));

        let score = values.get(&2).expect("score stats");
        assert_eq!(score.type_, exprs::TExprNodeType::FLOAT_LITERAL);
        assert_eq!(score.min_float_value.map(|v| v.0), Some(1.5));
        assert_eq!(score.max_float_value.map(|v| v.0), Some(9.25));

        assert!(!values.contains_key(&3), "string stats must not be bridged");
    }

    #[test]
    fn to_thrift_scan_dispatches_iceberg_wire_emission() {
        let planner = IcebergConnectorScanPlanner::new();
        let table_info = dummy_iceberg_table_info();
        let catalog = table_info.catalog.clone();
        let namespace = table_info.namespace.clone();
        let table = table_info.table.clone();
        let snapshot_id = table_info.current_snapshot_id;
        let table_handle = IcebergConnectorScanPlanner::table_handle_from_source(
            &catalog,
            &namespace,
            &table,
            snapshot_id,
            table_info,
            vec![dummy_iceberg_file()],
            vec!["id".to_string()],
        );
        let scan = planner
            .begin_scan(table_handle, Default::default())
            .expect("begin_scan");
        let splits = planner
            .plan_splits(&scan, Default::default())
            .expect("plan_splits");

        let plan = to_thrift_scan(
            "iceberg",
            &scan,
            &splits,
            ThriftScanContext {
                database: "default".to_string(),
                table: "orders".to_string(),
                node_id: 17,
                scan_tuple_id: 2,
                cloud_properties: BTreeMap::from([(
                    "aws.s3.endpoint".to_string(),
                    "http://localhost:9000".to_string(),
                )]),
                ..ThriftScanContext::default()
            },
        )
        .expect("iceberg thrift scan");

        let node = plan.node.expect("hdfs scan node");
        assert_eq!(node.node_id, 17);
        assert_eq!(node.node_type, plan_nodes::TPlanNodeType::HDFS_SCAN_NODE);
        let hdfs = node.hdfs_scan_node.as_ref().expect("hdfs scan payload");
        assert_eq!(hdfs.tuple_id, Some(2));
        assert_eq!(hdfs.hive_column_names, Some(vec!["id".to_string()]));
        assert_eq!(hdfs.table_name.as_deref(), Some("orders"));
        assert!(hdfs.cloud_configuration.is_some());

        assert_eq!(plan.scan_ranges.len(), 1);
        let range = plan.scan_ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range");
        assert_eq!(
            range.full_path.as_deref(),
            Some("s3://bucket/data/file.parquet")
        );
    }
}
