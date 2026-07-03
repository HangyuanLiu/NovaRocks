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

use arrow::datatypes::DataType;
use std::collections::{BTreeMap, HashMap};

use super::expr::lower_proto_expr;
use super::layout::{chunk_schema_from_output_columns, layout_from_output_columns};
use super::node::{LoweredNode, NodeLoweringContext};
use crate::cache::{CacheOptions, DataCacheManager, ExternalDataCacheRangeOptions};
use crate::common::ids::SlotId;
use crate::connector::iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use crate::connector::iceberg::metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanRange,
    IcebergMetadataTableType,
};
use crate::connector::{HdfsIcebergRuntimePruningConfig, HdfsScanConfig, ScanConfig};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::formats::FileFormatConfig;
use crate::formats::parquet::{ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind};
use crate::fs::object_store::{ObjectStoreConfig, apply_object_store_runtime_defaults};
use crate::fs::object_store_credentials::{ObjectStoreCredentials, ObjectStoreCredentialsSource};
use crate::fs::scan_context::FileScanRange;
use crate::proto::{common, novarocks, plan};

pub(crate) fn lower_scan_node(
    node: &plan::DistributedNode,
    _physical: &plan::PlanNode,
    scan: &plan::ScanNode,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    if !node.children.is_empty() {
        return Err(format!(
            "ScanNode node_id={} expected no children, got {}",
            node.node_id,
            node.children.len()
        ));
    }
    if !scan.dict_columns.is_empty() {
        return Err("ScanNode dict_columns are not supported by native lowering yet".to_string());
    }
    if !scan.variant_columns.is_empty() {
        return Err(
            "ScanNode variant_columns are not supported by native lowering yet".to_string(),
        );
    }
    let table = scan
        .table
        .as_ref()
        .ok_or_else(|| "ScanNode table missing".to_string())?;
    let source = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
        .ok_or_else(|| "ScanNode table source missing".to_string())?;
    match source {
        plan::scan_source::Kind::IcebergDataFiles(source) => {
            lower_iceberg_data_files_scan(node, scan, source, ctx, arena)
        }
        plan::scan_source::Kind::IcebergMetadataTable(source) => {
            lower_iceberg_metadata_scan(node, scan, source, ctx, arena)
        }
        plan::scan_source::Kind::IcebergDeltaTable(_) => {
            unsupported_scan_source("IcebergDeltaTable")
        }
        plan::scan_source::Kind::IcebergVersionTable(_) => {
            unsupported_scan_source("IcebergVersionTable")
        }
        plan::scan_source::Kind::IcebergMvTargetState(_) => {
            unsupported_scan_source("IcebergMvTargetState")
        }
        plan::scan_source::Kind::IcebergMvTargetLocator(_) => {
            unsupported_scan_source("IcebergMvTargetLocator")
        }
    }
}

fn lower_iceberg_data_files_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergDataFiles,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let output_columns = scan_output_columns(scan)?;
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let table = source
        .table
        .as_ref()
        .ok_or_else(|| "IcebergDataFiles table missing".to_string())?;
    let ranges = decode_file_scan_ranges(node.node_id, table, ctx.scan_ranges(node.node_id))?;
    let read_columns = scan_read_columns(scan, output_columns)?;
    let cache_options = CacheOptions::from_query_options(None)?;
    let parquet_cfg = ParquetScanConfig {
        columns: read_columns.names,
        chunk_schema: output_schema.clone(),
        slot_kinds: vec![ParquetSlotKind::Regular; read_columns.slot_count],
        case_sensitive: true,
        enable_page_index: false,
        min_max_predicates: Vec::new(),
        runtime_min_max_filter_columns: HashMap::new(),
        variant_path_predicates: Vec::new(),
        batch_size: Some(4096),
        datacache: DataCacheManager::instance().external_context(cache_options),
        cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        iceberg_output_schema: Some(output_schema.arrow_schema_ref()),
        variant_path_columns: Vec::new(),
        query_global_dicts: Default::default(),
    };
    let object_store_config = resolve_cloud_object_store_config(&source.cloud_properties)?;
    let iceberg_runtime_pruning = Some(HdfsIcebergRuntimePruningConfig {
        slot_to_column: output_columns
            .iter()
            .map(|col| (SlotId::new(col.column_id), col.name.clone()))
            .collect(),
        min_max_filter_columns: HashMap::new(),
        discrete_set_max_values: 256,
    });
    let cfg = HdfsScanConfig {
        original_range_count: ranges.len(),
        ranges,
        has_more: false,
        limit: parse_scan_limit(node.limit)?,
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        format: Some(FileFormatConfig::Parquet(parquet_cfg)),
        object_store_config,
        iceberg_table_locations: table_location_map(table),
        query_global_dicts: Default::default(),
        iceberg_runtime_pruning,
    };
    let predicate = lower_scan_predicate(scan, arena, &layout)?;
    let scan_node = ctx
        .connectors()?
        .create_scan_node("hdfs", ScanConfig::Hdfs(Box::new(cfg)))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_accept_empty_scan_ranges(true);
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

fn lower_iceberg_metadata_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergMetadataTable,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let output_columns = scan_output_columns(scan)?;
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let metadata_table_type = metadata_table_type(source.metadata_table_type)?;
    let ranges = decode_metadata_scan_ranges(ctx.scan_ranges(node.node_id))?;
    let cfg = IcebergMetadataScanConfig {
        metadata_table_type,
        serialized_table: source.serialized_table.clone(),
        serialized_predicate: source.metadata_payload.clone().unwrap_or_default(),
        load_column_stats: false,
        ranges,
        batch_size: 4096,
        output_columns: metadata_output_columns(output_columns)?,
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
    };
    let predicate = lower_scan_predicate(scan, arena, &layout)?;
    if predicate.is_some() {
        return Err(
            "IcebergMetadataTable native scan predicates are not supported yet".to_string(),
        );
    }
    let scan_node = ctx
        .connectors()?
        .create_scan_node("iceberg", ScanConfig::IcebergMetadata(cfg))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_accept_empty_scan_ranges(true);
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

fn scan_output_columns(scan: &plan::ScanNode) -> Result<&[common::OutputColumn], String> {
    if scan.columns.is_empty() {
        return Err("ScanNode columns are empty".to_string());
    }
    Ok(&scan.columns)
}

struct ReadColumns {
    names: Vec<String>,
    slot_count: usize,
}

fn scan_read_columns(
    scan: &plan::ScanNode,
    output_columns: &[common::OutputColumn],
) -> Result<ReadColumns, String> {
    if scan.required_columns.is_empty() {
        return Ok(ReadColumns {
            names: output_columns.iter().map(|col| col.name.clone()).collect(),
            slot_count: output_columns.len(),
        });
    }
    if scan.required_columns.len() != output_columns.len() {
        return Err(format!(
            "ScanNode required_columns length mismatch: required={} outputs={}",
            scan.required_columns.len(),
            output_columns.len()
        ));
    }
    Ok(ReadColumns {
        names: scan.required_columns.clone(),
        slot_count: output_columns.len(),
    })
}

fn decode_file_scan_ranges(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    ranges: &[novarocks::ScanRange],
) -> Result<Vec<FileScanRange>, String> {
    ranges
        .iter()
        .enumerate()
        .filter_map(|(idx, range)| {
            if range.empty.unwrap_or(false) {
                None
            } else {
                Some(decode_file_scan_range(node_id, table, idx, range))
            }
        })
        .collect()
}

fn decode_file_scan_range(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    idx: usize,
    range: &novarocks::ScanRange,
) -> Result<FileScanRange, String> {
    if range.has_more.unwrap_or(false) {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
        ));
    }
    let Some(novarocks::scan_range::Kind::Hdfs(hdfs)) = range.kind.as_ref() else {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} expected HDFS range"
        ));
    };
    if !hdfs.file_format.eq_ignore_ascii_case("PARQUET") {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} unsupported file_format {}; only PARQUET is supported",
            hdfs.file_format
        ));
    }
    if hdfs.deletion_vector_descriptor.is_some() {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} deletion vectors are not supported by native lowering yet"
        ));
    }
    let path = hdfs_range_path(table, hdfs)?;
    let file_len = nonnegative_u64(hdfs.file_length, "file_length")?;
    let offset = nonnegative_u64(hdfs.offset, "offset")?;
    if offset > file_len {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} offset {} exceeds file_length {}",
            hdfs.offset, hdfs.file_length
        ));
    }
    let length = if hdfs.length > 0 {
        nonnegative_u64(hdfs.length, "length")?
    } else {
        file_len - offset
    };
    Ok(FileScanRange {
        path,
        file_len,
        offset,
        length,
        scan_range_id: i32::try_from(idx)
            .map_err(|_| format!("ScanNode node_id={node_id} range index overflow"))?,
        first_row_id: hdfs.first_row_id,
        data_sequence_number: hdfs.data_sequence_number,
        ivm_change_op: None,
        included_positions: if hdfs.included_positions.is_empty() {
            None
        } else {
            Some(hdfs.included_positions.clone())
        },
        external_datacache: hdfs_external_datacache(hdfs),
        delete_files: decode_delete_files(node_id, idx, &hdfs.delete_files)?,
        iceberg_file_pruning: None,
    })
}

fn hdfs_range_path(
    table: &plan::IcebergTableInfo,
    hdfs: &novarocks::HdfsScanRange,
) -> Result<String, String> {
    if let Some(path) = hdfs.full_path.as_deref()
        && !path.is_empty()
    {
        return Ok(path.to_string());
    }
    let Some(relative_path) = hdfs
        .relative_path
        .as_deref()
        .filter(|path| !path.is_empty())
    else {
        return Err("HDFS range missing full_path/relative_path".to_string());
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
        .map_err(|_| format!("HDFS range {field} must be non-negative, got {value}"))
}

fn hdfs_external_datacache(
    hdfs: &novarocks::HdfsScanRange,
) -> Option<ExternalDataCacheRangeOptions> {
    hdfs.datacache_options
        .as_ref()
        .map(|opts| ExternalDataCacheRangeOptions {
            modification_time: hdfs.modification_time,
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

fn decode_metadata_scan_ranges(
    ranges: &[novarocks::ScanRange],
) -> Result<Vec<IcebergMetadataScanRange>, String> {
    if ranges.is_empty() {
        return Ok(vec![IcebergMetadataScanRange {
            path: String::new(),
            serialized_split: String::new(),
        }]);
    }
    ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            if range.has_more.unwrap_or(false) {
                return Err(format!(
                    "IcebergMetadataTable range {idx} has_more is not supported by native lowering"
                ));
            }
            let Some(novarocks::scan_range::Kind::Hdfs(hdfs)) = range.kind.as_ref() else {
                return Err(format!(
                    "IcebergMetadataTable range {idx} expected HDFS range"
                ));
            };
            Ok(IcebergMetadataScanRange {
                path: hdfs.full_path.clone().unwrap_or_default(),
                serialized_split: hdfs.serialized_split.clone().unwrap_or_default(),
            })
        })
        .collect()
}

fn metadata_output_columns(
    output_columns: &[common::OutputColumn],
) -> Result<Vec<IcebergMetadataOutputColumn>, String> {
    output_columns
        .iter()
        .map(|col| {
            let data_type = col
                .r#type
                .as_ref()
                .ok_or_else(|| format!("metadata output column {} type missing", col.name))
                .and_then(super::decode_type)?;
            Ok(IcebergMetadataOutputColumn {
                name: col.name.clone(),
                slot_id: SlotId::new(col.column_id),
                data_type,
                nullable: col.nullable,
            })
        })
        .collect()
}

fn metadata_table_type(value: i32) -> Result<IcebergMetadataTableType, String> {
    match plan::IcebergMetadataTableType::try_from(value)
        .map_err(|_| format!("unknown Iceberg metadata table type {value}"))?
    {
        plan::IcebergMetadataTableType::Files => Ok(IcebergMetadataTableType::Files),
        plan::IcebergMetadataTableType::Manifests => Ok(IcebergMetadataTableType::Manifests),
        plan::IcebergMetadataTableType::LogicalIcebergMetadata => {
            Ok(IcebergMetadataTableType::LogicalIcebergMetadata)
        }
        plan::IcebergMetadataTableType::Snapshots => Ok(IcebergMetadataTableType::Snapshots),
        plan::IcebergMetadataTableType::History => Ok(IcebergMetadataTableType::History),
        plan::IcebergMetadataTableType::Refs => Ok(IcebergMetadataTableType::Refs),
        plan::IcebergMetadataTableType::Partitions => Ok(IcebergMetadataTableType::Partitions),
        plan::IcebergMetadataTableType::Unspecified => {
            Err("Iceberg metadata table type is unspecified".to_string())
        }
    }
}

fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &super::layout::Layout,
) -> Result<Option<crate::exec::expr::ExprId>, String> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = lower_proto_expr(expr, arena, layout)
            .map_err(|err| format!("ScanNode predicate {idx}: {err}"))?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

fn parse_scan_limit(limit: i64) -> Result<Option<usize>, String> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(format!("ScanNode limit must be -1 or >= 0, got {limit}"))
    } else {
        Ok(Some(limit as usize))
    }
}

fn resolve_cloud_object_store_config(
    cloud_properties: &HashMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let props = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let Some(credentials) = ObjectStoreCredentials::optional_from_aws_s3_properties(
        ObjectStoreCredentialsSource::AwsS3Properties,
        &props,
    )?
    else {
        return Ok(None);
    };
    let mut cfg = credentials.to_object_store_config();
    apply_object_store_runtime_defaults(&mut cfg);
    Ok(Some(cfg))
}

fn table_location_map(table: &plan::IcebergTableInfo) -> HashMap<i64, String> {
    let mut locations = HashMap::new();
    if !table.location.is_empty() {
        locations.insert(i64::from(table.schema_id), table.location.clone());
    }
    locations
}

fn unsupported_scan_source(source: &str) -> Result<LoweredNode, String> {
    Err(format!("{source} native scan source is not implemented"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::ConnectorRegistry;
    use crate::exec::node::ExecNodeKind;
    use crate::proto::common;
    use crate::sql::codegen::proto_encode::types::encode_type;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            catalog: "rest".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: Some(1),
            schema_id: 7,
            location: "s3://bucket/warehouse/db/t".to_string(),
            schema: None,
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn scan_node(source: plan::scan_source::Kind) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: Vec::new(),
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource { kind: Some(source) }),
                    }),
                    alias: None,
                    columns,
                    predicates: Vec::new(),
                    required_columns: Vec::new(),
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn hdfs_range() -> novarocks::ScanRange {
        novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::Hdfs(
                novarocks::HdfsScanRange {
                    file_format: "PARQUET".to_string(),
                    full_path: Some("s3://bucket/warehouse/db/t/data-1.parquet".to_string()),
                    relative_path: None,
                    table_id: None,
                    offset: 0,
                    length: 10,
                    file_length: 10,
                    delete_files: Vec::new(),
                    deletion_vector_descriptor: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    modification_time: None,
                    datacache_options: None,
                    included_positions: Vec::new(),
                    serialized_split: None,
                    use_iceberg_jni_metadata_reader: false,
                },
            )),
            volume_id: None,
            empty: None,
            has_more: None,
        }
    }

    #[test]
    fn lowers_iceberg_data_file_scan_to_scan_node() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![hdfs_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower_native::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert_eq!(scan.node_id(), Some(10));
        assert_eq!(scan.output_chunk_schema().slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn rejects_internal_range_for_iceberg_data_scan() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(
                10,
                vec![novarocks::ScanRange {
                    kind: Some(novarocks::scan_range::Kind::Internal(
                        novarocks::InternalScanRange {
                            version: 1,
                            tablet_id: 1,
                            partition_id: 1,
                            db_name: None,
                            table_name: None,
                            catalog_name: None,
                            fill_data_cache: false,
                            skip_page_cache: false,
                            skip_disk_cache: false,
                        },
                    )),
                    volume_id: None,
                    empty: None,
                    has_more: None,
                }],
            );
        let mut arena = ExprArena::default();
        let err = crate::lower_native::lower_proto_node(&node, &mut arena, &ctx).unwrap_err();
        assert!(err.contains("expected HDFS range"));
    }
}
