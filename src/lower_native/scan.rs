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

use arrow::datatypes::{DataType, Schema};
use std::collections::{BTreeMap, HashMap, HashSet};

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
use crate::connector::iceberg::{
    IcebergArrowColumn, IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor,
    IcebergTableDescriptor, build_projected_output_schema,
};
use crate::connector::{HdfsIcebergRuntimePruningConfig, HdfsScanConfig, ScanConfig};
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
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
    let table = source
        .table
        .as_ref()
        .ok_or_else(|| "IcebergDataFiles table missing".to_string())?;
    let read_plan = scan_read_plan(scan, table, output_columns)?;
    let ranges = decode_file_scan_ranges(node.node_id, table, ctx.scan_ranges(node.node_id)?)?;
    let cache_options = CacheOptions::from_query_options(ctx.query_options())?;
    let batch_size = scan_batch_size(ctx.query_options())?;
    let parquet_cfg = ParquetScanConfig {
        columns: read_plan.read_columns.clone(),
        chunk_schema: read_plan.read_schema.clone(),
        slot_kinds: read_plan.slot_kinds.clone(),
        case_sensitive: true,
        enable_page_index: false,
        min_max_predicates: Vec::new(),
        runtime_min_max_filter_columns: HashMap::new(),
        variant_path_predicates: Vec::new(),
        batch_size: Some(batch_size),
        datacache: DataCacheManager::instance().external_context(cache_options),
        cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        iceberg_output_schema: Some(read_plan.read_schema.arrow_schema_ref()),
        variant_path_columns: Vec::new(),
        query_global_dicts: Default::default(),
    };
    let object_store_config = resolve_cloud_object_store_config(&source.cloud_properties)?;
    let iceberg_runtime_pruning = Some(HdfsIcebergRuntimePruningConfig {
        slot_to_column: read_plan
            .read_layout
            .order()
            .iter()
            .copied()
            .zip(read_plan.read_columns.iter().cloned())
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
    let predicate = lower_scan_predicate(scan, arena, &read_plan.read_layout)?;
    let scan_node = ctx
        .connectors()?
        .create_scan_node("hdfs", ScanConfig::Hdfs(Box::new(cfg)))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(read_plan.read_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_accept_empty_scan_ranges(true);
    let scan_lowered = LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout: read_plan.read_layout.clone(),
        output_schema: read_plan.read_schema.clone(),
    };
    maybe_project_data_scan_output(node.node_id, scan_lowered, read_plan, arena)
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
    let ranges = decode_metadata_scan_ranges(ctx.scan_ranges(node.node_id)?)?;
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

#[derive(Clone, Debug)]
struct ScanReadPlan {
    output_layout: super::layout::Layout,
    output_schema: ChunkSchemaRef,
    read_layout: super::layout::Layout,
    read_schema: ChunkSchemaRef,
    read_columns: Vec<String>,
    slot_kinds: Vec<ParquetSlotKind>,
}

#[derive(Clone, Debug)]
struct PredicateColumnRef {
    column_id: u32,
    name: Option<String>,
    r#type: Option<common::TypeDesc>,
    nullable: bool,
}

fn scan_read_plan(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ScanReadPlan, String> {
    let output_layout = layout_from_output_columns(output_columns)?;
    let output_schema = iceberg_chunk_schema_from_output_columns(table, output_columns)?;

    let mut read_columns = output_columns.to_vec();
    let mut read_names = output_columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<HashSet<_>>();
    let mut read_slots = output_columns
        .iter()
        .map(|col| col.column_id)
        .collect::<HashSet<_>>();
    let mut next_hidden_column_id = output_columns
        .iter()
        .map(|col| col.column_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1);

    let predicate_refs = scan_predicate_column_refs(&scan.predicates)?;
    let predicate_refs_by_name = predicate_refs
        .values()
        .filter_map(|col| col.name.as_ref().map(|name| (name.clone(), col)))
        .collect::<HashMap<_, _>>();
    let required_names = scan
        .required_columns
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    for required in &scan.required_columns {
        if read_names.contains(required) {
            continue;
        }
        let col = if let Some(pred_col) = predicate_refs_by_name.get(required) {
            output_column_from_predicate_ref(pred_col)?
        } else {
            let hidden_id = allocate_hidden_column_id(&mut next_hidden_column_id, &read_slots)?;
            output_column_from_table_def(scan, required, hidden_id)?
        };
        push_read_column(&mut read_columns, &mut read_names, &mut read_slots, col)?;
    }

    for pred_col in predicate_refs.values() {
        if read_slots.contains(&pred_col.column_id) {
            continue;
        }
        let name = pred_col.name.as_ref().ok_or_else(|| {
            format!(
                "ScanNode predicate column_id={} is not an output column and does not carry a column name",
                pred_col.column_id
            )
        })?;
        if !required_names.is_empty() && !required_names.contains(name) {
            return Err(format!(
                "ScanNode predicate column {} is not listed in required_columns",
                name
            ));
        }
        push_read_column(
            &mut read_columns,
            &mut read_names,
            &mut read_slots,
            output_column_from_predicate_ref(pred_col)?,
        )?;
    }

    let read_layout = layout_from_output_columns(&read_columns)?;
    let read_schema = iceberg_chunk_schema_from_output_columns(table, &read_columns)?;
    let slot_kinds = vec![ParquetSlotKind::Regular; read_columns.len()];
    Ok(ScanReadPlan {
        output_layout,
        output_schema,
        read_layout,
        read_schema,
        read_columns: read_columns.into_iter().map(|col| col.name).collect(),
        slot_kinds,
    })
}

fn push_read_column(
    read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    col: common::OutputColumn,
) -> Result<(), String> {
    if !read_slots.insert(col.column_id) {
        return Err(format!(
            "ScanNode read columns contain duplicate column_id={}",
            col.column_id
        ));
    }
    if !read_names.insert(col.name.clone()) {
        return Err(format!(
            "ScanNode read columns contain duplicate column name {}",
            col.name
        ));
    }
    read_columns.push(col);
    Ok(())
}

fn allocate_hidden_column_id(next: &mut u32, used: &HashSet<u32>) -> Result<u32, String> {
    loop {
        let id = *next;
        *next = next
            .checked_add(1)
            .ok_or_else(|| "ScanNode hidden read column id overflow".to_string())?;
        if !used.contains(&id) {
            return Ok(id);
        }
    }
}

fn output_column_from_predicate_ref(
    col: &PredicateColumnRef,
) -> Result<common::OutputColumn, String> {
    let name = col.name.clone().ok_or_else(|| {
        format!(
            "ScanNode predicate column_id={} requires a column name for hidden read binding",
            col.column_id
        )
    })?;
    Ok(common::OutputColumn {
        column_id: col.column_id,
        name,
        r#type: col.r#type.clone(),
        nullable: col.nullable,
        is_internal: false,
    })
}

fn output_column_from_table_def(
    scan: &plan::ScanNode,
    name: &str,
    column_id: u32,
) -> Result<common::OutputColumn, String> {
    let table = scan
        .table
        .as_ref()
        .ok_or_else(|| "ScanNode table missing".to_string())?;
    let column = table
        .columns
        .iter()
        .chain(table.iceberg_row_lineage_metadata_columns.iter())
        .find(|col| col.name == name)
        .ok_or_else(|| {
            format!("ScanNode required column {name} is not in table column definitions")
        })?;
    let ty = column
        .logical_type
        .as_ref()
        .or(column.data_type.as_ref())
        .ok_or_else(|| format!("ScanNode required column {name} type missing"))?
        .clone();
    Ok(common::OutputColumn {
        column_id,
        name: column.name.clone(),
        r#type: Some(ty),
        nullable: column.nullable,
        is_internal: true,
    })
}

fn scan_predicate_column_refs(
    predicates: &[crate::proto::expr::Expr],
) -> Result<BTreeMap<u32, PredicateColumnRef>, String> {
    let mut refs = BTreeMap::new();
    for predicate in predicates {
        collect_predicate_column_refs(predicate, &mut refs)?;
    }
    Ok(refs)
}

fn collect_predicate_column_refs(
    expr: &crate::proto::expr::Expr,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    use crate::proto::expr::expr::Kind;

    let Some(kind) = expr.kind.as_ref() else {
        return Ok(());
    };
    match kind {
        Kind::ColumnRef(col) => {
            let next = PredicateColumnRef {
                column_id: col.column_id,
                name: col.column.clone(),
                r#type: expr.r#type.clone(),
                nullable: expr.nullable,
            };
            if let Some(prev) = refs.insert(col.column_id, next.clone()) {
                if prev.name != next.name {
                    return Err(format!(
                        "ScanNode predicate column_id={} has inconsistent names {:?} and {:?}",
                        col.column_id, prev.name, next.name
                    ));
                }
            }
        }
        Kind::Literal(_) | Kind::LambdaParamRef(_) => {}
        Kind::BinaryOp(binary) => {
            collect_optional_box_expr(&binary.left, refs)?;
            collect_optional_box_expr(&binary.right, refs)?;
        }
        Kind::UnaryOp(unary) => collect_optional_box_expr(&unary.operand, refs)?,
        Kind::FunctionCall(call) => collect_expr_list(&call.args, refs)?,
        Kind::AggregateCall(call) => {
            collect_expr_list(&call.args, refs)?;
            collect_sort_items(&call.order_by, refs)?;
        }
        Kind::WindowCall(call) => {
            collect_expr_list(&call.args, refs)?;
            collect_expr_list(&call.partition_by, refs)?;
            collect_sort_items(&call.order_by, refs)?;
        }
        Kind::Cast(cast) => collect_optional_box_expr(&cast.operand, refs)?,
        Kind::IsNull(is_null) => collect_optional_box_expr(&is_null.operand, refs)?,
        Kind::InList(in_list) => {
            collect_optional_box_expr(&in_list.operand, refs)?;
            collect_expr_list(&in_list.list, refs)?;
        }
        Kind::Between(between) => {
            collect_optional_box_expr(&between.operand, refs)?;
            collect_optional_box_expr(&between.low, refs)?;
            collect_optional_box_expr(&between.high, refs)?;
        }
        Kind::Like(like) => {
            collect_optional_box_expr(&like.operand, refs)?;
            collect_optional_box_expr(&like.pattern, refs)?;
        }
        Kind::CaseExpr(case_expr) => {
            collect_optional_box_expr(&case_expr.operand, refs)?;
            for branch in &case_expr.when_then {
                collect_optional_expr(&branch.when, refs)?;
                collect_optional_expr(&branch.then, refs)?;
            }
            collect_optional_box_expr(&case_expr.else_expr, refs)?;
        }
        Kind::IsTruth(is_truth) => collect_optional_box_expr(&is_truth.operand, refs)?,
        Kind::Lambda(lambda) => collect_optional_box_expr(&lambda.body, refs)?,
        Kind::Nested(nested) => collect_optional_box_expr(&nested.inner, refs)?,
    }
    Ok(())
}

fn collect_optional_box_expr(
    expr: &Option<Box<crate::proto::expr::Expr>>,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    if let Some(expr) = expr.as_ref() {
        collect_predicate_column_refs(expr, refs)?;
    }
    Ok(())
}

fn collect_optional_expr(
    expr: &Option<crate::proto::expr::Expr>,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    if let Some(expr) = expr.as_ref() {
        collect_predicate_column_refs(expr, refs)?;
    }
    Ok(())
}

fn collect_expr_list(
    exprs: &[crate::proto::expr::Expr],
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    for expr in exprs {
        collect_predicate_column_refs(expr, refs)?;
    }
    Ok(())
}

fn collect_sort_items(
    items: &[crate::proto::expr::SortItem],
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    for item in items {
        collect_optional_expr(&item.expr, refs)?;
    }
    Ok(())
}

fn iceberg_chunk_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ChunkSchemaRef, String> {
    let slot_ids = output_columns
        .iter()
        .map(|col| SlotId::new(col.column_id))
        .collect::<Vec<_>>();
    let arrow_schema = iceberg_arrow_schema_from_output_columns(table, output_columns)?;
    ChunkSchema::try_ref_from_schema_and_slot_ids(arrow_schema.as_ref(), &slot_ids)
}

fn iceberg_arrow_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<std::sync::Arc<Schema>, String> {
    let descriptor = iceberg_table_descriptor(table)?;
    let columns = output_columns
        .iter()
        .map(|col| {
            let desc = col
                .r#type
                .as_ref()
                .ok_or_else(|| format!("ScanNode output column {} type missing", col.name))?;
            Ok(IcebergArrowColumn {
                name: col.name.clone(),
                data_type: super::decode_type(desc)?,
                nullable: col.nullable,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    build_projected_output_schema(&descriptor, &columns)?
        .ok_or_else(|| "IcebergDataFiles table schema missing".to_string())
}

fn iceberg_table_descriptor(
    table: &plan::IcebergTableInfo,
) -> Result<IcebergTableDescriptor, String> {
    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| "IcebergDataFiles table schema missing".to_string())?;
    Ok(IcebergTableDescriptor {
        columns: Vec::new(),
        iceberg_schema: Some(IcebergSchemaDescriptor {
            fields: schema
                .fields
                .iter()
                .map(iceberg_schema_field_descriptor)
                .collect(),
        }),
        equality_delete_schema: None,
        partition_info: Vec::new(),
        current_snapshot_id: table.current_snapshot_id,
        serialized_metadata: table.serialized_metadata.clone(),
    })
}

fn iceberg_schema_field_descriptor(
    field: &plan::IcebergSchemaFieldDef,
) -> IcebergSchemaFieldDescriptor {
    IcebergSchemaFieldDescriptor {
        name: field.name.clone(),
        field_id: Some(field.field_id),
        children: field
            .children
            .iter()
            .map(iceberg_schema_field_descriptor)
            .collect(),
        initial_default_json: field.initial_default_json.clone(),
    }
}

fn maybe_project_data_scan_output(
    node_id: i32,
    scan_lowered: LoweredNode,
    read_plan: ScanReadPlan,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    if read_plan.read_layout.order() == read_plan.output_layout.order() {
        return Ok(LoweredNode {
            node: scan_lowered.node,
            layout: read_plan.output_layout,
            output_schema: read_plan.output_schema,
        });
    }
    let exprs = read_plan
        .output_layout
        .order()
        .iter()
        .map(|slot_id| {
            let slot = read_plan.read_schema.slot(*slot_id).ok_or_else(|| {
                format!("ScanNode projection references missing read slot {slot_id}")
            })?;
            Ok(arena.push_typed(ExprNode::SlotId(*slot_id), slot.data_type().clone()))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(scan_lowered.node),
                node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: read_plan.output_layout.order().to_vec(),
                expr_slot_schemas: Some(read_plan.output_schema.slots().to_vec()),
                output_indices: None,
                output_chunk_schema: read_plan.output_schema.clone(),
            }),
        },
        layout: read_plan.output_layout,
        output_schema: read_plan.output_schema,
    })
}

fn scan_batch_size(
    query_options: Option<&crate::runtime::native_fragment_wire::QueryOptions>,
) -> Result<usize, String> {
    let Some(value) = query_options.and_then(|opts| opts.batch_size) else {
        return Ok(4096);
    };
    let batch_size = usize::try_from(value).map_err(|_| {
        format!("native ScanNode query_options.batch_size must be positive, got {value}")
    })?;
    if batch_size == 0 {
        return Err("native ScanNode query_options.batch_size must be positive".to_string());
    }
    Ok(batch_size)
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
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::connector::ConnectorRegistry;
    use crate::exec::node::ExecNodeKind;
    use crate::proto::{common, expr};
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

    fn column_def(name: &str, data_type: DataType) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(type_desc(&data_type)),
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn schema_field(field_id: i32, name: &str) -> plan::IcebergSchemaFieldDef {
        plan::IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
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
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(10, "id"), schema_field(11, "flag")],
            }),
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn scan_node(source: plan::scan_source::Kind) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        scan_node_with(columns, Vec::new(), Vec::new(), source)
    }

    fn scan_node_with(
        columns: Vec<common::OutputColumn>,
        predicates: Vec<expr::Expr>,
        required_columns: Vec<String>,
        source: plan::scan_source::Kind,
    ) -> plan::DistributedNode {
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
                        columns: vec![
                            column_def("id", DataType::Int64),
                            column_def("flag", DataType::Boolean),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource { kind: Some(source) }),
                    }),
                    alias: None,
                    columns,
                    predicates,
                    required_columns,
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn column_ref(column_id: u32, name: &str, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: Some(name.to_string()),
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
    fn iceberg_data_file_scan_output_schema_carries_field_ids() {
        let schema = iceberg_arrow_schema_from_output_columns(
            &table_info(),
            &[output_column(1, "id", DataType::Int64)],
        )
        .expect("iceberg schema");
        assert_eq!(
            schema.field(0).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"10".to_string())
        );
    }

    #[test]
    fn rejects_missing_scan_ranges() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()));
        let mut arena = ExprArena::default();
        let err = crate::lower_native::lower_proto_node(&node, &mut arena, &ctx).unwrap_err();
        assert!(err.contains("missing scan ranges"), "err={err}");
    }

    #[test]
    fn predicate_only_required_column_uses_read_layout_and_projects_outputs() {
        let node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![column_ref(2, "flag", DataType::Boolean)],
            vec!["id".to_string(), "flag".to_string()],
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![hdfs_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower_native::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(1)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected scan wrapper project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        let ExecNodeKind::Scan(scan) = project.input.kind else {
            panic!("expected project input scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
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
