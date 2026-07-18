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

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::datatypes::DataType;

use super::super::layout::layout_from_output_columns;
use super::super::node::DecodedNode;
use super::common::output_column_data_type;
use super::schema::{
    iceberg_chunk_schema_from_output_columns,
    iceberg_chunk_schema_from_output_columns_with_variants,
};
use super::variant_path::{NativeVariantPathPlan, parse_native_scan_variant_path_columns};
use super::virtual_columns::{iceberg_virtual_count_column, record_iceberg_virtual_column};
use crate::common::ids::SlotId;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::exec::row_position::IcebergVirtualSpec;
use crate::formats::parquet::{ParquetSlotKind, VariantPathSpec};
use crate::proto::{common, plan};

#[derive(Clone, Debug)]
pub(super) struct ScanReadPlan {
    pub(super) output_layout: super::super::layout::Layout,
    pub(super) output_schema: ChunkSchemaRef,
    pub(super) read_layout: super::super::layout::Layout,
    pub(super) read_schema: ChunkSchemaRef,
    pub(super) parquet_schema: ChunkSchemaRef,
    pub(super) read_columns: Vec<String>,
    pub(super) read_slot_ids: Vec<SlotId>,
    pub(super) slot_kinds: Vec<ParquetSlotKind>,
    pub(super) variant_path_columns: Vec<VariantPathSpec>,
    pub(super) iceberg_virtual: IcebergVirtualSpec,
}

#[derive(Clone, Debug)]
struct PredicateColumnRef {
    column_id: u32,
    name: Option<String>,
    r#type: Option<common::TypeDesc>,
    nullable: bool,
}

pub(super) fn scan_read_plan(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ScanReadPlan, String> {
    let output_layout = layout_from_output_columns(output_columns)?;
    let mut variant_path_plan =
        parse_native_scan_variant_path_columns(scan, table, output_columns)?;
    let output_schema = iceberg_chunk_schema_from_output_columns_with_variants(
        table,
        output_columns,
        &variant_path_plan,
    )?;

    let mut scan_columns = output_columns.to_vec();
    let mut scan_names = output_columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<HashSet<_>>();
    let mut scan_slots = output_columns
        .iter()
        .map(|col| col.column_id)
        .collect::<HashSet<_>>();
    let mut physical_read_columns = Vec::new();
    let mut read_names = HashSet::new();
    let mut read_slots = HashSet::new();
    let mut iceberg_virtual = IcebergVirtualSpec::default();
    for col in output_columns {
        if variant_path_plan
            .output_slot_ids
            .contains(&SlotId::new(col.column_id))
        {
            continue;
        }
        if record_iceberg_virtual_column(table, col, &mut iceberg_virtual)? {
            continue;
        }
        push_physical_read_column(
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            col.clone(),
        )?;
    }
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
        if scan_names.contains(required) || read_names.contains(required) {
            continue;
        }
        let col = if let Some(pred_col) = predicate_refs_by_name.get(required) {
            output_column_from_predicate_ref(pred_col)?
        } else {
            let hidden_id = allocate_hidden_column_id(&mut next_hidden_column_id, &scan_slots)?;
            output_column_from_table_def(scan, required, hidden_id)?
        };
        push_scan_column(
            table,
            &mut scan_columns,
            &mut scan_names,
            &mut scan_slots,
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            &mut iceberg_virtual,
            col,
        )?;
    }

    for pred_col in predicate_refs.values() {
        if scan_slots.contains(&pred_col.column_id) {
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
        push_scan_column(
            table,
            &mut scan_columns,
            &mut scan_names,
            &mut scan_slots,
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            &mut iceberg_virtual,
            output_column_from_predicate_ref(pred_col)?,
        )?;
    }

    ensure_native_variant_source_read_columns(
        scan,
        &mut variant_path_plan,
        &mut physical_read_columns,
        &mut read_names,
        &mut read_slots,
        &mut scan_slots,
        &mut next_hidden_column_id,
    )?;
    ensure_virtual_only_scan_has_row_count_carrier(
        &mut scan_columns,
        &mut scan_names,
        &mut scan_slots,
        &mut physical_read_columns,
        &mut read_names,
        &mut read_slots,
        &mut next_hidden_column_id,
        &iceberg_virtual,
    )?;

    let read_layout = layout_from_output_columns(&scan_columns)?;
    let read_schema = iceberg_chunk_schema_from_output_columns_with_variants(
        table,
        &scan_columns,
        &variant_path_plan,
    )?;
    let parquet_schema = iceberg_chunk_schema_from_output_columns(table, &physical_read_columns)?;
    let read_slot_ids = physical_read_columns
        .iter()
        .map(|col| SlotId::new(col.column_id))
        .collect::<Vec<_>>();
    let slot_kinds = physical_read_columns
        .iter()
        .map(parquet_slot_kind_from_native_column)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScanReadPlan {
        output_layout,
        output_schema,
        read_layout,
        read_schema,
        parquet_schema,
        read_columns: physical_read_columns
            .into_iter()
            .map(|col| col.name)
            .collect(),
        read_slot_ids,
        slot_kinds,
        variant_path_columns: variant_path_plan.specs,
        iceberg_virtual,
    })
}

#[allow(clippy::too_many_arguments)]
fn ensure_native_variant_source_read_columns(
    scan: &plan::ScanNode,
    plan: &mut NativeVariantPathPlan,
    physical_read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    scan_slots: &mut HashSet<u32>,
    next_hidden_column_id: &mut u32,
) -> Result<(), String> {
    if plan.specs.is_empty() {
        return Ok(());
    }

    let mut reserved_slots = scan_slots.clone();
    reserved_slots.extend(plan.specs.iter().map(|spec| spec.source_slot_id.as_u32()));
    reserved_slots.extend(plan.specs.iter().map(|spec| spec.output_slot_id.as_u32()));

    for spec in &mut plan.specs {
        if let Some(read_col) = physical_read_columns.iter().find(|col| {
            SlotId::new(col.column_id) == spec.source_slot_id || col.name == spec.source_name
        }) {
            spec.source_read_slot_id = SlotId::new(read_col.column_id);
            continue;
        }

        let hidden_id = allocate_hidden_column_id(next_hidden_column_id, &reserved_slots)?;
        reserved_slots.insert(hidden_id);
        scan_slots.insert(hidden_id);
        let source_col = output_column_from_table_def(scan, &spec.source_name, hidden_id)?;
        push_physical_read_column(physical_read_columns, read_names, read_slots, source_col)?;
        spec.source_read_slot_id = SlotId::new(hidden_id);
    }

    Ok(())
}

fn push_physical_read_column(
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

#[allow(clippy::too_many_arguments)]
fn push_scan_column(
    table: &plan::IcebergTableInfo,
    scan_columns: &mut Vec<common::OutputColumn>,
    scan_names: &mut HashSet<String>,
    scan_slots: &mut HashSet<u32>,
    physical_read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    iceberg_virtual: &mut IcebergVirtualSpec,
    col: common::OutputColumn,
) -> Result<(), String> {
    if !scan_names.insert(col.name.clone()) {
        return Err(format!("ScanNode duplicate read column name {}", col.name));
    }
    if !scan_slots.insert(col.column_id) {
        return Err(format!(
            "ScanNode duplicate read column id {} for {}",
            col.column_id, col.name
        ));
    }
    if !record_iceberg_virtual_column(table, &col, iceberg_virtual)? {
        push_physical_read_column(physical_read_columns, read_names, read_slots, col.clone())?;
    }
    scan_columns.push(col);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn ensure_virtual_only_scan_has_row_count_carrier(
    scan_columns: &mut Vec<common::OutputColumn>,
    scan_names: &mut HashSet<String>,
    scan_slots: &mut HashSet<u32>,
    physical_read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    next_hidden_column_id: &mut u32,
    iceberg_virtual: &IcebergVirtualSpec,
) -> Result<(), String> {
    if !physical_read_columns.is_empty() || iceberg_virtual.is_empty() {
        return Ok(());
    }
    let column_id = allocate_hidden_column_id(next_hidden_column_id, scan_slots)?;
    let column = iceberg_virtual_count_column(column_id);
    if !scan_names.insert(column.name.clone()) {
        return Err(format!(
            "ScanNode duplicate read column name {}",
            column.name
        ));
    }
    if !scan_slots.insert(column.column_id) {
        return Err(format!(
            "ScanNode duplicate read column id {} for {}",
            column.column_id, column.name
        ));
    }
    scan_columns.push(column.clone());
    push_physical_read_column(physical_read_columns, read_names, read_slots, column)
}

fn parquet_slot_kind_from_native_column(
    column: &common::OutputColumn,
) -> Result<ParquetSlotKind, String> {
    let data_type = output_column_data_type(column)?;
    if matches!(data_type, DataType::LargeBinary) {
        Ok(ParquetSlotKind::Variant)
    } else {
        Ok(ParquetSlotKind::Regular)
    }
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

pub(super) fn maybe_project_data_scan_output(
    node_id: i32,
    scan_lowered: DecodedNode,
    read_plan: ScanReadPlan,
    arena: &mut ExprArena,
) -> Result<DecodedNode, String> {
    if read_plan.read_layout.order() == read_plan.output_layout.order() {
        return Ok(DecodedNode {
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
    Ok(DecodedNode {
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
