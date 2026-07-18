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

use super::super::node::DecodedNode;
use super::common::{DecodedScanOutputColumns, ProvenancedOutputColumn};
use super::schema::iceberg_chunk_schema_from_provenanced_columns;
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
use crate::protocol::common::error::FieldPath;
use crate::protocol::native::decode::NativeFragmentDecodeError;

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
    expression_path: FieldPath,
}

#[derive(Clone, Debug)]
struct HiddenColumnIdAllocator {
    next: u32,
    upper_bound_path: FieldPath,
}

impl HiddenColumnIdAllocator {
    fn from_output_columns(columns: &[ProvenancedOutputColumn]) -> Option<Self> {
        let max_column = columns
            .iter()
            .max_by_key(|column| column.column().column_id)?;
        Some(Self {
            next: max_column.column().column_id.saturating_add(1),
            upper_bound_path: max_column.source_path().field("column_id"),
        })
    }

    fn allocate(&mut self, used: &HashSet<u32>) -> Result<u32, NativeFragmentDecodeError> {
        loop {
            let id = self.next;
            self.next = self.next.checked_add(1).ok_or_else(|| {
                NativeFragmentDecodeError::out_of_range(
                    self.upper_bound_path.clone(),
                    "hidden read column id overflow",
                )
            })?;
            if !used.contains(&id) {
                return Ok(id);
            }
        }
    }
}

pub(super) fn scan_read_plan(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    decoded_output_columns: &DecodedScanOutputColumns,
    scan_path: FieldPath,
    source_path: FieldPath,
) -> Result<ScanReadPlan, NativeFragmentDecodeError> {
    let output_columns = decoded_output_columns.columns();
    let output_layout = decoded_output_columns.layout();
    let mut variant_path_plan = parse_native_scan_variant_path_columns(scan, table, output_columns)
        .map_err(|error| error.into_native(scan_path.clone()))?;
    let output_schema = iceberg_chunk_schema_from_provenanced_columns(
        table,
        source_path.clone(),
        decoded_output_columns.provenanced(),
        &variant_path_plan,
    )?;

    let mut scan_columns = decoded_output_columns.provenanced().to_vec();
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
    for col in decoded_output_columns.provenanced() {
        let raw = col.column();
        if variant_path_plan
            .output_slot_ids
            .contains(&SlotId::new(raw.column_id))
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
    let mut hidden_column_ids =
        HiddenColumnIdAllocator::from_output_columns(decoded_output_columns.provenanced())
            .ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    scan_path.clone().field("columns"),
                    "ScanNode columns missing",
                )
            })?;

    let predicate_refs = scan_predicate_column_refs(&scan.predicates, scan_path.clone())?;
    let predicate_refs_by_name = predicate_refs
        .values()
        .filter_map(|col| col.name.as_ref().map(|name| (name.clone(), col)))
        .collect::<HashMap<_, _>>();
    let required_names = scan
        .required_columns
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    for (required_index, required) in scan.required_columns.iter().enumerate() {
        if scan_names.contains(required) || read_names.contains(required) {
            continue;
        }
        let predicate = predicate_refs_by_name.get(required).copied();
        let hidden_id = match predicate {
            Some(predicate) => predicate.column_id,
            None => hidden_column_ids.allocate(&scan_slots)?,
        };
        let col = match output_column_from_table_def(scan, required, hidden_id, scan_path.clone())?
        {
            Some(column) => column,
            None if predicate.is_some() => {
                output_column_from_predicate_ref(predicate.expect("checked predicate"))?
            }
            None => {
                return Err(NativeFragmentDecodeError::invalid_value(
                    scan_path
                        .clone()
                        .field("required_columns")
                        .index(required_index),
                    format!("required column {required} is not in table column definitions"),
                ));
            }
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
            NativeFragmentDecodeError::missing(
                pred_col
                    .expression_path
                    .clone()
                    .field("column_ref")
                    .field("column"),
                format!(
                    "predicate column_id={} is not an output column and does not carry a column name",
                    pred_col.column_id
                ),
            )
        })?;
        if !required_names.is_empty() && !required_names.contains(name) {
            return Err(NativeFragmentDecodeError::inconsistent(
                scan_path.clone().field("required_columns"),
                format!("predicate column {name} is not listed in required_columns"),
            ));
        }
        let column =
            output_column_from_table_def(scan, name, pred_col.column_id, scan_path.clone())?
                .map(Ok)
                .unwrap_or_else(|| output_column_from_predicate_ref(pred_col))?;
        push_scan_column(
            table,
            &mut scan_columns,
            &mut scan_names,
            &mut scan_slots,
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            &mut iceberg_virtual,
            column,
        )?;
    }

    ensure_native_variant_source_read_columns(
        scan,
        &mut variant_path_plan,
        &mut physical_read_columns,
        &mut read_names,
        &mut read_slots,
        &mut scan_slots,
        &mut hidden_column_ids,
        scan_path.clone(),
    )?;
    ensure_virtual_only_scan_has_row_count_carrier(
        &mut scan_columns,
        &mut scan_names,
        &mut scan_slots,
        &mut physical_read_columns,
        &mut read_names,
        &mut read_slots,
        &mut hidden_column_ids,
        &iceberg_virtual,
        decoded_output_columns
            .provenanced()
            .first()
            .expect("scan output columns")
            .source_path(),
    )?;

    let read_layout = super::super::layout::Layout::for_slots(
        scan_columns
            .iter()
            .map(|column| SlotId::new(column.column().column_id)),
    );
    let read_schema = iceberg_chunk_schema_from_provenanced_columns(
        table,
        source_path.clone(),
        &scan_columns,
        &variant_path_plan,
    )?;
    let parquet_schema = iceberg_chunk_schema_from_provenanced_columns(
        table,
        source_path,
        &physical_read_columns,
        &NativeVariantPathPlan::default(),
    )?;
    let read_slot_ids = physical_read_columns
        .iter()
        .map(|col| SlotId::new(col.column().column_id))
        .collect::<Vec<_>>();
    let slot_kinds = physical_read_columns
        .iter()
        .map(parquet_slot_kind_from_native_column)
        .collect::<Vec<_>>();
    Ok(ScanReadPlan {
        output_layout,
        output_schema,
        read_layout,
        read_schema,
        parquet_schema,
        read_columns: physical_read_columns
            .into_iter()
            .map(|col| col.column().name.clone())
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
    physical_read_columns: &mut Vec<ProvenancedOutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    scan_slots: &mut HashSet<u32>,
    hidden_column_ids: &mut HiddenColumnIdAllocator,
    scan_path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    if plan.specs.is_empty() {
        return Ok(());
    }

    let mut reserved_slots = scan_slots.clone();
    reserved_slots.extend(plan.specs.iter().map(|spec| spec.source_slot_id.as_u32()));
    reserved_slots.extend(plan.specs.iter().map(|spec| spec.output_slot_id.as_u32()));

    for spec in &mut plan.specs {
        if let Some(read_col) = physical_read_columns.iter().find(|col| {
            SlotId::new(col.column().column_id) == spec.source_slot_id
                || col.column().name == spec.source_name
        }) {
            spec.source_read_slot_id = SlotId::new(read_col.column().column_id);
            continue;
        }

        let hidden_id = hidden_column_ids.allocate(&reserved_slots)?;
        reserved_slots.insert(hidden_id);
        scan_slots.insert(hidden_id);
        let source_col =
            output_column_from_table_def(scan, &spec.source_name, hidden_id, scan_path.clone())?
                .ok_or_else(|| {
                    NativeFragmentDecodeError::invalid_value(
                        scan_path.clone().field("variant_columns"),
                        format!(
                            "variant source column {} is not in table column definitions",
                            spec.source_name
                        ),
                    )
                })?;
        push_physical_read_column(physical_read_columns, read_names, read_slots, source_col)?;
        spec.source_read_slot_id = SlotId::new(hidden_id);
    }

    Ok(())
}

fn push_physical_read_column(
    read_columns: &mut Vec<ProvenancedOutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    col: ProvenancedOutputColumn,
) -> Result<(), NativeFragmentDecodeError> {
    let raw = col.column();
    if !read_slots.insert(raw.column_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            col.source_path(),
            format!("read columns contain duplicate column_id={}", raw.column_id),
        ));
    }
    if !read_names.insert(raw.name.clone()) {
        return Err(NativeFragmentDecodeError::inconsistent(
            col.name_path(),
            format!("read columns contain duplicate column name {}", raw.name),
        ));
    }
    read_columns.push(col);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_scan_column(
    table: &plan::IcebergTableInfo,
    scan_columns: &mut Vec<ProvenancedOutputColumn>,
    scan_names: &mut HashSet<String>,
    scan_slots: &mut HashSet<u32>,
    physical_read_columns: &mut Vec<ProvenancedOutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    iceberg_virtual: &mut IcebergVirtualSpec,
    col: ProvenancedOutputColumn,
) -> Result<(), NativeFragmentDecodeError> {
    let raw = col.column();
    if !scan_names.insert(raw.name.clone()) {
        return Err(NativeFragmentDecodeError::inconsistent(
            col.name_path(),
            format!("duplicate read column name {}", raw.name),
        ));
    }
    if !scan_slots.insert(raw.column_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            col.source_path(),
            format!(
                "duplicate read column id {} for {}",
                raw.column_id, raw.name
            ),
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
    scan_columns: &mut Vec<ProvenancedOutputColumn>,
    scan_names: &mut HashSet<String>,
    scan_slots: &mut HashSet<u32>,
    physical_read_columns: &mut Vec<ProvenancedOutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    hidden_column_ids: &mut HiddenColumnIdAllocator,
    iceberg_virtual: &IcebergVirtualSpec,
    source_path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    if !physical_read_columns.is_empty() || iceberg_virtual.is_empty() {
        return Ok(());
    }
    let column_id = hidden_column_ids.allocate(scan_slots)?;
    let raw_column = iceberg_virtual_count_column(column_id);
    let column = ProvenancedOutputColumn::trusted_internal(
        raw_column,
        source_path.clone(),
        source_path.field("name"),
    );
    if !scan_names.insert(column.column().name.clone()) {
        return Err(NativeFragmentDecodeError::inconsistent(
            column.name_path(),
            format!("duplicate read column name {}", column.column().name),
        ));
    }
    if !scan_slots.insert(column.column().column_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            column.source_path(),
            format!(
                "duplicate read column id {} for {}",
                column.column().column_id,
                column.column().name
            ),
        ));
    }
    scan_columns.push(column.clone());
    push_physical_read_column(physical_read_columns, read_names, read_slots, column)
}

fn parquet_slot_kind_from_native_column(column: &ProvenancedOutputColumn) -> ParquetSlotKind {
    if matches!(
        column.slot_schema().data_type(),
        arrow::datatypes::DataType::LargeBinary
    ) {
        ParquetSlotKind::Variant
    } else {
        ParquetSlotKind::Regular
    }
}

fn output_column_from_predicate_ref(
    col: &PredicateColumnRef,
) -> Result<ProvenancedOutputColumn, NativeFragmentDecodeError> {
    let name = col.name.clone().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            col.expression_path
                .clone()
                .field("column_ref")
                .field("column"),
            format!(
                "predicate column_id={} requires a column name for hidden read binding",
                col.column_id
            ),
        )
    })?;
    let source_path = col.expression_path.clone().field("column_ref");
    let column = common::OutputColumn {
        column_id: col.column_id,
        name,
        r#type: col.r#type.clone(),
        nullable: col.nullable,
        is_internal: false,
    };
    ProvenancedOutputColumn::decode(
        column,
        source_path.clone(),
        source_path.field("column"),
        col.expression_path.clone().field("type"),
    )
}

fn output_column_from_table_def(
    scan: &plan::ScanNode,
    name: &str,
    column_id: u32,
    scan_path: FieldPath,
) -> Result<Option<ProvenancedOutputColumn>, NativeFragmentDecodeError> {
    let table = scan.table.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            scan_path.clone().field("table"),
            "ScanNode table missing",
        )
    })?;
    let located = table
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.name == name)
        .map(|(index, column)| {
            (
                column,
                scan_path
                    .clone()
                    .field("table")
                    .field("columns")
                    .index(index),
            )
        })
        .or_else(|| {
            table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name == name)
                .map(|(index, column)| {
                    (
                        column,
                        scan_path
                            .clone()
                            .field("table")
                            .field("iceberg_row_lineage_metadata_columns")
                            .index(index),
                    )
                })
        });
    let Some((column, source_path)) = located else {
        return Ok(None);
    };
    let (ty, type_path) = if let Some(logical_type) = column.logical_type.as_ref() {
        (
            logical_type.clone(),
            source_path.clone().field("logical_type"),
        )
    } else if let Some(data_type) = column.data_type.as_ref() {
        (data_type.clone(), source_path.clone().field("data_type"))
    } else {
        return Err(NativeFragmentDecodeError::missing(
            source_path.clone().field("data_type"),
            format!("required column {name} type missing"),
        ));
    };
    let output = common::OutputColumn {
        column_id,
        name: column.name.clone(),
        r#type: Some(ty),
        nullable: column.nullable,
        is_internal: true,
    };
    ProvenancedOutputColumn::decode(
        output,
        source_path.clone(),
        source_path.field("name"),
        type_path,
    )
    .map(Some)
}

fn scan_predicate_column_refs(
    predicates: &[crate::proto::expr::Expr],
    scan_path: FieldPath,
) -> Result<BTreeMap<u32, PredicateColumnRef>, NativeFragmentDecodeError> {
    let mut refs = BTreeMap::new();
    for (index, predicate) in predicates.iter().enumerate() {
        collect_predicate_column_refs(
            predicate,
            scan_path.clone().field("predicates").index(index),
            &mut refs,
        )?;
    }
    Ok(refs)
}

fn collect_predicate_column_refs(
    expr: &crate::proto::expr::Expr,
    expression_path: FieldPath,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), NativeFragmentDecodeError> {
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
                expression_path: expression_path.clone(),
            };
            if let Some(prev) = refs.insert(col.column_id, next.clone()) {
                if prev.name != next.name {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        expression_path.field("column_ref").field("column"),
                        format!(
                            "predicate column_id={} has inconsistent names {:?} and {:?}",
                            col.column_id, prev.name, next.name
                        ),
                    ));
                }
            }
        }
        Kind::Literal(_) | Kind::LambdaParamRef(_) => {}
        Kind::BinaryOp(binary) => {
            let path = expression_path.field("binary_op");
            collect_optional_box_expr(&binary.left, path.clone().field("left"), refs)?;
            collect_optional_box_expr(&binary.right, path.field("right"), refs)?;
        }
        Kind::UnaryOp(unary) => collect_optional_box_expr(
            &unary.operand,
            expression_path.field("unary_op").field("operand"),
            refs,
        )?,
        Kind::FunctionCall(call) => collect_expr_list(
            &call.args,
            expression_path.field("function_call").field("args"),
            refs,
        )?,
        Kind::AggregateCall(call) => {
            let path = expression_path.field("aggregate_call");
            collect_expr_list(&call.args, path.clone().field("args"), refs)?;
            collect_sort_items(&call.order_by, path.field("order_by"), refs)?;
        }
        Kind::WindowCall(call) => {
            let path = expression_path.field("window_call");
            collect_expr_list(&call.args, path.clone().field("args"), refs)?;
            collect_expr_list(&call.partition_by, path.clone().field("partition_by"), refs)?;
            collect_sort_items(&call.order_by, path.field("order_by"), refs)?;
        }
        Kind::Cast(cast) => collect_optional_box_expr(
            &cast.operand,
            expression_path.field("cast").field("operand"),
            refs,
        )?,
        Kind::IsNull(is_null) => collect_optional_box_expr(
            &is_null.operand,
            expression_path.field("is_null").field("operand"),
            refs,
        )?,
        Kind::InList(in_list) => {
            let path = expression_path.field("in_list");
            collect_optional_box_expr(&in_list.operand, path.clone().field("operand"), refs)?;
            collect_expr_list(&in_list.list, path.field("list"), refs)?;
        }
        Kind::Between(between) => {
            let path = expression_path.field("between");
            collect_optional_box_expr(&between.operand, path.clone().field("operand"), refs)?;
            collect_optional_box_expr(&between.low, path.clone().field("low"), refs)?;
            collect_optional_box_expr(&between.high, path.field("high"), refs)?;
        }
        Kind::Like(like) => {
            let path = expression_path.field("like");
            collect_optional_box_expr(&like.operand, path.clone().field("operand"), refs)?;
            collect_optional_box_expr(&like.pattern, path.field("pattern"), refs)?;
        }
        Kind::CaseExpr(case_expr) => {
            let path = expression_path.field("case_expr");
            collect_optional_box_expr(&case_expr.operand, path.clone().field("operand"), refs)?;
            for (index, branch) in case_expr.when_then.iter().enumerate() {
                let branch_path = path.clone().field("when_then").index(index);
                collect_optional_expr(&branch.when, branch_path.clone().field("when"), refs)?;
                collect_optional_expr(&branch.then, branch_path.field("then"), refs)?;
            }
            collect_optional_box_expr(&case_expr.else_expr, path.field("else_expr"), refs)?;
        }
        Kind::IsTruth(is_truth) => collect_optional_box_expr(
            &is_truth.operand,
            expression_path.field("is_truth").field("operand"),
            refs,
        )?,
        Kind::Lambda(lambda) => collect_optional_box_expr(
            &lambda.body,
            expression_path.field("lambda").field("body"),
            refs,
        )?,
        Kind::Nested(nested) => collect_optional_box_expr(
            &nested.inner,
            expression_path.field("nested").field("inner"),
            refs,
        )?,
    }
    Ok(())
}

fn collect_optional_box_expr(
    expr: &Option<Box<crate::proto::expr::Expr>>,
    expression_path: FieldPath,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), NativeFragmentDecodeError> {
    if let Some(expr) = expr.as_ref() {
        collect_predicate_column_refs(expr, expression_path, refs)?;
    }
    Ok(())
}

fn collect_optional_expr(
    expr: &Option<crate::proto::expr::Expr>,
    expression_path: FieldPath,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), NativeFragmentDecodeError> {
    if let Some(expr) = expr.as_ref() {
        collect_predicate_column_refs(expr, expression_path, refs)?;
    }
    Ok(())
}

fn collect_expr_list(
    exprs: &[crate::proto::expr::Expr],
    list_path: FieldPath,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), NativeFragmentDecodeError> {
    for (index, expr) in exprs.iter().enumerate() {
        collect_predicate_column_refs(expr, list_path.clone().index(index), refs)?;
    }
    Ok(())
}

fn collect_sort_items(
    items: &[crate::proto::expr::SortItem],
    list_path: FieldPath,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), NativeFragmentDecodeError> {
    for (index, item) in items.iter().enumerate() {
        collect_optional_expr(
            &item.expr,
            list_path.clone().index(index).field("expr"),
            refs,
        )?;
    }
    Ok(())
}

pub(super) fn maybe_project_data_scan_output(
    node_id: i32,
    scan_lowered: DecodedNode,
    read_plan: ScanReadPlan,
    arena: &mut ExprArena,
) -> DecodedNode {
    if read_plan.read_layout.order() == read_plan.output_layout.order() {
        return DecodedNode {
            node: scan_lowered.node,
            layout: read_plan.output_layout,
            output_schema: read_plan.output_schema,
        };
    }
    let exprs = read_plan
        .output_layout
        .order()
        .iter()
        .zip(read_plan.output_schema.slots())
        .map(|(slot_id, slot)| {
            arena.push_typed(ExprNode::SlotId(*slot_id), slot.data_type().clone())
        })
        .collect::<Vec<_>>();
    DecodedNode {
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
    }
}
