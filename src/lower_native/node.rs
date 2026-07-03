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

//! Proto node lowering placeholder.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use super::expr::lower_proto_expr;
use super::layout::{
    Layout, chunk_schema_from_output_columns, layout_from_output_columns,
    slot_schemas_from_output_columns,
};
use crate::common::config::exchange_wait_ms;
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::limit::LimitNode;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::set_op::{SetOpKind, SetOpNode};
use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{common, expr, plan};
use crate::runtime::exchange::ExchangeKey;

#[derive(Clone, Debug)]
pub(crate) struct LoweredNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeLoweringContext {
    exchange_sender_counts: HashMap<ExchangeKey, usize>,
    fragment_instance_hi: i64,
    fragment_instance_lo: i64,
}

impl NodeLoweringContext {
    #[allow(dead_code)]
    pub(crate) fn with_fragment_instance_id(mut self, hi: i64, lo: i64) -> Self {
        self.fragment_instance_hi = hi;
        self.fragment_instance_lo = lo;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        self.exchange_sender_counts.insert(key, count);
        self
    }

    fn exchange_key(&self, node_id: i32) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: self.fragment_instance_hi,
            finst_id_lo: self.fragment_instance_lo,
            node_id,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn lower_proto_node(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let children = node
        .children
        .iter()
        .map(|child| lower_proto_node(child, arena, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    let payload = node
        .payload
        .as_ref()
        .ok_or_else(|| format!("DistributedNode node_id={} payload missing", node.node_id))?;
    match payload {
        plan::distributed_node::Payload::Physical(physical) => {
            lower_physical_node(node, physical, children, arena, ctx)
        }
        plan::distributed_node::Payload::Exchange(exchange) => {
            lower_exchange_receiver(node, exchange, children, ctx)
        }
    }
}

fn lower_physical_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
    _ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let kind = physical
        .kind
        .as_ref()
        .ok_or_else(|| format!("PlanNode node_id={} kind missing", node.node_id))?;
    match kind {
        plan::plan_node::Kind::Values(values) => {
            lower_values_node(node, physical, values, children, arena)
        }
        plan::plan_node::Kind::Project(project) => {
            lower_project_node(node, project, children, arena)
        }
        plan::plan_node::Kind::Filter(filter) => lower_filter_node(node, filter, children, arena),
        plan::plan_node::Kind::Limit(limit) => lower_limit_node(node, limit, children),
        plan::plan_node::Kind::Sort(sort) => lower_sort_node(node, physical, sort, children, arena),
        plan::plan_node::Kind::Topn(topn) => lower_topn_node(node, topn, children, arena),
        plan::plan_node::Kind::SetOp(set_op) => {
            lower_set_op_node(node, physical, set_op, children, arena)
        }
        plan::plan_node::Kind::AssertOneRow(assert) => {
            lower_assert_one_row_node(node, assert, children)
        }
        plan::plan_node::Kind::Scan(_) => unsupported("Scan"),
        plan::plan_node::Kind::HashAggregate(_) => unsupported("HashAggregate"),
        plan::plan_node::Kind::HashJoin(_) => unsupported("HashJoin"),
        plan::plan_node::Kind::NestLoopJoin(_) => unsupported("NestLoopJoin"),
        plan::plan_node::Kind::Window(_) => unsupported("Window"),
        plan::plan_node::Kind::Repeat(_) => unsupported("Repeat"),
        plan::plan_node::Kind::GenerateSeries(_) => unsupported("GenerateSeries"),
        plan::plan_node::Kind::TableFunction(_) => unsupported("TableFunction"),
        plan::plan_node::Kind::Decode(_) => unsupported("Decode"),
        plan::plan_node::Kind::ChangeEventExpand(_) => unsupported("ChangeEventExpand"),
        plan::plan_node::Kind::CteAnchor(_) => unsupported("CTEAnchor"),
        plan::plan_node::Kind::CteProduce(_) => unsupported("CTEProduce"),
        plan::plan_node::Kind::CteConsume(_) => unsupported("CTEConsume"),
        plan::plan_node::Kind::Redistribute(_) => unsupported("Redistribute"),
    }
}

fn unsupported<T>(kind: &str) -> Result<T, String> {
    Err(format!(
        "{kind} native proto node lowering is not implemented in M2.4a"
    ))
}

fn check_arity(kind: &str, expected: &str, actual: usize, ok: bool) -> Result<(), String> {
    if ok {
        Ok(())
    } else {
        Err(format!("{kind} expected {expected} children, got {actual}"))
    }
}

fn check_exact_arity(kind: &str, expected: usize, actual: usize) -> Result<(), String> {
    check_arity(kind, &expected.to_string(), actual, actual == expected)
}

fn check_min_arity(kind: &str, min: usize, actual: usize) -> Result<(), String> {
    check_arity(kind, &format!(">={min}"), actual, actual >= min)
}

fn lower_values_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    values: &plan::ValuesNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ValuesNode", 0, children.len())?;
    let columns = if values.columns.is_empty() {
        &physical.output_columns
    } else {
        &values.columns
    };
    let layout = layout_from_output_columns(columns)?;
    let output_schema = chunk_schema_from_output_columns(columns)?;
    let chunk = materialize_values_chunk(&values.rows, columns, output_schema.clone(), arena)?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout,
        output_schema,
    })
}

fn materialize_values_chunk(
    rows: &[plan::ExprList],
    columns: &[common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Chunk, String> {
    if rows.is_empty() {
        let batch = RecordBatch::new_empty(output_schema.arrow_schema_ref());
        return Chunk::try_new_with_chunk_schema(batch, output_schema);
    }
    let column_count = columns.len();
    let mut arrays_by_column = vec![Vec::<ArrayRef>::with_capacity(rows.len()); column_count];
    let input_layout = Layout::default();
    let one_row = empty_chunk_with_row_count(1)?;

    for (row_idx, row) in rows.iter().enumerate() {
        if row.values.len() != column_count {
            return Err(format!(
                "ValuesNode row {row_idx} width mismatch: expected {column_count}, got {}",
                row.values.len()
            ));
        }
        for (col_idx, expr) in row.values.iter().enumerate() {
            let expr_id = lower_proto_expr(expr, arena, &input_layout)
                .map_err(|err| format!("ValuesNode row {row_idx} column {col_idx}: {err}"))?;
            let array = arena
                .eval(expr_id, &one_row)
                .map_err(|err| format!("ValuesNode row {row_idx} column {col_idx}: {err}"))?;
            if array.len() != 1 {
                return Err(format!(
                    "ValuesNode row {row_idx} column {col_idx} evaluated to {} rows, expected 1",
                    array.len()
                ));
            }
            arrays_by_column[col_idx].push(array);
        }
    }

    let columns = arrays_by_column
        .into_iter()
        .enumerate()
        .map(|(col_idx, parts)| {
            let refs = parts
                .iter()
                .map(|part| part.as_ref() as &dyn Array)
                .collect::<Vec<_>>();
            concat(&refs).map_err(|err| format!("ValuesNode column {col_idx} concat failed: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Chunk::try_new_with_columns(output_schema, columns)
}

fn empty_chunk_with_row_count(row_count: usize) -> Result<Chunk, String> {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = RecordBatch::try_new_with_options(schema, Vec::new(), &options)
        .map_err(|err| format!("build empty values input chunk failed: {err}"))?;
    Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
}

fn lower_project_node(
    node: &plan::DistributedNode,
    project: &plan::ProjectNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ProjectNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let output_columns = project_output_columns(project)?;
    let layout = layout_from_output_columns(&output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&output_columns)?;
    let expr_slot_schemas = slot_schemas_from_output_columns(&output_columns)?;

    let exprs = project
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let expr = item
                .expr
                .as_ref()
                .ok_or_else(|| format!("ProjectNode item {idx} expr missing"))?;
            lower_proto_expr(expr, arena, &child.layout)
                .map_err(|err| format!("ProjectNode item {idx}: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expr_slot_ids = layout.order().to_vec();

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: None,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

fn project_output_columns(
    project: &plan::ProjectNode,
) -> Result<Vec<common::OutputColumn>, String> {
    project
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let expr = item
                .expr
                .as_ref()
                .ok_or_else(|| format!("ProjectNode item {idx} expr missing"))?;
            let r#type = expr
                .r#type
                .clone()
                .ok_or_else(|| format!("ProjectNode item {idx} expr type missing"))?;
            Ok(common::OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                r#type: Some(r#type),
                nullable: expr.nullable,
                is_internal: false,
            })
        })
        .collect()
}

fn lower_filter_node(
    node: &plan::DistributedNode,
    filter: &plan::FilterNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("FilterNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let predicate = filter
        .predicate
        .as_ref()
        .ok_or_else(|| "FilterNode predicate missing".to_string())?;
    let predicate = lower_proto_expr(predicate, arena, &child.layout)
        .map_err(|err| format!("FilterNode predicate: {err}"))?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                predicate,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_limit_node(
    node: &plan::DistributedNode,
    limit_node: &plan::LimitNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("LimitNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let payload_limit = parse_optional_nonnegative_i64(limit_node.limit, "LimitNode.limit")?;
    let outer_limit = parse_distributed_limit(node.limit, "LimitNode DistributedNode.limit")?;
    let limit = merge_limits("LimitNode", payload_limit, outer_limit)?;
    let offset =
        parse_optional_nonnegative_i64(limit_node.offset, "LimitNode.offset")?.unwrap_or(0);
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Limit(LimitNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                limit,
                offset,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_sort_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    sort: &plan::SortNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("SortNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let output_columns = if sort.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &sort.output_columns
    };
    let (layout, output_schema) = if output_columns.is_empty() {
        (child.layout.clone(), child.output_schema.clone())
    } else {
        let layout = layout_from_output_columns(output_columns)?;
        if layout.order() != child.layout.order() {
            return Err(format!(
                "SortNode output column reorder is not implemented in M2.4a: child={:?} output={:?}",
                child.layout.order(),
                layout.order()
            ));
        }
        (layout, chunk_schema_from_output_columns(output_columns)?)
    };
    let order_by = lower_sort_items("SortNode", &sort.items, arena, &child.layout)?;
    let limit = parse_distributed_limit(node.limit, "SortNode DistributedNode.limit")?;
    let offset = parse_optional_nonnegative_i64(sort.offset, "SortNode.offset")?.unwrap_or(0);
    let topn_type = parse_sort_topn_type(sort.topn_type)?;
    let partition_exprs = sort
        .analytic_partition_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let expr = lower_proto_expr(expr, arena, &child.layout)
                .map_err(|err| format!("SortNode analytic_partition_by[{idx}]: {err}"))?;
            Ok(SortExpression {
                expr,
                asc: true,
                nulls_first: true,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let partition_limit = sort.partition_limit.map(|value| value as usize);
    let use_top_n = partition_limit.is_some();
    if use_top_n && topn_type != SortTopNType::RowNumber && offset != 0 {
        return Err(format!(
            "SortNode node_id={} topn_type {:?} requires offset=0, got {}",
            node.node_id, topn_type, offset
        ));
    }
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                use_top_n,
                order_by,
                limit,
                offset,
                topn_type,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs,
                partition_limit,
            }),
        },
        layout,
        output_schema,
    })
}

fn lower_topn_node(
    node: &plan::DistributedNode,
    topn: &plan::TopNNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("TopNNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let payload_limit = parse_optional_nonnegative_i64(topn.limit, "TopNNode.limit")?;
    let outer_limit = parse_distributed_limit(node.limit, "TopNNode DistributedNode.limit")?;
    let limit = merge_limits("TopNNode", payload_limit, outer_limit)?;
    if limit.is_none() {
        return Err("TopNNode requires a non-negative limit".to_string());
    }
    let offset = parse_optional_nonnegative_i64(topn.offset, "TopNNode.offset")?.unwrap_or(0);
    let phase = plan::TopNPhase::try_from(topn.phase)
        .map_err(|_| format!("TopNNode unknown phase {}", topn.phase))?;
    if phase == plan::TopNPhase::TopnPhaseUnspecified {
        return Err("TopNNode phase is unspecified".to_string());
    }
    let order_by = lower_sort_items("TopNNode", &topn.items, arena, &child.layout)?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                use_top_n: true,
                order_by,
                limit,
                offset,
                topn_type: SortTopNType::RowNumber,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_sort_items(
    node_kind: &str,
    items: &[expr::SortItem],
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<SortExpression>, String> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let expr = item
                .expr
                .as_ref()
                .ok_or_else(|| format!("{node_kind} sort item {idx} expr missing"))?;
            let expr = lower_proto_expr(expr, arena, input_layout)
                .map_err(|err| format!("{node_kind} sort item {idx}: {err}"))?;
            Ok(SortExpression {
                expr,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

fn parse_sort_topn_type(value: Option<i32>) -> Result<SortTopNType, String> {
    let Some(value) = value else {
        return Ok(SortTopNType::RowNumber);
    };
    match plan::SortTopNType::try_from(value)
        .map_err(|_| format!("SortNode unknown topn_type {value}"))?
    {
        plan::SortTopNType::SortTopnTypeUnspecified | plan::SortTopNType::SortTopnTypeRowNumber => {
            Ok(SortTopNType::RowNumber)
        }
        plan::SortTopNType::SortTopnTypeRank => Ok(SortTopNType::Rank),
        plan::SortTopNType::SortTopnTypeDenseRank => Ok(SortTopNType::DenseRank),
    }
}

fn lower_exchange_receiver(
    node: &plan::DistributedNode,
    exchange: &plan::ExchangeReceiver,
    children: Vec<LoweredNode>,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    check_exact_arity("ExchangeReceiver", 0, children.len())?;
    let flavor = exchange
        .flavor
        .as_ref()
        .and_then(|flavor| flavor.kind.as_ref())
        .ok_or_else(|| "ExchangeReceiver flavor missing".to_string())?;
    match flavor {
        plan::exchange_flavor::Kind::Distribution(true) => {}
        plan::exchange_flavor::Kind::Distribution(false) => {
            return Err("ExchangeReceiver distribution flavor must be true".to_string());
        }
        plan::exchange_flavor::Kind::LimitOffset(_) => {
            return unsupported("ExchangeReceiver LimitOffset");
        }
        plan::exchange_flavor::Kind::TopnSplit(_) => {
            return unsupported("ExchangeReceiver TopNSplit");
        }
        plan::exchange_flavor::Kind::CteMulticast(_) => {
            return unsupported("ExchangeReceiver CteMulticast");
        }
    }

    let key = ctx.exchange_key(node.node_id);
    let expected_senders = ctx
        .exchange_sender_counts
        .get(&key)
        .copied()
        .ok_or_else(|| {
            format!(
                "ExchangeReceiver missing sender count for node_id {} (key={:?})",
                node.node_id, key
            )
        })?;
    if expected_senders == 0 {
        return Err(format!(
            "ExchangeReceiver sender count must be > 0 for node_id {}",
            node.node_id
        ));
    }
    let layout = layout_from_output_columns(&exchange.output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&exchange.output_columns)?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::ExchangeSource(ExchangeSourceNode::new(
                key,
                expected_senders,
                Duration::from_millis(exchange_wait_ms()),
                output_schema.clone(),
            )),
        },
        layout,
        output_schema,
    })
}

fn lower_set_op_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    set_op: &plan::SetOpNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_min_arity("SetOpNode", 2, children.len())?;
    let kind = plan::PlanSetOpKind::try_from(set_op.kind)
        .map_err(|_| format!("SetOpNode unknown kind {}", set_op.kind))?;
    let output_columns = if set_op.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &set_op.output_columns
    };
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let inputs = normalize_set_op_inputs(
        node.node_id,
        children,
        &set_op.child_output_columns,
        output_columns,
        output_schema.clone(),
        arena,
    )?;
    match kind {
        plan::PlanSetOpKind::UnionAll => Ok(LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: node.node_id,
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Intersect => Ok(LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Intersect,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Except => Ok(LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Except,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::UnionDistinct => unsupported("UnionDistinct"),
        plan::PlanSetOpKind::Unspecified => Err("SetOpNode kind is unspecified".to_string()),
    }
}

fn normalize_set_op_inputs(
    node_id: i32,
    children: Vec<LoweredNode>,
    child_output_columns: &[plan::OutputColumnList],
    output_columns: &[common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, String> {
    if child_output_columns.is_empty() {
        return Ok(children.into_iter().map(|child| child.node).collect());
    }
    if child_output_columns.len() != children.len() {
        return Err(format!(
            "SetOpNode child_output_columns size mismatch: expected {}, got {}",
            children.len(),
            child_output_columns.len()
        ));
    }
    let output_slots = slot_ids_from_columns(output_columns)?;
    let output_slot_schemas = slot_schemas_from_output_columns(output_columns)?;
    children
        .into_iter()
        .zip(child_output_columns.iter())
        .enumerate()
        .map(|(idx, (child, child_columns))| {
            if child_columns.columns.len() != output_columns.len() {
                return Err(format!(
                    "SetOpNode child {idx} output width mismatch: expected {}, got {}",
                    output_columns.len(),
                    child_columns.columns.len()
                ));
            }
            let expected_child_layout = layout_from_output_columns(&child_columns.columns)?;
            if expected_child_layout.order() != child.layout.order() {
                return Err(format!(
                    "SetOpNode child {idx} output columns do not match child layout: columns={:?} layout={:?}",
                    expected_child_layout.order(),
                    child.layout.order()
                ));
            }
            let exprs = child_columns
                .columns
                .iter()
                .map(|col| {
                    let slot = SlotId::new(col.column_id);
                    let data_type = col
                        .r#type
                        .as_ref()
                        .ok_or_else(|| {
                            format!(
                                "SetOpNode child {idx} column {} type missing",
                                col.column_id
                            )
                        })
                        .and_then(super::decode_type)?;
                    Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(child.node),
                    node_id,
                    is_subordinate: true,
                    exprs,
                    expr_slot_ids: output_slots.clone(),
                    expr_slot_schemas: Some(output_slot_schemas.clone()),
                    output_indices: None,
                    output_chunk_schema: output_schema.clone(),
                }),
            })
        })
        .collect()
}

fn slot_ids_from_columns(cols: &[common::OutputColumn]) -> Result<Vec<SlotId>, String> {
    Ok(layout_from_output_columns(cols)?.order().to_vec())
}

fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("AssertOneRowNode", 1, children.len())?;
    let child = children.pop().expect("child");
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                mode: AssertNumRowsMode::Global {
                    desired_num_rows: Some(1),
                    assertion: Assertion::Le,
                    subquery_string: Some(assert.subquery_text.clone()),
                },
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn parse_optional_nonnegative_i64(
    value: Option<i64>,
    label: &str,
) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            if value < 0 {
                Err(format!("{label} must be >= 0, got {value}"))
            } else {
                Ok(value as usize)
            }
        })
        .transpose()
}

fn parse_distributed_limit(value: i64, label: &str) -> Result<Option<usize>, String> {
    if value == -1 {
        Ok(None)
    } else if value < 0 {
        Err(format!("{label} must be -1 or >= 0, got {value}"))
    } else {
        Ok(Some(value as usize))
    }
}

fn merge_limits(
    node_kind: &str,
    payload_limit: Option<usize>,
    outer_limit: Option<usize>,
) -> Result<Option<usize>, String> {
    match (payload_limit, outer_limit) {
        (Some(left), Some(right)) if left != right => Err(format!(
            "{node_kind} payload limit {left} conflicts with DistributedNode.limit {right}"
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;

    use super::{NodeLoweringContext, lower_proto_node};
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::exec::node::sort::SortTopNType;
    use crate::proto::{common, expr, plan};
    use crate::runtime::exchange::ExchangeKey;
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

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    fn bool_literal(value: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            })),
        }
    }

    fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "name", DataType::Utf8),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10), string_literal("alice")],
                    },
                    plan::ExprList {
                        values: vec![int_literal(20), string_literal("bob")],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> super::LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_values_rows_into_chunk_schema() {
        let lowered = lower(&values_node(10));
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.node_id, 10);
        assert_eq!(values.chunk.len(), 2);
        assert_eq!(
            values.chunk.chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(2)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let id_column = values
            .chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("id column");
        let id = id_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id");
        assert_eq!(id.value(0), 10);
        assert_eq!(id.value(1), 20);

        let name_column = values
            .chunk
            .column_by_slot_id(SlotId::new(2))
            .expect("name column");
        let name = name_column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 name");
        assert_eq!(name.value(0), "alice");
        assert_eq!(name.value(1), "bob");
    }

    #[test]
    fn lowers_project_items_to_output_slots_and_schema() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.node_id, 20);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(7)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(7)]);
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "projected_id"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn lowers_filter_limit_shape() {
        let filter = physical_node(
            20,
            plan::plan_node::Kind::Filter(plan::FilterNode {
                predicate: Some(bool_literal(true)),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let limit = physical_node(
            30,
            plan::plan_node::Kind::Limit(plan::LimitNode {
                limit: Some(5),
                offset: Some(1),
            }),
            Vec::new(),
            vec![filter],
        );

        let lowered = lower(&limit);
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 30);
        assert_eq!(limit.limit, Some(5));
        assert_eq!(limit.offset, 1);
        assert!(matches!(limit.input.kind, ExecNodeKind::Filter(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_sort_and_topn_shapes() {
        let mut sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                offset: Some(2),
                partition_limit: None,
                topn_type: None,
            }),
            vec![output_column(1, "id", DataType::Int64)],
            vec![one_col_values_node(10)],
        );
        sort.limit = 9;
        let lowered_sort = lower(&sort);
        let ExecNodeKind::Sort(sort) = lowered_sort.node.kind else {
            panic!("expected Sort");
        };
        assert!(!sort.use_top_n);
        assert_eq!(sort.limit, Some(9));
        assert_eq!(sort.offset, 2);
        assert_eq!(sort.order_by.len(), 1);

        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhaseFinal as i32,
                is_split: false,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered_topn = lower(&topn);
        let ExecNodeKind::Sort(topn) = lowered_topn.node.kind else {
            panic!("expected TopN as Sort");
        };
        assert!(topn.use_top_n);
        assert_eq!(topn.limit, Some(3));
        assert_eq!(topn.offset, 0);
        assert_eq!(topn.topn_type, SortTopNType::RowNumber);
    }

    #[test]
    fn exchange_receiver_requires_sender_count() {
        let exchange = plan::DistributedNode {
            node_id: 40,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Hash as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                },
            )),
        };

        let mut arena = ExprArena::default();
        let err =
            lower_proto_node(&exchange, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("ExchangeReceiver"));
        assert!(err.contains("sender count"));

        let lowered = lower_proto_node(
            &exchange,
            &mut arena,
            &NodeLoweringContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 40,
                },
                2,
            ),
        )
        .expect("plain exchange");
        let ExecNodeKind::ExchangeSource(exchange) = lowered.node.kind else {
            panic!("expected ExchangeSource");
        };
        assert_eq!(exchange.expected_senders, 2);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn rejects_unsupported_scan_and_union_distinct() {
        let scan = physical_node(
            50,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&scan, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("Scan"));
        assert!(err.contains("not implemented"));

        let union_distinct = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionDistinct as i32,
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                child_output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let err = lower_proto_node(&union_distinct, &mut arena, &NodeLoweringContext::default())
            .unwrap_err();
        assert!(err.contains("UnionDistinct"));
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn lowers_union_all_intersect_except_and_assert_one_row() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns.clone(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let lowered = lower(&union_all);
        assert!(matches!(lowered.node.kind, ExecNodeKind::UnionAll(_)));

        for (kind, expected) in [
            (plan::PlanSetOpKind::Intersect, SetOpKind::Intersect),
            (plan::PlanSetOpKind::Except, SetOpKind::Except),
        ] {
            let set_op = physical_node(
                61,
                plan::plan_node::Kind::SetOp(plan::SetOpNode {
                    kind: kind as i32,
                    output_columns: output_columns.clone(),
                    child_output_columns: Vec::new(),
                }),
                output_columns.clone(),
                vec![one_col_values_node(10), one_col_values_node(11)],
            );
            let lowered = lower(&set_op);
            let ExecNodeKind::SetOp(set_op) = lowered.node.kind else {
                panic!("expected SetOp");
            };
            assert_eq!(
                std::mem::discriminant(&set_op.kind),
                std::mem::discriminant(&expected)
            );
            assert_eq!(set_op.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }

        let assert_one_row = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "select id from t".to_string(),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_one_row);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => {
                assert_eq!(desired_num_rows, Some(1));
                assert!(matches!(assertion, Assertion::Le));
                assert_eq!(subquery_string.as_deref(), Some("select id from t"));
            }
            AssertNumRowsMode::PerKeyAtMostOne { .. } => panic!("expected global assert"),
        }
    }
}
