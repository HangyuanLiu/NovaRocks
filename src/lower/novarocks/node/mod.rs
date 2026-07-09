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

mod aggregate;
mod common;
mod exchange;
mod filter;
mod generate_series;
mod hash_join;
mod limit;
mod project;
mod set_op;
mod sort;
mod table_function;
mod topn;
mod values;
mod window;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use self::common::*;
use arrow::datatypes::{DataType, Field};

use super::expr::lower_proto_expr;
use super::layout::{Layout, chunk_schema_from_output_columns, layout_from_output_columns};
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::ExprArena;
use crate::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use crate::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{novarocks, plan};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::query_options::QueryOptions;
use crate::sql::common::ChangeStreamBranchKind;

#[derive(Clone, Debug)]
pub(crate) struct LoweredNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeLoweringContext {
    exchange_sender_counts: HashMap<ExchangeKey, usize>,
    scan_ranges: HashMap<i32, Vec<novarocks::ScanRangeParams>>,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<crate::connector::ConnectorRegistry>>,
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

    #[allow(dead_code)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks::ScanRangeParams>,
    ) -> Self {
        self.scan_ranges.insert(node_id, ranges);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_connector_registry(
        mut self,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Self {
        self.connectors = Some(connectors);
        self
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[novarocks::ScanRangeParams], String> {
        self.scan_ranges
            .get(&node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| format!("native ScanNode node_id={node_id} missing scan ranges"))
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    pub(crate) fn connectors(&self) -> Result<&crate::connector::ConnectorRegistry, String> {
        self.connectors.as_deref().ok_or_else(|| {
            "native ScanNode requires ConnectorRegistry in NodeLoweringContext".to_string()
        })
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
    let lowered = match payload {
        plan::distributed_node::Payload::Physical(physical) => {
            lower_physical_node(node, physical, children, arena, ctx)
        }
        plan::distributed_node::Payload::Exchange(exchange) => {
            exchange::lower_exchange_receiver(node, exchange, children, arena, ctx)
        }
    }?;
    apply_distributed_limit_if_needed(node, lowered)
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: LoweredNode,
) -> Result<LoweredNode, String> {
    let Some(limit) = parse_distributed_limit(node.limit, "DistributedNode.limit")? else {
        return Ok(lowered);
    };
    if matches!(
        lowered.node.kind,
        ExecNodeKind::Limit(_) | ExecNodeKind::Sort(_)
    ) {
        return Ok(lowered);
    }
    lowered.node = ExecNode {
        kind: ExecNodeKind::Limit(LimitNode {
            input: Box::new(lowered.node),
            node_id: node.node_id,
            limit: Some(limit),
            offset: 0,
        }),
    };
    Ok(lowered)
}

fn lower_physical_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let kind = physical
        .kind
        .as_ref()
        .ok_or_else(|| format!("PlanNode node_id={} kind missing", node.node_id))?;
    match kind {
        plan::plan_node::Kind::Values(values) => {
            values::lower_values_node(node, physical, values, children, arena)
        }
        plan::plan_node::Kind::Project(project) => {
            project::lower_project_node(node, project, children, arena)
        }
        plan::plan_node::Kind::Filter(filter) => {
            filter::lower_filter_node(node, filter, children, arena)
        }
        plan::plan_node::Kind::Limit(limit) => limit::lower_limit_node(node, limit, children),
        plan::plan_node::Kind::Sort(sort) => {
            sort::lower_sort_node(node, physical, sort, children, arena)
        }
        plan::plan_node::Kind::Topn(topn) => topn::lower_topn_node(node, topn, children, arena),
        plan::plan_node::Kind::SetOp(set_op) => {
            set_op::lower_set_op_node(node, physical, set_op, children, arena)
        }
        plan::plan_node::Kind::AssertOneRow(assert) => {
            lower_assert_one_row_node(node, assert, children)
        }
        plan::plan_node::Kind::Scan(scan) => {
            super::scan::lower_scan_node(node, physical, scan, ctx, arena)
        }
        plan::plan_node::Kind::HashAggregate(aggregate) => {
            aggregate::lower_hash_aggregate_node(node, physical, aggregate, children, arena)
        }
        plan::plan_node::Kind::HashJoin(join) => {
            hash_join::lower_hash_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::NestLoopJoin(join) => {
            lower_nest_loop_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::Window(window) => {
            window::lower_window_node(node, physical, window, children, arena)
        }
        plan::plan_node::Kind::Repeat(repeat) => lower_repeat_node(node, repeat, children),
        plan::plan_node::Kind::GenerateSeries(generate_series) => {
            generate_series::lower_generate_series_node(node, generate_series, children, arena)
        }
        plan::plan_node::Kind::TableFunction(table_function) => {
            table_function::lower_table_function_node(node, table_function, children, arena)
        }
        plan::plan_node::Kind::Decode(_) => unsupported("Decode"),
        plan::plan_node::Kind::ChangeEventExpand(expand) => {
            lower_change_event_expand_node(node, physical, expand, children, arena)
        }
        plan::plan_node::Kind::CteAnchor(_) => unsupported("CTEAnchor"),
        plan::plan_node::Kind::CteProduce(_) => unsupported("CTEProduce"),
        plan::plan_node::Kind::CteConsume(_) => unsupported("CTEConsume"),
        plan::plan_node::Kind::Redistribute(redistribute) => {
            lower_redistribute_node(physical, redistribute, children, arena)
        }
    }
}

fn lower_nest_loop_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::NestLoopJoinNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("NestLoopJoinNode", 2, children.len())?;
    let mut it = children.into_iter();
    let mut left = it.next().expect("left");
    let mut right = it.next().expect("right");
    let join_kind = plan::JoinKind::try_from(join.join_type)
        .map_err(|_| format!("NestLoopJoinNode unknown join_type {}", join.join_type))?;
    let join_type = match join_kind {
        plan::JoinKind::RightSemi => {
            std::mem::swap(&mut left, &mut right);
            NestedLoopJoinType::LeftSemi
        }
        plan::JoinKind::RightAnti => {
            std::mem::swap(&mut left, &mut right);
            NestedLoopJoinType::LeftAnti
        }
        _ => proto_nested_loop_join_type(join.join_type, "NestLoopJoinNode")?,
    };
    let join_layout = concat_layouts(&left.layout, &right.layout)?;
    let join_scope_chunk_schema = Arc::new(ChunkSchema::concat(&[
        left.output_schema.clone(),
        right.output_schema.clone(),
    ])?);
    let is_semi_anti = matches!(
        join_type,
        NestedLoopJoinType::LeftSemi
            | NestedLoopJoinType::LeftAnti
            | NestedLoopJoinType::NullAwareLeftAnti
    );
    let output_schema = if is_semi_anti && !physical.output_columns.is_empty() {
        chunk_schema_from_output_columns(&physical.output_columns)
            .map_err(|err| format!("NestLoopJoinNode output_columns: {err}"))?
    } else {
        hash_join::join_output_chunk_schema(
            physical,
            join_scope_chunk_schema.clone(),
            "NestLoopJoinNode",
        )?
    };
    let join_conjunct = join
        .condition
        .as_ref()
        .map(|expr| lower_proto_expr(expr, arena, &join_layout))
        .transpose()
        .map_err(|err| format!("NestLoopJoinNode condition: {err}"))?;
    let output_layout = if is_semi_anti {
        Layout::for_slots(output_schema.slot_ids().iter().copied())
    } else {
        join_layout.clone()
    };
    let execution_scope_chunk_schema = if is_semi_anti {
        join_scope_chunk_schema
    } else {
        output_schema.clone()
    };

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                join_conjunct,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: execution_scope_chunk_schema,
            }),
        },
        layout: output_layout,
        output_schema,
    })
}

fn proto_nested_loop_join_type(value: i32, node_kind: &str) -> Result<NestedLoopJoinType, String> {
    match plan::JoinKind::try_from(value)
        .map_err(|_| format!("{node_kind} unknown join_type {value}"))?
    {
        plan::JoinKind::Inner => Ok(NestedLoopJoinType::Inner),
        plan::JoinKind::Cross => Ok(NestedLoopJoinType::Cross),
        plan::JoinKind::LeftOuter => Ok(NestedLoopJoinType::LeftOuter),
        plan::JoinKind::RightOuter => Ok(NestedLoopJoinType::RightOuter),
        plan::JoinKind::FullOuter => Ok(NestedLoopJoinType::FullOuter),
        plan::JoinKind::LeftSemi => Ok(NestedLoopJoinType::LeftSemi),
        plan::JoinKind::LeftAnti => Ok(NestedLoopJoinType::LeftAnti),
        plan::JoinKind::NullAwareLeftAnti => Ok(NestedLoopJoinType::NullAwareLeftAnti),
        plan::JoinKind::RightSemi | plan::JoinKind::RightAnti => Err(format!(
            "{node_kind} right semi/anti must be rewritten before nested-loop join type lowering"
        )),
        plan::JoinKind::Unspecified => Err(format!("{node_kind} join_type is unspecified")),
    }
}

fn lower_repeat_node(
    node: &plan::DistributedNode,
    repeat: &plan::RepeatNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("RepeatNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let repeat_times = repeat.grouping_ids.len();
    if repeat_times == 0 {
        return Err("RepeatNode grouping_ids is empty".to_string());
    }
    if repeat.repeat_column_ref_ids.len() != repeat_times {
        return Err(format!(
            "RepeatNode repeat_column_ref_ids size mismatch: expected {}, got {}",
            repeat_times,
            repeat.repeat_column_ref_ids.len()
        ));
    }
    let all_slot_ids = repeat
        .all_rollup_column_ids
        .iter()
        .copied()
        .map(SlotId::new)
        .collect::<Vec<_>>();
    let all_slot_set = all_slot_ids.iter().copied().collect::<HashSet<_>>();
    let null_slot_ids = repeat
        .repeat_column_ref_ids
        .iter()
        .enumerate()
        .map(|(idx, keep_ids)| {
            let keep = keep_ids
                .values
                .iter()
                .copied()
                .map(SlotId::new)
                .collect::<HashSet<_>>();
            for slot in &keep {
                if !all_slot_set.contains(slot) {
                    return Err(format!(
                        "RepeatNode keep set {idx} contains unknown rollup slot {}",
                        slot
                    ));
                }
            }
            let mut nulls = all_slot_ids
                .iter()
                .copied()
                .filter(|slot| !keep.contains(slot))
                .collect::<Vec<_>>();
            nulls.sort_by_key(|slot| slot.as_u32());
            Ok(nulls)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let grouping_slot_ids = repeat
        .grouping_fn_ids
        .iter()
        .map(|entry| SlotId::new(entry.value))
        .collect::<Vec<_>>();
    let grouping_list = repeat_grouping_values(repeat)?;
    let (layout, output_schema) =
        repeat_output_layout_and_schema(&child, &repeat.grouping_fn_ids, &grouping_slot_ids)?;

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Repeat(RepeatNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                null_slot_ids,
                grouping_slot_ids,
                grouping_list,
                repeat_times,
            }),
        },
        layout,
        output_schema,
    })
}

fn repeat_output_layout_and_schema(
    child: &LoweredNode,
    grouping_fn_ids: &[plan::NamedUInt32],
    grouping_slot_ids: &[SlotId],
) -> Result<(Layout, ChunkSchemaRef), String> {
    let mut slots = child.output_schema.slots().to_vec();
    let mut output_slot_ids = child.layout.order().to_vec();
    for (idx, slot_id) in grouping_slot_ids.iter().copied().enumerate() {
        if child.layout.contains_slot(slot_id) || output_slot_ids.contains(&slot_id) {
            return Err(format!(
                "RepeatNode grouping slot {} duplicates input slot",
                slot_id
            ));
        }
        let name = grouping_fn_ids
            .get(idx)
            .map(|entry| entry.name.as_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("__grouping_fn");
        let field = Field::new(name, DataType::Int64, true);
        slots.push(ChunkSlotSchema::new_with_field(slot_id, field, None, None));
        output_slot_ids.push(slot_id);
    }
    let layout = Layout::for_slots(output_slot_ids);
    let output_schema = Arc::new(ChunkSchema::try_new(slots)?);
    Ok((layout, output_schema))
}

fn repeat_grouping_values(repeat: &plan::RepeatNode) -> Result<Vec<Vec<i64>>, String> {
    if repeat.grouping_fn_ids.len() != repeat.grouping_fn_arg_ids.len() {
        return Err(format!(
            "RepeatNode grouping fn length mismatch: ids={} arg_ids={}",
            repeat.grouping_fn_ids.len(),
            repeat.grouping_fn_arg_ids.len()
        ));
    }
    let repeat_times = repeat.grouping_ids.len();
    let keep_sets = repeat
        .repeat_column_ref_ids
        .iter()
        .map(|ids| ids.values.iter().copied().collect::<HashSet<_>>())
        .collect::<Vec<_>>();
    repeat
        .grouping_fn_arg_ids
        .iter()
        .enumerate()
        .map(|(idx, args)| {
            if args.values.len() > 63 {
                return Err(format!(
                    "RepeatNode grouping_fn_arg_ids[{idx}] has too many arguments: {}",
                    args.values.len()
                ));
            }
            let mut values = Vec::with_capacity(repeat_times);
            for (repeat_idx, keep) in keep_sets.iter().enumerate() {
                let mut value = 0i64;
                for (arg_idx, column_id) in args.values.iter().enumerate() {
                    if !keep.contains(column_id) {
                        let reverse_bit_pos = args.values.len() - 1 - arg_idx;
                        value |= 1i64 << reverse_bit_pos;
                    }
                }
                if repeat_idx >= repeat_times {
                    return Err("RepeatNode internal repeat index overflow".to_string());
                }
                values.push(value);
            }
            Ok(values)
        })
        .collect()
}

fn lower_change_event_expand_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    expand: &plan::ChangeEventExpandNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ChangeEventExpandNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let output_columns = if expand.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &expand.output_columns
    };
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let output_slot_ids = layout.order().to_vec();
    let output_set = output_slot_ids.iter().copied().collect::<HashSet<_>>();
    let change_op_slot_id = SlotId::new(expand.change_op_column_id);
    if !output_set.contains(&change_op_slot_id) {
        return Err(format!(
            "ChangeEventExpandNode change_op_column_id {} is not in outputs",
            expand.change_op_column_id
        ));
    }
    let change_op_field = output_schema.slot(change_op_slot_id).ok_or_else(|| {
        format!(
            "ChangeEventExpandNode change_op_column_id {} missing from output schema",
            expand.change_op_column_id
        )
    })?;
    if change_op_field.data_type() != &DataType::Int8 {
        return Err(format!(
            "ChangeEventExpandNode change_op_column_id {} must be Int8, got {:?}",
            expand.change_op_column_id,
            change_op_field.data_type()
        ));
    }
    let data_route_slot_id = expand.data_route_column_id.map(SlotId::new);
    if let Some(slot_id) = data_route_slot_id {
        if slot_id == change_op_slot_id {
            return Err(format!(
                "ChangeEventExpandNode data_route_column_id {} must differ from change_op_column_id {}",
                slot_id, change_op_slot_id
            ));
        }
        if !output_set.contains(&slot_id) {
            return Err(format!(
                "ChangeEventExpandNode data_route_column_id {} is not in outputs",
                slot_id
            ));
        }
        let route_field = output_schema.slot(slot_id).ok_or_else(|| {
            format!(
                "ChangeEventExpandNode data_route_column_id {} missing from output schema",
                slot_id
            )
        })?;
        if !is_signed_integer_route_type(route_field.data_type()) {
            return Err(format!(
                "ChangeEventExpandNode data_route_column_id {} must be a signed integer route type, got {:?}",
                slot_id,
                route_field.data_type()
            ));
        }
    }

    let mut events = Vec::with_capacity(expand.events.len());
    for (event_idx, event) in expand.events.iter().enumerate() {
        let branch_kind = change_event_branch_kind(event.branch_kind)?;
        if matches!(
            branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        ) && data_route_slot_id.is_none()
        {
            return Err(format!(
                "ChangeEventExpandNode data branch {:?} requires data_route_column_id",
                branch_kind
            ));
        }
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| lower_proto_expr(expr, arena, &child.layout))
            .transpose()
            .map_err(|err| format!("ChangeEventExpandNode event {event_idx} predicate: {err}"))?;
        let assignments = event
            .assignments
            .iter()
            .enumerate()
            .map(|(assign_idx, assignment)| {
                let slot_id = SlotId::new(assignment.output_column_id);
                if !output_set.contains(&slot_id) {
                    return Err(format!(
                        "ChangeEventExpandNode event {event_idx} assignment {assign_idx} output column {} is not in outputs",
                        assignment.output_column_id
                    ));
                }
                let expr = assignment
                    .expr
                    .as_ref()
                    .map(|expr| lower_proto_expr(expr, arena, &child.layout))
                    .transpose()
                    .map_err(|err| {
                        format!(
                            "ChangeEventExpandNode event {event_idx} assignment {assign_idx}: {err}"
                        )
                    })?;
                Ok(ChangeEventRuntimeOutputExpr {
                    output_slot_id: slot_id,
                    expr,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        events.push(ChangeEventRuntimeSpec {
            predicate,
            branch_kind,
            assignments,
        });
    }

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                output_chunk_schema: output_schema.clone(),
                change_op_slot_id,
                data_route_slot_id,
            }),
        },
        layout,
        output_schema,
    })
}

fn is_signed_integer_route_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn change_event_branch_kind(value: i32) -> Result<ChangeStreamBranchKind, String> {
    match plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown change event branch kind {value}"))?
    {
        plan::ChangeStreamBranchKind::DeleteDv => Ok(ChangeStreamBranchKind::DeleteDv),
        plan::ChangeStreamBranchKind::ReuseData => Ok(ChangeStreamBranchKind::ReuseData),
        plan::ChangeStreamBranchKind::FreshData => Ok(ChangeStreamBranchKind::FreshData),
        plan::ChangeStreamBranchKind::Unspecified => {
            Err("change event branch kind is unspecified".to_string())
        }
    }
}

fn lower_redistribute_node(
    physical: &plan::PlanNode,
    redistribute: &plan::RedistributeNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("RedistributeNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let mode = redistribute
        .mode
        .as_ref()
        .and_then(|mode| mode.mode.as_ref())
        .ok_or_else(|| "RedistributeNode mode missing".to_string())?;
    match mode {
        plan::redistribute_mode::Mode::Gather(true)
        | plan::redistribute_mode::Mode::Broadcast(true) => {}
        plan::redistribute_mode::Mode::Hash(hash) => {
            if hash.cols.is_empty() {
                return Err("RedistributeNode hash mode requires cols".to_string());
            }
            for col in &hash.cols {
                child.layout.resolve_column_id(*col)?;
            }
        }
        plan::redistribute_mode::Mode::Gather(false)
        | plan::redistribute_mode::Mode::Broadcast(false) => {
            return Err("RedistributeNode boolean mode must be true".to_string());
        }
    }
    for (idx, expr) in redistribute.partition_exprs.iter().enumerate() {
        lower_proto_expr(expr, arena, &child.layout)
            .map_err(|err| format!("RedistributeNode partition_exprs[{idx}]: {err}"))?;
    }
    let output_columns = if redistribute.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &redistribute.output_columns
    };
    if output_columns.is_empty() {
        return Ok(child);
    }
    let layout = layout_from_output_columns(output_columns)?;
    if layout.order() != child.layout.order() {
        return Err(format!(
            "RedistributeNode output columns must preserve child order: child={:?} output={:?}",
            child.layout.order(),
            layout.order()
        ));
    }
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    Ok(LoweredNode {
        node: child.node,
        layout,
        output_schema,
    })
}

fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("AssertOneRowNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let desired_num_rows = parse_optional_nonnegative_i64(
        assert.desired_num_rows,
        "AssertOneRowNode.desired_num_rows",
    )?
    .or(Some(1));
    let assertion = lower_row_count_assertion(assert.assertion)?;
    let mode = if assert.group_key_column_ids.is_empty() {
        if !assert.group_key_labels.is_empty() || assert.keyed_message_prefix.is_some() {
            return Err(
                "AssertOneRowNode group_key_column_ids is required when keyed metadata is present"
                    .to_string(),
            );
        }
        AssertNumRowsMode::Global {
            desired_num_rows,
            assertion,
            subquery_string: Some(assert.subquery_text.clone()),
        }
    } else {
        if desired_num_rows != Some(1) || !matches!(assertion, Assertion::Le) {
            return Err(
                "AssertOneRowNode keyed assertions only support desired_num_rows <= 1".to_string(),
            );
        }
        if !assert.group_key_labels.is_empty()
            && assert.group_key_labels.len() != assert.group_key_column_ids.len()
        {
            return Err(format!(
                "AssertOneRowNode group_key_labels length mismatch: key_columns={} labels={}",
                assert.group_key_column_ids.len(),
                assert.group_key_labels.len()
            ));
        }
        let key_slots = assert
            .group_key_column_ids
            .iter()
            .map(|column_id| {
                child
                    .layout
                    .resolve_column_id(*column_id)
                    .map_err(|err| format!("AssertOneRowNode group key: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key_labels = if assert.group_key_labels.is_empty() {
            assert
                .group_key_column_ids
                .iter()
                .map(|column_id| format!("column_{column_id}"))
                .collect()
        } else {
            assert.group_key_labels.clone()
        };
        AssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix: assert
                .keyed_message_prefix
                .clone()
                .unwrap_or_else(|| "assert_num_rows failed".to_string()),
        }
    };
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                mode,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_row_count_assertion(value: i32) -> Result<Assertion, String> {
    match value {
        value if value == plan::RowCountAssertion::Unspecified as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Eq as i32 => Ok(Assertion::Eq),
        value if value == plan::RowCountAssertion::Ne as i32 => Ok(Assertion::Ne),
        value if value == plan::RowCountAssertion::Lt as i32 => Ok(Assertion::Lt),
        value if value == plan::RowCountAssertion::Le as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Gt as i32 => Ok(Assertion::Gt),
        value if value == plan::RowCountAssertion::Ge as i32 => Ok(Assertion::Ge),
        other => Err(format!(
            "AssertOneRowNode assertion {other} is not supported"
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{NodeLoweringContext, lower_proto_node};
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::proto::{common, expr, plan};
    use crate::sql::codegen::proto_encode::types::encode_type;

    pub(super) fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    pub(super) fn output_column_with_nullable(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    pub(super) fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
    ) -> common::OutputColumn {
        output_column_with_nullable(column_id, name, data_type, true)
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

    pub(super) fn string_literal(value: &str) -> expr::Expr {
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

    pub(super) fn bool_literal(value: bool) -> expr::Expr {
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

    fn null_literal(data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    pub(super) fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
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

    pub(super) fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    pub(super) fn physical_node(
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

    pub(super) fn values_node(node_id: i32) -> plan::DistributedNode {
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

    pub(super) fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        one_col_values_node_with(node_id, 1, "id", 10)
    }

    pub(super) fn one_col_values_node_with(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
    ) -> plan::DistributedNode {
        one_col_values_node_with_nullable(node_id, column_id, name, value, true)
    }

    pub(super) fn one_col_values_node_with_nullable(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        nullable: bool,
    ) -> plan::DistributedNode {
        let columns = vec![output_column_with_nullable(
            column_id,
            name,
            DataType::Int64,
            nullable,
        )];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn three_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
            output_column(3, "c", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20), int_literal(30)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn lower(node: &plan::DistributedNode) -> super::LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
    }

    #[test]
    fn rejects_scan_without_context_and_union_distinct() {
        let scan = physical_node(
            50,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&scan, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("Scan"));
        assert!(err.contains("table missing"));

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
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: Vec::new(),
                group_key_labels: Vec::new(),
                keyed_message_prefix: None,
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

    #[test]
    fn lowers_keyed_assert_num_rows_from_native_proto() {
        let assert_node = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "DML change-stream matched row uniqueness".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![1],
                group_key_labels: vec!["_row_id".to_string()],
                keyed_message_prefix: Some("MOR UPDATE matched target row".to_string()),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_node);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
            } => {
                assert_eq!(key_slots, vec![SlotId::new(1)]);
                assert_eq!(key_labels, vec!["_row_id".to_string()]);
                assert_eq!(message_prefix, "MOR UPDATE matched target row");
            }
            AssertNumRowsMode::Global { .. } => panic!("expected keyed assert"),
        }
    }

    #[test]
    fn lowers_hash_aggregate_and_join_shapes() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "cnt", DataType::Int64),
        ];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![output_columns[0].clone()],
                    aggregate_columns: vec![output_columns[1].clone()],
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.node_id, 20);
        assert_eq!(aggregate.group_by.len(), 1);
        assert_eq!(aggregate.functions.len(), 1);
        assert!(aggregate.need_finalize);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: Vec::new(),
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(join.probe_keys.len(), 1);
        assert_eq!(join.build_keys.len(), 1);
        assert_eq!(join.eq_null_safe, vec![false]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(matches!(
            join.join_type,
            crate::exec::node::join::JoinType::Inner
        ));
    }

    #[test]
    fn nested_loop_join_output_schema_uses_plan_output_nullability() {
        let output_columns = vec![
            output_column_with_nullable(1, "lhs", DataType::Int64, false),
            output_column_with_nullable(2, "rhs", DataType::Int64, true),
        ];
        let join = physical_node(
            30,
            plan::plan_node::Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                condition: Some(bool_literal(true)),
            }),
            output_columns,
            vec![
                one_col_values_node_with_nullable(10, 1, "lhs", 10, false),
                one_col_values_node_with_nullable(11, 2, "rhs", 10, false),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(!lowered.output_schema.slots()[0].nullable());
        assert!(lowered.output_schema.slots()[1].nullable());
        let ExecNodeKind::NestedLoopJoin(join) = lowered.node.kind else {
            panic!("expected NestedLoopJoin");
        };
        assert!(!join.join_scope_chunk_schema.slots()[0].nullable());
        assert!(join.join_scope_chunk_schema.slots()[1].nullable());
    }

    #[test]
    fn nested_loop_right_semi_swaps_inputs_for_left_semi_execution() {
        let right_output = vec![output_column(2, "rhs", DataType::Int64)];
        let join = physical_node(
            30,
            plan::plan_node::Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: plan::JoinKind::RightSemi as i32,
                condition: Some(bool_literal(true)),
            }),
            right_output,
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 20),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let ExecNodeKind::NestedLoopJoin(join) = lowered.node.kind else {
            panic!("expected NestedLoopJoin");
        };
        assert!(matches!(
            join.join_type,
            crate::exec::node::nljoin::NestedLoopJoinType::LeftSemi
        ));
        assert_eq!(join.left_chunk_schema.slot_ids(), &[SlotId::new(2)]);
        assert_eq!(join.right_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(2), SlotId::new(1)]
        );
    }

    #[test]
    fn lowers_repeat_change_event_and_redistribute_shapes() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["id".to_string()],
                all_rollup_column_ids: vec![1],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.repeat_times, 2);
        assert_eq!(repeat.null_slot_ids, vec![vec![], vec![SlotId::new(1)]]);
        assert_eq!(repeat.grouping_slot_ids, vec![SlotId::new(9)]);
        assert_eq!(repeat.grouping_list, vec![vec![0, 1]]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(9)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(9)]
        );

        let change_event = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::DeleteDv as i32,
                    assignments: vec![plan::DistributedChangeEventOutputExpr {
                        output_column_id: 2,
                        expr: None,
                    }],
                }],
                output_columns: vec![
                    output_column(1, "id", DataType::Int64),
                    output_column(2, "op", DataType::Int8),
                ],
                change_op_column_id: 2,
                data_route_column_id: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&change_event);
        let ExecNodeKind::ChangeEventExpand(change_event) = lowered.node.kind else {
            panic!("expected ChangeEventExpand");
        };
        assert_eq!(
            change_event.output_slot_ids,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(change_event.change_op_slot_id, SlotId::new(2));
        assert_eq!(change_event.events.len(), 1);

        let redistribute = physical_node(
            40,
            plan::plan_node::Kind::Redistribute(plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(true)),
                }),
                partition_exprs: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&redistribute);
        assert!(matches!(lowered.node.kind, ExecNodeKind::Values(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn repeat_grouping_function_uses_sql_reverse_bit_order() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1, 2] },
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: vec![2] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1, 2, 3],
                all_rollup_columns: vec!["a".to_string(), "b".to_string()],
                all_rollup_column_ids: vec![1, 2],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1, 2] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![two_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.grouping_list, vec![vec![0, 1, 2, 3]]);
    }

    #[test]
    fn change_event_rejects_invalid_data_route_slot() {
        let same_slot = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::ReuseData as i32,
                    assignments: Vec::new(),
                }],
                output_columns: vec![output_column(2, "op", DataType::Int8)],
                change_op_column_id: 2,
                data_route_column_id: Some(2),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err =
            lower_proto_node(&same_slot, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("must differ"));

        let non_integer = physical_node(
            31,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::ReuseData as i32,
                    assignments: Vec::new(),
                }],
                output_columns: vec![
                    output_column(2, "op", DataType::Int8),
                    output_column(3, "route", DataType::Utf8),
                ],
                change_op_column_id: 2,
                data_route_column_id: Some(3),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&non_integer, &mut arena, &NodeLoweringContext::default())
            .unwrap_err();
        assert!(err.contains("signed integer route type"));
    }
}
