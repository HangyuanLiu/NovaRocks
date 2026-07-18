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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::compat::expr::lower_t_expr;
use crate::lower::compat::layout::{Layout, chunk_schema_for_layout};
use crate::lower::compat::node::Lowered;
use crate::sql::common::ChangeStreamBranchKind;
use crate::thrift::{descriptors, plan_nodes, types};

pub(crate) fn lower_change_event_expand_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: &descriptors::TDescriptorTable,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    let child = children.into_iter().next().expect("child");
    let payload = node.change_event_expand_node.as_ref().ok_or_else(|| {
        format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} missing change_event_expand_node payload",
            node.node_id
        )
    })?;
    if payload.output_slot_ids.is_empty() {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} output_slot_ids is empty",
            node.node_id
        ));
    }

    let output_set: HashSet<types::TSlotId> = payload.output_slot_ids.iter().copied().collect();
    if output_set.len() != payload.output_slot_ids.len() {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} output_slot_ids contains duplicates",
            node.node_id
        ));
    }
    require_route_slot_in_outputs(
        "change_op_slot_id",
        payload.change_op_slot_id,
        &output_set,
        node.node_id,
    )?;
    if let Some(data_route_slot_id) = payload.data_route_slot_id {
        if data_route_slot_id == payload.change_op_slot_id {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} change_op_slot_id {} and data_route_slot_id {} must be distinct",
                node.node_id, payload.change_op_slot_id, data_route_slot_id
            ));
        }
        require_route_slot_in_outputs(
            "data_route_slot_id",
            data_route_slot_id,
            &output_set,
            node.node_id,
        )?;
    }

    let mut events = Vec::with_capacity(payload.events.len());
    for (event_idx, event) in payload.events.iter().enumerate() {
        let branch_kind = change_event_branch_kind_from_thrift(event.branch_kind)?;
        if matches!(
            branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        ) && payload.data_route_slot_id.is_none()
        {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} data branch {:?} requires data_route_slot_id",
                node.node_id, branch_kind
            ));
        }
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| lower_t_expr(expr, arena, &child.layout, last_query_id, fe_addr))
            .transpose()
            .map_err(|err| {
                format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} failed to lower predicate for event {}: {}",
                    node.node_id, event_idx, err
                )
            })?;
        let mut assignments = Vec::with_capacity(event.assignments.len());
        for assignment in &event.assignments {
            if !output_set.contains(&assignment.output_slot_id) {
                return Err(format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} assignment output slot {} is not in output_slot_ids",
                    node.node_id, assignment.output_slot_id
                ));
            }
            let expr = event_assignment_expr(
                assignment,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                node.node_id,
            )?;
            assignments.push(ChangeEventRuntimeOutputExpr {
                output_slot_id: SlotId::try_from(assignment.output_slot_id)?,
                expr,
            });
        }
        events.push(ChangeEventRuntimeSpec {
            predicate,
            branch_kind,
            assignments,
        });
    }

    let output_slot_ids = payload
        .output_slot_ids
        .iter()
        .copied()
        .map(SlotId::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let layout = output_layout_for_slots(&out_layout, &payload.output_slot_ids)?;
    let output_chunk_schema = chunk_schema_for_layout(desc_tbl, &layout)?;
    let change_op_slot_id = SlotId::try_from(payload.change_op_slot_id)?;
    let change_op_slot = output_chunk_schema.slot(change_op_slot_id).ok_or_else(|| {
        format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} change_op_slot_id {} is missing from output schema",
            node.node_id, payload.change_op_slot_id
        )
    })?;
    if change_op_slot.data_type() != &DataType::Int8 {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} change_op_slot_id {} must be TINYINT/Int8, got {:?}",
            node.node_id,
            payload.change_op_slot_id,
            change_op_slot.data_type()
        ));
    }
    let data_route_slot_id = payload
        .data_route_slot_id
        .map(SlotId::try_from)
        .transpose()?;
    if let Some(data_route_slot_id) = data_route_slot_id {
        let data_route_slot = output_chunk_schema.slot(data_route_slot_id).ok_or_else(|| {
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} data_route_slot_id {} is missing from output schema",
                node.node_id, data_route_slot_id
            )
        })?;
        if !is_signed_integer_route_type(data_route_slot.data_type()) {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} data_route_slot_id {} must be a signed integer route type, got {:?}",
                node.node_id,
                data_route_slot_id,
                data_route_slot.data_type()
            ));
        }
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                output_chunk_schema,
                change_op_slot_id,
                data_route_slot_id,
            }),
        },
        layout,
    })
}

fn is_signed_integer_route_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn require_route_slot_in_outputs(
    name: &str,
    slot_id: types::TSlotId,
    output_set: &HashSet<types::TSlotId>,
    node_id: i32,
) -> Result<(), String> {
    if !output_set.contains(&slot_id) {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} {} {} is not in output_slot_ids",
            node_id, name, slot_id
        ));
    }
    Ok(())
}

fn event_assignment_expr(
    assignment: &plan_nodes::TChangeEventOutputExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    node_id: i32,
) -> Result<Option<ExprId>, String> {
    assignment
        .expr
        .as_ref()
        .map(|expr| lower_t_expr(expr, arena, input_layout, last_query_id, fe_addr))
        .transpose()
        .map_err(|err| {
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} failed to lower assignment for output slot {}: {}",
                node_id, assignment.output_slot_id, err
            )
        })
}

fn change_event_branch_kind_from_thrift(
    kind: plan_nodes::TChangeEventBranchKind,
) -> Result<ChangeStreamBranchKind, String> {
    match kind {
        plan_nodes::TChangeEventBranchKind::DELETE_DV => Ok(ChangeStreamBranchKind::DeleteDv),
        plan_nodes::TChangeEventBranchKind::REUSE_DATA => Ok(ChangeStreamBranchKind::ReuseData),
        plan_nodes::TChangeEventBranchKind::FRESH_DATA => Ok(ChangeStreamBranchKind::FreshData),
        other => Err(format!("unknown change event branch kind: {other:?}")),
    }
}

fn output_layout_for_slots(
    out_layout: &Layout,
    output_slot_ids: &[types::TSlotId],
) -> Result<Layout, String> {
    let requested: HashSet<types::TSlotId> = output_slot_ids.iter().copied().collect();
    let mut tuple_by_slot = HashMap::with_capacity(output_slot_ids.len());
    for (tuple_id, slot_id) in &out_layout.order {
        if !requested.contains(slot_id) {
            continue;
        }
        if let Some(previous_tuple_id) = tuple_by_slot.insert(*slot_id, *tuple_id)
            && previous_tuple_id != *tuple_id
        {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE output slot {} appears in multiple output layout tuples: {} and {}",
                slot_id, previous_tuple_id, tuple_id
            ));
        }
    }

    let mut order = Vec::with_capacity(output_slot_ids.len());
    for slot_id in output_slot_ids {
        let tuple_id = tuple_by_slot.get(slot_id).copied().ok_or_else(|| {
            format!(
                "CHANGE_EVENT_EXPAND_NODE output slot {} is missing from output layout",
                slot_id
            )
        })?;
        order.push((tuple_id, *slot_id));
    }
    let index = order
        .iter()
        .enumerate()
        .map(|(idx, key)| (*key, idx))
        .collect();
    Ok(Layout { order, index })
}
