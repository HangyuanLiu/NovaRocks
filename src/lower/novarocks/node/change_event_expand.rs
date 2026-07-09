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

use std::collections::HashSet;

use arrow::datatypes::DataType;

use super::super::expr::lower_proto_expr;
use super::super::layout::{chunk_schema_from_output_columns, layout_from_output_columns};
use super::LoweredNode;
use super::common::check_exact_arity;
use crate::common::ids::SlotId;
use crate::exec::expr::ExprArena;
use crate::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;
use crate::sql::common::ChangeStreamBranchKind;

pub(crate) fn lower_change_event_expand_node(
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{one_col_values_node, output_column, physical_node};
    use super::super::{NodeLoweringContext, lower_proto_node};
    use crate::exec::expr::ExprArena;
    use crate::proto::plan;

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
