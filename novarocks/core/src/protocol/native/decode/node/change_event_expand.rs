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

use super::super::NativeFragmentDecodeError;
use super::super::layout::Layout;
use super::{DecodedNode, NativePlanDecodeContext};
use crate::common::ids::SlotId;
use crate::exec::expr::ExprArena;
use crate::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;
use crate::protocol::common::error::FieldPath;
use crate::sql::common::ChangeStreamBranchKind;

pub(super) fn lower_change_event_expand_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    expand: &plan::ChangeEventExpandNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let (output_columns, output_columns_path) = if expand.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&expand.output_columns, path.clone().field("output_columns"))
    };
    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path)?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let output_slot_ids = layout.order().to_vec();
    let output_set = output_slot_ids.iter().copied().collect::<HashSet<_>>();
    let change_op_slot_id = SlotId::new(expand.change_op_column_id);
    if !output_set.contains(&change_op_slot_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("change_op_column_id"),
            format!(
                "ChangeEventExpandNode change_op_column_id {} is not in outputs",
                expand.change_op_column_id
            ),
        ));
    }
    let change_op_field = output_schema.slot(change_op_slot_id).ok_or_else(|| {
        NativeFragmentDecodeError::inconsistent(
            path.clone().field("change_op_column_id"),
            format!(
                "ChangeEventExpandNode change_op_column_id {} missing from output schema",
                expand.change_op_column_id
            ),
        )
    })?;
    if change_op_field.data_type() != &DataType::Int8 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("change_op_column_id"),
            format!(
                "ChangeEventExpandNode change_op_column_id {} must be Int8, got {:?}",
                expand.change_op_column_id,
                change_op_field.data_type()
            ),
        ));
    }
    let data_route_slot_id = expand.data_route_column_id.map(SlotId::new);
    if let Some(slot_id) = data_route_slot_id {
        if slot_id == change_op_slot_id {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("data_route_column_id"),
                format!(
                    "ChangeEventExpandNode data_route_column_id {} must differ from change_op_column_id {}",
                    slot_id, change_op_slot_id
                ),
            ));
        }
        if !output_set.contains(&slot_id) {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("data_route_column_id"),
                format!(
                    "ChangeEventExpandNode data_route_column_id {} is not in outputs",
                    slot_id
                ),
            ));
        }
        let route_field = output_schema.slot(slot_id).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone().field("data_route_column_id"),
                format!(
                    "ChangeEventExpandNode data_route_column_id {} missing from output schema",
                    slot_id
                ),
            )
        })?;
        if !is_signed_integer_route_type(route_field.data_type()) {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone().field("data_route_column_id"),
                format!(
                    "ChangeEventExpandNode data_route_column_id {} must be a signed integer route type, got {:?}",
                    slot_id,
                    route_field.data_type()
                ),
            ));
        }
    }

    let mut events = Vec::with_capacity(expand.events.len());
    for (event_idx, event) in expand.events.iter().enumerate() {
        let event_path = path.clone().field("events").index(event_idx);
        let branch_kind =
            change_event_branch_kind(event.branch_kind, event_path.clone().field("branch_kind"))?;
        if matches!(
            branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        ) && data_route_slot_id.is_none()
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                event_path.clone().field("branch_kind"),
                format!(
                    "ChangeEventExpandNode data branch {:?} requires data_route_column_id",
                    branch_kind
                ),
            ));
        }
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| {
                ctx.decode_expression(
                    expr,
                    event_path.clone().field("predicate"),
                    arena,
                    &child.layout,
                )
            })
            .transpose()?;
        let assignments = event
            .assignments
            .iter()
            .enumerate()
            .map(|(assign_idx, assignment)| {
                let slot_id = SlotId::new(assignment.output_column_id);
                if !output_set.contains(&slot_id) {
                    return Err(NativeFragmentDecodeError::inconsistent(event_path.clone().field("assignments").index(assign_idx).field("output_column_id"), format!(
                        "ChangeEventExpandNode event {event_idx} assignment {assign_idx} output column {} is not in outputs",
                        assignment.output_column_id
                    )));
                }
                let expr = assignment
                    .expr
                    .as_ref()
                    .map(|expr| ctx.decode_expression(expr, event_path.clone().field("assignments").index(assign_idx).field("expr"), arena, &child.layout))
                    .transpose()?;
                Ok(ChangeEventRuntimeOutputExpr {
                    output_slot_id: slot_id,
                    expr,
                })
            })
            .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        events.push(ChangeEventRuntimeSpec {
            predicate,
            branch_kind,
            assignments,
        });
    }

    Ok(DecodedNode {
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

fn change_event_branch_kind(
    value: i32,
    path: FieldPath,
) -> Result<ChangeStreamBranchKind, NativeFragmentDecodeError> {
    match plan::ChangeStreamBranchKind::try_from(value).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone(),
            format!("unknown change event branch kind {value}"),
        )
    })? {
        plan::ChangeStreamBranchKind::DeleteDv => Ok(ChangeStreamBranchKind::DeleteDv),
        plan::ChangeStreamBranchKind::ReuseData => Ok(ChangeStreamBranchKind::ReuseData),
        plan::ChangeStreamBranchKind::FreshData => Ok(ChangeStreamBranchKind::FreshData),
        plan::ChangeStreamBranchKind::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
            path,
            "change event branch kind is unspecified",
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{one_col_values_node, output_column, physical_node};
    use super::super::{NativePlanDecodeContext, decode_node};
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
            decode_node(&same_slot, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
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
        let err = decode_node(
            &non_integer,
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("signed integer route type"));
    }
}
