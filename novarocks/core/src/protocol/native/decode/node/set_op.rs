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

use super::super::layout::{
    chunk_schema_from_output_columns, layout_from_output_columns, slot_schemas_from_output_columns,
};
use super::common::{check_min_arity, slot_ids_from_columns, unsupported};
use super::{super::decode_type, DecodedNode};
use crate::common::ids::SlotId;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::set_op::{SetOpKind, SetOpNode};
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{common as proto_common, plan};

pub(super) fn lower_set_op_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    set_op: &plan::SetOpNode,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
) -> Result<DecodedNode, String> {
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
        plan::PlanSetOpKind::UnionAll => Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: node.node_id,
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Intersect => Ok(DecodedNode {
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
        plan::PlanSetOpKind::Except => Ok(DecodedNode {
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
    children: Vec<DecodedNode>,
    child_output_columns: &[plan::OutputColumnList],
    output_columns: &[proto_common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, String> {
    if child_output_columns.is_empty() {
        return normalize_set_op_inputs_by_position(
            node_id,
            children,
            output_columns,
            output_schema,
            arena,
        );
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
                        .and_then(decode_type)?;
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

fn normalize_set_op_inputs_by_position(
    node_id: i32,
    children: Vec<DecodedNode>,
    output_columns: &[proto_common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, String> {
    let output_slots = slot_ids_from_columns(output_columns)?;
    let output_slot_schemas = slot_schemas_from_output_columns(output_columns)?;
    children
        .into_iter()
        .enumerate()
        .map(|(idx, child)| {
            if child.layout.order().len() != output_slots.len() {
                return Err(format!(
                    "SetOpNode child {idx} width mismatch without child_output_columns: expected {}, got {}",
                    output_slots.len(),
                    child.layout.order().len()
                ));
            }
            if child.layout.order() == output_slots.as_slice() {
                return Ok(child.node);
            }
            let exprs = child
                .layout
                .order()
                .iter()
                .copied()
                .map(|slot| {
                    let data_type = child
                        .output_schema
                        .slot(slot)
                        .ok_or_else(|| {
                            format!(
                                "SetOpNode child {idx} slot {} missing from child output schema",
                                slot
                            )
                        })?
                        .data_type()
                        .clone();
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{lower, one_col_values_node_with, output_column, physical_node};
    use crate::common::ids::SlotId;
    use crate::exec::node::ExecNodeKind;
    use crate::proto::plan;

    #[test]
    fn union_all_retags_child_slots_when_sidecar_is_missing() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns,
            vec![
                one_col_values_node_with(10, 11, "lhs_id", 10),
                one_col_values_node_with(11, 21, "rhs_id", 20),
            ],
        );
        let lowered = lower(&union_all);
        let ExecNodeKind::UnionAll(union) = lowered.node.kind else {
            panic!("expected UnionAll");
        };
        assert_eq!(union.inputs.len(), 2);
        for input in union.inputs {
            let ExecNodeKind::Project(project) = input.kind else {
                panic!("expected retagging Project");
            };
            assert!(project.is_subordinate);
            assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
            assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(1)]);
    }
}
