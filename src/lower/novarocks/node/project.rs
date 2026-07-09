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

use super::super::expr::lower_proto_expr;
use super::super::layout::{
    Layout, chunk_schema_from_output_columns, layout_from_output_columns,
    slot_schemas_from_output_columns,
};
use super::LoweredNode;
use super::common::check_exact_arity;
use crate::common::ids::SlotId;
use crate::exec::expr::ExprArena;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{common as proto_common, expr, plan};

pub(crate) fn lower_project_node(
    node: &plan::DistributedNode,
    project: &plan::ProjectNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ProjectNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let project_outputs = project_output_plan(project, &child.layout)?;
    let layout = layout_from_output_columns(&project_outputs.output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&project_outputs.output_columns)?;
    let expr_slot_schemas = slot_schemas_from_output_columns(&project_outputs.computed_columns)?;

    let exprs = project_outputs
        .computed_item_indices
        .iter()
        .map(|idx| {
            let item = project
                .items
                .get(*idx)
                .ok_or_else(|| format!("ProjectNode item {idx} missing"))?;
            let expr = item
                .expr
                .as_ref()
                .ok_or_else(|| format!("ProjectNode item {} expr missing", idx))?;
            lower_proto_expr(expr, arena, &child.layout)
                .map_err(|err| format!("ProjectNode item {}: {err}", idx))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expr_slot_ids = project_outputs
        .computed_columns
        .iter()
        .map(|column| SlotId::new(column.column_id))
        .collect();

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: project_outputs.output_indices,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

struct ProjectOutputPlan {
    computed_item_indices: Vec<usize>,
    computed_columns: Vec<proto_common::OutputColumn>,
    output_columns: Vec<proto_common::OutputColumn>,
    output_indices: Option<Vec<usize>>,
}

fn project_output_plan(
    project: &plan::ProjectNode,
    input_layout: &Layout,
) -> Result<ProjectOutputPlan, String> {
    let item_outputs = project
        .items
        .iter()
        .enumerate()
        .map(project_item_output)
        .collect::<Result<Vec<_>, _>>()?;
    let input_column_ids = input_layout
        .order()
        .iter()
        .map(|slot| slot.as_u32())
        .collect::<HashSet<_>>();
    let output_column_id_candidates = item_outputs
        .iter()
        .map(|item| item.output_column_id)
        .collect::<HashSet<_>>();
    let mut used_output_column_ids = HashSet::new();
    let mut used_compute_column_ids = input_column_ids.clone();
    let mut next_synthetic_column_id = output_column_id_candidates
        .iter()
        .chain(used_compute_column_ids.iter())
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let mut first_expr_index_by_column_id = HashMap::new();
    let mut computed_item_indices = Vec::new();
    let mut computed_columns = Vec::new();
    let mut output_columns = Vec::with_capacity(project.items.len());
    let mut output_indices = Vec::with_capacity(project.items.len());
    let mut needs_output_indices = false;

    for item in item_outputs {
        let preferred_compute_column_id = item.preferred_compute_column_id;
        let mut compute_column_id = if item.can_reuse_input_slot
            || !input_column_ids.contains(&preferred_compute_column_id)
        {
            preferred_compute_column_id
        } else {
            allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?
        };
        if !item.can_reuse_input_slot && used_compute_column_ids.contains(&compute_column_id) {
            compute_column_id = allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?;
        }

        let (computed_idx, is_duplicate_compute) = if item.can_reuse_input_slot
            && let Some(computed_idx) = first_expr_index_by_column_id.get(&compute_column_id)
        {
            (*computed_idx, true)
        } else {
            let computed_idx = computed_columns.len();
            first_expr_index_by_column_id.insert(compute_column_id, computed_idx);
            used_compute_column_ids.insert(compute_column_id);
            computed_item_indices.push(item.item_index);
            computed_columns.push(proto_common::OutputColumn {
                column_id: compute_column_id,
                name: item.output_name.clone(),
                r#type: Some(item.r#type.clone()),
                nullable: item.nullable,
                is_internal: false,
            });
            (computed_idx, false)
        };

        let output_column_id = if used_output_column_ids.insert(item.output_column_id) {
            item.output_column_id
        } else {
            allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?
        };
        output_columns.push(proto_common::OutputColumn {
            column_id: output_column_id,
            name: item.output_name.clone(),
            r#type: Some(item.r#type),
            nullable: item.nullable,
            is_internal: false,
        });
        if is_duplicate_compute
            || computed_idx != output_indices.len()
            || compute_column_id != output_column_id
        {
            needs_output_indices = true;
        }
        output_indices.push(computed_idx);
    }

    Ok(ProjectOutputPlan {
        computed_item_indices,
        computed_columns,
        output_columns,
        output_indices: needs_output_indices.then_some(output_indices),
    })
}

fn allocate_project_synthetic_column_id(
    next_synthetic_column_id: &mut u32,
    used_output_column_ids: &mut HashSet<u32>,
    used_compute_column_ids: &mut HashSet<u32>,
) -> Result<u32, String> {
    while used_output_column_ids.contains(next_synthetic_column_id)
        || used_compute_column_ids.contains(next_synthetic_column_id)
    {
        *next_synthetic_column_id = next_synthetic_column_id
            .checked_add(1)
            .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    }
    let synthetic = *next_synthetic_column_id;
    used_output_column_ids.insert(synthetic);
    used_compute_column_ids.insert(synthetic);
    *next_synthetic_column_id = next_synthetic_column_id
        .checked_add(1)
        .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    Ok(synthetic)
}

struct ProjectItemOutput {
    item_index: usize,
    preferred_compute_column_id: u32,
    output_column_id: u32,
    can_reuse_input_slot: bool,
    output_name: String,
    r#type: proto_common::TypeDesc,
    nullable: bool,
}

fn project_item_output(
    (idx, item): (usize, &plan::ProjectItem),
) -> Result<ProjectItemOutput, String> {
    let expr = item
        .expr
        .as_ref()
        .ok_or_else(|| format!("ProjectNode item {idx} expr missing"))?;
    let r#type = expr
        .r#type
        .clone()
        .ok_or_else(|| format!("ProjectNode item {idx} expr type missing"))?;
    let (preferred_compute_column_id, can_reuse_input_slot) = match expr.kind.as_ref() {
        Some(expr::expr::Kind::ColumnRef(column)) => (column.column_id, true),
        _ => (item.output_column_id, false),
    };
    Ok(ProjectItemOutput {
        item_index: idx,
        preferred_compute_column_id,
        output_column_id: item.output_column_id,
        can_reuse_input_slot,
        output_name: item.output_name.clone(),
        r#type,
        nullable: expr.nullable,
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::{NodeLoweringContext, lower_proto_node};
    use super::*;
    use crate::exec::expr::ExprArena;
    use crate::proto::{common, expr, plan};
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

    fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
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

    fn lower(node: &plan::DistributedNode) -> LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
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
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(7)]);
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "projected_id"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn wraps_project_distributed_limit_as_limit_node() {
        let mut project = physical_node(
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
        project.limit = 1;

        let lowered = lower(&project);
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 20);
        assert_eq!(limit.limit, Some(1));
        assert_eq!(limit.offset, 0);
        assert!(matches!(limit.input.kind, ExecNodeKind::Project(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(7)]);
    }

    #[test]
    fn parent_project_can_reference_child_project_output_column_id() {
        let inner = physical_node(
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
        let outer = physical_node(
            21,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(7, DataType::Int64)),
                    output_name: "outer_id".to_string(),
                    output_column_id: 9,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![inner],
        );

        let lowered = lower(&outer);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(7)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(9)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(9)]);
    }

    #[test]
    fn lowers_project_reused_input_slots_with_output_indices_when_output_ids_change() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "left_out".to_string(),
                        output_column_id: 7,
                    },
                    plan::ProjectItem {
                        expr: Some(column_ref(2, DataType::Int64)),
                        output_name: "right_out".to_string(),
                        output_column_id: 8,
                    },
                ],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![two_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1), SlotId::new(2)]);
        assert_eq!(project.output_indices, Some(vec![0, 1]));
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(7), SlotId::new(8)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7), SlotId::new(8)]);
    }

    #[test]
    fn lowers_project_duplicate_output_ids_with_output_indices() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "left_copy".to_string(),
                        output_column_id: 7,
                    },
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "right_copy".to_string(),
                        output_column_id: 7,
                    },
                ],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.exprs.len(), 1);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
        assert_eq!(project.output_indices, Some(vec![0, 0]));
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(7), SlotId::new(8)]
        );
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "left_copy"
        );
        assert_eq!(
            project.output_chunk_schema.field(1).unwrap().name(),
            "right_copy"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7), SlotId::new(8)]);
    }
}
