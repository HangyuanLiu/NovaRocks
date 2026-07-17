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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use super::super::expr::lower_proto_expr;
use super::super::layout::{Layout, chunk_schema_from_output_columns};
use super::LoweredNode;
use super::common::{check_exact_arity, concat_layouts, proto_join_type};
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::join::{
    JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;
use crate::types::wider_type;

pub(super) fn lower_hash_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::HashJoinNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("HashJoinNode", 2, children.len())?;
    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    if join.eq_conditions.is_empty() {
        return Err("HashJoinNode requires non-empty eq_conditions".to_string());
    }
    let join_type = proto_join_type(join.join_type, "HashJoinNode")?;
    let distribution_mode = hash_join_distribution_mode(join)?;
    let join_layout = concat_layouts(&left.layout, &right.layout)?;
    let join_scope_chunk_schema = Arc::new(ChunkSchema::concat(&[
        left.output_schema.clone(),
        right.output_schema.clone(),
    ])?);
    let output_schema =
        join_output_chunk_schema(physical, join_scope_chunk_schema.clone(), "HashJoinNode")?;

    let mut probe_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut build_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut eq_null_safe = Vec::with_capacity(join.eq_conditions.len());
    let right_semi_physical_right_probe = join_type == JoinType::RightSemi;
    for (idx, cond) in join.eq_conditions.iter().enumerate() {
        let left_expr = cond
            .left
            .as_ref()
            .ok_or_else(|| format!("HashJoinNode eq_conditions[{idx}] left missing"))?;
        let right_expr = cond
            .right
            .as_ref()
            .ok_or_else(|| format!("HashJoinNode eq_conditions[{idx}] right missing"))?;
        let probe_key = lower_proto_expr(left_expr, arena, &left.layout)
            .map_err(|err| format!("HashJoinNode eq_conditions[{idx}] left: {err}"))?;
        let build_key = lower_proto_expr(right_expr, arena, &right.layout)
            .map_err(|err| format!("HashJoinNode eq_conditions[{idx}] right: {err}"))?;
        if right_semi_physical_right_probe {
            probe_keys.push(build_key);
            build_keys.push(probe_key);
        } else {
            probe_keys.push(probe_key);
            build_keys.push(build_key);
        }
        eq_null_safe.push(cond.null_safe);
    }
    let raw_probe_keys = probe_keys.clone();
    let raw_build_keys = build_keys.clone();
    coerce_join_key_types(&mut probe_keys, &mut build_keys, arena)?;
    for key in probe_keys.iter().chain(build_keys.iter()) {
        if let Some(dt) = arena.data_type(*key)
            && matches!(dt, DataType::LargeBinary)
        {
            return Err("VARIANT is not supported in HASH_JOIN keys".to_string());
        }
    }

    let residual_predicate = join
        .other_condition
        .as_ref()
        .map(|expr| lower_proto_expr(expr, arena, &join_layout))
        .transpose()
        .map_err(|err| format!("HashJoinNode other_condition: {err}"))?;
    validate_join_runtime_filter_intents(
        join,
        RuntimeFilterLoweringInput {
            join_type,
            probe_layout: if right_semi_physical_right_probe {
                &right.layout
            } else {
                &left.layout
            },
            build_layout: if right_semi_physical_right_probe {
                &left.layout
            } else {
                &right.layout
            },
            raw_probe_keys: &raw_probe_keys,
            raw_build_keys: &raw_build_keys,
            probe_keys: &probe_keys,
            build_keys: &build_keys,
            arena,
        },
    )?;

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                distribution_mode,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: output_schema.clone(),
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate,
                runtime_filter_execution: JoinRuntimeFilterExecution::Native {
                    producers: Vec::new(),
                },
            }),
        },
        layout: join_layout,
        output_schema,
    })
}

pub(super) fn join_output_chunk_schema(
    physical: &plan::PlanNode,
    fallback: ChunkSchemaRef,
    node_kind: &str,
) -> Result<ChunkSchemaRef, String> {
    if physical.output_columns.is_empty() {
        return Ok(fallback);
    }
    let output_schema = chunk_schema_from_output_columns(&physical.output_columns)
        .map_err(|err| format!("{node_kind} output_columns: {err}"))?;
    if output_schema.slot_ids() == fallback.slot_ids() {
        return Ok(output_schema);
    }
    Ok(fallback)
}

fn hash_join_distribution_mode(join: &plan::HashJoinNode) -> Result<JoinDistributionMode, String> {
    if let Some(mode) = join.execution_mode {
        return match plan::JoinExecutionMode::try_from(mode)
            .map_err(|_| format!("HashJoinNode unknown execution_mode {mode}"))?
        {
            plan::JoinExecutionMode::Broadcast => Ok(JoinDistributionMode::Broadcast),
            plan::JoinExecutionMode::Partitioned | plan::JoinExecutionMode::Colocate => {
                Ok(JoinDistributionMode::Partitioned)
            }
            plan::JoinExecutionMode::Unspecified => {
                Err("HashJoinNode execution_mode is unspecified".to_string())
            }
        };
    }

    match plan::JoinDistribution::try_from(join.distribution)
        .map_err(|_| format!("HashJoinNode unknown distribution {}", join.distribution))?
    {
        plan::JoinDistribution::Broadcast | plan::JoinDistribution::Unknown => {
            Ok(JoinDistributionMode::Broadcast)
        }
        plan::JoinDistribution::Shuffle | plan::JoinDistribution::Colocate => {
            Ok(JoinDistributionMode::Partitioned)
        }
        plan::JoinDistribution::Unspecified => {
            Err("HashJoinNode distribution is unspecified".to_string())
        }
    }
}

struct RuntimeFilterLoweringInput<'a> {
    join_type: JoinType,
    probe_layout: &'a Layout,
    build_layout: &'a Layout,
    raw_probe_keys: &'a [ExprId],
    raw_build_keys: &'a [ExprId],
    probe_keys: &'a [ExprId],
    build_keys: &'a [ExprId],
    arena: &'a mut ExprArena,
}

fn validate_join_runtime_filter_intents(
    join: &plan::HashJoinNode,
    input: RuntimeFilterLoweringInput<'_>,
) -> Result<(), String> {
    if !is_runtime_filter_safe_join_type(input.join_type) {
        return Ok(());
    }
    for rf in &join.build_runtime_filters {
        let expr_order = rf.expr_order as usize;
        if expr_order >= input.probe_keys.len() || expr_order >= input.build_keys.len() {
            return Err(format!(
                "HashJoinNode runtime filter {} expr_order {} out of range",
                rf.filter_id, expr_order
            ));
        }
        validate_runtime_filter_intent(
            rf,
            expr_order,
            input.probe_layout,
            input.build_layout,
            input.raw_probe_keys[expr_order],
            input.raw_build_keys[expr_order],
            input.arena,
        )?;
    }
    Ok(())
}

fn is_runtime_filter_safe_join_type(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    )
}

fn validate_runtime_filter_intent(
    rf: &plan::RuntimeFilterBuildIntent,
    expr_order: usize,
    probe_layout: &Layout,
    build_layout: &Layout,
    expected_probe_key: ExprId,
    expected_build_key: ExprId,
    arena: &mut ExprArena,
) -> Result<(), String> {
    let probe_expr = rf.probe_expr.as_ref().ok_or_else(|| {
        format!(
            "HashJoinNode runtime filter {} probe_expr missing",
            rf.filter_id
        )
    })?;
    let probe_expr_id = lower_proto_expr(probe_expr, arena, probe_layout).map_err(|err| {
        format!(
            "HashJoinNode runtime filter {} probe_expr: {err}",
            rf.filter_id
        )
    })?;
    if !exprs_equivalent(arena, probe_expr_id, expected_probe_key) {
        return Err(format!(
            "HashJoinNode runtime filter {} probe_expr does not match join key at expr_order {}",
            rf.filter_id, expr_order
        ));
    }

    let build_expr = rf.build_expr.as_ref().ok_or_else(|| {
        format!(
            "HashJoinNode runtime filter {} build_expr missing",
            rf.filter_id
        )
    })?;
    let build_expr_id = lower_proto_expr(build_expr, arena, build_layout).map_err(|err| {
        format!(
            "HashJoinNode runtime filter {} build_expr: {err}",
            rf.filter_id
        )
    })?;
    if !exprs_equivalent(arena, build_expr_id, expected_build_key) {
        return Err(format!(
            "HashJoinNode runtime filter {} build_expr does not match join key at expr_order {}",
            rf.filter_id, expr_order
        ));
    }

    Ok(())
}

pub(super) fn exprs_equivalent(arena: &ExprArena, left: ExprId, right: ExprId) -> bool {
    if arena.data_type(left) != arena.data_type(right) {
        return false;
    }
    let Some(left_node) = arena.node(left) else {
        return false;
    };
    let Some(right_node) = arena.node(right) else {
        return false;
    };
    match (left_node, right_node) {
        (ExprNode::Literal(left), ExprNode::Literal(right)) => {
            format!("{left:?}") == format!("{right:?}")
        }
        (ExprNode::SlotId(left), ExprNode::SlotId(right)) => left == right,
        (ExprNode::ArrayExpr { elements: left }, ExprNode::ArrayExpr { elements: right })
        | (ExprNode::StructExpr { fields: left }, ExprNode::StructExpr { fields: right }) => {
            expr_id_slices_equivalent(arena, left, right)
        }
        (
            ExprNode::LambdaFunction {
                body: left_body,
                arg_slots: left_args,
                common_sub_exprs: left_common,
                is_nondeterministic: left_nondeterministic,
            },
            ExprNode::LambdaFunction {
                body: right_body,
                arg_slots: right_args,
                common_sub_exprs: right_common,
                is_nondeterministic: right_nondeterministic,
            },
        ) => {
            left_args == right_args
                && left_nondeterministic == right_nondeterministic
                && exprs_equivalent(arena, *left_body, *right_body)
                && common_sub_exprs_equivalent(arena, left_common, right_common)
        }
        (
            ExprNode::DictDecode {
                child: left,
                dict: left_dict,
            },
            ExprNode::DictDecode {
                child: right,
                dict: right_dict,
            },
        ) => Arc::ptr_eq(left_dict, right_dict) && exprs_equivalent(arena, *left, *right),
        (ExprNode::Cast(left), ExprNode::Cast(right))
        | (ExprNode::CastTime(left), ExprNode::CastTime(right))
        | (ExprNode::CastTimeFromDatetime(left), ExprNode::CastTimeFromDatetime(right))
        | (ExprNode::Not(left), ExprNode::Not(right))
        | (ExprNode::IsNull(left), ExprNode::IsNull(right))
        | (ExprNode::IsNotNull(left), ExprNode::IsNotNull(right))
        | (ExprNode::Clone(left), ExprNode::Clone(right)) => exprs_equivalent(arena, *left, *right),
        (ExprNode::Add(ll, lr), ExprNode::Add(rl, rr))
        | (ExprNode::Sub(ll, lr), ExprNode::Sub(rl, rr))
        | (ExprNode::Mul(ll, lr), ExprNode::Mul(rl, rr))
        | (ExprNode::Div(ll, lr), ExprNode::Div(rl, rr))
        | (ExprNode::Mod(ll, lr), ExprNode::Mod(rl, rr))
        | (ExprNode::Eq(ll, lr), ExprNode::Eq(rl, rr))
        | (ExprNode::EqForNull(ll, lr), ExprNode::EqForNull(rl, rr))
        | (ExprNode::Ne(ll, lr), ExprNode::Ne(rl, rr))
        | (ExprNode::Lt(ll, lr), ExprNode::Lt(rl, rr))
        | (ExprNode::Le(ll, lr), ExprNode::Le(rl, rr))
        | (ExprNode::Gt(ll, lr), ExprNode::Gt(rl, rr))
        | (ExprNode::Ge(ll, lr), ExprNode::Ge(rl, rr))
        | (ExprNode::And(ll, lr), ExprNode::And(rl, rr))
        | (ExprNode::Or(ll, lr), ExprNode::Or(rl, rr)) => {
            exprs_equivalent(arena, *ll, *rl) && exprs_equivalent(arena, *lr, *rr)
        }
        (
            ExprNode::In {
                child: left_child,
                values: left_values,
                is_not_in: left_not,
            },
            ExprNode::In {
                child: right_child,
                values: right_values,
                is_not_in: right_not,
            },
        ) => {
            left_not == right_not
                && exprs_equivalent(arena, *left_child, *right_child)
                && expr_id_slices_equivalent(arena, left_values, right_values)
        }
        (
            ExprNode::Case {
                has_case_expr: left_has_case,
                has_else_expr: left_has_else,
                children: left_children,
            },
            ExprNode::Case {
                has_case_expr: right_has_case,
                has_else_expr: right_has_else,
                children: right_children,
            },
        ) => {
            left_has_case == right_has_case
                && left_has_else == right_has_else
                && expr_id_slices_equivalent(arena, left_children, right_children)
        }
        (
            ExprNode::FunctionCall {
                kind: left_kind,
                args: left_args,
            },
            ExprNode::FunctionCall {
                kind: right_kind,
                args: right_args,
            },
        ) => left_kind == right_kind && expr_id_slices_equivalent(arena, left_args, right_args),
        _ => false,
    }
}

fn expr_id_slices_equivalent(arena: &ExprArena, left: &[ExprId], right: &[ExprId]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| exprs_equivalent(arena, *left, *right))
}

fn common_sub_exprs_equivalent(
    arena: &ExprArena,
    left: &[(SlotId, ExprId)],
    right: &[(SlotId, ExprId)],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_slot, left_expr), (right_slot, right_expr))| {
                left_slot == right_slot && exprs_equivalent(arena, *left_expr, *right_expr)
            })
}

fn coerce_join_key_types(
    probe_keys: &mut [ExprId],
    build_keys: &mut [ExprId],
    arena: &mut ExprArena,
) -> Result<(), String> {
    for idx in 0..probe_keys.len() {
        let probe_expr = probe_keys[idx];
        let build_expr = build_keys[idx];
        let probe_type = arena
            .data_type(probe_expr)
            .ok_or_else(|| "HASH_JOIN probe key type missing".to_string())?
            .clone();
        let build_type = arena
            .data_type(build_expr)
            .ok_or_else(|| "HASH_JOIN build key type missing".to_string())?
            .clone();
        if probe_type == build_type {
            continue;
        }
        let common_type = common_join_key_type(&probe_type, &build_type)?;
        match common_type {
            Some(target_type) => {
                if probe_type != target_type {
                    probe_keys[idx] =
                        arena.push_typed(ExprNode::Cast(probe_expr), target_type.clone());
                }
                if build_type != target_type {
                    build_keys[idx] = arena.push_typed(ExprNode::Cast(build_expr), target_type);
                }
            }
            None => {
                build_keys[idx] = arena.push_typed(ExprNode::Cast(build_expr), probe_type);
            }
        }
    }
    Ok(())
}

fn common_join_key_type(left: &DataType, right: &DataType) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(Some(left.clone()));
    }
    match (left, right) {
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => Ok(Some(crate::types::coercion::decimal_compare_type(
            left, right,
        )?)),
        (DataType::List(left_field), DataType::List(right_field)) => {
            let Some(elem_type) =
                common_join_key_type(left_field.data_type(), right_field.data_type())?
            else {
                return Ok(None);
            };
            Ok(Some(DataType::List(Arc::new(Field::new(
                left_field.name(),
                elem_type,
                left_field.is_nullable() || right_field.is_nullable(),
            )))))
        }
        _ => Ok(Some(wider_type(left, right))),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{
        column_ref, lower, one_col_values_node_with, one_col_values_node_with_nullable,
        output_column_with_nullable, physical_node, two_col_values_node,
    };
    use super::super::{NodeLoweringContext, lower_proto_node};
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::join::JoinRuntimeFilterExecution;
    use crate::proto::plan;

    #[test]
    fn hash_join_output_schema_uses_plan_output_nullability() {
        let output_columns = vec![
            output_column_with_nullable(1, "lhs", DataType::Int64, false),
            output_column_with_nullable(2, "rhs", DataType::Int64, true),
        ];
        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
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
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert!(!join.join_scope_chunk_schema.slots()[0].nullable());
        assert!(join.join_scope_chunk_schema.slots()[1].nullable());
    }

    #[test]
    fn hash_join_execution_mode_overrides_distribution_and_unknown_defaults_broadcast() {
        let mut join = physical_node(
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
                execution_mode: Some(plan::JoinExecutionMode::Partitioned as i32),
                build_runtime_filters: Vec::new(),
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join_node) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(
            join_node.distribution_mode,
            crate::exec::node::join::JoinDistributionMode::Partitioned
        );

        let plan::distributed_node::Payload::Physical(physical) =
            join.payload.as_mut().expect("physical")
        else {
            panic!("expected physical");
        };
        let Some(plan::plan_node::Kind::HashJoin(hash_join)) = physical.kind.as_mut() else {
            panic!("expected hash join");
        };
        hash_join.distribution = plan::JoinDistribution::Unknown as i32;
        hash_join.execution_mode = None;
        let lowered = lower(&join);
        let ExecNodeKind::Join(join_node) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(
            join_node.distribution_mode,
            crate::exec::node::join::JoinDistributionMode::Broadcast
        );
    }

    #[test]
    fn hash_join_runtime_filter_skips_unsafe_join_type_and_rejects_mismatched_exprs() {
        let outer_with_rf = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(3, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: vec![plan::RuntimeFilterBuildIntent {
                    filter_id: 1,
                    build_expr: Some(column_ref(3, DataType::Int64)),
                    probe_expr: Some(column_ref(1, DataType::Int64)),
                    expr_order: 0,
                    execution_mode: plan::JoinExecutionMode::Broadcast as i32,
                }],
            }),
            Vec::new(),
            vec![
                two_col_values_node(10),
                one_col_values_node_with(11, 3, "rhs", 10),
            ],
        );
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node(&outer_with_rf, &mut arena, &NodeLoweringContext::default())
            .expect("outer join runtime filters should be skipped");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native runtime-filter execution")
        };
        assert!(producers.is_empty());

        let mismatched_probe = physical_node(
            31,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(3, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: vec![plan::RuntimeFilterBuildIntent {
                    filter_id: 2,
                    build_expr: Some(column_ref(3, DataType::Int64)),
                    probe_expr: Some(column_ref(2, DataType::Int64)),
                    expr_order: 0,
                    execution_mode: plan::JoinExecutionMode::Broadcast as i32,
                }],
            }),
            Vec::new(),
            vec![
                two_col_values_node(10),
                one_col_values_node_with(11, 3, "rhs", 10),
            ],
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(
            &mismatched_probe,
            &mut arena,
            &NodeLoweringContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("probe_expr does not match"));
    }
}
