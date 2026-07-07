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
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::join::{
    JoinDistributionMode, JoinNode, JoinRuntimeFilterSpec, JoinType, RuntimeFilterMergeNode,
};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::compat::expr::lower_t_expr;
use crate::lower::compat::layout::{
    Layout, chunk_schema_for_layout, chunk_schema_for_layout_with_nullable_tuples,
};
use crate::lower::compat::node::Lowered;
use crate::novarocks_logging::warn;
use crate::types::wider_type;

use crate::thrift::{descriptors, plan_nodes, runtime_filter, types};

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

fn lower_runtime_filter_merge_nodes(
    nodes: Option<&[types::TNetworkAddress]>,
) -> Vec<RuntimeFilterMergeNode> {
    nodes
        .unwrap_or_default()
        .iter()
        .map(|addr| RuntimeFilterMergeNode {
            host: addr.hostname.clone(),
            port: addr.port,
        })
        .collect()
}

fn validate_runtime_filter_intent(
    desc: &runtime_filter::TRuntimeFilterDescription,
    filter_id: i32,
    expr_order: usize,
    probe_layout: &Layout,
    build_layout: &Layout,
    expected_probe_key: ExprId,
    expected_build_key: ExprId,
    arena: &mut ExprArena,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<(), String> {
    let build_expr = desc
        .build_expr
        .as_ref()
        .ok_or_else(|| format!("runtime filter {} missing build_expr", filter_id))?;
    let build_expr_id = lower_t_expr(build_expr, arena, build_layout, last_query_id, fe_addr)
        .map_err(|err| format!("runtime filter {} build_expr: {err}", filter_id))?;
    if !exprs_equivalent(arena, build_expr_id, expected_build_key) {
        return Err(format!(
            "runtime filter {} build_expr does not match join key at expr_order {}",
            filter_id, expr_order
        ));
    }

    let target_exprs = desc
        .plan_node_id_to_target_expr
        .as_ref()
        .filter(|targets| !targets.is_empty())
        .ok_or_else(|| {
            format!(
                "runtime filter {} missing plan_node_id_to_target_expr",
                filter_id
            )
        })?;
    for probe_expr in target_exprs.values() {
        let probe_expr_id =
            lower_t_expr(probe_expr, arena, probe_layout, last_query_id, fe_addr)
                .map_err(|err| format!("runtime filter {} probe target_expr: {err}", filter_id))?;
        if !exprs_equivalent(arena, probe_expr_id, expected_probe_key) {
            return Err(format!(
                "runtime filter {} probe target_expr does not match join key at expr_order {}",
                filter_id, expr_order
            ));
        }
    }

    Ok(())
}

fn exprs_equivalent(arena: &ExprArena, left: ExprId, right: ExprId) -> bool {
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
    left: &[(crate::common::ids::SlotId, ExprId)],
    right: &[(crate::common::ids::SlotId, ExprId)],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_slot, left_expr), (right_slot, right_expr))| {
                left_slot == right_slot && exprs_equivalent(arena, *left_expr, *right_expr)
            })
}

/// Lower a HASH_JOIN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_hash_join_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.len() != 2 {
        return Err(format!(
            "HASH_JOIN_NODE expected 2 children, got {}",
            children.len()
        ));
    }

    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    let Some(join) = node.hash_join_node.as_ref() else {
        return Err("HASH_JOIN_NODE missing hash_join_node payload".to_string());
    };

    let join_type = match join.join_op {
        plan_nodes::TJoinOp::INNER_JOIN => JoinType::Inner,
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN => JoinType::LeftOuter,
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN => JoinType::RightOuter,
        plan_nodes::TJoinOp::FULL_OUTER_JOIN => JoinType::FullOuter,
        plan_nodes::TJoinOp::LEFT_SEMI_JOIN => JoinType::LeftSemi,
        plan_nodes::TJoinOp::RIGHT_SEMI_JOIN => JoinType::RightSemi,
        plan_nodes::TJoinOp::LEFT_ANTI_JOIN => JoinType::LeftAnti,
        plan_nodes::TJoinOp::RIGHT_ANTI_JOIN => JoinType::RightAnti,
        plan_nodes::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN => JoinType::NullAwareLeftAnti,
        other => {
            return Err(format!(
                "unsupported HASH_JOIN_NODE join_op={other:?} (supported: INNER/LEFT_OUTER/RIGHT_OUTER/FULL_OUTER/LEFT_SEMI/RIGHT_SEMI/LEFT_ANTI/RIGHT_ANTI/NULL_AWARE_LEFT_ANTI)"
            ));
        }
    };
    let is_skew_join = join.is_skew_join.unwrap_or(false);

    if join.eq_join_conjuncts.is_empty() {
        return Err("HASH_JOIN_NODE requires non-empty eq_join_conjuncts".to_string());
    }

    // Lower residual join predicates (FE: other_join_conjuncts) on the joined output layout
    // (left columns then right columns).
    let mut residual_predicate: Option<crate::exec::expr::ExprId> = None;
    let distribution_mode = match join.distribution_mode {
        Some(plan_nodes::TJoinDistributionMode::BROADCAST)
        | Some(plan_nodes::TJoinDistributionMode::REPLICATED) => JoinDistributionMode::Broadcast,
        _ => JoinDistributionMode::Partitioned,
    };

    let mut probe_keys = Vec::with_capacity(join.eq_join_conjuncts.len());
    let mut build_keys = Vec::with_capacity(join.eq_join_conjuncts.len());
    let mut eq_null_safe = Vec::with_capacity(join.eq_join_conjuncts.len());
    let right_semi_physical_right_probe = join_type == JoinType::RightSemi;
    for cond in &join.eq_join_conjuncts {
        let null_safe = match cond.opcode {
            Some(op) if op == crate::thrift::opcodes::TExprOpcode::EQ_FOR_NULL => true,
            Some(op) if op == crate::thrift::opcodes::TExprOpcode::EQ => false,
            None => false,
            Some(other) => {
                return Err(format!(
                    "unsupported HASH_JOIN_NODE eq_join_conjunct opcode={other:?} (expected EQ or EQ_FOR_NULL)"
                ));
            }
        };
        eq_null_safe.push(null_safe);
        let left_key = lower_t_expr(&cond.left, arena, &left.layout, last_query_id, fe_addr)?;
        let right_key = lower_t_expr(&cond.right, arena, &right.layout, last_query_id, fe_addr)?;
        if right_semi_physical_right_probe {
            probe_keys.push(right_key);
            build_keys.push(left_key);
        } else {
            probe_keys.push(left_key);
            build_keys.push(right_key);
        }
    }
    let raw_probe_keys = probe_keys.clone();
    let raw_build_keys = build_keys.clone();
    for idx in 0..probe_keys.len() {
        let probe_expr = *probe_keys
            .get(idx)
            .ok_or_else(|| "HASH_JOIN probe key missing".to_string())?;
        let build_expr = *build_keys
            .get(idx)
            .ok_or_else(|| "HASH_JOIN build key missing".to_string())?;
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
                    let casted = arena.push_typed(ExprNode::Cast(probe_expr), target_type.clone());
                    probe_keys[idx] = casted;
                }
                if build_type != target_type {
                    let casted = arena.push_typed(ExprNode::Cast(build_expr), target_type);
                    build_keys[idx] = casted;
                }
            }
            None => {
                let casted = arena.push_typed(ExprNode::Cast(build_expr), probe_type);
                build_keys[idx] = casted;
            }
        }
    }
    for key in probe_keys.iter().chain(build_keys.iter()) {
        if let Some(dt) = arena.data_type(*key)
            && matches!(dt, DataType::LargeBinary)
        {
            return Err("VARIANT is not supported in HASH_JOIN keys".to_string());
        }
    }

    // Join outputs concatenated rows (left then right) in child output order.
    // Use a layout that matches that physical row layout so SLOT_REF resolution stays correct.
    let mut order = Vec::with_capacity(left.layout.order.len() + right.layout.order.len());
    order.extend_from_slice(&left.layout.order);
    order.extend_from_slice(&right.layout.order);
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    let layout = Layout { order, index };

    if let Some(other) = join.other_join_conjuncts.as_ref().filter(|v| !v.is_empty()) {
        let mut lowered = Vec::with_capacity(other.len());
        for e in other {
            lowered.push(lower_t_expr(e, arena, &layout, last_query_id, fe_addr)?);
        }
        let mut it = lowered.into_iter();
        let Some(first) = it.next() else {
            return Err("HASH_JOIN_NODE other_join_conjuncts is empty".to_string());
        };
        let mut acc = first;
        for next in it {
            acc = arena.push_typed(ExprNode::And(acc, next), DataType::Boolean);
        }
        residual_predicate = Some(acc);
    }

    let mut runtime_filters = Vec::new();
    if is_skew_join {
        if join
            .build_runtime_filters
            .as_ref()
            .is_some_and(|filters| !filters.is_empty())
        {
            warn!(
                "skip runtime filters for skew hash join: node_id={}",
                node.node_id
            );
        }
    } else if let Some(filters) = join
        .build_runtime_filters
        .as_ref()
        .filter(|v| !v.is_empty())
        && matches!(
            join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    {
        if join.eq_join_conjuncts.is_empty() {
            return Err("HASH_JOIN_NODE runtime filters require eq_join_conjuncts".to_string());
        }
        for desc in filters {
            let filter_id = desc
                .filter_id
                .ok_or_else(|| "runtime filter missing filter_id".to_string())?;
            let expr_order = desc
                .expr_order
                .ok_or_else(|| format!("runtime filter {} missing expr_order", filter_id))?
                as usize;
            if expr_order >= join.eq_join_conjuncts.len() {
                return Err(format!(
                    "runtime filter {} expr_order {} out of range (eq_join_conjuncts={})",
                    filter_id,
                    expr_order,
                    join.eq_join_conjuncts.len()
                ));
            }
            if eq_null_safe.get(expr_order).copied().unwrap_or(false) {
                // Null-safe equality (`<=>`) must preserve NULL-key matches.
                // Runtime filters currently prune NULL probe rows, so skip building
                // runtime filters on null-safe join keys.
                continue;
            }
            if desc.filter_type != Some(runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER) {
                return Err(format!(
                    "runtime filter {} has unsupported filter_type {:?}",
                    filter_id, desc.filter_type
                ));
            }
            let build_key = build_keys
                .get(expr_order)
                .ok_or_else(|| "runtime filter build key missing".to_string())?;
            let probe_key = probe_keys
                .get(expr_order)
                .ok_or_else(|| "runtime filter probe key missing".to_string())?;
            let (probe_layout, build_layout) = if right_semi_physical_right_probe {
                (&right.layout, &left.layout)
            } else {
                (&left.layout, &right.layout)
            };
            validate_runtime_filter_intent(
                desc,
                filter_id,
                expr_order,
                probe_layout,
                build_layout,
                raw_probe_keys[expr_order],
                raw_build_keys[expr_order],
                arena,
                last_query_id,
                fe_addr,
            )?;
            let build_type = arena
                .data_type(*build_key)
                .ok_or_else(|| "runtime filter build key type missing".to_string())?;
            let probe_type = arena
                .data_type(*probe_key)
                .ok_or_else(|| "runtime filter probe key type missing".to_string())?;
            let supported = |t: &DataType| {
                matches!(
                    t,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Boolean
                        | DataType::Utf8
                        | DataType::Date32
                        | DataType::Timestamp(_, _)
                        | DataType::Decimal128(_, _)
                )
            };
            if !supported(build_type) || !supported(probe_type) {
                warn!(
                    "skip runtime filter {} due to unsupported key types build={:?} probe={:?}",
                    filter_id, build_type, probe_type
                );
                continue;
            }
            let Some(ExprNode::SlotId(probe_slot_id)) = arena.node(*probe_key) else {
                continue;
            };
            let merge_nodes =
                lower_runtime_filter_merge_nodes(desc.runtime_filter_merge_nodes.as_deref());
            let has_remote_targets = desc.has_remote_targets.unwrap_or(false);
            runtime_filters.push(JoinRuntimeFilterSpec {
                filter_id,
                expr_order,
                probe_expr_id: *probe_key,
                build_expr_id: *build_key,
                probe_slot_id: *probe_slot_id,
                build_data_type: build_type.clone(),
                merge_nodes,
                has_remote_targets,
            });
        }
    }

    // Use the full join-scope layout for all join types.  For SEMI/ANTI joins
    // the FE's sort_tuple_slot_exprs / analytic output columns may reference
    // slots from the pruned (build or probe) side.  StarRocks BE handles this
    // by emitting NULL-filled placeholder columns for the pruned side in the
    // join output chunk.  We do the same, so downstream nodes (SORT, ANALYTIC,
    // EXCHANGE) always find every declared slot in the chunk.
    let output_layout = layout.clone();

    // For SEMI/ANTI joins, FE may still attach both tuples in row_tuples (join-scope),
    // while the logical output is output-side only. Accept that, but require the output-side
    // tuple(s) to be present when row_tuples is not empty.
    if matches!(
        join_type,
        JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::NullAwareLeftAnti
    ) {
        let out_tuples: HashSet<_> = node.row_tuples.iter().copied().collect();
        if !out_tuples.is_empty() {
            let left_tuples: HashSet<_> = left.layout.order.iter().map(|(t, _)| *t).collect();
            let right_tuples: HashSet<_> = right.layout.order.iter().map(|(t, _)| *t).collect();
            let expected = match join_type {
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti => {
                    left_tuples
                }
                JoinType::RightSemi | JoinType::RightAnti => right_tuples,
                JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => HashSet::new(),
            };
            if !expected.is_empty() && !expected.is_subset(&out_tuples) {
                return Err(format!(
                    "HASH_JOIN_NODE row_tuples {:?} must include output side tuples {:?} for join_type={:?}",
                    node.row_tuples, expected, join_type
                ));
            }
        }
    }

    let Some(desc_tbl) = desc_tbl else {
        return Err("HASH_JOIN_NODE requires desc_tbl for schema".to_string());
    };
    let left_chunk_schema = chunk_schema_for_layout(desc_tbl, &left.layout)?;
    let right_chunk_schema = chunk_schema_for_layout(desc_tbl, &right.layout)?;
    let join_scope_chunk_schema = chunk_schema_for_layout_with_nullable_tuples(
        desc_tbl,
        &layout,
        &node.row_tuples,
        &node.nullable_tuples,
    )?;

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                distribution_mode,
                left_chunk_schema,
                right_chunk_schema,
                join_scope_chunk_schema,
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate,
                runtime_filters,
            }),
        },
        layout: output_layout,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::lookup::LookUpNode;
    use crate::exec::node::values::ValuesNode;
    use crate::lower::compat::type_lowering::scalar_type_desc;
    use crate::sql::codegen::descriptors::DescriptorTableBuilder;
    use crate::sql::codegen::expr_compiler::build_slot_ref_texpr;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TSlotRef};
    use crate::thrift::opcodes::TExprOpcode;

    #[test]
    fn common_join_key_type_promotes_mixed_integers() {
        assert_eq!(
            common_join_key_type(&DataType::Int32, &DataType::Int64).unwrap(),
            Some(DataType::Int64)
        );
    }

    #[test]
    fn lower_runtime_filter_merge_nodes_preserves_host_and_port() {
        let merge_nodes = vec![
            types::TNetworkAddress::new("merge-a".to_string(), 18090),
            types::TNetworkAddress::new("".to_string(), 18091),
        ];

        assert_eq!(
            lower_runtime_filter_merge_nodes(Some(&merge_nodes)),
            vec![
                crate::exec::node::join::RuntimeFilterMergeNode {
                    host: "merge-a".to_string(),
                    port: 18090,
                },
                crate::exec::node::join::RuntimeFilterMergeNode {
                    host: "".to_string(),
                    port: 18091,
                },
            ]
        );
    }

    #[test]
    fn hash_join_scope_schema_honors_nullable_tuples() {
        let desc_tbl = descriptors::TDescriptorTable::new(
            Some(vec![
                slot_desc(1, 11, "left_k", false),
                slot_desc(2, 22, "count(1)", false),
            ]),
            vec![tuple_desc(1), tuple_desc(2)],
            None::<Vec<descriptors::TTableDescriptor>>,
            None::<bool>,
        );
        let node = crate::sql::codegen::nodes::build_hash_join_node(
            7,
            &[1],
            &[2],
            plan_nodes::TJoinOp::LEFT_OUTER_JOIN,
            plan_nodes::TJoinDistributionMode::BROADCAST,
            vec![plan_nodes::TEqJoinCondition {
                left: slot_ref_expr(1, 11),
                right: slot_ref_expr(2, 22),
                opcode: None,
            }],
            Vec::new(),
        );
        assert_eq!(node.nullable_tuples, vec![false, true]);

        let lowered = lower_hash_join_node(
            vec![child(1, 11), child(2, 22)],
            &node,
            &mut ExprArena::default(),
            Some(&desc_tbl),
            None,
            None,
        )
        .expect("lower hash join");

        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join node");
        };
        assert!(
            !join.join_scope_chunk_schema.slots()[0].nullable(),
            "preserved left side should remain non-nullable"
        );
        assert!(
            join.join_scope_chunk_schema.slots()[1].nullable(),
            "nullable_tuples must null-extend the right side even when slot descriptors are non-nullable"
        );
    }

    #[test]
    fn right_semi_hash_join_lowers_runtime_filter_from_left_build_to_right_probe() {
        let mut desc_builder = DescriptorTableBuilder::new();
        let arrow_int = DataType::Int32;
        let thrift_int = scalar_type_desc(types::TPrimitiveType::INT);
        desc_builder.add_slot(11, 1, "l_k", &arrow_int, false, 0);
        desc_builder.add_tuple(1, None);
        desc_builder.add_slot(22, 2, "r_k", &arrow_int, false, 0);
        desc_builder.add_tuple(2, None);
        let desc_tbl = desc_builder.build();

        let left_layout = Layout {
            order: vec![(1, 11)],
            index: BTreeMap::from([((1, 11), 0)]).into_iter().collect(),
        };
        let right_layout = Layout {
            order: vec![(2, 22)],
            index: BTreeMap::from([((2, 22), 0)]).into_iter().collect(),
        };
        let left = Lowered {
            node: ExecNode {
                kind: ExecNodeKind::LookUp(LookUpNode {
                    node_id: 101,
                    row_pos_descs: Default::default(),
                    output_chunk_schema: chunk_schema_for_layout(&desc_tbl, &left_layout)
                        .expect("left schema"),
                }),
            },
            layout: left_layout,
        };
        let right = Lowered {
            node: ExecNode {
                kind: ExecNodeKind::LookUp(LookUpNode {
                    node_id: 202,
                    row_pos_descs: Default::default(),
                    output_chunk_schema: chunk_schema_for_layout(&desc_tbl, &right_layout)
                        .expect("right schema"),
                }),
            },
            layout: right_layout,
        };

        let left_key = build_slot_ref_texpr(11, 1, thrift_int.clone());
        let right_key = build_slot_ref_texpr(22, 2, thrift_int);
        let mut node = crate::lower::compat::node::test_plan_node(
            10,
            plan_nodes::TPlanNodeType::HASH_JOIN_NODE,
            2,
        );
        node.row_tuples = vec![2];
        node.hash_join_node = Some(plan_nodes::THashJoinNode {
            join_op: plan_nodes::TJoinOp::RIGHT_SEMI_JOIN,
            eq_join_conjuncts: vec![plan_nodes::TEqJoinCondition {
                left: left_key.clone(),
                right: right_key.clone(),
                opcode: Some(TExprOpcode::EQ),
            }],
            other_join_conjuncts: None,
            is_push_down: None,
            add_probe_filters: None,
            is_rewritten_from_not_in: None,
            sql_join_predicates: None,
            sql_predicates: None,
            distribution_mode: Some(plan_nodes::TJoinDistributionMode::BROADCAST),
            build_runtime_filters_from_planner: None,
            partition_exprs: None,
            output_columns: None,
            interpolate_passthrough: None,
            late_materialization: None,
            enable_partition_hash_join: None,
            is_skew_join: None,
            common_slot_map: None,
            asof_join_condition: None,
            build_runtime_filters: Some(vec![runtime_filter::TRuntimeFilterDescription {
                filter_id: Some(7),
                expr_order: Some(0),
                build_expr: Some(left_key),
                plan_node_id_to_target_expr: Some(BTreeMap::from([(202, right_key)])),
                filter_type: Some(runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER),
                ..Default::default()
            }]),
        });

        let mut arena = ExprArena::default();
        let lowered = lower_hash_join_node(
            vec![left, right],
            &node,
            &mut arena,
            Some(&desc_tbl),
            None,
            None,
        )
        .expect("lower right semi hash join");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected join node");
        };

        assert_eq!(join.runtime_filters.len(), 1);
        assert!(matches!(
            arena.node(join.build_keys[0]),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(11)
        ));
        assert!(matches!(
            arena.node(join.probe_keys[0]),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(22)
        ));
        assert_eq!(join.runtime_filters[0].probe_slot_id, SlotId::new(22));
    }

    fn type_desc() -> types::TTypeDesc {
        scalar_type_desc(types::TPrimitiveType::BIGINT)
    }

    fn slot_desc(
        tuple_id: types::TTupleId,
        slot_id: types::TSlotId,
        name: &str,
        nullable: bool,
    ) -> descriptors::TSlotDescriptor {
        descriptors::TSlotDescriptor::new(
            Some(slot_id),
            Some(tuple_id),
            Some(type_desc()),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(name.to_string()),
            Some(0),
            Some(true),
            Some(true),
            Some(nullable),
            None::<i32>,
            None::<String>,
            None::<bool>,
        )
    }

    fn tuple_desc(tuple_id: types::TTupleId) -> descriptors::TTupleDescriptor {
        descriptors::TTupleDescriptor::new(
            Some(tuple_id),
            Some(8),
            Some(1),
            None::<types::TTableId>,
            Some(1),
        )
    }

    fn slot_ref_expr(tuple_id: types::TTupleId, slot_id: types::TSlotId) -> TExpr {
        TExpr {
            nodes: vec![TExprNode {
                node_type: TExprNodeType::SLOT_REF,
                type_: type_desc(),
                num_children: 0,
                slot_ref: Some(TSlotRef { slot_id, tuple_id }),
                ..default_expr_node()
            }],
        }
    }

    fn default_expr_node() -> TExprNode {
        TExprNode {
            node_type: TExprNodeType::INT_LITERAL,
            type_: type_desc(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn single_slot_layout(tuple_id: types::TTupleId, slot_id: types::TSlotId) -> Layout {
        let mut index = HashMap::new();
        index.insert((tuple_id, slot_id), 0);
        Layout {
            order: vec![(tuple_id, slot_id)],
            index,
        }
    }

    fn child(tuple_id: types::TTupleId, slot_id: types::TSlotId) -> Lowered {
        let slot = SlotId::new(u32::try_from(slot_id).expect("test slot id"));
        let field = Field::new("dummy", DataType::Int64, true);
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot,
                field.clone(),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let batch = RecordBatch::try_new(
            chunk_schema.arrow_schema_ref(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .expect("record batch");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk,
                    node_id: tuple_id,
                }),
            },
            layout: single_slot_layout(tuple_id, slot_id),
        }
    }
}
