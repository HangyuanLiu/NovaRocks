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

use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::join::{
    JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::lower_t_expr_at;
use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_schema_for_layout, chunk_schema_for_layout_with_nullable_tuples,
};
use crate::protocol::starrocks::decode::node::Lowered;
use novarocks_types::wider_type;

use crate::thrift::{descriptors, plan_nodes};

fn common_join_key_type(left: &DataType, right: &DataType) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(Some(left.clone()));
    }
    match (left, right) {
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => Ok(Some(novarocks_types::coercion::decimal_compare_type(
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

/// Lower a HASH_JOIN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_hash_join_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let payload_path = node_path.clone().field("hash_join_node");
    if children.len() != 2 {
        return Err(StarRocksFragmentDecodeError::invalid_value(
            node_path.clone().field("num_children"),
            format!("HASH_JOIN_NODE expected 2 children, got {}", children.len()),
        ));
    }

    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    let Some(join) = node.hash_join_node.as_ref() else {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path,
            "HASH_JOIN_NODE missing hash_join_node payload",
        ));
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
            return Err(StarRocksFragmentDecodeError::unsupported(
                payload_path.clone().field("join_op"),
                format!(
                    "unsupported HASH_JOIN_NODE join_op={other:?} (supported: INNER/LEFT_OUTER/RIGHT_OUTER/FULL_OUTER/LEFT_SEMI/RIGHT_SEMI/LEFT_ANTI/RIGHT_ANTI/NULL_AWARE_LEFT_ANTI)"
                ),
            ));
        }
    };
    if join.eq_join_conjuncts.is_empty() {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path.clone().field("eq_join_conjuncts"),
            "HASH_JOIN_NODE requires non-empty eq_join_conjuncts",
        ));
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
    for (cond_index, cond) in join.eq_join_conjuncts.iter().enumerate() {
        let cond_path = payload_path
            .clone()
            .field("eq_join_conjuncts")
            .index(cond_index);
        let null_safe = match cond.opcode {
            Some(op) if op == crate::thrift::opcodes::TExprOpcode::EQ_FOR_NULL => true,
            Some(op) if op == crate::thrift::opcodes::TExprOpcode::EQ => false,
            None => false,
            Some(other) => {
                return Err(StarRocksFragmentDecodeError::unsupported(
                    cond_path.clone().field("opcode"),
                    format!(
                        "unsupported HASH_JOIN_NODE eq_join_conjunct opcode={other:?} (expected EQ or EQ_FOR_NULL)"
                    ),
                ));
            }
        };
        eq_null_safe.push(null_safe);
        let left_key = lower_t_expr_at(
            &cond.left,
            arena,
            &left.layout,
            last_query_id,
            fe_addr,
            cond_path.clone().field("left"),
        )?;
        let right_key = lower_t_expr_at(
            &cond.right,
            arena,
            &right.layout,
            last_query_id,
            fe_addr,
            cond_path.field("right"),
        )?;
        if right_semi_physical_right_probe {
            probe_keys.push(right_key);
            build_keys.push(left_key);
        } else {
            probe_keys.push(left_key);
            build_keys.push(right_key);
        }
    }
    for idx in 0..probe_keys.len() {
        let probe_expr = *probe_keys.get(idx).ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                payload_path.clone().field("eq_join_conjuncts").index(idx),
                "HASH_JOIN probe key missing",
            )
        })?;
        let build_expr = *build_keys.get(idx).ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                payload_path.clone().field("eq_join_conjuncts").index(idx),
                "HASH_JOIN build key missing",
            )
        })?;
        let probe_type = arena
            .data_type(probe_expr)
            .ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    payload_path.clone().field("eq_join_conjuncts").index(idx),
                    "HASH_JOIN probe key type missing",
                )
            })?
            .clone();
        let build_type = arena
            .data_type(build_expr)
            .ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    payload_path.clone().field("eq_join_conjuncts").index(idx),
                    "HASH_JOIN build key type missing",
                )
            })?
            .clone();
        if probe_type == build_type {
            continue;
        }

        let common_type = common_join_key_type(&probe_type, &build_type).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("eq_join_conjuncts").index(idx),
                detail,
            )
        })?;
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
            return Err(StarRocksFragmentDecodeError::unsupported(
                payload_path.clone().field("eq_join_conjuncts"),
                "VARIANT is not supported in HASH_JOIN keys",
            ));
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
        for (index, e) in other.iter().enumerate() {
            lowered.push(lower_t_expr_at(
                e,
                arena,
                &layout,
                last_query_id,
                fe_addr,
                payload_path
                    .clone()
                    .field("other_join_conjuncts")
                    .index(index),
            )?);
        }
        let mut it = lowered.into_iter();
        let Some(first) = it.next() else {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("other_join_conjuncts"),
                "HASH_JOIN_NODE other_join_conjuncts is empty",
            ));
        };
        let mut acc = first;
        for next in it {
            acc = arena.push_typed(ExprNode::And(acc, next), DataType::Boolean);
        }
        residual_predicate = Some(acc);
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
                return Err(StarRocksFragmentDecodeError::inconsistent(
                    node_path.clone().field("row_tuples"),
                    format!(
                        "HASH_JOIN_NODE row_tuples {:?} must include output side tuples {:?} for join_type={:?}",
                        node.row_tuples, expected, join_type
                    ),
                ));
            }
        }
    }

    let Some(desc_tbl) = desc_tbl else {
        return Err(StarRocksFragmentDecodeError::missing(
            node_path.clone().field("row_tuples"),
            "HASH_JOIN_NODE requires desc_tbl for schema",
        ));
    };
    let left_chunk_schema = chunk_schema_for_layout(desc_tbl, &left.layout).map_err(|detail| {
        StarRocksFragmentDecodeError::invalid_value(node_path.clone().field("row_tuples"), detail)
    })?;
    let right_chunk_schema =
        chunk_schema_for_layout(desc_tbl, &right.layout).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(
                node_path.clone().field("row_tuples"),
                detail,
            )
        })?;
    let join_scope_chunk_schema = chunk_schema_for_layout_with_nullable_tuples(
        desc_tbl,
        &layout,
        &node.row_tuples,
        &node.nullable_tuples,
    )
    .map_err(|detail| {
        StarRocksFragmentDecodeError::invalid_value(node_path.field("row_tuples"), detail)
    })?;

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
                runtime_filter_execution: JoinRuntimeFilterExecution {
                    producers: Vec::new(),
                },
            }),
        },
        layout: output_layout,
    })
}
