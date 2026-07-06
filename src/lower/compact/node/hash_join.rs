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
    JoinDistributionMode, JoinNode, JoinRuntimeFilterSpec, JoinType, RuntimeFilterMergeNode,
};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::compact::expr::lower_t_expr;
use crate::lower::compact::layout::{
    Layout, chunk_schema_for_layout, chunk_schema_for_layout_with_nullable_tuples,
};
use crate::lower::compact::node::Lowered;
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
        probe_keys.push(lower_t_expr(
            &cond.left,
            arena,
            &left.layout,
            last_query_id,
            fe_addr,
        )?);
        build_keys.push(lower_t_expr(
            &cond.right,
            arena,
            &right.layout,
            last_query_id,
            fe_addr,
        )?);
    }
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
    use super::*;
    use std::collections::HashMap;

    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::values::ValuesNode;
    use crate::lower::compact::type_lowering::scalar_type_desc;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TSlotRef};

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
