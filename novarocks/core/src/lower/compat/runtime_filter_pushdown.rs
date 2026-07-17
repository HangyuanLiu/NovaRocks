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

use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::aggregate::AggregateNode;
use crate::exec::node::analytic::AnalyticNode;
use crate::exec::node::assert::AssertNumRowsNode;
use crate::exec::node::fetch::FetchNode;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::join::{
    CompatJoinRuntimeFilterSpec, JoinNode, JoinRuntimeFilterExecution, JoinType,
};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::nljoin::NestedLoopJoinNode;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::set_op::SetOpNode;
use crate::exec::node::sort::SortNode;
use crate::exec::node::table_function::TableFunctionNode;
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::{ExecNode, ExecNodeKind, RuntimeFilterProbeSpec};

#[derive(Clone)]
struct RuntimeFilterPushSpec {
    filter_id: i32,
    expr_id: ExprId,
    slot_id: SlotId,
    build_node_id: Option<i32>,
}

fn expr_slot_ref(arena: &ExprArena, expr_id: ExprId) -> Option<SlotId> {
    match arena.node(expr_id) {
        Some(ExprNode::SlotId(slot_id)) => Some(*slot_id),
        _ => None,
    }
}

fn collect_expr_slots(arena: &ExprArena, expr_id: ExprId, out: &mut HashSet<SlotId>) {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else { continue };
        match node {
            ExprNode::Literal(_) => {}
            ExprNode::SlotId(slot_id) => {
                out.insert(*slot_id);
            }
            ExprNode::ArrayExpr { elements } => {
                for child in elements {
                    stack.push(*child);
                }
            }
            ExprNode::StructExpr { fields } => {
                for child in fields {
                    stack.push(*child);
                }
            }
            ExprNode::LambdaFunction { .. } => {
                // Do not descend into nested lambdas when collecting slots.
            }
            ExprNode::DictDecode { child, .. } => {
                stack.push(*child);
            }
            ExprNode::Cast(child)
            | ExprNode::CastTime(child)
            | ExprNode::CastTimeFromDatetime(child)
            | ExprNode::Not(child)
            | ExprNode::IsNull(child)
            | ExprNode::IsNotNull(child)
            | ExprNode::Clone(child) => {
                stack.push(*child);
            }
            ExprNode::Add(a, b)
            | ExprNode::Sub(a, b)
            | ExprNode::Mul(a, b)
            | ExprNode::Div(a, b)
            | ExprNode::Mod(a, b)
            | ExprNode::Eq(a, b)
            | ExprNode::EqForNull(a, b)
            | ExprNode::Ne(a, b)
            | ExprNode::Lt(a, b)
            | ExprNode::Le(a, b)
            | ExprNode::Gt(a, b)
            | ExprNode::Ge(a, b)
            | ExprNode::And(a, b)
            | ExprNode::Or(a, b) => {
                stack.push(*a);
                stack.push(*b);
            }
            ExprNode::In { child, values, .. } => {
                stack.push(*child);
                for value in values {
                    stack.push(*value);
                }
            }
            ExprNode::Case { children, .. } => {
                for child in children {
                    stack.push(*child);
                }
            }
            ExprNode::FunctionCall { args, .. } => {
                for arg in args {
                    stack.push(*arg);
                }
            }
        }
    }
}

fn expr_slot_ids(arena: &ExprArena, expr_id: ExprId) -> HashSet<SlotId> {
    let mut out = HashSet::new();
    collect_expr_slots(arena, expr_id, &mut out);
    out
}

fn output_slots_for_node(node: &ExecNode) -> Option<HashSet<SlotId>> {
    match &node.kind {
        ExecNodeKind::Project(project) => Some(
            project
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::Aggregate(aggregate) => Some(
            aggregate
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::Analytic(analytic) => Some(
            analytic
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::SetOp(set_op) => Some(
            set_op
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::ExchangeSource(exchange) => Some(
            exchange
                .expected_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::Scan(scan) => Some(
            scan.output_chunk_schema()
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::IcebergDeltaScan(scan) => Some(
            scan.output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        #[cfg(feature = "compat")]
        ExecNodeKind::Fetch(fetch) => Some(
            fetch
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::LookUp(lookup) => Some(
            lookup
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::AssertNumRows(AssertNumRowsNode { input, .. }) => {
            output_slots_for_node(input)
        }
        ExecNodeKind::Filter(FilterNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::NativeRuntimeFilterConsumer(consumer) => {
            output_slots_for_node(&consumer.input)
        }
        ExecNodeKind::Repeat(RepeatNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::ChangeEventExpand(node) => {
            Some(node.output_slot_ids.iter().copied().collect())
        }
        ExecNodeKind::Limit(LimitNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::Sort(SortNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::TableFunction(table_function) => Some(
            table_function
                .output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::UnionAll(UnionAllNode { inputs, .. }) => {
            inputs.first().and_then(output_slots_for_node)
        }
        ExecNodeKind::Values(_) => None,
        ExecNodeKind::Join(_) | ExecNodeKind::NestedLoopJoin(_) => None,
    }
}

fn filter_specs_by_output_slots(
    arena: &ExprArena,
    specs: &[RuntimeFilterPushSpec],
    output_slots: &HashSet<SlotId>,
) -> Vec<RuntimeFilterPushSpec> {
    specs
        .iter()
        .filter(|spec| {
            let slots = expr_slot_ids(arena, spec.expr_id);
            output_slots.contains(&spec.slot_id)
                || slots.is_empty()
                || slots.iter().all(|slot| output_slots.contains(slot))
        })
        .cloned()
        .collect()
}

fn filter_specs_for_child(
    arena: &ExprArena,
    specs: &[RuntimeFilterPushSpec],
    child: &ExecNode,
) -> Vec<RuntimeFilterPushSpec> {
    let Some(output_slots) = output_slots_for_node(child) else {
        return specs.to_vec();
    };
    filter_specs_by_output_slots(arena, specs, &output_slots)
}

pub(crate) fn push_down_local_runtime_filters(root: &mut ExecNode, arena: &ExprArena) {
    let inherited = Vec::new();
    push_down_local_runtime_filters_inner(root, arena, &inherited);
}

fn push_down_local_runtime_filters_inner(
    node: &mut ExecNode,
    arena: &ExprArena,
    inherited: &[RuntimeFilterPushSpec],
) {
    match &mut node.kind {
        ExecNodeKind::AssertNumRows(AssertNumRowsNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Values(_) => {}
        ExecNodeKind::NativeRuntimeFilterConsumer(consumer) => {
            push_down_local_runtime_filters_inner(&mut consumer.input, arena, inherited);
        }
        ExecNodeKind::Project(project) => {
            let mut rewritten = Vec::new();
            for spec in inherited {
                let Some(slot_id) = expr_slot_ref(arena, spec.expr_id) else {
                    continue;
                };
                let Some(pos) = project
                    .output_chunk_schema
                    .slot_ids()
                    .iter()
                    .position(|s| *s == slot_id)
                else {
                    continue;
                };
                let expr_idx = project
                    .output_indices
                    .as_ref()
                    .and_then(|indices| indices.get(pos).copied())
                    .unwrap_or(pos);
                let Some(&new_expr_id) = project.exprs.get(expr_idx) else {
                    continue;
                };
                let mut next = spec.clone();
                next.expr_id = new_expr_id;
                let expr_slots = expr_slot_ids(arena, new_expr_id);
                if expr_slots.len() == 1
                    && let Some(slot) = expr_slots.iter().next()
                {
                    next.slot_id = *slot;
                }
                rewritten.push(next);
            }
            let filtered = filter_specs_for_child(arena, &rewritten, &project.input);
            push_down_local_runtime_filters_inner(&mut project.input, arena, &filtered);
        }
        ExecNodeKind::Filter(FilterNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Repeat(RepeatNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::ChangeEventExpand(node) => {
            let filtered = filter_specs_for_child(arena, inherited, &node.input);
            push_down_local_runtime_filters_inner(&mut node.input, arena, &filtered);
        }
        ExecNodeKind::UnionAll(UnionAllNode { inputs, .. }) => {
            for input in inputs {
                let filtered = filter_specs_for_child(arena, inherited, input);
                push_down_local_runtime_filters_inner(input, arena, &filtered);
            }
        }
        ExecNodeKind::Limit(LimitNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::TableFunction(TableFunctionNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::ExchangeSource(_exchange) => {
            // Do not push runtime filter specs to exchange source nodes.
            // Exchange sources are cross-fragment data channels; runtime filters
            // should be applied at the scan level in the producing fragment.
            // Pushing specs here creates a RuntimeFilterProbe dependency that
            // may never be satisfied in multi-fragment coordinated execution,
            // causing the pipeline to hang.
        }
        ExecNodeKind::Scan(scan) => {
            if inherited.is_empty() {
                return;
            }
            let output_slots: HashSet<SlotId> = scan
                .output_chunk_schema()
                .slot_ids()
                .iter()
                .copied()
                .collect();
            let filtered = filter_specs_by_output_slots(arena, inherited, &output_slots);
            if filtered.is_empty() {
                return;
            }
            let specs: Vec<RuntimeFilterProbeSpec> = filtered
                .iter()
                .filter_map(|spec| {
                    // Skip a probe spec whose expr has no type in the arena (only a
                    // stale/out-of-bounds expr_id; valid ids always have a type slot).
                    // A runtime filter against an unknown-type expr would be unsound.
                    let data_type = arena.data_type(spec.expr_id)?.clone();
                    Some(RuntimeFilterProbeSpec {
                        filter_id: spec.filter_id,
                        expr_id: spec.expr_id,
                        slot_id: spec.slot_id,
                        data_type,
                    })
                })
                .collect();
            scan.add_runtime_filter_specs(&specs);
            let waiting_set = filtered
                .iter()
                .filter_map(|spec| spec.build_node_id)
                .chain(scan.local_rf_waiting_set().iter().copied())
                .collect::<Vec<_>>();
            if !waiting_set.is_empty() {
                *scan = scan.clone().with_local_rf_waiting_set(waiting_set);
            }
        }
        ExecNodeKind::IcebergDeltaScan(_) => {
            // delta source is a leaf; runtime filters do not apply for A1
        }
        #[cfg(feature = "compat")]
        ExecNodeKind::Fetch(FetchNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::LookUp(_) => {}
        ExecNodeKind::Aggregate(AggregateNode {
            input, group_by, ..
        }) => {
            let mut group_by_slots = HashSet::new();
            for expr_id in group_by {
                if let Some(slot_id) = expr_slot_ref(arena, *expr_id) {
                    group_by_slots.insert(slot_id);
                }
            }
            let mut pushable = Vec::new();
            for spec in inherited {
                let Some(slot_id) = expr_slot_ref(arena, spec.expr_id) else {
                    continue;
                };
                if group_by_slots.contains(&slot_id) {
                    pushable.push(spec.clone());
                }
            }
            let filtered = filter_specs_for_child(arena, &pushable, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Join(JoinNode {
            left,
            right,
            node_id,
            join_type,
            probe_keys,
            build_keys: _build_keys,
            runtime_filter_execution,
            ..
        }) => {
            let right_semi_physical_right_probe = *join_type == JoinType::RightSemi;
            let (probe_child, build_child) = if right_semi_physical_right_probe {
                (right.as_mut(), left.as_mut())
            } else {
                (left.as_mut(), right.as_mut())
            };

            let runtime_filters: &[CompatJoinRuntimeFilterSpec] = match runtime_filter_execution {
                JoinRuntimeFilterExecution::Native { .. } => &[][..],
                #[cfg(feature = "compat")]
                JoinRuntimeFilterExecution::Compat { legacy_specs } => legacy_specs.as_slice(),
            };
            let mut probe_filters = inherited.to_vec();
            if matches!(
                *join_type,
                JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
            ) && !runtime_filters.is_empty()
            {
                let probe_exprs = probe_keys;
                for rf in runtime_filters {
                    if let Some(expr_id) = probe_exprs.get(rf.expr_order) {
                        probe_filters.push(RuntimeFilterPushSpec {
                            filter_id: rf.filter_id,
                            expr_id: *expr_id,
                            slot_id: rf.probe_slot_id,
                            build_node_id: Some(*node_id),
                        });
                    }
                }
            }

            let probe_filters = filter_specs_for_child(arena, &probe_filters, probe_child);
            let build_filters = filter_specs_for_child(arena, inherited, build_child);

            push_down_local_runtime_filters_inner(probe_child, arena, &probe_filters);
            push_down_local_runtime_filters_inner(build_child, arena, &build_filters);
        }
        ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode { left, right, .. }) => {
            let left_filters = filter_specs_for_child(arena, inherited, left);
            let right_filters = filter_specs_for_child(arena, inherited, right);
            push_down_local_runtime_filters_inner(left, arena, &left_filters);
            push_down_local_runtime_filters_inner(right, arena, &right_filters);
        }
        ExecNodeKind::Sort(SortNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Analytic(AnalyticNode { .. }) => {}
        ExecNodeKind::SetOp(SetOpNode { inputs, .. }) => {
            for input in inputs {
                let filtered = filter_specs_for_child(arena, inherited, input);
                push_down_local_runtime_filters_inner(input, arena, &filtered);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::exec::chunk::ChunkSchema;
    use crate::exec::node::BoxedExecIter;
    use crate::exec::node::join::JoinDistributionMode;
    #[cfg(feature = "compat")]
    use crate::exec::node::join::{CompatJoinRuntimeFilterSpec, JoinRuntimeFilterExecution};
    use crate::exec::node::project::ProjectNode;
    use crate::exec::node::scan::ScanNode;
    use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp};
    use crate::runtime::profile::RuntimeProfile;

    use super::*;

    struct DummyScanOp;

    impl ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::default())
        }
    }

    fn int_schema_for_slot(slot_id: SlotId, name: &str) -> crate::exec::chunk::ChunkSchemaRef {
        let schema = Schema::new(vec![Field::new(name, DataType::Int32, true)]);
        ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[slot_id]).expect("chunk schema")
    }

    fn scan_node_with_slot(node_id: i32, slot_id: SlotId, name: &str) -> ExecNode {
        ExecNode {
            kind: ExecNodeKind::Scan(
                ScanNode::new(Arc::new(DummyScanOp))
                    .with_node_id(node_id)
                    .with_output_chunk_schema(int_schema_for_slot(slot_id, name)),
            ),
        }
    }

    fn scan_runtime_filter_ids(node: &ExecNode) -> Vec<i32> {
        let ExecNodeKind::Scan(scan) = &node.kind else {
            panic!("expected scan node");
        };
        scan.runtime_filter_specs()
            .iter()
            .map(|spec| spec.filter_id)
            .collect()
    }

    #[cfg(feature = "compat")]
    #[test]
    fn right_semi_local_runtime_filter_pushes_to_preserved_right_child() {
        let left_slot = SlotId::new(11);
        let right_slot = SlotId::new(22);
        let mut arena = ExprArena::default();
        let left_key = arena.push_typed(ExprNode::SlotId(left_slot), DataType::Int32);
        let right_key = arena.push_typed(ExprNode::SlotId(right_slot), DataType::Int32);
        let left_schema = int_schema_for_slot(left_slot, "l_k");
        let right_schema = int_schema_for_slot(right_slot, "r_k");
        let join_scope_schema = {
            let schema = Schema::new(vec![
                Field::new("l_k", DataType::Int32, true),
                Field::new("r_k", DataType::Int32, true),
            ]);
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[left_slot, right_slot])
                .expect("chunk schema")
        };

        let mut root = ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(scan_node_with_slot(101, left_slot, "l_k")),
                right: Box::new(scan_node_with_slot(202, right_slot, "r_k")),
                node_id: 10,
                join_type: JoinType::RightSemi,
                distribution_mode: JoinDistributionMode::Broadcast,
                left_chunk_schema: left_schema,
                right_chunk_schema: right_schema,
                join_scope_chunk_schema: join_scope_schema,
                probe_keys: vec![right_key],
                build_keys: vec![left_key],
                eq_null_safe: vec![false],
                residual_predicate: None,
                runtime_filter_execution: JoinRuntimeFilterExecution::Compat {
                    legacy_specs: vec![CompatJoinRuntimeFilterSpec {
                        filter_id: 7,
                        expr_order: 0,
                        probe_expr_id: right_key,
                        build_expr_id: left_key,
                        probe_slot_id: right_slot,
                        build_data_type: DataType::Int32,
                        merge_nodes: Vec::new(),
                        has_remote_targets: false,
                    }],
                },
            }),
        };

        push_down_local_runtime_filters(&mut root, &arena);

        let ExecNodeKind::Join(join) = &root.kind else {
            panic!("expected join node");
        };
        assert!(
            scan_runtime_filter_ids(&join.left).is_empty(),
            "left existence-only child must not receive the RightSemi RF probe"
        );
        assert_eq!(
            scan_runtime_filter_ids(&join.right),
            vec![7],
            "right preserved child must receive the RightSemi RF probe"
        );
    }

    #[test]
    fn runtime_filter_pushdown_accepts_probe_slot_target() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(9)), DataType::Int32);
        let specs = vec![RuntimeFilterPushSpec {
            filter_id: 1,
            expr_id,
            slot_id: SlotId::new(3),
            build_node_id: None,
        }];
        let output_slots = HashSet::from([SlotId::new(3)]);

        let filtered = filter_specs_by_output_slots(&arena, &specs, &output_slots);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].filter_id, 1);
    }

    #[test]
    fn runtime_filter_pushdown_rejects_unrelated_slot_target() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(9)), DataType::Int32);
        let specs = vec![RuntimeFilterPushSpec {
            filter_id: 1,
            expr_id,
            slot_id: SlotId::new(8),
            build_node_id: None,
        }];
        let output_slots = HashSet::from([SlotId::new(3)]);

        let filtered = filter_specs_by_output_slots(&arena, &specs, &output_slots);

        assert!(filtered.is_empty());
    }

    #[cfg(feature = "compat")]
    #[test]
    fn runtime_filter_pushdown_traverses_project_to_join() {
        let mut arena = ExprArena::default();
        let probe_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let build_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(8)), DataType::Int32);
        let probe_schema = int_schema_for_slot(SlotId::new(3), "probe_key");
        let build_schema = int_schema_for_slot(SlotId::new(8), "build_key");
        let scan =
            ScanNode::new(Arc::new(DummyScanOp)).with_output_chunk_schema(probe_schema.clone());
        let build =
            ScanNode::new(Arc::new(DummyScanOp)).with_output_chunk_schema(build_schema.clone());
        let join = ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(ExecNode {
                    kind: ExecNodeKind::Scan(scan),
                }),
                right: Box::new(ExecNode {
                    kind: ExecNodeKind::Scan(build),
                }),
                node_id: 4,
                join_type: JoinType::Inner,
                distribution_mode: JoinDistributionMode::Broadcast,
                left_chunk_schema: probe_schema.clone(),
                right_chunk_schema: build_schema.clone(),
                join_scope_chunk_schema: probe_schema.clone(),
                probe_keys: vec![probe_expr],
                build_keys: vec![build_expr],
                eq_null_safe: vec![false],
                residual_predicate: None,
                runtime_filter_execution: JoinRuntimeFilterExecution::Compat {
                    legacy_specs: vec![CompatJoinRuntimeFilterSpec {
                        filter_id: 7,
                        expr_order: 0,
                        probe_expr_id: probe_expr,
                        build_expr_id: build_expr,
                        probe_slot_id: SlotId::new(3),
                        build_data_type: DataType::Int32,
                        merge_nodes: Vec::new(),
                        has_remote_targets: false,
                    }],
                },
            }),
        };
        let mut root = ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(join),
                node_id: 5,
                is_subordinate: false,
                exprs: vec![probe_expr],
                expr_slot_ids: vec![SlotId::new(3)],
                expr_slot_schemas: None,
                output_indices: None,
                output_chunk_schema: probe_schema,
            }),
        };

        push_down_local_runtime_filters(&mut root, &arena);

        let ExecNodeKind::Project(project) = root.kind else {
            panic!("expected project root");
        };
        let ExecNodeKind::Join(join) = project.input.kind else {
            panic!("expected join child");
        };
        let ExecNodeKind::Scan(scan) = join.left.kind else {
            panic!("expected probe scan");
        };
        let specs = scan.runtime_filter_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].filter_id, 7);
        assert_eq!(specs[0].slot_id, SlotId::new(3));
        assert_eq!(specs[0].expr_id, probe_expr);
        assert_eq!(scan.local_rf_waiting_set(), &[4]);
    }
}
