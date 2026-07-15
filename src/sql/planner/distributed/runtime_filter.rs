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

//! Read-only projection of the query-global runtime-filter graph.

use std::collections::{BTreeMap, BTreeSet};

use crate::runtime_filter::model::contract::{
    ChannelId, ContributionKind, PlanFragmentId, PlanNodeId,
};
use crate::runtime_filter::model::graph::{RuntimeFilterBindingRole, RuntimeFilterBindingSpec};
use crate::sql::analysis::expr_display::typed_expr_display_name;
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::physical::{JoinDistribution, JoinExecutionMode};

use super::{DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId};

#[derive(Clone, Debug)]
pub(crate) struct GraphRuntimeFilterBuild {
    pub filter_id: i32,
    pub channel_id: ChannelId,
    pub build_expr: TypedExpr,
    pub probe_expr: TypedExpr,
    pub expr_order: usize,
    pub execution_mode: JoinExecutionMode,
    pub source_fragment_id: FragmentId,
    pub target_fragment_ids: Vec<FragmentId>,
}

#[derive(Clone, Debug)]
pub(crate) struct GraphRuntimeFilterProbe {
    pub filter_id: i32,
    pub channel_id: ChannelId,
    pub probe_expr: TypedExpr,
    pub source_fragment_id: FragmentId,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeFilterGraphProjection {
    builds_by_node: BTreeMap<(FragmentId, i32), Vec<GraphRuntimeFilterBuild>>,
    probes_by_node: BTreeMap<(FragmentId, i32), Vec<GraphRuntimeFilterProbe>>,
}

impl RuntimeFilterGraphProjection {
    pub(crate) fn builds_for(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> &[GraphRuntimeFilterBuild] {
        self.builds_by_node
            .get(&(fragment_id, node_id))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn builds_for_node(&self, node_id: i32) -> &[GraphRuntimeFilterBuild] {
        self.builds_by_node
            .iter()
            .find_map(|((_, candidate_node_id), builds)| {
                (*candidate_node_id == node_id).then_some(builds.as_slice())
            })
            .unwrap_or_default()
    }

    pub(crate) fn probes_for(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> &[GraphRuntimeFilterProbe] {
        self.probes_by_node
            .get(&(fragment_id, node_id))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn builds(
        &self,
    ) -> impl Iterator<Item = (&(FragmentId, i32), &Vec<GraphRuntimeFilterBuild>)> {
        self.builds_by_node.iter()
    }

    pub(crate) fn probes(
        &self,
    ) -> impl Iterator<Item = (&(FragmentId, i32), &Vec<GraphRuntimeFilterProbe>)> {
        self.probes_by_node.iter()
    }
}

pub(crate) fn project_runtime_filters(
    plan: &DistributedPlan,
) -> Result<RuntimeFilterGraphProjection, String> {
    let graph = plan.runtime_filter_graph();
    let mut projection = RuntimeFilterGraphProjection::default();
    let mut assigned_join_keys = BTreeMap::<(FragmentId, i32), BTreeSet<usize>>::new();
    for channel in graph.channels() {
        if !channel
            .allowed_contribution_kinds
            .contains(&ContributionKind::ValueDomainDelta)
        {
            continue;
        }
        let filter_id = i32::try_from(channel.channel_id.get()).map_err(|_| {
            format!(
                "runtime filter channel {:?} does not fit native filter id",
                channel.channel_id
            )
        })?;
        let bindings = graph
            .bindings()
            .filter(|binding| binding.channel_id == channel.channel_id)
            .collect::<Vec<_>>();
        let producers = bindings
            .iter()
            .copied()
            .filter(|binding| matches!(binding.role, RuntimeFilterBindingRole::Producer(_)))
            .collect::<Vec<_>>();
        let consumers = bindings
            .iter()
            .copied()
            .filter(|binding| matches!(binding.role, RuntimeFilterBindingRole::Consumer(_)))
            .collect::<Vec<_>>();
        let [producer] = producers.as_slice() else {
            return Err(format!(
                "runtime filter channel {:?} requires exactly one native Join producer, found {}",
                channel.channel_id,
                producers.len()
            ));
        };
        let producer_node = find_node(
            plan,
            producer.location.fragment_id,
            producer.location.node_id,
        )?;
        let producer_node_key = (
            producer.location.fragment_id.get(),
            producer.location.node_id.get(),
        );
        let (expr_order, execution_mode, probe_expr) = join_projection_metadata(
            producer_node,
            producer,
            assigned_join_keys.entry(producer_node_key).or_default(),
        )?;
        let mut target_fragment_ids = consumers
            .iter()
            .map(|binding| binding.location.fragment_id.get())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        target_fragment_ids.sort_unstable();
        projection
            .builds_by_node
            .entry((
                producer.location.fragment_id.get(),
                producer.location.node_id.get(),
            ))
            .or_default()
            .push(GraphRuntimeFilterBuild {
                filter_id,
                channel_id: channel.channel_id,
                build_expr: producer.expression.clone(),
                probe_expr,
                expr_order,
                execution_mode,
                source_fragment_id: producer.location.fragment_id.get(),
                target_fragment_ids,
            });
        for consumer in consumers {
            projection
                .probes_by_node
                .entry((
                    consumer.location.fragment_id.get(),
                    consumer.location.node_id.get(),
                ))
                .or_default()
                .push(GraphRuntimeFilterProbe {
                    filter_id,
                    channel_id: channel.channel_id,
                    probe_expr: consumer.expression.clone(),
                    source_fragment_id: producer.location.fragment_id.get(),
                });
        }
    }
    Ok(projection)
}

fn find_node(
    plan: &DistributedPlan,
    fragment_id: PlanFragmentId,
    node_id: PlanNodeId,
) -> Result<&DistributedNode, String> {
    fn visit(node: &DistributedNode, node_id: i32) -> Option<&DistributedNode> {
        if node.node_id == node_id {
            return Some(node);
        }
        node.children.iter().find_map(|child| visit(child, node_id))
    }
    let fragment = plan
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == fragment_id.get())
        .ok_or_else(|| {
            format!(
                "runtime filter binding references missing fragment {}",
                fragment_id.get()
            )
        })?;
    visit(&fragment.root, node_id.get()).ok_or_else(|| {
        format!(
            "runtime filter binding references missing node {} in fragment {}",
            node_id.get(),
            fragment_id.get()
        )
    })
}

fn join_projection_metadata(
    node: &DistributedNode,
    producer: &RuntimeFilterBindingSpec,
    assigned_join_keys: &mut BTreeSet<usize>,
) -> Result<(usize, JoinExecutionMode, TypedExpr), String> {
    let DistributedNodeKind::HashJoin(join) = &node.payload else {
        return Err(format!(
            "runtime filter producer binding {:?} is not attached to HashJoin",
            producer.binding_id
        ));
    };
    let matched = join
        .eq_conditions
        .iter()
        .enumerate()
        .find_map(|(index, condition)| {
            if condition.null_safe
                || condition.left.data_type != condition.right.data_type
                || assigned_join_keys.contains(&index)
            {
                return None;
            }
            if expressions_are_exactly_equivalent(&condition.left, &producer.expression) {
                Some((index, condition.right.clone()))
            } else if expressions_are_exactly_equivalent(&condition.right, &producer.expression) {
                Some((index, condition.left.clone()))
            } else {
                None
            }
        });
    let Some((expr_order, probe_expr)) = matched else {
        return Err(format!(
            "runtime filter producer binding {:?} has no unassigned exact HashJoin key",
            producer.binding_id
        ));
    };
    assigned_join_keys.insert(expr_order);
    let execution_mode = join.execution_mode.unwrap_or(match join.distribution {
        JoinDistribution::Broadcast => JoinExecutionMode::Broadcast,
        JoinDistribution::Shuffle => JoinExecutionMode::Partitioned,
        JoinDistribution::Colocate => JoinExecutionMode::Colocate,
        JoinDistribution::Unknown => {
            return Err(format!(
                "runtime filter producer binding {:?} has unknown Join execution mode",
                producer.binding_id
            ));
        }
    });
    Ok((expr_order, execution_mode, probe_expr))
}

fn expressions_are_exactly_equivalent(left: &TypedExpr, right: &TypedExpr) -> bool {
    left.data_type == right.data_type
        && left.nullable == right.nullable
        && expression_column_ids(left) == expression_column_ids(right)
        && typed_expr_display_name(left) == typed_expr_display_name(right)
}

fn expression_column_ids(expr: &TypedExpr) -> Vec<ColumnId> {
    fn collect(expr: &TypedExpr, out: &mut Vec<ColumnId>) {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => out.push(*column_id),
            ExprKind::BinaryOp { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr) => collect(expr, out),
            ExprKind::FunctionCall { args, .. } => {
                for arg in args {
                    collect(arg, out);
                }
            }
            ExprKind::LambdaFunction { body, .. } => collect(body, out),
            ExprKind::AggregateCall { args, order_by, .. } => {
                for arg in args {
                    collect(arg, out);
                }
                for item in order_by {
                    collect(&item.expr, out);
                }
            }
            ExprKind::InList { expr, list, .. } => {
                collect(expr, out);
                for item in list {
                    collect(item, out);
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                collect(expr, out);
                collect(low, out);
                collect(high, out);
            }
            ExprKind::Like { expr, pattern, .. } => {
                collect(expr, out);
                collect(pattern, out);
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    collect(operand, out);
                }
                for (when, then) in when_then {
                    collect(when, out);
                    collect(then, out);
                }
                if let Some(else_expr) = else_expr {
                    collect(else_expr, out);
                }
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    collect(arg, out);
                }
                for expr in partition_by {
                    collect(expr, out);
                }
                for item in order_by {
                    collect(&item.expr, out);
                }
            }
            ExprKind::Lambda { body, .. } => collect(body, out),
            ExprKind::Literal(_)
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::SubqueryPlaceholder { .. } => {}
        }
    }
    let mut ids = Vec::new();
    collect(expr, &mut ids);
    ids.sort_unstable();
    ids.dedup();
    ids
}
