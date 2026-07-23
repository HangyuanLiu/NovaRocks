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

use std::collections::BTreeMap;

use crate::runtime_filter::model::contract::{BindingId, ChannelId, CompletionRequirement};
use crate::runtime_filter::model::graph::{RuntimeFilterBindingRole, RuntimeFilterGraph};
use crate::sql::planner::physical::JoinDistribution;
use crate::sql::planner::physical::runtime_filter::JoinExecutionMode;
use crate::sql::planner::physical::runtime_filter_placement::rf_sides_for_join;

use super::{
    DistributedNode, DistributedNodeKind, FragmentEdge, FragmentEdgeKind, FragmentId,
    FragmentStreamKind, PartitionKind, PlanFragment,
};

/// Planner-sealed proof that one partitioned hash-join producer can complete
/// its build phase without consuming the probe-side fragment on which a
/// blocking runtime-filter consumer runs.
///
/// This is deliberately structural. The deployment compiler revalidates every
/// field against the exact sealed edge set and its execution-dependency graph
/// before it may exempt a coarse fragment-level wait-for cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterJoinProgressCertificate {
    pub(crate) channel: ChannelId,
    pub(crate) producer_binding: BindingId,
    pub(crate) producer_fragment: FragmentId,
    pub(crate) probe_input_fragment: FragmentId,
    pub(crate) probe_target_exchange_node: i32,
    pub(crate) build_input_fragment: FragmentId,
    pub(crate) build_target_exchange_node: i32,
}

pub(crate) type RuntimeFilterJoinProgressCatalog =
    BTreeMap<(ChannelId, BindingId, FragmentId), RuntimeFilterJoinProgressCertificate>;

fn nearest_unary_exchange(node: &DistributedNode) -> Option<&DistributedNode> {
    if matches!(node.payload, DistributedNodeKind::Exchange(_)) {
        return Some(node);
    }
    match node.children.as_slice() {
        [child] => nearest_unary_exchange(child),
        _ => None,
    }
}

fn exact_partitioned_hash_edge<'a>(
    edges: &'a [FragmentEdge],
    producer_fragment: FragmentId,
    exchange_node: &DistributedNode,
) -> Option<&'a FragmentEdge> {
    let DistributedNodeKind::Exchange(exchange) = &exchange_node.payload else {
        return None;
    };
    edges.iter().find(|edge| {
        edge.source_fragment_id == exchange.source_fragment_id
            && edge.target_fragment_id == producer_fragment
            && edge.target_exchange_node_id == exchange_node.node_id
            && matches!(edge.output_partition.kind, PartitionKind::Hash)
            && edge.stream_kind == FragmentStreamKind::Partitioned
            && edge.edge_kind == FragmentEdgeKind::Stream
    })
}

fn visit(
    node: &DistributedNode,
    edges: &[FragmentEdge],
    graph: &RuntimeFilterGraph,
    catalog: &mut RuntimeFilterJoinProgressCatalog,
) {
    if let DistributedNodeKind::HashJoin(join) = &node.payload
        && join.execution_mode == Some(JoinExecutionMode::Partitioned)
        && join.distribution == JoinDistribution::Shuffle
        && let Some(sides) = rf_sides_for_join(join.join_type)
        && let (Some(probe_child), Some(build_child)) = (
            node.children.get(sides.probe_child),
            node.children.get(sides.build_child),
        )
        && let (Some(probe_exchange), Some(build_exchange)) = (
            nearest_unary_exchange(probe_child),
            nearest_unary_exchange(build_child),
        )
        && let (Some(probe_edge), Some(build_edge)) = (
            exact_partitioned_hash_edge(edges, node.fragment_id, probe_exchange),
            exact_partitioned_hash_edge(edges, node.fragment_id, build_exchange),
        )
        && probe_edge.source_fragment_id != build_edge.source_fragment_id
    {
        for binding_id in &node.runtime_filter_binding_ids {
            let Some(binding) = graph.binding(*binding_id) else {
                continue;
            };
            let RuntimeFilterBindingRole::Producer(requirement) = &binding.role else {
                continue;
            };
            if requirement.completion_requirement != CompletionRequirement::ProducerClosed {
                continue;
            }
            let certificate = RuntimeFilterJoinProgressCertificate {
                channel: binding.channel_id,
                producer_binding: binding.binding_id,
                producer_fragment: node.fragment_id,
                probe_input_fragment: probe_edge.source_fragment_id,
                probe_target_exchange_node: probe_edge.target_exchange_node_id,
                build_input_fragment: build_edge.source_fragment_id,
                build_target_exchange_node: build_edge.target_exchange_node_id,
            };
            catalog.insert(
                (
                    certificate.channel,
                    certificate.producer_binding,
                    certificate.producer_fragment,
                ),
                certificate,
            );
        }
    }
    for child in &node.children {
        visit(child, edges, graph, catalog);
    }
}

pub(super) fn build_runtime_filter_join_progress_catalog(
    fragments: &[PlanFragment],
    edges: &[FragmentEdge],
    graph: &RuntimeFilterGraph,
) -> RuntimeFilterJoinProgressCatalog {
    let mut catalog = RuntimeFilterJoinProgressCatalog::new();
    for fragment in fragments {
        visit(&fragment.root, edges, graph, &mut catalog);
    }
    catalog
}
