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

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::runtime_filter::model::contract::{BindingId, ChannelId, ConsumerActivation};
use crate::sql::planner::distributed::{
    FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind, PartitionKind,
    RuntimeFilterJoinProgressCatalog, RuntimeFilterJoinProgressCertificate,
};

use super::DeploymentError;

/// Query-level fragment execution dependency graph.
///
/// Data flows `source -> target` along a `FragmentEdge`, so `target` depends on
/// `source`. `deps[f]` is the TRANSITIVE closure of fragments that `f` depends
/// on (must complete/produce before `f`). Cyclic input is rejected at build.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExecutionDependencyGraph {
    deps: BTreeMap<FragmentId, BTreeSet<FragmentId>>,
}

impl ExecutionDependencyGraph {
    pub(crate) fn from_fragment_edges(edges: &[FragmentEdge]) -> Result<Self, String> {
        // Direct predecessors: `target` depends on `source`.
        let mut direct: BTreeMap<FragmentId, BTreeSet<FragmentId>> = BTreeMap::new();
        let mut nodes: BTreeSet<FragmentId> = BTreeSet::new();
        for e in edges {
            nodes.insert(e.source_fragment_id);
            nodes.insert(e.target_fragment_id);
            direct
                .entry(e.target_fragment_id)
                .or_default()
                .insert(e.source_fragment_id);
        }

        // Kahn's algorithm over the depends-on edges to reject cycles.
        let mut in_degree: BTreeMap<FragmentId, usize> =
            nodes.iter().map(|n| (*n, 0usize)).collect();
        for (dependent, preds) in &direct {
            *in_degree.get_mut(dependent).expect("node tracked") += preds.len();
        }
        // adjacency: source -> fragments that depend on it
        let mut dependents: BTreeMap<FragmentId, Vec<FragmentId>> = BTreeMap::new();
        for (dependent, preds) in &direct {
            for p in preds {
                dependents.entry(*p).or_default().push(*dependent);
            }
        }
        let mut queue: VecDeque<FragmentId> = in_degree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(n, _)| *n)
            .collect();
        let mut visited = 0usize;
        while let Some(n) = queue.pop_front() {
            visited += 1;
            if let Some(children) = dependents.get(&n) {
                for c in children {
                    let d = in_degree.get_mut(c).expect("node tracked");
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(*c);
                    }
                }
            }
        }
        if visited != nodes.len() {
            return Err("cycle detected in fragment dependency graph".to_string());
        }

        // Transitive closure via BFS over direct predecessors. One BFS per
        // fragment: ~O(V*(V+E)), acceptable since per-query fragment counts
        // are small (tens).
        let mut deps: BTreeMap<FragmentId, BTreeSet<FragmentId>> = BTreeMap::new();
        for start in &nodes {
            let mut closure: BTreeSet<FragmentId> = BTreeSet::new();
            let mut frontier: VecDeque<FragmentId> = VecDeque::new();
            if let Some(ps) = direct.get(start) {
                frontier.extend(ps.iter().copied());
            }
            while let Some(f) = frontier.pop_front() {
                if closure.insert(f)
                    && let Some(ps) = direct.get(&f)
                {
                    frontier.extend(ps.iter().copied());
                }
            }
            deps.insert(*start, closure);
        }
        Ok(Self { deps })
    }

    /// True iff fragment `a` (transitively) depends on fragment `b`.
    pub(crate) fn reaches(&self, a: FragmentId, b: FragmentId) -> bool {
        self.deps.get(&a).is_some_and(|s| s.contains(&b))
    }
}

/// One consumer binding's wait-for input, projected from the runtime-filter
/// graph model (channels/bindings) by the compiler and validated against the
/// `ExecutionDependencyGraph`.
#[derive(Clone, Debug)]
pub(crate) struct ProducerWaitInput {
    pub binding: BindingId,
    pub fragment: FragmentId,
}

#[derive(Clone, Debug)]
pub(crate) struct ConsumerWaitInput {
    pub channel: ChannelId,
    pub binding: BindingId,
    /// Fragment the consumer executes on.
    pub consumer_fragment: FragmentId,
    pub activation: ConsumerActivation,
    /// Exact producer bindings and the fragments on which they execute.
    pub producers: Vec<ProducerWaitInput>,
}

fn exact_partitioned_hash_edge(
    edges: &[FragmentEdge],
    source: FragmentId,
    target: FragmentId,
    target_exchange_node: i32,
) -> bool {
    edges.iter().any(|edge| {
        edge.source_fragment_id == source
            && edge.target_fragment_id == target
            && edge.target_exchange_node_id == target_exchange_node
            && matches!(edge.output_partition.kind, PartitionKind::Hash)
            && edge.stream_kind == FragmentStreamKind::Partitioned
            && edge.edge_kind == FragmentEdgeKind::Stream
    })
}

fn certifies_join_build_progress(
    deps: &ExecutionDependencyGraph,
    edges: &[FragmentEdge],
    consumer_fragment: FragmentId,
    certificate: RuntimeFilterJoinProgressCertificate,
) -> bool {
    let producer_fragment = certificate.producer_fragment;
    let probe_input = certificate.probe_input_fragment;
    let build_input = certificate.build_input_fragment;

    exact_partitioned_hash_edge(
        edges,
        probe_input,
        producer_fragment,
        certificate.probe_target_exchange_node,
    ) && exact_partitioned_hash_edge(
        edges,
        build_input,
        producer_fragment,
        certificate.build_target_exchange_node,
    ) && deps.reaches(producer_fragment, probe_input)
        && deps.reaches(producer_fragment, build_input)
        && (consumer_fragment == probe_input || deps.reaches(probe_input, consumer_fragment))
        && build_input != consumer_fragment
        && !deps.reaches(build_input, consumer_fragment)
        && !deps.reaches(consumer_fragment, build_input)
}

/// Reject any `BlockingSnapshot` consumer whose wait edge closes an execution
/// cycle: a producer fragment that transitively depends on the consumer fragment.
/// `NonBlockingLive` consumers add no wait edge and always pass.
pub(crate) fn validate_wait_for(
    deps: &ExecutionDependencyGraph,
    edges: &[FragmentEdge],
    consumers: &[ConsumerWaitInput],
    join_progress: &RuntimeFilterJoinProgressCatalog,
) -> Result<(), DeploymentError> {
    for c in consumers {
        if c.activation != ConsumerActivation::BlockingSnapshot {
            continue;
        }
        for producer in &c.producers {
            // Blocking wait: consumer waits for the producer's first version. If
            // the producer fragment depends (transitively) on the consumer
            // fragment, the producer can't run until the consumer does, but the
            // consumer is blocked on the producer → cycle.
            if deps.reaches(producer.fragment, c.consumer_fragment) {
                let certificate = join_progress
                    .get(&(c.channel, producer.binding, producer.fragment))
                    .copied();
                if !certificate.is_some_and(|certificate| {
                    certificate.channel == c.channel
                        && certificate.producer_binding == producer.binding
                        && certificate.producer_fragment == producer.fragment
                        && certifies_join_build_progress(
                            deps,
                            edges,
                            c.consumer_fragment,
                            certificate,
                        )
                }) {
                    return Err(DeploymentError::BlockingFeedbackCycle {
                        channel: c.channel,
                        binding: c.binding,
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::DeploymentError;
    use super::*;
    use crate::runtime_filter::model::contract::{ConsumerActivation, LateApplyGranularity};
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, PartitionKind,
    };

    pub(super) fn edge(source: u32, target: u32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: 0,
            output_partition: DataPartition {
                kind: PartitionKind::Unpartitioned,
                exprs: Vec::new(),
            },
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    fn partitioned_edge(source: u32, target: u32, exchange_node: i32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: exchange_node,
            output_partition: DataPartition {
                kind: PartitionKind::Hash,
                exprs: Vec::new(),
            },
            stream_kind: FragmentStreamKind::Partitioned,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    fn join_certificate() -> RuntimeFilterJoinProgressCertificate {
        RuntimeFilterJoinProgressCertificate {
            channel: ChannelId::new(7),
            producer_binding: BindingId::new(100),
            producer_fragment: 1,
            probe_input_fragment: 2,
            probe_target_exchange_node: 20,
            build_input_fragment: 3,
            build_target_exchange_node: 30,
        }
    }

    fn wait(
        binding: u32,
        consumer_frag: u32,
        activation: ConsumerActivation,
        producers: Vec<u32>,
    ) -> ConsumerWaitInput {
        ConsumerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(binding),
            consumer_fragment: consumer_frag,
            activation,
            producers: producers
                .into_iter()
                .enumerate()
                .map(|(index, fragment)| ProducerWaitInput {
                    binding: BindingId::new(100 + u32::try_from(index).unwrap()),
                    fragment,
                })
                .collect(),
        }
    }

    fn validate(
        deps: &ExecutionDependencyGraph,
        edges: &[FragmentEdge],
        consumers: &[ConsumerWaitInput],
    ) -> Result<(), DeploymentError> {
        validate_wait_for(deps, edges, consumers, &Default::default())
    }

    #[test]
    fn reachability_is_transitive_and_acyclic_chain_ok() {
        // data flow 3 -> 2 -> 1 (leaf 3 feeds 2 feeds root 1); target depends on source.
        let g = ExecutionDependencyGraph::from_fragment_edges(&[edge(3, 2), edge(2, 1)]).unwrap();
        assert!(g.reaches(1, 2));
        assert!(g.reaches(1, 3)); // transitive
        assert!(g.reaches(2, 3));
        assert!(!g.reaches(3, 1));
        assert!(!g.reaches(2, 1));
    }

    #[test]
    fn cycle_is_rejected() {
        let err = ExecutionDependencyGraph::from_fragment_edges(&[edge(1, 2), edge(2, 1)]);
        assert!(err.is_err());
    }

    #[test]
    fn reachability_dedups_diamond_paths() {
        // D(4) feeds both B(2) and C(3); both feed A(1). A reaches D via two paths.
        let g = ExecutionDependencyGraph::from_fragment_edges(&[
            edge(4, 2),
            edge(4, 3),
            edge(2, 1),
            edge(3, 1),
        ])
        .unwrap();
        assert!(g.reaches(1, 2));
        assert!(g.reaches(1, 3));
        assert!(g.reaches(1, 4)); // transitive via both paths, deduped
        assert!(!g.reaches(4, 1));
    }

    #[test]
    fn empty_edges_yields_empty_graph() {
        let g = ExecutionDependencyGraph::from_fragment_edges(&[]).unwrap();
        assert!(!g.reaches(1, 1));
    }

    #[test]
    fn blocking_consumer_upstream_of_producer_is_a_cycle() {
        // scan(2) -> topn(1): producer topn(1) depends on consumer scan(2).
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let err = validate(&deps, &[edge(2, 1)], &[c]).unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }

    #[test]
    fn non_blocking_same_shape_is_allowed() {
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(
            10,
            2,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Split,
            },
            vec![1],
        );
        assert!(validate(&deps, &[edge(2, 1)], &[c]).is_ok());
    }

    #[test]
    fn blocking_consumer_downstream_of_producer_is_fine() {
        // build(2) -> probe(1): consumer probe(1) depends on producer build(2). No cycle.
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(10, 1, ConsumerActivation::BlockingSnapshot, vec![2]);
        assert!(validate(&deps, &[edge(2, 1)], &[c]).is_ok());
    }

    #[test]
    fn blocking_consumer_with_one_cyclic_producer_among_many_is_rejected() {
        // consumer on fragment 2; producers on fragment 9 (unrelated, not in the
        // graph) and fragment 1 (depends on 2 via edge(2,1) → closes a cycle).
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![9, 1]);
        let err = validate(&deps, &[edge(2, 1)], &[c]).unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }

    #[test]
    fn partitioned_join_progress_certificate_allows_probe_side_wait_cycle() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let certificate = join_certificate();
        let catalog = BTreeMap::from([(
            (
                certificate.channel,
                certificate.producer_binding,
                certificate.producer_fragment,
            ),
            certificate,
        )]);

        assert!(validate_wait_for(&deps, &edges, &[consumer], &catalog).is_ok());
    }

    #[test]
    fn forged_join_progress_certificate_is_rejected() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let mut certificate = join_certificate();
        certificate.build_target_exchange_node = 31;
        let catalog = BTreeMap::from([(
            (
                certificate.channel,
                certificate.producer_binding,
                certificate.producer_fragment,
            ),
            certificate,
        )]);

        assert!(matches!(
            validate_wait_for(&deps, &edges, &[consumer], &catalog),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn join_build_source_reverse_dependency_rejects_certificate() {
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            edge(2, 3),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let certificate = join_certificate();
        let catalog = BTreeMap::from([(
            (
                certificate.channel,
                certificate.producer_binding,
                certificate.producer_fragment,
            ),
            certificate,
        )]);

        assert!(matches!(
            validate_wait_for(&deps, &edges, &[consumer], &catalog),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }
}
