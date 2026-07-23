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
    FragmentEdge, FragmentId, JoinBuildProgressCatalog, JoinBuildProgressProof,
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

/// Why a sealed proof failed deployment revalidation. Rendered into the cycle
/// error so the operator sees which proof rule was left unsatisfied
/// (spec contract #9).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProofRejection {
    /// Duplicate entries or frontier/non-build overlap.
    OverlappingPartition,
    /// The two sets do not exactly cover the fragment's sealed in-edges
    /// (missing, extra, or forged edges).
    PartitionMismatch,
    /// Placement sanity: the consumer is neither fragment-local nor on/under
    /// any non-build input.
    ConsumerOutsideProbeRegion,
}

/// Deployment-side revalidation of one planner-sealed proof against the exact
/// sealed edge set. Checks: (1) every claimed edge exists; (2) the two sets
/// form an exact, disjoint partition of the producer fragment's in-edges;
/// (3) placement sanity: the consumer sits on/under some non-build input, or
/// is fragment-local to the producer. Any failure means the proof is stale or
/// forged; the caller keeps the coarse wait edge.
fn revalidate_proof(
    proof: &JoinBuildProgressProof,
    edges: &[FragmentEdge],
    deps: &ExecutionDependencyGraph,
    consumer_fragment: FragmentId,
) -> Result<(), ProofRejection> {
    let sealed_in_edges: BTreeSet<(FragmentId, i32)> = edges
        .iter()
        .filter(|e| e.target_fragment_id == proof.producer_fragment)
        .map(|e| (e.source_fragment_id, e.target_exchange_node_id))
        .collect();
    let frontier: BTreeSet<(FragmentId, i32)> = proof
        .build_frontier
        .iter()
        .map(|f| (f.source_fragment, f.target_exchange_node))
        .collect();
    let non_build: BTreeSet<(FragmentId, i32)> = proof
        .non_build_inputs
        .iter()
        .map(|f| (f.source_fragment, f.target_exchange_node))
        .collect();
    // Duplicates inside either list, or overlap across them, break the
    // partition (len check catches intra-list duplicates).
    if frontier.len() != proof.build_frontier.len()
        || non_build.len() != proof.non_build_inputs.len()
        || !frontier.is_disjoint(&non_build)
    {
        return Err(ProofRejection::OverlappingPartition);
    }
    let union: BTreeSet<(FragmentId, i32)> = frontier.union(&non_build).copied().collect();
    if union != sealed_in_edges {
        return Err(ProofRejection::PartitionMismatch);
    }
    // Placement sanity: fragment-local consumer, or consumer on/under a
    // non-build input's source fragment.
    let sane = consumer_fragment == proof.producer_fragment
        || non_build.iter().any(|(source, _)| {
            *source == consumer_fragment || deps.reaches(*source, consumer_fragment)
        });
    if sane {
        Ok(())
    } else {
        Err(ProofRejection::ConsumerOutsideProbeRegion)
    }
}

/// Reject any `BlockingSnapshot` consumer whose wait edge closes an execution
/// cycle: a producer fragment that transitively depends on the consumer fragment.
/// `NonBlockingLive` consumers add no wait edge and always pass.
pub(crate) fn validate_wait_for(
    deps: &ExecutionDependencyGraph,
    _edges: &[FragmentEdge],
    consumers: &[ConsumerWaitInput],
    _join_progress: &JoinBuildProgressCatalog,
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
                return Err(DeploymentError::BlockingFeedbackCycle {
                    channel: c.channel,
                    binding: c.binding,
                    cycle: Vec::new(),
                });
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
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, FrontierEdge,
        PartitionKind,
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

    fn proof(
        producer_fragment: u32,
        join_node: i32,
        frontier: Vec<(u32, i32)>,
        non_build: Vec<(u32, i32)>,
    ) -> JoinBuildProgressProof {
        let fe = |(s, x): &(u32, i32)| FrontierEdge {
            source_fragment: *s,
            target_exchange_node: *x,
        };
        JoinBuildProgressProof {
            channel: ChannelId::new(7),
            producer_binding: BindingId::new(100),
            producer_fragment,
            join_node_id: join_node,
            build_frontier: frontier.iter().map(fe).collect(),
            non_build_inputs: non_build.iter().map(fe).collect(),
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
    fn revalidation_accepts_exact_partition() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        assert_eq!(revalidate_proof(&p, &edges, &deps, 2), Ok(()));
    }

    #[test]
    fn revalidation_rejects_forged_exchange_node() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 31)], vec![(2, 20)]); // 31 not sealed
        assert_eq!(
            revalidate_proof(&p, &edges, &deps, 2),
            Err(ProofRejection::PartitionMismatch)
        );
    }

    #[test]
    fn revalidation_rejects_incomplete_partition() {
        // Sealed in-edges of frag 1 are {2->1@20, 3->1@30, 4->1@40}; the proof
        // omits 4->1@40 entirely -> planner missed an input, reject.
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            partitioned_edge(4, 1, 40),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        assert_eq!(
            revalidate_proof(&p, &edges, &deps, 2),
            Err(ProofRejection::PartitionMismatch)
        );
    }

    #[test]
    fn revalidation_rejects_overlapping_partition() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30), (2, 20)], vec![(2, 20)]);
        assert_eq!(
            revalidate_proof(&p, &edges, &deps, 2),
            Err(ProofRejection::OverlappingPartition)
        );
    }

    #[test]
    fn revalidation_sanity_requires_consumer_under_non_build_input() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        // consumer on frag 3 (the build input) violates placement sanity.
        assert_eq!(
            revalidate_proof(&p, &edges, &deps, 3),
            Err(ProofRejection::ConsumerOutsideProbeRegion)
        );
    }

    #[test]
    fn revalidation_allows_same_fragment_local_consumer() {
        // Colocate-style: no in-edges at all, consumer co-located with producer.
        let edges: Vec<FragmentEdge> = vec![];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![], vec![]);
        assert_eq!(revalidate_proof(&p, &edges, &deps, 1), Ok(()));
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
    fn multi_build_frontier_proof_cannot_bypass_blocking_feedback_cycle() {
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            partitioned_edge(4, 1, 40),
            edge(2, 4),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let mut proof = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        proof.build_frontier.push(FrontierEdge {
            source_fragment: 4,
            target_exchange_node: 40,
        });
        let catalog = BTreeMap::from([(
            (
                proof.channel,
                proof.producer_binding,
                proof.producer_fragment,
            ),
            proof,
        )]);

        assert!(matches!(
            validate_wait_for(&deps, &edges, &[consumer], &catalog),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }
}
