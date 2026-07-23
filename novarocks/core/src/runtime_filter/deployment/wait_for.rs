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

/// Refined wait-graph node: a whole fragment, or the build-ready milestone of
/// one hash join inside its fragment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum WaitNode {
    Frag(FragmentId),
    BuildReady {
        fragment: FragmentId,
        join_node: i32,
    },
}

/// Edge provenance, kept for cycle-path rendering.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct WaitProvenance {
    channel: ChannelId,
    consumer_binding: BindingId,
    producer_binding: BindingId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum WaitEdgeKind {
    DataFlow,
    Frontier,
    Wait(WaitProvenance),
    Backpressure {
        wait: WaitProvenance,
        multicast_fragment: FragmentId,
    },
}

type WaitGraph = BTreeMap<WaitNode, BTreeSet<(WaitNode, WaitEdgeKind)>>;

fn ensure_wait_node(node: WaitNode, succ: &mut WaitGraph) {
    succ.entry(node).or_default();
}

fn add_wait_edge(from: WaitNode, to: WaitNode, kind: WaitEdgeKind, succ: &mut WaitGraph) {
    succ.entry(from).or_default().insert((to, kind));
    succ.entry(to).or_default();
}

/// Reject a `BlockingSnapshot` consumer when data-flow, accepted build-ready
/// proofs, waits, and multicast backpressure compose a global execution cycle.
/// Rejected or missing proofs stay fail-closed as coarse fragment wait edges.
pub(crate) fn validate_wait_for(
    deps: &ExecutionDependencyGraph,
    edges: &[FragmentEdge],
    consumers: &[ConsumerWaitInput],
    join_progress: &JoinBuildProgressCatalog,
) -> Result<(), DeploymentError> {
    if !consumers
        .iter()
        .any(|c| c.activation == ConsumerActivation::BlockingSnapshot)
    {
        return Ok(());
    }

    // a -> b means "b depends on a". BTree collections make construction and
    // traversal independent of input order.
    let mut succ = WaitGraph::new();

    // E1: sealed fragment data flow.
    for edge in edges {
        add_wait_edge(
            WaitNode::Frag(edge.source_fragment_id),
            WaitNode::Frag(edge.target_fragment_id),
            WaitEdgeKind::DataFlow,
            &mut succ,
        );
    }

    // Resolve every producer wait independently. An accepted proof redirects
    // the wait source to its build-ready milestone and contributes E2 frontier
    // edges. Any missing or rejected proof preserves the coarse fragment edge.
    let mut accepted_build_ready = BTreeSet::new();
    let mut rejections = BTreeMap::new();
    let mut wait_sources = Vec::with_capacity(consumers.len());
    for consumer in consumers {
        let mut sources = Vec::with_capacity(consumer.producers.len());
        for producer in &consumer.producers {
            let wait = WaitProvenance {
                channel: consumer.channel,
                consumer_binding: consumer.binding,
                producer_binding: producer.binding,
            };
            let source = if consumer.activation == ConsumerActivation::BlockingSnapshot {
                match join_progress.get(&(consumer.channel, producer.binding, producer.fragment)) {
                    Some(proof) => {
                        match revalidate_proof(proof, edges, deps, consumer.consumer_fragment) {
                            Ok(()) => {
                                let build_ready = WaitNode::BuildReady {
                                    fragment: proof.producer_fragment,
                                    join_node: proof.join_node_id,
                                };
                                if accepted_build_ready
                                    .insert((proof.producer_fragment, proof.join_node_id))
                                {
                                    for frontier in &proof.build_frontier {
                                        add_wait_edge(
                                            WaitNode::Frag(frontier.source_fragment),
                                            build_ready,
                                            WaitEdgeKind::Frontier,
                                            &mut succ,
                                        );
                                    }
                                    add_wait_edge(
                                        build_ready,
                                        WaitNode::Frag(proof.producer_fragment),
                                        WaitEdgeKind::Frontier,
                                        &mut succ,
                                    );
                                }
                                build_ready
                            }
                            Err(reason) => {
                                rejections.insert(wait, reason);
                                WaitNode::Frag(producer.fragment)
                            }
                        }
                    }
                    None => WaitNode::Frag(producer.fragment),
                }
            } else {
                WaitNode::Frag(producer.fragment)
            };
            sources.push(source);
        }
        wait_sources.push(sources);
    }

    // E3: only blocking consumers introduce waits.
    for (consumer, sources) in consumers.iter().zip(&wait_sources) {
        if consumer.activation != ConsumerActivation::BlockingSnapshot {
            continue;
        }
        ensure_wait_node(WaitNode::Frag(consumer.consumer_fragment), &mut succ);
        for (producer, source) in consumer.producers.iter().zip(sources) {
            add_wait_edge(
                *source,
                WaitNode::Frag(consumer.consumer_fragment),
                WaitEdgeKind::Wait(WaitProvenance {
                    channel: consumer.channel,
                    consumer_binding: consumer.binding,
                    producer_binding: producer.binding,
                }),
                &mut succ,
            );
        }
    }

    // E4: one blocked branch of a multicast/split/router stalls every branch.
    // Therefore the multicast fragment cannot complete before each blocking
    // wait on or below any branch target is released.
    let mut out_edges: BTreeMap<FragmentId, BTreeSet<(FragmentId, i32)>> = BTreeMap::new();
    for edge in edges {
        out_edges
            .entry(edge.source_fragment_id)
            .or_default()
            .insert((edge.target_fragment_id, edge.target_exchange_node_id));
    }
    for (multicast_fragment, branches) in &out_edges {
        if branches.len() < 2 {
            continue;
        }
        for (target, _) in branches {
            for (consumer, sources) in consumers.iter().zip(&wait_sources) {
                if consumer.activation != ConsumerActivation::BlockingSnapshot {
                    continue;
                }
                let on_branch = consumer.consumer_fragment == *target
                    || deps.reaches(consumer.consumer_fragment, *target);
                if !on_branch {
                    continue;
                }
                for (producer, source) in consumer.producers.iter().zip(sources) {
                    add_wait_edge(
                        *source,
                        WaitNode::Frag(*multicast_fragment),
                        WaitEdgeKind::Backpressure {
                            wait: WaitProvenance {
                                channel: consumer.channel,
                                consumer_binding: consumer.binding,
                                producer_binding: producer.binding,
                            },
                            multicast_fragment: *multicast_fragment,
                        },
                        &mut succ,
                    );
                }
            }
        }
    }

    match find_cycle(&succ) {
        None => Ok(()),
        Some(cycle_edges) => {
            let (channel, binding) = cycle_edges
                .iter()
                .find_map(|(_, kind, _)| match kind {
                    WaitEdgeKind::Wait(wait) | WaitEdgeKind::Backpressure { wait, .. } => {
                        Some((wait.channel, wait.consumer_binding))
                    }
                    WaitEdgeKind::DataFlow | WaitEdgeKind::Frontier => None,
                })
                .expect("a refined-graph cycle must cross a wait or backpressure edge");
            Err(DeploymentError::BlockingFeedbackCycle {
                channel,
                binding,
                cycle: cycle_edges
                    .iter()
                    .map(|step| render_cycle_step(step, &rejections))
                    .collect(),
            })
        }
    }
}

/// Run Kahn's algorithm over the complete refined graph. If nodes remain,
/// follow deterministic residual predecessors until one repeats, then reverse
/// that backward walk into a closed, forward-oriented cycle.
fn find_cycle(succ: &WaitGraph) -> Option<Vec<(WaitNode, WaitEdgeKind, WaitNode)>> {
    let mut in_degree: BTreeMap<WaitNode, usize> = succ.keys().map(|node| (*node, 0)).collect();
    let mut pred: BTreeMap<WaitNode, BTreeSet<(WaitNode, WaitEdgeKind)>> =
        succ.keys().map(|node| (*node, BTreeSet::new())).collect();
    for (from, tos) in succ {
        for (to, kind) in tos {
            *in_degree.get_mut(to).expect("node tracked") += 1;
            pred.get_mut(to)
                .expect("node tracked")
                .insert((*from, *kind));
        }
    }

    let mut queue: VecDeque<WaitNode> = in_degree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(node, _)| *node)
        .collect();
    let mut remaining = in_degree.len();
    while let Some(node) = queue.pop_front() {
        remaining -= 1;
        for (to, _) in succ.get(&node).expect("node tracked") {
            let degree = in_degree.get_mut(to).expect("node tracked");
            *degree -= 1;
            if *degree == 0 {
                queue.push_back(*to);
            }
        }
    }
    if remaining == 0 {
        return None;
    }

    let residual: BTreeSet<WaitNode> = in_degree
        .iter()
        .filter(|(_, degree)| **degree > 0)
        .map(|(node, _)| *node)
        .collect();
    let mut current = *residual.iter().next().expect("nonempty residual");
    let mut backward_path = Vec::new();
    let mut seen = BTreeMap::new();
    loop {
        if let Some(&cycle_start) = seen.get(&current) {
            let mut cycle = backward_path[cycle_start..].to_vec();
            cycle.reverse();
            return Some(cycle);
        }
        seen.insert(current, backward_path.len());
        let (from, kind) = pred
            .get(&current)
            .expect("residual node tracked")
            .iter()
            .find(|(from, _)| residual.contains(from))
            .copied()
            .expect("residual node keeps a residual predecessor");
        backward_path.push((from, kind, current));
        current = from;
    }
}

fn render_wait_node(node: &WaitNode) -> String {
    match node {
        WaitNode::Frag(fragment) => format!("frag {fragment}"),
        WaitNode::BuildReady {
            fragment,
            join_node,
        } => format!("build-ready(frag {fragment}, join {join_node})"),
    }
}

fn render_cycle_step(
    (from, kind, to): &(WaitNode, WaitEdgeKind, WaitNode),
    rejections: &BTreeMap<WaitProvenance, ProofRejection>,
) -> String {
    let label = match kind {
        WaitEdgeKind::DataFlow => "dataflow".to_string(),
        WaitEdgeKind::Frontier => "frontier".to_string(),
        WaitEdgeKind::Wait(wait) => render_wait_provenance("wait", wait, rejections),
        WaitEdgeKind::Backpressure {
            wait,
            multicast_fragment,
            ..
        } => render_wait_provenance(
            &format!("backpressure(frag {multicast_fragment})"),
            wait,
            rejections,
        ),
    };
    format!(
        "{} --{label}--> {}",
        render_wait_node(from),
        render_wait_node(to)
    )
}

fn render_wait_provenance(
    edge: &str,
    wait: &WaitProvenance,
    rejections: &BTreeMap<WaitProvenance, ProofRejection>,
) -> String {
    let rejection = rejections
        .get(wait)
        .map(|reason| format!(" (proof rejected: {reason:?})"))
        .unwrap_or_default();
    format!(
        "{edge} channel={} consumer-binding={} producer-binding={}{rejection}",
        wait.channel.get(),
        wait.consumer_binding.get(),
        wait.producer_binding.get(),
    )
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

    fn catalog(proofs: Vec<JoinBuildProgressProof>) -> JoinBuildProgressCatalog {
        proofs
            .into_iter()
            .map(|p| ((p.channel, p.producer_binding, p.producer_fragment), p))
            .collect()
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
    fn accepted_proof_refines_wait_edge_and_passes() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        assert!(validate_wait_for(&deps, &edges, &[consumer], &c).is_ok());
    }

    #[test]
    fn frontier_depending_on_consumer_is_rejected() {
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            edge(2, 3),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        let err = validate_wait_for(&deps, &edges, &[consumer], &c).unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }

    #[test]
    fn forged_proof_falls_back_to_coarse_edge_and_rejects() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 31)], vec![(2, 20)])]);
        let err = validate_wait_for(&deps, &edges, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { cycle, .. } = err else {
            panic!("expected cycle");
        };
        assert!(
            cycle
                .iter()
                .any(|step| step.contains("proof rejected: PartitionMismatch"))
        );
    }

    #[test]
    fn cycle_provenance_attributes_consumer_and_producer_bindings() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 31)], vec![(2, 20)])]);
        let err = validate_wait_for(&deps, &edges, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle {
            channel,
            binding,
            cycle,
        } = err
        else {
            panic!("expected cycle");
        };
        assert_eq!(channel, ChannelId::new(7));
        assert_eq!(binding, BindingId::new(10));
        let rendered = cycle.join(", ");
        assert!(rendered.contains("channel=7"));
        assert!(rendered.contains("consumer-binding=10"));
        assert!(rendered.contains("producer-binding=100"));
        assert!(rendered.contains("proof rejected: PartitionMismatch"));
    }

    #[test]
    fn two_individually_valid_proofs_composing_a_cycle_are_rejected() {
        let edges = vec![
            partitioned_edge(1, 2, 20),
            partitioned_edge(3, 2, 30),
            edge(4, 3),
            partitioned_edge(4, 5, 50),
            partitioned_edge(6, 5, 60),
            edge(1, 6),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p1 = proof(2, 10, vec![(3, 30)], vec![(1, 20)]);
        let mut p2 = proof(5, 11, vec![(6, 60)], vec![(4, 50)]);
        p2.channel = ChannelId::new(8);
        p2.producer_binding = BindingId::new(200);
        let c1 = ConsumerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(10),
            consumer_fragment: 1,
            activation: ConsumerActivation::BlockingSnapshot,
            producers: vec![ProducerWaitInput {
                binding: BindingId::new(100),
                fragment: 2,
            }],
        };
        let c2 = ConsumerWaitInput {
            channel: ChannelId::new(8),
            binding: BindingId::new(11),
            consumer_fragment: 4,
            activation: ConsumerActivation::BlockingSnapshot,
            producers: vec![ProducerWaitInput {
                binding: BindingId::new(200),
                fragment: 5,
            }],
        };
        let err = validate_wait_for(&deps, &edges, &[c1, c2], &catalog(vec![p1, p2])).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { cycle, .. } = err else {
            panic!("expected cycle");
        };
        assert!(!cycle.is_empty());
    }

    #[test]
    fn multicast_backpressure_edge_closes_cycle() {
        let edges = vec![
            partitioned_edge(1, 4, 21),
            partitioned_edge(1, 3, 22),
            partitioned_edge(4, 2, 20),
            partitioned_edge(3, 5, 24),
            partitioned_edge(5, 2, 23),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 5, ConsumerActivation::BlockingSnapshot, vec![2]);
        let c = catalog(vec![proof(2, 10, vec![(4, 20)], vec![(5, 23)])]);
        let err = validate_wait_for(&deps, &edges, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { binding, cycle, .. } = err else {
            panic!("expected cycle");
        };
        assert_eq!(binding, BindingId::new(10));
        assert!(cycle.iter().any(|step| step.contains(
            "--backpressure(frag 1) channel=7 consumer-binding=10 producer-binding=100-->"
        )));
    }

    #[test]
    fn multicast_branch_without_consumer_adds_no_backpressure_edge() {
        let edges = vec![
            partitioned_edge(1, 4, 21),
            partitioned_edge(1, 3, 22),
            partitioned_edge(4, 2, 20),
            partitioned_edge(5, 2, 23),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 5, ConsumerActivation::BlockingSnapshot, vec![2]);
        let c = catalog(vec![proof(2, 10, vec![(4, 20)], vec![(5, 23)])]);
        assert!(validate_wait_for(&deps, &edges, &[consumer], &c).is_ok());
    }

    #[test]
    fn fragment_local_consumer_passes_with_empty_frontier_proof() {
        let edges: Vec<FragmentEdge> = vec![];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 1, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![], vec![])]);
        assert!(validate_wait_for(&deps, &edges, &[consumer], &c).is_ok());
    }

    #[test]
    fn cycle_detection_is_deterministic_under_input_permutation() {
        let edges_a = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            edge(2, 3),
        ];
        let mut edges_b = edges_a.clone();
        edges_b.reverse();
        let deps_a = ExecutionDependencyGraph::from_fragment_edges(&edges_a).unwrap();
        let deps_b = ExecutionDependencyGraph::from_fragment_edges(&edges_b).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        let e_a = validate_wait_for(&deps_a, &edges_a, &[consumer.clone()], &c).unwrap_err();
        let e_b = validate_wait_for(&deps_b, &edges_b, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { cycle, .. } = &e_a else {
            panic!("expected cycle");
        };
        assert!(!cycle.is_empty());
        assert_eq!(e_a, e_b);
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
