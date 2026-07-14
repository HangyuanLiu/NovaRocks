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

use crate::sql::planner::distributed::{FragmentEdge, FragmentId};

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

#[cfg(test)]
mod tests {
    use super::*;
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
}
