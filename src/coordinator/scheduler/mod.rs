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

//! Fragment scheduler: decides which backend each fragment instance lands on.
//!
//! This is a pure decision layer. The coordinator (PR-4) reads the produced
//! `SchedulingPlan` to build `TExecPlanFragmentParams` for each instance and
//! submits them through the dispatcher.
//!
//! # Instance-count policy (StarRocks-style "instance follows upstream")
//!
//! - A **scan fragment** gets `max(1, min(N, max scan-node range count))`
//!   instances. The count derives from scan-range coverage, so a virtual scan
//!   with a single placeholder range runs once instead of once per backend, and
//!   an empty-snapshot scan falls back to one zero-row instance.
//! - A **non-scan fragment** gets `max(upstream_N)` over incoming
//!   `HashPartitioned` / `BucketShuffleHashPartitioned` edges, or 1 if no such
//!   edge exists.
//! - The **result root fragment** is forced to 1 instance (it holds the
//!   ResultSink; the FE fetches exactly one finst). Write-only DAGs may have
//!   multiple terminal write fragments; in that case one writer instance is
//!   selected as the execution anchor without changing writer parallelism.
//!
//! # Backend assignment
//!
//! - Full-fanout fragments: instance `i` lands on live backend slot `i`. The
//!   stored `backend_idx` is the backend id from the live snapshot, which may
//!   be sparse.
//! - Short scan fragments (`1 < count < N`): instance placement starts from
//!   `live[(query_id.lo as usize) % N]` and wraps, so many small scans do not
//!   all pile onto live slot 0.
//! - Single-instance fragments (including the root): `backend_idx =
//!   live[(query_id.lo as usize) % N].0`.
//!
//! # Scan-split policy (Scheme C)
//!
//! D2 scan split policy: scheme C — partition the preparation-built per_node_scan_ranges
//! across instances. The scheduler never re-invokes to_thrift_scan, so the
//! min_max/cloud_props/change_op context (known during preparation) is preserved.
//!
//! Round-robin: `range[i]` goes to `instance[i % count]`.

mod runtime_filter;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use crate::common::types::UniqueId;
use crate::coordinator::cluster::{LiveBackend, LiveBackendSnapshot};
use crate::coordinator::prepare::{FragmentSchedulingView, PreparedFragment};
use crate::runtime::endpoint::{
    FragmentDestination, RuntimeEndpoint, RuntimeFilterProberDestination,
};
use crate::runtime::scan_range::ScanRangeParams;
use crate::sql::planner::distributed::{
    FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind, PartitionKind,
};

pub(crate) use runtime_filter::{
    PlannedRuntimeFilter, RuntimeFilterPlanResult, plan_runtime_filters,
};

#[derive(Clone, Copy, Debug)]
struct IncomingEdge {
    source_fragment_id: FragmentId,
    is_native_hash_partitioned: bool,
    stream_kind: FragmentStreamKind,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Placement information for one fragment instance.
#[derive(Clone, Debug)]
pub(crate) struct FragmentInstancePlacement {
    /// The fragment this instance belongs to. The coordinator verifies this
    /// self-description against the `SchedulingPlan::by_fragment` key before
    /// constructing submissions.
    pub(crate) fragment_id: FragmentId,
    pub(crate) instance_index: usize,
    pub(crate) finst_id: UniqueId,
    /// Backend id from the scheduler's live backend snapshot.
    pub(crate) backend_idx: usize,
    /// Native runtime endpoint for this fragment instance.
    pub(crate) endpoint: RuntimeEndpoint,
    /// Scan ranges for this instance, keyed by plan node id.
    pub(crate) scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    /// Destinations this instance should push its output to.
    pub(crate) destinations: Vec<FragmentDestination>,
    /// Runtime filter prober destinations, keyed by filter_id.
    pub(crate) runtime_filter_prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    /// Number of upstream senders per exchange node id.
    pub(crate) per_exch_num_senders: BTreeMap<i32, i32>,
}

/// The result of scheduling a multi-fragment plan.
#[derive(Debug)]
pub(crate) struct SchedulingPlan {
    /// Fragment chosen as the execution anchor for fetch/write coordination.
    pub(crate) root_fragment_id: FragmentId,
    /// All instance placements, indexed by fragment id.
    pub(crate) by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    /// The finst id of the root fragment's (single) instance.
    pub(crate) root_finst_id: UniqueId,
    /// Which backend index the root instance is assigned to.
    pub(crate) root_backend_idx: usize,
}

impl SchedulingPlan {
    pub(crate) fn fragment_ids(&self) -> impl ExactSizeIterator<Item = FragmentId> + '_ {
        self.by_fragment.keys().copied()
    }
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Decides which backend each fragment instance lands on.
pub(crate) struct FragmentScheduler {
    backends: Vec<SocketAddr>,
    live_backend_snapshot: LiveBackendSnapshot,
}

impl FragmentScheduler {
    /// Create a new scheduler with the given backends.
    pub(crate) fn new(backends: Vec<SocketAddr>) -> Self {
        Self::from_live_backend_snapshot(LiveBackendSnapshot::from_endpoints(backends))
    }

    /// Create a scheduler from explicit backend ids and endpoints.
    pub(crate) fn new_with_backend_ids(backends: Vec<LiveBackend>) -> Self {
        Self::from_live_backend_snapshot(LiveBackendSnapshot::new(backends))
    }

    /// Create a scheduler from the immutable live backend snapshot shared by bootstrap.
    pub(crate) fn from_live_backend_snapshot(live_backend_snapshot: LiveBackendSnapshot) -> Self {
        let backends = live_backend_snapshot
            .entries()
            .iter()
            .map(|(_, endpoint)| *endpoint)
            .collect();
        Self {
            backends,
            live_backend_snapshot,
        }
    }

    /// Return the configured backends.
    pub(crate) fn backends(&self) -> &[SocketAddr] {
        &self.backends
    }

    /// Return the immutable live backend snapshot owned by this scheduler.
    pub(crate) fn live_backend_snapshot(&self) -> &LiveBackendSnapshot {
        &self.live_backend_snapshot
    }

    /// Schedule the planner-sealed static view against this scheduler's single
    /// immutable live-backend snapshot.
    pub(crate) fn schedule(
        &self,
        view: FragmentSchedulingView<'_>,
        query_id: UniqueId,
        rf_plan: Option<&RuntimeFilterPlanResult>,
    ) -> Result<SchedulingPlan, String> {
        let live = self.live_backend_snapshot.entries();
        let n = live.len();
        if n == 0 {
            return Err("no live backend available".into());
        }

        let topo = view.topological_order();
        let root_fragment_id = view.execution_anchor();

        let fr_by_id: BTreeMap<FragmentId, &PreparedFragment> = view
            .fragments()
            .map(|fragment| (fragment.fragment_id(), fragment))
            .collect();

        // Defensive projection/desync guards. These do NOT rebuild the graph;
        // they turn any drift between the sealed projection and the scheduled
        // fragment set into a loud error instead of silent misscheduling.
        let scheduled_ids: BTreeSet<FragmentId> = fr_by_id.keys().copied().collect();
        let ordered_ids: BTreeSet<FragmentId> = topo.iter().copied().collect();
        if ordered_ids.len() != topo.len() || ordered_ids != scheduled_ids {
            return Err(format!(
                "sealed topological order {topo:?} is not a permutation of scheduled fragment ids {scheduled_ids:?}"
            ));
        }
        if !scheduled_ids.contains(&root_fragment_id) {
            return Err(format!(
                "sealed execution anchor fragment {root_fragment_id} is not among scheduled fragments {scheduled_ids:?}"
            ));
        }

        // Step 2: compute instance counts in topological order.
        // Incoming edges are driven by planner-owned native partition semantics.
        let mut incoming: BTreeMap<FragmentId, Vec<IncomingEdge>> = BTreeMap::new();
        for e in view.edges() {
            let stream_kind = match e.edge_kind {
                FragmentEdgeKind::Stream => e.stream_kind,
                FragmentEdgeKind::CteMulticast { .. } => FragmentStreamKind::Broadcast,
                FragmentEdgeKind::IcebergChangeStreamRouter { .. } => e.stream_kind,
            };
            incoming
                .entry(e.target_fragment_id)
                .or_default()
                .push(IncomingEdge {
                    source_fragment_id: e.source_fragment_id,
                    is_native_hash_partitioned: matches!(
                        e.output_partition.kind,
                        PartitionKind::Hash
                    ),
                    stream_kind,
                });
        }

        let mut instance_counts: BTreeMap<FragmentId, usize> = BTreeMap::new();
        for &fid in topo {
            let fr = fr_by_id
                .get(&fid)
                .ok_or_else(|| format!("fragment {fid} missing from fragment list"))?;

            let has_gather_input = incoming
                .get(&fid)
                .map(|ins| {
                    ins.iter()
                        .any(|edge| edge.stream_kind == FragmentStreamKind::Gather)
                })
                .unwrap_or(false);

            let count = if has_gather_input {
                1
            } else if fr.has_scan_nodes() {
                // Scan fragment: instance count derives from scan-range coverage,
                // not an unconditional per-backend fan-out. A virtual scan with a
                // single placeholder range (Iceberg metadata/delta) collapses to a
                // single instance instead of N-1 empty instances that would
                // duplicate output under distributed execution. Zero ranges (empty
                // snapshot / fully pruned) fall back to one instance producing zero
                // rows. Native scheduling metadata only carries Iceberg native
                // scan ranges; compat thrift ranges, when present, are merged
                // later by the coordinator per placement and are not part of
                // this native fan-out formula.
                let max_ranges = fr
                    .scan_node_ids()
                    .iter()
                    .filter_map(|&node_id| view.scan_ranges(fid, node_id))
                    .map(<[ScanRangeParams]>::len)
                    .max()
                    .unwrap_or(0);
                max_ranges.clamp(1, n)
            } else {
                // Non-scan: inherit max from upstream hash-partitioned edges.
                let hash_max = incoming
                    .get(&fid)
                    .map(|ins| {
                        ins.iter()
                            .filter_map(|edge| {
                                if edge.is_native_hash_partitioned {
                                    instance_counts.get(&edge.source_fragment_id).copied()
                                } else {
                                    None
                                }
                            })
                            .max()
                    })
                    .flatten();
                hash_max.unwrap_or(1)
            };
            instance_counts.insert(fid, count);
        }

        // Step 3: force only result roots to 1 instance. Write-only DAG
        // anchors keep their exchange-derived parallelism. `force_single_instance`
        // is a runtime placement concern derived from the anchor fragment's own
        // output kind, not the sealed topology: a terminal-write anchor
        // (`is_terminal_write()`) keeps its parallelism, every other anchor (a
        // result/fetch root) collapses to a single instance. This uniform rule
        // reproduces the retired `select_execution_root_fragment` for both the
        // single-terminal and all-writes cases.
        let anchor_metadata = fr_by_id.get(&root_fragment_id).ok_or_else(|| {
            format!("execution anchor fragment {root_fragment_id} missing from fragment list")
        })?;
        let force_single_instance = !anchor_metadata.execution_role().is_terminal_write();
        if force_single_instance {
            instance_counts.insert(root_fragment_id, 1);
        }

        // Step 4: determine root backend index.
        let preferred_root_backend_idx = live[(query_id.lo as usize) % n].0;

        // Step 5: build placements.
        let mut by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>> = BTreeMap::new();

        for (&fid, &count) in &instance_counts {
            let fr = fr_by_id
                .get(&fid)
                .ok_or_else(|| format!("fragment {fid} missing from fragment list"))?;

            let mut instances: Vec<FragmentInstancePlacement> = (0..count)
                .map(
                    |instance_index| -> Result<FragmentInstancePlacement, String> {
                        let backend_idx = if count == 1 {
                            preferred_root_backend_idx
                        } else if count == n {
                            live[instance_index].0
                        } else {
                            // 1 < count < n: spread the short scan across backends from a
                            // query-derived offset so many small scans don't all pile onto live[0].
                            let start = (query_id.lo as usize) % n;
                            live[(start + instance_index) % n].0
                        };
                        let addr = live_backend_addr(live, backend_idx)?;
                        // finst_id encoding: hi = query_id.hi, lo = (fragment_id << 16) | instance_index.
                        // Unique within a query as long as instance_index < 65536 (always true:
                        // instance_count <= backends.len(), far below 65536).
                        debug_assert!(
                            instance_index < (1 << 16),
                            "instance_index {instance_index} overflows finst_id encoding"
                        );
                        let finst_id = UniqueId {
                            hi: query_id.hi,
                            lo: ((fid as i64) << 16) | (instance_index as i64),
                        };
                        Ok(FragmentInstancePlacement {
                            fragment_id: fid,
                            instance_index,
                            finst_id,
                            backend_idx,
                            endpoint: RuntimeEndpoint::from_socket_addr(addr),
                            scan_ranges: BTreeMap::new(),
                            destinations: Vec::new(),
                            runtime_filter_prober_params: BTreeMap::new(),
                            per_exch_num_senders: BTreeMap::new(),
                        })
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;

            // Step 6 (Scheme C): partition scan ranges round-robin.
            for &node_id in fr.scan_node_ids() {
                let all_ranges = view.scan_ranges(fid, node_id).ok_or_else(|| {
                    format!("prepared scan ranges missing for fragment {fid} scan node {node_id}")
                })?;
                for inst in instances.iter_mut() {
                    inst.scan_ranges.entry(node_id).or_default();
                }
                for (i, range) in all_ranges.iter().enumerate() {
                    instances[i % count]
                        .scan_ranges
                        .entry(node_id)
                        .or_default()
                        .push(range.clone());
                }
            }

            assert_scan_fragment_instances_nonempty(view, fr, &instances)?;

            by_fragment.insert(fid, instances);
        }

        // Compute root_finst_id from the selected execution anchor. Result
        // roots have one instance; write-only anchors may have multiple, so the
        // first placement is the coordination anchor.
        let root_placement = by_fragment
            .get(&root_fragment_id)
            .and_then(|insts| insts.first())
            .ok_or_else(|| "root fragment has no instances".to_string())?;
        let root_finst_id = root_placement.finst_id.clone();
        let root_backend_idx = root_placement.backend_idx;

        let mut plan = SchedulingPlan {
            root_fragment_id,
            by_fragment,
            root_finst_id,
            root_backend_idx,
        };
        populate_destinations(&mut plan, view.edges(), live)?;
        if let Some(rf_plan) = rf_plan {
            populate_runtime_filter_params(&mut plan, rf_plan, live)?;
        }
        populate_per_exch_num_senders(&mut plan, view.edges());
        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn live_backend_addr(live: &[LiveBackend], backend_idx: usize) -> Result<SocketAddr, String> {
    live.iter()
        .find_map(|(idx, addr)| (*idx == backend_idx).then_some(*addr))
        .ok_or_else(|| format!("backend index {backend_idx} missing from live snapshot"))
}

fn populate_destinations(
    plan: &mut SchedulingPlan,
    edges: &[FragmentEdge],
    live: &[LiveBackend],
) -> Result<(), String> {
    for edge in edges {
        let target_placements = plan
            .by_fragment
            .get(&edge.target_fragment_id)
            .map(|instances| {
                instances
                    .iter()
                    .map(|instance| (instance.finst_id, instance.backend_idx))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let destinations = target_placements
            .into_iter()
            .map(|(finst_id, backend_idx)| {
                let addr = live_backend_addr(live, backend_idx)?;
                Ok(FragmentDestination::new(
                    finst_id,
                    RuntimeEndpoint::from_socket_addr(addr),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if let Some(source_instances) = plan.by_fragment.get_mut(&edge.source_fragment_id) {
            for instance in source_instances {
                instance.destinations.extend(destinations.iter().cloned());
            }
        }
    }
    Ok(())
}

fn populate_runtime_filter_params(
    plan: &mut SchedulingPlan,
    rf_plan: &RuntimeFilterPlanResult,
    live: &[LiveBackend],
) -> Result<(), String> {
    let mut probe_instances_by_filter: BTreeMap<i32, Vec<(UniqueId, usize)>> = BTreeMap::new();
    for (fragment_id, probes) in &rf_plan.probe_side_filters {
        if let Some(instances) = plan.by_fragment.get(fragment_id) {
            let placements = instances
                .iter()
                .map(|instance| (instance.finst_id, instance.backend_idx))
                .collect::<Vec<_>>();
            for (filter_id, _scan_node_id) in probes {
                probe_instances_by_filter
                    .entry(*filter_id)
                    .or_default()
                    .extend(placements.iter().copied());
            }
        }
    }
    for (build_fragment_id, filter_ids) in &rf_plan.build_side_filters {
        if let Some(build_instances) = plan.by_fragment.get_mut(build_fragment_id) {
            for filter_id in filter_ids {
                if let Some(probe_list) = probe_instances_by_filter.get(filter_id) {
                    let probers = probe_list
                        .iter()
                        .map(|(finst_id, backend_idx)| {
                            let addr = live_backend_addr(live, *backend_idx)?;
                            Ok(RuntimeFilterProberDestination::new(
                                *finst_id,
                                RuntimeEndpoint::from_socket_addr(addr),
                            ))
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    for instance in build_instances.iter_mut() {
                        instance
                            .runtime_filter_prober_params
                            .insert(*filter_id, probers.clone());
                    }
                }
            }
        }
    }
    Ok(())
}

fn populate_per_exch_num_senders(plan: &mut SchedulingPlan, edges: &[FragmentEdge]) {
    for edge in edges {
        let upstream_count = plan
            .by_fragment
            .get(&edge.source_fragment_id)
            .map(|instances| instances.len())
            .unwrap_or(0) as i32;
        if let Some(target_instances) = plan.by_fragment.get_mut(&edge.target_fragment_id) {
            for instance in target_instances {
                *instance
                    .per_exch_num_senders
                    .entry(edge.target_exchange_node_id)
                    .or_insert(0) += upstream_count;
            }
        }
    }
}

/// Defensive invariant for the range-derived instance count: a scan
/// fragment must not produce a wholly-empty instance (all scan nodes
/// empty). The instance-count formula guarantees this; the guard turns
/// any regression into a loud error instead of silent duplicate/lost
/// output. The zero-range fallback (fragment total = 0, a single
/// zero-row instance) is the one legal exception.
fn assert_scan_fragment_instances_nonempty(
    view: FragmentSchedulingView<'_>,
    fr: &PreparedFragment,
    instances: &[FragmentInstancePlacement],
) -> Result<(), String> {
    if !fr.has_scan_nodes() {
        return Ok(());
    }
    let total = fr
        .scan_node_ids()
        .iter()
        .filter_map(|&node_id| view.scan_ranges(fr.fragment_id(), node_id))
        .map(<[ScanRangeParams]>::len)
        .sum::<usize>();
    if total == 0 {
        return Ok(());
    }
    for inst in instances {
        let has_any = inst.scan_ranges.values().any(|ranges| !ranges.is_empty());
        if !has_any {
            return Err(format!(
                "scan fragment {} instance {} has no scan ranges while the fragment carries {} range(s); instance count must derive from range coverage",
                fr.fragment_id(),
                inst.instance_index,
                total
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;

    use super::RuntimeFilterPlanResult;
    use crate::coordinator::prepare::{
        PreparedFragmentRole, PreparedFragmentSet, prepared_fragment_set_for_test,
    };
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, PartitionKind,
    };

    #[derive(Clone, Debug)]
    struct TestFragmentMetadata {
        fragment_id: FragmentId,
        has_scan_nodes: bool,
        output_kind: PreparedFragmentRole,
        native_scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    }

    #[derive(Clone, Debug)]
    struct TestTopology {
        topological_fragment_order: Vec<FragmentId>,
        execution_anchor_fragment_id: FragmentId,
    }

    impl TestTopology {
        fn new(
            topological_fragment_order: Vec<FragmentId>,
            execution_anchor_fragment_id: FragmentId,
        ) -> Self {
            Self {
                topological_fragment_order,
                execution_anchor_fragment_id,
            }
        }
    }

    /// Reproduce the planner's sealed topology derivation to build scheduler
    /// fixtures. TEST-ONLY: production consumes the planner's sealed
    /// sealed topology projection, and the scheduler must never rederive
    /// order/anchor from edges. Kept faithful to the planner
    /// algorithm (`build_topology_contract`) — ascending-id Kahn plus the
    /// single-terminal / all-writes anchor rule — so fixtures match what a sealed
    /// plan would produce.
    fn sealed_topology_for_test(
        fragments: &[TestFragmentMetadata],
        edges: &[FragmentEdge],
    ) -> TestTopology {
        let mut in_degree: BTreeMap<FragmentId, usize> = BTreeMap::new();
        let mut adjacency: BTreeMap<FragmentId, Vec<FragmentId>> = BTreeMap::new();
        for fr in fragments {
            in_degree.entry(fr.fragment_id).or_insert(0);
        }
        for e in edges {
            *in_degree.entry(e.target_fragment_id).or_insert(0) += 1;
            adjacency
                .entry(e.source_fragment_id)
                .or_default()
                .push(e.target_fragment_id);
        }
        let mut queue: std::collections::VecDeque<FragmentId> = in_degree
            .iter()
            .filter_map(|(&id, &deg)| (deg == 0).then_some(id))
            .collect();
        let mut order: Vec<FragmentId> = Vec::with_capacity(fragments.len());
        while let Some(fid) = queue.pop_front() {
            order.push(fid);
            if let Some(neighbors) = adjacency.get(&fid) {
                for &tgt in neighbors {
                    let deg = in_degree.get_mut(&tgt).expect("edge target seeded above");
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(tgt);
                    }
                }
            }
        }
        assert_eq!(
            order.len(),
            fragments.len(),
            "test topology fixture must be acyclic"
        );

        let sources: BTreeSet<FragmentId> = edges.iter().map(|e| e.source_fragment_id).collect();
        let terminals: Vec<&TestFragmentMetadata> = fragments
            .iter()
            .filter(|fr| !sources.contains(&fr.fragment_id))
            .collect();
        let anchor = match terminals.len() {
            1 => terminals[0].fragment_id,
            0 => panic!("test topology fixture has no terminal fragment"),
            _ if terminals
                .iter()
                .all(|fr| fr.output_kind.is_terminal_write()) =>
            {
                terminals
                    .iter()
                    .map(|fr| fr.fragment_id)
                    .min()
                    .expect("terminals checked non-empty")
            }
            _ => panic!("test topology fixture has an ambiguous execution anchor"),
        };
        TestTopology::new(order, anchor)
    }

    fn prepared_from_fixture(
        fragments: &[TestFragmentMetadata],
        edges: &[FragmentEdge],
        topology: &TestTopology,
    ) -> PreparedFragmentSet {
        prepared_fragment_set_for_test(
            fragments
                .iter()
                .map(|fragment| {
                    (
                        fragment.fragment_id,
                        fragment.output_kind,
                        fragment
                            .native_scan_ranges
                            .iter()
                            .map(|(&node_id, ranges)| (node_id, ranges.clone()))
                            .collect(),
                    )
                })
                .collect(),
            topology.topological_fragment_order.clone(),
            topology.execution_anchor_fragment_id,
            edges.to_vec(),
        )
    }

    impl FragmentScheduler {
        /// Test-only convenience: derive the sealed topology from `fragments` /
        /// `edges` (reproducing what the planner would seal) and schedule with it.
        /// Behavior-preserving fixtures use this; sealed-consumption tests call
        /// `schedule_fixture` directly with a hand-built sealed topology.
        fn assign_for_test(
            &self,
            fragments: &[TestFragmentMetadata],
            edges: &[FragmentEdge],
            query_id: UniqueId,
        ) -> Result<SchedulingPlan, String> {
            let topology = sealed_topology_for_test(fragments, edges);
            self.schedule_fixture(fragments, edges, &topology, query_id, None)
        }

        fn schedule_fixture(
            &self,
            fragments: &[TestFragmentMetadata],
            edges: &[FragmentEdge],
            topology: &TestTopology,
            query_id: UniqueId,
            rf_plan: Option<&RuntimeFilterPlanResult>,
        ) -> Result<SchedulingPlan, String> {
            let prepared = prepared_from_fixture(fragments, edges, topology);
            self.schedule(prepared.scheduling_view(), query_id, rf_plan)
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestPartitionType {
        HashPartitioned,
        BucketShuffleHashPartitioned,
        Unpartitioned,
    }

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn be(addr: &str) -> SocketAddr {
        SocketAddr::from_str(addr).expect("valid socket addr")
    }

    fn three_backends() -> Vec<SocketAddr> {
        vec![
            be("10.0.0.1:9010"),
            be("10.0.0.2:9010"),
            be("10.0.0.3:9010"),
        ]
    }

    fn two_backends() -> Vec<SocketAddr> {
        vec![be("10.0.0.1:9010"), be("10.0.0.2:9010")]
    }

    fn make_query_id(hi: i64, lo: i64) -> UniqueId {
        UniqueId { hi, lo }
    }

    fn dummy_query_id() -> UniqueId {
        make_query_id(1, 0)
    }

    fn scan_range_params(marker: i32) -> crate::runtime::scan_range::ScanRangeParams {
        let mut params = crate::runtime::scan_range::ScanRangeParams::file(
            crate::runtime::scan_range::FileScanRange {
                file_format: crate::runtime::scan_range::FileFormat::Parquet,
                full_path: Some(format!("s3://bucket/file-{marker}.parquet")),
                relative_path: None,
                table_id: None,
                offset: 0,
                length: 1,
                file_length: 1,
                delete_files: Vec::new(),
                deletion_vector_descriptor: None,
                first_row_id: None,
                data_sequence_number: None,
                modification_time: None,
                datacache_options: None,
                included_positions: Vec::new(),
                serialized_split: None,
                use_iceberg_jni_metadata_reader: false,
                ivm_change_op: None,
                file_pruning_min_max_values: None,
            },
        );
        params.volume_id = Some(marker);
        params
    }

    fn fake_fragment(
        fid: FragmentId,
        scan_node_id: Option<i32>,
        n_ranges: usize,
    ) -> TestFragmentMetadata {
        let native_scan_ranges = match scan_node_id {
            Some(node_id) => {
                let ranges: Vec<crate::runtime::scan_range::ScanRangeParams> =
                    (0..n_ranges as i32).map(scan_range_params).collect();
                BTreeMap::from([(node_id, ranges)])
            }
            None => BTreeMap::new(),
        };

        TestFragmentMetadata {
            fragment_id: fid,
            has_scan_nodes: scan_node_id.is_some(),
            output_kind: PreparedFragmentRole::NonTerminal,
            native_scan_ranges,
        }
    }

    fn test_placement(
        instance_index: usize,
        scan_ranges: BTreeMap<i32, Vec<crate::runtime::scan_range::ScanRangeParams>>,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id: 0,
            instance_index,
            finst_id: UniqueId {
                hi: 0,
                lo: instance_index as i64,
            },
            backend_idx: 0,
            endpoint: RuntimeEndpoint::from_socket_addr(be("10.0.0.1:9010")),
            scan_ranges,
            destinations: Vec::new(),
            runtime_filter_prober_params: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn fake_write_fragment(
        fid: FragmentId,
        scan_node_id: Option<i32>,
        n_ranges: usize,
    ) -> TestFragmentMetadata {
        let mut fragment = fake_fragment(fid, scan_node_id, n_ranges);
        fragment.output_kind = PreparedFragmentRole::TerminalWrite;
        fragment
    }

    fn assert_test_scan_fragment_instances_nonempty(
        fragment: &TestFragmentMetadata,
        instances: &[FragmentInstancePlacement],
    ) -> Result<(), String> {
        let topology = TestTopology::new(vec![fragment.fragment_id], fragment.fragment_id);
        let prepared = prepared_from_fixture(std::slice::from_ref(fragment), &[], &topology);
        let view = prepared.scheduling_view();
        let prepared_fragment = view
            .fragment(fragment.fragment_id)
            .expect("test fragment is prepared");
        assert_scan_fragment_instances_nonempty(view, prepared_fragment, instances)
    }

    /// Build a `FragmentEdge` with the given partition type.
    fn fake_edge(
        src: FragmentId,
        tgt: FragmentId,
        ptype: TestPartitionType,
        exch_node_id: i32,
    ) -> FragmentEdge {
        let stream_kind = match ptype {
            TestPartitionType::HashPartitioned
            | TestPartitionType::BucketShuffleHashPartitioned => FragmentStreamKind::Partitioned,
            TestPartitionType::Unpartitioned => FragmentStreamKind::Gather,
        };
        fake_stream_edge(src, tgt, ptype, exch_node_id, stream_kind)
    }

    fn fake_broadcast_edge(src: FragmentId, tgt: FragmentId, exch_node_id: i32) -> FragmentEdge {
        fake_stream_edge(
            src,
            tgt,
            TestPartitionType::Unpartitioned,
            exch_node_id,
            FragmentStreamKind::Broadcast,
        )
    }

    fn fake_cte_edge(src: FragmentId, tgt: FragmentId, exch_node_id: i32) -> FragmentEdge {
        let mut edge = fake_broadcast_edge(src, tgt, exch_node_id);
        edge.edge_kind = FragmentEdgeKind::CteMulticast {
            cte_id: 7,
            receive_producer_column_ids: Vec::new(),
        };
        edge
    }

    fn fake_stream_edge(
        src: FragmentId,
        tgt: FragmentId,
        ptype: TestPartitionType,
        exch_node_id: i32,
        stream_kind: FragmentStreamKind,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: src,
            target_fragment_id: tgt,
            target_exchange_node_id: exch_node_id,
            output_partition: native_partition_for_test(ptype),
            stream_kind,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    fn native_partition_for_test(ptype: TestPartitionType) -> DataPartition {
        let kind = match ptype {
            TestPartitionType::HashPartitioned
            | TestPartitionType::BucketShuffleHashPartitioned => PartitionKind::Hash,
            _ => PartitionKind::Unpartitioned,
        };
        DataPartition {
            kind,
            exprs: Vec::new(),
        }
    }

    fn fake_router_edge(
        src: FragmentId,
        tgt: FragmentId,
        ptype: TestPartitionType,
        exch_node_id: i32,
        stream_kind: FragmentStreamKind,
    ) -> FragmentEdge {
        let mut edge = fake_stream_edge(src, tgt, ptype, exch_node_id, stream_kind);
        edge.edge_kind = FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id: 42,
            branch_id: 1,
            branch_kind: crate::sql::common::ChangeStreamBranchKind::DeleteDv,
        };
        edge
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn guard_rejects_whole_instance_empty_scan() {
        let fr = fake_fragment(0, Some(1), 2);
        let instances = vec![
            test_placement(0, BTreeMap::from([(1, vec![scan_range_params(0)])])),
            test_placement(1, BTreeMap::from([(1, vec![])])),
        ];
        let err = assert_test_scan_fragment_instances_nonempty(&fr, &instances)
            .expect_err("whole-instance-empty must be rejected");
        assert!(err.contains("no scan ranges"), "got: {err}");
    }

    #[test]
    fn guard_allows_zero_range_fallback_and_node_local_empty() {
        let fr0 = fake_fragment(0, Some(1), 0);
        let insts0 = vec![test_placement(0, BTreeMap::from([(1, vec![])]))];
        assert!(assert_test_scan_fragment_instances_nonempty(&fr0, &insts0).is_ok());

        let mut fr1 = fake_fragment(0, Some(1), 1);
        fr1.native_scan_ranges.insert(2, vec![scan_range_params(9)]);
        let insts1 = vec![test_placement(
            0,
            BTreeMap::from([(1, vec![scan_range_params(0)]), (2, vec![])]),
        )];
        assert!(assert_test_scan_fragment_instances_nonempty(&fr1, &insts1).is_ok());
    }

    mod live_filter_tests {
        use super::*;

        #[test]
        fn assign_with_empty_live_snapshot_returns_explicit_error() {
            let scheduler = FragmentScheduler::new(Vec::new());
            // An empty live snapshot is rejected before the sealed topology is
            // ever inspected, so an empty projection is a legal placeholder here.
            let prepared = prepared_fragment_set_for_test(Vec::new(), Vec::new(), 0, Vec::new());
            let result = scheduler.schedule(prepared.scheduling_view(), dummy_query_id(), None);
            assert!(result.is_err());
            assert!(
                result.unwrap_err().contains("no live backend available"),
                "empty live snapshot should return explicit error"
            );
        }

        #[test]
        fn sparse_live_snapshot_preserves_original_backend_indices() {
            let fragments = vec![fake_fragment(0, Some(1), 2), fake_fragment(1, None, 0)];
            let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
            let live = vec![
                (2usize, be("10.0.0.1:9010")),
                (23usize, be("10.0.0.3:9010")),
            ];
            let scheduler = FragmentScheduler::new_with_backend_ids(live);
            let topology = sealed_topology_for_test(&fragments, &edges);

            let plan = scheduler
                .schedule_fixture(&fragments, &edges, &topology, make_query_id(7, 1), None)
                .expect("schedule");

            let placements = &plan.by_fragment[&0];
            assert_eq!(placements.len(), 2, "live.len() controls instance count");
            assert_eq!(placements[0].backend_idx, 2);
            assert_eq!(placements[1].backend_idx, 23);
            assert_eq!(
                plan.root_backend_idx, 23,
                "query_id.lo=1 chooses live slot 1"
            );

            for inst in placements {
                let dest = inst.destinations.first().expect("root destination");
                let endpoint = dest.endpoint();
                assert_eq!(endpoint.host(), "10.0.0.3");
                assert_eq!(endpoint.port(), 9010);
            }
        }

        #[test]
        fn one_snapshot_drives_zero_one_two_and_full_range_placement() {
            let fragments = vec![
                fake_fragment(0, Some(10), 0),
                fake_fragment(1, Some(11), 1),
                fake_fragment(2, Some(12), 2),
                fake_fragment(3, Some(13), 4),
                fake_fragment(10, None, 0),
            ];
            let edges = vec![
                fake_broadcast_edge(0, 10, 20),
                fake_broadcast_edge(1, 10, 21),
                fake_broadcast_edge(2, 10, 22),
                fake_broadcast_edge(3, 10, 23),
            ];
            let topology = sealed_topology_for_test(&fragments, &edges);
            let scheduler = FragmentScheduler::new_with_backend_ids(vec![
                (2, be("10.0.0.2:9010")),
                (11, be("10.0.0.11:9010")),
                (23, be("10.0.0.23:9010")),
            ]);

            let plan = scheduler
                .schedule_fixture(&fragments, &edges, &topology, make_query_id(7, 0), None)
                .expect("schedule all range coverage cases from one snapshot");
            let backend_ids = |fragment_id| {
                plan.by_fragment[&fragment_id]
                    .iter()
                    .map(|placement| placement.backend_idx)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                backend_ids(0),
                vec![2],
                "zero ranges use one fallback instance"
            );
            assert_eq!(backend_ids(1), vec![2], "one range uses one instance");
            assert_eq!(backend_ids(2), vec![2, 11], "two ranges use two live slots");
            assert_eq!(
                backend_ids(3),
                vec![2, 11, 23],
                "four ranges clamp to the full three-backend snapshot"
            );
        }
    }

    #[test]
    fn scan_root_fragment_forced_to_one_instance() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // Single scan fragment (is also the root: no outgoing edges).
        let fragments = vec![fake_fragment(0, Some(1), 3)];
        let edges: Vec<FragmentEdge> = vec![];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        // A lone scan fragment is also the root, so the root override (1 instance)
        // wins over range-derived scan fanout.
        assert_eq!(plan.by_fragment[&0].len(), 1);
    }

    #[test]
    fn scan_fragment_with_backend_count_ranges_gets_full_fanout() {
        // Non-root scan fragment with 3 ranges on 3 backends should get full fanout.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // F0=scan producer, F1=root consumer (UNPARTITIONED gather)
        let fragments = vec![fake_fragment(0, Some(1), 3), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "scan producer gets 3 instances"
        );
        assert_eq!(plan.by_fragment[&1].len(), 1, "root gets 1 instance");
    }

    #[test]
    fn change_stream_router_partitioned_edge_is_scheduled_like_stream_edge() {
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![
            fake_fragment(0, Some(1), 3),
            fake_fragment(1, None, 0),
            fake_fragment(2, None, 0),
        ];
        let edges = vec![
            fake_router_edge(
                0,
                1,
                TestPartitionType::HashPartitioned,
                10,
                FragmentStreamKind::Partitioned,
            ),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];

        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("router branch edge should schedule");
        assert_eq!(plan.by_fragment[&0].len(), 3, "scan source has 3 senders");
        assert_eq!(
            plan.by_fragment[&1].len(),
            3,
            "partitioned router target inherits upstream sender count"
        );

        for inst in &plan.by_fragment[&0] {
            assert_eq!(
                inst.destinations.len(),
                3,
                "router branch source sees every target writer instance"
            );
        }
        for inst in &plan.by_fragment[&1] {
            assert_eq!(
                inst.per_exch_num_senders.get(&10).copied(),
                Some(3),
                "router branch target sees same sender count as a stream edge"
            );
        }
    }

    #[test]
    fn hash_consumer_inherits_upstream_n() {
        // Topology: F0(scan) -> HASH -> F1(non-scan) -> UNPARTITIONED -> F2(root)
        // F0 has 2 backends, so F1 should inherit 2 instances.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan
            fake_fragment(1, None, 0),    // hash consumer (non-root)
            fake_fragment(2, None, 0),    // root
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::HashPartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "scan: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "hash consumer inherits 2 from upstream scan"
        );
        assert_eq!(plan.by_fragment[&2].len(), 1, "root: forced to 1");
    }

    #[test]
    fn scheduler_uses_native_partition_for_instance_count() {
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4),
            fake_fragment(1, None, 0),
            fake_fragment(2, None, 0),
        ];
        let mut hash_edge = fake_edge(0, 1, TestPartitionType::Unpartitioned, 10);
        hash_edge.output_partition = DataPartition::hash(Vec::new());
        hash_edge.stream_kind = FragmentStreamKind::Partitioned;
        let edges = vec![
            hash_edge,
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];

        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");

        assert_eq!(plan.by_fragment[&0].len(), 2, "scan: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "scheduler must derive hash fanout from native edge.output_partition"
        );
        assert_eq!(plan.by_fragment[&2].len(), 1, "root: forced to 1");
    }

    #[test]
    fn bucket_shuffle_consumer_inherits_upstream_n() {
        // Topology: F0(scan) -> BUCKET_SHUFFLE_HASH -> F1(non-root consumer) -> UNPARTITIONED -> F2(root)
        // F0 has 2 backends so F1 should inherit 2 instances.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan producer
            fake_fragment(1, None, 0),    // bucket-shuffle consumer (non-root)
            fake_fragment(2, None, 0),    // root gather
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::BucketShuffleHashPartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "scan: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "bucket-shuffle consumer inherits 2 from upstream scan"
        );
        assert_eq!(plan.by_fragment[&2].len(), 1, "root: forced to 1");
    }

    #[test]
    fn mixed_partition_edges_hash_wins_over_unpartitioned() {
        // Topology:
        //   F0(scan, N=2) -> HASH_PARTITIONED     -> F2(consumer, non-root)
        //   F1(non-scan)  -> BROADCAST             -> F2
        //   F2            -> UNPARTITIONED         -> F3(root)
        // F2 should get N=2 instances (HASH edge determines count; broadcast is ignored).
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan producer: 2 instances
            fake_fragment(1, None, 0),    // non-scan producer: 1 instance (UNPARTITIONED into F2)
            fake_fragment(2, None, 0),    // mixed consumer (non-root)
            fake_fragment(3, None, 0),    // root gather
        ];
        let edges = vec![
            fake_edge(0, 2, TestPartitionType::HashPartitioned, 10),
            fake_broadcast_edge(1, 2, 20),
            fake_edge(2, 3, TestPartitionType::Unpartitioned, 30),
        ];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "scan producer: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "unpartitioned producer: 1 instance"
        );
        assert_eq!(
            plan.by_fragment[&2].len(),
            2,
            "HASH edge wins: consumer gets 2 instances"
        );
        assert_eq!(plan.by_fragment[&3].len(), 1, "root: forced to 1");
    }

    #[test]
    fn unpartitioned_gather_is_one_instance() {
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan
            fake_fragment(1, None, 0),    // root (UNPARTITIONED gather)
        ];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 7))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "unpartitioned gather -> 1 instance"
        );
    }

    #[test]
    fn incoming_gather_forces_scan_consumer_to_one_instance() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, None, 0),    // gathered producer
            fake_fragment(1, Some(7), 6), // consumer also owns a scan
            fake_fragment(2, None, 0),    // root
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::Unpartitioned, 10),
            fake_edge(1, 2, TestPartitionType::HashPartitioned, 20),
        ];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 7))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "a true Gather input must not be consumed by every scan instance"
        );
    }

    #[test]
    fn root_fragment_is_always_one_instance() {
        // Even if an edge into the root is HASH_PARTITIONED, root stays at 1.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 2), // scan
            fake_fragment(1, None, 0),    // root
        ];
        let edges = vec![fake_edge(0, 1, TestPartitionType::HashPartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(5, 5))
            .expect("assign");
        assert_eq!(plan.by_fragment[&1].len(), 1, "root always 1");
    }

    #[test]
    fn multi_terminal_write_dag_keeps_writer_parallelism() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 6),
            fake_write_fragment(10, None, 0),
            fake_write_fragment(11, None, 0),
        ];
        let edges = vec![
            fake_router_edge(
                0,
                10,
                TestPartitionType::HashPartitioned,
                100,
                FragmentStreamKind::Partitioned,
            ),
            fake_router_edge(
                0,
                11,
                TestPartitionType::HashPartitioned,
                101,
                FragmentStreamKind::Partitioned,
            ),
        ];

        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(5, 5))
            .expect("assign");

        assert_eq!(plan.root_fragment_id, 10);
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "scan source stays distributed"
        );
        assert_eq!(
            plan.by_fragment[&10].len(),
            3,
            "write anchor is not forced to a single instance"
        );
        assert_eq!(plan.by_fragment[&11].len(), 3);
        assert_eq!(plan.root_backend_idx, plan.by_fragment[&10][0].backend_idx);
        assert_eq!(plan.root_finst_id, plan.by_fragment[&10][0].finst_id);
    }

    #[test]
    fn single_terminal_write_dag_keeps_writer_parallelism() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 6),
            fake_write_fragment(10, None, 0),
        ];
        let edges = vec![fake_router_edge(
            0,
            10,
            TestPartitionType::HashPartitioned,
            100,
            FragmentStreamKind::Partitioned,
        )];

        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(5, 5))
            .expect("assign");

        assert_eq!(plan.root_fragment_id, 10);
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "scan source stays distributed"
        );
        assert_eq!(
            plan.by_fragment[&10].len(),
            3,
            "single terminal writer is not forced to a single instance"
        );
        assert_eq!(plan.root_backend_idx, plan.by_fragment[&10][0].backend_idx);
        assert_eq!(plan.root_finst_id, plan.by_fragment[&10][0].finst_id);
    }

    #[test]
    fn full_fanout_backend_idx_equals_instance_index() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 3), // scan: 3 instances
            fake_fragment(1, None, 0),    // root
        ];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 3);
        for inst in f0 {
            assert_eq!(
                inst.backend_idx, inst.instance_index,
                "full-fanout: backend_idx == instance_index"
            );
        }
    }

    #[test]
    fn single_instance_lands_on_query_id_hash() {
        // query_id.lo = 7, n = 3 -> root_backend_idx = 7 % 3 = 1
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![fake_fragment(0, None, 0)]; // non-scan root
        let edges: Vec<FragmentEdge> = vec![];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 7))
            .expect("assign");
        assert_eq!(plan.root_backend_idx, 1, "7 % 3 == 1");
        assert_eq!(plan.by_fragment[&0][0].backend_idx, 1);
    }

    #[test]
    fn finst_id_encodes_fragment_id_and_instance_index() {
        // fragment_id=3, instance 0 -> finst.lo == 0x30000; finst.hi == query_id.hi
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // Use non-root scan so we get multi-instance (otherwise root override -> 1 inst)
        let fragments = vec![
            fake_fragment(3, Some(1), 3), // scan fragment, id=3
            fake_fragment(99, None, 0),   // root
        ];
        let edges = vec![fake_edge(3, 99, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(42, 0))
            .expect("assign");
        let inst0 = &plan.by_fragment[&3][0];
        assert_eq!(inst0.finst_id.hi, 42, "hi == query_id.hi");
        assert_eq!(
            inst0.finst_id.lo, 0x30000,
            "lo == (fid<<16)|idx == 3<<16 == 0x30000"
        );
    }

    #[test]
    fn scan_splits_round_robin_seven_ranges_three_instances() {
        // 7 ranges across 3 instances -> counts 3, 2, 2; total 7, no loss.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // Non-root scan with 3 backends
        let mut fr = fake_fragment(0, Some(1), 7);
        fr.fragment_id = 0;
        let root = fake_fragment(1, None, 0);
        let fragments = vec![fr, root];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 3);
        let counts: Vec<usize> = f0
            .iter()
            .map(|inst| inst.scan_ranges.get(&1).map(|r| r.len()).unwrap_or(0))
            .collect();
        assert_eq!(counts, vec![3, 2, 2], "round-robin 7 across 3: [3,2,2]");
        let total: usize = counts.iter().sum();
        assert_eq!(total, 7, "no ranges lost");
    }

    #[test]
    fn scan_instance_count_derives_from_range_count() {
        let scheduler = FragmentScheduler::new(three_backends());
        // 1 range on 3 backends -> single instance (virtual-scan root-fix case)
        let one = vec![fake_fragment(0, Some(1), 1), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&one, &edges, make_query_id(1, 0))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 1, "1 range -> 1 instance");

        // 2 ranges on 3 backends -> two instances (range < N)
        let two = vec![fake_fragment(0, Some(1), 2), fake_fragment(1, None, 0)];
        let plan = scheduler
            .assign_for_test(&two, &edges, make_query_id(1, 0))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "2 ranges -> 2 instances");

        // 5 ranges on 3 backends -> capped at N=3 (range >= N, unchanged)
        let many = vec![fake_fragment(0, Some(1), 5), fake_fragment(1, None, 0)];
        let plan = scheduler
            .assign_for_test(&many, &edges, make_query_id(1, 0))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "5 ranges on 3 BE -> 3 instances"
        );
    }

    #[test]
    fn short_scan_spreads_across_backends_by_query_offset() {
        // 2 ranges on 3 backends -> count=2 (1<count<n). query_id.lo=1 -> start=1
        // -> instances land on live positions 1,2 (not 0,1), spreading load.
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![fake_fragment(0, Some(1), 2), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 2);
        let idxs: Vec<usize> = f0.iter().map(|i| i.backend_idx).collect();
        assert_eq!(idxs, vec![1, 2], "start=lo%n=1 -> positions 1,2");
        assert_ne!(idxs[0], idxs[1], "distinct backends, no parallelism loss");
    }

    #[test]
    fn scan_instance_count_uses_max_over_scan_nodes_and_allows_node_local_empty() {
        // node 1: 3 ranges, node 2: 1 range; 3 backends.
        // count = min(3, max(3,1)) = 3. node 1 covers all 3 instances; node 2's
        // single range lands only on instance 0 (node-local empty on 1,2), but
        // NO instance is wholly empty.
        let scheduler = FragmentScheduler::new(three_backends());
        let mut fr = fake_fragment(0, Some(1), 3);
        fr.native_scan_ranges
            .insert(2, vec![scan_range_params(100)]);
        let fragments = vec![fr, fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 3, "count = max over scan nodes = 3");

        // node 1 non-empty on every instance
        for inst in f0 {
            assert!(!inst.scan_ranges.get(&1).unwrap().is_empty());
        }
        // node 2 (1 range) only on instance 0
        assert_eq!(f0[0].scan_ranges.get(&2).map(Vec::len), Some(1));
        assert_eq!(f0[1].scan_ranges.get(&2).map(Vec::len), Some(0));
        assert_eq!(f0[2].scan_ranges.get(&2).map(Vec::len), Some(0));
        // invariant: no whole-instance-empty
        for inst in f0 {
            assert!(inst.scan_ranges.values().any(|r| !r.is_empty()));
        }
    }

    #[test]
    fn scan_zero_range_falls_back_to_single_instance() {
        // Empty snapshot / fully-pruned scan: 0 ranges on 2 backends.
        // Root-fix: fall back to ONE instance (not N empty instances).
        // That single instance keeps an (empty) entry for the scan node so
        // the operator builds zero morsels -> zero rows.
        let scheduler = FragmentScheduler::new(two_backends());
        let fragments = vec![fake_fragment(0, Some(7), 0), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 1, "0 ranges -> single fallback instance");
        let ranges = f0[0]
            .scan_ranges
            .get(&7)
            .expect("empty scan-range entry preserved on the fallback instance");
        assert!(ranges.is_empty(), "fallback instance carries zero ranges");
    }

    #[test]
    fn scan_splits_no_overlap_no_loss() {
        // 6 ranges, 2 instances -> each range appears exactly once.
        // Use volume_id markers 0..5 as distinguishable identity.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fr = fake_fragment(0, Some(5), 6); // node_id=5, 6 ranges with markers 0..5
        let root = fake_fragment(1, None, 0);
        let fragments = vec![fr, root];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 2);
        let empty: Vec<crate::runtime::scan_range::ScanRangeParams> = vec![];
        let mut all_markers: Vec<i32> = f0
            .iter()
            .flat_map(|inst| {
                inst.scan_ranges
                    .get(&5)
                    .unwrap_or(&empty)
                    .iter()
                    .map(|r| r.volume_id.expect("marker set"))
            })
            .collect();
        all_markers.sort();
        assert_eq!(
            all_markers,
            vec![0, 1, 2, 3, 4, 5],
            "each range appears exactly once"
        );
    }

    #[test]
    fn fill_destinations_source_gets_all_target_instances() {
        // F0 scan (3 inst) -> UNPARTITIONED -> F1 root (1 inst).
        // After fill_destinations, each F0 instance should have 1 destination.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends.clone());
        let fragments = vec![fake_fragment(0, Some(1), 3), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");

        // Each source instance (F0) should have exactly 1 destination (F1's 1 instance).
        for inst in &plan.by_fragment[&0] {
            assert_eq!(
                inst.destinations.len(),
                1,
                "1 destination per source instance"
            );
        }
    }

    #[test]
    fn fill_destinations_sets_runtime_endpoint() {
        // Verify hostname/port comes from backends[target.backend_idx].
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends.clone());
        let fragments = vec![fake_fragment(0, Some(1), 1), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");

        // F1 root backend: query_id.lo=0, n=3 -> backend 0 -> "10.0.0.1:9010"
        let root_dest = &plan.by_fragment[&0][0].destinations[0];
        let endpoint = root_dest.endpoint();
        assert_eq!(endpoint.host(), "10.0.0.1");
        assert_eq!(endpoint.port(), 9010);
    }

    #[test]
    fn fill_runtime_filter_params_build_gets_all_probe_instances() {
        // 2 probe instances -> 2 probers with the right addresses.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends.clone());
        // F0=scan (2 inst), F1=root (1 inst); F0 is probe, F1 is build (artificial scenario).
        let fragments = vec![fake_fragment(0, Some(1), 2), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let rf_plan = RuntimeFilterPlanResult {
            all_filters: Default::default(),
            build_side_filters: {
                let mut m = std::collections::HashMap::new();
                m.insert(1u32, vec![42i32]); // fragment 1 builds filter 42
                m
            },
            probe_side_filters: {
                let mut m = std::collections::HashMap::new();
                m.insert(0u32, vec![(42i32, 1i32)]); // fragment 0 probes filter 42 on scan node 1
                m
            },
        };
        let topology = sealed_topology_for_test(&fragments, &edges);
        let plan = scheduler
            .schedule_fixture(
                &fragments,
                &edges,
                &topology,
                make_query_id(1, 0),
                Some(&rf_plan),
            )
            .expect("schedule with runtime filters");

        // F1 (build fragment) single instance should have 2 prober entries for filter 42.
        let build_inst = &plan.by_fragment[&1][0];
        let probers = build_inst
            .runtime_filter_prober_params
            .get(&42)
            .expect("filter 42 prober params");
        assert_eq!(probers.len(), 2, "2 probe instances -> 2 prober entries");

        // Verify addresses correspond to the 2 probe instances (F0: backends 0 and 1).
        let addrs: Vec<String> = probers
            .iter()
            .map(|p| p.endpoint().host().to_string())
            .collect();
        let ports: Vec<i32> = probers.iter().map(|p| p.endpoint().port()).collect();
        assert!(
            addrs.contains(&"10.0.0.1".to_string()),
            "probe instance 0 on backend 0"
        );
        assert!(
            addrs.contains(&"10.0.0.2".to_string()),
            "probe instance 1 on backend 1"
        );
        assert_eq!(
            ports,
            vec![9010, 9010],
            "both probe endpoints use port 9010"
        );
    }

    #[test]
    fn fill_per_exch_num_senders_accumulates_upstream_count() {
        // F0 (scan, 3 inst) -> exchange node 10 -> F2 (root)
        // F1 (scan, 3 inst) -> exchange node 20 -> F2 (root)
        // Each F2 instance should have per_exch_num_senders[10]=3, [20]=3.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 3),
            fake_fragment(1, Some(2), 3),
            fake_fragment(2, None, 0), // root
        ];
        let edges = vec![
            fake_edge(0, 2, TestPartitionType::Unpartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let plan = scheduler
            .assign_for_test(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");

        let root_inst = &plan.by_fragment[&2][0];
        assert_eq!(
            root_inst.per_exch_num_senders.get(&10).copied(),
            Some(3),
            "exch node 10 has 3 senders"
        );
        assert_eq!(
            root_inst.per_exch_num_senders.get(&20).copied(),
            Some(3),
            "exch node 20 has 3 senders"
        );
    }

    #[test]
    fn no_backends_returns_error() {
        let scheduler = FragmentScheduler::new(vec![]);
        let fragments = vec![fake_fragment(0, None, 0)];
        let edges: Vec<FragmentEdge> = vec![];
        let result = scheduler.assign_for_test(&fragments, &edges, make_query_id(1, 1));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no live backend available"));
    }

    // Cycle detection is no longer a scheduler responsibility: the planner seals
    // an acyclic `TopologyContract` (CGO-9B/Task 2) and rejects cycles up front
    // (`sql::planner::distributed::topology::cycle_between_two_fragments_is_rejected`).
    // The scheduler consumes the sealed order and never rediscovers the graph, so
    // it can no longer observe a cycle to reject.

    // -----------------------------------------------------------------------
    // Sealed-topology consumption (CGO-9B/Task 4)
    //
    // These tests build a sealed test topology by hand (not via
    // `sealed_topology_for_test`) so they prove the scheduler consumes the
    // *sealed* order/anchor rather than rederiving them from `edges`, while live
    // placement stays dynamic and registry-driven.
    // -----------------------------------------------------------------------

    #[test]
    fn scheduler_consumes_sealed_anchor_instead_of_edge_terminal() {
        // The only edge terminal (no outgoing edge) is F1, so edge-derivation
        // would anchor on F1. The sealed projection names F0 as the anchor; the
        // scheduler must honor the sealed fact.
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![fake_fragment(0, None, 0), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let topology = TestTopology::new(vec![0, 1], 0);

        let plan = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(1, 1), None)
            .expect("assign honors the sealed anchor");

        assert_eq!(
            plan.root_fragment_id, 0,
            "sealed execution anchor wins over the edge-derived terminal"
        );
    }

    #[test]
    fn scheduler_consumes_sealed_order_for_hash_inheritance() {
        // F0(scan,4) --HASH--> F1(non-scan) --gather--> F2(root). Processing F0
        // before F1 in the sealed order is what lets F1 inherit F0's fanout.
        let scheduler = FragmentScheduler::new(two_backends());
        let fragments = vec![
            fake_fragment(0, Some(1), 4),
            fake_fragment(1, None, 0),
            fake_fragment(2, None, 0),
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::HashPartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let topology = TestTopology::new(vec![0, 1, 2], 2);

        let plan = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(1, 1), None)
            .expect("assign consumes the sealed order");

        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "hash consumer inherits F0's count via the sealed leaves-first order"
        );
        assert_eq!(plan.root_fragment_id, 2);
    }

    #[test]
    fn sealed_write_anchor_keeps_writer_parallelism() {
        // Write-only DAG: two terminal writers. The sealed anchor is the min id
        // 10 and, being a terminal write, keeps its exchange-derived parallelism.
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![
            fake_fragment(0, Some(1), 6),
            fake_write_fragment(10, None, 0),
            fake_write_fragment(11, None, 0),
        ];
        let edges = vec![
            fake_router_edge(
                0,
                10,
                TestPartitionType::HashPartitioned,
                100,
                FragmentStreamKind::Partitioned,
            ),
            fake_router_edge(
                0,
                11,
                TestPartitionType::HashPartitioned,
                101,
                FragmentStreamKind::Partitioned,
            ),
        ];
        let topology = TestTopology::new(vec![0, 10, 11], 10);

        let plan = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(5, 5), None)
            .expect("assign schedules the sealed write DAG");

        assert_eq!(plan.root_fragment_id, 10, "sealed write anchor");
        assert_eq!(
            plan.by_fragment[&10].len(),
            3,
            "terminal-write anchor is not forced to a single instance"
        );
        assert_eq!(plan.by_fragment[&11].len(), 3);
    }

    #[test]
    fn sealed_router_shape_schedules_from_projection() {
        // Router source F0 --router--> terminal writer F1 (the sealed anchor).
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![
            fake_fragment(0, Some(1), 3),
            fake_write_fragment(1, None, 0),
        ];
        let edges = vec![fake_router_edge(
            0,
            1,
            TestPartitionType::HashPartitioned,
            10,
            FragmentStreamKind::Partitioned,
        )];
        let topology = TestTopology::new(vec![0, 1], 1);

        let plan = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(1, 1), None)
            .expect("assign schedules the sealed router shape");

        assert_eq!(plan.root_fragment_id, 1);
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "router source keeps scan fanout"
        );
        assert_eq!(
            plan.by_fragment[&1].len(),
            3,
            "router writer anchor keeps parallelism"
        );
    }

    #[test]
    fn sealed_cte_shape_schedules_from_projection() {
        // CTE multicast producer F0 --broadcast--> consumer/root F1.
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![fake_fragment(0, Some(1), 3), fake_fragment(1, None, 0)];
        let edges = vec![fake_cte_edge(0, 1, 10)];
        let topology = TestTopology::new(vec![0, 1], 1);

        let plan = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(1, 1), None)
            .expect("assign schedules the sealed CTE shape");

        assert_eq!(plan.root_fragment_id, 1);
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "CTE producer keeps scan fanout"
        );
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "CTE consumer/root is a single fetch instance"
        );
    }

    #[test]
    fn sealed_anchor_placement_stays_dynamic_across_1fe_3be() {
        // Same sealed topology, different live backend registries: the sealed
        // order/anchor are fixed while placement follows the live snapshot.
        let fragments = vec![fake_fragment(0, Some(1), 3), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let topology = TestTopology::new(vec![0, 1], 1);

        // 1FE+3BE dense registry: the scan fans out across all three backends.
        let live3 = vec![
            (2usize, be("10.0.0.1:9010")),
            (11usize, be("10.0.0.2:9010")),
            (23usize, be("10.0.0.3:9010")),
        ];
        let scheduler3 = FragmentScheduler::new_with_backend_ids(live3);
        let plan3 = scheduler3
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(9, 0), None)
            .expect("schedule against a 3-BE registry");
        assert_eq!(plan3.root_fragment_id, 1);
        let f0_dense: Vec<usize> = plan3.by_fragment[&0]
            .iter()
            .map(|inst| inst.backend_idx)
            .collect();
        assert_eq!(
            f0_dense,
            vec![2, 11, 23],
            "scan fans out across the live 3-BE registry"
        );
        assert_eq!(plan3.root_backend_idx, 2, "query_id.lo=0 -> live slot 0");

        // A different (sparse-id) registry must move placement dynamically while
        // the sealed anchor is unchanged.
        let live_sparse = vec![(3usize, be("10.0.0.4:9010")), (7usize, be("10.0.0.8:9010"))];
        let scheduler_sparse = FragmentScheduler::new_with_backend_ids(live_sparse);
        let plan_sparse = scheduler_sparse
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(9, 1), None)
            .expect("schedule against a sparse registry");
        assert_eq!(
            plan_sparse.root_fragment_id, 1,
            "sealed anchor is independent of live placement"
        );
        let f0_sparse: Vec<usize> = plan_sparse.by_fragment[&0]
            .iter()
            .map(|inst| inst.backend_idx)
            .collect();
        assert_eq!(
            f0_sparse,
            vec![3, 7],
            "scan placement follows the sparse live registry ids"
        );
    }

    #[test]
    fn assign_rejects_topological_order_desync() {
        // A projected order that is not a permutation of the scheduled fragment
        // ids is a desync bug; the scheduler fails loud without rebuilding it.
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![fake_fragment(0, None, 0), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let topology = TestTopology::new(vec![0], 1);

        let err = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(1, 1), None)
            .expect_err("order/fragment desync must be rejected");
        assert!(err.contains("not a permutation"), "got: {err}");
    }

    #[test]
    fn assign_rejects_anchor_absent_from_fragments() {
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![fake_fragment(0, None, 0)];
        let edges: Vec<FragmentEdge> = vec![];
        let topology = TestTopology::new(vec![0], 5);

        let err = scheduler
            .schedule_fixture(&fragments, &edges, &topology, make_query_id(1, 1), None)
            .expect_err("an anchor absent from the fragment set must be rejected");
        assert!(
            err.contains("is not among scheduled fragments"),
            "got: {err}"
        );
    }
}
