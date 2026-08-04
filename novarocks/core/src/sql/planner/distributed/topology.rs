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

// The topology contract is the planner-native record of the fragment graph's
// execution shape that CGO-9B/Task 4 (scheduler swap) and Task 5 (coordinator
// retirement) will consume. Until those tasks read every field and accessor,
// allow the not-yet-consumed surface.
#![allow(dead_code)]

//! Planner-native topology contract for a sealed distributed plan.
//!
//! Where [`super::boundary`] records *which columns* cross each plan seam, this
//! module records the *shape of the fragment graph* that later stages must
//! agree on: a stable leaves-first topological order, the single execution
//! anchor that coordinates fetch/write, the producer and terminal-write
//! fragment sets, and the optional result fragment. It derives all of this
//! purely from already-constructed planner artifacts (`PlanFragment`,
//! `FragmentEdge`, `DataSink`).
//!
//! The order and anchor algorithms reproduce the former execution coordinator
//! scheduler's `topological_sort_bottom_up` / `select_execution_root_fragment`
//! (retired in CGO-9B/Task 4, which swapped the scheduler onto this contract with
//! zero behavior change). The error strings match those former coordinator
//! diagnostics for the same continuity reason.
//!
//! The contract is deliberately runtime-independent: it carries no backend
//! count, address, destination, placement, or `force_single_instance`. Those
//! remain a runtime concern derived later from the anchor's sink. This module
//! depends only on planner types: no protobuf, no coordinator, no runtime
//! handles.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use super::{DataSink, FragmentEdge, FragmentId, PlanFragment};

/// The execution-shape contract for a sealed distributed plan.
///
/// Every field is derived deterministically from the finalized fragments and
/// edges. Two structurally identical drafts produce identical contracts,
/// including the topological order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TopologyContract {
    planner_root_fragment_id: FragmentId,
    result_fragment_id: Option<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
    terminal_write_fragment_ids: Vec<FragmentId>,
    producer_fragment_ids: Vec<FragmentId>,
    topological_fragment_order: Vec<FragmentId>,
}

impl TopologyContract {
    /// The draft's resolved root fragment id (the planner's logical root).
    pub(crate) fn planner_root_fragment_id(&self) -> FragmentId {
        self.planner_root_fragment_id
    }

    /// The result fragment id: the root iff its sink is [`DataSink::Result`],
    /// otherwise `None` (a write-only DAG has no result fragment).
    pub(crate) fn result_fragment_id(&self) -> Option<FragmentId> {
        self.result_fragment_id
    }

    /// The single fragment that coordinates fetch/write, selected by planner
    /// semantics (see [`select_execution_anchor`]).
    pub(crate) fn execution_anchor_fragment_id(&self) -> FragmentId {
        self.execution_anchor_fragment_id
    }

    /// Terminal fragments (fragments that are the source of no edge) whose sink
    /// is a terminal write, in fragment declaration order.
    pub(crate) fn terminal_write_fragment_ids(&self) -> &[FragmentId] {
        &self.terminal_write_fragment_ids
    }

    /// Fragments that are the source of at least one edge, in ascending id
    /// order.
    pub(crate) fn producer_fragment_ids(&self) -> &[FragmentId] {
        &self.producer_fragment_ids
    }

    /// The fragment ids in topological order: leaves (producers) first, root
    /// last. Stable and deterministic for a given draft.
    pub(crate) fn topological_fragment_order(&self) -> &[FragmentId] {
        &self.topological_fragment_order
    }
}

/// A reason topology derivation refused to seal the plan.
///
/// The `Cycle`, `NoExecutionAnchor`, and `AmbiguousExecutionAnchor` `Display`
/// strings are byte-for-byte identical to the execution coordinator's, so the
/// later scheduler swap does not churn diagnostics.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum TopologyError {
    /// An edge references a fragment id that is not in the plan. Structural
    /// validation already guarantees this cannot happen for sealed plans;
    /// deriving the topology re-checks it rather than indexing blindly, so a
    /// future invariant regression fails loudly instead of misbehaving.
    MissingEdgeEndpoint {
        source_fragment_id: FragmentId,
        target_fragment_id: FragmentId,
        missing_fragment_id: FragmentId,
    },
    /// The fragment/edge graph is not acyclic.
    Cycle,
    /// An edge points against the computed topological order (source not before
    /// target). A correct acyclic order never trips this; it is an independent
    /// re-verification that the derived order is a valid topological sort.
    EdgeViolatesTopologicalOrder {
        source_fragment_id: FragmentId,
        target_fragment_id: FragmentId,
    },
    /// Every fragment is the source of an edge, so no terminal fragment exists
    /// to anchor execution.
    NoExecutionAnchor,
    /// More than one terminal fragment exists and they are not all terminal
    /// writes, so the execution anchor is not uniquely determined.
    AmbiguousExecutionAnchor {
        terminal_fragment_ids: Vec<FragmentId>,
    },
}

impl fmt::Display for TopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEdgeEndpoint {
                source_fragment_id,
                target_fragment_id,
                missing_fragment_id,
            } => write!(
                formatter,
                "distributed plan topology edge source_fragment_id={source_fragment_id} target_fragment_id={target_fragment_id} references missing fragment id={missing_fragment_id}"
            ),
            // Kept identical to the coordinator's scheduler string.
            Self::Cycle => formatter.write_str("cycle detected in fragment graph"),
            Self::EdgeViolatesTopologicalOrder {
                source_fragment_id,
                target_fragment_id,
            } => write!(
                formatter,
                "distributed plan topology edge source_fragment_id={source_fragment_id} target_fragment_id={target_fragment_id} violates topological order"
            ),
            // Kept identical to the coordinator's scheduler string.
            Self::NoExecutionAnchor => {
                formatter.write_str("no root fragment found (every fragment has an outgoing edge)")
            }
            // Kept identical to the coordinator's scheduler string.
            Self::AmbiguousExecutionAnchor {
                terminal_fragment_ids,
            } => write!(
                formatter,
                "multiple root fragments: {terminal_fragment_ids:?}"
            ),
        }
    }
}

/// Derive the authoritative topology contract from already-constructed
/// fragments, resolved root fragment id, and edges.
///
/// Runs inside `seal_draft` after structural validation, so edge endpoints and
/// sink placement are already known-sound. This function only *derives* the
/// execution shape and *re-verifies* the graph-level invariants that structural
/// validation does not cover (acyclicity, order consistency, anchor
/// determinacy); it never repairs edges or guesses.
pub(in crate::sql::planner::distributed) fn build_topology_contract(
    fragments: &[PlanFragment],
    root_fragment_id: FragmentId,
    edges: &[FragmentEdge],
) -> Result<TopologyContract, TopologyError> {
    let fragments_by_id: BTreeMap<FragmentId, &PlanFragment> =
        fragments.iter().map(|f| (f.fragment_id, f)).collect();

    // Defensive endpoint existence. Structural validation already guarantees
    // this in the seal path; re-check so a direct/regressed caller fails loudly.
    for edge in edges {
        for endpoint in [edge.source_fragment_id, edge.target_fragment_id] {
            if !fragments_by_id.contains_key(&endpoint) {
                return Err(TopologyError::MissingEdgeEndpoint {
                    source_fragment_id: edge.source_fragment_id,
                    target_fragment_id: edge.target_fragment_id,
                    missing_fragment_id: endpoint,
                });
            }
        }
    }

    // Step 1: stable leaves-first topological order (cycle detection).
    let topological_fragment_order = topological_order(fragments, edges)?;

    // Step 2: independently verify every edge points forward in that order.
    verify_edge_direction(edges, &topological_fragment_order)?;

    // Producer set: every fragment that is the source of at least one edge.
    let producer_ids: BTreeSet<FragmentId> =
        edges.iter().map(|edge| edge.source_fragment_id).collect();
    let producer_fragment_ids: Vec<FragmentId> = producer_ids.iter().copied().collect();

    // Terminal fragments: those that are the source of no edge, in fragment
    // declaration order (the coordinator's diagnostic order).
    let terminal_fragment_ids: Vec<FragmentId> = fragments
        .iter()
        .map(|fragment| fragment.fragment_id)
        .filter(|id| !producer_ids.contains(id))
        .collect();

    // Step 3: the execution anchor, determined by planner semantics.
    let execution_anchor_fragment_id =
        select_execution_anchor(&fragments_by_id, &terminal_fragment_ids)?;

    // Terminal writes: terminal fragments whose sink is a terminal write.
    let terminal_write_fragment_ids: Vec<FragmentId> = terminal_fragment_ids
        .iter()
        .copied()
        .filter(|id| {
            fragments_by_id
                .get(id)
                .is_some_and(|fragment| is_terminal_write(&fragment.sink))
        })
        .collect();

    // The result fragment is the root iff its sink is a result sink.
    let result_fragment_id = fragments_by_id
        .get(&root_fragment_id)
        .filter(|fragment| matches!(fragment.sink, DataSink::Result))
        .map(|_| root_fragment_id);

    Ok(TopologyContract {
        planner_root_fragment_id: root_fragment_id,
        result_fragment_id,
        execution_anchor_fragment_id,
        terminal_write_fragment_ids,
        producer_fragment_ids,
        topological_fragment_order,
    })
}

/// Whether a sink is a *terminal write* for anchor selection.
///
/// This reproduces the coordinator's mapping (`fragment_output_kind` +
/// `FragmentOutputKind::is_terminal_write`) directly from [`DataSink`] so the
/// planner never imports codegen: an Iceberg write is a terminal write; result,
/// noop, and change-stream router sinks are not.
fn is_terminal_write(sink: &DataSink) -> bool {
    match sink {
        DataSink::ConnectorWrite(_) => true,
        _ => false,
    }
}

/// Return the fragment ids in topological order (leaves first, root last).
///
/// Kahn's algorithm, bottom-up, reproducing the former coordinator scheduler's
/// `topological_sort_bottom_up` (retired in CGO-9B/Task 4): in-degree is the
/// number of incoming edges;
/// zero-in-degree fragments are processed in ascending id order (`BTreeMap`
/// seeds the queue in key order). A produced order shorter than the fragment
/// count means a cycle.
fn topological_order(
    fragments: &[PlanFragment],
    edges: &[FragmentEdge],
) -> Result<Vec<FragmentId>, TopologyError> {
    let mut in_degree: BTreeMap<FragmentId, usize> = BTreeMap::new();
    let mut adjacency: BTreeMap<FragmentId, Vec<FragmentId>> = BTreeMap::new();

    for fragment in fragments {
        in_degree.entry(fragment.fragment_id).or_insert(0);
    }
    for edge in edges {
        *in_degree.entry(edge.target_fragment_id).or_insert(0) += 1;
        adjacency
            .entry(edge.source_fragment_id)
            .or_default()
            .push(edge.target_fragment_id);
    }

    let mut queue: VecDeque<FragmentId> = in_degree
        .iter()
        .filter_map(|(&id, &degree)| (degree == 0).then_some(id))
        .collect();

    let mut order: Vec<FragmentId> = Vec::with_capacity(fragments.len());
    while let Some(fragment_id) = queue.pop_front() {
        order.push(fragment_id);
        if let Some(targets) = adjacency.get(&fragment_id) {
            for &target in targets {
                let degree = in_degree.entry(target).or_insert(0);
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(target);
                }
            }
        }
    }

    if order.len() != fragments.len() {
        return Err(TopologyError::Cycle);
    }
    Ok(order)
}

/// Verify every edge points forward in the topological order (source strictly
/// before target). Independent of order construction, so it catches any drift
/// between the produced order and the edge set.
fn verify_edge_direction(
    edges: &[FragmentEdge],
    order: &[FragmentId],
) -> Result<(), TopologyError> {
    let position: BTreeMap<FragmentId, usize> = order
        .iter()
        .enumerate()
        .map(|(index, &id)| (id, index))
        .collect();
    for edge in edges {
        let source_position = position.get(&edge.source_fragment_id);
        let target_position = position.get(&edge.target_fragment_id);
        match (source_position, target_position) {
            (Some(source), Some(target)) if source < target => {}
            _ => {
                return Err(TopologyError::EdgeViolatesTopologicalOrder {
                    source_fragment_id: edge.source_fragment_id,
                    target_fragment_id: edge.target_fragment_id,
                });
            }
        }
    }
    Ok(())
}

/// Select the single execution anchor, reproducing the former coordinator
/// scheduler's `select_execution_root_fragment` (retired in CGO-9B/Task 4):
/// - exactly one terminal fragment -> that fragment;
/// - zero terminal fragments -> [`TopologyError::NoExecutionAnchor`];
/// - many terminals, all terminal writes -> the minimum fragment id;
/// - otherwise -> [`TopologyError::AmbiguousExecutionAnchor`].
///
/// The scheduler derives `force_single_instance` from the anchor's output kind
/// at placement time; that stays a runtime concern and is intentionally absent
/// here.
fn select_execution_anchor(
    fragments_by_id: &BTreeMap<FragmentId, &PlanFragment>,
    terminal_fragment_ids: &[FragmentId],
) -> Result<FragmentId, TopologyError> {
    let all_terminal_writes = || {
        terminal_fragment_ids.iter().all(|id| {
            fragments_by_id
                .get(id)
                .is_some_and(|fragment| is_terminal_write(&fragment.sink))
        })
    };
    match terminal_fragment_ids.len() {
        1 => Ok(terminal_fragment_ids[0]),
        0 => Err(TopologyError::NoExecutionAnchor),
        _ if all_terminal_writes() => Ok(terminal_fragment_ids
            .iter()
            .copied()
            .min()
            .expect("terminal fragments checked non-empty")),
        _ => Err(TopologyError::AmbiguousExecutionAnchor {
            terminal_fragment_ids: terminal_fragment_ids.to_vec(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::OutputColumn;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::sql::planner::distributed::write::change_stream::{
        ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec,
    };
    use crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding;
    use crate::sql::planner::distributed::write::plan::finalize_sql_change_stream_test_plan;
    use crate::sql::planner::distributed::write::sink::ConnectorWriteFragmentSink;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentId,
        FragmentStreamKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    use super::{
        TopologyError, build_topology_contract, select_execution_anchor, topological_order,
        verify_edge_direction,
    };

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn values_node(
        fragment_id: FragmentId,
        node_id: i32,
        columns: Vec<OutputColumn>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        }
    }

    fn iceberg_write_sink() -> DataSink {
        DataSink::ConnectorWrite(ConnectorWriteFragmentSink {
            handle: None,
            input: ConnectorWriteInputBinding::RootOutputByOrdinal,
            output_contract: None,
        })
    }

    /// A minimal fragment carrying only the fields the topology derivation
    /// reads: its id and sink. A unique node id keeps global-node-id checks
    /// (in other passes) happy if these are ever routed through them.
    fn fragment(id: FragmentId, sink: DataSink) -> PlanFragment {
        PlanFragment {
            fragment_id: id,
            root: values_node(id, (id as i32) * 100 + 1, Vec::new()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    /// A filler edge; the topology derivation reads only source/target.
    fn edge(source: FragmentId, target: FragmentId) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: 0,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    // ----- Seal-path fixtures (end-to-end through `seal_draft`) --------------

    fn single_result_plan() -> DistributedPlan {
        DistributedPlanDraftBuilder::new(
            vec![fragment(0, DataSink::Result)],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .seal()
        .expect("single result plan seals")
    }

    fn single_write_plan() -> DistributedPlan {
        let columns = vec![output_col(1, "id")];
        DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: values_node(0, 10, columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: iceberg_write_sink(),
                output_exprs: None,
                output_columns: columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .seal()
        .expect("single write plan seals")
    }

    fn stream_plan() -> DistributedPlan {
        let columns = vec![output_col(1, "k")];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: producer_fragment_id,
            root: values_node(producer_fragment_id, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(consumer_fragment_id),
            vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1],
            }],
            Default::default(),
        )
        .seal()
        .expect("stream plan seals")
    }

    fn cte_multicast_plan() -> DistributedPlan {
        let cte_id: CteId = 7;
        let producer_columns = vec![output_col(1, "k"), output_col(2, "payload")];
        let receive_columns = producer_columns.clone();
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[1].column_id];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: producer_fragment_id,
            root: values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(consumer_fragment_id),
            vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: vec![1, 2],
            }],
            Default::default(),
        )
        .seal()
        .expect("cte multicast plan seals")
    }

    fn change_stream_router_plan() -> DistributedPlan {
        let output_columns = vec![
            output_col(1, "op"),
            output_col(2, "route"),
            output_col(3, "delete_id"),
        ];
        let builder = DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: values_node(0, 10, output_columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        );
        let mut branch = ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        branch.output_partition_ordinals = vec![2];
        let dag = ChangeStreamWriteDagSpec::for_test(Some(0), None, vec![branch]);
        finalize_sql_change_stream_test_plan(builder, dag).expect("change-stream router plan seals")
    }

    // ----- Positive seal-path contracts -------------------------------------

    #[test]
    fn single_result_plan_contract() {
        let plan = single_result_plan();
        let topology = plan.topology();

        assert_eq!(topology.planner_root_fragment_id(), 0);
        assert_eq!(topology.result_fragment_id(), Some(0));
        assert_eq!(topology.execution_anchor_fragment_id(), 0);
        assert!(topology.terminal_write_fragment_ids().is_empty());
        assert!(topology.producer_fragment_ids().is_empty());
        assert_eq!(topology.topological_fragment_order(), &[0]);
    }

    #[test]
    fn single_write_plan_contract() {
        let plan = single_write_plan();
        let topology = plan.topology();

        assert_eq!(topology.planner_root_fragment_id(), 0);
        // A pure single-write plan has no result fragment; its lone terminal
        // Iceberg-write fragment is both the anchor and the terminal write.
        assert_eq!(topology.result_fragment_id(), None);
        assert_eq!(topology.execution_anchor_fragment_id(), 0);
        assert_eq!(topology.terminal_write_fragment_ids(), &[0]);
        assert!(topology.producer_fragment_ids().is_empty());
        assert_eq!(topology.topological_fragment_order(), &[0]);
    }

    #[test]
    fn stream_plan_contract_is_leaves_first_root_last() {
        let plan = stream_plan();
        let topology = plan.topology();

        assert_eq!(topology.planner_root_fragment_id(), 0);
        assert_eq!(topology.result_fragment_id(), Some(0));
        // The single terminal (the result root) is the anchor.
        assert_eq!(topology.execution_anchor_fragment_id(), 0);
        assert!(topology.terminal_write_fragment_ids().is_empty());
        assert_eq!(topology.producer_fragment_ids(), &[1]);
        // Producer fragment 1 is a leaf, result root 0 is last.
        assert_eq!(topology.topological_fragment_order(), &[1, 0]);
    }

    #[test]
    fn cte_multicast_plan_contract() {
        let plan = cte_multicast_plan();
        let topology = plan.topology();

        assert_eq!(topology.result_fragment_id(), Some(0));
        assert_eq!(topology.execution_anchor_fragment_id(), 0);
        assert_eq!(topology.producer_fragment_ids(), &[1]);
        assert_eq!(topology.topological_fragment_order(), &[1, 0]);
        assert!(topology.terminal_write_fragment_ids().is_empty());
    }

    #[test]
    fn change_stream_router_plan_contract() {
        let plan = change_stream_router_plan();
        let topology = plan.topology();

        // The router source (fragment 0) has no result sink, so no result
        // fragment; the single terminal writer fragment is the anchor.
        assert_eq!(topology.planner_root_fragment_id(), 0);
        assert_eq!(topology.result_fragment_id(), None);
        assert_eq!(topology.producer_fragment_ids(), &[0]);
        assert_eq!(topology.topological_fragment_order(), &[0, 1]);
        assert_eq!(topology.execution_anchor_fragment_id(), 1);
        assert_eq!(topology.terminal_write_fragment_ids(), &[1]);
    }

    #[test]
    fn seal_path_topology_derivation_is_deterministic() {
        // Same draft shape must derive an identical contract, order included.
        assert_eq!(stream_plan().topology(), stream_plan().topology());
        assert_eq!(
            change_stream_router_plan().topology(),
            change_stream_router_plan().topology()
        );
    }

    // ----- Direct-call algorithm coverage -----------------------------------

    #[test]
    fn multi_write_dag_anchor_is_the_minimum_terminal_id() {
        // One producer feeding two terminal Iceberg writers (ids 3 and 2): all
        // terminals are terminal writes, so the anchor is the minimum id.
        let fragments = vec![
            fragment(1, DataSink::Noop),
            fragment(3, iceberg_write_sink()),
            fragment(2, iceberg_write_sink()),
        ];
        let edges = vec![edge(1, 3), edge(1, 2)];

        let contract = build_topology_contract(&fragments, 2, &edges)
            .expect("all-terminal-write DAG derives a contract");

        assert_eq!(contract.execution_anchor_fragment_id(), 2);
        // No result fragment (the root is an Iceberg write, not a result).
        assert_eq!(contract.result_fragment_id(), None);
        assert_eq!(contract.producer_fragment_ids(), &[1]);
        // Terminal writes are listed in fragment declaration order: 3 then 2.
        assert_eq!(contract.terminal_write_fragment_ids(), &[3, 2]);
        assert_eq!(contract.topological_fragment_order(), &[1, 3, 2]);
    }

    #[test]
    fn cycle_between_two_fragments_is_rejected() {
        let fragments = vec![fragment(0, DataSink::Result), fragment(1, DataSink::Noop)];
        let edges = vec![edge(0, 1), edge(1, 0)];

        let error = build_topology_contract(&fragments, 0, &edges)
            .expect_err("a two-fragment cycle must not seal");

        assert_eq!(error, TopologyError::Cycle);
        assert_eq!(error.to_string(), "cycle detected in fragment graph");
    }

    #[test]
    fn ambiguous_anchor_with_multiple_non_write_terminals_is_rejected() {
        // Two disconnected terminals, neither a terminal write.
        let fragments = vec![fragment(0, DataSink::Result), fragment(1, DataSink::Noop)];

        let error = build_topology_contract(&fragments, 0, &[])
            .expect_err("multiple non-write terminals are ambiguous");

        assert_eq!(
            error,
            TopologyError::AmbiguousExecutionAnchor {
                terminal_fragment_ids: vec![0, 1],
            }
        );
        assert_eq!(error.to_string(), "multiple root fragments: [0, 1]");
    }

    #[test]
    fn illegal_result_write_terminal_mix_is_rejected() {
        // A result terminal coexisting with a terminal write is not all-write,
        // so the anchor is not determined.
        let fragments = vec![
            fragment(0, DataSink::Result),
            fragment(1, iceberg_write_sink()),
        ];

        let error = build_topology_contract(&fragments, 0, &[])
            .expect_err("a result/write terminal mix is illegal");

        assert_eq!(
            error,
            TopologyError::AmbiguousExecutionAnchor {
                terminal_fragment_ids: vec![0, 1],
            }
        );
    }

    #[test]
    fn missing_edge_endpoint_is_rejected_by_direct_derivation() {
        // Structural validation masks this in the seal path (it fails earlier
        // with its own Structural error); the direct derivation still guards it.
        let fragments = vec![fragment(0, DataSink::Result)];
        let edges = vec![edge(0, 9)];

        let error = build_topology_contract(&fragments, 0, &edges)
            .expect_err("an edge to a missing fragment must fail");

        assert_eq!(
            error,
            TopologyError::MissingEdgeEndpoint {
                source_fragment_id: 0,
                target_fragment_id: 9,
                missing_fragment_id: 9,
            }
        );
    }

    #[test]
    fn topological_order_rejects_a_self_loop_as_a_cycle() {
        let fragments = vec![fragment(0, DataSink::Result)];
        let error =
            topological_order(&fragments, &[edge(0, 0)]).expect_err("a self loop is a cycle");
        assert_eq!(error, TopologyError::Cycle);
    }

    #[test]
    fn topological_order_seeds_zero_in_degree_fragments_in_ascending_id() {
        // Two independent producers (2, 4) both feeding a single sink (1). The
        // leaves come out in ascending id order, the sink last.
        let fragments = vec![
            fragment(1, DataSink::Result),
            fragment(4, DataSink::Noop),
            fragment(2, DataSink::Noop),
        ];
        let order =
            topological_order(&fragments, &[edge(4, 1), edge(2, 1)]).expect("acyclic graph orders");
        assert_eq!(order, vec![2, 4, 1]);
    }

    #[test]
    fn no_execution_anchor_when_no_terminal_exists() {
        // Exercised directly: an empty terminal set is only reachable behind a
        // cycle in a finite graph, which `topological_order` rejects first.
        let fragments = vec![fragment(0, DataSink::Result)];
        let fragments_by_id = fragments.iter().map(|f| (f.fragment_id, f)).collect();

        let error = select_execution_anchor(&fragments_by_id, &[])
            .expect_err("no terminal fragment means no anchor");

        assert_eq!(error, TopologyError::NoExecutionAnchor);
        assert_eq!(
            error.to_string(),
            "no root fragment found (every fragment has an outgoing edge)"
        );
    }

    #[test]
    fn verify_edge_direction_rejects_a_backward_edge() {
        // A deliberately wrong order (target before source) must be caught by
        // the independent direction check.
        let error = verify_edge_direction(&[edge(1, 0)], &[0, 1])
            .expect_err("an edge pointing backward in the order must fail");
        assert_eq!(
            error,
            TopologyError::EdgeViolatesTopologicalOrder {
                source_fragment_id: 1,
                target_fragment_id: 0,
            }
        );
    }

    #[test]
    fn verify_edge_direction_accepts_forward_edges() {
        assert!(verify_edge_direction(&[edge(1, 0)], &[1, 0]).is_ok());
    }
}
