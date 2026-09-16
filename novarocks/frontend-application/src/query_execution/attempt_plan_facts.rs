// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The plan facts one attempt reads while it runs.

use novarocks_spi::connector::read_stack::{
    ConnectorReadColumnHandle, ConnectorReadConstraint, runtime::ConnectorReadAssignment,
};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_sql::plan_read::FragmentEdge;
use novarocks_sql::plan_read::FragmentId;
use novarocks_sql::plan_read::PartitionKind;
use novarocks_sql::planning::query_execution::SqlPreparedRuntimeFilterFacts;

use super::preparation::PreparedFragmentSet;

/// What an attempt reads about its plan after preparation has finished with
/// it.
///
/// These are values, not a borrow of the representation that produced them.
/// An attempt asks how rows move between fragments, whether any runtime
/// filter has a channel to deploy, and which write targets its root owns --
/// none of which requires knowing whether a sealed plan or a completed plan
/// built it. Projecting once, here, is what lets both answer.
/// One provider read of this plan, as the attempt that opens it reads it.
///
/// Everything here is a plan fact. The capability that performs the read is
/// not: it belongs to the attempt and is joined to these facts when the split
/// source is opened.
pub(crate) struct AttemptScanFacts {
    pub(crate) fragment_id: FragmentId,
    pub(crate) plan_node_id: i32,
    /// The provider columns this scan reads, in the order it produces them.
    pub(crate) assignments: Vec<ConnectorReadAssignment>,
    /// Which runtime filter constrains which provider column, already
    /// resolved against the assignments.
    pub(crate) dynamic_filters: Vec<(u32, ConnectorReadColumnHandle)>,
    pub(crate) constraint: ConnectorReadConstraint,
}

/// One exchange edge, as the pieces that place and connect tasks read it.
///
/// Four fields, which is what the task graph and the runtime-filter compiler
/// actually read. The sealed plan's own edge additionally carries the
/// analyzer expressions a hash partition was derived from and the payloads
/// of the CTE and change-stream edge kinds; nothing on this path reads
/// those, and a completed plan names its partition keys by value rather than
/// by expression, so carrying them would put something in the contract that
/// only one of the two representations can fill.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AttemptEdgeFacts {
    pub(crate) source_fragment_id: FragmentId,
    pub(crate) target_fragment_id: FragmentId,
    pub(crate) target_exchange_node_id: i32,
    pub(crate) partition_kind: PartitionKind,
}

impl AttemptEdgeFacts {
    fn from_fragment_edge(edge: &FragmentEdge) -> Self {
        Self {
            source_fragment_id: edge.source_fragment_id,
            target_fragment_id: edge.target_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
            partition_kind: edge.output_partition.kind,
        }
    }
}

pub(crate) struct AttemptPlanFacts {
    scheduling: super::fragment_scheduling::FragmentSchedulingFacts,
    edges: Box<[AttemptEdgeFacts]>,
    scans: Box<[AttemptScanFacts]>,
    runtime_filters: SqlPreparedRuntimeFilterFacts,
    write_root_targets: Option<Box<[WriteTargetOrdinal]>>,
}

impl AttemptPlanFacts {
    pub(crate) fn from_prepared(
        scheduling: super::fragment_scheduling::FragmentSchedulingFacts,
        prepared: &PreparedFragmentSet,
    ) -> Self {
        Self {
            scheduling,
            edges: prepared
                .scheduling_view()
                .edges()
                .iter()
                .map(AttemptEdgeFacts::from_fragment_edge)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            runtime_filters: prepared.runtime_filter_facts().clone(),
            scans: prepared
                .scan_bindings()
                .typed_scans()
                .map(|(fragment_id, plan_node_id, scan)| AttemptScanFacts {
                    fragment_id,
                    plan_node_id,
                    assignments: scan.prepared.table_scan.assignments().to_vec(),
                    dynamic_filters: super::split_assignment_round::feedback_bindings(
                        &scan.prepared.table_scan,
                    ),
                    constraint: scan.prepared.constraint.clone(),
                })
                .collect(),
            write_root_targets: prepared
                .write_root_targets()
                .map(<[WriteTargetOrdinal]>::to_vec)
                .map(Vec::into_boxed_slice),
        }
    }

    /// The facts scheduling reads, projected once from whichever
    /// representation built this execution.
    pub(crate) const fn scheduling(&self) -> &super::fragment_scheduling::FragmentSchedulingFacts {
        &self.scheduling
    }

    /// Every provider read this plan performs, in plan order.
    pub(crate) const fn scans(&self) -> &[AttemptScanFacts] {
        &self.scans
    }

    pub(crate) fn scan(
        &self,
        fragment_id: FragmentId,
        plan_node_id: i32,
    ) -> Option<&AttemptScanFacts> {
        self.scans
            .iter()
            .find(|scan| scan.fragment_id == fragment_id && scan.plan_node_id == plan_node_id)
    }

    /// The exchange edges of this plan, in the planner's own order.
    pub(crate) const fn edges(&self) -> &[AttemptEdgeFacts] {
        &self.edges
    }

    pub(crate) const fn runtime_filters(&self) -> &SqlPreparedRuntimeFilterFacts {
        &self.runtime_filters
    }

    pub(crate) fn write_root_targets(&self) -> Option<&[WriteTargetOrdinal]> {
        self.write_root_targets.as_deref()
    }
}
