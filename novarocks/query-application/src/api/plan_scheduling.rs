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

//! What deciding how many tasks to run, and where, reads about one plan.
//!
//! Scheduling reads no semantics. It reads which fragments exist, in what
//! order, which of them read a provider and how much work that read starts
//! with, and how rows flow between fragments. Every one of those is a
//! property both plan representations have, so they are stated here as
//! values rather than reached for through whichever representation produced
//! them.

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[cfg(test)]
use novarocks_physical_plan::Distribution;
use novarocks_physical_plan::PlanVersionId;

use super::NativeScanWork;

/// Which plan a scan belongs to.
///
/// Two plan versions' scans must never join to each other.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PlanSeal {
    Version(PlanVersionId),
}

/// One provider read of one plan, as scheduling and work accounting name it.
///
/// It is a join key, not an address. The only thing done with it is to match
/// a scan the plan declared against the work that scan was given, and the
/// seal is what makes a mismatch impossible to express rather than merely
/// unlikely.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanScanIdentity {
    plan: PlanSeal,
    fragment_id: u32,
    node_id: i32,
}

impl PlanScanIdentity {
    pub const fn new(plan: PlanSeal, fragment_id: u32, node_id: i32) -> Self {
        Self {
            plan,
            fragment_id,
            node_id,
        }
    }

    pub const fn plan(self) -> PlanSeal {
        self.plan
    }

    pub const fn fragment_id(self) -> u32 {
        self.fragment_id
    }

    pub const fn node_id(self) -> i32 {
        self.node_id
    }
}

/// One provider read and the work it starts with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanSchedulingFacts {
    pub scan: PlanScanIdentity,
    pub work: NativeScanWork,
}

/// One fragment, as scheduling reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentSchedulingFacts {
    pub fragment_id: u32,
    pub scans: Vec<ScanSchedulingFacts>,
}

/// How rows move across one fragment boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

/// One exchange edge, as scheduling reads it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulingEdgeFacts {
    pub source_fragment_id: u32,
    pub target_fragment_id: u32,
    pub target_exchange_node_id: i32,
    pub stream_kind: SchedulingStreamKind,
    pub hash_partitioned: bool,
}

/// Everything scheduling reads about one plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSchedulingFacts {
    pub topological_fragment_order: Vec<u32>,
    pub execution_anchor_fragment_id: u32,
    pub fragments: Vec<FragmentSchedulingFacts>,
    pub edges: Vec<SchedulingEdgeFacts>,
}

#[cfg(test)]
impl ExecutionSchedulingFacts {
    /// Project a completed fixture for supervisor scheduling tests.
    pub(crate) fn from_frozen_description(
        description: &crate::preparation::FrozenExecutionDescription,
        work: &BTreeMap<PlanScanIdentity, NativeScanWork>,
    ) -> Result<Self, String> {
        let plan = description.completed_candidate().plan();
        let mut in_degree = plan
            .fragments()
            .keys()
            .map(|id| (id.get(), 0_usize))
            .collect::<BTreeMap<_, _>>();
        let mut consumers = BTreeMap::<u32, Vec<u32>>::new();
        let mut producers = BTreeSet::new();
        for edge in plan.edges().values() {
            let source = edge.source.fragment.get();
            let destination = edge.destination.fragment.get();
            *in_degree
                .get_mut(&destination)
                .ok_or("absent destination")? += 1;
            consumers.entry(source).or_default().push(destination);
            producers.insert(source);
        }
        let mut ready = in_degree
            .iter()
            .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
            .collect::<VecDeque<_>>();
        let mut order = Vec::with_capacity(in_degree.len());
        while let Some(fragment) = ready.pop_front() {
            order.push(fragment);
            for destination in consumers.get(&fragment).map_or(&[][..], Vec::as_slice) {
                let degree = in_degree.get_mut(destination).expect("known destination");
                *degree -= 1;
                if *degree == 0 {
                    ready.push_back(*destination);
                }
            }
        }
        if order.len() != in_degree.len() {
            return Err("fixture plan has cyclic fragments".to_string());
        }
        let mut terminals = in_degree
            .keys()
            .filter(|id| !producers.contains(id))
            .copied();
        let anchor = terminals.next().ok_or("fixture plan has no anchor")?;
        if terminals.next().is_some() {
            return Err("fixture plan has multiple anchors".to_string());
        }
        let mut fragments = plan
            .fragments()
            .keys()
            .map(|id| {
                (
                    id.get(),
                    FragmentSchedulingFacts {
                        fragment_id: id.get(),
                        scans: Vec::new(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut scans = Vec::new();
        for &scan in description.scan_identities() {
            let scan_work = work.get(&scan).copied().ok_or_else(|| {
                format!("frozen scan node {} has no enumerated work", scan.node_id())
            })?;
            scans.push(ScanSchedulingFacts {
                scan,
                work: scan_work,
            });
        }
        for scan in scans {
            fragments
                .get_mut(&scan.scan.fragment_id())
                .ok_or("scan names absent fragment")?
                .scans
                .push(scan);
        }
        Ok(Self {
            topological_fragment_order: order,
            execution_anchor_fragment_id: anchor,
            fragments: fragments.into_values().collect(),
            edges: plan
                .edges()
                .values()
                .map(|edge| SchedulingEdgeFacts {
                    source_fragment_id: edge.source.fragment.get(),
                    target_fragment_id: edge.destination.fragment.get(),
                    target_exchange_node_id: i32::try_from(edge.destination.node.get())
                        .expect("fixture node id fits wire"),
                    stream_kind: match edge.partitioning.destination {
                        Distribution::Singleton => SchedulingStreamKind::Gather,
                        Distribution::Broadcast => SchedulingStreamKind::Broadcast,
                        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
                            SchedulingStreamKind::Partitioned
                        }
                        Distribution::Unconstrained | Distribution::RoundRobin => {
                            SchedulingStreamKind::Other
                        }
                    },
                    hash_partitioned: matches!(
                        edge.partitioning.destination,
                        Distribution::Hash { .. }
                    ),
                })
                .collect(),
        })
    }
}
