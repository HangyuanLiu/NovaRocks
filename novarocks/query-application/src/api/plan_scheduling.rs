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

use std::collections::BTreeMap;

use novarocks_physical_plan::PlanVersionId;
use novarocks_sql::plan_read::{FragmentId, FragmentStreamKind};
use novarocks_sql::planning::query_execution::{
    SealedPreparationPlanId, SealedScanIdentity, SqlExecutionSchedulingFacts,
};

use super::NativeScanWork;

/// Which plan a scan belongs to.
///
/// Two plans' scans must never join to each other, and the two
/// representations seal themselves differently: a sealed preparation plan
/// mints a process-local id, a completed plan carries its own version. Naming
/// both here keeps a value minted by one from ever comparing equal to a value
/// minted by the other.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PlanSeal {
    Sealed(SealedPreparationPlanId),
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
    fragment_id: FragmentId,
    node_id: i32,
}

impl PlanScanIdentity {
    pub const fn new(plan: PlanSeal, fragment_id: FragmentId, node_id: i32) -> Self {
        Self {
            plan,
            fragment_id,
            node_id,
        }
    }

    pub const fn plan(self) -> PlanSeal {
        self.plan
    }

    pub const fn fragment_id(self) -> FragmentId {
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
    pub fragment_id: FragmentId,
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
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub stream_kind: SchedulingStreamKind,
    pub hash_partitioned: bool,
}

/// Everything scheduling reads about one plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSchedulingFacts {
    pub topological_fragment_order: Vec<FragmentId>,
    pub execution_anchor_fragment_id: FragmentId,
    pub fragments: Vec<FragmentSchedulingFacts>,
    pub edges: Vec<SchedulingEdgeFacts>,
}

impl ExecutionSchedulingFacts {
    /// The same facts, from a sealed preparation plan's own scheduling
    /// projection and the work its owner enumerated for each scan.
    ///
    /// The plan states which fragments exist and how they feed each other;
    /// how much work a provider read starts with is not in the plan, because
    /// only whoever asked the provider knows it. Joining them here is what
    /// keeps a scan the plan declares from silently running with another
    /// plan's work: a scan with no entry is refused rather than defaulted to
    /// empty.
    pub fn from_sealed(
        sealed: &SqlExecutionSchedulingFacts,
        work: &BTreeMap<SealedScanIdentity, NativeScanWork>,
    ) -> Result<Self, String> {
        let fragments = sealed
            .fragments()
            .iter()
            .map(|fragment| {
                let scans = fragment
                    .scans()
                    .iter()
                    .map(|scan| {
                        let work = work.get(scan).copied().ok_or_else(|| {
                            format!("sealed scan node {} has no enumerated work", scan.node_id())
                        })?;
                        Ok(ScanSchedulingFacts {
                            scan: PlanScanIdentity::new(
                                PlanSeal::Sealed(scan.plan()),
                                fragment.fragment_id(),
                                scan.node_id(),
                            ),
                            work,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(FragmentSchedulingFacts {
                    fragment_id: fragment.fragment_id(),
                    scans,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            topological_fragment_order: sealed.topological_fragment_order().to_vec(),
            execution_anchor_fragment_id: sealed.execution_anchor_fragment_id(),
            fragments,
            edges: sealed
                .edges()
                .iter()
                .map(|edge| SchedulingEdgeFacts {
                    source_fragment_id: edge.source_fragment_id(),
                    target_fragment_id: edge.target_fragment_id(),
                    target_exchange_node_id: edge.target_exchange_node_id(),
                    stream_kind: match edge.stream_kind() {
                        FragmentStreamKind::Gather => SchedulingStreamKind::Gather,
                        FragmentStreamKind::Broadcast => SchedulingStreamKind::Broadcast,
                        FragmentStreamKind::Partitioned => SchedulingStreamKind::Partitioned,
                        FragmentStreamKind::Other => SchedulingStreamKind::Other,
                    },
                    hash_partitioned: edge.is_hash_partitioned(),
                })
                .collect(),
        })
    }
}
