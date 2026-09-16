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

//! What deciding how many backends run each fragment actually needs.
//!
//! Scheduling reads a narrow set of facts: which fragments exist and in what
//! order, which of them read a provider and how that read receives its work,
//! and how rows move between fragments. It reads none of the plan's semantics,
//! so it is given none - which is what lets one scheduler serve a plan in any
//! shape, and what keeps a change in plan representation from reaching it.

use std::collections::BTreeMap;

use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;
use novarocks_sql::plan_read::FragmentId;

/// How rows move across one fragment boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

/// One provider read, as scheduling sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedulingScanFacts {
    pub node_id: i32,
    /// The pieces of work this scan is already known to have, to spread across
    /// its instances.
    ///
    /// Ordinarily empty: enumeration belongs to the attempt, not to
    /// preparation, and a scan that starts with no work still has to be
    /// admitted so it can be told there is none.
    pub ranges: Vec<ScanRangeParams>,
    /// Runtime splits can use every admitted task; a whole relation has no
    /// split and must run on exactly one backend. Losing the distinction here
    /// would duplicate a direct metadata read on every live backend.
    pub work_source: Option<ConnectorReadWorkSource>,
}

/// One fragment, as scheduling sees it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchedulingFragmentFacts {
    pub scans: Vec<SchedulingScanFacts>,
}

/// One edge, as scheduling sees it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulingEdgeFacts {
    pub source: FragmentId,
    pub target: FragmentId,
    /// The exchange node in the target fragment that receives this edge.
    pub target_exchange_node_id: i32,
    pub native_hash_partitioned: bool,
    pub stream_kind: SchedulingStreamKind,
}

/// Everything scheduling reads about one plan.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentSchedulingFacts {
    /// The artifact these facts were projected from, so a schedule cannot be
    /// paired with another preparation's fragments.
    pub handoff_id: u64,
    /// Producers before consumers. Scheduling walks this order so a fragment's
    /// parallelism can depend on the fragments that feed it.
    pub order: Vec<FragmentId>,
    pub anchor: FragmentId,
    pub fragments: BTreeMap<FragmentId, SchedulingFragmentFacts>,
    pub edges: Vec<SchedulingEdgeFacts>,
}

impl FragmentSchedulingFacts {
    pub fn fragment(&self, fragment_id: FragmentId) -> Option<&SchedulingFragmentFacts> {
        self.fragments.get(&fragment_id)
    }

    /// The work one scan is already known to have. Absent means the plan has
    /// no such scan; empty means it has one that starts with no work, which
    /// still has to be admitted so it can be told there is none.
    pub fn scan_ranges(&self, fragment_id: FragmentId, node_id: i32) -> Option<&[ScanRangeParams]> {
        self.fragment(fragment_id)?
            .scans
            .iter()
            .find(|scan| scan.node_id == node_id)
            .map(|scan| scan.ranges.as_slice())
    }
}

impl SchedulingFragmentFacts {
    pub fn has_scans(&self) -> bool {
        !self.scans.is_empty()
    }
}

impl SchedulingScanFacts {
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }
}
