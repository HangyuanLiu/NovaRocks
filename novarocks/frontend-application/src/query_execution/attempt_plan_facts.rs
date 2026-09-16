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

use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_sql::plan_read::FragmentEdge;
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
pub(crate) struct AttemptPlanFacts {
    edges: Box<[FragmentEdge]>,
    runtime_filters: SqlPreparedRuntimeFilterFacts,
    write_root_targets: Option<Box<[WriteTargetOrdinal]>>,
}

impl AttemptPlanFacts {
    pub(crate) fn from_prepared(prepared: &PreparedFragmentSet) -> Self {
        Self {
            edges: prepared
                .scheduling_view()
                .edges()
                .to_vec()
                .into_boxed_slice(),
            runtime_filters: prepared.runtime_filter_facts().clone(),
            write_root_targets: prepared
                .write_root_targets()
                .map(<[WriteTargetOrdinal]>::to_vec)
                .map(Vec::into_boxed_slice),
        }
    }

    /// The exchange edges of this plan, in the planner's own order.
    pub(crate) const fn edges(&self) -> &[FragmentEdge] {
        &self.edges
    }

    pub(crate) const fn runtime_filters(&self) -> &SqlPreparedRuntimeFilterFacts {
        &self.runtime_filters
    }

    pub(crate) fn write_root_targets(&self) -> Option<&[WriteTargetOrdinal]> {
        self.write_root_targets.as_deref()
    }
}
