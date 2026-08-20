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

use super::DistributedPlan;
use super::activation_decision::DraftRuntimeFilterGraph;
use super::fragment::{DistributedPlanDraft, FragmentEdge, FragmentId, PlanFragment};

/// Planner-owned test fixture that constructs a draft and seals it through the
/// same production entrypoint. It never exposes mutable sealed-plan state.
pub(crate) struct DistributedPlanDraftBuilder {
    draft: DistributedPlanDraft,
}

impl DistributedPlanDraftBuilder {
    pub(crate) fn new(
        fragments: Vec<PlanFragment>,
        root_fragment_id: Option<FragmentId>,
        edges: Vec<FragmentEdge>,
        runtime_filter_graph: DraftRuntimeFilterGraph,
    ) -> Self {
        Self {
            draft: DistributedPlanDraft {
                fragments,
                root_fragment_id,
                edges,
                runtime_filter_graph,
            },
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for distributed-plan fixture mutation in focused tests."
    )]
    pub(crate) fn fragments_mut(&mut self) -> &mut Vec<PlanFragment> {
        &mut self.draft.fragments
    }

    #[allow(
        dead_code,
        reason = "Retained for distributed-plan fixture inspection in focused tests."
    )]
    pub(crate) fn fragments(&self) -> &[PlanFragment] {
        &self.draft.fragments
    }

    #[allow(
        dead_code,
        reason = "Retained for distributed-plan fixture mutation in focused tests."
    )]
    pub(crate) fn edges_mut(&mut self) -> &mut Vec<FragmentEdge> {
        &mut self.draft.edges
    }

    #[allow(
        dead_code,
        reason = "Retained for runtime-filter fixture construction in focused tests."
    )]
    pub(crate) fn set_runtime_filter_graph(&mut self, graph: DraftRuntimeFilterGraph) {
        self.draft.runtime_filter_graph = graph;
    }

    pub(crate) fn seal(self) -> Result<DistributedPlan, String> {
        super::seal::seal_draft(self.draft).map_err(|error| error.to_string())
    }

    pub(in crate::planner::distributed) fn into_draft(self) -> DistributedPlanDraft {
        self.draft
    }
}

pub(crate) fn draft_builder_from_plan(
    plan: &DistributedPlan,
    runtime_filter_graph: DraftRuntimeFilterGraph,
) -> DistributedPlanDraftBuilder {
    DistributedPlanDraftBuilder::new(
        plan.fragments().to_vec(),
        Some(plan.root_fragment_id()),
        plan.edges().to_vec(),
        runtime_filter_graph,
    )
}

#[allow(
    dead_code,
    reason = "Retained for distributed-plan fixture mutation in focused tests."
)]
pub(crate) fn rebuild_test_plan(
    plan: DistributedPlan,
    runtime_filter_graph: DraftRuntimeFilterGraph,
    mutate: impl FnOnce(&mut DistributedPlanDraftBuilder),
) -> DistributedPlan {
    let mut builder = draft_builder_from_plan(&plan, runtime_filter_graph);
    mutate(&mut builder);
    builder
        .seal()
        .expect("rebuilt test distributed plan must pass production minimal seal")
}

#[allow(
    dead_code,
    reason = "Retained for output-catalog fault fixtures in focused tests."
)]
pub(crate) fn remove_fragment_output_for_test(plan: &mut DistributedPlan, fragment_id: FragmentId) {
    plan.remove_fragment_output_for_test(fragment_id);
}

#[allow(
    unused_macros,
    reason = "The draft-builder macro is re-exported for distributed planner tests in sibling modules."
)]
macro_rules! distributed_plan_draft_builder_for_test {
    (
        $fragments:ident,
        root_fragment_id: $root_fragment_id:expr,
        edges: $edges:expr,
        runtime_filter_graph: $runtime_filter_graph:expr $(,)?
    ) => {
        $crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
            $fragments,
            Some($root_fragment_id),
            $edges,
            $runtime_filter_graph,
        )
    };
    (
        $fragments:ident,
        root_fragment_id: $root_fragment_id:expr,
        runtime_filter_graph: $runtime_filter_graph:expr,
        edges: $edges:expr $(,)?
    ) => {
        $crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
            $fragments,
            Some($root_fragment_id),
            $edges,
            $runtime_filter_graph,
        )
    };
    (
        fragments: $fragments:expr,
        root_fragment_id: $root_fragment_id:expr,
        edges: $edges:expr,
        runtime_filter_graph: $runtime_filter_graph:expr $(,)?
    ) => {
        $crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
            $fragments,
            Some($root_fragment_id),
            $edges,
            $runtime_filter_graph,
        )
    };
    (
        fragments: $fragments:expr,
        root_fragment_id: $root_fragment_id:expr,
        runtime_filter_graph: $runtime_filter_graph:expr,
        edges: $edges:expr $(,)?
    ) => {
        $crate::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
            $fragments,
            Some($root_fragment_id),
            $edges,
            $runtime_filter_graph,
        )
    };
}

#[allow(
    unused_imports,
    reason = "The macro re-export is the stable fixture entrypoint for distributed planner tests."
)]
pub(crate) use distributed_plan_draft_builder_for_test;

#[allow(
    unused_macros,
    reason = "The sealed-plan macro is re-exported for distributed planner tests in sibling modules."
)]
macro_rules! distributed_plan_for_test {
    ($($tokens:tt)*) => {
        $crate::planner::distributed::test_support::distributed_plan_draft_builder_for_test! {
            $($tokens)*
        }
        .seal()
        .expect("test distributed plan must pass production minimal seal")
    };
}

#[allow(
    unused_imports,
    reason = "The macro re-export is the stable sealed-plan fixture entrypoint for distributed planner tests."
)]
pub(crate) use distributed_plan_for_test;
