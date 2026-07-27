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

use crate::runtime_filter::model::contract::ConsumerActivation;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;

use super::DistributedPlan;
use super::activation_decision::{
    ActivationConstraint, ActivationFallback, DraftRuntimeFilterGraph, RequiredLiveReason,
};
use super::fragment::{DistributedPlanDraft, FragmentEdge, FragmentId, PlanFragment};

fn into_draft_runtime_filter_graph(graph: RuntimeFilterGraph) -> DraftRuntimeFilterGraph {
    graph
        .map_consumer_activations(|_, _, _, activation| {
            Ok::<_, std::convert::Infallible>(match activation {
                ConsumerActivation::BlockingSnapshot => ActivationConstraint::BlockingOrBatchLive {
                    fallback: ActivationFallback::BlockingSnapshot,
                },
                ConsumerActivation::NonBlockingLive { late_apply } => {
                    ActivationConstraint::LiveOnly {
                        late_apply: *late_apply,
                        reason: RequiredLiveReason::OrderedBoundContract,
                    }
                }
            })
        })
        .expect("infallible test graph activation projection")
}

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
        runtime_filter_graph: RuntimeFilterGraph,
    ) -> Self {
        Self {
            draft: DistributedPlanDraft {
                fragments,
                root_fragment_id,
                edges,
                runtime_filter_graph: into_draft_runtime_filter_graph(runtime_filter_graph),
            },
        }
    }

    pub(crate) fn fragments_mut(&mut self) -> &mut Vec<PlanFragment> {
        &mut self.draft.fragments
    }

    pub(crate) fn fragments(&self) -> &[PlanFragment] {
        &self.draft.fragments
    }

    pub(crate) fn edges_mut(&mut self) -> &mut Vec<FragmentEdge> {
        &mut self.draft.edges
    }

    pub(crate) fn set_runtime_filter_graph(&mut self, graph: RuntimeFilterGraph) {
        self.draft.runtime_filter_graph = into_draft_runtime_filter_graph(graph);
    }

    pub(crate) fn seal(self) -> Result<DistributedPlan, String> {
        super::seal::seal_draft(self.draft).map_err(|error| error.to_string())
    }

    pub(in crate::sql::planner::distributed) fn into_draft(self) -> DistributedPlanDraft {
        self.draft
    }
}

pub(crate) fn draft_builder_from_plan(plan: &DistributedPlan) -> DistributedPlanDraftBuilder {
    DistributedPlanDraftBuilder::new(
        plan.fragments().to_vec(),
        Some(plan.root_fragment_id()),
        plan.edges().to_vec(),
        plan.runtime_filter_graph().clone(),
    )
}

pub(crate) fn rebuild_test_plan(
    plan: DistributedPlan,
    mutate: impl FnOnce(&mut DistributedPlanDraftBuilder),
) -> DistributedPlan {
    let mut builder = draft_builder_from_plan(&plan);
    mutate(&mut builder);
    builder
        .seal()
        .expect("rebuilt test distributed plan must pass production minimal seal")
}

pub(crate) fn remove_fragment_output_for_test(plan: &mut DistributedPlan, fragment_id: FragmentId) {
    plan.remove_fragment_output_for_test(fragment_id);
}

macro_rules! distributed_plan_draft_builder_for_test {
    (
        $fragments:ident,
        root_fragment_id: $root_fragment_id:expr,
        edges: $edges:expr,
        runtime_filter_graph: $runtime_filter_graph:expr $(,)?
    ) => {
        $crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
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
        $crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
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
        $crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
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
        $crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder::new(
            $fragments,
            Some($root_fragment_id),
            $edges,
            $runtime_filter_graph,
        )
    };
}

pub(crate) use distributed_plan_draft_builder_for_test;

macro_rules! distributed_plan_for_test {
    ($($tokens:tt)*) => {
        $crate::sql::planner::distributed::test_support::distributed_plan_draft_builder_for_test! {
            $($tokens)*
        }
        .seal()
        .expect("test distributed plan must pass production minimal seal")
    };
}

pub(crate) use distributed_plan_for_test;
