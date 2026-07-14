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

use std::fmt;

use crate::runtime_filter::model::graph::RuntimeFilterGraph;

use super::fragment::{DistributedPlanDraft, FragmentEdge, FragmentId, PlanFragment};
use super::validation::{self, DistributedPlanValidationError};

#[derive(Clone, Debug)]
struct DistributedPlanData {
    fragments: Vec<PlanFragment>,
    root_fragment_id: FragmentId,
    edges: Vec<FragmentEdge>,
    // RFD-5A will populate and consume this slot; remove the allowance at that cutover.
    #[allow(dead_code)]
    runtime_filter_graph: RuntimeFilterGraph,
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlan {
    data: DistributedPlanData,
}

impl DistributedPlan {
    pub(crate) fn fragments(&self) -> &[PlanFragment] {
        &self.data.fragments
    }

    pub(crate) fn root_fragment_id(&self) -> FragmentId {
        self.data.root_fragment_id
    }

    pub(crate) fn edges(&self) -> &[FragmentEdge] {
        &self.data.edges
    }

    pub(crate) fn runtime_filter_graph(&self) -> &RuntimeFilterGraph {
        &self.data.runtime_filter_graph
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum DistributedPlanSealError {
    EmptyFragments,
    MissingRootFragmentId,
    RootFragmentNotFound { root_fragment_id: FragmentId },
    Structural(DistributedPlanValidationError),
}

impl fmt::Display for DistributedPlanSealError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFragments => formatter.write_str("distributed plan has no fragments"),
            Self::MissingRootFragmentId => {
                formatter.write_str("distributed plan is missing root fragment id")
            }
            Self::RootFragmentNotFound { root_fragment_id } => write!(
                formatter,
                "distributed plan root fragment id={root_fragment_id} was not found"
            ),
            Self::Structural(error) => error.fmt(formatter),
        }
    }
}

pub(in crate::sql::planner::distributed) fn seal_draft(
    draft: DistributedPlanDraft,
) -> Result<DistributedPlan, DistributedPlanSealError> {
    let DistributedPlanDraft {
        fragments,
        root_fragment_id,
        edges,
        runtime_filter_graph,
    } = draft;
    if fragments.is_empty() {
        return Err(DistributedPlanSealError::EmptyFragments);
    }
    let root_fragment_id =
        root_fragment_id.ok_or(DistributedPlanSealError::MissingRootFragmentId)?;
    if !fragments
        .iter()
        .any(|fragment| fragment.fragment_id == root_fragment_id)
    {
        return Err(DistributedPlanSealError::RootFragmentNotFound { root_fragment_id });
    }
    validation::validate_distributed_structure(&fragments, root_fragment_id, &edges)
        .map_err(DistributedPlanSealError::Structural)?;
    // CGO-9A Task 4 will insert `runtime_filter_graph.validate()` here, between
    // structural validation and immutable construction.
    Ok(DistributedPlan {
        data: DistributedPlanData {
            fragments,
            root_fragment_id,
            edges,
            runtime_filter_graph,
        },
    })
}

#[cfg(test)]
pub(super) mod test_support {
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::planner::distributed::fragment::{
        DataPartition, DataSink, DistributedPlanDraft, PlanFragment,
    };
    use crate::sql::planner::distributed::node::{DistributedNode, DistributedNodeKind};
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    pub(super) fn single_fragment_draft(root_fragment_id: Option<u32>) -> DistributedPlanDraft {
        DistributedPlanDraft {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 1,
                    fragment_id: 0,
                    tuple_ids: Vec::new(),
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
                    build_runtime_filters: Vec::new(),
                    probe_runtime_filters: Vec::new(),
                    children: Vec::new(),
                    stats: PhysicalPlanStats {
                        output_row_count: 0.0,
                        row_count_confidence: PlannerConfidence::Fallback,
                        column_statistics: Default::default(),
                        cost_estimate: None,
                        broadcast_decision: None,
                    },
                    payload: DistributedNodeKind::Values(
                        crate::sql::planner::payload::PlanValuesNode {
                            rows: Vec::new(),
                            columns: Vec::new(),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id,
            edges: Vec::new(),
            runtime_filter_graph: RuntimeFilterGraph::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::planner::distributed::fragment::DistributedPlanDraft;

    use super::{DistributedPlanSealError, seal_draft};

    #[test]
    fn minimal_seal_rejects_empty_fragments_before_root_state() {
        let draft = DistributedPlanDraft {
            fragments: Vec::new(),
            root_fragment_id: None,
            edges: Vec::new(),
            runtime_filter_graph: RuntimeFilterGraph::default(),
        };

        let error = seal_draft(draft).expect_err("empty draft must not seal");

        assert!(matches!(error, DistributedPlanSealError::EmptyFragments));
        assert_eq!(error.to_string(), "distributed plan has no fragments");
    }

    #[test]
    fn minimal_seal_rejects_missing_root_id() {
        let draft = super::test_support::single_fragment_draft(None);

        let error = seal_draft(draft).expect_err("missing root id must not seal");

        assert!(matches!(
            error,
            DistributedPlanSealError::MissingRootFragmentId
        ));
        assert_eq!(
            error.to_string(),
            "distributed plan is missing root fragment id"
        );
    }

    #[test]
    fn minimal_seal_rejects_root_id_not_present_in_fragments() {
        let draft = super::test_support::single_fragment_draft(Some(7));

        let error = seal_draft(draft).expect_err("unknown root id must not seal");

        assert!(matches!(
            error,
            DistributedPlanSealError::RootFragmentNotFound {
                root_fragment_id: 7
            }
        ));
        assert_eq!(
            error.to_string(),
            "distributed plan root fragment id=7 was not found"
        );
    }

    #[test]
    fn minimal_seal_constructs_an_immutable_plan_with_read_only_accessors() {
        let plan = seal_draft(super::test_support::single_fragment_draft(Some(0)))
            .expect("valid draft seals");

        assert_eq!(plan.fragments().len(), 1);
        assert_eq!(plan.root_fragment_id(), 0);
        assert!(plan.edges().is_empty());
        assert!(plan.runtime_filter_graph().is_empty());
    }
}
