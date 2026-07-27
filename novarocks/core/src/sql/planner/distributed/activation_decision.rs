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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[cfg(test)]
use std::cell::Cell;

use crate::novarocks_logging::debug;
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, ConsumerActivation, LateApplyGranularity,
};
#[cfg(test)]
use crate::runtime_filter::model::graph::PlanLocation;
use crate::runtime_filter::model::graph::{
    RuntimeFilterBindingRoleData, RuntimeFilterGraph, RuntimeFilterGraphData,
};
use crate::runtime_filter::model::join_progress::JoinBuildProgressCatalog;
use crate::runtime_filter::model::refined_wait_graph::{
    ConsumerWaitBehavior, CycleStep, RefinedFragmentEdge, RefinedWaitGraphBuildError,
    build_refined_wait_graph, project_consumer_waits,
};
use crate::runtime_filter::model::validation::{ActivationContract, GraphValidationError};

use super::fragment::PlanFragment;
use super::runtime_filter_progress::build_join_progress_proof_catalog;
use super::validation::{
    RuntimeFilterPlanValidationError, validate_runtime_filter_graph_against_plan,
};

pub(super) type DraftRuntimeFilterGraph = RuntimeFilterGraphData<ActivationConstraint>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivationConstraint {
    LiveOnly {
        late_apply: LateApplyGranularity,
        reason: RequiredLiveReason,
    },
    BlockingOrBatchLive {
        fallback: ActivationFallback,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivationFallback {
    BlockingSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequiredLiveReason {
    OrderedBoundContract,
    FencedFinalDomainContract,
}

impl ActivationContract for ActivationConstraint {
    fn satisfies_required_non_blocking(&self) -> bool {
        matches!(self, Self::LiveOnly { .. })
    }
}

#[derive(Clone, Debug)]
pub(super) struct ActivationDecisionOutput {
    pub(super) graph: RuntimeFilterGraph,
    pub(super) decisions: ActivationDecisionCatalog,
    pub(super) join_progress: JoinBuildProgressCatalog,
}

pub(super) type ActivationDecisionCatalog = BTreeMap<BindingId, ActivationDecision>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActivationDecision {
    pub(super) channel: ChannelId,
    pub(super) consumer_binding: BindingId,
    pub(super) consumer_fragment: u32,
    pub(super) activation: ConsumerActivation,
    pub(super) reason: ActivationDecisionReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ActivationDecisionReason {
    RequiredByContract {
        reason: RequiredLiveReason,
    },
    CycleForced {
        producer_bindings: Vec<BindingId>,
        witness: Vec<CycleStep>,
    },
    ConservativeFallback,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ActivationDecisionError {
    DraftGraph(GraphValidationError),
    DraftPlan(RuntimeFilterPlanValidationError),
    RefinedGraph(RefinedWaitGraphBuildError),
    DuplicateDecision(BindingId),
    MissingDecision(BindingId),
    FinalGraph(GraphValidationError),
    FinalPlan(RuntimeFilterPlanValidationError),
}

impl fmt::Display for ActivationDecisionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DraftGraph(error) => {
                write!(formatter, "draft runtime-filter graph is invalid: {error}")
            }
            Self::DraftPlan(error) => {
                write!(formatter, "draft runtime-filter plan is invalid: {error}")
            }
            Self::RefinedGraph(error) => {
                write!(
                    formatter,
                    "runtime-filter activation analysis failed: {error:?}"
                )
            }
            Self::DuplicateDecision(binding) => write!(
                formatter,
                "runtime-filter activation produced duplicate decision for binding={}",
                binding.get()
            ),
            Self::MissingDecision(binding) => write!(
                formatter,
                "runtime-filter activation is missing decision for binding={}",
                binding.get()
            ),
            Self::FinalGraph(error) => {
                write!(formatter, "sealed runtime-filter graph is invalid: {error}")
            }
            Self::FinalPlan(error) => {
                write!(formatter, "sealed runtime-filter plan is invalid: {error}")
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CycleDecision {
    channel: Option<ChannelId>,
    consumer_fragment: Option<u32>,
    producer_bindings: BTreeSet<BindingId>,
    witness: BTreeSet<CycleStep>,
}

#[cfg(test)]
thread_local! {
    static FORCE_FINAL_PLAN_FAILURE: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
fn force_final_plan_failure_for_test() {
    FORCE_FINAL_PLAN_FAILURE.with(|enabled| enabled.set(true));
}

#[cfg(test)]
fn take_final_plan_failure_for_test() -> bool {
    FORCE_FINAL_PLAN_FAILURE.with(|enabled| enabled.replace(false))
}

pub(super) struct ActivationDecisionPass;

impl ActivationDecisionPass {
    pub(super) fn run(
        draft: DraftRuntimeFilterGraph,
        fragments: &[PlanFragment],
        edges: &[RefinedFragmentEdge],
    ) -> Result<ActivationDecisionOutput, ActivationDecisionError> {
        draft
            .validate()
            .map_err(ActivationDecisionError::DraftGraph)?;
        validate_runtime_filter_graph_against_plan(&draft, fragments)
            .map_err(ActivationDecisionError::DraftPlan)?;
        let join_progress = build_join_progress_proof_catalog(fragments, &draft);
        Self::decide_and_materialize(draft, fragments, edges, join_progress)
    }

    #[cfg(test)]
    fn run_with_join_progress_for_test(
        draft: DraftRuntimeFilterGraph,
        fragments: &[PlanFragment],
        edges: &[RefinedFragmentEdge],
        join_progress: JoinBuildProgressCatalog,
    ) -> Result<ActivationDecisionOutput, ActivationDecisionError> {
        draft
            .validate()
            .map_err(ActivationDecisionError::DraftGraph)?;
        validate_runtime_filter_graph_against_plan(&draft, fragments)
            .map_err(ActivationDecisionError::DraftPlan)?;
        Self::decide_and_materialize(draft, fragments, edges, join_progress)
    }

    fn decide_and_materialize(
        draft: DraftRuntimeFilterGraph,
        fragments: &[PlanFragment],
        edges: &[RefinedFragmentEdge],
        join_progress: JoinBuildProgressCatalog,
    ) -> Result<ActivationDecisionOutput, ActivationDecisionError> {
        let projected = project_consumer_waits(&draft, |constraint| match constraint {
            ActivationConstraint::LiveOnly { .. } => ConsumerWaitBehavior::NeverBlocks,
            ActivationConstraint::BlockingOrBatchLive { .. } => {
                ConsumerWaitBehavior::BlocksUntilComplete
            }
        });
        let consumer_fragments = projected
            .iter()
            .map(|consumer| {
                (
                    (consumer.channel, consumer.binding),
                    consumer.consumer_fragment,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let refined = build_refined_wait_graph(edges, &projected, &join_progress)
            .map_err(ActivationDecisionError::RefinedGraph)?;
        let mut cycle_decisions = BTreeMap::<BindingId, CycleDecision>::new();
        for scc in refined.pure_blocking_sccs() {
            for wait in &scc.waits {
                let Some(consumer_fragment) = consumer_fragments
                    .get(&(wait.channel, wait.consumer_binding))
                    .copied()
                else {
                    return Err(ActivationDecisionError::MissingDecision(
                        wait.consumer_binding,
                    ));
                };
                let decision = cycle_decisions.entry(wait.consumer_binding).or_default();
                if let Some(channel) = decision.channel {
                    if channel != wait.channel {
                        return Err(ActivationDecisionError::DuplicateDecision(
                            wait.consumer_binding,
                        ));
                    }
                } else {
                    decision.channel = Some(wait.channel);
                }
                if let Some(fragment) = decision.consumer_fragment {
                    if fragment != consumer_fragment {
                        return Err(ActivationDecisionError::DuplicateDecision(
                            wait.consumer_binding,
                        ));
                    }
                } else {
                    decision.consumer_fragment = Some(consumer_fragment);
                }
                decision.producer_bindings.insert(wait.producer_binding);
                decision.witness.extend(scc.witness.iter().copied());
            }
        }

        let expected_consumers = draft
            .bindings()
            .filter_map(|binding| {
                matches!(binding.role, RuntimeFilterBindingRoleData::Consumer(_))
                    .then_some(binding.binding_id)
            })
            .collect::<BTreeSet<_>>();
        let mut decisions = ActivationDecisionCatalog::new();
        let graph = draft.map_consumer_activations(|binding, channel, location, constraint| {
            let (activation, reason) = match constraint {
                ActivationConstraint::LiveOnly { late_apply, reason } => (
                    ConsumerActivation::NonBlockingLive {
                        late_apply: *late_apply,
                    },
                    ActivationDecisionReason::RequiredByContract { reason: *reason },
                ),
                ActivationConstraint::BlockingOrBatchLive { fallback } => {
                    if let Some(cycle) = cycle_decisions.get(&binding) {
                        debug_assert_eq!(cycle.channel, Some(channel));
                        debug_assert_eq!(cycle.consumer_fragment, Some(location.fragment_id.get()));
                        (
                            ConsumerActivation::NonBlockingLive {
                                late_apply: LateApplyGranularity::Batch,
                            },
                            ActivationDecisionReason::CycleForced {
                                producer_bindings: cycle
                                    .producer_bindings
                                    .iter()
                                    .copied()
                                    .collect(),
                                witness: cycle.witness.iter().copied().collect(),
                            },
                        )
                    } else {
                        match fallback {
                            ActivationFallback::BlockingSnapshot => (
                                ConsumerActivation::BlockingSnapshot,
                                ActivationDecisionReason::ConservativeFallback,
                            ),
                        }
                    }
                }
            };
            let decision = ActivationDecision {
                channel,
                consumer_binding: binding,
                consumer_fragment: location.fragment_id.get(),
                activation,
                reason,
            };
            if decisions.insert(binding, decision).is_some() {
                return Err(ActivationDecisionError::DuplicateDecision(binding));
            }
            Ok(activation)
        })?;

        for binding in &expected_consumers {
            if !decisions.contains_key(binding) {
                return Err(ActivationDecisionError::MissingDecision(*binding));
            }
        }
        if let Some(binding) = decisions
            .keys()
            .find(|binding| !expected_consumers.contains(binding))
            .copied()
        {
            return Err(ActivationDecisionError::DuplicateDecision(binding));
        }

        #[cfg(test)]
        let mut graph = graph;

        #[cfg(test)]
        if take_final_plan_failure_for_test()
            && let Some(binding) = expected_consumers.first()
        {
            let fragment_id = graph
                .binding(*binding)
                .expect("decision binding remains in materialized graph")
                .location
                .fragment_id;
            graph
                .binding_mut_for_test(*binding)
                .expect("decision binding remains in materialized graph")
                .location = PlanLocation {
                fragment_id,
                node_id: crate::runtime_filter::model::contract::PlanNodeId::new(i32::MAX),
            };
        }

        graph
            .validate()
            .map_err(ActivationDecisionError::FinalGraph)?;
        validate_runtime_filter_graph_against_plan(&graph, fragments)
            .map_err(ActivationDecisionError::FinalPlan)?;
        log_decisions(&decisions);
        Ok(ActivationDecisionOutput {
            graph,
            decisions,
            join_progress,
        })
    }
}

fn log_decisions(decisions: &ActivationDecisionCatalog) {
    for decision in decisions.values() {
        debug!(
            "runtime-filter activation decision: channel={} consumer_binding={} consumer_fragment={} activation={:?} reason={:?}",
            decision.channel.get(),
            decision.consumer_binding.get(),
            decision.consumer_fragment,
            decision.activation,
            decision.reason,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, NullSemantics, PlanFragmentId, PlanNodeId,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirementData, PlanLocation,
        ProducerBindingTarget, ProducerRequirement, RuntimeFilterBindingRoleData,
        RuntimeFilterBindingSpecData, RuntimeFilterChannelSpec,
    };
    use crate::runtime_filter::model::join_progress::{
        FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
        JoinBuildProgressSkip,
    };
    use crate::runtime_filter::model::refined_wait_graph::RefinedFragmentEdge;
    use crate::runtime_filter::model::validation::GraphValidationErrorKind;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::distributed::fragment::{DataPartition, DataSink, PlanFragment};
    use crate::sql::planner::distributed::node::{DistributedNode, DistributedNodeKind};
    use crate::sql::planner::distributed::validation::RuntimeFilterPlanValidationError;
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    fn expression() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn channel(channel: u32) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id: ChannelId::new(channel),
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 30_000,
                max_retries: 3,
            },
        }
    }

    fn producer(
        binding: u32,
        channel: u32,
        fragment: u32,
        node: i32,
    ) -> RuntimeFilterBindingSpecData<ActivationConstraint> {
        RuntimeFilterBindingSpecData {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: Some(CoverageWitnessId::new(1)),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(node),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        }
    }

    fn consumer_with(
        binding: u32,
        channel: u32,
        fragment: u32,
        activation: ActivationConstraint,
    ) -> RuntimeFilterBindingSpecData<ActivationConstraint> {
        RuntimeFilterBindingSpecData {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(fragment as i32 * 10),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation,
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        }
    }

    fn consumer(
        binding: u32,
        channel: u32,
        fragment: u32,
    ) -> RuntimeFilterBindingSpecData<ActivationConstraint> {
        consumer_with(
            binding,
            channel,
            fragment,
            ActivationConstraint::BlockingOrBatchLive {
                fallback: ActivationFallback::BlockingSnapshot,
            },
        )
    }

    fn edge(source: u32, target: u32, exchange: i32) -> RefinedFragmentEdge {
        RefinedFragmentEdge {
            source_fragment: source,
            target_fragment: target,
            target_exchange_node: exchange,
        }
    }

    fn q23_edges(offset: u32) -> Vec<RefinedFragmentEdge> {
        let exchange_offset = offset as i32 * 10;
        vec![
            edge(1 + offset, 4 + offset, 21 + exchange_offset),
            edge(1 + offset, 3 + offset, 22 + exchange_offset),
            edge(4 + offset, 2 + offset, 20 + exchange_offset),
            edge(3 + offset, 5 + offset, 24 + exchange_offset),
            edge(5 + offset, 2 + offset, 23 + exchange_offset),
            edge(5 + offset, 4 + offset, 25 + exchange_offset),
        ]
    }

    fn proof(
        channel: u32,
        producer_binding: u32,
        offset: u32,
        join_node: i32,
    ) -> JoinBuildProgressProof {
        let exchange_offset = offset as i32 * 10;
        JoinBuildProgressProof {
            channel: ChannelId::new(channel),
            producer_binding: BindingId::new(producer_binding),
            producer_fragment: 2 + offset,
            join_node_id: join_node,
            build_frontier: vec![FrontierEdge {
                source_fragment: 4 + offset,
                target_exchange_node: 20 + exchange_offset,
            }],
            non_build_inputs: vec![FrontierEdge {
                source_fragment: 5 + offset,
                target_exchange_node: 23 + exchange_offset,
            }],
        }
    }

    fn graph(
        bindings: Vec<RuntimeFilterBindingSpecData<ActivationConstraint>>,
    ) -> DraftRuntimeFilterGraph {
        let channels: BTreeSet<ChannelId> =
            bindings.iter().map(|binding| binding.channel_id).collect();
        let mut graph = DraftRuntimeFilterGraph::default();
        for channel_id in channels {
            graph.insert_channel(channel(channel_id.get())).unwrap();
        }
        for binding in bindings {
            graph.insert_binding(binding).unwrap();
        }
        graph
    }

    fn catalog(proofs: Vec<JoinBuildProgressProof>) -> JoinBuildProgressCatalog {
        proofs
            .into_iter()
            .map(|proof| {
                (
                    (
                        proof.channel,
                        proof.producer_binding,
                        proof.producer_fragment,
                    ),
                    proof,
                )
            })
            .collect()
    }

    fn node(
        node_id: i32,
        fragment_id: u32,
        runtime_filter_binding_ids: Vec<BindingId>,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids,
            children,
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
        }
    }

    fn fragments_for(
        graph: &DraftRuntimeFilterGraph,
        edges: &[RefinedFragmentEdge],
    ) -> Vec<PlanFragment> {
        let mut fragment_ids = BTreeSet::new();
        for edge in edges {
            fragment_ids.insert(edge.source_fragment);
            fragment_ids.insert(edge.target_fragment);
        }
        let mut by_location: BTreeMap<(u32, i32), Vec<BindingId>> = BTreeMap::new();
        for binding in graph.bindings() {
            fragment_ids.insert(binding.location.fragment_id.get());
            by_location
                .entry((
                    binding.location.fragment_id.get(),
                    binding.location.node_id.get(),
                ))
                .or_default()
                .push(binding.binding_id);
        }
        fragment_ids
            .into_iter()
            .map(|fragment_id| {
                let children = by_location
                    .iter()
                    .filter(|((owner, _), _)| *owner == fragment_id)
                    .map(|((_, node_id), bindings)| {
                        node(*node_id, fragment_id, bindings.clone(), Vec::new())
                    })
                    .collect();
                PlanFragment {
                    fragment_id,
                    root: node(
                        10_000 + fragment_id as i32,
                        fragment_id,
                        Vec::new(),
                        children,
                    ),
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                }
            })
            .collect()
    }

    fn run_with_progress(
        graph: DraftRuntimeFilterGraph,
        edges: &[RefinedFragmentEdge],
        progress: JoinBuildProgressCatalog,
    ) -> Result<ActivationDecisionOutput, ActivationDecisionError> {
        let fragments = fragments_for(&graph, edges);
        ActivationDecisionPass::run_with_join_progress_for_test(graph, &fragments, edges, progress)
    }

    fn activation(output: &ActivationDecisionOutput, binding: u32) -> ConsumerActivation {
        let RuntimeFilterBindingRoleData::Consumer(requirement) =
            &output.graph.binding(BindingId::new(binding)).unwrap().role
        else {
            panic!("binding must be a consumer");
        };
        requirement.activation
    }

    #[test]
    fn acyclic_join_uses_conservative_blocking_fallback() {
        let output = run_with_progress(
            graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 1)]),
            &[edge(2, 1, 20)],
            JoinBuildProgressCatalog::new(),
        )
        .unwrap();

        assert_eq!(
            activation(&output, 10),
            ConsumerActivation::BlockingSnapshot
        );
        assert_eq!(
            output.decisions[&BindingId::new(10)].reason,
            ActivationDecisionReason::ConservativeFallback
        );
    }

    #[test]
    fn q23_pure_scc_uses_cycle_forced_batch_live_with_stable_provenance() {
        let output = run_with_progress(
            graph(vec![
                producer(100, 7, 2, 10),
                consumer(10, 7, 5),
                consumer(11, 7, 9),
            ]),
            &q23_edges(0),
            catalog(vec![proof(7, 100, 0, 10)]),
        )
        .unwrap();

        assert_eq!(
            activation(&output, 10),
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            }
        );
        let decision = &output.decisions[&BindingId::new(10)];
        assert_eq!(decision.channel, ChannelId::new(7));
        assert_eq!(decision.consumer_binding, BindingId::new(10));
        assert_eq!(decision.consumer_fragment, 5);
        assert_eq!(
            decision.activation,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            }
        );
        let ActivationDecisionReason::CycleForced {
            producer_bindings,
            witness,
        } = &decision.reason
        else {
            panic!("internal q23 consumer must be cycle-forced");
        };
        assert_eq!(producer_bindings, &vec![BindingId::new(100)]);
        assert!(!witness.is_empty());
        assert_eq!(
            output.decisions[&BindingId::new(11)].reason,
            ActivationDecisionReason::ConservativeFallback
        );
    }

    #[test]
    fn missing_rejected_and_skipped_proofs_keep_cycle_blocking() {
        let graph = graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 5)]);
        let mut rejected_proof = proof(7, 100, 0, 99);
        rejected_proof.join_node_id = 99;
        let mut skipped = JoinBuildProgressCatalog::new();
        skipped.insert_skip(
            (ChannelId::new(7), BindingId::new(100), 2),
            JoinBuildProgressSkip {
                join_node_id: 10,
                rule: FrontierSkip::UnauditedNode { node_id: 44 },
            },
        );

        for progress in [
            JoinBuildProgressCatalog::new(),
            catalog(vec![rejected_proof]),
            skipped,
        ] {
            let output = run_with_progress(graph.clone(), &q23_edges(0), progress).unwrap();
            assert_eq!(
                activation(&output, 10),
                ConsumerActivation::BlockingSnapshot
            );
            assert_eq!(
                output.decisions[&BindingId::new(10)].reason,
                ActivationDecisionReason::ConservativeFallback
            );
        }
    }

    #[test]
    fn live_only_is_required_by_contract_with_requested_granularity() {
        let output = run_with_progress(
            graph(vec![
                producer(100, 7, 2, 10),
                consumer_with(
                    10,
                    7,
                    1,
                    ActivationConstraint::LiveOnly {
                        late_apply: LateApplyGranularity::RowGroup,
                        reason: RequiredLiveReason::FencedFinalDomainContract,
                    },
                ),
            ]),
            &[edge(2, 1, 20)],
            JoinBuildProgressCatalog::new(),
        )
        .unwrap();

        assert_eq!(
            activation(&output, 10),
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::RowGroup,
            }
        );
        assert_eq!(
            output.decisions[&BindingId::new(10)].reason,
            ActivationDecisionReason::RequiredByContract {
                reason: RequiredLiveReason::FencedFinalDomainContract,
            }
        );
    }

    #[test]
    fn all_independent_pure_sccs_are_decided() {
        let mut edges = q23_edges(0);
        edges.extend(q23_edges(10));
        let output = run_with_progress(
            graph(vec![
                producer(100, 7, 2, 10),
                consumer(10, 7, 5),
                producer(200, 8, 12, 110),
                consumer(20, 8, 15),
            ]),
            &edges,
            catalog(vec![proof(7, 100, 0, 10), proof(8, 200, 10, 110)]),
        )
        .unwrap();

        for binding in [BindingId::new(10), BindingId::new(20)] {
            assert!(matches!(
                output.decisions[&binding].reason,
                ActivationDecisionReason::CycleForced { .. }
            ));
        }
    }

    #[test]
    fn multiple_internal_waits_for_one_consumer_are_deduplicated() {
        let coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let mut channel = channel(7);
        channel.availability_coverage = coverage.clone();
        channel.terminal_coverage = coverage;
        let mut second_producer = producer(101, 7, 2, 10);
        second_producer.coverage_witness_id = Some(CoverageWitnessId::new(2));
        let mut draft = DraftRuntimeFilterGraph::default();
        draft.insert_channel(channel).unwrap();
        for binding in [producer(100, 7, 2, 10), second_producer, consumer(10, 7, 5)] {
            draft.insert_binding(binding).unwrap();
        }

        let output = run_with_progress(
            draft,
            &q23_edges(0),
            catalog(vec![proof(7, 100, 0, 10), proof(7, 101, 0, 10)]),
        )
        .unwrap();

        assert_eq!(output.decisions.len(), 1);
        let ActivationDecisionReason::CycleForced {
            producer_bindings, ..
        } = &output.decisions[&BindingId::new(10)].reason
        else {
            panic!("internal q23 consumer must be cycle-forced");
        };
        assert_eq!(
            producer_bindings,
            &vec![BindingId::new(100), BindingId::new(101)]
        );
    }

    #[test]
    fn reversed_inputs_produce_identical_catalogs() {
        let bindings = vec![producer(100, 7, 2, 10), consumer(10, 7, 5)];
        let forward_graph = graph(bindings.clone());
        let reversed_graph = graph(bindings.into_iter().rev().collect());
        let edges = q23_edges(0);
        let mut reversed_edges = edges.clone();
        reversed_edges.reverse();
        let progress = catalog(vec![proof(7, 100, 0, 10)]);

        let forward = run_with_progress(forward_graph, &edges, progress.clone()).unwrap();
        let reverse = run_with_progress(reversed_graph, &reversed_edges, progress).unwrap();

        assert_eq!(forward.decisions, reverse.decisions);
    }

    #[test]
    fn every_consumer_has_exactly_one_decision() {
        let output = run_with_progress(
            graph(vec![
                producer(100, 7, 2, 10),
                consumer(10, 7, 5),
                consumer(11, 7, 9),
            ]),
            &q23_edges(0),
            catalog(vec![proof(7, 100, 0, 10)]),
        )
        .unwrap();

        assert_eq!(
            output.decisions.keys().copied().collect::<Vec<_>>(),
            vec![BindingId::new(10), BindingId::new(11)]
        );
    }

    #[test]
    fn invalid_draft_graph_or_plan_returns_no_output() {
        let mut invalid_graph = DraftRuntimeFilterGraph::default();
        invalid_graph
            .insert_binding(producer(100, 7, 2, 10))
            .unwrap();
        let graph_error = run_with_progress(invalid_graph, &[], JoinBuildProgressCatalog::new())
            .expect_err("invalid draft graph must not return an output");
        assert!(matches!(
            graph_error,
            ActivationDecisionError::DraftGraph(error)
                if error.kind == GraphValidationErrorKind::UnknownChannel
        ));

        let graph = graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 1)]);
        let plan_error = ActivationDecisionPass::run_with_join_progress_for_test(
            graph,
            &[],
            &[],
            JoinBuildProgressCatalog::new(),
        )
        .expect_err("invalid draft plan must not return an output");
        assert!(matches!(
            plan_error,
            ActivationDecisionError::DraftPlan(RuntimeFilterPlanValidationError::UnknownFragment(
                _
            ))
        ));
    }

    #[test]
    fn final_validation_failure_returns_no_output() {
        let graph = graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 5)]);
        let edges = q23_edges(0);
        let fragments = fragments_for(&graph, &edges);
        force_final_plan_failure_for_test();

        let error = ActivationDecisionPass::run_with_join_progress_for_test(
            graph,
            &fragments,
            &edges,
            catalog(vec![proof(7, 100, 0, 10)]),
        )
        .expect_err("final validation failure must not return an output");

        assert!(matches!(
            error,
            ActivationDecisionError::FinalPlan(
                RuntimeFilterPlanValidationError::BindingLocationMismatch(binding)
            ) if binding == BindingId::new(10)
        ));
    }

    #[test]
    fn materialization_changes_only_consumer_activation() {
        let draft = graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 5)]);
        let producer_before = format!("{:?}", draft.binding(BindingId::new(100)).unwrap());

        let output =
            run_with_progress(draft, &q23_edges(0), catalog(vec![proof(7, 100, 0, 10)])).unwrap();

        assert_eq!(
            activation(&output, 10),
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            }
        );
        assert_eq!(
            format!("{:?}", output.graph.binding(BindingId::new(100)).unwrap()),
            producer_before
        );
    }

    #[test]
    fn coarse_wait_in_same_scc_prevents_cycle_forced_activation() {
        let output = run_with_progress(
            graph(vec![
                producer(100, 7, 2, 10),
                consumer(10, 7, 5),
                producer(200, 8, 2, 20),
                consumer(20, 8, 5),
            ]),
            &q23_edges(0),
            catalog(vec![proof(7, 100, 0, 10)]),
        )
        .unwrap();

        assert!(
            output
                .decisions
                .values()
                .all(|decision| decision.reason == ActivationDecisionReason::ConservativeFallback)
        );
    }

    #[test]
    fn activation_constraints_preserve_required_live_contract() {
        let ordered = ActivationConstraint::LiveOnly {
            late_apply: LateApplyGranularity::Batch,
            reason: RequiredLiveReason::OrderedBoundContract,
        };
        let fenced = ActivationConstraint::LiveOnly {
            late_apply: LateApplyGranularity::RowGroup,
            reason: RequiredLiveReason::FencedFinalDomainContract,
        };
        let fallback = ActivationConstraint::BlockingOrBatchLive {
            fallback: ActivationFallback::BlockingSnapshot,
        };

        assert!(ordered.satisfies_required_non_blocking());
        assert!(fenced.satisfies_required_non_blocking());
        assert!(!fallback.satisfies_required_non_blocking());
    }
}
