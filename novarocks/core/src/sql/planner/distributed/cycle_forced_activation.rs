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

use crate::novarocks_logging::debug;
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, ConsumerActivation, LateApplyGranularity,
};
use crate::runtime_filter::model::graph::{ConsumerActivationUpdateError, RuntimeFilterGraph};
use crate::runtime_filter::model::join_progress::JoinBuildProgressCatalog;
use crate::runtime_filter::model::refined_wait_graph::{
    CycleStep, RefinedFragmentEdge, RefinedWaitGraphBuildError, build_refined_wait_graph,
    project_consumer_waits,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CycleForcedActivation {
    pub(crate) channel: ChannelId,
    pub(crate) consumer_binding: BindingId,
    pub(crate) consumer_fragment: u32,
    pub(crate) producer_bindings: Vec<BindingId>,
    pub(crate) witness: Vec<CycleStep>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CycleForcedActivationError {
    RefinedGraph(RefinedWaitGraphBuildError),
    MissingConsumerBinding {
        channel: ChannelId,
        binding: BindingId,
    },
    Mutation(ConsumerActivationUpdateError),
}

impl fmt::Display for CycleForcedActivationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RefinedGraph(error) => {
                write!(
                    formatter,
                    "cycle-forced runtime-filter analysis failed: {error:?}"
                )
            }
            Self::MissingConsumerBinding { channel, binding } => write!(
                formatter,
                "cycle-forced runtime-filter analysis lost consumer binding={} channel={}",
                binding.get(),
                channel.get()
            ),
            Self::Mutation(error) => {
                write!(
                    formatter,
                    "cycle-forced runtime-filter mutation failed: {error:?}"
                )
            }
        }
    }
}

pub(crate) fn decide_cycle_forced_activations(
    edges: &[RefinedFragmentEdge],
    graph: &RuntimeFilterGraph,
    join_progress: &JoinBuildProgressCatalog,
) -> Result<Vec<CycleForcedActivation>, CycleForcedActivationError> {
    let consumers = project_consumer_waits(graph);
    let consumer_fragments: BTreeMap<(ChannelId, BindingId), u32> = consumers
        .iter()
        .map(|consumer| {
            (
                (consumer.channel, consumer.binding),
                consumer.consumer_fragment,
            )
        })
        .collect();
    let refined = build_refined_wait_graph(edges, &consumers, join_progress, graph)
        .map_err(CycleForcedActivationError::RefinedGraph)?;
    let mut decisions: BTreeMap<
        (ChannelId, BindingId, u32),
        (BTreeSet<BindingId>, BTreeSet<CycleStep>),
    > = BTreeMap::new();
    for scc in refined.pure_blocking_sccs() {
        for wait in &scc.waits {
            let consumer_fragment = consumer_fragments
                .get(&(wait.channel, wait.consumer_binding))
                .copied()
                .ok_or(CycleForcedActivationError::MissingConsumerBinding {
                    channel: wait.channel,
                    binding: wait.consumer_binding,
                })?;
            let (producers, witness) = decisions
                .entry((wait.channel, wait.consumer_binding, consumer_fragment))
                .or_default();
            producers.insert(wait.producer_binding);
            witness.extend(scc.witness.iter().copied());
        }
    }
    Ok(decisions
        .into_iter()
        .map(
            |((channel, consumer_binding, consumer_fragment), (producers, witness))| {
                CycleForcedActivation {
                    channel,
                    consumer_binding,
                    consumer_fragment,
                    producer_bindings: producers.into_iter().collect(),
                    witness: witness.into_iter().collect(),
                }
            },
        )
        .collect())
}

pub(crate) fn apply_cycle_forced_activations(
    graph: &mut RuntimeFilterGraph,
    decisions: &[CycleForcedActivation],
) -> Result<(), CycleForcedActivationError> {
    let mut candidate = graph.clone();
    for decision in decisions {
        candidate
            .replace_consumer_activation_checked(
                decision.consumer_binding,
                decision.channel,
                decision.consumer_fragment,
                ConsumerActivation::BlockingSnapshot,
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
            )
            .map_err(CycleForcedActivationError::Mutation)?;
    }
    *graph = candidate;
    Ok(())
}

pub(crate) fn log_cycle_forced_activations(decisions: &[CycleForcedActivation]) {
    for decision in decisions {
        let producers: Vec<u32> = decision
            .producer_bindings
            .iter()
            .map(|binding| binding.get())
            .collect();
        debug!(
            "cycle-forced runtime-filter activation: channel={} consumer_binding={} consumer_fragment={} producers={producers:?} witness={:?}",
            decision.channel.get(),
            decision.consumer_binding.get(),
            decision.consumer_fragment,
            decision.witness,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use super::{
        CycleForcedActivationError, apply_cycle_forced_activations, decide_cycle_forced_activations,
    };
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, LateApplyGranularity, NullSemantics, PlanFragmentId,
        PlanNodeId, ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirement, PlanLocation,
        ProducerBindingTarget, ProducerRequirement, RuntimeFilterBindingRole,
        RuntimeFilterBindingSpec, RuntimeFilterChannelSpec, RuntimeFilterGraph,
    };
    use crate::runtime_filter::model::join_progress::{
        FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
        JoinBuildProgressSkip,
    };
    use crate::runtime_filter::model::refined_wait_graph::RefinedFragmentEdge;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};

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

    fn producer(binding: u32, channel: u32, fragment: u32, node: i32) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: Some(CoverageWitnessId::new(1)),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(node),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        }
    }

    fn consumer(binding: u32, channel: u32, fragment: u32) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(fragment as i32 * 10),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation: ConsumerActivation::BlockingSnapshot,
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        }
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

    fn graph(bindings: Vec<RuntimeFilterBindingSpec>) -> RuntimeFilterGraph {
        let channels: BTreeSet<ChannelId> =
            bindings.iter().map(|binding| binding.channel_id).collect();
        let mut graph = RuntimeFilterGraph::default();
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

    fn canonical_fixture() -> (
        Vec<RefinedFragmentEdge>,
        RuntimeFilterGraph,
        JoinBuildProgressCatalog,
    ) {
        let proof = proof(7, 100, 0, 10);
        (
            q23_edges(0),
            graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 5)]),
            catalog(vec![proof]),
        )
    }

    fn activation(graph: &RuntimeFilterGraph, binding: u32) -> ConsumerActivation {
        let RuntimeFilterBindingRole::Consumer(requirement) =
            &graph.binding(BindingId::new(binding)).unwrap().role
        else {
            panic!("binding must be a consumer");
        };
        requirement.activation
    }

    #[test]
    fn q23_pure_scc_decides_only_internal_consumer() {
        let (edges, mut graph, progress) = canonical_fixture();
        graph.insert_binding(consumer(11, 7, 9)).unwrap();

        let decisions = decide_cycle_forced_activations(&edges, &graph, &progress).unwrap();

        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].channel, ChannelId::new(7));
        assert_eq!(decisions[0].consumer_binding, BindingId::new(10));
        assert_eq!(decisions[0].consumer_fragment, 5);
        assert_eq!(decisions[0].producer_bindings, vec![BindingId::new(100)]);
        assert!(!decisions[0].witness.is_empty());
        assert_eq!(
            activation(&graph, 10),
            ConsumerActivation::BlockingSnapshot,
            "decision must not mutate the graph"
        );
    }

    #[test]
    fn acyclic_blocking_consumer_is_not_downgraded() {
        let graph = graph(vec![producer(100, 7, 2, 10), consumer(10, 7, 1)]);
        let edges = vec![edge(2, 1, 20)];

        let decisions =
            decide_cycle_forced_activations(&edges, &graph, &JoinBuildProgressCatalog::new())
                .unwrap();

        assert!(decisions.is_empty());
    }

    #[test]
    fn missing_rejected_and_skipped_proofs_keep_whole_scc_impure() {
        let (edges, graph, _) = canonical_fixture();
        let mut rejected_proof = proof(7, 100, 0, 99);
        rejected_proof.join_node_id = 99;
        let rejected = catalog(vec![rejected_proof]);
        let mut skipped = JoinBuildProgressCatalog::new();
        skipped.insert_skip(
            (ChannelId::new(7), BindingId::new(100), 2),
            JoinBuildProgressSkip {
                join_node_id: 10,
                rule: FrontierSkip::UnauditedNode { node_id: 44 },
            },
        );

        for progress in [JoinBuildProgressCatalog::new(), rejected, skipped] {
            assert!(
                decide_cycle_forced_activations(&edges, &graph, &progress)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn coarse_wait_in_same_scc_prevents_pure_cycle_downgrade() {
        let edges = q23_edges(0);
        let graph = graph(vec![
            producer(100, 7, 2, 10),
            consumer(10, 7, 5),
            producer(200, 8, 2, 20),
            consumer(20, 8, 5),
        ]);
        let progress = catalog(vec![proof(7, 100, 0, 10)]);

        let decisions = decide_cycle_forced_activations(&edges, &graph, &progress).unwrap();

        assert!(decisions.is_empty());
    }

    #[test]
    fn all_independent_pure_sccs_are_decided() {
        let mut edges = q23_edges(0);
        edges.extend(q23_edges(10));
        let graph = graph(vec![
            producer(100, 7, 2, 10),
            consumer(10, 7, 5),
            producer(200, 8, 12, 110),
            consumer(20, 8, 15),
        ]);
        let progress = catalog(vec![proof(7, 100, 0, 10), proof(8, 200, 10, 110)]);

        let decisions = decide_cycle_forced_activations(&edges, &graph, &progress).unwrap();

        assert_eq!(
            decisions
                .iter()
                .map(|decision| decision.consumer_binding)
                .collect::<Vec<_>>(),
            vec![BindingId::new(10), BindingId::new(20)]
        );
    }

    #[test]
    fn multiple_internal_waits_for_one_consumer_are_deduplicated() {
        let graph = graph(vec![
            producer(100, 7, 2, 10),
            producer(101, 7, 2, 10),
            consumer(10, 7, 5),
        ]);
        let progress = catalog(vec![proof(7, 100, 0, 10), proof(7, 101, 0, 10)]);

        let decisions = decide_cycle_forced_activations(&q23_edges(0), &graph, &progress).unwrap();

        assert_eq!(decisions.len(), 1);
        assert_eq!(
            decisions[0].producer_bindings,
            vec![BindingId::new(100), BindingId::new(101)]
        );
    }

    #[test]
    fn reversed_edges_and_bindings_produce_identical_decisions() {
        let bindings = vec![producer(100, 7, 2, 10), consumer(10, 7, 5)];
        let forward_graph = graph(bindings.clone());
        let reverse_graph = graph(bindings.into_iter().rev().collect());
        let forward_edges = q23_edges(0);
        let mut reverse_edges = forward_edges.clone();
        reverse_edges.reverse();
        let progress = catalog(vec![proof(7, 100, 0, 10)]);

        let forward =
            decide_cycle_forced_activations(&forward_edges, &forward_graph, &progress).unwrap();
        let reverse =
            decide_cycle_forced_activations(&reverse_edges, &reverse_graph, &progress).unwrap();

        assert_eq!(forward, reverse);
    }

    #[test]
    fn stale_decision_fails_fast_during_apply() {
        let (edges, mut graph, progress) = canonical_fixture();
        let decisions = decide_cycle_forced_activations(&edges, &graph, &progress).unwrap();
        let RuntimeFilterBindingRole::Consumer(requirement) =
            &mut graph.binding_mut_for_test(BindingId::new(10)).unwrap().role
        else {
            panic!("fixture binding is a consumer");
        };
        requirement.activation = ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        };

        let error = apply_cycle_forced_activations(&mut graph, &decisions)
            .expect_err("stale decision must fail");

        assert!(matches!(
            error,
            CycleForcedActivationError::Mutation(
                crate::runtime_filter::model::graph::ConsumerActivationUpdateError::CurrentActivationMismatch {
                    binding,
                    ..
                }
            ) if binding == BindingId::new(10)
        ));
    }

    #[test]
    fn later_stale_decision_leaves_earlier_valid_activation_unchanged() {
        let mut edges = q23_edges(0);
        edges.extend(q23_edges(10));
        let mut graph = graph(vec![
            producer(100, 7, 2, 10),
            consumer(10, 7, 5),
            producer(200, 8, 12, 110),
            consumer(20, 8, 15),
        ]);
        let progress = catalog(vec![proof(7, 100, 0, 10), proof(8, 200, 10, 110)]);
        let decisions = decide_cycle_forced_activations(&edges, &graph, &progress).unwrap();
        let RuntimeFilterBindingRole::Consumer(requirement) =
            &mut graph.binding_mut_for_test(BindingId::new(20)).unwrap().role
        else {
            panic!("fixture binding is a consumer");
        };
        requirement.activation = ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        };

        let error = apply_cycle_forced_activations(&mut graph, &decisions)
            .expect_err("a later stale decision must reject the whole activation transaction");

        assert!(matches!(
            error,
            CycleForcedActivationError::Mutation(
                crate::runtime_filter::model::graph::ConsumerActivationUpdateError::CurrentActivationMismatch {
                    binding,
                    ..
                }
            ) if binding == BindingId::new(20)
        ));
        assert_eq!(
            activation(&graph, 10),
            ConsumerActivation::BlockingSnapshot,
            "an earlier valid decision must roll back with the rejected transaction"
        );
        assert_eq!(
            activation(&graph, 20),
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            "the preexisting stale activation must remain unchanged"
        );
    }

    #[test]
    fn applying_decision_changes_only_consumer_binding_activation() {
        let (edges, mut graph, progress) = canonical_fixture();
        let decisions = decide_cycle_forced_activations(&edges, &graph, &progress).unwrap();
        let producer_before = format!("{:?}", graph.binding(BindingId::new(100)).unwrap());

        apply_cycle_forced_activations(&mut graph, &decisions).unwrap();

        assert_eq!(
            activation(&graph, 10),
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            }
        );
        assert_eq!(
            format!("{:?}", graph.binding(BindingId::new(100)).unwrap()),
            producer_before
        );
        assert_eq!(decisions[0].consumer_binding, BindingId::new(10));
        assert_eq!(decisions[0].producer_bindings, vec![BindingId::new(100)]);
    }
}
