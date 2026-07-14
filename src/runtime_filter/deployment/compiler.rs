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

use crate::common::types::UniqueId;
use crate::coordinator::scheduler::{LiveBackendSnapshot, SchedulingPlan};
use crate::runtime_filter::deployment::role_graph::{
    ChannelRoleInputs, ConsumerPlacement, ProducerPlacement, RoleGraph, RouteEdgeAllocator,
    build_channel_role_graph,
};
use crate::runtime_filter::deployment::shard::{
    ChannelProjectionSpec, ConsumerBindingFacts, project_install_views,
};
use crate::runtime_filter::deployment::wait_for::{
    ConsumerWaitInput, ExecutionDependencyGraph, validate_wait_for,
};
use crate::runtime_filter::deployment::{
    DeploymentError, RuntimeFilterDeploymentPlan, RuntimeFilterDeploymentPolicy,
};
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CompletionRequirement, CoverageWitnessId,
};
use crate::runtime_filter::model::graph::{RuntimeFilterBindingRole, RuntimeFilterGraph};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::sql::planner::distributed::{FragmentEdge, FragmentId};

/// Compile a query-global [`RuntimeFilterGraph`] plus COOR-2 scheduling/placement
/// into a coordinator-side [`RuntimeFilterDeploymentPlan`]: a full role graph
/// (Producer/Aggregator/Relay/Consumer, all routes) and the per-participant
/// loopback `RuntimeFilterInstallView` shards each BE installs today.
///
/// Pipeline: `graph.validate()` -> build the fragment `ExecutionDependencyGraph`
/// -> resolve per-(channel,binding) participant placement -> reject
/// `BlockingSnapshot` feedback cycles via `validate_wait_for` -> build each
/// channel's role graph -> project loopback install views -> assemble the plan.
///
/// Pure and deterministic: never mutates `scheduling`, iterates only
/// `BTreeMap`/`BTreeSet`, and never hardcodes backend/replica counts (they are
/// read from `backends` and clamped by `policy.replica_redundancy`).
pub(crate) fn compile(
    graph: &RuntimeFilterGraph,
    scheduling: &SchedulingPlan,
    edges: &[FragmentEdge],
    backends: &LiveBackendSnapshot,
    policy: &RuntimeFilterDeploymentPolicy,
    epoch: DeploymentEpoch,
) -> Result<RuntimeFilterDeploymentPlan, DeploymentError> {
    // 1. RFD-1 validation first: no downstream step may paper over a graph the
    // model itself already rejects.
    graph.validate().map_err(DeploymentError::GraphInvalid)?;

    // 2. Fragment execution dependency graph (also rejects a cyclic plan).
    let exec_deps = ExecutionDependencyGraph::from_fragment_edges(edges)
        .map_err(|_| DeploymentError::FragmentCycle)?;

    // 3. Resolve per-(channel,binding) participant placement + the expected
    // finst instances from the scheduling plan. `participant == backend_idx`,
    // checked against the live snapshot.
    let known_backends: BTreeSet<usize> = backends.entries().iter().map(|(id, _)| *id).collect();
    let mut instances: BTreeMap<
        (ChannelId, BindingId, RuntimeFilterParticipantId),
        BTreeSet<UniqueId>,
    > = BTreeMap::new();
    let mut producer_placements: BTreeMap<ChannelId, Vec<ProducerPlacement>> = BTreeMap::new();
    let mut consumer_placements: BTreeMap<ChannelId, Vec<ConsumerPlacement>> = BTreeMap::new();
    let mut consumer_facts: BTreeMap<BindingId, ConsumerBindingFacts> = BTreeMap::new();
    let mut channel_completion: BTreeMap<ChannelId, CompletionRequirement> = BTreeMap::new();
    let mut producer_witness: BTreeMap<ChannelId, BTreeMap<BindingId, CoverageWitnessId>> =
        BTreeMap::new();

    for binding in graph.bindings() {
        let fragment: FragmentId = binding.location.fragment_id.get();
        let placements =
            scheduling
                .by_fragment
                .get(&fragment)
                .ok_or(DeploymentError::MissingPlacement {
                    fragment: binding.location.fragment_id,
                })?;
        let mut participants: BTreeSet<RuntimeFilterParticipantId> = BTreeSet::new();
        for p in placements {
            if !known_backends.contains(&p.backend_idx) {
                return Err(DeploymentError::UnknownBackend {
                    backend_idx: p.backend_idx,
                });
            }
            let participant = RuntimeFilterParticipantId::new(p.backend_idx as u32);
            participants.insert(participant);
            instances
                .entry((binding.channel_id, binding.binding_id, participant))
                .or_default()
                .insert(p.finst_id);
        }
        match &binding.role {
            RuntimeFilterBindingRole::Producer(req) => {
                producer_placements
                    .entry(binding.channel_id)
                    .or_default()
                    .push(ProducerPlacement {
                        binding: binding.binding_id,
                        participants,
                    });
                channel_completion
                    .entry(binding.channel_id)
                    .or_insert(req.completion_requirement);
                if let Some(witness) = binding.coverage_witness_id {
                    producer_witness
                        .entry(binding.channel_id)
                        .or_default()
                        .insert(binding.binding_id, witness);
                }
            }
            RuntimeFilterBindingRole::Consumer(req) => {
                consumer_placements
                    .entry(binding.channel_id)
                    .or_default()
                    .push(ConsumerPlacement {
                        binding: binding.binding_id,
                        participants,
                    });
                consumer_facts.insert(
                    binding.binding_id,
                    ConsumerBindingFacts {
                        activation: req.activation,
                        capabilities: req.capabilities.clone(),
                    },
                );
            }
        }
    }

    // 4. Wait-for cycle validation: only `BlockingSnapshot` consumers add a
    // wait edge, and only a real execution-topology cycle is rejected.
    let mut consumer_waits: Vec<ConsumerWaitInput> = Vec::new();
    for channel in graph.channels() {
        let producer_fragments: Vec<FragmentId> = graph
            .bindings()
            .filter(|binding| {
                binding.channel_id == channel.channel_id
                    && matches!(binding.role, RuntimeFilterBindingRole::Producer(_))
            })
            .map(|binding| binding.location.fragment_id.get())
            .collect();
        for binding in graph
            .bindings()
            .filter(|binding| binding.channel_id == channel.channel_id)
        {
            if let RuntimeFilterBindingRole::Consumer(req) = &binding.role {
                consumer_waits.push(ConsumerWaitInput {
                    channel: channel.channel_id,
                    binding: binding.binding_id,
                    consumer_fragment: binding.location.fragment_id.get(),
                    activation: req.activation,
                    producer_fragments: producer_fragments.clone(),
                });
            }
        }
    }
    validate_wait_for(&exec_deps, &consumer_waits)?;

    // 5. Role graph per channel + the per-channel projection spec the shard
    // projector needs. The completion requirement is precomputed here from
    // the channel's producer bindings, since the model channel spec itself
    // carries no completion field (`graph.validate()` guarantees every
    // channel has >=1 producer, all agreeing on this value).
    let mut alloc = RouteEdgeAllocator::new();
    let mut role_graph = RoleGraph::default();
    let mut channel_specs: BTreeMap<ChannelId, ChannelProjectionSpec> = BTreeMap::new();
    for channel in graph.channels() {
        let channel_id = channel.channel_id;
        let inputs = ChannelRoleInputs {
            channel_id,
            availability_coverage: channel.availability_coverage.clone(),
            producers: producer_placements
                .get(&channel_id)
                .cloned()
                .unwrap_or_default(),
            consumers: consumer_placements
                .get(&channel_id)
                .cloned()
                .unwrap_or_default(),
        };
        let channel_role_graph =
            build_channel_role_graph(&inputs, policy.replica_redundancy, &mut alloc);
        role_graph.channels.insert(channel_id, channel_role_graph);

        let completion =
            channel_completion
                .get(&channel_id)
                .copied()
                .ok_or(DeploymentError::EmptyCoverage {
                    channel: channel_id,
                })?;
        channel_specs.insert(
            channel_id,
            ChannelProjectionSpec {
                channel_id,
                logical_domain: channel.logical_domain.clone(),
                lifecycle: channel.lifecycle,
                availability_coverage: channel.availability_coverage.clone(),
                terminal_coverage: channel.terminal_coverage.clone(),
                reduction_requirement: channel.reduction_requirement,
                allowed_contribution_kinds: channel.allowed_contribution_kinds.clone(),
                completion_requirement: completion,
                policy: channel.policy,
                producer_witness: producer_witness
                    .get(&channel_id)
                    .cloned()
                    .unwrap_or_default(),
            },
        );
    }

    // 6. Project loopback install views (remote routes stay in `role_graph`
    // for RFD-4 to consume).
    let install_views = project_install_views(
        epoch,
        &role_graph,
        &channel_specs,
        &consumer_facts,
        &instances,
        policy.core_budget,
    );

    let participants: BTreeSet<RuntimeFilterParticipantId> = backends
        .entries()
        .iter()
        .map(|(id, _)| RuntimeFilterParticipantId::new(*id as u32))
        .collect();

    Ok(RuntimeFilterDeploymentPlan {
        epoch,
        participants,
        install_views,
        role_graph,
    })
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::coordinator::scheduler::FragmentInstancePlacement;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ConsumerActivation, ContributionKind, NullSemantics, PlanFragmentId,
        PlanNodeId, ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement,
        RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
    };
    use crate::runtime_filter::port::install::RuntimeFilterCoreBudget;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdgeKind, FragmentStreamKind, PartitionKind,
    };

    /// Mirrors `model::graph::tests::expression()` exactly (literal `Int(1)`,
    /// `Int64`, non-nullable) — that module is `pub(super)`-scoped to `model`,
    /// so RFD-2 cannot import it directly; the construction is copied instead.
    fn sample_typed_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn placement(
        fragment_id: u32,
        instance_index: usize,
        backend_idx: usize,
        finst: UniqueId,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id: finst,
            backend_idx,
            endpoint: RuntimeEndpoint::from_socket_addr("127.0.0.1:9060".parse().unwrap()),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            runtime_filter_prober_params: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn edge(source: u32, target: u32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: 0,
            output_partition: DataPartition {
                kind: PartitionKind::Unpartitioned,
                exprs: Vec::new(),
            },
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    /// Membership/CompleteOnce/SetUnion Join-shaped channel — mirrors
    /// `model::graph::tests::join_channel()`, which RFD-2 cannot import
    /// directly for the same `pub(super)` reason as `sample_typed_expr()`.
    fn channel_spec(id: u32) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id: ChannelId::new(id),
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
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::Membership]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
        }
    }

    fn producer_binding(binding: u32, channel: u32, fragment: u32) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: Some(CoverageWitnessId::new(1)),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(1),
            },
            expression: sample_typed_expr(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                // NOTE: the plan's "Test helper reference" appendix lists only
                // `ValueDomainDelta` here, which fails `validate_producer`'s
                // `RequiredProducerContributionMissing(ProducerClosed)` check
                // (a Membership channel without FinalDomainShard requires
                // {ValueDomainDelta, ProducerClosed}, matching
                // `join_producer_binding` in model::graph::tests). Corrected.
                contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
            }),
        }
    }

    fn consumer_binding(
        binding: u32,
        channel: u32,
        fragment: u32,
        activation: ConsumerActivation,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(2),
            },
            expression: sample_typed_expr(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities: BTreeSet::from([ArtifactCapability::Membership]),
                activation,
            }),
        }
    }

    #[test]
    fn compile_colocated_join_yields_one_loopback_view() {
        let mut graph = RuntimeFilterGraph::default();
        graph.insert_channel(channel_spec(5)).unwrap();
        graph.insert_binding(producer_binding(10, 5, 2)).unwrap();
        graph
            .insert_binding(consumer_binding(
                11,
                5,
                1,
                ConsumerActivation::BlockingSnapshot,
            ))
            .unwrap();

        // Data flow build(frag 2) -> probe(frag 1): consumer(1) depends on
        // producer(2). No cycle.
        let edges = vec![edge(2, 1)];
        // Both fragments scheduled onto backend 0 -> co-located -> loopback.
        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(1u32, vec![placement(1, 0, 0, UniqueId { hi: 1, lo: 1 })]);
        by_fragment.insert(2u32, vec![placement(2, 0, 0, UniqueId { hi: 1, lo: 2 })]);
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment,
            root_finst_id: UniqueId { hi: 1, lo: 1 },
            root_backend_idx: 0,
        };
        let backends = LiveBackendSnapshot::from_endpoints(vec![
            "127.0.0.1:9060".parse::<SocketAddr>().unwrap(),
        ]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(1024),
            replica_redundancy: 1,
        };

        let plan = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(7),
        )
        .unwrap();
        assert_eq!(plan.participants.len(), 1);
        assert_eq!(plan.install_views.len(), 1);
        assert_eq!(plan.epoch.get(), 7);
    }

    #[test]
    fn compile_rejects_blocking_feedback_cycle() {
        let mut graph = RuntimeFilterGraph::default();
        // A Membership channel (not OrderedBound) is used deliberately: an
        // OrderedBound/FinalDomainShard channel forbids `BlockingSnapshot`
        // consumers at `graph.validate()` time (`BlockingFeedbackConsumer`),
        // which would fire before `compile`'s wait-for check ever runs. Using
        // Membership + an execution-topology cycle isolates the wait-for path.
        graph.insert_channel(channel_spec(5)).unwrap();
        graph.insert_binding(producer_binding(10, 5, 1)).unwrap();
        graph
            .insert_binding(consumer_binding(
                11,
                5,
                2,
                ConsumerActivation::BlockingSnapshot,
            ))
            .unwrap();

        // scan(2) -> topn(1): the producer's own fragment (1) depends on the
        // consumer's fragment (2), but the consumer blocks waiting for the
        // producer's first snapshot -> execution feedback cycle.
        let edges = vec![edge(2, 1)];
        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(1u32, vec![placement(1, 0, 0, UniqueId { hi: 1, lo: 1 })]);
        by_fragment.insert(2u32, vec![placement(2, 0, 0, UniqueId { hi: 1, lo: 2 })]);
        let scheduling = SchedulingPlan {
            root_fragment_id: 2,
            by_fragment,
            root_finst_id: UniqueId { hi: 1, lo: 2 },
            root_backend_idx: 0,
        };
        let backends = LiveBackendSnapshot::from_endpoints(vec![
            "127.0.0.1:9060".parse::<SocketAddr>().unwrap(),
        ]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(1024),
            replica_redundancy: 1,
        };

        let err = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(7),
        )
        .unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }
}
