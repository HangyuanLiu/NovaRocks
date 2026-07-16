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
use crate::runtime_filter::deployment::role_graph::{RoleGraph, RouteKind};
use crate::runtime_filter::deployment::{BindingInstanceIndex, DeploymentError};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerArtifactProfile};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    ConsumerDeployment, MaterializationPolicy, ProducerDeployment, RuntimeFilterChannelDeployment,
    RuntimeFilterCoreBudget, RuntimeFilterInstallView,
};
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;

/// Channel-level facts the projection stamps into each shard (mirrors the model
/// channel spec, plus the per-channel completion requirement and producer→witness
/// map the compiler pre-computes from the global graph).
#[derive(Clone, Debug)]
pub(crate) struct ChannelProjectionSpec {
    pub channel_id: ChannelId,
    pub logical_domain: RuntimeFilterLogicalDomain,
    pub lifecycle: RuntimeFilterLifecycle,
    pub availability_coverage: Coverage,
    pub terminal_coverage: Coverage,
    pub reduction_requirement: ReductionRequirement,
    pub allowed_contribution_kinds: BTreeSet<ContributionKind>,
    pub completion_requirement: CompletionRequirement,
    pub policy: RuntimeFilterPolicyRequirement,
    pub producer_witness: BTreeMap<BindingId, CoverageWitnessId>,
}

/// Consumer activation + capabilities, looked up per binding.
#[derive(Clone, Debug)]
pub(crate) struct ConsumerBindingFacts {
    pub activation: ConsumerActivation,
    pub capabilities: BTreeSet<ArtifactCapability>,
}

/// Lower a consumer's semantic `ArtifactCapability` set into the physical
/// `ConsumerArtifactProfile` the M2 install contract requires (RFD-3/M2 §159-162):
/// `Membership` → `ValueSet`, `EmptyDomain` → `EmptyDomain`, and an M3A
/// `OrderedBound` contract with `OrderedRange` capability → an exact Range
/// profile carrying the validated order digest. Membership channels never map
/// `OrderedRange` to Range. Bloom/Bitset are not selected here.
fn consumer_artifact_profile(
    logical_domain: &RuntimeFilterLogicalDomain,
    capabilities: &BTreeSet<ArtifactCapability>,
) -> Result<ConsumerArtifactProfile, DeploymentError> {
    if let RuntimeFilterLogicalDomain::OrderedBound(plan) = logical_domain {
        let contract = RuntimeOrderContract::try_from_plan(plan).map_err(|_| {
            DeploymentError::InvalidArtifactProfile(
                crate::runtime_filter::port::artifact::ArtifactContractError::UnsupportedSchema,
            )
        })?;
        if capabilities.contains(&ArtifactCapability::OrderedRange) {
            return ConsumerArtifactProfile::new_ordered_range(contract.digest())
                .map_err(DeploymentError::InvalidArtifactProfile);
        }
    }
    let mut accepted = BTreeSet::new();
    if capabilities.contains(&ArtifactCapability::Membership) {
        accepted.insert(ArtifactKind::ValueSet);
    }
    if capabilities.contains(&ArtifactCapability::EmptyDomain) {
        accepted.insert(ArtifactKind::EmptyDomain);
    }
    ConsumerArtifactProfile::new(accepted, None).map_err(DeploymentError::InvalidArtifactProfile)
}

/// Project the role graph + placement into per-participant install views.
///
/// LOOPBACK ONLY (RFD-2 range decision): only consumers reachable via a
/// `Loopback` route edge are projected; remote-route consumers stay in the
/// coordinator-side plan for RFD-4. The count of skipped remote consumers is
/// logged (no silent truncation).
///
/// PRECONDITION: the caller (RFD-2's `compile`) MUST supply `channel_specs`,
/// `producer_witness`, and `consumer_facts` entries covering every channel /
/// producer binding / loopback-consumer binding present in `role_graph`. A
/// missing entry is logged (`tracing::warn!`) and the offending binding/channel
/// is skipped rather than panicking, except for an Aggregator's producer
/// authority: every producer witness and placement must be present so the Core
/// view cannot silently disagree with routing authorization.
///
/// Fails with [`DeploymentError::InvalidArtifactProfile`] if a consumer's
/// semantic capabilities cannot form a valid M2 physical artifact profile.
pub(crate) fn project_install_views(
    epoch: DeploymentEpoch,
    role_graph: &RoleGraph,
    channel_specs: &BTreeMap<ChannelId, ChannelProjectionSpec>,
    consumer_facts: &BTreeMap<BindingId, ConsumerBindingFacts>,
    instances: &BindingInstanceIndex,
    core_budget: RuntimeFilterCoreBudget,
    materialization: MaterializationPolicy,
) -> Result<BTreeMap<RuntimeFilterParticipantId, RuntimeFilterInstallView>, DeploymentError> {
    // participant -> channel -> (producers, consumers)
    #[allow(clippy::type_complexity)]
    let mut per_participant: BTreeMap<
        RuntimeFilterParticipantId,
        BTreeMap<
            ChannelId,
            (
                BTreeMap<BindingId, ProducerDeployment>,
                BTreeMap<BindingId, ConsumerDeployment>,
            ),
        >,
    > = BTreeMap::new();
    let mut skipped_remote_consumers = 0usize;

    for (channel_id, cg) in &role_graph.channels {
        let Some(spec) = channel_specs.get(channel_id) else {
            tracing::warn!(
                channel = channel_id.get(),
                "RFD-2 projection: channel missing from channel_specs; skipped"
            );
            continue;
        };
        // Producers: every hosted producer binding on each participant.
        for (participant, bindings) in &cg.producers {
            for binding in bindings {
                let Some(witness) = spec.producer_witness.get(binding).copied() else {
                    tracing::warn!(
                        channel = channel_id.get(),
                        binding = binding.get(),
                        "RFD-2 projection: producer binding missing witness; skipped"
                    );
                    continue;
                };
                let expected = instances
                    .get(&(*channel_id, *binding, *participant))
                    .cloned()
                    .unwrap_or_default();
                per_participant
                    .entry(*participant)
                    .or_default()
                    .entry(*channel_id)
                    .or_default()
                    .0
                    .insert(*binding, ProducerDeployment::new(witness, expected));
            }
        }
        // Aggregators reduce contributions from every producer participant, so
        // their Core authority must contain the same query-global finst set as
        // the routing shard. Keep non-aggregator producer views local-only.
        if let Some(aggregator) = cg.aggregator {
            let mut aggregator_producers: BTreeMap<BindingId, BTreeSet<UniqueId>> = BTreeMap::new();
            for (participant, bindings) in &cg.producers {
                for binding in bindings {
                    let Some(_witness) = spec.producer_witness.get(binding) else {
                        return Err(DeploymentError::InvalidInstallProjection {
                            detail: format!(
                                "runtime filter aggregator projection missing producer witness \
                                 for channel {} binding {}",
                                channel_id.get(),
                                binding.get()
                            ),
                        });
                    };
                    let Some(expected) = instances.get(&(*channel_id, *binding, *participant))
                    else {
                        return Err(DeploymentError::InvalidInstallProjection {
                            detail: format!(
                                "runtime filter aggregator projection missing producer placement \
                                 for channel {} binding {} participant {}",
                                channel_id.get(),
                                binding.get(),
                                participant.get()
                            ),
                        });
                    };
                    if expected.is_empty() {
                        return Err(DeploymentError::InvalidInstallProjection {
                            detail: format!(
                                "runtime filter aggregator projection missing producer placement \
                                 for channel {} binding {} participant {}",
                                channel_id.get(),
                                binding.get(),
                                participant.get()
                            ),
                        });
                    }
                    aggregator_producers
                        .entry(*binding)
                        .or_default()
                        .extend(expected.iter().copied());
                }
            }
            for (binding, expected) in aggregator_producers {
                let witness = spec.producer_witness[&binding];
                per_participant
                    .entry(aggregator)
                    .or_default()
                    .entry(*channel_id)
                    .or_default()
                    .0
                    .insert(binding, ProducerDeployment::new(witness, expected));
            }
        }
        // Consumers: only those reachable through a Loopback route edge.
        let loopback: BTreeMap<(RuntimeFilterParticipantId, BindingId), RouteEdgeId> = cg
            .routes
            .iter()
            .filter(|r| r.kind == RouteKind::Loopback)
            .map(|r| ((r.to.participant, r.to.binding), r.edge_id))
            .collect();
        for (participant, bindings) in &cg.consumers {
            for binding in bindings {
                let Some(route_edge_id) = loopback.get(&(*participant, *binding)).copied() else {
                    skipped_remote_consumers += 1;
                    continue;
                };
                let Some(facts) = consumer_facts.get(binding) else {
                    tracing::warn!(
                        channel = channel_id.get(),
                        binding = binding.get(),
                        "RFD-2 projection: loopback consumer binding missing consumer_facts; skipped"
                    );
                    continue;
                };
                let profile = consumer_artifact_profile(&spec.logical_domain, &facts.capabilities)?;
                let expected = instances
                    .get(&(*channel_id, *binding, *participant))
                    .cloned()
                    .unwrap_or_default();
                per_participant
                    .entry(*participant)
                    .or_default()
                    .entry(*channel_id)
                    .or_default()
                    .1
                    .insert(
                        *binding,
                        ConsumerDeployment::with_profile(
                            facts.activation,
                            facts.capabilities.clone(),
                            profile,
                            route_edge_id,
                            expected,
                        ),
                    );
            }
        }
    }

    if skipped_remote_consumers > 0 {
        tracing::info!(
            skipped_remote_consumers,
            "RFD-2 projected loopback only; remote consumer(s) deferred to RFD-4 transport"
        );
    }

    let mut views = BTreeMap::new();
    for (participant, channels) in per_participant {
        let mut channel_deployments = BTreeMap::new();
        for (channel_id, (producers, consumers)) in channels {
            let spec = &channel_specs[&channel_id];
            channel_deployments.insert(
                channel_id,
                RuntimeFilterChannelDeployment::new(
                    channel_id,
                    spec.logical_domain.clone(),
                    spec.lifecycle,
                    spec.availability_coverage.clone(),
                    spec.terminal_coverage.clone(),
                    spec.reduction_requirement,
                    spec.allowed_contribution_kinds.clone(),
                    spec.completion_requirement,
                    spec.policy,
                    core_budget,
                    materialization,
                    producers,
                    consumers,
                ),
            );
        }
        views.insert(
            participant,
            RuntimeFilterInstallView::new(epoch, participant, channel_deployments),
        );
    }
    Ok(views)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::types::UniqueId;
    use crate::runtime_filter::deployment::role_graph::*;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};

    fn membership_channel(id: u32) -> ChannelProjectionSpec {
        ChannelProjectionSpec {
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
            completion_requirement: CompletionRequirement::ProducerClosed,
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
            producer_witness: BTreeMap::from([(BindingId::new(10), CoverageWitnessId::new(1))]),
        }
    }

    fn top_k_summary_channel(id: u32) -> ChannelProjectionSpec {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        ChannelProjectionSpec {
            channel_id: ChannelId::new(id),
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                comparator_digest:
                    crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                        &keys,
                        crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                    ),
                keys,
                inclusive: true,
            }),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement: ReductionRequirement::MergeTopKSummary(
                TopKSummaryRequirement::try_new(3).unwrap(),
            ),
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
            producer_witness: BTreeMap::from([(BindingId::new(10), CoverageWitnessId::new(1))]),
        }
    }

    /// M2 Membership consumers must declare both `Membership` and `EmptyDomain`
    /// semantics (RFD-3/M2 install收紧 §158); the derived profile then accepts
    /// `{ValueSet, EmptyDomain}`.
    fn membership_consumer_facts(binding: u32) -> (BindingId, ConsumerBindingFacts) {
        (
            BindingId::new(binding),
            ConsumerBindingFacts {
                activation: ConsumerActivation::BlockingSnapshot,
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
            },
        )
    }

    fn pid(raw: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(raw)
    }

    fn finst(raw: i64) -> UniqueId {
        UniqueId {
            hi: raw,
            lo: raw + 100,
        }
    }

    fn all_of_projection_fixture() -> (
        RoleGraph,
        BTreeMap<ChannelId, ChannelProjectionSpec>,
        BTreeMap<BindingId, ConsumerBindingFacts>,
        BindingInstanceIndex,
    ) {
        let channel_id = ChannelId::new(5);
        let producer_binding = BindingId::new(10);
        let second_producer_binding = BindingId::new(20);
        let consumer_binding = BindingId::new(11);
        let aggregator = pid(2);
        let remote_producer = pid(7);
        let second_remote_producer = pid(13);
        let remote_consumer = pid(11);

        let mut channel = ChannelRoleGraph::empty(channel_id);
        channel
            .producers
            .insert(aggregator, BTreeSet::from([producer_binding]));
        channel.producers.insert(
            remote_producer,
            BTreeSet::from([producer_binding, second_producer_binding]),
        );
        channel.producers.insert(
            second_remote_producer,
            BTreeSet::from([second_producer_binding]),
        );
        channel
            .consumers
            .insert(remote_consumer, BTreeSet::from([consumer_binding]));
        channel.aggregator = Some(aggregator);
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(1),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: aggregator,
                binding: producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(2),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: remote_producer,
                binding: producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(3),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: remote_producer,
                binding: second_producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: second_producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(4),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: second_remote_producer,
                binding: second_producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: second_producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(5),
            kind: RouteKind::FromAggregator,
            from: RouteEndpoint {
                participant: aggregator,
                binding: consumer_binding,
            },
            to: RouteEndpoint {
                participant: remote_consumer,
                binding: consumer_binding,
            },
        });

        let instances = BTreeMap::from([
            (
                (channel_id, producer_binding, aggregator),
                BTreeSet::from([finst(2)]),
            ),
            (
                (channel_id, producer_binding, remote_producer),
                BTreeSet::from([finst(7)]),
            ),
            (
                (channel_id, second_producer_binding, remote_producer),
                BTreeSet::from([finst(17)]),
            ),
            (
                (channel_id, second_producer_binding, second_remote_producer),
                BTreeSet::from([finst(13)]),
            ),
            (
                (channel_id, consumer_binding, remote_consumer),
                BTreeSet::from([finst(11)]),
            ),
        ]);
        let mut channel_spec = membership_channel(5);
        channel_spec
            .producer_witness
            .insert(second_producer_binding, CoverageWitnessId::new(2));

        (
            RoleGraph {
                channels: BTreeMap::from([(channel_id, channel)]),
            },
            BTreeMap::from([(channel_id, channel_spec)]),
            BTreeMap::from([membership_consumer_facts(11)]),
            instances,
        )
    }

    fn project_all_of_fixture()
    -> Result<BTreeMap<RuntimeFilterParticipantId, RuntimeFilterInstallView>, DeploymentError> {
        let (role_graph, channel_specs, consumer_facts, instances) = all_of_projection_fixture();
        project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
    }

    #[test]
    fn all_of_aggregator_projects_union_of_remote_producer_instances() {
        let views = project_all_of_fixture().expect("projection succeeds");

        let aggregator_producer =
            &views[&pid(2)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(10)];
        assert_eq!(
            aggregator_producer.expected_fragment_instances(),
            &BTreeSet::from([finst(2), finst(7)])
        );
        assert_eq!(
            views[&pid(2)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(20)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(13), finst(17)])
        );
    }

    #[test]
    fn aggregator_without_local_consumer_still_gets_core_channel() {
        let views = project_all_of_fixture().expect("projection succeeds");

        let channel = &views[&pid(2)].channels()[&ChannelId::new(5)];
        assert_eq!(
            channel.producers()[&BindingId::new(10)].expected_fragment_instances(),
            &BTreeSet::from([finst(2), finst(7)])
        );
        assert!(channel.consumers().is_empty());
    }

    #[test]
    fn non_aggregator_producer_keeps_only_its_local_instances() {
        let views = project_all_of_fixture().expect("projection succeeds");

        assert_eq!(
            views[&pid(7)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(10)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(7)])
        );
        assert_eq!(
            views[&pid(7)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(20)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(17)])
        );
        assert_eq!(
            views[&pid(13)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(20)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(13)])
        );
    }

    #[test]
    fn remote_only_consumer_still_has_no_install_view_until_m2c() {
        let views = project_all_of_fixture().expect("projection succeeds");

        assert!(!views.contains_key(&pid(11)));
    }

    #[test]
    fn all_of_aggregator_rejects_missing_producer_witness() {
        let (role_graph, mut channel_specs, consumer_facts, instances) =
            all_of_projection_fixture();
        channel_specs
            .get_mut(&ChannelId::new(5))
            .unwrap()
            .producer_witness
            .clear();

        let err = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            DeploymentError::InvalidInstallProjection { detail }
                if detail.contains("producer witness")
        ));
    }

    #[test]
    fn all_of_aggregator_rejects_missing_producer_placement() {
        let (role_graph, channel_specs, consumer_facts, mut instances) =
            all_of_projection_fixture();
        instances.remove(&(ChannelId::new(5), BindingId::new(10), pid(7)));

        let err = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            DeploymentError::InvalidInstallProjection { detail }
                if detail.contains("producer placement")
        ));
    }

    #[test]
    fn ordered_range_projector_emits_exact_profile_contract() {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        };
        let expected =
            crate::runtime_filter::port::ordered_bound::RuntimeOrderContract::try_from_plan(&plan)
                .unwrap()
                .digest();
        let profile = consumer_artifact_profile(
            &RuntimeFilterLogicalDomain::OrderedBound(plan),
            &BTreeSet::from([ArtifactCapability::OrderedRange]),
        )
        .unwrap();

        assert_eq!(
            profile.accepted_kinds(),
            &BTreeSet::from([ArtifactKind::Range])
        );
        assert_eq!(profile.order_contract_digest(), Some(expected));
    }

    #[test]
    fn projection_preserves_top_k_summary_requirement() {
        let participant = RuntimeFilterParticipantId::new(0);
        let mut channel_graph = ChannelRoleGraph::empty(ChannelId::new(5));
        channel_graph
            .producers
            .insert(participant, BTreeSet::from([BindingId::new(10)]));
        let mut role_graph = RoleGraph::default();
        role_graph.channels.insert(ChannelId::new(5), channel_graph);

        let projected = top_k_summary_channel(5);
        assert_eq!(
            projected.reduction_requirement,
            ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(3).unwrap())
        );
        let channel_specs = BTreeMap::from([(ChannelId::new(5), projected)]);
        let instances = BTreeMap::from([(
            (ChannelId::new(5), BindingId::new(10), participant),
            BTreeSet::from([UniqueId { hi: 1, lo: 2 }]),
        )]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &BTreeMap::new(),
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let deployment = &views[&participant].channels()[&ChannelId::new(5)];
        assert_eq!(
            deployment.reduction_requirement(),
            ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(3).unwrap())
        );
    }

    #[test]
    fn loopback_projection_passes_be_side_validate_view() {
        let part = RuntimeFilterParticipantId::new(0);
        let finst = UniqueId { hi: 1, lo: 2 };
        let mut cg = ChannelRoleGraph::empty(ChannelId::new(5));
        cg.producers
            .insert(part, BTreeSet::from([BindingId::new(10)]));
        cg.consumers
            .insert(part, BTreeSet::from([BindingId::new(11)]));
        cg.routes.push(RouteEdge {
            channel: ChannelId::new(5),
            edge_id: crate::runtime_filter::port::identity::RouteEdgeId::new(1),
            kind: RouteKind::Loopback,
            from: RouteEndpoint {
                participant: part,
                binding: BindingId::new(10),
            },
            to: RouteEndpoint {
                participant: part,
                binding: BindingId::new(11),
            },
        });
        let mut role_graph = RoleGraph::default();
        role_graph.channels.insert(ChannelId::new(5), cg);

        let mut instances: BTreeMap<
            (ChannelId, BindingId, RuntimeFilterParticipantId),
            BTreeSet<UniqueId>,
        > = BTreeMap::new();
        instances.insert(
            (ChannelId::new(5), BindingId::new(10), part),
            BTreeSet::from([finst]),
        );
        instances.insert(
            (ChannelId::new(5), BindingId::new(11), part),
            BTreeSet::from([finst]),
        );

        let mut channel_specs = BTreeMap::new();
        channel_specs.insert(ChannelId::new(5), membership_channel(5));

        let consumer_facts = BTreeMap::from([membership_consumer_facts(11)]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let view = views.get(&part).expect("participant has a view");
        // Reuse the BE-side validator to prove the shard is well-formed.
        crate::runtime_filter::service::registry::validate_view_for_test(view)
            .expect("compiler output must satisfy BE install contract");
    }

    #[test]
    fn remote_consumer_without_loopback_route_is_excluded_from_views() {
        // Producer lives on participant 0; consumer lives on participant 1 with
        // NO Loopback route between them (e.g. a ToAggregator/FromAggregator
        // shape not yet wired to the M1 install DTO). RFD-2 projects loopback
        // consumers only, so binding 11 must never surface in any install view.
        let producer_participant = RuntimeFilterParticipantId::new(0);
        let consumer_participant = RuntimeFilterParticipantId::new(1);
        let finst = UniqueId { hi: 1, lo: 2 };

        let mut cg = ChannelRoleGraph::empty(ChannelId::new(5));
        cg.producers
            .insert(producer_participant, BTreeSet::from([BindingId::new(10)]));
        cg.consumers
            .insert(consumer_participant, BTreeSet::from([BindingId::new(11)]));
        // Deliberately no RouteKind::Loopback edge to (consumer_participant, 11).

        let mut role_graph = RoleGraph::default();
        role_graph.channels.insert(ChannelId::new(5), cg);

        let mut instances: BTreeMap<
            (ChannelId, BindingId, RuntimeFilterParticipantId),
            BTreeSet<UniqueId>,
        > = BTreeMap::new();
        instances.insert(
            (ChannelId::new(5), BindingId::new(10), producer_participant),
            BTreeSet::from([finst]),
        );
        instances.insert(
            (ChannelId::new(5), BindingId::new(11), consumer_participant),
            BTreeSet::from([finst]),
        );

        let mut channel_specs = BTreeMap::new();
        channel_specs.insert(ChannelId::new(5), membership_channel(5));

        let consumer_facts = BTreeMap::from([membership_consumer_facts(11)]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");

        // The remote consumer must never be silently promoted into a view.
        assert!(
            !views.values().any(|v| v
                .channels()
                .values()
                .any(|c| c.consumers().contains_key(&BindingId::new(11)))),
            "remote (non-loopback) consumer binding must not be projected into any install view"
        );
        assert!(
            views.get(&consumer_participant).is_none(),
            "a participant with only a skipped remote consumer must get no view"
        );
        // The producer's own participant still gets its producer-side deployment.
        let producer_view = views
            .get(&producer_participant)
            .expect("producer participant has a view");
        assert!(
            producer_view.channels()[&ChannelId::new(5)]
                .producers()
                .contains_key(&BindingId::new(10))
        );
    }
}
