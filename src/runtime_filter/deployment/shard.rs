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
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    CompleteOnceChannelDeployment, ConsumerDeployment, ProducerDeployment, RuntimeFilterCoreBudget,
    RuntimeFilterInstallView,
};

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

type InstanceIndex =
    BTreeMap<(ChannelId, BindingId, RuntimeFilterParticipantId), BTreeSet<UniqueId>>;

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
/// is skipped rather than panicking; there is no downstream "all consumers
/// present" check, so a silent drop here would be invisible.
pub(crate) fn project_install_views(
    epoch: DeploymentEpoch,
    role_graph: &RoleGraph,
    channel_specs: &BTreeMap<ChannelId, ChannelProjectionSpec>,
    consumer_facts: &BTreeMap<BindingId, ConsumerBindingFacts>,
    instances: &InstanceIndex,
    core_budget: RuntimeFilterCoreBudget,
) -> BTreeMap<RuntimeFilterParticipantId, RuntimeFilterInstallView> {
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
                        ConsumerDeployment::new(
                            facts.activation,
                            facts.capabilities.clone(),
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
                CompleteOnceChannelDeployment::new(
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
    views
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

        let consumer_facts = BTreeMap::from([(
            BindingId::new(11),
            ConsumerBindingFacts {
                activation: ConsumerActivation::BlockingSnapshot,
                capabilities: BTreeSet::from([ArtifactCapability::Membership]),
            },
        )]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
        );
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

        let consumer_facts = BTreeMap::from([(
            BindingId::new(11),
            ConsumerBindingFacts {
                activation: ConsumerActivation::BlockingSnapshot,
                capabilities: BTreeSet::from([ArtifactCapability::Membership]),
            },
        )]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
        );

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
