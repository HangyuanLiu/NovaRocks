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
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;

use super::identity::{DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterCoreBudget {
    max_reducer_bytes: u64,
}

impl RuntimeFilterCoreBudget {
    pub(crate) const fn new(max_reducer_bytes: u64) -> Self {
        Self { max_reducer_bytes }
    }

    pub(crate) const fn max_reducer_bytes(self) -> u64 {
        self.max_reducer_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProducerDeployment {
    coverage_witness_id: CoverageWitnessId,
    expected_fragment_instances: BTreeSet<UniqueId>,
}

impl ProducerDeployment {
    pub(crate) fn new(
        coverage_witness_id: CoverageWitnessId,
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> Self {
        Self {
            coverage_witness_id,
            expected_fragment_instances,
        }
    }

    pub(crate) const fn coverage_witness_id(&self) -> CoverageWitnessId {
        self.coverage_witness_id
    }

    pub(crate) const fn expected_fragment_instances(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instances
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerDeployment {
    activation: ConsumerActivation,
    capabilities: BTreeSet<ArtifactCapability>,
    loopback_route_edge_id: RouteEdgeId,
    expected_fragment_instances: BTreeSet<UniqueId>,
}

impl ConsumerDeployment {
    pub(crate) fn new(
        activation: ConsumerActivation,
        capabilities: BTreeSet<ArtifactCapability>,
        loopback_route_edge_id: RouteEdgeId,
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> Self {
        Self {
            activation,
            capabilities,
            loopback_route_edge_id,
            expected_fragment_instances,
        }
    }

    pub(crate) const fn activation(&self) -> ConsumerActivation {
        self.activation
    }

    pub(crate) const fn capabilities(&self) -> &BTreeSet<ArtifactCapability> {
        &self.capabilities
    }

    pub(crate) const fn loopback_route_edge_id(&self) -> RouteEdgeId {
        self.loopback_route_edge_id
    }

    pub(crate) const fn expected_fragment_instances(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instances
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompleteOnceChannelDeployment {
    channel_id: ChannelId,
    logical_domain: RuntimeFilterLogicalDomain,
    lifecycle: RuntimeFilterLifecycle,
    availability_coverage: Coverage,
    terminal_coverage: Coverage,
    reduction_requirement: ReductionRequirement,
    allowed_contribution_kinds: BTreeSet<ContributionKind>,
    completion_requirement: CompletionRequirement,
    policy: RuntimeFilterPolicyRequirement,
    core_budget: RuntimeFilterCoreBudget,
    producers: BTreeMap<BindingId, ProducerDeployment>,
    consumers: BTreeMap<BindingId, ConsumerDeployment>,
}

impl CompleteOnceChannelDeployment {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        channel_id: ChannelId,
        logical_domain: RuntimeFilterLogicalDomain,
        lifecycle: RuntimeFilterLifecycle,
        availability_coverage: Coverage,
        terminal_coverage: Coverage,
        reduction_requirement: ReductionRequirement,
        allowed_contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        policy: RuntimeFilterPolicyRequirement,
        core_budget: RuntimeFilterCoreBudget,
        producers: BTreeMap<BindingId, ProducerDeployment>,
        consumers: BTreeMap<BindingId, ConsumerDeployment>,
    ) -> Self {
        Self {
            channel_id,
            logical_domain,
            lifecycle,
            availability_coverage,
            terminal_coverage,
            reduction_requirement,
            allowed_contribution_kinds,
            completion_requirement,
            policy,
            core_budget,
            producers,
            consumers,
        }
    }

    pub(crate) const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }
    pub(crate) const fn logical_domain(&self) -> &RuntimeFilterLogicalDomain {
        &self.logical_domain
    }
    pub(crate) const fn lifecycle(&self) -> RuntimeFilterLifecycle {
        self.lifecycle
    }
    pub(crate) const fn availability_coverage(&self) -> &Coverage {
        &self.availability_coverage
    }
    pub(crate) const fn terminal_coverage(&self) -> &Coverage {
        &self.terminal_coverage
    }
    pub(crate) const fn reduction_requirement(&self) -> ReductionRequirement {
        self.reduction_requirement
    }
    pub(crate) const fn allowed_contribution_kinds(&self) -> &BTreeSet<ContributionKind> {
        &self.allowed_contribution_kinds
    }
    pub(crate) const fn completion_requirement(&self) -> CompletionRequirement {
        self.completion_requirement
    }
    pub(crate) const fn policy(&self) -> RuntimeFilterPolicyRequirement {
        self.policy
    }
    pub(crate) const fn core_budget(&self) -> RuntimeFilterCoreBudget {
        self.core_budget
    }
    pub(crate) const fn producers(&self) -> &BTreeMap<BindingId, ProducerDeployment> {
        &self.producers
    }
    pub(crate) const fn consumers(&self) -> &BTreeMap<BindingId, ConsumerDeployment> {
        &self.consumers
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterInstallView {
    epoch: DeploymentEpoch,
    local_participant_id: RuntimeFilterParticipantId,
    channels: BTreeMap<ChannelId, CompleteOnceChannelDeployment>,
}

impl RuntimeFilterInstallView {
    pub(crate) fn new(
        epoch: DeploymentEpoch,
        local_participant_id: RuntimeFilterParticipantId,
        channels: BTreeMap<ChannelId, CompleteOnceChannelDeployment>,
    ) -> Self {
        Self {
            epoch,
            local_participant_id,
            channels,
        }
    }

    pub(crate) const fn epoch(&self) -> DeploymentEpoch {
        self.epoch
    }
    pub(crate) const fn local_participant_id(&self) -> RuntimeFilterParticipantId {
        self.local_participant_id
    }
    pub(crate) const fn channels(&self) -> &BTreeMap<ChannelId, CompleteOnceChannelDeployment> {
        &self.channels
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::identity::*;

    use super::*;

    #[test]
    fn install_view_keeps_expected_producer_and_consumer_instances() {
        let channel_id = ChannelId::new(1);
        let producer_binding_id = BindingId::new(2);
        let consumer_binding_id = BindingId::new(3);
        let witness_id = CoverageWitnessId::new(4);
        let producer_instances =
            BTreeSet::from([UniqueId { hi: 10, lo: 11 }, UniqueId { hi: 12, lo: 13 }]);
        let consumer_instances = BTreeSet::from([UniqueId { hi: 14, lo: 15 }]);

        let deployment = CompleteOnceChannelDeployment::new(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness_id),
            Coverage::Leaf(witness_id),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 128,
                max_artifact_bytes: 256,
                deadline_ms: 1_000,
                max_retries: 7,
            },
            RuntimeFilterCoreBudget::new(512),
            BTreeMap::from([(
                producer_binding_id,
                ProducerDeployment::new(witness_id, producer_instances.clone()),
            )]),
            BTreeMap::from([(
                consumer_binding_id,
                ConsumerDeployment::new(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([ArtifactCapability::Membership]),
                    RouteEdgeId::new(5),
                    consumer_instances.clone(),
                ),
            )]),
        );
        let view = RuntimeFilterInstallView::new(
            DeploymentEpoch::new(6),
            RuntimeFilterParticipantId::new(7),
            BTreeMap::from([(channel_id, deployment)]),
        );

        let installed = view.channels().get(&channel_id).unwrap();
        assert_eq!(
            installed
                .producers()
                .get(&producer_binding_id)
                .unwrap()
                .expected_fragment_instances(),
            &producer_instances
        );
        assert_eq!(
            installed
                .consumers()
                .get(&consumer_binding_id)
                .unwrap()
                .expected_fragment_instances(),
            &consumer_instances
        );
        assert_eq!(installed.policy().max_retries, 7);
        assert_eq!(installed.core_budget().max_reducer_bytes(), 512);
    }
}
