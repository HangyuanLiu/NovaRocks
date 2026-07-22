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

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ContributionKind,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::port::artifact::{ArtifactMembershipSchema, ConsumerArtifactProfile};
use crate::runtime_filter::port::identity::DeploymentEpoch;
use crate::runtime_filter::port::ordered_bound::{RuntimeOrderContract, RuntimeOrderKey};
use crate::runtime_filter::port::producer::{
    ProducerAdapter, ProducerPortKind, RuntimeContractViolation, RuntimeContractViolationKind,
};
use crate::runtime_filter::port::subscription::{SubscriptionHandle, SubscriptionKind};
use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;

use super::RuntimeFilterService;

/// Immutable query-owned runtime-filter authority carried into one native
/// fragment execution. It deliberately contains no process-global lookup seam.
#[derive(Clone)]
pub(crate) struct NativeRuntimeFilterExecutionContext {
    service: Arc<RuntimeFilterService>,
    query_id: UniqueId,
    epoch: DeploymentEpoch,
    fragment_instance_id: UniqueId,
}

impl std::fmt::Debug for NativeRuntimeFilterExecutionContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeRuntimeFilterExecutionContext")
            .field("query_id", &self.query_id)
            .field("epoch", &self.epoch)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .finish_non_exhaustive()
    }
}

impl NativeRuntimeFilterExecutionContext {
    pub(crate) fn new(
        service: Arc<RuntimeFilterService>,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            service,
            query_id,
            epoch,
            fragment_instance_id,
        }
    }

    #[cfg(test)]
    pub(crate) const fn service(&self) -> &Arc<RuntimeFilterService> {
        &self.service
    }

    pub(crate) const fn query_id(&self) -> UniqueId {
        self.query_id
    }

    pub(crate) const fn epoch(&self) -> DeploymentEpoch {
        self.epoch
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn resolve_producer(
        &self,
        binding_id: BindingId,
        channel_id: ChannelId,
        requested_kind: ProducerPortKind,
    ) -> Result<ResolvedNativeProducer, RuntimeContractViolation> {
        let installed = self.resolve_installation()?;
        let channel = installed.channel_deployment(channel_id).ok_or_else(|| {
            resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native producer channel is not installed",
            )
        })?;
        let producer = channel.producers().get(&binding_id).ok_or_else(|| {
            resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native binding does not have the installed producer role",
            )
        })?;
        if !producer
            .expected_fragment_instances()
            .contains(&self.fragment_instance_id)
        {
            return Err(resolution_violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "native producer fragment instance is not installed for the binding",
            ));
        }
        if installed.producer_participant(channel_id, binding_id, self.fragment_instance_id)
            != Some(installed.participant_id())
        {
            return Err(resolution_violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "native producer fragment instance is not owned by the local participant",
            ));
        }
        let route = installed.producer(binding_id).ok_or_else(|| {
            resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native binding does not have a local producer route",
            )
        })?;
        if route.channel_id() != channel_id || route.kind != requested_kind {
            return Err(resolution_violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "native producer port kind or channel does not match the installed route",
            ));
        }
        let contract = installed_contract(channel.logical_domain())?;
        let topk_contract_digest = installed_topk_contract_digest(
            channel.logical_domain(),
            channel.reduction_requirement(),
        )?;
        Ok(ResolvedNativeProducer {
            service: Arc::clone(&self.service),
            binding_id,
            channel_id,
            fragment_instance_id: self.fragment_instance_id,
            kind: requested_kind,
            contract,
            reduction_requirement: channel.reduction_requirement(),
            allowed_contribution_kinds: channel.allowed_contribution_kinds().clone(),
            completion_requirement: channel.completion_requirement(),
            topk_contract_digest,
            max_contribution_bytes: route.inbound_contract().limits().max_contribution_bytes(),
        })
    }

    pub(crate) fn resolve_consumer(
        &self,
        binding_id: BindingId,
        channel_id: ChannelId,
        requested_kind: SubscriptionKind,
    ) -> Result<ResolvedNativeConsumer, RuntimeContractViolation> {
        let installed = self.resolve_installation()?;
        let channel = installed.channel_deployment(channel_id).ok_or_else(|| {
            resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native consumer channel is not installed",
            )
        })?;
        let consumer = channel.consumers().get(&binding_id).ok_or_else(|| {
            resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native binding does not have the installed consumer role",
            )
        })?;
        if !consumer
            .expected_fragment_instances()
            .contains(&self.fragment_instance_id)
        {
            return Err(resolution_violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "native consumer fragment instance is not installed for the binding",
            ));
        }
        let installed_kind = match consumer.activation() {
            crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot => {
                SubscriptionKind::BlockingSnapshot
            }
            crate::runtime_filter::model::contract::ConsumerActivation::NonBlockingLive {
                ..
            } => SubscriptionKind::NonBlockingLive,
        };
        if installed_kind != requested_kind {
            return Err(resolution_violation(
                RuntimeContractViolationKind::SubscriptionActivationMismatch,
                "native consumer activation does not match the requested subscription kind",
            ));
        }
        Ok(ResolvedNativeConsumer {
            service: Arc::clone(&self.service),
            binding_id,
            channel_id,
            fragment_instance_id: self.fragment_instance_id,
            subscription_kind: requested_kind,
            activation: consumer.activation(),
            capabilities: consumer.capabilities().clone(),
            artifact_profile: consumer.artifact_profile().clone(),
            contract: installed_contract(channel.logical_domain())?,
            lifecycle: channel.lifecycle(),
            reduction_requirement: channel.reduction_requirement(),
            topk_contract_digest: installed_topk_contract_digest(
                channel.logical_domain(),
                channel.reduction_requirement(),
            )?,
        })
    }

    fn resolve_installation(
        &self,
    ) -> Result<Arc<super::registry::InstalledDeployment>, RuntimeContractViolation> {
        if self.service._query_id != self.query_id {
            return Err(resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native runtime-filter context query does not match its Service",
            ));
        }
        let installed = self.service.registry.active_installation().ok_or_else(|| {
            resolution_violation(
                RuntimeContractViolationKind::ServiceUnavailable,
                "native runtime-filter deployment is not active",
            )
        })?;
        if installed.epoch() != self.epoch {
            return Err(resolution_violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "native runtime-filter execution epoch does not match the active installation",
            ));
        }
        Ok(installed)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InstalledNativeRuntimeFilterContract {
    Membership {
        canonical_schema: Arc<[u8]>,
        schema_digest: [u8; 32],
    },
    Ordered {
        keys: Arc<[RuntimeOrderKey]>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    },
}

pub(crate) struct ResolvedNativeProducer {
    service: Arc<RuntimeFilterService>,
    binding_id: BindingId,
    channel_id: ChannelId,
    fragment_instance_id: UniqueId,
    kind: ProducerPortKind,
    contract: InstalledNativeRuntimeFilterContract,
    reduction_requirement: ReductionRequirement,
    allowed_contribution_kinds: BTreeSet<ContributionKind>,
    completion_requirement: CompletionRequirement,
    topk_contract_digest: Option<[u8; 32]>,
    max_contribution_bytes: usize,
}

impl std::fmt::Debug for ResolvedNativeProducer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedNativeProducer")
            .field("binding_id", &self.binding_id)
            .field("channel_id", &self.channel_id)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl ResolvedNativeProducer {
    pub(crate) const fn contract(&self) -> &InstalledNativeRuntimeFilterContract {
        &self.contract
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

    pub(crate) const fn topk_contract_digest(&self) -> Option<[u8; 32]> {
        self.topk_contract_digest
    }

    pub(crate) const fn max_contribution_bytes(&self) -> usize {
        self.max_contribution_bytes
    }

    pub(crate) fn open_membership(
        &self,
        local_partition_count: u32,
    ) -> Result<Arc<dyn ProducerAdapter>, RuntimeContractViolation> {
        if self.kind != ProducerPortKind::Membership {
            return Err(resolution_violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "resolved producer is not a membership producer",
            ));
        }
        self.service
            .open_producer(
                self.binding_id,
                self.fragment_instance_id,
                local_partition_count,
                self.kind,
            )?
            .into_membership()
    }
}

pub(crate) struct ResolvedNativeConsumer {
    service: Arc<RuntimeFilterService>,
    binding_id: BindingId,
    channel_id: ChannelId,
    fragment_instance_id: UniqueId,
    subscription_kind: SubscriptionKind,
    activation: crate::runtime_filter::model::contract::ConsumerActivation,
    capabilities: BTreeSet<ArtifactCapability>,
    artifact_profile: ConsumerArtifactProfile,
    contract: InstalledNativeRuntimeFilterContract,
    lifecycle: RuntimeFilterLifecycle,
    reduction_requirement: ReductionRequirement,
    topk_contract_digest: Option<[u8; 32]>,
}

impl std::fmt::Debug for ResolvedNativeConsumer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedNativeConsumer")
            .field("binding_id", &self.binding_id)
            .field("channel_id", &self.channel_id)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("subscription_kind", &self.subscription_kind)
            .finish_non_exhaustive()
    }
}

impl ResolvedNativeConsumer {
    pub(crate) const fn activation(
        &self,
    ) -> crate::runtime_filter::model::contract::ConsumerActivation {
        self.activation
    }

    pub(crate) const fn capabilities(&self) -> &BTreeSet<ArtifactCapability> {
        &self.capabilities
    }

    pub(crate) const fn artifact_profile(&self) -> &ConsumerArtifactProfile {
        &self.artifact_profile
    }

    pub(crate) const fn contract(&self) -> &InstalledNativeRuntimeFilterContract {
        &self.contract
    }

    pub(crate) const fn lifecycle(&self) -> RuntimeFilterLifecycle {
        self.lifecycle
    }

    pub(crate) const fn reduction_requirement(&self) -> ReductionRequirement {
        self.reduction_requirement
    }

    pub(crate) const fn topk_contract_digest(&self) -> Option<[u8; 32]> {
        self.topk_contract_digest
    }

    pub(crate) fn subscribe(&self) -> Result<SubscriptionHandle, RuntimeContractViolation> {
        self.service.subscribe(
            self.binding_id,
            self.fragment_instance_id,
            self.subscription_kind,
        )
    }
}

fn installed_contract(
    logical_domain: &RuntimeFilterLogicalDomain,
) -> Result<InstalledNativeRuntimeFilterContract, RuntimeContractViolation> {
    match logical_domain {
        RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } => {
            let schema =
                ArtifactMembershipSchema::new(value_type, *null_semantics).map_err(|_| {
                    resolution_violation(
                        RuntimeContractViolationKind::TypeMismatch,
                        "installed membership schema is invalid",
                    )
                })?;
            Ok(InstalledNativeRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            })
        }
        RuntimeFilterLogicalDomain::OrderedBound(contract) => {
            let contract = RuntimeOrderContract::try_from_plan(contract).map_err(|_| {
                resolution_violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "installed ordered contract is invalid",
                )
            })?;
            Ok(InstalledNativeRuntimeFilterContract::Ordered {
                keys: Arc::from(contract.keys()),
                comparator_digest: contract.plan_comparator_digest().get(),
                order_contract_digest: contract.digest().bytes(),
            })
        }
    }
}

fn installed_topk_contract_digest(
    logical_domain: &RuntimeFilterLogicalDomain,
    reduction: ReductionRequirement,
) -> Result<Option<[u8; 32]>, RuntimeContractViolation> {
    let ReductionRequirement::MergeTopKSummary(requirement) = reduction else {
        return Ok(None);
    };
    let RuntimeFilterLogicalDomain::OrderedBound(order) = logical_domain else {
        return Err(resolution_violation(
            RuntimeContractViolationKind::TypeMismatch,
            "installed TopK reduction is missing its ordered contract",
        ));
    };
    RuntimeTopKSummaryContract::try_from_plan(order, requirement)
        .map(|contract| Some(contract.digest().bytes()))
        .map_err(|_| {
            resolution_violation(
                RuntimeContractViolationKind::OrderedContractMismatch,
                "installed TopK summary contract is invalid",
            )
        })
}

fn resolution_violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}
