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
use std::time::Duration;

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter as execution;
use novarocks_spi::connector::ConnectorScalarValue;

use novarocks::runtime_filter_transition::codec::contribution::{
    ContributionCodecError, RuntimeFilterContribution as CoreContribution,
    decode_canonical_membership_body, decode_contribution,
};
use novarocks::runtime_filter_transition::materializer::codec::{
    ArtifactCodecError, MembershipProbe, encode_range_leaf, indexed_membership_contains,
    indexed_membership_range_may_match,
};
use novarocks::runtime_filter_transition::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ContributionKind,
    NullSemantics, ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
};
use novarocks::runtime_filter_transition::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ArtifactSchemaDigest, ConsumerArtifactProfile,
    LEAF_CODEC_VERSION, PhysicalArtifact, ResidentMembershipIndexView,
};
use novarocks::runtime_filter_transition::port::identity::{DeploymentEpoch, LogicalVersion};
use novarocks::runtime_filter_transition::port::ordered_bound::{
    OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey,
};
use novarocks::runtime_filter_transition::port::producer::{
    OrderedBoundProducerAdapter, ProducerAdapter, ProducerFailureReason, ProducerHandle,
    ProducerPortKind, RuntimeContractViolation, RuntimeContractViolationKind,
};
use novarocks::runtime_filter_transition::port::subscription::{
    ArtifactAcquireOutcome, ArtifactUnsupportedReason, BlockingSnapshotSubscription,
    LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription, SubscriptionHandle,
    SubscriptionKind, UnavailableReason,
};
use novarocks::runtime_filter_transition::port::topk_summary::RuntimeTopKSummaryContract;
use novarocks_types::UniqueId;

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

    #[cfg(test)]
    pub(crate) fn installed_ordered_consumer_context_for_exec_test()
    -> (Self, Arc<RuntimeOrderContract>) {
        let service = super::tests::installed_ordered_service_fixture();
        let context = Self::new(
            service,
            UniqueId::new(70, 0),
            DeploymentEpoch::new(9),
            UniqueId::new(70, 2),
        );
        let resolved = context
            .resolve_consumer(
                BindingId::new(2),
                ChannelId::new(1),
                SubscriptionKind::NonBlockingLive,
            )
            .expect("installed ordered test consumer resolves as live");
        let InstalledRuntimeFilterExecutionContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } = resolved.contract()
        else {
            panic!("installed ordered test consumer must expose an ordered contract")
        };
        let contract = Arc::new(
            RuntimeOrderContract::from_codec(
                keys.to_vec(),
                novarocks::runtime_filter_transition::model::contract::ComparatorDigest::new(*comparator_digest),
                novarocks::runtime_filter_transition::port::ordered_bound::OrderContractDigest::
                    from_bytes_for_codec(*order_contract_digest),
            )
            .expect("installed ordered test contract is valid"),
        );
        (context, contract)
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
            inbound_contract: route.inbound_contract().clone(),
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
            novarocks::runtime_filter_transition::model::contract::ConsumerActivation::BlockingSnapshot => {
                SubscriptionKind::BlockingSnapshot
            }
            novarocks::runtime_filter_transition::model::contract::ConsumerActivation::NonBlockingLive {
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
            snapshot_compiler: snapshot_predicate_compiler(channel.logical_domain())?,
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
pub(crate) enum InstalledRuntimeFilterExecutionContract {
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
    contract: InstalledRuntimeFilterExecutionContract,
    reduction_requirement: ReductionRequirement,
    allowed_contribution_kinds: BTreeSet<ContributionKind>,
    completion_requirement: CompletionRequirement,
    topk_contract_digest: Option<[u8; 32]>,
    max_contribution_bytes: usize,
    inbound_contract: super::registry::InboundProducerContract,
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
    pub(crate) const fn kind(&self) -> ProducerPortKind {
        self.kind
    }

    pub(crate) const fn contract(&self) -> &InstalledRuntimeFilterExecutionContract {
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

    pub(crate) fn execution_contract(&self) -> execution::RuntimeFilterExecutionContract {
        to_execution_contract(&self.contract)
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

    pub(crate) fn open_ordered_bound(
        &self,
        local_partition_count: u32,
    ) -> Result<Arc<dyn OrderedBoundProducerAdapter>, RuntimeContractViolation> {
        if self.kind != ProducerPortKind::OrderedBound {
            return Err(resolution_violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "resolved producer is not an ordered-bound producer",
            ));
        }
        match self.service.open_producer(
            self.binding_id,
            self.fragment_instance_id,
            local_partition_count,
            self.kind,
        )? {
            ProducerHandle::OrderedBound(adapter) => Ok(adapter),
            ProducerHandle::Membership(_)
            | ProducerHandle::TopKSummary(_)
            | ProducerHandle::FinalDomain(_) => Err(resolution_violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "resolved ordered-bound producer opened a different typed port",
            )),
        }
    }

    fn open_handle(
        &self,
        local_partition_count: u32,
    ) -> Result<ProducerHandle, RuntimeContractViolation> {
        self.service.open_producer(
            self.binding_id,
            self.fragment_instance_id,
            local_partition_count,
            self.kind,
        )
    }

    fn execution_producer(
        &self,
        local_partition_count: u32,
    ) -> Result<execution::RuntimeFilterProducerHandle, execution::RuntimeFilterContractViolation>
    {
        let handle = self
            .open_handle(local_partition_count)
            .map_err(execution_violation)?;
        Ok(Arc::new(NativeExecutionProducerAdapter {
            handle,
            binding_id: self.binding_id,
            fragment_instance_id: self.fragment_instance_id,
            inbound_contract: self.inbound_contract.clone(),
            max_contribution_bytes: self.max_contribution_bytes,
        }))
    }

    pub(crate) fn open_execution_producer(
        &self,
        local_partition_count: u32,
    ) -> Result<execution::RuntimeFilterProducerHandle, execution::RuntimeFilterContractViolation>
    {
        self.execution_producer(local_partition_count)
    }
}

impl execution::RuntimeFilterSession for NativeRuntimeFilterExecutionContext {
    fn open_producer(
        &self,
        request: execution::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        let contract = request.contract();
        let resolved = self
            .resolve_producer(
                BindingId::new(contract.binding_id().get()),
                ChannelId::new(contract.channel_id().get()),
                execution_producer_port_kind(contract.kind()),
            )
            .map_err(execution_violation)?;
        if to_execution_contract(resolved.contract()) != *contract.contract() {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "producer execution contract does not match the installed route",
            ));
        }
        match resolved.execution_producer(request.local_partition_count()) {
            Ok(handle) => Ok(execution::RuntimeFilterBindOutcome::Bound(handle)),
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                Ok(execution::RuntimeFilterBindOutcome::Unavailable(
                    execution::UnavailableReason::RouteUnavailable,
                ))
            }
            Err(error) => Err(error),
        }
    }

    fn subscribe(
        &self,
        request: execution::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        let contract = request.contract();
        let requested_kind = match contract.activation() {
            execution::ConsumerActivation::BlockingSnapshot => SubscriptionKind::BlockingSnapshot,
            execution::ConsumerActivation::NonBlockingLive { .. } => {
                SubscriptionKind::NonBlockingLive
            }
        };
        let resolved = self
            .resolve_consumer(
                BindingId::new(contract.binding_id().get()),
                ChannelId::new(contract.channel_id().get()),
                requested_kind,
            )
            .map_err(execution_violation)?;
        if to_execution_contract(resolved.contract()) != *contract.contract() {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "consumer execution contract does not match the installed route",
            ));
        }
        let handle = match resolved.subscribe() {
            Ok(handle) => handle,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                return Ok(execution::RuntimeFilterBindOutcome::Unavailable(
                    execution::UnavailableReason::RouteUnavailable,
                ));
            }
            Err(error) => return Err(execution_violation(error)),
        };
        let binding_id = execution::RuntimeFilterBindingId::new(contract.binding_id().get());
        let contract_digest = execution_contract_digest(contract.contract());
        let outcome = match handle {
            SubscriptionHandle::Blocking(subscription) => {
                execution::RuntimeFilterSubscriptionHandle::Blocking(Arc::new(
                    NativeExecutionBlockingSubscription {
                        binding_id,
                        contract_digest,
                        compiler: resolved.snapshot_compiler.clone(),
                        subscription,
                    },
                ))
            }
            SubscriptionHandle::Live(subscription) => {
                execution::RuntimeFilterSubscriptionHandle::Live(Arc::new(
                    NativeExecutionLiveSubscription {
                        binding_id,
                        contract_digest,
                        compiler: resolved.snapshot_compiler.clone(),
                        subscription,
                    },
                ))
            }
        };
        Ok(execution::RuntimeFilterBindOutcome::Bound(outcome))
    }

    fn open_final_domain_completion(
        &self,
        request: execution::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        execution::RuntimeFilterBindOutcome<execution::RuntimeFilterFinalDomainCompletionHandle>,
        execution::RuntimeFilterContractViolation,
    > {
        let contract = request.contract();
        if contract.kind() != execution::RuntimeFilterProducerKind::FinalDomain {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::RoleMismatch,
                "final-domain completion requires a FinalDomain producer contract",
            ));
        }
        let resolved = self
            .resolve_producer(
                BindingId::new(contract.binding_id().get()),
                ChannelId::new(contract.channel_id().get()),
                ProducerPortKind::FinalDomain,
            )
            .map_err(execution_violation)?;
        if to_execution_contract(resolved.contract()) != *contract.contract() {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "final-domain execution contract does not match the installed route",
            ));
        }
        let max_domain_canonical_bytes = resolved.max_contribution_bytes();
        let session = match self.service.open_final_aggregate_producer(
            resolved.binding_id,
            self.fragment_instance_id,
            request.local_partition_count(),
        ) {
            Ok(session) => session,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                return Ok(execution::RuntimeFilterBindOutcome::Unavailable(
                    execution::UnavailableReason::RouteUnavailable,
                ));
            }
            Err(error) => return Err(execution_violation(error)),
        };
        Ok(execution::RuntimeFilterBindOutcome::Bound(Arc::new(
            NativeExecutionFinalDomainCompletion {
                session: Arc::new(session),
                max_domain_canonical_bytes,
                expected_contract_digest: execution_contract_digest(contract.contract()),
            },
        )))
    }
}

struct NativeExecutionFinalDomainCompletion {
    session: Arc<super::final_domain_completion::FinalDomainCompletionSession>,
    max_domain_canonical_bytes: usize,
    expected_contract_digest: [u8; 32],
}

impl execution::RuntimeFilterFinalDomainCompletion for NativeExecutionFinalDomainCompletion {
    fn membership_key_type(&self) -> DataType {
        self.session.membership_key_type().clone()
    }

    fn max_domain_canonical_bytes(&self) -> usize {
        self.max_domain_canonical_bytes
    }

    fn contract_digest(&self) -> [u8; 32] {
        self.expected_contract_digest
    }

    fn claim_partition(
        &self,
        partition: execution::PartitionId,
    ) -> Result<
        execution::RuntimeFilterFinalDomainPartitionHandle,
        execution::RuntimeFilterContractViolation,
    > {
        self.session
            .partition(
                novarocks::runtime_filter_transition::port::identity::PartitionId::new(
                    partition.get(),
                ),
            )
            .map(|committer| {
                Box::new(NativeExecutionFinalDomainPartition {
                    committer,
                    expected_key_type: self.session.membership_key_type().clone(),
                    max_domain_canonical_bytes: self.max_domain_canonical_bytes,
                    expected_contract_digest: self.expected_contract_digest,
                }) as execution::RuntimeFilterFinalDomainPartitionHandle
            })
            .map_err(execution_violation)
    }

    fn fail(
        &self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        self.session
            .fail(core_failure_reason(reason))
            .map(execution_submit_outcome)
            .map_err(execution_violation)
    }
}

struct NativeExecutionFinalDomainPartition {
    committer: super::final_domain_completion::FinalDomainPartitionCommitter,
    expected_key_type: DataType,
    max_domain_canonical_bytes: usize,
    expected_contract_digest: [u8; 32],
}

impl execution::RuntimeFilterFinalDomainPartition for NativeExecutionFinalDomainPartition {
    fn seal(
        &mut self,
        payload: execution::RuntimeFilterFinalDomain,
    ) -> Result<(), execution::RuntimeFilterContractViolation> {
        if payload.canonical_bytes().len() > self.max_domain_canonical_bytes {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "final-domain payload exceeds the opened producer budget",
            ));
        }
        if payload.data_type() != &self.expected_key_type {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "final-domain payload type does not match the opened producer contract",
            ));
        }
        if payload.contract_digest() != self.expected_contract_digest {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "final-domain payload contract digest does not match the opened producer contract",
            ));
        }
        let decoded_execution = execution::contribution::decode_value_domain(
            payload.canonical_bytes(),
            payload.data_type(),
            self.max_domain_canonical_bytes,
        )
        .map_err(|error| {
            execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                error.to_string(),
            )
        })?;
        if decoded_execution.data_type() != self.expected_key_type {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "strictly decoded final-domain value type does not match the opened producer contract",
            ));
        }
        let domain = decode_canonical_membership_body(
            payload.canonical_bytes(),
            &self.expected_key_type,
            self.max_domain_canonical_bytes,
        )
        .map_err(|error| {
            execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                error.to_string(),
            )
        })?;
        self.committer.seal(domain).map_err(execution_violation)
    }

    fn close(&mut self) -> Result<(), execution::RuntimeFilterContractViolation> {
        self.committer.close().map_err(execution_violation)
    }
}

struct NativeExecutionProducerAdapter {
    handle: ProducerHandle,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    inbound_contract: super::registry::InboundProducerContract,
    max_contribution_bytes: usize,
}

impl execution::RuntimeFilterProducer for NativeExecutionProducerAdapter {
    fn max_contribution_bytes(&self) -> usize {
        self.max_contribution_bytes
    }

    fn submit(
        &self,
        partition: execution::PartitionId,
        sequence: execution::ProducerSequence,
        contribution: execution::RuntimeFilterContribution,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        let partition =
            novarocks::runtime_filter_transition::port::identity::PartitionId::new(partition.get());
        let sequence = novarocks::runtime_filter_transition::port::identity::ProducerSequence::new(
            sequence.get(),
        );
        if contribution.contract_digest() != self.inbound_contract.schema_digest() {
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::ContractMismatch,
                "contribution contract digest does not match the installed producer route",
            ));
        }
        let stream = novarocks::runtime_filter_transition::port::identity::ProducerStreamId::new(
            self.binding_id,
            self.fragment_instance_id,
            partition,
        );
        let decoded = decode_contribution(
            contribution.canonical_bytes(),
            &contribution.contract_digest(),
            self.inbound_contract.codec_expectation(stream, sequence),
            self.inbound_contract.limits().max_encoded_bytes(),
        )
        .map_err(codec_violation)?;
        match (&self.handle, decoded) {
            (ProducerHandle::Membership(adapter), CoreContribution::Membership(delta)) => adapter
                .submit(partition, sequence, delta)
                .map(execution_submit_outcome)
                .map_err(execution_violation),
            (ProducerHandle::OrderedBound(adapter), CoreContribution::OrderedBound(update)) => {
                adapter
                    .submit_bound(partition, sequence, update)
                    .map(execution_submit_outcome)
                    .map_err(execution_violation)
            }
            (ProducerHandle::TopKSummary(adapter), CoreContribution::TopKSummary(summary)) => {
                adapter
                    .submit_summary(partition, sequence, summary)
                    .map(execution_submit_outcome)
                    .map_err(execution_violation)
            }
            (ProducerHandle::FinalDomain(adapter), CoreContribution::FinalDomain(shard)) => adapter
                .complete(partition, sequence, shard)
                .map(execution_submit_outcome)
                .map_err(execution_violation),
            _ => Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::RoleMismatch,
                "contribution kind does not match the opened producer port",
            )),
        }
    }

    fn close_partition(
        &self,
        partition: execution::PartitionId,
        terminal: execution::ProducerSequence,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        let partition =
            novarocks::runtime_filter_transition::port::identity::PartitionId::new(partition.get());
        let terminal = novarocks::runtime_filter_transition::port::identity::ProducerSequence::new(
            terminal.get(),
        );
        match &self.handle {
            ProducerHandle::Membership(adapter) => adapter.close_partition(partition, terminal),
            ProducerHandle::OrderedBound(adapter) => adapter.close_partition(partition, terminal),
            ProducerHandle::TopKSummary(adapter) => adapter.close_partition(partition, terminal),
            ProducerHandle::FinalDomain(adapter) => adapter.close_partition(partition, terminal),
        }
        .map(execution_submit_outcome)
        .map_err(execution_violation)
    }

    fn fail(
        &self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
    {
        let reason = core_failure_reason(reason);
        match &self.handle {
            ProducerHandle::Membership(adapter) => adapter.fail(reason),
            ProducerHandle::OrderedBound(adapter) => adapter.fail(reason),
            ProducerHandle::TopKSummary(adapter) => adapter.fail(reason),
            ProducerHandle::FinalDomain(adapter) => adapter.fail(reason),
        }
        .map(execution_submit_outcome)
        .map_err(execution_violation)
    }
}

fn core_failure_reason(reason: execution::RuntimeFilterProducerFailure) -> ProducerFailureReason {
    match reason {
        execution::RuntimeFilterProducerFailure::Cancelled => ProducerFailureReason::Cancelled,
        execution::RuntimeFilterProducerFailure::ExecutionFailed => {
            ProducerFailureReason::ExecutionFailed
        }
        execution::RuntimeFilterProducerFailure::UpstreamUnavailable => {
            ProducerFailureReason::UpstreamUnavailable
        }
    }
}

fn execution_producer_port_kind(kind: execution::RuntimeFilterProducerKind) -> ProducerPortKind {
    match kind {
        execution::RuntimeFilterProducerKind::Membership => ProducerPortKind::Membership,
        execution::RuntimeFilterProducerKind::OrderedBound => ProducerPortKind::OrderedBound,
        execution::RuntimeFilterProducerKind::TopKSummary => ProducerPortKind::TopKSummary,
        execution::RuntimeFilterProducerKind::FinalDomain => ProducerPortKind::FinalDomain,
    }
}

fn to_execution_contract(
    contract: &InstalledRuntimeFilterExecutionContract,
) -> execution::RuntimeFilterExecutionContract {
    match contract {
        InstalledRuntimeFilterExecutionContract::Membership {
            canonical_schema,
            schema_digest,
        } => execution::RuntimeFilterExecutionContract::Membership {
            canonical_schema: Arc::clone(canonical_schema),
            schema_digest: *schema_digest,
        },
        InstalledRuntimeFilterExecutionContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } => execution::RuntimeFilterExecutionContract::Ordered {
            keys: keys
                .iter()
                .map(|key| {
                    execution::RuntimeOrderKey::new(
                        key.data_type().clone(),
                        match key.direction() {
                            novarocks::runtime_filter_transition::model::contract::SortDirection::Ascending => {
                                execution::RuntimeOrderSortDirection::Ascending
                            }
                            novarocks::runtime_filter_transition::model::contract::SortDirection::Descending => {
                                execution::RuntimeOrderSortDirection::Descending
                            }
                        },
                        match key.null_order() {
                            novarocks::runtime_filter_transition::model::contract::NullOrder::First => {
                                execution::RuntimeOrderNullOrder::First
                            }
                            novarocks::runtime_filter_transition::model::contract::NullOrder::Last => {
                                execution::RuntimeOrderNullOrder::Last
                            }
                        },
                    )
                })
                .collect::<Vec<_>>()
                .into(),
            comparator_digest: *comparator_digest,
            order_contract_digest: *order_contract_digest,
        },
    }
}

fn execution_violation(
    error: RuntimeContractViolation,
) -> execution::RuntimeFilterContractViolation {
    let kind = match error.kind() {
        RuntimeContractViolationKind::ServiceUnavailable => {
            execution::RuntimeFilterContractViolationKind::SessionClosed
        }
        RuntimeContractViolationKind::UnauthorizedBinding
        | RuntimeContractViolationKind::UnauthorizedFragmentInstance => {
            execution::RuntimeFilterContractViolationKind::UnauthorizedBinding
        }
        RuntimeContractViolationKind::ProducerPortMismatch
        | RuntimeContractViolationKind::ConsumerPortMismatch
        | RuntimeContractViolationKind::SubscriptionActivationMismatch => {
            execution::RuntimeFilterContractViolationKind::RoleMismatch
        }
        RuntimeContractViolationKind::InvalidPartitionCount => {
            execution::RuntimeFilterContractViolationKind::InvalidPartitionCount
        }
        _ => execution::RuntimeFilterContractViolationKind::ContractMismatch,
    };
    execution::RuntimeFilterContractViolation::new(kind, error.to_string())
}

fn codec_violation(error: ContributionCodecError) -> execution::RuntimeFilterContractViolation {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::ContractMismatch,
        error.to_string(),
    )
}

fn execution_submit_outcome(
    outcome: novarocks::runtime_filter_transition::port::producer::SubmitOutcome,
) -> execution::RuntimeFilterSubmitOutcome {
    match outcome {
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::Applied => {
            execution::RuntimeFilterSubmitOutcome::Applied
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::Duplicate => {
            execution::RuntimeFilterSubmitOutcome::Duplicate
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::Stale => {
            execution::RuntimeFilterSubmitOutcome::Stale
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::SequenceAdvancedEqual => {
            execution::RuntimeFilterSubmitOutcome::SequenceAdvancedEqual
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::StreamAcceptedNoGlobalChange => {
            execution::RuntimeFilterSubmitOutcome::StreamAcceptedNoGlobalChange
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::Published => {
            execution::RuntimeFilterSubmitOutcome::Published
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::PendingGap => {
            execution::RuntimeFilterSubmitOutcome::PendingGap
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::PendingFinalSnapshot => {
            execution::RuntimeFilterSubmitOutcome::PendingFinalSnapshot
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::CoverageStillPossible => {
            execution::RuntimeFilterSubmitOutcome::CoverageStillPossible
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::TerminalNoop => {
            execution::RuntimeFilterSubmitOutcome::TerminalNoop
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::Completed => {
            execution::RuntimeFilterSubmitOutcome::Completed
        }
        novarocks::runtime_filter_transition::port::producer::SubmitOutcome::CompletedWithoutArtifact => {
            execution::RuntimeFilterSubmitOutcome::CompletedWithoutArtifact
        }
    }
}

pub(crate) struct ResolvedNativeConsumer {
    service: Arc<RuntimeFilterService>,
    binding_id: BindingId,
    channel_id: ChannelId,
    fragment_instance_id: UniqueId,
    subscription_kind: SubscriptionKind,
    activation: novarocks::runtime_filter_transition::model::contract::ConsumerActivation,
    capabilities: BTreeSet<ArtifactCapability>,
    artifact_profile: ConsumerArtifactProfile,
    contract: InstalledRuntimeFilterExecutionContract,
    lifecycle: RuntimeFilterLifecycle,
    reduction_requirement: ReductionRequirement,
    topk_contract_digest: Option<[u8; 32]>,
    snapshot_compiler: SnapshotPredicateCompiler,
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
    ) -> novarocks::runtime_filter_transition::model::contract::ConsumerActivation {
        self.activation
    }

    pub(crate) const fn capabilities(&self) -> &BTreeSet<ArtifactCapability> {
        &self.capabilities
    }

    pub(crate) const fn artifact_profile(&self) -> &ConsumerArtifactProfile {
        &self.artifact_profile
    }

    pub(crate) const fn contract(&self) -> &InstalledRuntimeFilterExecutionContract {
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

    pub(crate) fn subscribe_live(
        &self,
    ) -> Result<Arc<dyn NonBlockingLiveSubscription>, RuntimeContractViolation> {
        if self.subscription_kind != SubscriptionKind::NonBlockingLive {
            return Err(resolution_violation(
                RuntimeContractViolationKind::SubscriptionActivationMismatch,
                "resolved consumer is not a non-blocking live consumer",
            ));
        }
        self.subscribe()?.into_live()
    }
}

#[derive(Clone)]
enum SnapshotPredicateCompiler {
    Membership {
        data_type: DataType,
        null_semantics: novarocks::runtime_filter_transition::model::contract::NullSemantics,
    },
    Ordered {
        order_contract: Arc<RuntimeOrderContract>,
    },
}

/// Backend-owned queries over one retained, already-validated artifact. This
/// adapter intentionally exposes neither Arrow memory nor connector facts;
/// Execution owns row/scan evaluation and every resulting outcome.
enum NativeRuntimeFilterArtifactQuery {
    Membership {
        artifact: Arc<PhysicalArtifact>,
        data_type: DataType,
        null_semantics: NullSemantics,
    },
    Ordered {
        artifact: Arc<PhysicalArtifact>,
        data_type: DataType,
    },
}

impl NativeRuntimeFilterArtifactQuery {
    fn membership(
        bundle: &novarocks::runtime_filter_transition::port::artifact::ArtifactBundle,
        data_type: DataType,
        null_semantics: NullSemantics,
    ) -> Result<Self, execution::UnavailableReason> {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .map_err(|_| execution::UnavailableReason::MaterializationFailed)?;
        if bundle.profile_id() != profile.id() {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        let [(kind, artifact)] = bundle.artifacts() else {
            return Err(execution::UnavailableReason::MaterializationFailed);
        };
        if !matches!(kind, ArtifactKind::ValueSet | ArtifactKind::EmptyDomain)
            || artifact.kind() != *kind
            || artifact.version() != bundle.version()
            || artifact.codec_version() != LEAF_CODEC_VERSION
            || artifact.membership_index().is_none()
        {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        let schema = ArtifactMembershipSchema::new(&data_type, null_semantics)
            .map_err(|_| execution::UnavailableReason::MaterializationFailed)?;
        if artifact.schema_digest() != schema.digest()
            || (artifact.contains_null() && null_semantics != NullSemantics::NullSafeEqual)
            || (matches!(kind, ArtifactKind::EmptyDomain)
                != matches!(
                    artifact
                        .membership_index()
                        .expect("checked membership index")
                        .view(),
                    ResidentMembershipIndexView::EmptyDomain
                ))
        {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        Ok(Self::Membership {
            artifact: Arc::clone(artifact),
            data_type,
            null_semantics,
        })
    }

    fn ordered(
        bundle: &novarocks::runtime_filter_transition::port::artifact::ArtifactBundle,
        order_contract: Arc<RuntimeOrderContract>,
    ) -> Result<Self, execution::UnavailableReason> {
        if order_contract.keys().len() != 1 {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        let profile = ConsumerArtifactProfile::new_ordered_range(order_contract.digest())
            .map_err(|_| execution::UnavailableReason::MaterializationFailed)?;
        if bundle.profile_id() != profile.id() {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        let [(kind, artifact)] = bundle.artifacts() else {
            return Err(execution::UnavailableReason::MaterializationFailed);
        };
        if *kind != ArtifactKind::Range
            || artifact.kind() != ArtifactKind::Range
            || artifact.version() != bundle.version()
            || artifact.codec_version() != LEAF_CODEC_VERSION
            || artifact.schema_digest()
                != ArtifactSchemaDigest::from_canonical_bytes(order_contract.digest().bytes())
        {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        let range = artifact
            .range()
            .ok_or(execution::UnavailableReason::MaterializationFailed)?;
        if range.contract().as_ref() != order_contract.as_ref()
            || range.contract().digest() != order_contract.digest()
            || range
                .contract()
                .compare(range.bound(), range.bound())
                .is_err()
        {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        let canonical = encode_range_leaf(range.contract(), range.bound(), bundle.version())
            .map_err(map_codec_unavailable)?;
        if canonical.as_slice() != artifact.canonical_bytes() {
            return Err(execution::UnavailableReason::MaterializationFailed);
        }
        Ok(Self::Ordered {
            artifact: Arc::clone(artifact),
            data_type: order_contract.keys()[0].data_type().clone(),
        })
    }

    fn ordered_range(
        &self,
    ) -> Result<
        &novarocks::runtime_filter_transition::port::artifact::RangeArtifactData,
        execution::RuntimeFilterArtifactQueryError,
    > {
        match self {
            Self::Ordered { artifact, .. } => artifact
                .range()
                .ok_or(execution::RuntimeFilterArtifactQueryError::ContractViolation),
            Self::Membership { .. } => {
                Err(execution::RuntimeFilterArtifactQueryError::ContractViolation)
            }
        }
    }

    fn ordered_matches_tuple(
        &self,
        value: OrderedTuple,
    ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        let range = self.ordered_range()?;
        Ok(range
            .contract()
            .compare(&value, range.bound())
            .map_err(|_| execution::RuntimeFilterArtifactQueryError::ContractViolation)?
            != std::cmp::Ordering::Greater)
    }
}

impl execution::RuntimeFilterArtifactQuery for NativeRuntimeFilterArtifactQuery {
    fn data_type(&self) -> &DataType {
        match self {
            Self::Membership { data_type, .. } | Self::Ordered { data_type, .. } => data_type,
        }
    }

    fn matches_null(&self) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                null_semantics,
                ..
            } => Ok(*null_semantics == NullSemantics::NullSafeEqual && artifact.contains_null()),
            Self::Ordered { .. } => {
                let range = self.ordered_range()?;
                let value = OrderedTuple::try_new(range.contract(), [None])
                    .map_err(|_| execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                self.ordered_matches_tuple(value)
            }
        }
    }

    fn has_non_null_matches(&self) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership { artifact, .. } => {
                let index = artifact
                    .membership_index()
                    .ok_or(execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                Ok(match index.view() {
                    ResidentMembershipIndexView::EmptyDomain => false,
                    ResidentMembershipIndexView::Fixed { count, .. } => count > 0,
                    ResidentMembershipIndexView::Utf8 { length_offsets, .. } => {
                        !length_offsets.is_empty()
                    }
                })
            }
            Self::Ordered { .. } => Ok(true),
        }
    }

    fn non_null_value_may_match(
        &self,
        value: execution::RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                data_type,
                ..
            } => {
                let index = artifact
                    .membership_index()
                    .ok_or(execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                indexed_membership_contains(
                    artifact.canonical_bytes(),
                    index,
                    membership_probe(value, data_type)?,
                )
                .map_err(map_codec_query)
            }
            Self::Ordered { data_type, .. } => {
                let range = self.ordered_range()?;
                let scalar = ordered_scalar(value, data_type)?;
                let tuple = OrderedTuple::try_new(range.contract(), [Some(scalar)])
                    .map_err(|_| execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                self.ordered_matches_tuple(tuple)
            }
        }
    }

    fn non_null_range_may_match(
        &self,
        inclusive_min: &ConnectorScalarValue,
        inclusive_max: &ConnectorScalarValue,
    ) -> Result<bool, execution::RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                data_type,
                ..
            } => {
                let index = artifact
                    .membership_index()
                    .ok_or(execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                indexed_membership_range_may_match(
                    artifact.canonical_bytes(),
                    index,
                    connector_membership_probe(inclusive_min, data_type)?,
                    connector_membership_probe(inclusive_max, data_type)?,
                )
                .map_err(map_codec_query)
            }
            Self::Ordered { data_type, .. } => {
                let range = self.ordered_range()?;
                let min = OrderedTuple::try_new(
                    range.contract(),
                    [Some(connector_ordered_scalar(inclusive_min, data_type)?)],
                )
                .map_err(|_| execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                let max = OrderedTuple::try_new(
                    range.contract(),
                    [Some(connector_ordered_scalar(inclusive_max, data_type)?)],
                )
                .map_err(|_| execution::RuntimeFilterArtifactQueryError::ContractViolation)?;
                Ok(self.ordered_matches_tuple(min)? || self.ordered_matches_tuple(max)?)
            }
        }
    }
}

fn map_codec_unavailable(error: ArtifactCodecError) -> execution::UnavailableReason {
    match error {
        ArtifactCodecError::ResourceUnavailable | ArtifactCodecError::ResourceLimit => {
            execution::UnavailableReason::MaterializationFailed
        }
        _ => execution::UnavailableReason::MaterializationFailed,
    }
}

fn map_codec_query(error: ArtifactCodecError) -> execution::RuntimeFilterArtifactQueryError {
    match error {
        ArtifactCodecError::ResourceUnavailable | ArtifactCodecError::ResourceLimit => {
            execution::RuntimeFilterArtifactQueryError::ResourceUnavailable
        }
        _ => execution::RuntimeFilterArtifactQueryError::ContractViolation,
    }
}

fn membership_probe<'a>(
    value: execution::RuntimeFilterScalarRef<'a>,
    expected: &DataType,
) -> Result<MembershipProbe<'a>, execution::RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (execution::RuntimeFilterScalarRef::Boolean(value), DataType::Boolean) => {
            Ok(MembershipProbe::Boolean(value))
        }
        (execution::RuntimeFilterScalarRef::Int8(value), DataType::Int8) => {
            Ok(MembershipProbe::Int8(value))
        }
        (execution::RuntimeFilterScalarRef::Int16(value), DataType::Int16) => {
            Ok(MembershipProbe::Int16(value))
        }
        (execution::RuntimeFilterScalarRef::Int32(value), DataType::Int32) => {
            Ok(MembershipProbe::Int32(value))
        }
        (execution::RuntimeFilterScalarRef::Int64(value), DataType::Int64) => {
            Ok(MembershipProbe::Int64(value))
        }
        (execution::RuntimeFilterScalarRef::Utf8(value), DataType::Utf8) => {
            Ok(MembershipProbe::Utf8(value))
        }
        (execution::RuntimeFilterScalarRef::Date32(value), DataType::Date32) => {
            Ok(MembershipProbe::Date32(value))
        }
        (
            execution::RuntimeFilterScalarRef::TimestampMicrosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            execution::RuntimeFilterScalarRef::TimestampNanosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(MembershipProbe::Timestamp(value)),
        _ => Err(execution::RuntimeFilterArtifactQueryError::Unsupported),
    }
}

fn connector_membership_probe<'a>(
    value: &'a ConnectorScalarValue,
    expected: &DataType,
) -> Result<MembershipProbe<'a>, execution::RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (ConnectorScalarValue::Boolean(value), DataType::Boolean) => {
            Ok(MembershipProbe::Boolean(*value))
        }
        (ConnectorScalarValue::Int8(value), DataType::Int8) => Ok(MembershipProbe::Int8(*value)),
        (ConnectorScalarValue::Int16(value), DataType::Int16) => Ok(MembershipProbe::Int16(*value)),
        (ConnectorScalarValue::Int32(value), DataType::Int32) => Ok(MembershipProbe::Int32(*value)),
        (ConnectorScalarValue::Int64(value), DataType::Int64) => Ok(MembershipProbe::Int64(*value)),
        (ConnectorScalarValue::Date32(value), DataType::Date32) => {
            Ok(MembershipProbe::Date32(*value))
        }
        (
            ConnectorScalarValue::TimestampMicros(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            ConnectorScalarValue::TimestampNanos(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(MembershipProbe::Timestamp(*value)),
        (ConnectorScalarValue::Utf8(value), DataType::Utf8) => Ok(MembershipProbe::Utf8(value)),
        _ => Err(execution::RuntimeFilterArtifactQueryError::Unsupported),
    }
}

fn ordered_scalar(
    value: execution::RuntimeFilterScalarRef<'_>,
    expected: &DataType,
) -> Result<OrderedScalar, execution::RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (execution::RuntimeFilterScalarRef::Boolean(value), DataType::Boolean) => {
            Ok(OrderedScalar::Boolean(value))
        }
        (execution::RuntimeFilterScalarRef::Int8(value), DataType::Int8) => {
            Ok(OrderedScalar::Int8(value))
        }
        (execution::RuntimeFilterScalarRef::Int16(value), DataType::Int16) => {
            Ok(OrderedScalar::Int16(value))
        }
        (execution::RuntimeFilterScalarRef::Int32(value), DataType::Int32) => {
            Ok(OrderedScalar::Int32(value))
        }
        (execution::RuntimeFilterScalarRef::Int64(value), DataType::Int64) => {
            Ok(OrderedScalar::Int64(value))
        }
        (execution::RuntimeFilterScalarRef::Utf8(value), DataType::Utf8) => {
            Ok(OrderedScalar::Utf8(Arc::from(value)))
        }
        (execution::RuntimeFilterScalarRef::Date32(value), DataType::Date32) => {
            Ok(OrderedScalar::Date32(value))
        }
        (
            execution::RuntimeFilterScalarRef::TimestampMicrosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            execution::RuntimeFilterScalarRef::TimestampNanosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(OrderedScalar::Timestamp(value)),
        _ => Err(execution::RuntimeFilterArtifactQueryError::Unsupported),
    }
}

fn connector_ordered_scalar(
    value: &ConnectorScalarValue,
    expected: &DataType,
) -> Result<OrderedScalar, execution::RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (ConnectorScalarValue::Boolean(value), DataType::Boolean) => {
            Ok(OrderedScalar::Boolean(*value))
        }
        (ConnectorScalarValue::Int8(value), DataType::Int8) => Ok(OrderedScalar::Int8(*value)),
        (ConnectorScalarValue::Int16(value), DataType::Int16) => Ok(OrderedScalar::Int16(*value)),
        (ConnectorScalarValue::Int32(value), DataType::Int32) => Ok(OrderedScalar::Int32(*value)),
        (ConnectorScalarValue::Int64(value), DataType::Int64) => Ok(OrderedScalar::Int64(*value)),
        (ConnectorScalarValue::Date32(value), DataType::Date32) => {
            Ok(OrderedScalar::Date32(*value))
        }
        (
            ConnectorScalarValue::TimestampMicros(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            ConnectorScalarValue::TimestampNanos(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(OrderedScalar::Timestamp(*value)),
        (ConnectorScalarValue::Utf8(value), DataType::Utf8) => {
            Ok(OrderedScalar::Utf8(Arc::from(value.as_str())))
        }
        _ => Err(execution::RuntimeFilterArtifactQueryError::Unsupported),
    }
}

impl SnapshotPredicateCompiler {
    fn compile(
        &self,
        bundle: &novarocks::runtime_filter_transition::port::artifact::ArtifactBundle,
        binding_id: execution::RuntimeFilterBindingId,
        contract_digest: [u8; 32],
    ) -> Result<Arc<execution::RuntimeFilterSnapshot>, execution::UnavailableReason> {
        // Design: ADR-0043 (docs/adr/ADR-0043-runtime-filter-artifact-query-and-evaluator-boundary.md)
        // Backend owns this immutable artifact adapter. It exposes only
        // retained-artifact primitives; Execution owns Arrow/scan evaluation.
        let artifact_query: Arc<dyn execution::RuntimeFilterArtifactQuery> = match self {
            Self::Membership {
                data_type,
                null_semantics,
            } => Arc::new(NativeRuntimeFilterArtifactQuery::membership(
                bundle,
                data_type.clone(),
                *null_semantics,
            )?),
            Self::Ordered { order_contract } => Arc::new(
                NativeRuntimeFilterArtifactQuery::ordered(bundle, Arc::clone(order_contract))?,
            ),
        };
        Ok(Arc::new(execution::RuntimeFilterSnapshot::new(
            binding_id,
            execution::LogicalVersion::new(bundle.version().get()),
            contract_digest,
            artifact_query,
        )))
    }
}

struct NativeExecutionBlockingSubscription {
    binding_id: execution::RuntimeFilterBindingId,
    contract_digest: [u8; 32],
    compiler: SnapshotPredicateCompiler,
    subscription: Arc<dyn BlockingSnapshotSubscription>,
}

impl execution::BlockingSnapshotSubscription for NativeExecutionBlockingSubscription {
    fn acquire(&self, timeout: Duration) -> execution::SnapshotAcquireOutcome {
        map_acquire_outcome(
            self.subscription.acquire(timeout),
            self.binding_id,
            self.contract_digest,
            &self.compiler,
        )
    }

    fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
        self.subscription.snapshot().and_then(|bundle| {
            self.compiler
                .compile(&bundle, self.binding_id, self.contract_digest)
                .ok()
        })
    }
}

struct NativeExecutionLiveSubscription {
    binding_id: execution::RuntimeFilterBindingId,
    contract_digest: [u8; 32],
    compiler: SnapshotPredicateCompiler,
    subscription: Arc<dyn NonBlockingLiveSubscription>,
}

impl execution::NonBlockingLiveSubscription for NativeExecutionLiveSubscription {
    fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
        self.subscription.snapshot().and_then(|bundle| {
            self.compiler
                .compile(&bundle, self.binding_id, self.contract_digest)
                .ok()
        })
    }

    fn poll_after(
        &self,
        observed: Option<execution::LogicalVersion>,
    ) -> execution::LivePollOutcome {
        let observed = observed.map(|version| LogicalVersion::new(version.get()));
        match self.subscription.poll_after(observed) {
            LivePollOutcome::Updated { bundle, terminal } => {
                match self
                    .compiler
                    .compile(&bundle, self.binding_id, self.contract_digest)
                {
                    Ok(snapshot) => execution::LivePollOutcome::Updated {
                        snapshot,
                        terminal: terminal.map(map_live_terminal),
                    },
                    Err(reason) => execution::LivePollOutcome::Idle {
                        latest_version: Some(execution::LogicalVersion::new(
                            bundle.version().get(),
                        )),
                        terminal: Some(execution::LiveTerminal::Unavailable(reason)),
                    },
                }
            }
            LivePollOutcome::Idle {
                latest_version,
                terminal,
            } => execution::LivePollOutcome::Idle {
                latest_version: latest_version
                    .map(|version| execution::LogicalVersion::new(version.get())),
                terminal: terminal.map(map_live_terminal),
            },
        }
    }
}

fn map_acquire_outcome(
    outcome: ArtifactAcquireOutcome,
    binding_id: execution::RuntimeFilterBindingId,
    contract_digest: [u8; 32],
    compiler: &SnapshotPredicateCompiler,
) -> execution::SnapshotAcquireOutcome {
    match outcome {
        ArtifactAcquireOutcome::Published(bundle) => {
            match compiler.compile(&bundle, binding_id, contract_digest) {
                Ok(snapshot) => execution::SnapshotAcquireOutcome::Published(snapshot),
                Err(reason) => execution::SnapshotAcquireOutcome::Unavailable(reason),
            }
        }
        ArtifactAcquireOutcome::Unsupported(reason) => {
            execution::SnapshotAcquireOutcome::Unsupported(map_unsupported_reason(reason))
        }
        ArtifactAcquireOutcome::Unavailable(reason) => {
            execution::SnapshotAcquireOutcome::Unavailable(map_unavailable_reason(reason))
        }
        ArtifactAcquireOutcome::Cancelled => execution::SnapshotAcquireOutcome::Cancelled,
        ArtifactAcquireOutcome::TimedOut => execution::SnapshotAcquireOutcome::TimedOut,
    }
}

fn map_unavailable_reason(reason: UnavailableReason) -> execution::UnavailableReason {
    match reason {
        UnavailableReason::ResourceLimit => execution::UnavailableReason::ResourceLimit,
        UnavailableReason::IncompleteCoverage => execution::UnavailableReason::IncompleteCoverage,
        UnavailableReason::ProducerFailed => execution::UnavailableReason::ProducerFailed,
        UnavailableReason::MaterializationFailed => {
            execution::UnavailableReason::MaterializationFailed
        }
        UnavailableReason::RouteUnavailable => execution::UnavailableReason::RouteUnavailable,
    }
}

fn map_unsupported_reason(
    reason: ArtifactUnsupportedReason,
) -> execution::ArtifactUnsupportedReason {
    match reason {
        ArtifactUnsupportedReason::RangeDeferred => {
            execution::ArtifactUnsupportedReason::RangeDeferred
        }
        ArtifactUnsupportedReason::NoAcceptedRepresentation => {
            execution::ArtifactUnsupportedReason::NoAcceptedRepresentation
        }
    }
}

fn map_live_terminal(terminal: LiveTerminal) -> execution::LiveTerminal {
    match terminal {
        LiveTerminal::Completed => execution::LiveTerminal::Completed,
        LiveTerminal::CompletedWithoutArtifact => execution::LiveTerminal::CompletedWithoutArtifact,
        LiveTerminal::Cancelled => execution::LiveTerminal::Cancelled,
        LiveTerminal::DegradedLogical(reason)
        | LiveTerminal::DegradedArtifact(reason)
        | LiveTerminal::DegradedDelivery(reason)
        | LiveTerminal::Unavailable(reason) => {
            execution::LiveTerminal::Unavailable(map_unavailable_reason(reason))
        }
    }
}

fn snapshot_predicate_compiler(
    logical_domain: &RuntimeFilterLogicalDomain,
) -> Result<SnapshotPredicateCompiler, RuntimeContractViolation> {
    match logical_domain {
        RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } => Ok(SnapshotPredicateCompiler::Membership {
            data_type: value_type.clone(),
            null_semantics: *null_semantics,
        }),
        RuntimeFilterLogicalDomain::OrderedBound(contract) => {
            let order_contract = RuntimeOrderContract::try_from_plan(contract).map_err(|_| {
                resolution_violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "installed ordered contract is invalid",
                )
            })?;
            Ok(SnapshotPredicateCompiler::Ordered {
                order_contract: Arc::new(order_contract),
            })
        }
    }
}

fn execution_contract_digest(contract: &execution::RuntimeFilterExecutionContract) -> [u8; 32] {
    match contract {
        execution::RuntimeFilterExecutionContract::Membership { schema_digest, .. } => {
            *schema_digest
        }
        execution::RuntimeFilterExecutionContract::Ordered {
            order_contract_digest,
            ..
        } => *order_contract_digest,
    }
}

fn installed_contract(
    logical_domain: &RuntimeFilterLogicalDomain,
) -> Result<InstalledRuntimeFilterExecutionContract, RuntimeContractViolation> {
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
            Ok(InstalledRuntimeFilterExecutionContract::Membership {
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
            Ok(InstalledRuntimeFilterExecutionContract::Ordered {
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

#[cfg(test)]
mod artifact_query_tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterScalarRef,
    };
    use novarocks_spi::connector::ConnectorScalarValue;

    use super::{NativeRuntimeFilterArtifactQuery, map_codec_query};
    use novarocks::runtime_filter_transition::materializer::codec::{
        ArtifactCodecError, ArtifactDecodeExpectations, decode_leaf, encode_membership_leaf,
    };
    use novarocks::runtime_filter_transition::model::contract::{ChannelId, NullSemantics};
    use novarocks::runtime_filter_transition::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    };
    use novarocks::runtime_filter_transition::port::identity::LogicalVersion;
    use novarocks::runtime_filter_transition::port::support::{
        ArtifactRetainedBudget, MemoryAccountError, RuntimeFilterMemoryAccount,
    };
    use novarocks::runtime_filter_transition::port::value_domain::{
        MembershipValues, ReducedMembershipDomain,
    };

    struct UnlimitedMemory;

    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _: usize) {}
    }

    fn membership_query(
        values: impl IntoIterator<Item = i64>,
        contains_null: bool,
        null_semantics: NullSemantics,
    ) -> NativeRuntimeFilterArtifactQuery {
        let version = LogicalVersion::FIRST;
        let domain = ReducedMembershipDomain::new(MembershipValues::int64(values), contains_null);
        let encoded = encode_membership_leaf(&domain, null_semantics, version).unwrap();
        let kind = ArtifactKind::from_tag(encoded[6]).unwrap();
        let schema = ArtifactMembershipSchema::new(&DataType::Int64, null_semantics).unwrap();
        let artifact = decode_leaf(
            &encoded,
            ArtifactDecodeExpectations {
                expected_kind: kind,
                expected_schema_digest: schema.digest(),
                expected_logical_version: version,
                expected_hash_contract: None,
            },
            encoded.len(),
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(UnlimitedMemory),
        )
        .unwrap();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let bundle = ArtifactBundle::new(
            ChannelId::new(7),
            version,
            &profile,
            vec![(kind, artifact)],
            usize::MAX,
        )
        .unwrap();
        NativeRuntimeFilterArtifactQuery::membership(&bundle, DataType::Int64, null_semantics)
            .unwrap()
    }

    #[test]
    fn membership_adapter_uses_indexed_closed_range_and_null_primitives() {
        let query = membership_query([1, 5, 9], true, NullSemantics::NullSafeEqual);

        assert!(query.matches_null().unwrap());
        assert!(query.has_non_null_matches().unwrap());
        assert!(
            query
                .non_null_value_may_match(RuntimeFilterScalarRef::Int64(5))
                .unwrap()
        );
        assert!(
            !query
                .non_null_value_may_match(RuntimeFilterScalarRef::Int64(4))
                .unwrap()
        );
        assert!(
            query
                .non_null_range_may_match(
                    &ConnectorScalarValue::Int64(4),
                    &ConnectorScalarValue::Int64(6),
                )
                .unwrap()
        );
        assert!(
            !query
                .non_null_range_may_match(
                    &ConnectorScalarValue::Int64(6),
                    &ConnectorScalarValue::Int64(8),
                )
                .unwrap()
        );
    }

    #[test]
    fn adapter_rejects_unsupported_types_and_preserves_query_error_classes() {
        let query = membership_query([1], false, NullSemantics::NeverMatches);

        assert_eq!(
            query.non_null_value_may_match(RuntimeFilterScalarRef::Float64(1.0)),
            Err(RuntimeFilterArtifactQueryError::Unsupported)
        );
        assert_eq!(
            query.non_null_range_may_match(
                &ConnectorScalarValue::TimestampMicros(1),
                &ConnectorScalarValue::TimestampMicros(2),
            ),
            Err(RuntimeFilterArtifactQueryError::Unsupported)
        );
        assert_eq!(
            map_codec_query(ArtifactCodecError::ResourceLimit),
            RuntimeFilterArtifactQueryError::ResourceUnavailable
        );
        assert_eq!(
            map_codec_query(ArtifactCodecError::NonCanonicalPayload),
            RuntimeFilterArtifactQueryError::ContractViolation
        );
    }
}
