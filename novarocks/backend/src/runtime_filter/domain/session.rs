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

//! Backend-private runtime-filter session for one installed channel.
//!
//! Execution owns the producer/consumer contracts and contribution format.
//! Backend owns the installed binding/instance/route authority, reduction
//! lifetime, coverage fences, materialization, and subscription fan-out.  It
//! never evaluates Arrow rows or scan facts.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_execution::runtime_filter::{
    PartitionId, ProducerSequence, RuntimeFilterBindOutcome, RuntimeFilterBindingId,
    RuntimeFilterConsumerContract, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind, RuntimeFilterContribution, RuntimeFilterExecutionContract,
    RuntimeFilterProducer, RuntimeFilterProducerFailure, RuntimeFilterProducerHandle,
    RuntimeFilterProducerOpenRequest, RuntimeFilterSnapshot, RuntimeFilterSubmitOutcome,
    RuntimeFilterSubscriptionHandle, RuntimeFilterSubscriptionRequest, SnapshotAcquireOutcome,
    UnavailableReason,
};
use novarocks_types::UniqueId;

use super::{
    BackendChannelIdentity, BackendChannelInstall, BackendConsumerInstall, BackendCoverageProgress,
    BackendCoverageState, BackendInstallPolicy, BackendInstallPolicyError,
    BackendParticipantIdentity, BackendProducerInstall, BackendProducerStreamIdentity,
    BackendReducedLogicalDomain, BackendReducedLogicalSnapshot, BackendReductionApply,
    BackendReductionState, BackendReductionStateError, BackendRouteEdgeId,
    BackendRuntimeFilterEventObserver, BackendSubscriptionError, BackendSubscriptionGroup,
};

#[derive(Debug)]
pub(crate) enum BackendRuntimeFilterSessionError {
    MissingProducer,
    InstallPolicy(BackendInstallPolicyError),
    Reduction(BackendReductionStateError),
    ProducerShapeMismatch,
    ConsumerContractMismatch,
}

impl fmt::Display for BackendRuntimeFilterSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter channel session: {self:?}"
        )
    }
}

impl std::error::Error for BackendRuntimeFilterSessionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendRuntimeFilterSessionSubmission {
    outcome: RuntimeFilterSubmitOutcome,
    publication: Option<BackendReducedLogicalSnapshot>,
}

impl BackendRuntimeFilterSessionSubmission {
    pub(crate) const fn outcome(&self) -> RuntimeFilterSubmitOutcome {
        self.outcome
    }

    pub(crate) const fn publication(&self) -> Option<&BackendReducedLogicalSnapshot> {
        self.publication.as_ref()
    }
}

#[derive(Debug)]
struct BackendOpenedProducer {
    partition_count: u32,
    terminal_partitions: BTreeMap<PartitionId, ProducerSequence>,
}

impl BackendOpenedProducer {
    fn new(partition_count: u32) -> Self {
        Self {
            partition_count,
            terminal_partitions: BTreeMap::new(),
        }
    }

    fn contains_partition(&self, partition: PartitionId) -> bool {
        partition.get() < self.partition_count
    }

    fn is_closed(&self) -> bool {
        self.terminal_partitions.len() == self.partition_count as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendProducerBindingTerminal {
    Closed,
    Failed,
}

#[derive(Default)]
struct BackendProducerBindingProgress {
    instances: BTreeMap<UniqueId, BackendOpenedProducer>,
    terminal: Option<BackendProducerBindingTerminal>,
}

struct BackendInstalledConsumer {
    contract: RuntimeFilterConsumerContract,
    profile: crate::runtime_filter::artifact::ConsumerArtifactProfile,
    routes: BTreeSet<BackendRouteEdgeId>,
    subscriptions: BackendSubscriptionGroup,
}

/// A channel-wide Backend session.  A single strict reduction state is shared
/// by every installed producer binding only after construction verifies they
/// have identical Execution semantics and contribution budget.
pub(crate) struct BackendRuntimeFilterSession {
    policy: BackendInstallPolicy,
    participant: BackendParticipantIdentity,
    channel: BackendChannelInstall,
    producers: BTreeMap<RuntimeFilterBindingId, BackendProducerInstall>,
    producer_progress: Mutex<BTreeMap<RuntimeFilterBindingId, BackendProducerBindingProgress>>,
    consumers: BTreeMap<RuntimeFilterBindingId, BackendInstalledConsumer>,
    reduction: Mutex<BackendReductionState>,
    availability: Mutex<BackendCoverageState>,
    terminal: Mutex<BackendCoverageState>,
}

impl BackendRuntimeFilterSession {
    // Design: ADR-0044 (docs/adr/ADR-0044-backend-runtime-filter-participant-domain.md)
    /// Builds a session from one sealed channel installation. The caller
    /// supplies only the Backend event observer; no Core transition adapter is
    /// involved in construction.
    pub(crate) fn from_channel_install(
        participant: BackendParticipantIdentity,
        channel: BackendChannelInstall,
        events: Arc<dyn BackendRuntimeFilterEventObserver>,
    ) -> Result<Self, BackendRuntimeFilterSessionError> {
        let first = channel
            .producers()
            .values()
            .next()
            .ok_or(BackendRuntimeFilterSessionError::MissingProducer)?;
        validate_producer_shape(&channel, first, first)?;
        for producer in channel.producers().values() {
            validate_producer_shape(&channel, first, producer)?;
        }
        let policy = BackendInstallPolicy::new(
            participant,
            first.contract().clone(),
            channel.availability_coverage().clone(),
            first.max_contribution_bytes(),
        )
        .map_err(BackendRuntimeFilterSessionError::InstallPolicy)?;
        let reduction = BackendReductionState::new(policy.clone())
            .map_err(BackendRuntimeFilterSessionError::Reduction)?;
        let availability = BackendCoverageState::new(channel.availability_coverage())
            .expect("BackendChannelInstall has validated coverage");
        let terminal = BackendCoverageState::new(channel.terminal_coverage())
            .expect("BackendChannelInstall has validated coverage");

        let mut consumers = BTreeMap::new();
        for (binding_id, consumer) in channel.consumers() {
            validate_consumer_shape(&channel, first, consumer)?;
            let session_channel =
                BackendChannelIdentity::new(participant, *binding_id, channel.channel_id());
            consumers.insert(
                *binding_id,
                BackendInstalledConsumer {
                    contract: consumer.contract().clone(),
                    profile: consumer.profile().clone(),
                    routes: consumer.route_edge_ids().clone(),
                    subscriptions: BackendSubscriptionGroup::new(
                        session_channel,
                        *binding_id,
                        consumer.contract().activation(),
                        consumer.route_edge_ids().iter().copied(),
                        consumer.expected_fragment_instances().iter().copied(),
                        Arc::clone(&events),
                    ),
                },
            );
        }

        Ok(Self {
            policy,
            participant,
            producers: channel.producers().clone(),
            channel,
            producer_progress: Mutex::new(BTreeMap::new()),
            consumers,
            reduction: Mutex::new(reduction),
            availability: Mutex::new(availability),
            terminal: Mutex::new(terminal),
        })
    }

    pub(crate) const fn policy(&self) -> &BackendInstallPolicy {
        &self.policy
    }

    pub(crate) const fn channel(&self) -> &BackendChannelInstall {
        &self.channel
    }

    pub(crate) fn availability_progress(&self) -> BackendCoverageProgress {
        self.availability
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .progress(self.channel.availability_coverage())
    }

    pub(crate) fn terminal_progress(&self) -> BackendCoverageProgress {
        self.terminal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .progress(self.channel.terminal_coverage())
    }

    /// Opens one authorized producer binding/instance. Binding, witness, and
    /// expected instance authority all come from the sealed channel install.
    pub(crate) fn open_producer(
        self: &Arc<Self>,
        fragment_instance_id: UniqueId,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<RuntimeFilterBindOutcome<RuntimeFilterProducerHandle>, RuntimeFilterContractViolation>
    {
        let binding_id = request.contract().binding_id();
        self.register_producer(binding_id, fragment_instance_id, request)?;
        Ok(RuntimeFilterBindOutcome::Bound(Arc::new(
            BackendRuntimeFilterProducer {
                session: Arc::clone(self),
                binding_id,
                fragment_instance_id,
            },
        )))
    }

    /// Resolves an installed consumer by its exact Execution binding contract
    /// and fragment instance. A missing local slot remains an unavailable
    /// route, rather than a permissive synthetic subscription.
    pub(crate) fn subscribe(
        &self,
        fragment_instance_id: UniqueId,
        request: RuntimeFilterSubscriptionRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterSubscriptionHandle>,
        RuntimeFilterContractViolation,
    > {
        let binding_id = request.contract().binding_id();
        let consumer = self.consumers.get(&binding_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer binding is not installed in this Backend channel session",
            )
        })?;
        if request.contract() != &consumer.contract {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "consumer execution contract does not match the installed Backend binding",
            ));
        }
        Ok(consumer
            .subscriptions
            .handle(fragment_instance_id)
            .map(RuntimeFilterBindOutcome::Bound)
            .unwrap_or(RuntimeFilterBindOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            )))
    }

    /// Strictly reduces one canonical Execution contribution. A participant
    /// artifact owner may consume `publication`; this session never turns it
    /// into an artifact bundle itself.
    pub(crate) fn submit(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<BackendRuntimeFilterSessionSubmission, RuntimeFilterContractViolation> {
        let stream = self.open_stream(binding_id, fragment_instance_id, partition)?;
        let (apply, publication) = self
            .reduction
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .submit(stream, sequence, contribution)
            .map_err(reduction_violation)?;
        if let Some(snapshot) = publication.as_ref() {
            self.publish_reduced_snapshot(snapshot)?;
        }
        Ok(BackendRuntimeFilterSessionSubmission {
            outcome: match apply {
                BackendReductionApply::Applied { .. } => RuntimeFilterSubmitOutcome::Published,
                BackendReductionApply::Duplicate => RuntimeFilterSubmitOutcome::Duplicate,
                BackendReductionApply::Stale => RuntimeFilterSubmitOutcome::Stale,
                BackendReductionApply::SequenceAdvancedEqual => {
                    RuntimeFilterSubmitOutcome::SequenceAdvancedEqual
                }
            },
            publication,
        })
    }

    /// Closes one producer partition. Its witness is marked satisfied only
    /// when every expected fragment instance for that same producer binding
    /// has opened and closed every declared local partition.
    pub(crate) fn close_partition(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let install = self.producer_install(binding_id)?;
        let binding_closed =
            {
                let mut progress = self
                    .producer_progress
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let binding = progress.get_mut(&binding_id).ok_or_else(|| {
                    contract_violation(
                        RuntimeFilterContractViolationKind::UnauthorizedBinding,
                        "producer binding has not been opened in this Backend session",
                    )
                })?;
                if binding.terminal.is_some() {
                    return Ok(RuntimeFilterSubmitOutcome::TerminalNoop);
                }
                let instance = binding.instances.get_mut(&fragment_instance_id).ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer fragment instance has not been opened in this Backend session",
                )
            })?;
                validate_open_partition(instance, partition)?;
                match instance.terminal_partitions.get(&partition) {
                    Some(previous) if *previous == terminal => {
                        return Ok(RuntimeFilterSubmitOutcome::TerminalNoop);
                    }
                    Some(_) => {
                        return Err(contract_violation(
                            RuntimeFilterContractViolationKind::ContractMismatch,
                            "producer partition closed with a conflicting terminal sequence",
                        ));
                    }
                    None => {
                        instance.terminal_partitions.insert(partition, terminal);
                        let closed = install
                            .expected_fragment_instances()
                            .iter()
                            .all(|expected| {
                                binding
                                    .instances
                                    .get(expected)
                                    .is_some_and(BackendOpenedProducer::is_closed)
                            });
                        if closed {
                            binding.terminal = Some(BackendProducerBindingTerminal::Closed);
                        }
                        closed
                    }
                }
            };
        if !binding_closed {
            return Ok(RuntimeFilterSubmitOutcome::CoverageStillPossible);
        }
        self.mark_satisfied(install.coverage_witness());
        Ok(match self.availability_progress() {
            BackendCoverageProgress::Satisfied => RuntimeFilterSubmitOutcome::PendingFinalSnapshot,
            BackendCoverageProgress::Pending => RuntimeFilterSubmitOutcome::CoverageStillPossible,
            BackendCoverageProgress::Impossible => {
                RuntimeFilterSubmitOutcome::CompletedWithoutArtifact
            }
        })
    }

    /// Fails one producer binding and fail-opens every installed consumer via
    /// the typed Execution unavailable outcome. No evaluator effect is made.
    pub(crate) fn fail(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        _reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        let install = self.producer_install(binding_id)?;
        {
            let mut progress = self
                .producer_progress
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let binding = progress.get_mut(&binding_id).ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer binding has not been opened in this Backend session",
                )
            })?;
            if !binding.instances.contains_key(&fragment_instance_id) {
                return Err(contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer fragment instance has not been opened in this Backend session",
                ));
            }
            if binding.terminal.is_some() {
                return Ok(RuntimeFilterSubmitOutcome::TerminalNoop);
            }
            binding.terminal = Some(BackendProducerBindingTerminal::Failed);
        }
        self.mark_impossible(install.coverage_witness());
        for consumer in self.consumers.values() {
            for route in &consumer.routes {
                consumer
                    .subscriptions
                    .publish(
                        *route,
                        SnapshotAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
                        None,
                    )
                    .map_err(subscription_violation)?;
            }
        }
        Ok(RuntimeFilterSubmitOutcome::CompletedWithoutArtifact)
    }

    /// Injects an immutable artifact-owner result for its authorized consumer
    /// route. It accepts neither Arrow values nor row/scan evaluator facts.
    pub(crate) fn publish_materialized(
        &self,
        route_edge_id: BackendRouteEdgeId,
        outcome: SnapshotAcquireOutcome,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let consumer = self
            .consumers
            .values()
            .find(|consumer| consumer.routes.contains(&route_edge_id))
            .ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "materialized artifact route is not installed in this Backend session",
                )
            })?;
        consumer
            .subscriptions
            .publish(route_edge_id, outcome, None)
            .map_err(subscription_violation)
    }

    /// Materialize a reduced semantic snapshot into each accepted Backend
    /// profile and publish only an immutable Execution snapshot or a typed
    /// fail-open outcome. No Arrow batch or scan fact crosses this boundary.
    fn publish_reduced_snapshot(
        &self,
        snapshot: &BackendReducedLogicalSnapshot,
    ) -> Result<(), RuntimeFilterContractViolation> {
        for consumer in self.consumers.values() {
            let outcome = match (snapshot.domain(), consumer.contract.contract()) {
                (
                    BackendReducedLogicalDomain::Membership(domain),
                    RuntimeFilterExecutionContract::Membership(schema),
                ) => match crate::runtime_filter::materializer::materialize_membership(
                    snapshot.channel_id().get(),
                    domain,
                    schema,
                    snapshot.logical_version(),
                    &consumer.profile,
                    crate::runtime_filter::materializer::MaterializationAdmission::new(
                        self.channel.max_artifact_bytes(),
                    ),
                ) {
                    crate::runtime_filter::materializer::MaterializationOutcome::Published(bundle) => {
                        match crate::runtime_filter::artifact_query::BackendRuntimeFilterArtifactQuery::membership(
                            &bundle,
                            schema.data_type().clone(),
                            schema.null_semantics(),
                        ) {
                            Ok(query) => SnapshotAcquireOutcome::Published(Arc::new(
                                RuntimeFilterSnapshot::new(
                                    consumer.contract.binding_id(),
                                    snapshot.logical_version(),
                                    schema.digest(),
                                    Arc::new(query),
                                ),
                            )),
                            Err(_) => SnapshotAcquireOutcome::Unavailable(
                                UnavailableReason::MaterializationFailed,
                            ),
                        }
                    }
                    crate::runtime_filter::materializer::MaterializationOutcome::Unsupported(_) => {
                        SnapshotAcquireOutcome::Unsupported(
                            novarocks_execution::runtime_filter::ArtifactUnsupportedReason::NoAcceptedRepresentation,
                        )
                    }
                    crate::runtime_filter::materializer::MaterializationOutcome::Unavailable(_) => {
                        SnapshotAcquireOutcome::Unavailable(UnavailableReason::MaterializationFailed)
                    }
                },
                (
                    BackendReducedLogicalDomain::OrderedBound(bound),
                    RuntimeFilterExecutionContract::Ordered(order),
                ) => match crate::runtime_filter::materializer::range::materialize_range(
                    snapshot.channel_id().get(),
                    order,
                    bound,
                    snapshot.logical_version(),
                    &consumer.profile,
                    &crate::runtime_filter::materializer::MaterializationAdmission::new(
                        self.channel.max_artifact_bytes(),
                    ),
                ) {
                    Ok(bundle) => match crate::runtime_filter::artifact_query::BackendRuntimeFilterArtifactQuery::ordered(
                        &bundle,
                        Arc::clone(order),
                    ) {
                        Ok(query) => SnapshotAcquireOutcome::Published(Arc::new(
                            RuntimeFilterSnapshot::new(
                                consumer.contract.binding_id(),
                                snapshot.logical_version(),
                                order.digest(),
                                Arc::new(query),
                            ),
                        )),
                        Err(_) => SnapshotAcquireOutcome::Unavailable(
                            UnavailableReason::MaterializationFailed,
                        ),
                    },
                    Err(_) => SnapshotAcquireOutcome::Unavailable(
                        UnavailableReason::MaterializationFailed,
                    ),
                },
                _ => SnapshotAcquireOutcome::Unavailable(UnavailableReason::MaterializationFailed),
            };
            for route in &consumer.routes {
                consumer
                    .subscriptions
                    .publish(*route, outcome.clone(), None)
                    .map_err(subscription_violation)?;
            }
        }
        Ok(())
    }

    fn register_producer(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let install = self.producer_install(binding_id)?;
        if request.contract() != install.contract() {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "producer execution contract does not match the installed Backend binding",
            ));
        }
        if request.local_partition_count() == 0 {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::InvalidPartitionCount,
                "producer requires a non-zero local partition count",
            ));
        }
        if !install
            .expected_fragment_instances()
            .contains(&fragment_instance_id)
        {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer fragment instance is not authorized by the Backend installation",
            ));
        }
        let mut progress = self
            .producer_progress
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let binding = progress.entry(binding_id).or_default();
        if binding.terminal.is_some() {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::SessionClosed,
                "producer binding is already terminal",
            ));
        }
        if let Some(existing) = binding.instances.get(&fragment_instance_id) {
            if existing.partition_count == request.local_partition_count() {
                return Ok(());
            }
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "producer instance was reopened with a different local partition count",
            ));
        }
        binding.instances.insert(
            fragment_instance_id,
            BackendOpenedProducer::new(request.local_partition_count()),
        );
        Ok(())
    }

    fn open_stream(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
    ) -> Result<BackendProducerStreamIdentity, RuntimeFilterContractViolation> {
        self.producer_install(binding_id)?;
        let progress = self
            .producer_progress
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let binding = progress.get(&binding_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer binding has not been opened in this Backend session",
            )
        })?;
        if binding.terminal.is_some() {
            return Err(contract_violation(
                RuntimeFilterContractViolationKind::SessionClosed,
                "producer binding is already terminal",
            ));
        }
        let instance = binding
            .instances
            .get(&fragment_instance_id)
            .ok_or_else(|| {
                contract_violation(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "producer fragment instance has not been opened in this Backend session",
                )
            })?;
        validate_open_partition(instance, partition)?;
        Ok(BackendProducerStreamIdentity::new(
            BackendChannelIdentity::new(self.participant, binding_id, self.channel.channel_id()),
            fragment_instance_id,
            partition,
        ))
    }

    fn producer_install(
        &self,
        binding_id: RuntimeFilterBindingId,
    ) -> Result<&BackendProducerInstall, RuntimeFilterContractViolation> {
        self.producers.get(&binding_id).ok_or_else(|| {
            contract_violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed in this Backend channel session",
            )
        })
    }

    fn mark_satisfied(&self, witness: super::BackendCoverageWitnessId) {
        self.availability
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mark_satisfied(witness);
        self.terminal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mark_satisfied(witness);
    }

    fn mark_impossible(&self, witness: super::BackendCoverageWitnessId) {
        self.availability
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mark_impossible(witness);
        self.terminal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mark_impossible(witness);
    }
}

struct BackendRuntimeFilterProducer {
    session: Arc<BackendRuntimeFilterSession>,
    binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
}

impl RuntimeFilterProducer for BackendRuntimeFilterProducer {
    fn max_contribution_bytes(&self) -> usize {
        self.session.policy.max_contribution_bytes()
    }

    fn submit(
        &self,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.session
            .submit(
                self.binding_id,
                self.fragment_instance_id,
                partition,
                sequence,
                contribution,
            )
            .map(|submission| submission.outcome())
    }

    fn close_partition(
        &self,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.session.close_partition(
            self.binding_id,
            self.fragment_instance_id,
            partition,
            terminal,
        )
    }

    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.session
            .fail(self.binding_id, self.fragment_instance_id, reason)
    }
}

fn validate_producer_shape(
    channel: &BackendChannelInstall,
    expected: &BackendProducerInstall,
    producer: &BackendProducerInstall,
) -> Result<(), BackendRuntimeFilterSessionError> {
    if producer.contract().channel_id() != channel.channel_id()
        || producer.contract().contract() != channel.execution_contract()
        || producer.contract().kind() != expected.contract().kind()
        || producer.contract().reduction() != expected.contract().reduction()
        || producer.contract().completion() != expected.contract().completion()
        || producer.max_contribution_bytes() != expected.max_contribution_bytes()
    {
        return Err(BackendRuntimeFilterSessionError::ProducerShapeMismatch);
    }
    Ok(())
}

fn validate_consumer_shape(
    channel: &BackendChannelInstall,
    producer: &BackendProducerInstall,
    consumer: &BackendConsumerInstall,
) -> Result<(), BackendRuntimeFilterSessionError> {
    if consumer.contract().channel_id() != channel.channel_id()
        || consumer.contract().contract() != channel.execution_contract()
        || consumer.contract().reduction() != producer.contract().reduction()
    {
        return Err(BackendRuntimeFilterSessionError::ConsumerContractMismatch);
    }
    Ok(())
}

fn validate_open_partition(
    producer: &BackendOpenedProducer,
    partition: PartitionId,
) -> Result<(), RuntimeFilterContractViolation> {
    if !producer.contains_partition(partition) {
        return Err(contract_violation(
            RuntimeFilterContractViolationKind::InvalidPartitionCount,
            "producer partition is outside its opened local partition count",
        ));
    }
    if producer.terminal_partitions.contains_key(&partition) {
        return Err(contract_violation(
            RuntimeFilterContractViolationKind::SessionClosed,
            "producer partition is already terminal",
        ));
    }
    Ok(())
}

fn reduction_violation(error: BackendReductionStateError) -> RuntimeFilterContractViolation {
    contract_violation(
        RuntimeFilterContractViolationKind::ContractMismatch,
        format!("Backend reduction rejected the Execution contribution: {error:?}"),
    )
}

fn subscription_violation(error: BackendSubscriptionError) -> RuntimeFilterContractViolation {
    contract_violation(
        RuntimeFilterContractViolationKind::UnauthorizedBinding,
        format!("Backend subscription publication was not installed: {error:?}"),
    )
}

fn contract_violation(
    kind: RuntimeFilterContractViolationKind,
    detail: impl Into<Arc<str>>,
) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_execution::runtime_filter::{
        RuntimeFilterChannelId, RuntimeFilterConsumerContract, RuntimeFilterLateApplyGranularity,
        RuntimeFilterSubscriptionRequest,
    };

    use super::*;
    use crate::runtime_filter::{
        artifact::{ArtifactKind, ConsumerArtifactProfile},
        domain::{
            BackendChannelLifecycle, BackendCoverage, BackendCoverageWitnessId,
            BackendMaterializationPolicy, BackendProducerInstall,
            CollectingBackendRuntimeFilterEventObserver,
        },
        test_support::BackendRuntimeFilterFixture,
    };

    fn instance(raw: i64) -> UniqueId {
        UniqueId::new(raw, raw + 1)
    }

    fn channel() -> (BackendChannelInstall, BackendRuntimeFilterFixture) {
        let fixture = BackendRuntimeFilterFixture::membership();
        let schema = fixture.producer_contract().contract().clone();
        let first = BackendProducerInstall::new(
            fixture.producer_contract(),
            BackendCoverageWitnessId::new(29),
            [instance(37), instance(41)],
            1024,
        )
        .unwrap();
        let second_contract =
            novarocks_execution::runtime_filter::RuntimeFilterProducerContract::membership(
                RuntimeFilterBindingId::new(8),
                RuntimeFilterChannelId::new(11),
                schema.clone(),
            )
            .unwrap();
        let second = BackendProducerInstall::new(
            second_contract,
            BackendCoverageWitnessId::new(31),
            [instance(43)],
            1024,
        )
        .unwrap();
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();
        let blocking = BackendConsumerInstall::new(
            RuntimeFilterConsumerContract::membership_blocking(
                RuntimeFilterBindingId::new(70),
                RuntimeFilterChannelId::new(11),
                schema.clone(),
            )
            .unwrap(),
            profile.clone(),
            [BackendRouteEdgeId::new(101)],
            [instance(51)],
        )
        .unwrap();
        let live = BackendConsumerInstall::new(
            RuntimeFilterConsumerContract::membership_live(
                RuntimeFilterBindingId::new(71),
                RuntimeFilterChannelId::new(11),
                RuntimeFilterLateApplyGranularity::Row,
                schema.clone(),
            )
            .unwrap(),
            profile,
            [BackendRouteEdgeId::new(102)],
            [instance(53)],
        )
        .unwrap();
        let coverage = BackendCoverage::all_of([
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            BackendCoverage::witness(BackendCoverageWitnessId::new(31)),
        ])
        .unwrap();
        let channel = BackendChannelInstall::new(
            RuntimeFilterChannelId::new(11),
            schema,
            BackendChannelLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            BackendMaterializationPolicy::new(8, 3, 5, 1, 1024, 1024, 1).unwrap(),
            1024,
            1024,
            [first, second],
            [blocking, live],
            [],
        )
        .unwrap();
        (channel, fixture)
    }

    fn session() -> (
        Arc<BackendRuntimeFilterSession>,
        BackendRuntimeFilterFixture,
    ) {
        let (channel, fixture) = channel();
        (
            Arc::new(
                BackendRuntimeFilterSession::from_channel_install(
                    fixture.identity(),
                    channel,
                    Arc::new(CollectingBackendRuntimeFilterEventObserver::default()),
                )
                .unwrap(),
            ),
            fixture,
        )
    }

    #[test]
    fn binding_witness_waits_for_every_expected_instance_and_partition() {
        let (session, fixture) = session();
        let RuntimeFilterBindOutcome::Bound(first) = session
            .open_producer(
                instance(37),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
            )
            .unwrap()
        else {
            panic!("installed producer must bind")
        };
        first
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                fixture.membership_contribution(),
            )
            .unwrap();
        assert_eq!(
            first
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            RuntimeFilterSubmitOutcome::CoverageStillPossible
        );
        let RuntimeFilterBindOutcome::Bound(second_instance) = session
            .open_producer(
                instance(41),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
            )
            .unwrap()
        else {
            panic!("second expected producer instance must bind")
        };
        second_instance
            .close_partition(PartitionId::new(0), ProducerSequence::new(0))
            .unwrap();
        assert_eq!(
            session.availability_progress(),
            BackendCoverageProgress::Pending
        );
        let second_binding =
            novarocks_execution::runtime_filter::RuntimeFilterProducerContract::membership(
                RuntimeFilterBindingId::new(8),
                RuntimeFilterChannelId::new(11),
                fixture.producer_contract().contract().clone(),
            )
            .unwrap();
        let RuntimeFilterBindOutcome::Bound(second_binding_producer) = session
            .open_producer(
                instance(43),
                RuntimeFilterProducerOpenRequest::new(second_binding, 1),
            )
            .unwrap()
        else {
            panic!("second installed producer binding must bind")
        };
        assert_eq!(
            second_binding_producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(0))
                .unwrap(),
            RuntimeFilterSubmitOutcome::PendingFinalSnapshot
        );
        assert_eq!(
            session.availability_progress(),
            BackendCoverageProgress::Satisfied
        );
    }

    #[test]
    fn multiple_consumer_bindings_resolve_their_own_blocking_and_live_slots() {
        let (session, _) = session();
        let blocking = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        let live = RuntimeFilterConsumerContract::membership_live(
            RuntimeFilterBindingId::new(71),
            RuntimeFilterChannelId::new(11),
            RuntimeFilterLateApplyGranularity::Row,
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        assert!(matches!(
            session
                .subscribe(
                    instance(51),
                    RuntimeFilterSubscriptionRequest::new(blocking)
                )
                .unwrap(),
            RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(_))
        ));
        assert!(matches!(
            session
                .subscribe(instance(53), RuntimeFilterSubscriptionRequest::new(live))
                .unwrap(),
            RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Live(_))
        ));
    }

    #[test]
    fn producer_failure_fail_opens_every_consumer_binding() {
        let (session, fixture) = session();
        let RuntimeFilterBindOutcome::Bound(producer) = session
            .open_producer(
                instance(37),
                RuntimeFilterProducerOpenRequest::new(fixture.producer_contract(), 1),
            )
            .unwrap()
        else {
            panic!("installed producer must bind")
        };
        let blocking = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            RuntimeFilterChannelId::new(11),
            session.channel().execution_contract().clone(),
        )
        .unwrap();
        let RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(
            subscription,
        )) = session
            .subscribe(
                instance(51),
                RuntimeFilterSubscriptionRequest::new(blocking),
            )
            .unwrap()
        else {
            panic!("installed blocking consumer must bind")
        };
        producer
            .fail(RuntimeFilterProducerFailure::ExecutionFailed)
            .unwrap();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            SnapshotAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed)
        ));
    }
}
