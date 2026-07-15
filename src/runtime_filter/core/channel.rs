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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CoverageWitnessId, NullSemantics, RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::events::{
    ProducerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity,
};
use crate::runtime_filter::port::identity::{
    ContributionIdentity, DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence,
    ProducerStreamId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::RuntimeFilterChannelDeployment;
use crate::runtime_filter::port::ordered_bound::{OrderedBoundUpdate, RuntimeOrderContract};
use crate::runtime_filter::port::producer::{
    ProducerFailureReason, RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::subscription::UnavailableReason;
use crate::runtime_filter::port::support::{
    RetainedMemoryReservation, RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use crate::runtime_filter::port::value_domain::{LogicalSnapshot, ValueDomainDelta};

use super::coverage::{CoverageProgress, WitnessProgress, evaluate};
use super::error::ChannelBuildError;
use super::ordered_reducer::{OrderedApplyOutcome, OrderedCloseOutcome, OrderedReducer};
use super::reducer::{MembershipReducer, ReducerError};
use super::state::{InstanceState, LogicalTerminal, TerminalProgress};

const REPLAY_METADATA_BYTES: usize = size_of::<u64>() + 32;
const TERMINAL_METADATA_BYTES: usize = size_of::<u64>();

#[derive(Debug)]
pub(crate) enum ChannelAction {
    None,
    Progress {
        order: Option<u64>,
        outcome: SubmitOutcome,
        events: Vec<RuntimeFilterEvent>,
    },
    VisibleSnapshot {
        order: u64,
        outcome: SubmitOutcome,
        version: LogicalVersion,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Completed {
        order: u64,
        outcome: SubmitOutcome,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Unavailable {
        order: u64,
        outcome: SubmitOutcome,
        reason: UnavailableReason,
        events: Vec<RuntimeFilterEvent>,
    },
    CompletedWithoutArtifact {
        order: u64,
        outcome: SubmitOutcome,
        events: Vec<RuntimeFilterEvent>,
    },
    DegradedLogical {
        order: u64,
        outcome: SubmitOutcome,
        reason: UnavailableReason,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Cancelled {
        order: u64,
        events: Vec<RuntimeFilterEvent>,
    },
}

impl ChannelAction {
    pub(crate) fn logical_terminal(&self) -> Option<LogicalTerminal> {
        match self {
            Self::Completed { .. } => Some(LogicalTerminal::Completed),
            Self::CompletedWithoutArtifact { .. } => {
                Some(LogicalTerminal::CompletedWithoutArtifact)
            }
            Self::DegradedLogical { reason, .. } => Some(LogicalTerminal::DegradedLogical(*reason)),
            Self::Unavailable { reason, .. } => Some(LogicalTerminal::Unavailable(*reason)),
            Self::Cancelled { .. } => Some(LogicalTerminal::Cancelled),
            Self::None | Self::Progress { .. } | Self::VisibleSnapshot { .. } => None,
        }
    }

    pub(crate) fn outcome(&self) -> SubmitOutcome {
        match self {
            Self::None | Self::Cancelled { .. } => SubmitOutcome::TerminalNoop,
            Self::Progress { outcome, .. }
            | Self::VisibleSnapshot { outcome, .. }
            | Self::Completed { outcome, .. }
            | Self::Unavailable { outcome, .. }
            | Self::CompletedWithoutArtifact { outcome, .. }
            | Self::DegradedLogical { outcome, .. } => *outcome,
        }
    }

    pub(crate) fn snapshot(&self) -> Option<Arc<LogicalSnapshot>> {
        match self {
            Self::VisibleSnapshot { snapshot, .. }
            | Self::Completed { snapshot, .. }
            | Self::DegradedLogical { snapshot, .. } => Some(snapshot.clone()),
            _ => None,
        }
    }

    pub(crate) const fn unavailable_reason(&self) -> Option<UnavailableReason> {
        match self {
            Self::Unavailable { reason, .. } | Self::DegradedLogical { reason, .. } => {
                Some(*reason)
            }
            _ => None,
        }
    }

    pub(crate) fn events(&self) -> &[RuntimeFilterEvent] {
        match self {
            Self::None => &[],
            Self::Progress { events, .. }
            | Self::VisibleSnapshot { events, .. }
            | Self::Completed { events, .. }
            | Self::Unavailable { events, .. }
            | Self::CompletedWithoutArtifact { events, .. }
            | Self::DegradedLogical { events, .. }
            | Self::Cancelled { events, .. } => events,
        }
    }

    pub(crate) const fn dispatch_order(&self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Progress { order, .. } => *order,
            Self::VisibleSnapshot { order, .. }
            | Self::Completed { order, .. }
            | Self::Unavailable { order, .. }
            | Self::CompletedWithoutArtifact { order, .. }
            | Self::DegradedLogical { order, .. }
            | Self::Cancelled { order, .. } => Some(*order),
        }
    }
}

struct ProducerRuntime {
    witness_id: CoverageWitnessId,
    instances: BTreeMap<UniqueId, InstanceState>,
}

enum ChannelTerminal {
    Collecting,
    Completed {
        order: u64,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Unavailable {
        order: u64,
        reason: UnavailableReason,
        events: Vec<RuntimeFilterEvent>,
    },
    CompletedWithoutArtifact {
        order: u64,
        events: Vec<RuntimeFilterEvent>,
    },
    DegradedLogical {
        order: u64,
        reason: UnavailableReason,
        snapshot: Arc<LogicalSnapshot>,
        events: Vec<RuntimeFilterEvent>,
    },
    Cancelled {
        order: u64,
        events: Vec<RuntimeFilterEvent>,
    },
}

struct OrderedCoreState {
    reducer: OrderedReducer,
    availability_witnesses: BTreeMap<CoverageWitnessId, WitnessProgress>,
    latest: Option<Arc<LogicalSnapshot>>,
}

struct ChannelState {
    terminal: ChannelTerminal,
    producers: BTreeMap<BindingId, ProducerRuntime>,
    witnesses: BTreeMap<CoverageWitnessId, WitnessProgress>,
    reducer: Option<MembershipReducer>,
    ordered: Option<OrderedCoreState>,
    reservation: RetainedMemoryReservation,
    next_dispatch_order: u64,
}

struct LockedAction {
    action: ChannelAction,
    release_after_unlock: Option<RetainedMemoryReservation>,
}

impl LockedAction {
    fn without_release(action: ChannelAction) -> Self {
        Self {
            action,
            release_after_unlock: None,
        }
    }

    fn finish(self) -> ChannelAction {
        drop(self.release_after_unlock);
        self.action
    }
}

pub(crate) struct RuntimeFilterChannel {
    event_identity: RuntimeFilterEventIdentity,
    channel_id: ChannelId,
    availability_coverage: Coverage,
    terminal_coverage: Coverage,
    data_type: Option<DataType>,
    null_semantics: Option<NullSemantics>,
    max_contribution_bytes: u64,
    max_reducer_bytes: u64,
    deadline: OnceLock<Instant>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    state: Mutex<ChannelState>,
}

impl RuntimeFilterChannel {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
        deployment: &RuntimeFilterChannelDeployment,
        deadline: Instant,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<Self, ChannelBuildError> {
        let channel =
            Self::new_unanchored(query_id, participant_id, epoch, deployment, memory_account)?;
        channel
            .deadline
            .set(deadline)
            .expect("new channel deadline is initialized exactly once");
        Ok(channel)
    }

    pub(crate) fn new_unanchored(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
        deployment: &RuntimeFilterChannelDeployment,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<Self, ChannelBuildError> {
        let (data_type, null_semantics, reducer, ordered_contract) =
            match deployment.logical_domain() {
                RuntimeFilterLogicalDomain::Membership {
                    value_type,
                    null_semantics,
                } => {
                    let reducer = MembershipReducer::try_new(value_type.clone(), *null_semantics)
                        .map_err(|_| ChannelBuildError::UnsupportedMembershipType)?;
                    (
                        Some(value_type.clone()),
                        Some(*null_semantics),
                        Some(reducer),
                        None,
                    )
                }
                RuntimeFilterLogicalDomain::OrderedBound(plan) => (
                    None,
                    None,
                    None,
                    Some(Arc::new(
                        RuntimeOrderContract::try_from_plan(plan)
                            .map_err(|_| ChannelBuildError::UnsupportedContract)?,
                    )),
                ),
            };
        let mut witnesses = BTreeMap::new();
        let producers = deployment
            .producers()
            .iter()
            .map(|(binding_id, producer)| {
                witnesses.insert(producer.coverage_witness_id(), WitnessProgress::Pending);
                let instances = producer
                    .expected_fragment_instances()
                    .iter()
                    .copied()
                    .map(|instance| (instance, InstanceState::default()))
                    .collect();
                (
                    *binding_id,
                    ProducerRuntime {
                        witness_id: producer.coverage_witness_id(),
                        instances,
                    },
                )
            })
            .collect();
        if deployment
            .availability_coverage()
            .witness_ids_in_order()
            .iter()
            .chain(deployment.terminal_coverage().witness_ids_in_order().iter())
            .any(|witness| !witnesses.contains_key(witness))
        {
            return Err(ChannelBuildError::MissingCoverageWitness);
        }
        Ok(Self {
            event_identity: RuntimeFilterEventIdentity::new(
                query_id,
                participant_id,
                deployment.channel_id(),
                epoch,
            ),
            channel_id: deployment.channel_id(),
            availability_coverage: deployment.availability_coverage().clone(),
            terminal_coverage: deployment.terminal_coverage().clone(),
            data_type,
            null_semantics,
            max_contribution_bytes: deployment.policy().max_contribution_bytes,
            max_reducer_bytes: deployment.core_budget().max_reducer_bytes(),
            deadline: OnceLock::new(),
            memory_account,
            state: Mutex::new(ChannelState {
                terminal: ChannelTerminal::Collecting,
                producers,
                witnesses: witnesses.clone(),
                reducer,
                ordered: ordered_contract.map(|contract| OrderedCoreState {
                    reducer: OrderedReducer::new(contract),
                    availability_witnesses: witnesses,
                    latest: None,
                }),
                reservation: RetainedMemoryReservation::empty(),
                next_dispatch_order: 0,
            }),
        })
    }

    pub(crate) fn initialize_deadline(&self, deadline: Instant) -> Result<(), ()> {
        self.deadline.set(deadline).map_err(|_| ())
    }

    pub(crate) fn open_producer(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let mut state = self.state.lock().unwrap();
        let installed_count =
            instance_ref(&state, binding_id, fragment_instance_id)?.local_partition_count();
        if local_partition_count == 0 {
            return Err(violation(
                RuntimeContractViolationKind::InvalidPartitionCount,
                "local partition count must be non-zero",
            ));
        }
        if let Some(installed_count) = installed_count {
            return if installed_count == local_partition_count {
                Ok(SubmitOutcome::Duplicate)
            } else {
                Err(violation(
                    RuntimeContractViolationKind::PartitionCountConflict,
                    "producer instance reopened with a different partition count",
                ))
            };
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(SubmitOutcome::TerminalNoop);
        }
        let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
        instance.open(local_partition_count);
        Ok(SubmitOutcome::Applied)
    }

    pub(crate) fn authorize_submit(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
    ) -> Result<(), RuntimeContractViolation> {
        let state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
        temporary_lease: TemporaryContributionLease,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let Some(data_type) = self.data_type.as_ref() else {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "ordered channel cannot accept membership deltas",
            ));
        };
        let identity = ContributionIdentity::new(
            self.event_identity.query_id(),
            self.event_identity.participant_id(),
            self.channel_id,
            self.event_identity.epoch(),
            ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
            sequence,
        );
        let mut incoming_reservation: Option<RetainedMemoryReservation> = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            if !delta.matches_data_type(data_type) {
                return Err(violation(
                    RuntimeContractViolationKind::TypeMismatch,
                    "delta type does not match channel membership type",
                ));
            }
            let contribution_bytes = match delta.estimated_contribution_bytes() {
                Ok(bytes) => bytes,
                Err(_) => {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            if temporary_lease.bytes() != contribution_bytes {
                return Err(violation(
                    RuntimeContractViolationKind::InvalidContributionLease,
                    "temporary contribution lease does not match canonical payload size",
                ));
            }
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            let fingerprint = delta.fingerprint();
            {
                let partition =
                    partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
                if let Some(previous) =
                    partition.and_then(|partition| partition.seen.get(&sequence))
                {
                    return if *previous == fingerprint {
                        Ok(ChannelAction::Progress {
                            order: Some(next_dispatch_order(&mut state)),
                            outcome: SubmitOutcome::Duplicate,
                            events: vec![RuntimeFilterEvent::DeltaDuplicateIgnored { identity }],
                        })
                    } else {
                        Err(violation(
                            RuntimeContractViolationKind::ConflictingReplay,
                            "same contribution identity carried a different payload",
                        ))
                    };
                }
            }

            let instance_progress =
                instance_mut(&mut state, binding_id, fragment_instance_id)?.progress;
            let (partition_progress, terminal_sequence) = {
                let partition =
                    partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
                partition.map_or((TerminalProgress::Pending, None), |partition| {
                    (partition.progress, partition.terminal_sequence)
                })
            };
            if instance_progress == TerminalProgress::Impossible
                || partition_progress == TerminalProgress::Impossible
            {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            if partition_progress == TerminalProgress::Satisfied {
                return Err(violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "new delta arrived after partition close",
                ));
            }
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            if terminal_sequence.is_some_and(|terminal| sequence >= terminal) {
                return Err(violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "delta sequence is outside the exclusive terminal range",
                ));
            }

            if u64::try_from(contribution_bytes)
                .map_or(true, |bytes| bytes > self.max_contribution_bytes)
            {
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            let projection = match state
                .reducer
                .as_ref()
                .expect("membership channel owns a membership reducer")
                .preflight(&delta)
            {
                Ok(projection) => projection,
                Err(ReducerError::TypeMismatch | ReducerError::UnsupportedType) => {
                    return Err(violation(
                        RuntimeContractViolationKind::TypeMismatch,
                        "delta type does not match channel membership type",
                    ));
                }
                Err(ReducerError::SizeOverflow) => {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            let retained_growth = match projection
                .retained_growth()
                .checked_add(REPLAY_METADATA_BYTES)
            {
                Some(bytes) => bytes,
                None => {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    drop(incoming_reservation);
                    return Ok(locked.finish());
                }
            };
            let projected_total = state.reservation.bytes().checked_add(retained_growth);
            if projected_total
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            if incoming_reservation
                .as_ref()
                .map(|reservation| reservation.bytes())
                != Some(retained_growth)
            {
                if reservation_failed_for == Some(retained_growth) {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(incoming_reservation.take());
                match RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    retained_growth,
                ) {
                    Ok(reservation) => incoming_reservation = Some(reservation),
                    Err(_) => reservation_failed_for = Some(retained_growth),
                }
                continue;
            }
            let incoming = incoming_reservation
                .take()
                .expect("matching retained reservation must exist");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            state
                .reducer
                .as_mut()
                .expect("membership channel owns a membership reducer")
                .commit_preflighted(&delta)
                .expect("preflighted reducer commit must preserve type invariants");
            let partition = partition_mut_for_commit(
                &mut state,
                binding_id,
                fragment_instance_id,
                partition_id,
            );
            partition.seen.insert(sequence, fingerprint);
            if partition.is_gapless() {
                partition.progress = TerminalProgress::Satisfied;
            }
            let events = vec![RuntimeFilterEvent::DeltaAccepted { identity }];
            let locked = self.refresh_after_progress(
                &mut state,
                binding_id,
                fragment_instance_id,
                SubmitOutcome::Applied,
                events,
            );
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn submit_ordered(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
        temporary_lease: TemporaryContributionLease,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let contribution_bytes = update.canonical_contribution_bytes().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::InvalidContributionLease,
                "ordered contribution canonical size overflowed",
            )
        })?;
        if temporary_lease.bytes() != contribution_bytes {
            return Err(violation(
                RuntimeContractViolationKind::InvalidContributionLease,
                "temporary contribution lease does not match canonical ordered payload size",
            ));
        }
        let identity = ContributionIdentity::new(
            self.event_identity.query_id(),
            self.event_identity.participant_id(),
            self.channel_id,
            self.event_identity.epoch(),
            ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
            sequence,
        );
        let stream_id = identity.stream();
        let mut metadata_reservation = None;
        let mut snapshot_reservation = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            let ordered = state.ordered.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "membership channel cannot accept ordered bounds",
                )
            })?;
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                ordered
                    .reducer
                    .validate_tombstone_update(stream_id, sequence, &update)?;
                return Ok(terminal_action_from_state(&state));
            }
            if u64::try_from(contribution_bytes)
                .map_or(true, |bytes| bytes > self.max_contribution_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let before_bytes = ordered.reducer.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let mut next_reducer = ordered.reducer.clone();
            let apply_outcome = next_reducer.apply(stream_id, sequence, update.clone())?;
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            let after_bytes = next_reducer.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let metadata_growth = after_bytes.saturating_sub(before_bytes);
            let mut availability_witnesses = ordered.availability_witnesses.clone();
            if !matches!(
                apply_outcome,
                OrderedApplyOutcome::Stale | OrderedApplyOutcome::Duplicate
            ) {
                let witness_id = state
                    .producers
                    .get(&binding_id)
                    .expect("authorized ordered producer")
                    .witness_id;
                availability_witnesses
                    .get_mut(&witness_id)
                    .expect("ordered availability witness is installed")
                    .advance(WitnessProgress::Satisfied);
            }
            let availability = evaluate(&self.availability_coverage, &availability_witnesses);
            let publish = availability == CoverageProgress::Satisfied
                && (ordered.latest.is_none()
                    || matches!(apply_outcome, OrderedApplyOutcome::GlobalTightened(_)));
            let (version, snapshot_bytes) = if publish {
                let version =
                    ordered
                        .latest
                        .as_ref()
                        .map_or(Ok(LogicalVersion::FIRST), |latest| {
                            latest.version().checked_next().ok_or_else(|| {
                                violation(
                                    RuntimeContractViolationKind::LogicalVersionOverflow,
                                    "ordered logical version overflowed",
                                )
                            })
                        })?;
                let bytes = next_reducer
                    .global()
                    .expect("satisfied ordered availability owns a global bound")
                    .estimated_retained_bytes()
                    .ok_or_else(|| {
                        violation(
                            RuntimeContractViolationKind::OrderedContractMismatch,
                            "ordered snapshot retained size overflowed",
                        )
                    })?;
                (Some(version), bytes)
            } else {
                (None, 0)
            };
            let projected_bytes = state
                .reservation
                .bytes()
                .checked_add(metadata_growth)
                .and_then(|bytes| bytes.checked_add(snapshot_bytes));
            if projected_bytes
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            let reservations_match = metadata_reservation
                .as_ref()
                .map(RetainedMemoryReservation::bytes)
                == Some(metadata_growth)
                && snapshot_reservation
                    .as_ref()
                    .map(RetainedMemoryReservation::bytes)
                    == Some(snapshot_bytes);
            if !reservations_match {
                let required = (metadata_growth, snapshot_bytes);
                if reservation_failed_for == Some(required) {
                    let locked = self.make_ordered_unavailable_or_degraded(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                        Vec::new(),
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(metadata_reservation.take());
                drop(snapshot_reservation.take());
                metadata_reservation = RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    metadata_growth,
                )
                .ok();
                snapshot_reservation =
                    RetainedMemoryReservation::try_new(self.memory_account.clone(), snapshot_bytes)
                        .ok();
                if metadata_reservation.is_none() || snapshot_reservation.is_none() {
                    reservation_failed_for = Some(required);
                }
                continue;
            }

            let incoming_metadata = metadata_reservation
                .take()
                .expect("matching ordered metadata reservation exists");
            if let Err(failure) = state.reservation.absorb(incoming_metadata) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            let mut events = match apply_outcome {
                OrderedApplyOutcome::Stale => {
                    vec![RuntimeFilterEvent::OrderedUpdateStale { identity }]
                }
                OrderedApplyOutcome::Duplicate => {
                    vec![RuntimeFilterEvent::DeltaDuplicateIgnored { identity }]
                }
                OrderedApplyOutcome::SequenceAdvancedEqual => {
                    vec![RuntimeFilterEvent::OrderedUpdateEqual { identity }]
                }
                OrderedApplyOutcome::StreamTightened => {
                    vec![RuntimeFilterEvent::OrderedStreamTightened { identity }]
                }
                OrderedApplyOutcome::GlobalTightened(_) => Vec::new(),
            };
            let outcome = match apply_outcome {
                OrderedApplyOutcome::Stale => SubmitOutcome::Stale,
                OrderedApplyOutcome::Duplicate => SubmitOutcome::Duplicate,
                OrderedApplyOutcome::SequenceAdvancedEqual => SubmitOutcome::SequenceAdvancedEqual,
                OrderedApplyOutcome::StreamTightened => SubmitOutcome::StreamAcceptedNoGlobalChange,
                OrderedApplyOutcome::GlobalTightened(_) => SubmitOutcome::Published,
            };
            let availability_was_satisfied = state.ordered.as_ref().is_some_and(|ordered| {
                evaluate(&self.availability_coverage, &ordered.availability_witnesses)
                    == CoverageProgress::Satisfied
            });
            let ordered = state
                .ordered
                .as_mut()
                .expect("ordered channel owns ordered state");
            ordered.reducer = next_reducer;
            ordered.availability_witnesses = availability_witnesses;
            if !availability_was_satisfied && availability == CoverageProgress::Satisfied {
                events.push(RuntimeFilterEvent::OrderedAvailabilityReached {
                    identity: self.event_identity,
                });
            }
            let published = version.map(|version| {
                let domain = ordered
                    .reducer
                    .global()
                    .expect("published ordered version owns a global bound")
                    .clone();
                let reservation = snapshot_reservation
                    .take()
                    .expect("published ordered version owns exact reservation");
                let snapshot = Arc::new(LogicalSnapshot::ordered(
                    self.channel_id,
                    version,
                    domain,
                    reservation,
                ));
                ordered.latest = Some(snapshot.clone());
                events.push(RuntimeFilterEvent::OrderedGlobalTightened { identity, version });
                events.push(RuntimeFilterEvent::LogicalVersionPublished {
                    identity: self.event_identity,
                    version,
                });
                snapshot
            });
            refresh_ordered_instance_progress(&mut state, binding_id, fragment_instance_id);
            let locked =
                self.refresh_after_ordered_progress(&mut state, outcome, published, events);
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn close_ordered_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let stream_id = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let mut reservation = None;
        let mut reservation_failed_for = None;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            let ordered = state.ordered.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "membership channel cannot accept ordered close",
                )
            })?;
            let before_bytes = ordered.reducer.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let mut next_reducer = ordered.reducer.clone();
            let close_outcome = next_reducer.close(stream_id, terminal_sequence)?;
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(terminal_action_from_state(&state));
            }
            let after_bytes = next_reducer.estimated_retained_bytes().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedContractMismatch,
                    "ordered reducer retained size overflowed",
                )
            })?;
            let growth = after_bytes.saturating_sub(before_bytes);
            if state
                .reservation
                .bytes()
                .checked_add(growth)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                return Ok(locked.finish());
            }
            if reservation.as_ref().map(RetainedMemoryReservation::bytes) != Some(growth) {
                if reservation_failed_for == Some(growth) {
                    let locked = self.make_ordered_unavailable_or_degraded(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                        Vec::new(),
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(reservation.take());
                reservation =
                    RetainedMemoryReservation::try_new(self.memory_account.clone(), growth).ok();
                if reservation.is_none() {
                    reservation_failed_for = Some(growth);
                }
                continue;
            }
            let incoming = reservation
                .take()
                .expect("matching ordered close reservation exists");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_ordered_unavailable_or_degraded(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                    Vec::new(),
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            state
                .ordered
                .as_mut()
                .expect("ordered channel owns ordered state")
                .reducer = next_reducer;
            refresh_ordered_instance_progress(&mut state, binding_id, fragment_instance_id);
            let outcome = match close_outcome {
                OrderedCloseOutcome::Duplicate => SubmitOutcome::Duplicate,
                OrderedCloseOutcome::PendingFinalSnapshot => SubmitOutcome::PendingFinalSnapshot,
                OrderedCloseOutcome::Satisfied => SubmitOutcome::Applied,
            };
            let locked = self.refresh_after_ordered_progress(&mut state, outcome, None, Vec::new());
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn close_partition(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let mut incoming_reservation: Option<RetainedMemoryReservation> = None;
        let mut reservation_failed = false;
        loop {
            let mut state = self.state.lock().unwrap();
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            if matches!(
                state.terminal,
                ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
            ) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            {
                let partition =
                    partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
                if let Some(previous) = partition.and_then(|partition| partition.terminal_sequence)
                {
                    if previous != terminal_sequence {
                        return Err(violation(
                            RuntimeContractViolationKind::ConflictingTerminalSequence,
                            "partition close replay changed terminal sequence",
                        ));
                    }
                    return Ok(progress(SubmitOutcome::Duplicate));
                }
            }
            if !matches!(state.terminal, ChannelTerminal::Collecting) {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
            if instance.progress == TerminalProgress::Impossible {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            let partition =
                partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
            if partition.is_some_and(|partition| {
                partition
                    .seen
                    .keys()
                    .next_back()
                    .is_some_and(|sequence| *sequence >= terminal_sequence)
            }) {
                return Err(violation(
                    RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                    "partition already contains a delta outside terminal range",
                ));
            }
            let projected_total = state
                .reservation
                .bytes()
                .checked_add(TERMINAL_METADATA_BYTES);
            if projected_total
                .and_then(|bytes| u64::try_from(bytes).ok())
                .is_none_or(|bytes| bytes > self.max_reducer_bytes)
            {
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming_reservation);
                return Ok(locked.finish());
            }
            if incoming_reservation
                .as_ref()
                .map(|reservation| reservation.bytes())
                != Some(TERMINAL_METADATA_BYTES)
            {
                if reservation_failed {
                    let locked = self.make_unavailable(
                        &mut state,
                        UnavailableReason::ResourceLimit,
                        SubmitOutcome::TerminalNoop,
                    );
                    drop(state);
                    return Ok(locked.finish());
                }
                drop(state);
                drop(incoming_reservation.take());
                match RetainedMemoryReservation::try_new(
                    self.memory_account.clone(),
                    TERMINAL_METADATA_BYTES,
                ) {
                    Ok(reservation) => incoming_reservation = Some(reservation),
                    Err(_) => reservation_failed = true,
                }
                continue;
            }
            let incoming = incoming_reservation
                .take()
                .expect("matching terminal reservation must exist");
            if let Err(failure) = state.reservation.absorb(incoming) {
                let (_, incoming) = failure.into_parts();
                let locked = self.make_unavailable(
                    &mut state,
                    UnavailableReason::ResourceLimit,
                    SubmitOutcome::TerminalNoop,
                );
                drop(state);
                drop(incoming);
                return Ok(locked.finish());
            }
            let partition = partition_mut_for_commit(
                &mut state,
                binding_id,
                fragment_instance_id,
                partition_id,
            );
            partition.terminal_sequence = Some(terminal_sequence);
            let outcome = if partition.is_gapless() {
                partition.progress = TerminalProgress::Satisfied;
                SubmitOutcome::Applied
            } else {
                SubmitOutcome::PendingGap
            };
            let mut events = Vec::new();
            if outcome == SubmitOutcome::PendingGap {
                events.push(RuntimeFilterEvent::SequenceGapObserved {
                    identity: ContributionIdentity::new(
                        self.event_identity.query_id(),
                        self.event_identity.participant_id(),
                        self.channel_id,
                        self.event_identity.epoch(),
                        ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
                        terminal_sequence,
                    ),
                });
            }
            let locked = self.refresh_after_progress(
                &mut state,
                binding_id,
                fragment_instance_id,
                outcome,
                events,
            );
            drop(state);
            return Ok(locked.finish());
        }
    }

    pub(crate) fn fail_instance(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        reason: ProducerFailureReason,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let mut state = self.state.lock().unwrap();
        instance_mut(&mut state, binding_id, fragment_instance_id)?;
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        if state.ordered.is_some() {
            let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
            if instance.progress != TerminalProgress::Pending {
                return Ok(progress(SubmitOutcome::TerminalNoop));
            }
            instance.progress = TerminalProgress::Impossible;
            let witness_id = state
                .producers
                .get(&binding_id)
                .expect("authorized ordered producer")
                .witness_id;
            state
                .ordered
                .as_mut()
                .expect("ordered channel owns ordered state")
                .availability_witnesses
                .get_mut(&witness_id)
                .expect("ordered availability witness is installed")
                .advance(WitnessProgress::Impossible);
            refresh_ordered_witness(&mut state, binding_id);
            let producer_identity =
                ProducerEventIdentity::new(self.event_identity, binding_id, fragment_instance_id);
            let locked = self.refresh_after_ordered_progress(
                &mut state,
                SubmitOutcome::CoverageStillPossible,
                None,
                vec![RuntimeFilterEvent::ProducerInstanceFailed {
                    identity: producer_identity,
                    reason,
                }],
            );
            drop(state);
            return Ok(locked.finish());
        }
        let instance = instance_mut(&mut state, binding_id, fragment_instance_id)?;
        if instance.progress != TerminalProgress::Pending {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        instance.progress = TerminalProgress::Impossible;
        let producer_identity =
            ProducerEventIdentity::new(self.event_identity, binding_id, fragment_instance_id);
        let locked = self.refresh_after_progress(
            &mut state,
            binding_id,
            fragment_instance_id,
            SubmitOutcome::Applied,
            vec![RuntimeFilterEvent::ProducerInstanceFailed {
                identity: producer_identity,
                reason,
            }],
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn expire_deadline(&self, now: Instant) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        if self.deadline.get().is_none_or(|deadline| now < *deadline)
            || !matches!(state.terminal, ChannelTerminal::Collecting)
        {
            return ChannelAction::None;
        }
        let locked = if state.ordered.is_some() {
            self.make_ordered_unavailable_or_degraded(
                &mut state,
                UnavailableReason::IncompleteCoverage,
                SubmitOutcome::TerminalNoop,
                Vec::new(),
            )
        } else {
            self.make_unavailable(
                &mut state,
                UnavailableReason::IncompleteCoverage,
                SubmitOutcome::TerminalNoop,
            )
        };
        drop(state);
        locked.finish()
    }

    pub(crate) fn cancel(&self) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return ChannelAction::None;
        }
        let release_after_unlock = self.detach_collecting_state(&mut state);
        let events = vec![RuntimeFilterEvent::ChannelCancelled {
            identity: self.event_identity,
        }];
        let order = next_dispatch_order(&mut state);
        state.terminal = ChannelTerminal::Cancelled {
            order,
            events: events.clone(),
        };
        let action = ChannelAction::Cancelled { order, events };
        drop(state);
        drop(release_after_unlock);
        action
    }

    pub(crate) fn terminal_action(&self) -> ChannelAction {
        let state = self.state.lock().unwrap();
        terminal_action_from_state(&state)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reject_submit_resource_exhausted(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: &ValueDomainDelta,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let Some(data_type) = self.data_type.as_ref() else {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "ordered channel cannot accept membership deltas",
            ));
        };
        let identity = ContributionIdentity::new(
            self.event_identity.query_id(),
            self.event_identity.participant_id(),
            self.channel_id,
            self.event_identity.epoch(),
            ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
            sequence,
        );
        let mut state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        if !delta.matches_data_type(data_type) {
            return Err(violation(
                RuntimeContractViolationKind::TypeMismatch,
                "delta type does not match channel membership type",
            ));
        }
        if matches!(
            state.terminal,
            ChannelTerminal::Unavailable { .. } | ChannelTerminal::Cancelled { .. }
        ) {
            return Ok(terminal_action_from_state(&state));
        }
        let fingerprint = delta.fingerprint();
        if let Some(previous) =
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?
                .and_then(|partition| partition.seen.get(&sequence))
        {
            return if *previous == fingerprint {
                Ok(ChannelAction::Progress {
                    order: Some(next_dispatch_order(&mut state)),
                    outcome: SubmitOutcome::Duplicate,
                    events: vec![RuntimeFilterEvent::DeltaDuplicateIgnored { identity }],
                })
            } else {
                Err(violation(
                    RuntimeContractViolationKind::ConflictingReplay,
                    "same contribution identity carried a different payload",
                ))
            };
        }
        let instance_progress =
            instance_mut(&mut state, binding_id, fragment_instance_id)?.progress;
        let (partition_progress, terminal_sequence) =
            partition_state(&state, binding_id, fragment_instance_id, partition_id)?
                .map_or((TerminalProgress::Pending, None), |partition| {
                    (partition.progress, partition.terminal_sequence)
                });
        if instance_progress == TerminalProgress::Impossible
            || partition_progress == TerminalProgress::Impossible
        {
            return Ok(progress(SubmitOutcome::TerminalNoop));
        }
        if partition_progress == TerminalProgress::Satisfied {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "new delta arrived after partition close",
            ));
        }
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(terminal_action_from_state(&state));
        }
        if terminal_sequence.is_some_and(|terminal| sequence >= terminal) {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "delta sequence is outside the exclusive terminal range",
            ));
        }
        let locked = self.make_unavailable(
            &mut state,
            UnavailableReason::ResourceLimit,
            SubmitOutcome::TerminalNoop,
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn reject_ordered_submit_resource_exhausted(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: &OrderedBoundUpdate,
    ) -> Result<ChannelAction, RuntimeContractViolation> {
        let stream_id = ProducerStreamId::new(binding_id, fragment_instance_id, partition_id);
        let mut state = self.state.lock().unwrap();
        partition_state(&state, binding_id, fragment_instance_id, partition_id)?;
        let ordered = state.ordered.as_ref().ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "membership channel cannot accept ordered bounds",
            )
        })?;
        ordered
            .reducer
            .validate_tombstone_update(stream_id, sequence, update)?;
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return Ok(terminal_action_from_state(&state));
        }
        let locked = self.make_ordered_unavailable_or_degraded(
            &mut state,
            UnavailableReason::ResourceLimit,
            SubmitOutcome::TerminalNoop,
            Vec::new(),
        );
        drop(state);
        Ok(locked.finish())
    }

    pub(crate) fn resource_exhausted(&self) -> ChannelAction {
        let mut state = self.state.lock().unwrap();
        if !matches!(state.terminal, ChannelTerminal::Collecting) {
            return terminal_action_from_state(&state);
        }
        let locked = if state.ordered.is_some() {
            self.make_ordered_unavailable_or_degraded(
                &mut state,
                UnavailableReason::ResourceLimit,
                SubmitOutcome::TerminalNoop,
                Vec::new(),
            )
        } else {
            self.make_unavailable(
                &mut state,
                UnavailableReason::ResourceLimit,
                SubmitOutcome::TerminalNoop,
            )
        };
        drop(state);
        locked.finish()
    }

    pub(crate) fn snapshot(&self) -> Option<Arc<LogicalSnapshot>> {
        let state = self.state.lock().unwrap();
        match &state.terminal {
            ChannelTerminal::Completed { snapshot, .. } => Some(snapshot.clone()),
            ChannelTerminal::DegradedLogical { snapshot, .. } => Some(snapshot.clone()),
            _ => state
                .ordered
                .as_ref()
                .and_then(|ordered| ordered.latest.clone()),
        }
    }

    pub(crate) fn availability_progress(&self) -> CoverageProgress {
        let state = self.state.lock().unwrap();
        evaluate(
            &self.availability_coverage,
            state
                .ordered
                .as_ref()
                .map_or(&state.witnesses, |ordered| &ordered.availability_witnesses),
        )
    }

    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(
            self.state.lock().unwrap().terminal,
            ChannelTerminal::Collecting
        )
    }

    fn refresh_after_progress(
        &self,
        state: &mut ChannelState,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        outcome: SubmitOutcome,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        let producer = state
            .producers
            .get_mut(&binding_id)
            .expect("authorized producer binding");
        let instance = producer
            .instances
            .get_mut(&fragment_instance_id)
            .expect("authorized producer instance");
        let was_pending = instance.progress == TerminalProgress::Pending;
        instance.refresh_satisfied();
        if was_pending && instance.progress == TerminalProgress::Satisfied {
            events.push(RuntimeFilterEvent::ProducerInstanceClosed {
                identity: ProducerEventIdentity::new(
                    self.event_identity,
                    binding_id,
                    fragment_instance_id,
                ),
            });
        }
        let witness_progress = if producer
            .instances
            .values()
            .any(|instance| instance.progress == TerminalProgress::Impossible)
        {
            WitnessProgress::Impossible
        } else if producer
            .instances
            .values()
            .all(|instance| instance.progress == TerminalProgress::Satisfied)
        {
            WitnessProgress::Satisfied
        } else {
            WitnessProgress::Pending
        };
        state
            .witnesses
            .get_mut(&producer.witness_id)
            .expect("installed producer witness")
            .advance(witness_progress);

        match evaluate(&self.terminal_coverage, &state.witnesses) {
            CoverageProgress::Satisfied => {
                let replacement = MembershipReducer::try_new(
                    self.data_type
                        .clone()
                        .expect("membership channel owns its data type"),
                    self.null_semantics
                        .expect("membership channel owns null semantics"),
                )
                .expect("validated membership type");
                let domain = std::mem::replace(
                    state
                        .reducer
                        .as_mut()
                        .expect("membership channel owns a membership reducer"),
                    replacement,
                )
                .into_domain();
                let reservation =
                    std::mem::replace(&mut state.reservation, RetainedMemoryReservation::empty());
                let snapshot =
                    Arc::new(LogicalSnapshot::first(self.channel_id, domain, reservation));
                events.push(RuntimeFilterEvent::ChannelCompleted {
                    identity: self.event_identity,
                    version: snapshot.version(),
                });
                let order = next_dispatch_order(state);
                state.terminal = ChannelTerminal::Completed {
                    order,
                    snapshot: snapshot.clone(),
                    events: events.clone(),
                };
                LockedAction::without_release(ChannelAction::Completed {
                    order,
                    outcome: SubmitOutcome::Completed,
                    snapshot,
                    events,
                })
            }
            CoverageProgress::Impossible => self.make_unavailable_with_events(
                state,
                UnavailableReason::ProducerFailed,
                outcome,
                events,
            ),
            CoverageProgress::Pending => {
                let order = (!events.is_empty()).then(|| next_dispatch_order(state));
                LockedAction::without_release(ChannelAction::Progress {
                    order,
                    outcome,
                    events,
                })
            }
        }
    }

    fn refresh_after_ordered_progress(
        &self,
        state: &mut ChannelState,
        outcome: SubmitOutcome,
        published: Option<Arc<LogicalSnapshot>>,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        match evaluate(&self.terminal_coverage, &state.witnesses) {
            CoverageProgress::Satisfied => {
                if let Some(snapshot) = state
                    .ordered
                    .as_ref()
                    .and_then(|ordered| ordered.latest.clone())
                {
                    events.push(RuntimeFilterEvent::ChannelCompleted {
                        identity: self.event_identity,
                        version: snapshot.version(),
                    });
                    let order = next_dispatch_order(state);
                    state.terminal = ChannelTerminal::Completed {
                        order,
                        snapshot: snapshot.clone(),
                        events: events.clone(),
                    };
                    LockedAction::without_release(ChannelAction::Completed {
                        order,
                        outcome: SubmitOutcome::Completed,
                        snapshot,
                        events,
                    })
                } else {
                    events.push(RuntimeFilterEvent::ChannelCompletedWithoutArtifact {
                        identity: self.event_identity,
                    });
                    let order = next_dispatch_order(state);
                    state.terminal = ChannelTerminal::CompletedWithoutArtifact {
                        order,
                        events: events.clone(),
                    };
                    LockedAction::without_release(ChannelAction::CompletedWithoutArtifact {
                        order,
                        outcome: SubmitOutcome::CompletedWithoutArtifact,
                        events,
                    })
                }
            }
            CoverageProgress::Impossible => self.make_ordered_unavailable_or_degraded(
                state,
                UnavailableReason::ProducerFailed,
                outcome,
                events,
            ),
            CoverageProgress::Pending => {
                if let Some(snapshot) = published {
                    let order = next_dispatch_order(state);
                    LockedAction::without_release(ChannelAction::VisibleSnapshot {
                        order,
                        outcome,
                        version: snapshot.version(),
                        snapshot,
                        events,
                    })
                } else {
                    let order = (!events.is_empty()).then(|| next_dispatch_order(state));
                    LockedAction::without_release(ChannelAction::Progress {
                        order,
                        outcome,
                        events,
                    })
                }
            }
        }
    }

    fn make_ordered_unavailable_or_degraded(
        &self,
        state: &mut ChannelState,
        reason: UnavailableReason,
        outcome: SubmitOutcome,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        if let Some(snapshot) = state
            .ordered
            .as_ref()
            .and_then(|ordered| ordered.latest.clone())
        {
            events.push(RuntimeFilterEvent::ChannelLogicalDegraded {
                identity: self.event_identity,
                reason,
                retained_version: snapshot.version(),
            });
            let order = next_dispatch_order(state);
            state.terminal = ChannelTerminal::DegradedLogical {
                order,
                reason,
                snapshot: snapshot.clone(),
                events: events.clone(),
            };
            LockedAction::without_release(ChannelAction::DegradedLogical {
                order,
                outcome,
                reason,
                snapshot,
                events,
            })
        } else {
            self.make_unavailable_with_events(state, reason, outcome, events)
        }
    }

    fn make_unavailable(
        &self,
        state: &mut ChannelState,
        reason: UnavailableReason,
        outcome: SubmitOutcome,
    ) -> LockedAction {
        self.make_unavailable_with_events(state, reason, outcome, Vec::new())
    }

    fn make_unavailable_with_events(
        &self,
        state: &mut ChannelState,
        reason: UnavailableReason,
        outcome: SubmitOutcome,
        mut events: Vec<RuntimeFilterEvent>,
    ) -> LockedAction {
        let release_after_unlock = self.detach_collecting_state(state);
        events.push(RuntimeFilterEvent::ChannelUnavailable {
            identity: self.event_identity,
            reason,
        });
        let order = next_dispatch_order(state);
        state.terminal = ChannelTerminal::Unavailable {
            order,
            reason,
            events: events.clone(),
        };
        LockedAction {
            action: ChannelAction::Unavailable {
                order,
                outcome,
                reason,
                events,
            },
            release_after_unlock: Some(release_after_unlock),
        }
    }

    fn detach_collecting_state(&self, state: &mut ChannelState) -> RetainedMemoryReservation {
        let mut reservation =
            std::mem::replace(&mut state.reservation, RetainedMemoryReservation::empty());
        if self.data_type.is_some() {
            state.reducer = Some(
                MembershipReducer::try_new(
                    self.data_type
                        .clone()
                        .expect("membership channel owns its data type"),
                    self.null_semantics
                        .expect("membership channel owns null semantics"),
                )
                .expect("validated membership type"),
            );
        } else if let Some(ordered) = state.ordered.as_mut() {
            let tombstone_bytes = ordered
                .reducer
                .retain_protocol_tombstones()
                .expect("accounted ordered tombstone size remains representable");
            let released = reservation.split_off_excess(tombstone_bytes);
            state.reservation = reservation;
            reservation = released;
        }
        for producer in state.producers.values_mut() {
            for instance in producer.instances.values_mut() {
                instance.clear_partitions();
            }
        }
        reservation
    }
}

fn progress(outcome: SubmitOutcome) -> ChannelAction {
    ChannelAction::Progress {
        order: None,
        outcome,
        events: Vec::new(),
    }
}

fn refresh_ordered_instance_progress(
    state: &mut ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) {
    let terminal_count = state
        .ordered
        .as_ref()
        .expect("ordered channel owns ordered state")
        .reducer
        .terminal_partition_count(binding_id, fragment_instance_id);
    let instance = instance_mut(state, binding_id, fragment_instance_id)
        .expect("authorized ordered producer instance");
    if instance.progress == TerminalProgress::Pending
        && instance
            .local_partition_count()
            .is_some_and(|count| usize::try_from(count) == Ok(terminal_count))
    {
        instance.progress = TerminalProgress::Satisfied;
    }
    refresh_ordered_witness(state, binding_id);
}

fn refresh_ordered_witness(state: &mut ChannelState, binding_id: BindingId) {
    let producer = state
        .producers
        .get(&binding_id)
        .expect("authorized ordered producer");
    let progress = if producer
        .instances
        .values()
        .any(|instance| instance.progress == TerminalProgress::Impossible)
    {
        WitnessProgress::Impossible
    } else if producer
        .instances
        .values()
        .all(|instance| instance.progress == TerminalProgress::Satisfied)
    {
        WitnessProgress::Satisfied
    } else {
        WitnessProgress::Pending
    };
    state
        .witnesses
        .get_mut(&producer.witness_id)
        .expect("installed ordered terminal witness")
        .advance(progress);
}

fn terminal_action_from_state(state: &ChannelState) -> ChannelAction {
    match &state.terminal {
        ChannelTerminal::Collecting => ChannelAction::None,
        ChannelTerminal::Completed {
            order,
            snapshot,
            events,
        } => ChannelAction::Completed {
            order: *order,
            outcome: SubmitOutcome::TerminalNoop,
            snapshot: snapshot.clone(),
            events: events.clone(),
        },
        ChannelTerminal::Unavailable {
            order,
            reason,
            events,
        } => ChannelAction::Unavailable {
            order: *order,
            outcome: SubmitOutcome::TerminalNoop,
            reason: *reason,
            events: events.clone(),
        },
        ChannelTerminal::CompletedWithoutArtifact { order, events } => {
            ChannelAction::CompletedWithoutArtifact {
                order: *order,
                outcome: SubmitOutcome::TerminalNoop,
                events: events.clone(),
            }
        }
        ChannelTerminal::DegradedLogical {
            order,
            reason,
            snapshot,
            events,
        } => ChannelAction::DegradedLogical {
            order: *order,
            outcome: SubmitOutcome::TerminalNoop,
            reason: *reason,
            snapshot: snapshot.clone(),
            events: events.clone(),
        },
        ChannelTerminal::Cancelled { order, events } => ChannelAction::Cancelled {
            order: *order,
            events: events.clone(),
        },
    }
}

fn next_dispatch_order(state: &mut ChannelState) -> u64 {
    let order = state.next_dispatch_order;
    state.next_dispatch_order = state
        .next_dispatch_order
        .checked_add(1)
        .expect("runtime filter channel dispatch order exhausted");
    order
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

fn instance_mut(
    state: &mut ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) -> Result<&mut InstanceState, RuntimeContractViolation> {
    let producer = state.producers.get_mut(&binding_id).ok_or_else(|| {
        violation(
            RuntimeContractViolationKind::UnauthorizedBinding,
            "producer binding is not installed for this channel",
        )
    })?;
    producer
        .instances
        .get_mut(&fragment_instance_id)
        .ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is not installed for this binding",
            )
        })
}

fn instance_ref(
    state: &ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
) -> Result<&InstanceState, RuntimeContractViolation> {
    let producer = state.producers.get(&binding_id).ok_or_else(|| {
        violation(
            RuntimeContractViolationKind::UnauthorizedBinding,
            "producer binding is not installed for this channel",
        )
    })?;
    producer
        .instances
        .get(&fragment_instance_id)
        .ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is not installed for this binding",
            )
        })
}

fn partition_state(
    state: &ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
) -> Result<Option<&super::state::PartitionState>, RuntimeContractViolation> {
    let instance = instance_ref(state, binding_id, fragment_instance_id)?;
    let count = instance.local_partition_count().ok_or_else(|| {
        violation(
            RuntimeContractViolationKind::InvalidPartitionCount,
            "producer instance must be opened before mutation",
        )
    })?;
    if partition_id.get() >= count {
        return Err(violation(
            RuntimeContractViolationKind::InvalidPartition,
            "partition is outside the opened local partition range",
        ));
    }
    Ok(instance.partition(partition_id))
}

fn partition_mut_for_commit(
    state: &mut ChannelState,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
) -> &mut super::state::PartitionState {
    instance_mut(state, binding_id, fragment_instance_id)
        .expect("authorized producer instance must remain installed")
        .partition_mut_for_commit(partition_id)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::events::RuntimeFilterEvent;
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::install::*;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, comparator_digest_for_test,
    };
    use crate::runtime_filter::port::producer::{
        ProducerFailureReason, RuntimeContractViolation, RuntimeContractViolationKind,
        SubmitOutcome,
    };
    use crate::runtime_filter::port::subscription::UnavailableReason;
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterMemoryAccount, TemporaryContributionLease,
    };
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ValueDomainDelta,
    };

    use super::{ChannelAction, RuntimeFilterChannel};

    #[derive(Default)]
    struct Account {
        current: AtomicUsize,
        peak: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for Account {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            let current = self.current.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.peak.fetch_max(current, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.current.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct ReentrantAccount {
        current: AtomicUsize,
        channel: Mutex<Option<Weak<RuntimeFilterChannel>>>,
    }

    impl ReentrantAccount {
        fn reenter(&self) {
            let channel = self
                .channel
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade);
            if let Some(channel) = channel {
                let _ = channel.snapshot();
                let _ = channel.is_terminal();
            }
        }
    }

    impl RuntimeFilterMemoryAccount for ReentrantAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.current.fetch_add(bytes, Ordering::SeqCst);
            self.reenter();
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.current.fetch_sub(bytes, Ordering::SeqCst);
            self.reenter();
        }
    }

    struct RejectingAccount;

    impl RuntimeFilterMemoryAccount for RejectingAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    struct BlockingRejectingAccount {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl RuntimeFilterMemoryAccount for BlockingRejectingAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId { hi: 1, lo }
    }

    fn deployment_with_coverages(
        availability_coverage: Coverage,
        terminal_coverage: Coverage,
        producers: &[(u32, u32, i64)],
        budget: u64,
        max: u64,
    ) -> RuntimeFilterChannelDeployment {
        let producers = producers
            .iter()
            .map(|(binding, witness, instance)| {
                (
                    BindingId::new(*binding),
                    ProducerDeployment::new(
                        CoverageWitnessId::new(*witness),
                        BTreeSet::from([uid(*instance)]),
                    ),
                )
            })
            .collect();
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage,
            terminal_coverage,
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: max,
                max_artifact_bytes: 1,
                deadline_ms: 10,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(budget),
            crate::runtime_filter::port::install::MaterializationPolicy::for_test(),
            producers,
            BTreeMap::new(),
        )
    }

    fn deployment(
        coverage: Coverage,
        producers: &[(u32, u32, i64)],
        budget: u64,
        max: u64,
    ) -> RuntimeFilterChannelDeployment {
        deployment_with_coverages(coverage.clone(), coverage, producers, budget, max)
    }

    fn channel_with(
        coverage: Coverage,
        producers: &[(u32, u32, i64)],
        budget: u64,
        max: u64,
    ) -> (RuntimeFilterChannel, Arc<Account>, Instant) {
        let account = Arc::new(Account::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        let channel = RuntimeFilterChannel::new(
            uid(99),
            RuntimeFilterParticipantId::new(1),
            DeploymentEpoch::new(1),
            &deployment(coverage, producers, budget, max),
            deadline,
            account.clone(),
        )
        .unwrap();
        (channel, account, deadline)
    }

    fn channel_from(
        deployment: RuntimeFilterChannelDeployment,
    ) -> (RuntimeFilterChannel, Arc<Account>, Instant) {
        let account = Arc::new(Account::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        let channel = RuntimeFilterChannel::new(
            uid(99),
            RuntimeFilterParticipantId::new(1),
            DeploymentEpoch::new(1),
            &deployment,
            deadline,
            account.clone(),
        )
        .unwrap();
        (channel, account, deadline)
    }

    fn one_channel() -> (RuntimeFilterChannel, Arc<Account>, Instant) {
        channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            4096,
            4096,
        )
    }

    fn multi_instance_deployment() -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(1);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 1,
                deadline_ms: 10,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(4096),
            crate::runtime_filter::port::install::MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10), uid(11)])),
            )]),
            BTreeMap::new(),
        )
    }

    fn submit(
        channel: &RuntimeFilterChannel,
        account: Arc<Account>,
        binding: u32,
        instance: i64,
        sequence: u64,
        values: &[i64],
    ) -> Result<ChannelAction, crate::runtime_filter::port::producer::RuntimeContractViolation>
    {
        let delta = ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        channel.submit(
            BindingId::new(binding),
            uid(instance),
            PartitionId::new(0),
            ProducerSequence::new(sequence),
            delta,
            TemporaryContributionLease::new(account, bytes),
        )
    }

    #[test]
    fn complete_once_exposes_no_snapshot_before_terminal_coverage() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account, 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Applied
        );
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn complete_once_tracks_availability_without_publishing() {
        let terminal_coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, account, _) = channel_from(deployment_with_coverages(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage,
            &[(10, 1, 10), (20, 2, 20)],
            4096,
            4096,
        ));
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .open_producer(BindingId::new(20), uid(20), 1)
            .unwrap();
        submit(&channel, account, 10, 10, 0, &[1]).unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            channel.availability_progress(),
            crate::runtime_filter::core::coverage::CoverageProgress::Satisfied
        );
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn complete_once_completes_with_union_and_final_version_one() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 1, &[2]).unwrap();
        submit(&channel, account, 10, 10, 0, &[1]).unwrap();
        let action = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(2),
            )
            .unwrap();
        let snapshot = action.snapshot().unwrap();
        assert_eq!(snapshot.version(), LogicalVersion::FIRST);
        assert_eq!(snapshot.domain().values(), &MembershipValues::int64([1, 2]));
    }

    #[test]
    fn complete_once_empty_union_is_valid_completed_domain() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let action = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(action.snapshot().unwrap().domain().values().is_empty());
    }

    #[test]
    fn expected_instances_must_open_and_all_close_before_completion() {
        let (channel, _, _) = channel_from(multi_instance_deployment());
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(channel.snapshot().is_none());
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(11), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(11),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(channel.snapshot().is_some());
    }

    #[test]
    fn max_partition_count_open_stays_sparse_and_rejects_boundary() {
        let (channel, account, _) = one_channel();
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), u32::MAX)
                .unwrap(),
            SubmitOutcome::Applied
        );
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(u32::MAX),
                    ProducerSequence::new(0),
                    delta,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(u32::MAX - 1),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn open_is_idempotent_but_conflicts_and_unauthorized_coordinates_fail_first() {
        let (channel, account, _) = one_channel();
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(99), uid(10), 1)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(99), 1)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(1),
                    ProducerSequence::new(0),
                    delta,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn contribution_lease_must_match_canonical_payload_size() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([1]), false),
                    TemporaryContributionLease::new(account.clone(), 0),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidContributionLease
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn value_domain_union_deduplicates_exact_replay_and_rejects_conflict_after_completion() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            submit(&channel, account, 10, 10, 0, &[2])
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
    }

    #[test]
    fn type_mismatch_precedes_replay_and_completed_losing_stream_terminal_noop() {
        let coverage = Coverage::AnyOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, account, _) = channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .open_producer(BindingId::new(20), uid(20), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();

        let mismatched_replay = ValueDomainDelta::new(MembershipValues::int32([1]), false);
        let bytes = mismatched_replay.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    mismatched_replay,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );

        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        let mismatched_loser = ValueDomainDelta::new(MembershipValues::int32([2]), false);
        let bytes = mismatched_loser.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(20),
                    uid(20),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    mismatched_loser,
                    TemporaryContributionLease::new(account, bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );
    }

    #[test]
    fn concurrent_duplicate_submits_reduce_exactly_once() {
        let (channel, account, _) = one_channel();
        let channel = Arc::new(channel);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let channel = channel.clone();
            let account = account.clone();
            let barrier = barrier.clone();
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                submit(&channel, account, 10, 10, 0, &[1])
                    .unwrap()
                    .outcome()
            }));
        }
        barrier.wait();
        let mut outcomes = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>();
        outcomes.sort_by_key(|outcome| match outcome {
            SubmitOutcome::Applied => 0,
            SubmitOutcome::Duplicate => 1,
            _ => 2,
        });
        assert_eq!(
            outcomes,
            vec![SubmitOutcome::Applied, SubmitOutcome::Duplicate]
        );
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            channel.snapshot().unwrap().domain().values(),
            &MembershipValues::int64([1])
        );
    }

    #[test]
    fn unseen_sequence_order_and_exact_replay_do_not_change_complete_domain() {
        let orders = [
            [0_u64, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for order in orders {
            let (channel, account, _) = one_channel();
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            for sequence in order {
                let values = [i64::try_from(sequence).unwrap() + 10];
                assert_eq!(
                    submit(&channel, account.clone(), 10, 10, sequence, &values)
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::Applied
                );
                assert_eq!(
                    submit(&channel, account.clone(), 10, 10, sequence, &values)
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::Duplicate
                );
            }
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(3),
                )
                .unwrap();
            assert_eq!(
                channel.snapshot().unwrap().domain().values(),
                &MembershipValues::int64([10, 11, 12]),
                "order={order:?}"
            );
        }
    }

    #[test]
    fn close_waits_for_every_sequence_below_terminal_and_rejects_seen_outside() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 2, &[3]).unwrap();
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(2)
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        let pending = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(3),
            )
            .unwrap();
        assert_eq!(pending.outcome(), SubmitOutcome::PendingGap);
        assert!(
            pending
                .events()
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::SequenceGapObserved { .. }))
        );
        submit(&channel, account, 10, 10, 1, &[2]).unwrap();
        assert!(channel.snapshot().is_some());
    }

    #[test]
    fn any_of_replica_failure_does_not_override_remaining_replica() {
        let coverage = Coverage::AnyOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, _, _) = channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .open_producer(BindingId::new(20), uid(20), 1)
            .unwrap();
        assert_eq!(
            channel
                .fail_instance(
                    BindingId::new(10),
                    uid(10),
                    ProducerFailureReason::ExecutionFailed
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::Applied
        );
        assert!(!channel.is_terminal());
    }

    #[test]
    fn all_of_required_instance_failure_becomes_unavailable() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let action = channel
            .fail_instance(
                BindingId::new(10),
                uid(10),
                ProducerFailureReason::ExecutionFailed,
            )
            .unwrap();
        assert_eq!(
            action.unavailable_reason(),
            Some(UnavailableReason::ProducerFailed)
        );
    }

    #[test]
    fn close_before_fail_keeps_instance_satisfied() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert_eq!(
            channel
                .fail_instance(
                    BindingId::new(10),
                    uid(10),
                    ProducerFailureReason::ExecutionFailed
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::TerminalNoop
        );
        assert!(channel.snapshot().is_some());
    }

    #[test]
    fn fail_before_close_keeps_instance_impossible() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .fail_instance(
                BindingId::new(10),
                uid(10),
                ProducerFailureReason::ExecutionFailed,
            )
            .unwrap();
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0)
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(
            channel
                .snapshot()
                .map(|snapshot| snapshot.domain().values().clone()),
            None
        );
    }

    #[test]
    fn concurrent_close_fail_race_has_one_irreversible_terminal_result() {
        let (channel, _, _) = one_channel();
        let channel = Arc::new(channel);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let close = {
            let channel = channel.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                channel
                    .close_partition(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    )
                    .unwrap()
            })
        };
        let fail = {
            let channel = channel.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                channel
                    .fail_instance(
                        BindingId::new(10),
                        uid(10),
                        ProducerFailureReason::ExecutionFailed,
                    )
                    .unwrap()
            })
        };
        barrier.wait();
        let actions = [close.join().unwrap(), fail.join().unwrap()];
        assert_eq!(
            actions
                .iter()
                .filter(|action| action.outcome() != SubmitOutcome::TerminalNoop)
                .count(),
            1
        );
        assert!(channel.is_terminal());
    }

    #[test]
    fn completed_channel_never_changes_version_or_domain() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            submit(&channel, account, 10, 10, 1, &[2])
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
        assert_eq!(
            channel.snapshot().unwrap().domain().values(),
            &MembershipValues::int64([1])
        );
    }

    #[test]
    fn resource_limits_are_unavailable_not_empty_domain() {
        let (channel, account, _) = channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            4096,
            1,
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
        assert_eq!(account.current.load(Ordering::SeqCst), 0);

        let (channel, account, _) = channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            1,
            4096,
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
        assert_eq!(account.current.load(Ordering::SeqCst), 0);

        let (channel, account, _) = channel_with(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            1,
            4096,
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                )
                .unwrap()
                .unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn hard_deadline_and_cancel_are_irreversible() {
        let (channel, account, deadline) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        assert!(account.current.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            channel.expire_deadline(deadline - Duration::from_nanos(1)),
            ChannelAction::None
        ));
        assert_eq!(
            channel.expire_deadline(deadline).unavailable_reason(),
            Some(UnavailableReason::IncompleteCoverage)
        );
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 1, &[2])
                .unwrap()
                .outcome(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(account.current.load(Ordering::SeqCst), 0);

        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
        assert!(matches!(channel.cancel(), ChannelAction::None));
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unavailable_and_cancelled_tombstones_discard_replay_metadata() {
        for cancel in [false, true] {
            let coverage = Coverage::AllOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(1)),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]);
            let (channel, account, deadline) =
                channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap();
            submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap();
            assert!(account.current.load(Ordering::SeqCst) > 0);
            if cancel {
                assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
            } else {
                assert_eq!(
                    channel.expire_deadline(deadline).unavailable_reason(),
                    Some(UnavailableReason::IncompleteCoverage)
                );
            }
            assert_eq!(account.current.load(Ordering::SeqCst), 0);

            for values in [[1_i64], [2_i64]] {
                assert_eq!(
                    submit(&channel, account.clone(), 10, 10, 0, &values)
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::TerminalNoop
                );
            }
            for terminal in [1_u64, 2_u64] {
                assert_eq!(
                    channel
                        .close_partition(
                            BindingId::new(10),
                            uid(10),
                            PartitionId::new(0),
                            ProducerSequence::new(terminal),
                        )
                        .unwrap()
                        .outcome(),
                    SubmitOutcome::TerminalNoop
                );
            }
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn completed_tombstone_retains_delta_and_close_replay_contract() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap();
        assert_eq!(
            submit(&channel, account.clone(), 10, 10, 0, &[1])
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            submit(&channel, account, 10, 10, 0, &[2])
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .unwrap()
                .outcome(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            channel
                .close_partition(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(2),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingTerminalSequence
        );
    }

    #[test]
    fn terminal_reopen_checks_existing_count_before_terminal_noop() {
        let (channel, _, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            channel
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );

        let coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (unavailable, _, deadline) =
            channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        unavailable
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        unavailable.expire_deadline(deadline);
        assert_eq!(
            unavailable
                .open_producer(BindingId::new(10), uid(10), 1)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(
            unavailable
                .open_producer(BindingId::new(10), uid(10), 2)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );

        let coverage = Coverage::AnyOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        let (channel, _, _) = channel_with(coverage, &[(10, 1, 10), (20, 2, 20)], 4096, 4096);
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap();
        assert_eq!(
            channel
                .open_producer(BindingId::new(20), uid(20), 1)
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
    }

    #[test]
    fn temporary_and_retained_memory_have_distinct_lifetimes() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        let retained = account.current.load(Ordering::SeqCst);
        assert!(retained > 0);
        assert!(account.peak.load(Ordering::SeqCst) > retained);
        let snapshot = channel
            .close_partition(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(1),
            )
            .unwrap()
            .snapshot()
            .unwrap();
        let completed_retained = account.current.load(Ordering::SeqCst);
        assert!(completed_retained > retained);
        drop(channel);
        assert_eq!(account.current.load(Ordering::SeqCst), completed_retained);
        drop(snapshot);
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_memory_account_rejection_fails_open_as_resource_limit() {
        let deadline = Instant::now() + Duration::from_secs(10);
        let channel = RuntimeFilterChannel::new(
            uid(99),
            RuntimeFilterParticipantId::new(1),
            DeploymentEpoch::new(1),
            &deployment(
                Coverage::Leaf(CoverageWitnessId::new(1)),
                &[(10, 1, 10)],
                4096,
                4096,
            ),
            deadline,
            Arc::new(RejectingAccount),
        )
        .unwrap();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let temporary = Arc::new(Account::default());
        let action = submit(&channel, temporary, 10, 10, 0, &[1]).unwrap();
        assert_eq!(
            action.unavailable_reason(),
            Some(UnavailableReason::ResourceLimit)
        );
        assert!(channel.snapshot().is_none());
    }

    #[test]
    fn rejected_reservation_revalidates_terminal_state_before_resource_limit() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let account = Arc::new(BlockingRejectingAccount {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let channel = Arc::new(
            RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment(
                    Coverage::Leaf(CoverageWitnessId::new(1)),
                    &[(10, 1, 10)],
                    4096,
                    4096,
                ),
                Instant::now() + Duration::from_secs(10),
                account,
            )
            .unwrap(),
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let submit_channel = channel.clone();
        let temporary = Arc::new(Account::default());
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            done_tx
                .send(submit(&submit_channel, temporary, 10, 10, 0, &[1]))
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(channel.cancel(), ChannelAction::Cancelled { .. }));
        release_tx.send(()).unwrap();
        let action = done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(action.outcome(), SubmitOutcome::TerminalNoop);
        assert!(matches!(
            channel.terminal_action(),
            ChannelAction::Cancelled { .. }
        ));
    }

    #[test]
    fn memory_account_callbacks_reenter_channel_without_deadlock() {
        let account = Arc::new(ReentrantAccount::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        let deployment = deployment(
            Coverage::Leaf(CoverageWitnessId::new(1)),
            &[(10, 1, 10)],
            4096,
            4096,
        );
        let channel = Arc::new(
            RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment,
                deadline,
                account.clone(),
            )
            .unwrap(),
        );
        *account.channel.lock().unwrap() = Some(Arc::downgrade(&channel));
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
            let bytes = delta.estimated_contribution_bytes().unwrap();
            let result = channel.submit(
                BindingId::new(10),
                uid(10),
                PartitionId::new(0),
                ProducerSequence::new(0),
                delta,
                TemporaryContributionLease::new(account.clone(), bytes),
            );
            if result.is_ok() {
                let _ = channel.cancel();
            }
            done_tx
                .send((result.is_ok(), account.current.load(Ordering::SeqCst)))
                .unwrap();
        });

        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            (true, 0)
        );
    }

    #[test]
    fn temporary_lease_drops_on_duplicate_conflict_and_type_error() {
        let (channel, account, _) = one_channel();
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        let retained = account.current.load(Ordering::SeqCst);
        submit(&channel, account.clone(), 10, 10, 0, &[1]).unwrap();
        assert_eq!(account.current.load(Ordering::SeqCst), retained);
        submit(&channel, account.clone(), 10, 10, 0, &[2]).unwrap_err();
        assert_eq!(account.current.load(Ordering::SeqCst), retained);

        let delta = ValueDomainDelta::new(MembershipValues::int32([1]), false);
        let bytes = delta.estimated_contribution_bytes().unwrap();
        assert_eq!(
            channel
                .submit(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    delta,
                    TemporaryContributionLease::new(account.clone(), bytes),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::TypeMismatch
        );
        assert_eq!(account.current.load(Ordering::SeqCst), retained);
    }

    #[derive(Debug, Eq, PartialEq)]
    enum TestAction {
        Published(u64, i64),
        SequenceAdvancedEqual,
        StreamAcceptedNoGlobalChange,
        PendingFinalSnapshot,
        CoverageStillPossible,
        Completed(Option<(u64, i64)>),
        CompletedWithoutArtifact,
        Other(SubmitOutcome),
    }

    fn ordered_value(snapshot: &LogicalSnapshot) -> i64 {
        let Some(OrderedScalar::Int64(value)) = snapshot
            .ordered_bound()
            .expect("ordered snapshot")
            .bound()
            .values()
            .first()
            .and_then(Option::as_ref)
        else {
            panic!("test ordered snapshot contains one int64")
        };
        *value
    }

    fn test_action(action: ChannelAction) -> TestAction {
        match action {
            ChannelAction::VisibleSnapshot {
                version, snapshot, ..
            } => TestAction::Published(version.get(), ordered_value(&snapshot)),
            ChannelAction::Completed { snapshot, .. } => {
                TestAction::Completed(Some((snapshot.version().get(), ordered_value(&snapshot))))
            }
            ChannelAction::CompletedWithoutArtifact { .. } => TestAction::CompletedWithoutArtifact,
            action => match action.outcome() {
                SubmitOutcome::SequenceAdvancedEqual => TestAction::SequenceAdvancedEqual,
                SubmitOutcome::StreamAcceptedNoGlobalChange => {
                    TestAction::StreamAcceptedNoGlobalChange
                }
                SubmitOutcome::PendingFinalSnapshot => TestAction::PendingFinalSnapshot,
                SubmitOutcome::CoverageStillPossible => TestAction::CoverageStillPossible,
                outcome => TestAction::Other(outcome),
            },
        }
    }

    fn int_bound(value: i64) -> i64 {
        value
    }

    struct OrderedChannelHarness {
        channel: Arc<RuntimeFilterChannel>,
        contract: Arc<RuntimeOrderContract>,
        streams: Vec<(BindingId, UniqueId)>,
        temporary_account: Arc<Account>,
    }

    impl OrderedChannelHarness {
        fn with_streams(count: usize) -> Self {
            Self::with_streams_and_limits(count, 1024, 4096, Arc::new(Account::default()))
        }

        fn with_streams_and_limits(
            count: usize,
            max_contribution_bytes: u64,
            max_reducer_bytes: u64,
            memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Self {
            Self::with_streams_coverage_and_limits(
                count,
                true,
                max_contribution_bytes,
                max_reducer_bytes,
                memory_account,
            )
        }

        fn with_streams_coverage_and_limits(
            count: usize,
            any_of: bool,
            max_contribution_bytes: u64,
            max_reducer_bytes: u64,
            memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        ) -> Self {
            let keys = vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }];
            let plan = OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            };
            let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
            let streams = (0..count)
                .map(|index| {
                    (
                        BindingId::new(10 + u32::try_from(index).unwrap()),
                        uid(10 + i64::try_from(index).unwrap()),
                    )
                })
                .collect::<Vec<_>>();
            let witnesses = (0..count)
                .map(|index| CoverageWitnessId::new(1 + u32::try_from(index).unwrap()))
                .collect::<Vec<_>>();
            let coverage_children = witnesses.iter().copied().map(Coverage::Leaf).collect();
            let coverage = if any_of {
                Coverage::AnyOf(coverage_children)
            } else {
                Coverage::AllOf(coverage_children)
            };
            let deployment = RuntimeFilterChannelDeployment::new(
                ChannelId::new(1),
                RuntimeFilterLogicalDomain::OrderedBound(plan),
                RuntimeFilterLifecycle::MonotonicUpdates,
                coverage.clone(),
                coverage,
                ReductionRequirement::TightenOrderedBound,
                BTreeSet::from([
                    ContributionKind::OrderedBoundUpdate,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes,
                    max_artifact_bytes: 1024,
                    deadline_ms: 100,
                    max_retries: 0,
                },
                RuntimeFilterCoreBudget::new(max_reducer_bytes),
                MaterializationPolicy::for_test(),
                streams
                    .iter()
                    .zip(&witnesses)
                    .map(|((binding, instance), witness)| {
                        (
                            *binding,
                            ProducerDeployment::new(*witness, BTreeSet::from([*instance])),
                        )
                    })
                    .collect(),
                BTreeMap::new(),
            );
            let channel = Arc::new(
                RuntimeFilterChannel::new(
                    uid(99),
                    RuntimeFilterParticipantId::new(1),
                    DeploymentEpoch::new(1),
                    &deployment,
                    Instant::now() + Duration::from_secs(10),
                    memory_account,
                )
                .unwrap(),
            );
            for (binding, instance) in &streams {
                channel.open_producer(*binding, *instance, 1).unwrap();
            }
            Self {
                channel,
                contract,
                streams,
                temporary_account: Arc::new(Account::default()),
            }
        }

        fn single_stream_anyof() -> Self {
            Self::with_streams(1)
        }

        fn two_stream_anyof() -> Self {
            Self::with_streams(2)
        }

        fn submit(
            &self,
            stream: usize,
            sequence: u64,
            value: i64,
        ) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            let tuple =
                OrderedTuple::try_new(&self.contract, [Some(OrderedScalar::Int64(value))]).unwrap();
            let update = OrderedBoundUpdate::new(&self.contract, tuple).unwrap();
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            self.channel
                .submit_ordered(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(sequence),
                    update,
                    TemporaryContributionLease::new(
                        self.temporary_account.clone(),
                        contribution_bytes,
                    ),
                )
                .map(test_action)
        }

        fn close(
            &self,
            stream: usize,
            terminal: u64,
        ) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            self.channel
                .close_ordered_partition(
                    binding,
                    instance,
                    PartitionId::new(0),
                    ProducerSequence::new(terminal),
                )
                .map(test_action)
        }

        fn fail_stream(&self, stream: usize) -> Result<TestAction, RuntimeContractViolation> {
            let (binding, instance) = self.streams[stream];
            self.channel
                .fail_instance(binding, instance, ProducerFailureReason::ExecutionFailed)
                .map(test_action)
        }

        fn latest(&self) -> Option<(u64, i64)> {
            self.channel
                .snapshot()
                .map(|snapshot| (snapshot.version().get(), ordered_value(&snapshot)))
        }

        fn state_digest(&self) -> String {
            let state = self.channel.state.lock().unwrap();
            let latest = state
                .ordered
                .as_ref()
                .and_then(|ordered| ordered.latest.as_ref())
                .map(|snapshot| (snapshot.version().get(), ordered_value(snapshot)));
            format!(
                "{:?}:{:?}:{}",
                state.ordered.as_ref().expect("ordered state").reducer,
                latest,
                state.next_dispatch_order
            )
        }
    }

    fn utf8_order_contract() -> (OrderContract, Arc<RuntimeOrderContract>) {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Utf8,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
        (plan, contract)
    }

    fn ordered_single_channel(
        plan: OrderContract,
        max_contribution_bytes: u64,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Arc<RuntimeFilterChannel> {
        let witness = CoverageWitnessId::new(1);
        let deployment = RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::OrderedBound(plan),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes,
                max_artifact_bytes: 4096,
                deadline_ms: 100,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10)])),
            )]),
            BTreeMap::new(),
        );
        let channel = Arc::new(
            RuntimeFilterChannel::new(
                uid(99),
                RuntimeFilterParticipantId::new(1),
                DeploymentEpoch::new(1),
                &deployment,
                Instant::now() + Duration::from_secs(10),
                memory_account,
            )
            .unwrap(),
        );
        channel
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        channel
    }

    fn utf8_update(contract: &RuntimeOrderContract, len: usize) -> OrderedBoundUpdate {
        OrderedBoundUpdate::new(
            contract,
            OrderedTuple::try_new(
                contract,
                [Some(OrderedScalar::Utf8(Arc::from("x".repeat(len))))],
            )
            .unwrap(),
        )
        .unwrap()
    }

    mod ordered {
        use super::*;

        #[test]
        fn higher_equal_advances_sequence_without_new_version() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.submit(0, 3, int_bound(100)).unwrap(),
                TestAction::Published(1, 100)
            );
            assert_eq!(
                harness.submit(0, 7, int_bound(100)).unwrap(),
                TestAction::SequenceAdvancedEqual
            );
            assert_eq!(harness.latest(), Some((1, 100)));
        }

        #[test]
        fn higher_looser_is_contract_violation_and_state_is_unchanged() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.submit(0, 3, int_bound(100)).unwrap();
            let before = harness.state_digest();
            let error = harness.submit(0, 4, int_bound(101)).unwrap_err();
            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::OrderedBoundLoosened
            );
            assert_eq!(harness.state_digest(), before);
        }

        #[test]
        fn another_stream_may_be_looser_than_global_without_violation() {
            let harness = OrderedChannelHarness::two_stream_anyof();
            assert_eq!(
                harness.submit(0, 0, int_bound(50)).unwrap(),
                TestAction::Published(1, 50)
            );
            assert_eq!(
                harness.submit(1, 0, int_bound(90)).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(harness.latest(), Some((1, 50)));
        }

        #[test]
        fn cumulative_close_zero_completes_without_artifact() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.close(0, 0).unwrap(),
                super::TestAction::CompletedWithoutArtifact
            );
        }

        #[test]
        fn cumulative_close_waits_only_for_terminal_minus_one_and_allows_gaps() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.submit(0, 7, super::int_bound(40)).unwrap(),
                super::TestAction::Published(1, 40)
            );
            assert_eq!(
                harness.close(0, 8).unwrap(),
                super::TestAction::Completed(Some((1, 40)))
            );
        }

        #[test]
        fn close_before_final_snapshot_and_snapshot_before_close_both_complete() {
            let close_first = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                close_first.close(0, 8).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert_eq!(
                close_first.submit(0, 7, int_bound(40)).unwrap(),
                TestAction::Completed(Some((1, 40)))
            );
            let update_first = OrderedChannelHarness::single_stream_anyof();
            update_first.submit(0, 7, int_bound(40)).unwrap();
            assert_eq!(
                update_first.close(0, 8).unwrap(),
                TestAction::Completed(Some((1, 40)))
            );
        }

        #[test]
        fn availability_and_terminal_coverage_are_independent() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.submit(0, 0, int_bound(80)).unwrap(),
                TestAction::Published(1, 80)
            );
            assert!(!harness.channel.is_terminal());
        }

        #[test]
        fn logical_versions_advance_only_when_global_bound_tightens() {
            let harness = OrderedChannelHarness::two_stream_anyof();
            assert_eq!(
                harness.submit(0, 0, int_bound(80)).unwrap(),
                TestAction::Published(1, 80)
            );
            assert_eq!(
                harness.submit(1, 0, int_bound(90)).unwrap(),
                TestAction::StreamAcceptedNoGlobalChange
            );
            assert_eq!(
                harness.submit(1, 1, int_bound(70)).unwrap(),
                TestAction::Published(2, 70)
            );
        }

        #[test]
        fn anyof_one_producer_failure_keeps_channel_available_until_other_completes() {
            let harness = OrderedChannelHarness::two_stream_anyof();
            harness.submit(0, 0, int_bound(80)).unwrap();
            assert_eq!(
                harness.fail_stream(0).unwrap(),
                TestAction::CoverageStillPossible
            );
            assert_eq!(
                harness.submit(1, 0, int_bound(70)).unwrap(),
                TestAction::Published(2, 70)
            );
            assert_eq!(
                harness.close(1, 1).unwrap(),
                TestAction::Completed(Some((2, 70)))
            );
        }

        #[test]
        fn conflicting_close_replay_is_rejected_after_channel_completion() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            assert_eq!(
                harness.close(0, 0).unwrap(),
                TestAction::CompletedWithoutArtifact
            );
            assert_eq!(
                harness.close(0, 1).unwrap_err().kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
        }

        #[test]
        fn update_at_terminal_is_rejected_after_channel_completion() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.submit(0, 0, int_bound(40)).unwrap();
            harness.close(0, 1).unwrap();
            assert_eq!(
                harness.submit(0, 1, int_bound(30)).unwrap_err().kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn channel_action_exposes_exact_logical_terminal_mapping() {
            let harness = OrderedChannelHarness::single_stream_anyof();
            harness.close(0, 0).unwrap();
            assert_eq!(
                harness.channel.terminal_action().logical_terminal(),
                Some(crate::runtime_filter::core::state::LogicalTerminal::CompletedWithoutArtifact)
            );
        }

        #[test]
        fn submit_reservation_failure_revalidates_concurrent_cancel() {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let harness = OrderedChannelHarness::with_streams_and_limits(
                1,
                1024,
                4096,
                Arc::new(BlockingRejectingAccount {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            );
            let channel = harness.channel.clone();
            let contract = harness.contract.clone();
            let (binding, instance) = harness.streams[0];
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let tuple =
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(40))]).unwrap();
                let update = OrderedBoundUpdate::new(&contract, tuple).unwrap();
                let contribution_bytes = update.canonical_contribution_bytes().unwrap();
                done_tx
                    .send(channel.submit_ordered(
                        binding,
                        instance,
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        update,
                        TemporaryContributionLease::new(
                            Arc::new(Account::default()),
                            contribution_bytes,
                        ),
                    ))
                    .unwrap();
            });

            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            release_tx.send(()).unwrap();
            release_tx.send(()).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert!(matches!(
                harness.channel.terminal_action(),
                ChannelAction::Cancelled { .. }
            ));
        }

        #[test]
        fn close_reservation_failure_revalidates_concurrent_cancel() {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let harness = OrderedChannelHarness::with_streams_and_limits(
                1,
                1024,
                4096,
                Arc::new(BlockingRejectingAccount {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            );
            let channel = harness.channel.clone();
            let (binding, instance) = harness.streams[0];
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::spawn(move || {
                done_tx
                    .send(channel.close_ordered_partition(
                        binding,
                        instance,
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    ))
                    .unwrap();
            });

            entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            release_tx.send(()).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .outcome(),
                SubmitOutcome::TerminalNoop
            );
            assert!(matches!(
                harness.channel.terminal_action(),
                ChannelAction::Cancelled { .. }
            ));
        }

        #[test]
        fn unavailable_retains_ordered_protocol_tombstone() {
            let account = Arc::new(Account::default());
            let harness = OrderedChannelHarness::with_streams_coverage_and_limits(
                2,
                false,
                1024,
                4096,
                account.clone(),
            );
            assert_eq!(
                harness.submit(0, 0, int_bound(40)).unwrap(),
                TestAction::Other(SubmitOutcome::Published)
            );
            assert_eq!(
                harness.close(0, 2).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert!(matches!(
                harness
                    .channel
                    .expire_deadline(Instant::now() + Duration::from_secs(20)),
                ChannelAction::Unavailable {
                    reason: UnavailableReason::IncompleteCoverage,
                    ..
                }
            ));
            {
                let state = harness.channel.state.lock().unwrap();
                let reducer = &state.ordered.as_ref().unwrap().reducer;
                assert!(reducer.global().is_none());
                assert!(state.reservation.bytes() > 0);
                assert_eq!(
                    state.reservation.bytes(),
                    reducer.estimated_retained_bytes().unwrap()
                );
                assert_eq!(
                    account.current.load(Ordering::SeqCst),
                    state.reservation.bytes()
                );
            }
            assert_eq!(
                harness.close(0, 3).unwrap_err().kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
            assert_eq!(
                harness.submit(0, 2, int_bound(40)).unwrap_err().kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn cancelled_retains_ordered_protocol_tombstone() {
            let account = Arc::new(Account::default());
            let harness =
                OrderedChannelHarness::with_streams_and_limits(1, 1024, 4096, account.clone());
            assert_eq!(
                harness.submit(0, 0, int_bound(40)).unwrap(),
                TestAction::Published(1, 40)
            );
            assert_eq!(
                harness.close(0, 2).unwrap(),
                TestAction::PendingFinalSnapshot
            );
            assert!(matches!(
                harness.channel.cancel(),
                ChannelAction::Cancelled { .. }
            ));
            {
                let state = harness.channel.state.lock().unwrap();
                let ordered = state.ordered.as_ref().unwrap();
                assert!(ordered.reducer.global().is_none());
                assert!(state.reservation.bytes() > 0);
                assert_eq!(
                    state.reservation.bytes(),
                    ordered.reducer.estimated_retained_bytes().unwrap()
                );
                assert_eq!(
                    account.current.load(Ordering::SeqCst),
                    state.reservation.bytes()
                        + ordered.latest.as_ref().unwrap().retained_memory_bytes()
                );
            }
            assert_eq!(
                harness.close(0, 3).unwrap_err().kind(),
                RuntimeContractViolationKind::ConflictingTerminalSequence
            );
            assert_eq!(
                harness.submit(0, 2, int_bound(40)).unwrap_err().kind(),
                RuntimeContractViolationKind::SequenceOutsideTerminalRange
            );
        }

        #[test]
        fn oversized_utf8_contribution_fails_open_without_reducer_mutation_or_leak() {
            let (plan, contract) = utf8_order_contract();
            let update = utf8_update(&contract, 256);
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            let account = Arc::new(Account::default());
            let channel = ordered_single_channel(
                plan,
                u64::try_from(contribution_bytes - 1).unwrap(),
                account.clone(),
            );
            let before_reducer = {
                let state = channel.state.lock().unwrap();
                format!("{:?}", state.ordered.as_ref().unwrap().reducer)
            };

            let action = channel
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    update,
                    TemporaryContributionLease::new(account.clone(), contribution_bytes),
                )
                .unwrap();
            assert_eq!(
                action.unavailable_reason(),
                Some(UnavailableReason::ResourceLimit)
            );
            let state = channel.state.lock().unwrap();
            assert_eq!(
                format!("{:?}", state.ordered.as_ref().unwrap().reducer),
                before_reducer
            );
            assert_eq!(state.reservation.bytes(), 0);
            drop(state);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn exact_ordered_contribution_budget_is_accepted_and_fully_released() {
            let (plan, contract) = utf8_order_contract();
            let update = utf8_update(&contract, 64);
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            let account = Arc::new(Account::default());
            let channel = ordered_single_channel(
                plan,
                u64::try_from(contribution_bytes).unwrap(),
                account.clone(),
            );

            let action = channel
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    update,
                    TemporaryContributionLease::new(account.clone(), contribution_bytes),
                )
                .unwrap();
            assert!(matches!(
                action,
                ChannelAction::VisibleSnapshot {
                    version: LogicalVersion::FIRST,
                    ..
                }
            ));
            drop(action);
            drop(channel);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn mismatched_ordered_contribution_lease_preserves_state_and_releases_memory() {
            let (plan, contract) = utf8_order_contract();
            let update = utf8_update(&contract, 32);
            let contribution_bytes = update.canonical_contribution_bytes().unwrap();
            let account = Arc::new(Account::default());
            let channel = ordered_single_channel(
                plan,
                u64::try_from(contribution_bytes).unwrap(),
                account.clone(),
            );
            let before = {
                let state = channel.state.lock().unwrap();
                format!("{:?}", state.ordered.as_ref().unwrap().reducer)
            };

            let error = channel
                .submit_ordered(
                    BindingId::new(10),
                    uid(10),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    update,
                    TemporaryContributionLease::new(account.clone(), contribution_bytes - 1),
                )
                .unwrap_err();
            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::InvalidContributionLease
            );
            let state = channel.state.lock().unwrap();
            assert_eq!(
                format!("{:?}", state.ordered.as_ref().unwrap().reducer),
                before
            );
            assert_eq!(state.reservation.bytes(), 0);
            drop(state);
            assert_eq!(account.current.load(Ordering::SeqCst), 0);
        }
    }
}
