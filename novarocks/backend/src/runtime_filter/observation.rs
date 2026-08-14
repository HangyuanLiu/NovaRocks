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

//! Attempt-local runtime-filter observation owned by the Backend participant.
//!
//! The store retains one folded value for each identity authorized by the
//! sealed participant installation. Producer partitions become authorized
//! only after the matching installed producer instance freezes its partition
//! count. Raw events and scan-unit identifiers are never retained.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use novarocks_execution::runtime_filter::{
    ArtifactUnsupportedReason, LiveTerminal, LogicalVersion, UnavailableReason,
    scan_domain::{RuntimeFilterScanUnitDecision, RuntimeFilterScanUnitNotEvaluatedReason},
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendEnvelopeKind,
    BackendParticipantIdentity, BackendParticipantInstall, BackendProducerStreamIdentity,
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendTransportEventIdentity,
    BackendTransportEventKind,
};

thread_local! {
    static OBSERVER_CALLBACK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterObservationError {
    UnknownParticipant,
    UnknownChannel(BackendChannelIdentity),
    UnknownProducerInstance {
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
    },
    ProducerInstanceNotOpened {
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
    },
    ConflictingProducerPartitionCount {
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
    },
    UnknownProducerStream(BackendProducerStreamIdentity),
    ProducerPartitionCountExceeded,
    UnknownTransportRoute(BackendTransportEventIdentity),
    UnknownConsumer(BackendConsumerSubscriptionIdentity),
    IdentityMismatch,
    InvalidVersion,
    VersionRegression,
    ConflictingChannelTerminal {
        channel: BackendChannelIdentity,
        observed: RuntimeFilterChannelTerminal,
        incoming: RuntimeFilterChannelTerminal,
    },
    DeliveryConflict,
    DeliveryResourceLimit,
    InvalidRowEffect,
    CounterOverflow,
}

impl fmt::Display for RuntimeFilterObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter observation: {self:?}"
        )
    }
}

impl std::error::Error for RuntimeFilterObservationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterChannelObservation {
    identity: BackendChannelIdentity,
    latest_published_version: Option<LogicalVersion>,
    terminal: Option<RuntimeFilterChannelTerminal>,
    published: u64,
    completed: u64,
    unavailable: u64,
    cancelled: u64,
}

impl RuntimeFilterChannelObservation {
    pub(crate) const fn identity(&self) -> BackendChannelIdentity {
        self.identity
    }

    pub(crate) const fn latest_published_version(&self) -> Option<LogicalVersion> {
        self.latest_published_version
    }

    pub(crate) const fn terminal(&self) -> Option<RuntimeFilterChannelTerminal> {
        self.terminal
    }

    pub(crate) const fn published(&self) -> u64 {
        self.published
    }

    pub(crate) const fn completed(&self) -> u64 {
        self.completed
    }

    pub(crate) const fn unavailable(&self) -> u64 {
        self.unavailable
    }

    pub(crate) const fn cancelled(&self) -> u64 {
        self.cancelled
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterChannelTerminal {
    Completed(LogicalVersion),
    Unavailable(UnavailableReason),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterProducerStreamObservation {
    identity: BackendProducerStreamIdentity,
    latest_accepted_sequence: Option<u64>,
    accepted: u64,
    duplicate: u64,
    stale: u64,
    conflict: u64,
    resource_limit: u64,
}

impl RuntimeFilterProducerStreamObservation {
    pub(crate) const fn identity(&self) -> BackendProducerStreamIdentity {
        self.identity
    }

    pub(crate) const fn latest_accepted_sequence(&self) -> Option<u64> {
        self.latest_accepted_sequence
    }

    pub(crate) const fn accepted(&self) -> u64 {
        self.accepted
    }

    pub(crate) const fn duplicate(&self) -> u64 {
        self.duplicate
    }

    pub(crate) const fn stale(&self) -> u64 {
        self.stale
    }

    pub(crate) const fn conflict(&self) -> u64 {
        self.conflict
    }

    pub(crate) const fn resource_limit(&self) -> u64 {
        self.resource_limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTransportObservation {
    identity: BackendTransportEventIdentity,
    sent: u64,
    retried: u64,
    acked: u64,
    failed_open: u64,
    sent_bytes: u64,
    retried_bytes: u64,
    acked_bytes: u64,
    failed_open_bytes: u64,
    bytes: u64,
}

impl RuntimeFilterTransportObservation {
    pub(crate) const fn identity(&self) -> BackendTransportEventIdentity {
        self.identity
    }

    pub(crate) const fn sent(&self) -> u64 {
        self.sent
    }

    pub(crate) const fn retried(&self) -> u64 {
        self.retried
    }

    pub(crate) const fn acked(&self) -> u64 {
        self.acked
    }

    pub(crate) const fn failed_open(&self) -> u64 {
        self.failed_open
    }

    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) const fn sent_bytes(&self) -> u64 {
        self.sent_bytes
    }

    pub(crate) const fn retried_bytes(&self) -> u64 {
        self.retried_bytes
    }

    pub(crate) const fn acked_bytes(&self) -> u64 {
        self.acked_bytes
    }

    pub(crate) const fn failed_open_bytes(&self) -> u64 {
        self.failed_open_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterConsumerObservation {
    identity: BackendConsumerSubscriptionIdentity,
    latest_delivered_version: Option<LogicalVersion>,
    latest_applied_version: Option<LogicalVersion>,
    outcome: Option<RuntimeFilterConsumerOutcome>,
    terminal: Option<LiveTerminal>,
    row_evaluations: u64,
    row_input: u64,
    row_output: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedObservation,
}

impl RuntimeFilterConsumerObservation {
    pub(crate) const fn identity(&self) -> BackendConsumerSubscriptionIdentity {
        self.identity
    }

    pub(crate) const fn latest_delivered_version(&self) -> Option<LogicalVersion> {
        self.latest_delivered_version
    }

    pub(crate) const fn latest_applied_version(&self) -> Option<LogicalVersion> {
        self.latest_applied_version
    }

    pub(crate) const fn outcome(&self) -> Option<RuntimeFilterConsumerOutcome> {
        self.outcome
    }

    pub(crate) const fn terminal(&self) -> Option<LiveTerminal> {
        self.terminal
    }

    pub(crate) const fn row_evaluations(&self) -> u64 {
        self.row_evaluations
    }

    pub(crate) const fn row_input(&self) -> u64 {
        self.row_input
    }

    pub(crate) const fn row_output(&self) -> u64 {
        self.row_output
    }

    pub(crate) const fn scan_evaluated(&self) -> u64 {
        self.scan_evaluated
    }

    pub(crate) const fn scan_kept(&self) -> u64 {
        self.scan_kept
    }

    pub(crate) const fn scan_pruned(&self) -> u64 {
        self.scan_pruned
    }

    pub(crate) const fn scan_not_evaluated(&self) -> u64 {
        self.scan_not_evaluated
    }

    pub(crate) const fn scan_not_evaluated_reasons(
        &self,
    ) -> RuntimeFilterScanNotEvaluatedObservation {
        self.scan_not_evaluated_reasons
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterScanNotEvaluatedObservation {
    pub(crate) unit_facts_missing: u64,
    pub(crate) column_facts_missing: u64,
    pub(crate) data_type_unsupported: u64,
    pub(crate) predicate_capability_unsupported: u64,
    pub(crate) resource_unavailable: u64,
    pub(crate) snapshot_unavailable: u64,
    pub(crate) snapshot_timed_out: u64,
    pub(crate) snapshot_not_published: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterConsumerOutcome {
    Acquired,
    TimedOut,
    Unavailable(UnavailableReason),
    Unsupported(ArtifactUnsupportedReason),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterObservationSnapshot {
    channels: Vec<RuntimeFilterChannelObservation>,
    producer_streams: Vec<RuntimeFilterProducerStreamObservation>,
    transport_routes: Vec<RuntimeFilterTransportObservation>,
    consumers: Vec<RuntimeFilterConsumerObservation>,
}

impl RuntimeFilterObservationSnapshot {
    pub(crate) fn channels(&self) -> &[RuntimeFilterChannelObservation] {
        &self.channels
    }

    pub(crate) fn producer_streams(&self) -> &[RuntimeFilterProducerStreamObservation] {
        &self.producer_streams
    }

    pub(crate) fn transport_routes(&self) -> &[RuntimeFilterTransportObservation] {
        &self.transport_routes
    }

    pub(crate) fn consumers(&self) -> &[RuntimeFilterConsumerObservation] {
        &self.consumers
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProducerInstanceIdentity {
    channel: BackendChannelIdentity,
    fragment_instance_id: UniqueId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProducerInstanceState {
    partition_count: Option<u32>,
}

struct ObservationState {
    participant: BackendParticipantIdentity,
    channels: BTreeMap<BackendChannelIdentity, RuntimeFilterChannelObservation>,
    producer_instances: BTreeMap<ProducerInstanceIdentity, ProducerInstanceState>,
    producer_streams:
        BTreeMap<BackendProducerStreamIdentity, RuntimeFilterProducerStreamObservation>,
    transport_routes: BTreeMap<BackendTransportEventIdentity, RuntimeFilterTransportObservation>,
    consumers: BTreeMap<BackendConsumerSubscriptionIdentity, RuntimeFilterConsumerObservation>,
    error: Option<RuntimeFilterObservationError>,
}

// Design: ADR-0068 (docs/adr/ADR-0068-backend-owned-runtime-filter-terminal-observation.md)
pub(crate) struct RuntimeFilterObservationStore {
    state: Mutex<ObservationState>,
}

impl RuntimeFilterObservationStore {
    pub(crate) fn from_install(install: &BackendParticipantInstall) -> Self {
        let participant = install.participant();
        let mut channels = BTreeMap::new();
        let mut producer_instances = BTreeMap::new();
        let mut transport_routes = BTreeMap::new();
        let mut consumers = BTreeMap::new();
        for channel in install.channels().values() {
            let transport_binding_id = channel
                .producers()
                .keys()
                .next()
                .copied()
                .or_else(|| channel.consumers().keys().next().copied());
            for (binding_id, producer) in channel.producers() {
                let identity =
                    BackendChannelIdentity::new(participant, *binding_id, channel.channel_id());
                channels.insert(identity, channel_observation(identity));
                for fragment_instance_id in producer.expected_fragment_instances() {
                    producer_instances.insert(
                        ProducerInstanceIdentity {
                            channel: identity,
                            fragment_instance_id: *fragment_instance_id,
                        },
                        ProducerInstanceState {
                            partition_count: None,
                        },
                    );
                }
                for kind in [
                    BackendEnvelopeKind::Contribution,
                    BackendEnvelopeKind::ProducerClosed,
                    BackendEnvelopeKind::ProducerUnavailable,
                ] {
                    if let Ok(decision) =
                        install
                            .routing()
                            .route_producer(channel.channel_id(), *binding_id, kind)
                    {
                        for route_edge_id in decision
                            .loopback_route_edge_ids()
                            .iter()
                            .copied()
                            .chain(decision.remote_routes().iter().map(|route| route.edge_id()))
                        {
                            let route = BackendTransportEventIdentity::new(identity, route_edge_id);
                            transport_routes
                                .entry(route)
                                .or_insert_with(|| transport_observation(route));
                        }
                    }
                }
            }
            for (binding_id, consumer) in channel.consumers() {
                let channel_identity =
                    BackendChannelIdentity::new(participant, *binding_id, channel.channel_id());
                channels.insert(channel_identity, channel_observation(channel_identity));
                for fragment_instance_id in consumer.expected_fragment_instances() {
                    let identity = BackendConsumerSubscriptionIdentity::new(
                        channel_identity,
                        *binding_id,
                        *fragment_instance_id,
                    );
                    consumers.insert(identity, consumer_observation(identity));
                }
                for route_edge_id in consumer.route_edge_ids() {
                    let identity =
                        BackendTransportEventIdentity::new(channel_identity, *route_edge_id);
                    transport_routes.insert(identity, transport_observation(identity));
                }
            }
            if let Some(binding_id) = transport_binding_id {
                let channel_identity =
                    BackendChannelIdentity::new(participant, binding_id, channel.channel_id());
                for route_edge_id in channel
                    .outbound_materialization_groups()
                    .values()
                    .flat_map(|group| group.route_edge_ids().iter().copied())
                {
                    let identity =
                        BackendTransportEventIdentity::new(channel_identity, route_edge_id);
                    transport_routes
                        .entry(identity)
                        .or_insert_with(|| transport_observation(identity));
                }
            }
        }
        Self {
            state: Mutex::new(ObservationState {
                participant,
                channels,
                producer_instances,
                producer_streams: BTreeMap::new(),
                transport_routes,
                consumers,
                error: None,
            }),
        }
    }

    pub(crate) fn register_producer_instance(
        &self,
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
        partition_count: u32,
    ) {
        let mut state = self.lock();
        if state.error.is_some() {
            return;
        }
        let key = ProducerInstanceIdentity {
            channel,
            fragment_instance_id,
        };
        let result = match state.producer_instances.get_mut(&key) {
            None => Err(RuntimeFilterObservationError::UnknownProducerInstance {
                channel,
                fragment_instance_id,
            }),
            Some(_) if partition_count == 0 => Err(RuntimeFilterObservationError::IdentityMismatch),
            Some(_)
                if partition_count
                    > super::domain::MAX_RUNTIME_FILTER_PRODUCER_PARTITIONS_PER_INSTANCE =>
            {
                Err(RuntimeFilterObservationError::ProducerPartitionCountExceeded)
            }
            Some(instance) => match instance.partition_count {
                Some(existing) if existing != partition_count => Err(
                    RuntimeFilterObservationError::ConflictingProducerPartitionCount {
                        channel,
                        fragment_instance_id,
                    },
                ),
                Some(_) => Ok(()),
                None => {
                    instance.partition_count = Some(partition_count);
                    Ok(())
                }
            },
        };
        if let Err(error) = result {
            remember_error(&mut state, error);
        }
    }

    pub(crate) fn fold(&self, event: &BackendRuntimeFilterEvent) {
        let mut state = self.lock();
        if state.error.is_some() {
            return;
        }
        if let Err(error) = fold_event(&mut state, event) {
            remember_error(&mut state, error);
        }
    }

    pub(crate) fn reject(&self, error: RuntimeFilterObservationError) {
        let mut state = self.lock();
        remember_error(&mut state, error);
    }

    pub(crate) fn capture(
        &self,
    ) -> Result<RuntimeFilterObservationSnapshot, RuntimeFilterObservationError> {
        let state = self.lock();
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        Ok(RuntimeFilterObservationSnapshot {
            channels: state.channels.values().cloned().collect(),
            producer_streams: state.producer_streams.values().cloned().collect(),
            transport_routes: state.transport_routes.values().cloned().collect(),
            consumers: state.consumers.values().cloned().collect(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ObservationState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

pub(crate) struct RuntimeFilterObservationEmitter {
    store: Arc<RuntimeFilterObservationStore>,
    observer: Option<Arc<dyn BackendRuntimeFilterEventObserver>>,
}

impl RuntimeFilterObservationEmitter {
    pub(crate) fn from_install(
        install: &BackendParticipantInstall,
        observer: Option<Arc<dyn BackendRuntimeFilterEventObserver>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store: Arc::new(RuntimeFilterObservationStore::from_install(install)),
            observer,
        })
    }

    pub(crate) fn register_producer_instance(
        &self,
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
        partition_count: u32,
    ) {
        self.store
            .register_producer_instance(channel, fragment_instance_id, partition_count);
    }

    pub(crate) fn capture(
        &self,
    ) -> Result<RuntimeFilterObservationSnapshot, RuntimeFilterObservationError> {
        self.store.capture()
    }

    pub(crate) fn reject(&self, error: RuntimeFilterObservationError) {
        self.store.reject(error);
    }
}

impl BackendRuntimeFilterEventObserver for RuntimeFilterObservationEmitter {
    fn record(&self, event: BackendRuntimeFilterEvent) {
        if OBSERVER_CALLBACK_DEPTH.with(|depth| depth.get() != 0) {
            return;
        }
        self.store.fold(&event);
        let Some(observer) = &self.observer else {
            return;
        };
        OBSERVER_CALLBACK_DEPTH.with(|depth| {
            let previous = depth.replace(depth.get().saturating_add(1));
            let _ = catch_unwind(AssertUnwindSafe(|| observer.record(event)));
            depth.set(previous);
        });
    }
}

fn fold_event(
    state: &mut ObservationState,
    event: &BackendRuntimeFilterEvent,
) -> Result<(), RuntimeFilterObservationError> {
    match event {
        BackendRuntimeFilterEvent::DeploymentInstalled { participant } => {
            if *participant != state.participant {
                return Err(RuntimeFilterObservationError::UnknownParticipant);
            }
        }
        BackendRuntimeFilterEvent::ChannelPlanned { channel } => {
            channel_mut(state, *channel)?;
        }
        BackendRuntimeFilterEvent::ContributionAccepted { stream, sequence } => {
            let stream = producer_stream_mut(state, *stream)?;
            if stream
                .latest_accepted_sequence
                .is_some_and(|latest| *sequence < latest)
            {
                return Err(RuntimeFilterObservationError::VersionRegression);
            }
            stream.latest_accepted_sequence = Some(*sequence);
            stream.accepted = increment(stream.accepted, 1)?;
        }
        BackendRuntimeFilterEvent::ContributionDuplicateIgnored { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.duplicate = increment(stream.duplicate, 1)?;
        }
        BackendRuntimeFilterEvent::ContributionStaleIgnored { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.stale = increment(stream.stale, 1)?;
        }
        BackendRuntimeFilterEvent::ContributionConflictRejected { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.conflict = increment(stream.conflict, 1)?;
        }
        BackendRuntimeFilterEvent::ContributionResourceLimitRejected { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.resource_limit = increment(stream.resource_limit, 1)?;
        }
        BackendRuntimeFilterEvent::LogicalVersionPublished { channel, version } => {
            validate_version(*version)?;
            let channel = channel_mut(state, *channel)?;
            if channel.latest_published_version == Some(*version) {
                return Ok(());
            }
            advance_version(&mut channel.latest_published_version, *version)?;
            channel.published = increment(channel.published, 1)?;
        }
        BackendRuntimeFilterEvent::ChannelCompleted { channel, version } => {
            validate_version(*version)?;
            let channel = channel_mut(state, *channel)?;
            advance_version(&mut channel.latest_published_version, *version)?;
            let terminal = RuntimeFilterChannelTerminal::Completed(*version);
            if channel.terminal == Some(terminal) {
                return Ok(());
            }
            if channel.terminal
                == Some(RuntimeFilterChannelTerminal::Unavailable(
                    UnavailableReason::IncompleteCoverage,
                ))
            {
                // A distributed AnyOf channel may first learn that one owner
                // completed without an artifact and later receive the final
                // artifact from another owner. The participant-level channel
                // outcome is Completed, not a conflicting terminal.
                channel.terminal = Some(terminal);
                channel.completed = 1;
                channel.unavailable = 0;
                return Ok(());
            }
            if let Some(observed) = channel.terminal {
                return Err(RuntimeFilterObservationError::ConflictingChannelTerminal {
                    channel: channel.identity,
                    observed,
                    incoming: terminal,
                });
            }
            channel.terminal = Some(terminal);
            channel.completed = increment(channel.completed, 1)?;
        }
        BackendRuntimeFilterEvent::ChannelUnavailable { channel, reason } => {
            let channel = channel_mut(state, *channel)?;
            let terminal = RuntimeFilterChannelTerminal::Unavailable(*reason);
            if channel.terminal == Some(terminal) {
                return Ok(());
            }
            if *reason == UnavailableReason::IncompleteCoverage
                && matches!(
                    channel.terminal,
                    Some(RuntimeFilterChannelTerminal::Completed(_))
                )
            {
                // The same AnyOf race in the opposite arrival order is an
                // idempotent no-op once an artifact has completed the channel.
                return Ok(());
            }
            if let Some(observed) = channel.terminal {
                return Err(RuntimeFilterObservationError::ConflictingChannelTerminal {
                    channel: channel.identity,
                    observed,
                    incoming: terminal,
                });
            }
            channel.terminal = Some(terminal);
            channel.unavailable = increment(channel.unavailable, 1)?;
        }
        BackendRuntimeFilterEvent::ChannelCancelled { channel } => {
            let channel = channel_mut(state, *channel)?;
            if channel.terminal.is_none() {
                channel.terminal = Some(RuntimeFilterChannelTerminal::Cancelled);
                channel.cancelled = increment(channel.cancelled, 1)?;
            }
        }
        BackendRuntimeFilterEvent::TransportEnvelope {
            identity,
            kind,
            bytes,
        } => {
            let bytes = u64::try_from(*bytes)
                .map_err(|_| RuntimeFilterObservationError::CounterOverflow)?;
            let route = state.transport_routes.get_mut(identity).ok_or(
                RuntimeFilterObservationError::UnknownTransportRoute(*identity),
            )?;
            let total_bytes = increment(route.bytes, bytes)?;
            match kind {
                BackendTransportEventKind::Sent => {
                    route.sent = increment(route.sent, 1)?;
                    route.sent_bytes = increment(route.sent_bytes, bytes)?;
                }
                BackendTransportEventKind::Retried => {
                    route.retried = increment(route.retried, 1)?;
                    route.retried_bytes = increment(route.retried_bytes, bytes)?;
                }
                BackendTransportEventKind::Acked(_status) => {
                    route.acked = increment(route.acked, 1)?;
                    route.acked_bytes = increment(route.acked_bytes, bytes)?;
                }
                BackendTransportEventKind::FailedOpen(_reason) => {
                    route.failed_open = increment(route.failed_open, 1)?;
                    route.failed_open_bytes = increment(route.failed_open_bytes, bytes)?;
                }
            }
            route.bytes = total_bytes;
        }
        BackendRuntimeFilterEvent::SubscriptionAcquired { identity, version } => {
            validate_version(*version)?;
            let consumer = consumer_mut(state, *identity)?;
            advance_version(&mut consumer.latest_delivered_version, *version)?;
            consumer.outcome = Some(RuntimeFilterConsumerOutcome::Acquired);
        }
        BackendRuntimeFilterEvent::SubscriptionTimedOut { identity } => {
            consumer_mut(state, *identity)?.outcome = Some(RuntimeFilterConsumerOutcome::TimedOut);
        }
        BackendRuntimeFilterEvent::SubscriptionUnavailable { identity, reason } => {
            consumer_mut(state, *identity)?.outcome =
                Some(RuntimeFilterConsumerOutcome::Unavailable(*reason));
        }
        BackendRuntimeFilterEvent::SubscriptionUnsupported { identity, reason } => {
            consumer_mut(state, *identity)?.outcome =
                Some(RuntimeFilterConsumerOutcome::Unsupported(*reason));
        }
        BackendRuntimeFilterEvent::SubscriptionCancelled { identity } => {
            consumer_mut(state, *identity)?.outcome = Some(RuntimeFilterConsumerOutcome::Cancelled);
        }
        BackendRuntimeFilterEvent::LiveSubscriptionUpdated {
            identity,
            version,
            terminal,
        } => {
            validate_version(*version)?;
            let consumer = consumer_mut(state, *identity)?;
            advance_version(&mut consumer.latest_delivered_version, *version)?;
            consumer.outcome = Some(RuntimeFilterConsumerOutcome::Acquired);
            if let Some(terminal) = terminal {
                consumer.terminal = Some(*terminal);
            }
        }
        BackendRuntimeFilterEvent::LiveSubscriptionIdle {
            identity,
            latest_version,
            terminal,
        } => {
            let consumer = consumer_mut(state, *identity)?;
            if let Some(version) = latest_version {
                validate_version(*version)?;
                advance_version(&mut consumer.latest_delivered_version, *version)?;
            }
            if let Some(terminal) = terminal {
                consumer.terminal = Some(*terminal);
            }
        }
        BackendRuntimeFilterEvent::LiveSubscriptionTerminal {
            identity,
            terminal,
            retained_version,
        } => {
            let consumer = consumer_mut(state, *identity)?;
            if let Some(version) = retained_version {
                validate_version(*version)?;
                advance_version(&mut consumer.latest_delivered_version, *version)?;
            }
            consumer.terminal = Some(*terminal);
        }
        BackendRuntimeFilterEvent::LoopbackDelivered {
            channel,
            consumer_binding_id,
            route_edge_id,
            version,
        } => {
            validate_version(*version)?;
            if channel.binding_id() != *consumer_binding_id {
                return Err(RuntimeFilterObservationError::IdentityMismatch);
            }
            channel_mut(state, *channel)?;
            let route = BackendTransportEventIdentity::new(*channel, *route_edge_id);
            if !state.transport_routes.contains_key(&route) {
                return Err(RuntimeFilterObservationError::UnknownTransportRoute(route));
            }
        }
        BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity,
            logical_version,
            input_rows,
            output_rows,
        } => {
            validate_version(*logical_version)?;
            if output_rows > input_rows {
                return Err(RuntimeFilterObservationError::InvalidRowEffect);
            }
            let consumer = consumer_mut(state, *identity)?;
            advance_version(&mut consumer.latest_applied_version, *logical_version)?;
            let evaluations = increment(consumer.row_evaluations, 1)?;
            let input = increment(consumer.row_input, *input_rows)?;
            let output = increment(consumer.row_output, *output_rows)?;
            consumer.row_evaluations = evaluations;
            consumer.row_input = input;
            consumer.row_output = output;
        }
        BackendRuntimeFilterEvent::ConsumerScanUnitEvaluated {
            identity,
            logical_version,
            decision,
        } => {
            validate_version(*logical_version)?;
            let consumer = consumer_mut(state, *identity)?;
            advance_version(&mut consumer.latest_applied_version, *logical_version)?;
            let evaluated = increment(consumer.scan_evaluated, 1)?;
            let (kept, pruned) = match decision {
                RuntimeFilterScanUnitDecision::Kept => {
                    (increment(consumer.scan_kept, 1)?, consumer.scan_pruned)
                }
                RuntimeFilterScanUnitDecision::Pruned => {
                    (consumer.scan_kept, increment(consumer.scan_pruned, 1)?)
                }
            };
            consumer.scan_evaluated = evaluated;
            consumer.scan_kept = kept;
            consumer.scan_pruned = pruned;
        }
        BackendRuntimeFilterEvent::ConsumerScanUnitNotEvaluated {
            identity,
            observed_version,
            reason,
        } => {
            let consumer = consumer_mut(state, *identity)?;
            if let Some(version) = observed_version {
                validate_version(*version)?;
            }
            consumer.scan_not_evaluated = increment(consumer.scan_not_evaluated, 1)?;
            let counter = match reason {
                RuntimeFilterScanUnitNotEvaluatedReason::UnitFactsMissing(_) => {
                    &mut consumer.scan_not_evaluated_reasons.unit_facts_missing
                }
                RuntimeFilterScanUnitNotEvaluatedReason::ColumnFactsMissing(_) => {
                    &mut consumer.scan_not_evaluated_reasons.column_facts_missing
                }
                RuntimeFilterScanUnitNotEvaluatedReason::DataTypeUnsupported => {
                    &mut consumer.scan_not_evaluated_reasons.data_type_unsupported
                }
                RuntimeFilterScanUnitNotEvaluatedReason::PredicateCapabilityUnsupported => {
                    &mut consumer
                        .scan_not_evaluated_reasons
                        .predicate_capability_unsupported
                }
                RuntimeFilterScanUnitNotEvaluatedReason::ResourceUnavailable => {
                    &mut consumer.scan_not_evaluated_reasons.resource_unavailable
                }
                RuntimeFilterScanUnitNotEvaluatedReason::SnapshotUnavailable => {
                    &mut consumer.scan_not_evaluated_reasons.snapshot_unavailable
                }
                RuntimeFilterScanUnitNotEvaluatedReason::SnapshotTimedOut => {
                    &mut consumer.scan_not_evaluated_reasons.snapshot_timed_out
                }
                RuntimeFilterScanUnitNotEvaluatedReason::SnapshotNotPublished => {
                    &mut consumer.scan_not_evaluated_reasons.snapshot_not_published
                }
            };
            *counter = increment(*counter, 1)?;
        }
    }
    Ok(())
}

fn channel_mut(
    state: &mut ObservationState,
    identity: BackendChannelIdentity,
) -> Result<&mut RuntimeFilterChannelObservation, RuntimeFilterObservationError> {
    state
        .channels
        .get_mut(&identity)
        .ok_or(RuntimeFilterObservationError::UnknownChannel(identity))
}

fn producer_stream_mut(
    state: &mut ObservationState,
    identity: BackendProducerStreamIdentity,
) -> Result<&mut RuntimeFilterProducerStreamObservation, RuntimeFilterObservationError> {
    if !state.producer_streams.contains_key(&identity) {
        let instance_key = ProducerInstanceIdentity {
            channel: identity.channel(),
            fragment_instance_id: identity.fragment_instance_id(),
        };
        let instance = state.producer_instances.get(&instance_key).ok_or(
            RuntimeFilterObservationError::UnknownProducerInstance {
                channel: identity.channel(),
                fragment_instance_id: identity.fragment_instance_id(),
            },
        )?;
        let partition_count = instance.partition_count.ok_or(
            RuntimeFilterObservationError::ProducerInstanceNotOpened {
                channel: identity.channel(),
                fragment_instance_id: identity.fragment_instance_id(),
            },
        )?;
        if identity.partition_id().get() >= partition_count {
            return Err(RuntimeFilterObservationError::UnknownProducerStream(
                identity,
            ));
        }
        state
            .producer_streams
            .insert(identity, producer_stream_observation(identity));
    }
    Ok(state
        .producer_streams
        .get_mut(&identity)
        .expect("authorized producer stream was inserted"))
}

fn consumer_mut(
    state: &mut ObservationState,
    identity: BackendConsumerSubscriptionIdentity,
) -> Result<&mut RuntimeFilterConsumerObservation, RuntimeFilterObservationError> {
    state
        .consumers
        .get_mut(&identity)
        .ok_or(RuntimeFilterObservationError::UnknownConsumer(identity))
}

fn validate_version(version: LogicalVersion) -> Result<(), RuntimeFilterObservationError> {
    if version.get() == 0 {
        return Err(RuntimeFilterObservationError::InvalidVersion);
    }
    Ok(())
}

fn advance_version(
    current: &mut Option<LogicalVersion>,
    observed: LogicalVersion,
) -> Result<(), RuntimeFilterObservationError> {
    if current.is_some_and(|current| observed < current) {
        return Err(RuntimeFilterObservationError::VersionRegression);
    }
    if current.is_none_or(|current| observed > current) {
        *current = Some(observed);
    }
    Ok(())
}

fn increment(current: u64, delta: u64) -> Result<u64, RuntimeFilterObservationError> {
    current
        .checked_add(delta)
        .ok_or(RuntimeFilterObservationError::CounterOverflow)
}

fn remember_error(state: &mut ObservationState, error: RuntimeFilterObservationError) {
    if state.error.is_none() {
        state.error = Some(error);
    }
}

fn channel_observation(identity: BackendChannelIdentity) -> RuntimeFilterChannelObservation {
    RuntimeFilterChannelObservation {
        identity,
        latest_published_version: None,
        terminal: None,
        published: 0,
        completed: 0,
        unavailable: 0,
        cancelled: 0,
    }
}

fn producer_stream_observation(
    identity: BackendProducerStreamIdentity,
) -> RuntimeFilterProducerStreamObservation {
    RuntimeFilterProducerStreamObservation {
        identity,
        latest_accepted_sequence: None,
        accepted: 0,
        duplicate: 0,
        stale: 0,
        conflict: 0,
        resource_limit: 0,
    }
}

fn transport_observation(
    identity: BackendTransportEventIdentity,
) -> RuntimeFilterTransportObservation {
    RuntimeFilterTransportObservation {
        identity,
        sent: 0,
        retried: 0,
        acked: 0,
        failed_open: 0,
        sent_bytes: 0,
        retried_bytes: 0,
        acked_bytes: 0,
        failed_open_bytes: 0,
        bytes: 0,
    }
}

fn consumer_observation(
    identity: BackendConsumerSubscriptionIdentity,
) -> RuntimeFilterConsumerObservation {
    RuntimeFilterConsumerObservation {
        identity,
        latest_delivered_version: None,
        latest_applied_version: None,
        outcome: None,
        terminal: None,
        row_evaluations: 0,
        row_input: 0,
        row_output: 0,
        scan_evaluated: 0,
        scan_kept: 0,
        scan_pruned: 0,
        scan_not_evaluated: 0,
        scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedObservation::default(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, Weak};
    use std::thread;

    use novarocks_execution::runtime_filter::{
        PartitionId, RuntimeFilterBindingId, RuntimeFilterConsumerContract,
        scan_domain::RuntimeFilterScanUnitNotEvaluatedReason,
    };

    use super::*;
    use crate::runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile};
    use crate::runtime_filter::domain::{
        BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall,
        BackendCoverageWitnessId, BackendMaterializationPolicy, BackendProducerInstall,
        BackendRouteEdgeId, BackendRoutingShard,
    };
    use crate::runtime_filter::test_support::BackendRuntimeFilterFixture;

    struct Fixture {
        install: BackendParticipantInstall,
        producer_channel: BackendChannelIdentity,
        producer_instance: UniqueId,
        consumer: BackendConsumerSubscriptionIdentity,
        route: BackendTransportEventIdentity,
    }

    fn fixture() -> Fixture {
        let fixture = BackendRuntimeFilterFixture::membership();
        let participant = fixture.identity();
        let producer = fixture.producer_contract();
        let producer_instance = UniqueId::new(101, 102);
        let consumer_instance = UniqueId::new(201, 202);
        let consumer_contract = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            producer.channel_id(),
            producer.contract().clone(),
        )
        .expect("consumer contract");
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("consumer profile");
        let route_edge_id = BackendRouteEdgeId::new(501);
        let channel = BackendChannelInstall::new(
            producer.channel_id(),
            producer.contract().clone(),
            BackendChannelLifecycle::CompleteOnce,
            fixture.coverage(),
            fixture.coverage(),
            BackendMaterializationPolicy::new(8, 3, 5, 1, 4096, 4096, 1)
                .expect("materialization policy"),
            4096,
            4096,
            [BackendProducerInstall::new(
                producer.clone(),
                BackendCoverageWitnessId::new(29),
                [producer_instance],
                4096,
            )
            .expect("producer install")],
            [BackendConsumerInstall::new(
                consumer_contract.clone(),
                profile,
                [route_edge_id],
                [consumer_instance],
            )
            .expect("consumer install")],
            [],
        )
        .expect("channel install");
        let routing = BackendRoutingShard::new(participant, 1, [])
            .expect("empty routing is sufficient for store tests");
        let install = BackendParticipantInstall::new(participant, 1, [channel], routing)
            .expect("participant install");
        let producer_channel =
            BackendChannelIdentity::new(participant, producer.binding_id(), producer.channel_id());
        let consumer_channel = BackendChannelIdentity::new(
            participant,
            consumer_contract.binding_id(),
            producer.channel_id(),
        );
        let consumer = BackendConsumerSubscriptionIdentity::new(
            consumer_channel,
            consumer_contract.binding_id(),
            consumer_instance,
        );
        Fixture {
            install,
            producer_channel,
            producer_instance,
            consumer,
            route: BackendTransportEventIdentity::new(consumer_channel, route_edge_id),
        }
    }

    #[test]
    fn folds_only_authorized_identities_into_an_owned_snapshot() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.register_producer_instance(fixture.producer_channel, fixture.producer_instance, 2);
        let stream = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(1),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: 3,
        });
        emitter.record(BackendRuntimeFilterEvent::ContributionDuplicateIgnored {
            stream,
            sequence: 3,
        });
        emitter.record(BackendRuntimeFilterEvent::ContributionStaleIgnored {
            stream,
            sequence: 2,
        });
        for _ in 0..2 {
            emitter.record(BackendRuntimeFilterEvent::LogicalVersionPublished {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            });
            emitter.record(BackendRuntimeFilterEvent::ChannelCompleted {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            });
        }
        emitter.record(BackendRuntimeFilterEvent::TransportEnvelope {
            identity: fixture.route,
            kind: BackendTransportEventKind::Sent,
            bytes: 17,
        });
        emitter.record(BackendRuntimeFilterEvent::SubscriptionAcquired {
            identity: fixture.consumer,
            version: LogicalVersion::FIRST,
        });
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 100,
            output_rows: 20,
        });
        emitter.record(BackendRuntimeFilterEvent::ConsumerScanUnitEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            decision: RuntimeFilterScanUnitDecision::Pruned,
        });
        emitter.record(BackendRuntimeFilterEvent::ConsumerScanUnitNotEvaluated {
            identity: fixture.consumer,
            observed_version: Some(LogicalVersion::FIRST),
            reason: RuntimeFilterScanUnitNotEvaluatedReason::SnapshotUnavailable,
        });

        let frozen = emitter.capture().expect("capture");
        assert_eq!(frozen.producer_streams().len(), 1);
        assert_eq!(
            frozen.producer_streams()[0].latest_accepted_sequence(),
            Some(3)
        );
        assert_eq!(frozen.producer_streams()[0].accepted(), 1);
        assert_eq!(frozen.producer_streams()[0].duplicate(), 1);
        assert_eq!(frozen.producer_streams()[0].stale(), 1);
        let channel = frozen
            .channels()
            .iter()
            .find(|channel| channel.identity() == fixture.producer_channel)
            .expect("producer channel observation");
        assert_eq!(channel.published(), 1);
        assert_eq!(channel.completed(), 1);
        assert_eq!(frozen.transport_routes()[0].sent(), 1);
        assert_eq!(frozen.transport_routes()[0].bytes(), 17);
        assert_eq!(frozen.consumers()[0].row_input(), 100);
        assert_eq!(frozen.consumers()[0].row_output(), 20);
        assert_eq!(frozen.consumers()[0].scan_evaluated(), 1);
        assert_eq!(frozen.consumers()[0].scan_pruned(), 1);
        assert_eq!(frozen.consumers()[0].scan_not_evaluated(), 1);

        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 5,
        });
        assert_eq!(frozen.consumers()[0].row_input(), 100);
        assert_eq!(emitter.capture().unwrap().consumers()[0].row_input(), 110);
    }

    #[test]
    fn anyof_completion_supersedes_incomplete_owner_in_both_arrival_orders() {
        for completed_first in [false, true] {
            let fixture = fixture();
            let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
            let incomplete = BackendRuntimeFilterEvent::ChannelUnavailable {
                channel: fixture.producer_channel,
                reason: UnavailableReason::IncompleteCoverage,
            };
            let completed = BackendRuntimeFilterEvent::ChannelCompleted {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            };
            if completed_first {
                emitter.record(completed);
                emitter.record(incomplete);
            } else {
                emitter.record(incomplete);
                emitter.record(completed);
            }

            let frozen = emitter.capture().expect("AnyOf completion");
            let channel = frozen
                .channels()
                .iter()
                .find(|channel| channel.identity() == fixture.producer_channel)
                .expect("producer channel observation");
            assert_eq!(
                channel.terminal(),
                Some(RuntimeFilterChannelTerminal::Completed(
                    LogicalVersion::FIRST
                ))
            );
            assert_eq!(channel.completed(), 1);
            assert_eq!(channel.unavailable(), 0);
        }
    }

    #[test]
    fn cross_driver_effect_reordering_currently_sticks_version_regression() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        for version in [LogicalVersion::new(2), LogicalVersion::FIRST] {
            emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                identity: fixture.consumer,
                logical_version: version,
                input_rows: 1,
                output_rows: 1,
            });
        }

        assert_eq!(
            emitter.capture(),
            Err(RuntimeFilterObservationError::VersionRegression)
        );
    }

    #[test]
    fn unknown_or_unopened_stream_is_a_first_wins_sticky_error() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        let stream = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(0),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: 1,
        });
        emitter.reject(RuntimeFilterObservationError::CounterOverflow);
        assert_eq!(
            emitter.capture(),
            Err(RuntimeFilterObservationError::ProducerInstanceNotOpened {
                channel: fixture.producer_channel,
                fragment_instance_id: fixture.producer_instance,
            })
        );
    }

    #[test]
    fn producer_partition_bound_cannot_expand_after_open() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.register_producer_instance(fixture.producer_channel, fixture.producer_instance, 1);
        let out_of_bound = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(1),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream: out_of_bound,
            sequence: 1,
        });
        assert_eq!(
            emitter.capture(),
            Err(RuntimeFilterObservationError::UnknownProducerStream(
                out_of_bound
            ))
        );
    }

    struct PanickingObserver;

    impl BackendRuntimeFilterEventObserver for PanickingObserver {
        fn record(&self, _: BackendRuntimeFilterEvent) {
            panic!("diagnostic observer panic");
        }
    }

    #[test]
    fn diagnostic_observer_panic_cannot_erase_store_state() {
        let fixture = fixture();
        let observer: Arc<dyn BackendRuntimeFilterEventObserver> = Arc::new(PanickingObserver);
        let emitter =
            RuntimeFilterObservationEmitter::from_install(&fixture.install, Some(observer));
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 4,
        });
        assert_eq!(emitter.capture().unwrap().consumers()[0].row_input(), 10);
    }

    struct ReentrantObserver {
        emitter: Mutex<Weak<RuntimeFilterObservationEmitter>>,
        event: BackendRuntimeFilterEvent,
    }

    impl BackendRuntimeFilterEventObserver for ReentrantObserver {
        fn record(&self, _: BackendRuntimeFilterEvent) {
            if let Some(emitter) = self.emitter.lock().unwrap().upgrade() {
                emitter.record(self.event.clone());
            }
        }
    }

    #[test]
    fn diagnostic_observer_reentrancy_cannot_double_fold() {
        let fixture = fixture();
        let event = BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 4,
        };
        let observer = Arc::new(ReentrantObserver {
            emitter: Mutex::new(Weak::new()),
            event: event.clone(),
        });
        let emitter =
            RuntimeFilterObservationEmitter::from_install(&fixture.install, Some(observer.clone()));
        *observer.emitter.lock().unwrap() = Arc::downgrade(&emitter);
        emitter.record(event);
        let captured = emitter.capture().unwrap();
        let consumer = &captured.consumers()[0];
        assert_eq!(consumer.row_evaluations(), 1);
        assert_eq!(consumer.row_input(), 10);
    }

    #[test]
    fn concurrent_fold_and_capture_preserve_checked_totals() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        let mut workers = Vec::new();
        for _ in 0..4 {
            let emitter = Arc::clone(&emitter);
            let identity = fixture.consumer;
            workers.push(thread::spawn(move || {
                for _ in 0..100 {
                    emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                        identity,
                        logical_version: LogicalVersion::FIRST,
                        input_rows: 1,
                        output_rows: 1,
                    });
                }
            }));
        }
        for _ in 0..10 {
            let _ = emitter.capture().expect("concurrent capture");
        }
        for worker in workers {
            worker.join().expect("worker");
        }
        let captured = emitter.capture().unwrap();
        let consumer = &captured.consumers()[0];
        assert_eq!(consumer.row_evaluations(), 400);
        assert_eq!(consumer.row_input(), 400);
        assert_eq!(consumer.row_output(), 400);
    }

    #[test]
    fn counter_overflow_is_sticky_and_never_wraps() {
        let fixture = fixture();
        let store = RuntimeFilterObservationStore::from_install(&fixture.install);
        {
            let mut state = store.lock();
            state
                .consumers
                .get_mut(&fixture.consumer)
                .expect("installed consumer")
                .row_input = u64::MAX;
        }
        store.fold(&BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 1,
            output_rows: 0,
        });
        assert_eq!(
            store.capture(),
            Err(RuntimeFilterObservationError::CounterOverflow)
        );
    }
}
