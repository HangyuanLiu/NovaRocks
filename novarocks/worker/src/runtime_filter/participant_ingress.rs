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

//! Worker-owned runtime-filter ingress verdicts after Native route decoding.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    PartitionId, ProducerSequence, RuntimeFilterBindingId, RuntimeFilterChannelId,
    RuntimeFilterContribution, RuntimeFilterContributionKind, RuntimeFilterProducerFailure,
    RuntimeFilterProducerKind, RuntimeFilterProducerOpenRequest, RuntimeFilterSubmitOutcome,
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendChannelIdentity, BackendEnvelopeKind, BackendIngressResult, BackendParticipantIdentity,
    BackendParticipantInstall, BackendProducerStreamIdentity, BackendRuntimeFilterEvent,
    BackendRuntimeFilterEventObserver, BackendRuntimeFilterSession,
};
use super::observation::RuntimeFilterObservationEmitter;

/// Exact producer coordinates recovered by the Native ingress adapter.
pub struct ProducerIngressRoute {
    channel_id: RuntimeFilterChannelId,
    binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
    partition: PartitionId,
    sequence: ProducerSequence,
    local_partition_count: u32,
}

impl ProducerIngressRoute {
    pub const fn new(
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: PartitionId,
        sequence: ProducerSequence,
        local_partition_count: u32,
    ) -> Self {
        Self {
            channel_id,
            binding_id,
            fragment_instance_id,
            partition,
            sequence,
            local_partition_count,
        }
    }
}

/// Decoded producer command whose interpretation belongs to the Worker.
pub enum ProducerIngressCommand {
    Contribution {
        schema_digest: [u8; 32],
        payload: Arc<[u8]>,
    },
    Closed,
}

/// Applies a decoded contribution or close frame against one installed participant.
///
/// The Native adapter extracts the exact wire facts, but Worker owns the
/// installed-route decision, producer-open validation, local session mutation,
/// and its observation evidence.
pub fn dispatch_producer_frame(
    install: &BackendParticipantInstall,
    producer_sessions: &BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    observation: &RuntimeFilterObservationEmitter,
    route: ProducerIngressRoute,
    command: ProducerIngressCommand,
) -> BackendIngressResult {
    let envelope_kind = match command {
        ProducerIngressCommand::Contribution { .. } => BackendEnvelopeKind::Contribution,
        ProducerIngressCommand::Closed => BackendEnvelopeKind::ProducerClosed,
    };
    if install
        .routing()
        .authorize_contribution(
            route.channel_id,
            route.binding_id,
            route.fragment_instance_id,
            envelope_kind,
        )
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [route-authority]: producer route is not installed",
        );
    }
    let Some(session) = producer_sessions.get(&route.binding_id) else {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is not installed",
        );
    };
    if session.channel().channel_id() != route.channel_id {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is installed for a different channel",
        );
    }
    let Some(channel_install) = session.channel().producers().get(&route.binding_id) else {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is not installed in its channel",
        );
    };
    if session
        .open_producer(
            route.fragment_instance_id,
            RuntimeFilterProducerOpenRequest::new(
                channel_install.contract().clone(),
                route.local_partition_count,
            ),
        )
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [producer-open]: producer open does not match the installed binding",
        );
    }
    observation.register_producer_instance(
        BackendChannelIdentity::new(install.participant(), route.binding_id, route.channel_id),
        route.fragment_instance_id,
        route.local_partition_count,
    );
    match command {
        ProducerIngressCommand::Contribution {
            schema_digest,
            payload,
        } => match session.submit(
            route.binding_id,
            route.fragment_instance_id,
            route.partition,
            route.sequence,
            RuntimeFilterContribution::new(
                contribution_kind(channel_install.contract().kind()),
                schema_digest,
                payload,
            ),
        ) {
            Ok(submission) => {
                record_contribution_outcome(
                    observation,
                    install.participant(),
                    route.binding_id,
                    route.channel_id,
                    route.fragment_instance_id,
                    route.partition,
                    route.sequence,
                    submission.outcome(),
                );
                if matches!(
                    submission.outcome(),
                    RuntimeFilterSubmitOutcome::Duplicate | RuntimeFilterSubmitOutcome::Stale
                ) {
                    BackendIngressResult::duplicate()
                } else {
                    BackendIngressResult::accepted()
                }
            }
            Err(_) => rejected(
                "runtime filter ingress rejected [contribution]: contribution violates the installed execution contract",
            ),
        },
        ProducerIngressCommand::Closed => match session.close_partition(
            route.binding_id,
            route.fragment_instance_id,
            route.partition,
            route.sequence,
        ) {
            Ok(RuntimeFilterSubmitOutcome::TerminalNoop) => BackendIngressResult::duplicate(),
            Ok(_) => BackendIngressResult::accepted(),
            Err(_) => rejected(
                "runtime filter ingress rejected [producer-close]: close violates the installed producer route",
            ),
        },
    }
}

/// Records the Worker-owned outcome of an already-authorized contribution.
pub fn record_contribution_outcome(
    observation: &RuntimeFilterObservationEmitter,
    participant: BackendParticipantIdentity,
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    fragment_instance_id: UniqueId,
    partition: PartitionId,
    sequence: ProducerSequence,
    outcome: RuntimeFilterSubmitOutcome,
) {
    let stream = BackendProducerStreamIdentity::new(
        BackendChannelIdentity::new(participant, binding_id, channel_id),
        fragment_instance_id,
        partition,
    );
    observation.record(match outcome {
        RuntimeFilterSubmitOutcome::Duplicate => {
            BackendRuntimeFilterEvent::ContributionDuplicateIgnored {
                stream,
                sequence: sequence.get(),
            }
        }
        RuntimeFilterSubmitOutcome::Stale => BackendRuntimeFilterEvent::ContributionStaleIgnored {
            stream,
            sequence: sequence.get(),
        },
        _ => BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: sequence.get(),
        },
    });
}

/// Applies a decoded producer-failure frame against one installed participant.
///
/// Native decoding deliberately remains outside this function. Once the
/// adapter has supplied exact route coordinates, installed-route authorization
/// and the local session verdict are Worker responsibilities.
pub fn dispatch_producer_failure(
    install: &BackendParticipantInstall,
    producer_sessions: &BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    channel_id: RuntimeFilterChannelId,
    binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
) -> BackendIngressResult {
    if install
        .routing()
        .authorize_contribution(
            channel_id,
            binding_id,
            fragment_instance_id,
            BackendEnvelopeKind::ProducerUnavailable,
        )
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [route-authority]: producer failure route is not installed",
        );
    }
    let Some(session) = producer_sessions.get(&binding_id) else {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is not installed",
        );
    };
    if session.channel().channel_id() != channel_id {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is installed for a different channel",
        );
    }
    match session.fail(
        binding_id,
        fragment_instance_id,
        RuntimeFilterProducerFailure::UpstreamUnavailable,
    ) {
        Ok(RuntimeFilterSubmitOutcome::TerminalNoop) => BackendIngressResult::duplicate(),
        Ok(_) => BackendIngressResult::accepted(),
        Err(_) => rejected(
            "runtime filter ingress rejected [producer-failure]: failure violates the installed producer route",
        ),
    }
}

fn rejected(reason: &'static str) -> BackendIngressResult {
    BackendIngressResult::rejected(reason).expect("runtime-filter rejection reason is non-empty")
}

fn contribution_kind(kind: RuntimeFilterProducerKind) -> RuntimeFilterContributionKind {
    match kind {
        RuntimeFilterProducerKind::Membership => RuntimeFilterContributionKind::Membership,
        RuntimeFilterProducerKind::OrderedBound => RuntimeFilterContributionKind::OrderedBound,
        RuntimeFilterProducerKind::TopKSummary => RuntimeFilterContributionKind::TopKSummary,
        RuntimeFilterProducerKind::FinalDomain => RuntimeFilterContributionKind::FinalDomain,
    }
}
