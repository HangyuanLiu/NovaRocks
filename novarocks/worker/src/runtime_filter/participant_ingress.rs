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
    RuntimeFilterProducerFailure, RuntimeFilterSubmitOutcome,
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendChannelIdentity, BackendEnvelopeKind, BackendIngressResult, BackendParticipantIdentity,
    BackendParticipantInstall, BackendProducerStreamIdentity, BackendRuntimeFilterEvent,
    BackendRuntimeFilterEventObserver, BackendRuntimeFilterSession,
};
use super::observation::RuntimeFilterObservationEmitter;

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
