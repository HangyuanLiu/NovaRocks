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

//! Backend Service event vocabulary.
//!
//! Events are emitted at the point of state transition. The participant-owned
//! emitter folds them into bounded observation state before notifying an
//! optional diagnostic observer.

use novarocks_execution::runtime_filter::{
    ArtifactUnsupportedReason, LiveTerminal, LogicalVersion, RuntimeFilterBindingId,
    UnavailableReason,
    scan_domain::{RuntimeFilterScanUnitDecision, RuntimeFilterScanUnitNotEvaluatedReason},
};

use super::{BackendAcceptStatus, BackendTransportFailOpenReason};
use super::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendProducerStreamIdentity,
    BackendRouteEdgeId,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendTransportEventIdentity {
    channel: BackendChannelIdentity,
    route_edge_id: BackendRouteEdgeId,
}

impl BackendTransportEventIdentity {
    pub(crate) const fn new(
        channel: BackendChannelIdentity,
        route_edge_id: BackendRouteEdgeId,
    ) -> Self {
        Self {
            channel,
            route_edge_id,
        }
    }

    pub(crate) const fn channel(self) -> BackendChannelIdentity {
        self.channel
    }

    pub(crate) const fn route_edge_id(self) -> BackendRouteEdgeId {
        self.route_edge_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendTransportEventKind {
    Sent,
    Retried,
    Acked(BackendAcceptStatus),
    FailedOpen(BackendTransportFailOpenReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterEvent {
    DeploymentInstalled {
        participant: super::BackendParticipantIdentity,
    },
    ChannelPlanned {
        channel: BackendChannelIdentity,
    },
    ContributionAccepted {
        stream: BackendProducerStreamIdentity,
        sequence: u64,
    },
    ContributionDuplicateIgnored {
        stream: BackendProducerStreamIdentity,
        sequence: u64,
    },
    ContributionStaleIgnored {
        stream: BackendProducerStreamIdentity,
        sequence: u64,
    },
    ContributionConflictRejected {
        stream: BackendProducerStreamIdentity,
        sequence: u64,
    },
    ContributionResourceLimitRejected {
        stream: BackendProducerStreamIdentity,
        sequence: u64,
    },
    LogicalVersionPublished {
        channel: BackendChannelIdentity,
        version: LogicalVersion,
    },
    ChannelCompleted {
        channel: BackendChannelIdentity,
        version: LogicalVersion,
    },
    ChannelUnavailable {
        channel: BackendChannelIdentity,
        reason: UnavailableReason,
    },
    ChannelCancelled {
        channel: BackendChannelIdentity,
    },
    TransportEnvelope {
        identity: BackendTransportEventIdentity,
        kind: BackendTransportEventKind,
        bytes: usize,
    },
    SubscriptionAcquired {
        identity: BackendConsumerSubscriptionIdentity,
        version: LogicalVersion,
    },
    SubscriptionTimedOut {
        identity: BackendConsumerSubscriptionIdentity,
    },
    SubscriptionUnavailable {
        identity: BackendConsumerSubscriptionIdentity,
        reason: UnavailableReason,
    },
    SubscriptionUnsupported {
        identity: BackendConsumerSubscriptionIdentity,
        reason: ArtifactUnsupportedReason,
    },
    SubscriptionCancelled {
        identity: BackendConsumerSubscriptionIdentity,
    },
    LiveSubscriptionUpdated {
        identity: BackendConsumerSubscriptionIdentity,
        version: LogicalVersion,
        terminal: Option<LiveTerminal>,
    },
    LiveSubscriptionIdle {
        identity: BackendConsumerSubscriptionIdentity,
        latest_version: Option<LogicalVersion>,
        terminal: Option<LiveTerminal>,
    },
    LiveSubscriptionTerminal {
        identity: BackendConsumerSubscriptionIdentity,
        terminal: LiveTerminal,
        retained_version: Option<LogicalVersion>,
    },
    LoopbackDelivered {
        channel: BackendChannelIdentity,
        consumer_binding_id: RuntimeFilterBindingId,
        route_edge_id: BackendRouteEdgeId,
        version: LogicalVersion,
    },
    ConsumerRowsEvaluated {
        identity: BackendConsumerSubscriptionIdentity,
        logical_version: LogicalVersion,
        input_rows: u64,
        output_rows: u64,
    },
    ConsumerScanUnitEvaluated {
        identity: BackendConsumerSubscriptionIdentity,
        logical_version: LogicalVersion,
        decision: RuntimeFilterScanUnitDecision,
    },
    ConsumerScanUnitNotEvaluated {
        identity: BackendConsumerSubscriptionIdentity,
        observed_version: Option<LogicalVersion>,
        reason: RuntimeFilterScanUnitNotEvaluatedReason,
    },
}

pub(crate) trait BackendRuntimeFilterEventObserver: Send + Sync {
    fn record(&self, event: BackendRuntimeFilterEvent);
}

/// A no-op diagnostic observer. Production observation is owned by the
/// participant emitter and never depends on this optional side channel.
#[derive(Default)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub(crate) struct DiscardBackendRuntimeFilterEventObserver;

impl BackendRuntimeFilterEventObserver for DiscardBackendRuntimeFilterEventObserver {
    fn record(&self, _: BackendRuntimeFilterEvent) {}
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct CollectingBackendRuntimeFilterEventObserver(
    std::sync::Mutex<Vec<BackendRuntimeFilterEvent>>,
);

#[cfg(test)]
impl CollectingBackendRuntimeFilterEventObserver {
    pub(crate) fn events(&self) -> Vec<BackendRuntimeFilterEvent> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

#[cfg(test)]
impl BackendRuntimeFilterEventObserver for CollectingBackendRuntimeFilterEventObserver {
    fn record(&self, event: BackendRuntimeFilterEvent) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(event);
    }
}
