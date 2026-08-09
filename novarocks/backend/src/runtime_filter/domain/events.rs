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
//! Events are emitted to an observer at the point of state transition. They
//! are not an observation store: production installs [`DiscardBackendRuntimeFilterEventObserver`]
//! and RFO-8 owns any bounded retention or terminal handoff.

use novarocks_execution::runtime_filter::{
    ArtifactUnsupportedReason, LiveTerminal, LogicalVersion, RuntimeFilterBindingId,
    UnavailableReason,
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
}

pub(crate) trait BackendRuntimeFilterEventObserver: Send + Sync {
    fn record(&self, event: BackendRuntimeFilterEvent);
}

/// The production observer intentionally drops full Service events. Retention,
/// aggregation and terminal handoff belong to RFO-8, not this physical-domain
/// migration.
#[derive(Default)]
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
