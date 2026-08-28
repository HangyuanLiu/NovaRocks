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

mod coverage;
mod dedupe;
mod events;
mod identity;
mod install;
mod participant_install;
mod reducer;
mod reduction_state;
mod routing;
mod session;
mod snapshot;
mod subscription;
mod transport;

/// Per installed producer instance bound used by both the execution binding
/// and the terminal observation store. A partition id is never a free-form
/// observation key beyond this frozen bound.
pub(crate) const MAX_RUNTIME_FILTER_PRODUCER_PARTITIONS_PER_INSTANCE: u32 = 16_384;

pub(crate) use coverage::{
    BackendCoverage, BackendCoverageProgress, BackendCoverageState, BackendCoverageWitnessId,
};
pub(crate) use dedupe::{BackendDeliveryAdmission, BackendIngressDedupe};
#[cfg(test)]
pub(crate) use events::CollectingBackendRuntimeFilterEventObserver;
pub(crate) use events::{
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendTransportEventIdentity,
    BackendTransportEventKind,
};
pub(crate) use identity::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendParticipantIdentity,
    BackendProducerStreamIdentity, BackendRouteEdgeId, BackendTransportSequence,
};
pub(crate) use install::{BackendInstallPolicy, BackendInstallPolicyError};
pub(crate) use participant_install::{
    BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall,
    BackendFrontendFeedbackPublication, BackendMaterializationOwner, BackendMaterializationPolicy,
    BackendOutboundMaterializationGroup, BackendParticipantInstall, BackendProducerInstall,
};
pub(crate) use reducer::{MembershipReducer, ReducerError};
pub(crate) use reduction_state::{
    BackendReductionApply, BackendReductionState, BackendReductionStateError,
};
pub(crate) use routing::{
    BackendRemoteRoute, BackendRouteDecision, BackendRouteEndpoint, BackendRoutePeer,
    BackendRouteRole, BackendRoutingChannel, BackendRoutingEdge, BackendRoutingError,
    BackendRoutingShard,
};
pub(crate) use session::{
    BackendFrontendFeedbackOutcome, BackendFrontendFeedbackSink, BackendMaterializedDelivery,
    BackendMaterializedDeliverySink, BackendRuntimeFilterSession,
};
#[cfg(test)]
pub(crate) use snapshot::BackendLogicalSnapshot;
pub(crate) use snapshot::{BackendReducedLogicalDomain, BackendReducedLogicalSnapshot};
pub(crate) use subscription::{BackendSubscriptionError, BackendSubscriptionGroup};
pub(crate) use transport::{
    BackendAcceptStatus, BackendContributionRouteIdentity, BackendDeliveryRouteIdentity,
    BackendEnvelopeKind, BackendIngressResult, BackendProducerOpenMetadata,
    BackendTransportFailOpenReason,
};

#[cfg(test)]
mod tests;
