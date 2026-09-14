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
pub const MAX_RUNTIME_FILTER_PRODUCER_PARTITIONS_PER_INSTANCE: u32 = 16_384;

pub use coverage::{
    BackendCoverage, BackendCoverageProgress, BackendCoverageState, BackendCoverageWitnessId,
};
pub use dedupe::{BackendDeliveryAdmission, BackendIngressDedupe};
#[cfg(test)]
pub use events::CollectingBackendRuntimeFilterEventObserver;
pub use events::{
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendTransportEventIdentity,
    BackendTransportEventKind,
};
pub use identity::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendParticipantIdentity,
    BackendProducerStreamIdentity, BackendRouteEdgeId, BackendTransportSequence,
};
pub use install::{BackendInstallPolicy, BackendInstallPolicyError};
pub use participant_install::{
    BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall,
    BackendFrontendFeedbackPublication, BackendMaterializationOwner, BackendMaterializationPolicy,
    BackendOutboundMaterializationGroup, BackendParticipantInstall, BackendProducerInstall,
};
pub use reducer::{MembershipReducer, ReducerError};
pub use reduction_state::{
    BackendReductionApply, BackendReductionState, BackendReductionStateError,
};
pub use routing::{
    BackendRemoteRoute, BackendRouteDecision, BackendRouteEndpoint, BackendRoutePeer,
    BackendRouteRole, BackendRoutingChannel, BackendRoutingEdge, BackendRoutingError,
    BackendRoutingShard,
};
pub use session::{
    BackendFrontendFeedbackOutcome, BackendFrontendFeedbackSink, BackendMaterializedDelivery,
    BackendMaterializedDeliverySink, BackendRuntimeFilterSession,
};
#[cfg(test)]
pub use snapshot::BackendLogicalSnapshot;
pub use snapshot::{BackendReducedLogicalDomain, BackendReducedLogicalSnapshot};
pub use subscription::{BackendSubscriptionError, BackendSubscriptionGroup};
pub use transport::{
    BackendAcceptStatus, BackendContributionRouteIdentity, BackendDeliveryRouteIdentity,
    BackendEnvelopeKind, BackendIngressResult, BackendProducerOpenMetadata,
    BackendTransportFailOpenReason,
};

#[cfg(test)]
mod tests;
