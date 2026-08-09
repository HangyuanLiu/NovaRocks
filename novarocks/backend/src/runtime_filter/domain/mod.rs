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
mod reducer;
mod reduction_state;
mod snapshot;
mod subscription;
mod transport;

pub(crate) use coverage::{
    BackendCoverage, BackendCoverageError, BackendCoverageProgress, BackendCoverageState,
    BackendCoverageWitnessId, BackendCoverageWitnessProgress,
};
pub(crate) use dedupe::{
    BackendContributionAdmission, BackendDeliveryAdmission, BackendIngressDedupe,
};
#[cfg(test)]
pub(crate) use events::CollectingBackendRuntimeFilterEventObserver;
pub(crate) use events::{
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendTransportEventIdentity,
    BackendTransportEventKind, DiscardBackendRuntimeFilterEventObserver,
};
pub(crate) use identity::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendParticipantIdentity,
    BackendProducerStreamIdentity, BackendRouteEdgeId, BackendTransportSequence,
};
pub(crate) use install::{BackendInstallPolicy, BackendInstallPolicyError};
pub(crate) use reducer::{MembershipReducer, ReducerError};
pub(crate) use reduction_state::{
    BackendReductionApply, BackendReductionState, BackendReductionStateError,
};
pub(crate) use snapshot::{
    BackendLogicalSnapshot, BackendLogicalSnapshotError, BackendReducedLogicalDomain,
    BackendReducedLogicalSnapshot,
};
pub(crate) use subscription::{
    BackendBlockingSubscription, BackendLiveSubscription, BackendSubscriptionError,
    BackendSubscriptionGroup,
};
pub(crate) use transport::{
    BackendAcceptStatus, BackendAckOutcome, BackendContributionRouteIdentity,
    BackendDeliveryRouteIdentity, BackendEnvelopeKind, BackendIngressResult,
    BackendProducerInstanceRouteIdentity, BackendProducerOpenMetadata, BackendReliableTransport,
    BackendRetryPolicy, BackendRetrySendOutcome, BackendRetryTick, BackendRouteIdentity,
    BackendRuntimeFilterEnvelope, BackendTransportEnvelope, BackendTransportError,
    BackendTransportFailOpenReason, BackendTransportResourceLimit,
};

#[cfg(test)]
mod tests;
