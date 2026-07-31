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

pub mod contract;
pub mod digest;
pub mod identity;
pub mod init_plan;
pub mod manifest;
pub mod metrics;
pub mod stage;
pub mod terminal;

pub use contract::{
    BackendQueryControl, FragmentLiveObservation, QueryAbortRequest, QueryControlAttach,
    QueryControlAttachment, QueryControlCommand, QueryControlEvent, QueryControlSession,
    QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryLifecycleError, QueryLifecycleErrorCode,
    QueryLifecycleIngress, QueryLifecycleTarget, QueryLifecycleTransport,
    QueryLifecycleTransportError, QueryLifecycleTransportErrorKind, QueryTerminalAck,
    QueryTerminalFallbackTransport, QueryTerminalIngress, QueryTerminalReportAck,
    QueryTerminalReportOutcome, QueryTerminationAck, QueryTerminationReason,
};
pub use contract::{decode_query_terminal_snapshot, encode_query_terminal_snapshot};
pub use identity::{AttemptId, QueryExecutionId};
pub(crate) use init_plan::QueryInitPlanHeader;
pub use init_plan::{
    QueryInitBarrier, QueryInitOptions, QueryInitParticipant, QueryInitPlan,
    QueryLifecycleAbortOutcome, QueryLifecycleLease, QueryLifecycleLeaseGuard,
};
pub use manifest::{
    ExchangeRouteManifest, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantManifestDigest, ParticipantQueryOptions, ParticipantRole, QueryControlEndpoint,
    RuntimeFilterContribution,
};
pub use stage::{
    QueryLaunchBarrier, QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck,
    QueryStartOutcome, QueryStartRequest, StageBatch, StageDigest, StageDigestVersion,
    StageFragment, StageParticipantBinding,
};
pub use terminal::{
    FragmentTerminalOutcome, FragmentTerminalSnapshot, ImmutableQueryTerminalRecord,
    QUERY_TERMINAL_SNAPSHOT_VERSION_V1, QueryTerminalProfileContributionV1, QueryTerminalSet,
    QueryTerminalSnapshot, QueryTerminalSnapshotDigest,
};
