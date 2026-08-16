//! Validated, role-neutral native query lifecycle values.
//!
//! Modules are added incrementally as the Core parallel models are retired.

pub mod control;
pub mod error;
pub mod identity;
pub mod manifest;
pub mod query_options;
pub mod stage;
pub mod terminal;

pub use control::{
    FragmentLiveObservation, QueryAbortRequest, QueryControlAttach, QueryControlCommand,
    QueryControlEvent, QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryTerminalAck,
    QueryTerminalReportAck, QueryTerminalReportOutcome, QueryTerminationAck,
    QueryTerminationReason,
};
pub use error::{ContractError, ContractErrorCode};
pub use identity::{AttemptId, QueryExecutionId};
pub use manifest::{
    ExchangeRouteManifest, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantManifestDigest, ParticipantRole, QueryControlEndpoint, RuntimeFilterContribution,
};
pub use query_options::QueryOptions;
pub use stage::{
    QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome,
    QueryStartRequest, StageDigest, StageDigestVersion, StageFragment,
};
pub use terminal::{
    FragmentTerminalSnapshot, NegativeAttestation, ParticipantTerminalOutcome,
    QueryTerminalProfileContributionV1, QueryTerminalSnapshot, TerminalizationProof,
};
