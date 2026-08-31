//! Validated, role-neutral native query lifecycle values.
//!
//! Modules are added incrementally as the Core parallel models are retired.

mod canonical;
pub mod control;
pub mod credential_lease;
pub mod identity;
pub mod manifest;
pub mod query_options;
pub mod scan_range;
pub mod stage;
pub mod terminal;

pub use control::{
    FragmentLiveObservation, QueryAbortRequest, QueryControlAttach, QueryControlCommand,
    QueryControlEvent, QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryTerminalAck,
    QueryTerminalReportAck, QueryTerminalReportOutcome, QueryTerminationAck,
    QueryTerminationReason, RuntimeFilterFeedbackEvent,
};
pub use credential_lease::{
    CredentialLeaseSecretEnvelope, decode_credential_lease_descriptor,
    decode_credential_lease_secret_envelope, encode_credential_lease_descriptor,
    encode_credential_lease_secret_envelope, validate_credential_lease_descriptors,
    validate_initial_credential_lease_envelopes,
};
pub use identity::{
    AttemptId, QueryExecutionId, decode_query_execution_id, encode_query_execution_id,
};
pub use manifest::{
    ExchangeRouteManifest, ParticipantAttemptRef, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantManifestDigest, QueryControlEndpoint, RuntimeFilterContribution,
};
pub use query_options::QueryOptions;
pub use scan_range::{FileScanRange, ScanRange, ScanRangeParams};
pub use stage::{
    QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome,
    QueryStartRequest, StageDigest, StageFragment,
};
pub use terminal::{
    FragmentTerminalOutcome, FragmentTerminalProfileTelemetry, FragmentTerminalSnapshot,
    NegativeAttestation, ParticipantTerminalOutcome, QueryTerminalProfileContributionTelemetry,
    QueryTerminalProfileContributionV1, QueryTerminalSnapshot, TerminalTelemetryUnavailable,
    TerminalizationProof,
};
