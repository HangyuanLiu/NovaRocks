//! Validated generated wire values for native query lifecycle control.
//!
//! This module owns only neutral protocol carriers. Every wrapper contains one
//! generated message; role-local control streams, transport, and runtime
//! profile interpretation remain with their application owners.

use super::identity::{QueryExecutionId, decode_query_execution_id, encode_query_execution_id};
use super::manifest::{ParticipantBackendIdentity, ParticipantManifest, ParticipantManifestDigest};
use super::terminal::ParticipantTerminalOutcome;
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, novarocks};

/// The generated enum is the sole init-outcome representation.
pub use novarocks::QueryInitOutcome;
/// The generated enum is the sole termination-reason representation.
pub use novarocks::QueryTerminationReason;
/// The generated enum is the sole terminal-report outcome representation.
pub use novarocks::ReportQueryTerminalOutcome as QueryTerminalReportOutcome;

/// A validated `InitQueryRequest`.
///
/// The request carries the participant manifest alone. Its descriptor-derived
/// identity is not carried here: each role derives it from the manifest at its
/// own admission boundary and retains it with role-local state.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryInitRequest {
    raw: novarocks::InitQueryRequest,
}

impl QueryInitRequest {
    /// Frames one validated generated manifest.
    pub fn from_manifest(manifest: ParticipantManifest) -> Self {
        Self::parse(novarocks::InitQueryRequest {
            manifest: Some(manifest.as_proto().clone()),
        })
        .expect("validated participant manifest forms a valid InitQuery request")
    }

    pub fn parse(raw: novarocks::InitQueryRequest) -> Result<Self, ProtocolError> {
        required_manifest(&raw.manifest)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::InitQueryRequest {
        &self.raw
    }

    pub fn manifest(&self) -> Result<ParticipantManifest, ProtocolError> {
        required_manifest(&self.raw.manifest)
    }
}

/// A validated `InitQueryResponse` acknowledgement.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryInitAck {
    raw: novarocks::InitQueryResponse,
}

impl QueryInitAck {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        outcome: QueryInitOutcome,
    ) -> Self {
        Self::parse(novarocks::InitQueryResponse {
            execution_id: Some(encode_query_execution_id(execution_id)),
            init_digest: digest.as_bytes().to_vec(),
            outcome: outcome as i32,
        })
        .expect("validated lifecycle identities form a valid InitQuery acknowledgement")
    }
    pub fn parse(raw: novarocks::InitQueryResponse) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id, "query execution id is required")?;
        manifest_digest(&raw.init_digest)?;
        parse_init_outcome(raw.outcome)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::InitQueryResponse {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id, "query execution id is required")
    }

    pub fn digest(&self) -> Result<ParticipantManifestDigest, ProtocolError> {
        manifest_digest(&self.raw.init_digest)
    }

    pub fn outcome(&self) -> Result<QueryInitOutcome, ProtocolError> {
        parse_init_outcome(self.raw.outcome)
    }
}

/// Validated attach frame. It is deliberately separate from the later stream
/// commands carried by `QueryControlCommand`.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryControlAttach {
    raw: novarocks::QueryControlAttach,
}

impl QueryControlAttach {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        frontend_owner_epoch: u64,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::QueryControlAttach {
            execution_id: Some(encode_query_execution_id(execution_id)),
            init_digest: digest.as_bytes().to_vec(),
            frontend_owner_epoch,
        })
    }

    pub fn parse(raw: novarocks::QueryControlAttach) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id, "query execution id is required")?;
        manifest_digest(&raw.init_digest)?;
        if raw.frontend_owner_epoch == 0 {
            return Err(invalid(
                FieldPath::root("query_control_attach").field("frontend_owner_epoch"),
                "frontend owner epoch must be nonzero",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlAttach {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id, "query execution id is required")
    }

    pub fn digest(&self) -> Result<ParticipantManifestDigest, ProtocolError> {
        manifest_digest(&self.raw.init_digest)
    }

    pub const fn frontend_owner_epoch(&self) -> u64 {
        self.raw.frontend_owner_epoch
    }
}

/// A validated active-stream control request. The exact oneof remains in the
/// generated message, rather than being mirrored by a Rust command enum.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryControlCommand {
    raw: novarocks::QueryControlRequest,
}

impl QueryControlCommand {
    pub fn parse(raw: novarocks::QueryControlRequest) -> Result<Self, ProtocolError> {
        use novarocks::query_control_request::Command;

        match raw.command.as_ref() {
            Some(Command::Heartbeat(_)) | Some(Command::Finalize(_)) => {}
            Some(Command::Abort(abort)) if !abort.reason.trim().is_empty() => {}
            Some(Command::TerminalAck(ack)) => {
                QueryTerminalAck::parse(ack.clone())?;
            }
            Some(Command::Abort(_)) => {
                return Err(invalid(
                    FieldPath::root("query_control_request")
                        .field("command")
                        .field("abort")
                        .field("reason"),
                    "query control abort reason must not be empty",
                ));
            }
            Some(Command::Attach(_)) => {
                return Err(ProtocolError::new(
                    FieldPath::root("query_control_request").field("command"),
                    ProtocolErrorKind::InvalidValue,
                    "attach is not a query control command",
                ));
            }
            None => {
                return Err(missing(
                    FieldPath::root("query_control_request").field("command"),
                    "query control command is required",
                ));
            }
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlRequest {
        &self.raw
    }
}

/// A validated active-stream control response. Its exact oneof stays generated
/// so new wire variants cannot silently diverge from a parallel Rust enum.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryControlEvent {
    raw: novarocks::QueryControlResponse,
}

impl QueryControlEvent {
    pub fn parse(raw: novarocks::QueryControlResponse) -> Result<Self, ProtocolError> {
        use novarocks::query_control_response::Event;

        match raw.event.as_ref() {
            Some(Event::ControlReady(_))
            | Some(Event::HeartbeatAck(_))
            | Some(Event::LocalDrained(_)) => {}
            Some(Event::LocalFailure(failure))
                if !failure.code.trim().is_empty() && !failure.detail.trim().is_empty() => {}
            Some(Event::TerminationAccepted(accepted)) => {
                parse_termination_reason(accepted.reason)?;
            }
            Some(Event::TerminalOutcome(outcome)) => {
                ParticipantTerminalOutcome::parse(outcome.clone())?;
            }
            Some(Event::FragmentObservation(observation)) => {
                FragmentLiveObservation::parse(observation.clone())?;
            }
            Some(Event::LocalFailure(_)) => {
                return Err(invalid(
                    FieldPath::root("query_control_response")
                        .field("event")
                        .field("local_failure"),
                    "local failure code and detail must not be empty",
                ));
            }
            None => {
                return Err(missing(
                    FieldPath::root("query_control_response").field("event"),
                    "query control event is required",
                ));
            }
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlResponse {
        &self.raw
    }
}

/// A validated unary abort request.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryAbortRequest {
    raw: novarocks::AbortQueryRequest,
}

impl QueryAbortRequest {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        reason: impl Into<String>,
    ) -> Self {
        Self::parse(novarocks::AbortQueryRequest {
            execution_id: Some(encode_query_execution_id(execution_id)),
            init_digest: digest.as_bytes().to_vec(),
            reason: reason.into(),
        })
        .expect("caller must provide a nonempty abort reason")
    }
    pub fn parse(raw: novarocks::AbortQueryRequest) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id, "query execution id is required")?;
        manifest_digest(&raw.init_digest)?;
        if raw.reason.trim().is_empty() {
            return Err(invalid(
                FieldPath::root("abort_query_request").field("reason"),
                "abort reason must not be empty",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::AbortQueryRequest {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id, "query execution id is required")
    }

    pub fn digest(&self) -> Result<ParticipantManifestDigest, ProtocolError> {
        manifest_digest(&self.raw.init_digest)
    }

    pub fn reason(&self) -> &str {
        &self.raw.reason
    }
}

/// A validated unary abort acknowledgement.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminationAck {
    raw: novarocks::AbortQueryResponse,
}

impl QueryTerminationAck {
    pub fn new(execution_id: QueryExecutionId, reason: QueryTerminationReason) -> Self {
        Self::parse(novarocks::AbortQueryResponse {
            execution_id: Some(encode_query_execution_id(execution_id)),
            accepted_reason: reason as i32,
        })
        .expect("validated lifecycle identity and reason form a valid abort acknowledgement")
    }
    pub fn parse(raw: novarocks::AbortQueryResponse) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id, "query execution id is required")?;
        parse_termination_reason(raw.accepted_reason)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::AbortQueryResponse {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id, "query execution id is required")
    }

    pub fn accepted_reason(&self) -> Result<QueryTerminationReason, ProtocolError> {
        parse_termination_reason(self.raw.accepted_reason)
    }
}

/// A validated terminal acknowledgement carried as a control command.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalAck {
    raw: novarocks::QueryControlTerminalAck,
}

impl QueryTerminalAck {
    pub fn parse(raw: novarocks::QueryControlTerminalAck) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id, "query execution id is required")?;
        manifest_digest(&raw.init_digest)?;
        digest_array(
            &raw.snapshot_digest,
            "query terminal snapshot digest must be 32 bytes",
        )?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlTerminalAck {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id, "query execution id is required")
    }

    pub fn init_digest(&self) -> Result<ParticipantManifestDigest, ProtocolError> {
        manifest_digest(&self.raw.init_digest)
    }

    pub const fn version(&self) -> u32 {
        self.raw.snapshot_version
    }

    pub fn digest(&self) -> Result<[u8; 32], ProtocolError> {
        digest_array(
            &self.raw.snapshot_digest,
            "query terminal snapshot digest must be 32 bytes",
        )
    }
}

/// A validated response to the independent participant-terminal report RPC.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalReportAck {
    raw: novarocks::ReportQueryTerminalResponse,
}

impl QueryTerminalReportAck {
    pub fn new(
        outcome: QueryTerminalReportOutcome,
        detail: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::ReportQueryTerminalResponse {
            outcome: outcome as i32,
            detail: detail.into(),
        })
    }

    pub fn parse(raw: novarocks::ReportQueryTerminalResponse) -> Result<Self, ProtocolError> {
        parse_terminal_report_outcome(raw.outcome)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::ReportQueryTerminalResponse {
        &self.raw
    }

    pub fn outcome(&self) -> Result<QueryTerminalReportOutcome, ProtocolError> {
        parse_terminal_report_outcome(self.raw.outcome)
    }

    pub fn detail(&self) -> &str {
        &self.raw.detail
    }
}

/// A validated best-effort fragment observation. Runtime-profile bytes remain
/// an opaque generated value here; their role-local conversion is not a
/// lifecycle-control contract concern.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentLiveObservation {
    raw: novarocks::FragmentLiveObservation,
}

impl FragmentLiveObservation {
    pub fn parse(raw: novarocks::FragmentLiveObservation) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id, "query execution id is required")?;
        manifest_digest(&raw.init_digest)?;
        required_backend(
            &raw.backend,
            "fragment observation backend identity is required",
        )?;
        let fragment_id = raw.fragment_instance_id.ok_or_else(|| {
            missing(
                FieldPath::root("fragment_live_observation").field("fragment_instance_id"),
                "fragment observation instance id is required",
            )
        })?;
        if is_missing_unique_id(fragment_id) {
            return Err(invalid(
                FieldPath::root("fragment_live_observation").field("fragment_instance_id"),
                "fragment observation instance id must be nonzero",
            ));
        }
        if raw.sequence == 0 {
            return Err(invalid(
                FieldPath::root("fragment_live_observation").field("sequence"),
                "fragment observation sequence must be nonzero",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::FragmentLiveObservation {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id, "query execution id is required")
    }

    pub fn init_digest(&self) -> Result<ParticipantManifestDigest, ProtocolError> {
        manifest_digest(&self.raw.init_digest)
    }

    pub fn backend(&self) -> Result<ParticipantBackendIdentity, ProtocolError> {
        required_backend(
            &self.raw.backend,
            "fragment observation backend identity is required",
        )
    }

    pub fn fragment_instance_id(&self) -> Result<common::UniqueId, ProtocolError> {
        self.raw.fragment_instance_id.ok_or_else(|| {
            missing(
                FieldPath::root("fragment_live_observation").field("fragment_instance_id"),
                "fragment observation instance id is required",
            )
        })
    }

    pub const fn sequence(&self) -> u64 {
        self.raw.sequence
    }

    pub const fn input_rows(&self) -> u64 {
        self.raw.input_rows
    }

    pub const fn output_rows(&self) -> u64 {
        self.raw.output_rows
    }

    pub const fn elapsed_ms(&self) -> u64 {
        self.raw.elapsed_ms
    }

    pub const fn profile(&self) -> Option<&novarocks::RuntimeProfileTree> {
        self.raw.profile.as_ref()
    }
}

fn required_manifest(
    raw: &Option<novarocks::ParticipantManifest>,
) -> Result<ParticipantManifest, ProtocolError> {
    let raw = raw.clone().ok_or_else(|| {
        missing(
            FieldPath::root("init_query_request").field("manifest"),
            "participant manifest is required",
        )
    })?;
    ParticipantManifest::parse(raw)
}

fn required_execution_id(
    raw: &Option<novarocks::QueryExecutionId>,
    missing_detail: &'static str,
) -> Result<QueryExecutionId, ProtocolError> {
    let raw = raw
        .as_ref()
        .ok_or_else(|| missing(FieldPath::root("query_execution_id"), missing_detail))?;
    decode_query_execution_id(raw)
}

fn required_backend(
    raw: &Option<novarocks::ParticipantBackendIdentity>,
    missing_detail: &'static str,
) -> Result<ParticipantBackendIdentity, ProtocolError> {
    let raw = raw.clone().ok_or_else(|| {
        missing(
            FieldPath::root("participant_backend_identity"),
            missing_detail,
        )
    })?;
    ParticipantBackendIdentity::parse(raw)
}

fn manifest_digest(raw: &[u8]) -> Result<ParticipantManifestDigest, ProtocolError> {
    ParticipantManifestDigest::try_from_slice(raw)
}

fn digest_array(raw: &[u8], detail: &'static str) -> Result<[u8; 32], ProtocolError> {
    raw.try_into()
        .map_err(|_| invalid(FieldPath::root("snapshot_digest"), detail))
}

fn parse_init_outcome(raw: i32) -> Result<QueryInitOutcome, ProtocolError> {
    match QueryInitOutcome::try_from(raw) {
        Ok(
            outcome @ (QueryInitOutcome::QueryInitApplied
            | QueryInitOutcome::QueryInitAlreadyApplied
            | QueryInitOutcome::QueryInitRejectedConflict
            | QueryInitOutcome::QueryInitRejectedStaleBackend
            | QueryInitOutcome::QueryInitRejectedCapacity
            | QueryInitOutcome::QueryInitRejectedInvalidManifest
            | QueryInitOutcome::QueryInitRejectedTerminated
            | QueryInitOutcome::QueryInitRejectedBackendDraining
            | QueryInitOutcome::QueryInitRejectedBackendProcessMismatch),
        ) => Ok(outcome),
        Ok(QueryInitOutcome::Unspecified) | Err(_) => Err(ProtocolError::new(
            FieldPath::root("init_query_response").field("outcome"),
            ProtocolErrorKind::InvalidValue,
            format!("unknown query init outcome {raw}"),
        )),
    }
}

fn parse_termination_reason(raw: i32) -> Result<QueryTerminationReason, ProtocolError> {
    match QueryTerminationReason::try_from(raw) {
        Ok(
            reason @ (QueryTerminationReason::QueryTerminationCoordinatorAbort
            | QueryTerminationReason::QueryTerminationCoordinatorFinalize
            | QueryTerminationReason::QueryTerminationCoordinatorStreamLost
            | QueryTerminationReason::QueryTerminationCoordinatorHeartbeatTimeout
            | QueryTerminationReason::QueryTerminationLocalFailure
            | QueryTerminationReason::QueryTerminationPreStartTimeout),
        ) => Ok(reason),
        Ok(QueryTerminationReason::Unspecified) | Err(_) => Err(ProtocolError::new(
            FieldPath::root("abort_query_response").field("accepted_reason"),
            ProtocolErrorKind::InvalidValue,
            format!("unknown query termination reason {raw}"),
        )),
    }
}

fn parse_terminal_report_outcome(raw: i32) -> Result<QueryTerminalReportOutcome, ProtocolError> {
    match QueryTerminalReportOutcome::try_from(raw) {
        Ok(
            outcome @ (QueryTerminalReportOutcome::Accepted
            | QueryTerminalReportOutcome::AlreadyAccepted
            | QueryTerminalReportOutcome::RejectedConflict
            | QueryTerminalReportOutcome::RejectedGone),
        ) => Ok(outcome),
        Ok(QueryTerminalReportOutcome::Unspecified) | Err(_) => Err(ProtocolError::new(
            FieldPath::root("report_query_terminal_response").field("outcome"),
            ProtocolErrorKind::InvalidValue,
            format!("unknown query terminal report outcome {raw}"),
        )),
    }
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

const fn is_missing_unique_id(id: common::UniqueId) -> bool {
    id.hi == 0 && id.lo == 0
}

#[cfg(test)]
mod tests {
    use super::{
        FragmentLiveObservation, QueryAbortRequest, QueryControlAttach, QueryControlCommand,
        QueryControlEvent, QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryTerminalAck,
        QueryTerminalReportAck, QueryTerminalReportOutcome, QueryTerminationAck,
        QueryTerminationReason,
    };
    use crate::lifecycle::manifest::ParticipantManifest;
    use novarocks_proto_models::{common, novarocks};

    fn id(hi: i64, lo: i64) -> common::UniqueId {
        common::UniqueId { hi, lo }
    }

    fn endpoint(port: u32) -> novarocks::QueryControlEndpoint {
        novarocks::QueryControlEndpoint {
            host: "127.0.0.1".into(),
            port,
        }
    }

    fn manifest() -> novarocks::ParticipantManifest {
        novarocks::ParticipantManifest {
            execution_id: Some(execution_id()),
            backend: Some(novarocks::ParticipantBackendIdentity {
                endpoint: Some(endpoint(9030)),
                process_id: Some(novarocks::BackendProcessId {
                    value: vec![
                        0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97, 0x7b, 0x33, 0xd1,
                        0x88, 0xad, 0x1f, 0x06,
                    ],
                }),
            }),
            participant_roles: vec![1],
            expected_fragment_instance_ids: vec![id(11, 12)],
            query_options: Some(novarocks::QueryOptions::default()),
            query_deadline_unix_ms: 1_000,
            pre_start_timeout_ms: 30_000,
            report_endpoint: Some(endpoint(9031)),
            ..Default::default()
        }
    }

    fn execution_id() -> novarocks::QueryExecutionId {
        novarocks::QueryExecutionId {
            query_id: Some(id(5, 6)),
            attempt_id: 1,
        }
    }

    fn manifest_digest() -> Vec<u8> {
        ParticipantManifest::parse(manifest())
            .expect("valid manifest")
            .digest()
            .expect("digest")
            .as_bytes()
            .to_vec()
    }

    #[test]
    fn init_request_carries_the_manifest_and_requires_it() {
        let raw = novarocks::InitQueryRequest {
            manifest: Some(manifest()),
        };
        let parsed = QueryInitRequest::parse(raw.clone()).expect("valid request");
        assert_eq!(parsed.as_proto(), &raw);
        assert_eq!(
            parsed
                .manifest()
                .expect("manifest")
                .digest()
                .expect("digest")
                .as_bytes()
                .to_vec(),
            manifest_digest(),
            "the receiver derives the same identity the sender retains"
        );

        let error = QueryInitRequest::parse(novarocks::InitQueryRequest { manifest: None })
            .expect_err("the manifest is required");
        assert_eq!(error.detail(), "participant manifest is required");
    }

    #[test]
    fn validates_init_ack_and_unary_control_values() {
        let ack = QueryInitAck::parse(novarocks::InitQueryResponse {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            outcome: QueryInitOutcome::QueryInitApplied as i32,
        })
        .expect("valid init ack");
        assert_eq!(
            ack.outcome().expect("outcome"),
            QueryInitOutcome::QueryInitApplied
        );

        let error = QueryInitAck::parse(novarocks::InitQueryResponse {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            outcome: 99,
        })
        .expect_err("unknown init outcome");
        assert_eq!(error.detail(), "unknown query init outcome 99");

        let attach = QueryControlAttach::parse(novarocks::QueryControlAttach {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            frontend_owner_epoch: 1,
        })
        .expect("valid attach");
        assert_eq!(attach.frontend_owner_epoch(), 1);

        let error = QueryAbortRequest::parse(novarocks::AbortQueryRequest {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            reason: " ".into(),
        })
        .expect_err("empty abort reason");
        assert_eq!(error.detail(), "abort reason must not be empty");

        let termination = QueryTerminationAck::parse(novarocks::AbortQueryResponse {
            execution_id: Some(execution_id()),
            accepted_reason: QueryTerminationReason::QueryTerminationCoordinatorAbort as i32,
        })
        .expect("valid termination acknowledgement");
        assert_eq!(
            termination.accepted_reason().expect("reason"),
            QueryTerminationReason::QueryTerminationCoordinatorAbort
        );

        let error = QueryTerminationAck::parse(novarocks::AbortQueryResponse {
            execution_id: Some(execution_id()),
            accepted_reason: 99,
        })
        .expect_err("unknown termination reason");
        assert_eq!(error.detail(), "unknown query termination reason 99");
    }

    #[test]
    fn validates_control_oneofs_without_parallel_command_or_event_enums() {
        let command = QueryControlCommand::parse(novarocks::QueryControlRequest {
            command: Some(novarocks::query_control_request::Command::Heartbeat(
                novarocks::QueryControlHeartbeat {
                    sequence: 1,
                    sent_mono_ns: 2,
                },
            )),
        })
        .expect("valid heartbeat");
        assert!(matches!(
            command.as_proto().command.as_ref(),
            Some(novarocks::query_control_request::Command::Heartbeat(_))
        ));

        let error = QueryControlCommand::parse(novarocks::QueryControlRequest {
            command: Some(novarocks::query_control_request::Command::Attach(
                novarocks::QueryControlAttach::default(),
            )),
        })
        .expect_err("attach is not a post-attach command");
        assert_eq!(error.detail(), "attach is not a query control command");

        let event = QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(
                novarocks::query_control_response::Event::TerminationAccepted(
                    novarocks::QueryControlTerminationAccepted {
                        reason: QueryTerminationReason::QueryTerminationPreStartTimeout as i32,
                    },
                ),
            ),
        })
        .expect("valid termination event");
        assert!(event.as_proto().event.is_some());

        let error = QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(novarocks::query_control_response::Event::LocalFailure(
                novarocks::QueryControlLocalFailure::default(),
            )),
        })
        .expect_err("local failure requires both fields");
        assert_eq!(
            error.detail(),
            "local failure code and detail must not be empty"
        );
    }

    #[test]
    fn validates_terminal_ack_report_ack_and_fragment_observation() {
        let terminal_ack = QueryTerminalAck::parse(novarocks::QueryControlTerminalAck {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            snapshot_digest: vec![4; 32],
            ..Default::default()
        })
        .expect("valid terminal ack");
        assert_eq!(terminal_ack.digest().expect("digest"), [4; 32]);

        let report_ack = QueryTerminalReportAck::parse(novarocks::ReportQueryTerminalResponse {
            outcome: QueryTerminalReportOutcome::Accepted as i32,
            detail: "stored".into(),
        })
        .expect("valid report ack");
        assert_eq!(report_ack.detail(), "stored");

        let error = QueryTerminalReportAck::parse(novarocks::ReportQueryTerminalResponse {
            outcome: 0,
            ..Default::default()
        })
        .expect_err("unspecified outcome");
        assert_eq!(error.detail(), "unknown query terminal report outcome 0");

        let observation = FragmentLiveObservation::parse(novarocks::FragmentLiveObservation {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            backend: manifest().backend,
            fragment_instance_id: Some(id(11, 12)),
            sequence: 1,
            ..Default::default()
        })
        .expect("valid observation");
        assert_eq!(observation.sequence(), 1);

        let error = FragmentLiveObservation::parse(novarocks::FragmentLiveObservation {
            execution_id: Some(execution_id()),
            init_digest: manifest_digest(),
            backend: manifest().backend,
            fragment_instance_id: Some(id(0, 0)),
            sequence: 1,
            ..Default::default()
        })
        .expect_err("zero fragment id");
        assert_eq!(
            error.detail(),
            "fragment observation instance id must be nonzero"
        );
    }
}
