//! Validated generated wire values for native query lifecycle control.
//!
//! This module owns only neutral protocol carriers. Every wrapper contains one
//! generated message; role-local control streams, transport, and runtime
//! profile interpretation remain with their application owners. Confidential
//! credential lease frames are deliberately fail-closed here: callers must use
//! the explicit TLS ingress parser after verifying the actual connection.

use std::fmt;

use super::credential_lease::{
    CredentialLeaseSecretEnvelope, decode_credential_lease_secret_envelope,
    encode_credential_lease_secret_envelope, validate_initial_credential_lease_envelopes,
    validate_lease_epoch,
};
use super::identity::{QueryExecutionId, decode_query_execution_id, encode_query_execution_id};
use super::manifest::{ParticipantAttemptRef, ParticipantManifest, ParticipantManifestDigest};
use super::terminal::ParticipantTerminalOutcome;
use crate::catalog::{validate_catalog_load_failed, validate_catalog_load_state};
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
#[derive(Clone, PartialEq)]
pub struct QueryInitRequest {
    raw: novarocks::InitQueryRequest,
}

impl QueryInitRequest {
    /// Frames one validated generated manifest.
    pub fn from_manifest(manifest: ParticipantManifest) -> Self {
        Self::from_manifest_with_credential_lease_envelopes(manifest, [])
            .expect("manifest without lease descriptors forms a valid InitQuery request")
    }

    /// Retain the public manifest after the FE has finished every Init retry
    /// and scrubbed its confidential side channel. This value is for local
    /// lifecycle identity/terminal validation only and must never be sent as a
    /// new Init RPC: BE ingress deliberately rejects descriptors without their
    /// TLS envelopes.
    pub fn retain_manifest_after_confidential_send(manifest: ParticipantManifest) -> Self {
        Self {
            raw: novarocks::InitQueryRequest {
                manifest: Some(manifest.as_proto().clone()),
                credential_lease_envelopes: Vec::new(),
            },
        }
    }

    /// Frames an Init request that carries confidential values. This constructor
    /// is for the FE TLS sender path. BE ingress must use `parse_tls` only
    /// after it has independently verified the native connection is TLS.
    pub fn from_manifest_with_credential_lease_envelopes(
        manifest: ParticipantManifest,
        envelopes: impl IntoIterator<Item = CredentialLeaseSecretEnvelope>,
    ) -> Result<Self, ProtocolError> {
        let mut credential_lease_envelopes = envelopes
            .into_iter()
            .map(|envelope| encode_credential_lease_secret_envelope(&envelope))
            .collect::<Vec<_>>();
        credential_lease_envelopes.sort_by(|left, right| left.lease_id.cmp(&right.lease_id));
        Self::parse_tls(novarocks::InitQueryRequest {
            manifest: Some(manifest.as_proto().clone()),
            credential_lease_envelopes,
        })
    }

    /// Parses an Init request that does not carry confidential lease material.
    /// The default parser must not be used as an h2c bypass for vended values.
    pub fn parse(raw: novarocks::InitQueryRequest) -> Result<Self, ProtocolError> {
        if !raw.credential_lease_envelopes.is_empty() {
            return Err(ProtocolError::new(
                FieldPath::root("init_query_request").field("credential_lease_envelopes"),
                ProtocolErrorKind::InvalidValue,
                "credential lease envelopes require TLS-aware InitQuery ingress",
            ));
        }
        Self::parse_inner(raw)
    }

    /// Parses confidential Init material after the BE RPC ingress has checked
    /// that the concrete native transport is TLS. This type intentionally does
    /// not accept a boolean transport hint: an h2c caller cannot opt in by
    /// putting a claim in its protobuf body.
    pub fn parse_tls(raw: novarocks::InitQueryRequest) -> Result<Self, ProtocolError> {
        Self::parse_inner(raw)
    }

    fn parse_inner(raw: novarocks::InitQueryRequest) -> Result<Self, ProtocolError> {
        let manifest = required_manifest(&raw.manifest)?;
        validate_initial_credential_lease_envelopes(
            &manifest.as_proto().credential_lease_descriptors,
            &raw.credential_lease_envelopes,
            FieldPath::root("init_query_request"),
        )?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::InitQueryRequest {
        &self.raw
    }

    pub fn manifest(&self) -> Result<ParticipantManifest, ProtocolError> {
        required_manifest(&self.raw.manifest)
    }

    /// Projects confidential values only when the TLS-bound request owner
    /// explicitly asks for them. The generated envelope is not exposed by a
    /// secret-bearing Debug implementation.
    pub fn credential_lease_envelopes(
        &self,
    ) -> Result<Vec<CredentialLeaseSecretEnvelope>, ProtocolError> {
        self.raw
            .credential_lease_envelopes
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, envelope)| {
                decode_credential_lease_secret_envelope(
                    envelope,
                    FieldPath::root("init_query_request")
                        .field("credential_lease_envelopes")
                        .index(index),
                )
            })
            .collect()
    }
}

impl fmt::Debug for QueryInitRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryInitRequest")
            .field(
                "credential_lease_descriptor_count",
                &self
                    .raw
                    .manifest
                    .as_ref()
                    .map_or(0, |manifest| manifest.credential_lease_descriptors.len()),
            )
            .field(
                "credential_lease_envelope_count",
                &self.raw.credential_lease_envelopes.len(),
            )
            .field("credential_lease_material", &"[REDACTED]")
            .finish()
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
    pub fn new(participant: ParticipantAttemptRef) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::QueryControlAttach {
            participant: Some(participant.as_proto().clone()),
        })
    }

    pub fn parse(raw: novarocks::QueryControlAttach) -> Result<Self, ProtocolError> {
        required_participant(
            &raw.participant,
            FieldPath::root("query_control_attach").field("participant"),
            "query control attach participant reference is required",
        )?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlAttach {
        &self.raw
    }

    pub fn participant(&self) -> Result<ParticipantAttemptRef, ProtocolError> {
        required_participant(
            &self.raw.participant,
            FieldPath::root("query_control_attach").field("participant"),
            "query control attach participant reference is required",
        )
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        self.participant()?.execution_id()
    }
}

/// A validated active-stream control request. The exact oneof remains in the
/// generated message, rather than being mirrored by a Rust command enum.
#[derive(Clone, PartialEq)]
pub struct QueryControlCommand {
    raw: novarocks::QueryControlRequest,
}

impl QueryControlCommand {
    pub fn parse(raw: novarocks::QueryControlRequest) -> Result<Self, ProtocolError> {
        if matches!(
            raw.command.as_ref(),
            Some(novarocks::query_control_request::Command::CredentialLeasePrepare(_))
        ) {
            return Err(ProtocolError::new(
                FieldPath::root("query_control_request").field("command"),
                ProtocolErrorKind::InvalidValue,
                "credential lease prepare requires TLS-aware query-control ingress",
            ));
        }
        Self::parse_inner(raw)
    }

    /// Parses a control frame after the BE stream owner has independently
    /// verified its concrete native transport is TLS.
    pub fn parse_tls(raw: novarocks::QueryControlRequest) -> Result<Self, ProtocolError> {
        Self::parse_inner(raw)
    }

    fn parse_inner(raw: novarocks::QueryControlRequest) -> Result<Self, ProtocolError> {
        use novarocks::query_control_request::Command;

        match raw.command.as_ref() {
            Some(Command::Heartbeat(_)) | Some(Command::Finalize(_)) => {}
            Some(Command::Abort(abort)) if !abort.reason.trim().is_empty() => {}
            Some(Command::TerminalAck(ack)) => {
                QueryTerminalAck::parse(ack.clone())?;
            }
            Some(Command::CredentialLeasePrepare(prepare)) => {
                let envelope = prepare.envelope.clone().ok_or_else(|| {
                    missing(
                        FieldPath::root("query_control_request")
                            .field("command")
                            .field("credential_lease_prepare")
                            .field("envelope"),
                        "credential lease prepare envelope is required",
                    )
                })?;
                decode_credential_lease_secret_envelope(
                    envelope,
                    FieldPath::root("query_control_request")
                        .field("command")
                        .field("credential_lease_prepare")
                        .field("envelope"),
                )?;
            }
            Some(Command::CredentialLeaseCommit(commit)) => {
                validate_lease_epoch(
                    &commit.lease_id,
                    commit.epoch,
                    FieldPath::root("query_control_request")
                        .field("command")
                        .field("credential_lease_commit"),
                )?;
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

    pub fn credential_lease_prepare(
        &self,
    ) -> Result<Option<CredentialLeaseSecretEnvelope>, ProtocolError> {
        match self.raw.command.as_ref() {
            Some(novarocks::query_control_request::Command::CredentialLeasePrepare(prepare)) => {
                let envelope = prepare.envelope.clone().ok_or_else(|| {
                    missing(
                        FieldPath::root("query_control_request")
                            .field("command")
                            .field("credential_lease_prepare")
                            .field("envelope"),
                        "credential lease prepare envelope is required",
                    )
                })?;
                decode_credential_lease_secret_envelope(
                    envelope,
                    FieldPath::root("query_control_request")
                        .field("command")
                        .field("credential_lease_prepare")
                        .field("envelope"),
                )
                .map(Some)
            }
            _ => Ok(None),
        }
    }

    pub fn credential_lease_commit(
        &self,
    ) -> Result<Option<(novarocks_spi::connector::CredentialLeaseId, u64)>, ProtocolError> {
        match self.raw.command.as_ref() {
            Some(novarocks::query_control_request::Command::CredentialLeaseCommit(commit)) => {
                validate_lease_epoch(
                    &commit.lease_id,
                    commit.epoch,
                    FieldPath::root("query_control_request")
                        .field("command")
                        .field("credential_lease_commit"),
                )
                .map(Some)
            }
            _ => Ok(None),
        }
    }
}

impl fmt::Debug for QueryControlCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let has_confidential_prepare = matches!(
            self.raw.command.as_ref(),
            Some(novarocks::query_control_request::Command::CredentialLeasePrepare(_))
        );
        formatter
            .debug_struct("QueryControlCommand")
            .field(
                "has_confidential_credential_lease_prepare",
                &has_confidential_prepare,
            )
            .field(
                "credential_lease_material",
                &if has_confidential_prepare {
                    "[REDACTED]"
                } else {
                    "none"
                },
            )
            .finish()
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
            Some(Event::ControlReady(ready)) => {
                let state = ready.catalog_load_state.as_ref().ok_or_else(|| {
                    missing(
                        FieldPath::root("query_control_response")
                            .field("event")
                            .field("control_ready")
                            .field("catalog_load_state"),
                        "catalog load state is required",
                    )
                })?;
                validate_catalog_load_state(
                    state,
                    FieldPath::root("query_control_response")
                        .field("event")
                        .field("control_ready")
                        .field("catalog_load_state"),
                )?;
            }
            Some(Event::CatalogReady(_))
            | Some(Event::HeartbeatAck(_))
            | Some(Event::LocalDrained(_)) => {}
            Some(Event::CatalogLoadFailed(failure)) => {
                validate_catalog_load_failed(
                    failure,
                    FieldPath::root("query_control_response")
                        .field("event")
                        .field("catalog_load_failed"),
                )?;
            }
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
            Some(Event::RuntimeFilterFeedback(feedback)) => {
                RuntimeFilterFeedbackEvent::parse(feedback.clone())?;
            }
            Some(Event::CredentialLeasePrepared(prepared)) => {
                validate_lease_epoch(
                    &prepared.lease_id,
                    prepared.epoch,
                    FieldPath::root("query_control_response")
                        .field("event")
                        .field("credential_lease_prepared"),
                )?;
            }
            Some(Event::CredentialLeaseCommitted(committed)) => {
                validate_lease_epoch(
                    &committed.lease_id,
                    committed.epoch,
                    FieldPath::root("query_control_response")
                        .field("event")
                        .field("credential_lease_committed"),
                )?;
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

/// A structurally validated terminal runtime-filter feedback event. Semantic
/// authorization against the frozen deployment remains FE application work.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterFeedbackEvent {
    raw: novarocks::RuntimeFilterFeedbackEvent,
}

impl RuntimeFilterFeedbackEvent {
    pub fn parse(raw: novarocks::RuntimeFilterFeedbackEvent) -> Result<Self, ProtocolError> {
        use novarocks::runtime_filter_feedback_event::TerminalOutcome;

        required_participant(
            &raw.participant_attempt,
            FieldPath::root("runtime_filter_feedback_event").field("participant_attempt"),
            "feedback participant reference is required",
        )?;
        if raw.participant_id == 0 {
            return Err(invalid(
                FieldPath::root("runtime_filter_feedback_event").field("participant_id"),
                "feedback participant id must be nonzero",
            ));
        }
        if raw.deployment_epoch == 0 {
            return Err(invalid(
                FieldPath::root("runtime_filter_feedback_event").field("deployment_epoch"),
                "feedback deployment epoch must be nonzero",
            ));
        }
        if raw.channel_id == 0 {
            return Err(invalid(
                FieldPath::root("runtime_filter_feedback_event").field("channel_id"),
                "feedback channel id must be nonzero",
            ));
        }
        digest_array(
            &raw.contract_digest,
            "feedback contract digest must be 32 bytes",
        )?;
        match raw.terminal_outcome.as_ref() {
            Some(TerminalOutcome::CanonicalDomain(domain))
                if !domain.is_empty() && domain.len() <= 64 * 1024 => {}
            Some(TerminalOutcome::UnavailableReason(reason))
                if matches!(
                    novarocks::RuntimeFilterFeedbackUnavailableReason::try_from(*reason),
                    Ok(
                        novarocks::RuntimeFilterFeedbackUnavailableReason::DomainBudget
                            | novarocks::RuntimeFilterFeedbackUnavailableReason::TypeUnsupported
                            | novarocks::RuntimeFilterFeedbackUnavailableReason::ReductionUnavailable
                            | novarocks::RuntimeFilterFeedbackUnavailableReason::ProducerUnavailable
                    )
                ) => {}
            Some(TerminalOutcome::CanonicalDomain(_)) => {
                return Err(invalid(
                    FieldPath::root("runtime_filter_feedback_event")
                        .field("terminal_outcome")
                        .field("canonical_domain"),
                    "feedback domain must be nonempty and at most 65536 bytes",
                ));
            }
            Some(TerminalOutcome::UnavailableReason(reason)) => {
                return Err(invalid(
                    FieldPath::root("runtime_filter_feedback_event")
                        .field("terminal_outcome")
                        .field("unavailable_reason"),
                    format!("unknown runtime filter feedback unavailable reason {reason}"),
                ));
            }
            None => {
                return Err(missing(
                    FieldPath::root("runtime_filter_feedback_event").field("terminal_outcome"),
                    "feedback terminal outcome is required",
                ));
            }
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::RuntimeFilterFeedbackEvent {
        &self.raw
    }

    pub fn participant(&self) -> Result<ParticipantAttemptRef, ProtocolError> {
        required_participant(
            &self.raw.participant_attempt,
            FieldPath::root("runtime_filter_feedback_event").field("participant_attempt"),
            "feedback participant reference is required",
        )
    }
}

/// A validated unary abort request.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryAbortRequest {
    raw: novarocks::AbortQueryRequest,
}

impl QueryAbortRequest {
    pub fn new(
        participant: ParticipantAttemptRef,
        digest: ParticipantManifestDigest,
        reason: impl Into<String>,
    ) -> Self {
        Self::parse(novarocks::AbortQueryRequest {
            init_digest: digest.as_bytes().to_vec(),
            reason: reason.into(),
            participant: Some(participant.as_proto().clone()),
        })
        .expect("caller must provide a nonempty abort reason")
    }
    pub fn parse(raw: novarocks::AbortQueryRequest) -> Result<Self, ProtocolError> {
        required_participant(
            &raw.participant,
            FieldPath::root("abort_query_request").field("participant"),
            "abort participant reference is required",
        )?;
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
        self.participant()?.execution_id()
    }

    pub fn participant(&self) -> Result<ParticipantAttemptRef, ProtocolError> {
        required_participant(
            &self.raw.participant,
            FieldPath::root("abort_query_request").field("participant"),
            "abort participant reference is required",
        )
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
        let participant = raw.participant.clone().ok_or_else(|| {
            missing(
                FieldPath::root("query_control_terminal_ack").field("participant"),
                "query terminal acknowledgement participant reference is required",
            )
        })?;
        ParticipantAttemptRef::parse(participant)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlTerminalAck {
        &self.raw
    }

    pub fn participant(&self) -> Result<ParticipantAttemptRef, ProtocolError> {
        let participant = self.raw.participant.clone().ok_or_else(|| {
            missing(
                FieldPath::root("query_control_terminal_ack").field("participant"),
                "query terminal acknowledgement participant reference is required",
            )
        })?;
        ParticipantAttemptRef::parse(participant)
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
        required_participant(
            &raw.participant,
            FieldPath::root("fragment_live_observation").field("participant"),
            "fragment observation participant reference is required",
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

    pub fn participant(&self) -> Result<ParticipantAttemptRef, ProtocolError> {
        required_participant(
            &self.raw.participant,
            FieldPath::root("fragment_live_observation").field("participant"),
            "fragment observation participant reference is required",
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

fn required_participant(
    raw: &Option<novarocks::ParticipantAttemptRef>,
    path: FieldPath,
    missing_detail: &'static str,
) -> Result<ParticipantAttemptRef, ProtocolError> {
    let raw = raw.clone().ok_or_else(|| missing(path, missing_detail))?;
    ParticipantAttemptRef::parse(raw)
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
            | QueryInitOutcome::QueryInitRejectedBackendProcessMismatch
            | QueryInitOutcome::QueryInitRejectedCompatibilityMismatch),
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
    use novarocks_proto_models::{catalog, common, novarocks};

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
            expected_fragment_instance_ids: vec![id(11, 12)],
            query_options: Some(novarocks::QueryOptions::default()),
            query_deadline_unix_ms: 1_000,
            pre_start_timeout_ms: 30_000,
            report_endpoint: Some(endpoint(9031)),
            native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                value: vec![0x71; 32],
            }),
            catalog_set: Some(catalog::CatalogSet { catalogs: vec![] }),
            ..Default::default()
        }
    }

    fn vended_lease_init() -> novarocks::InitQueryRequest {
        let mut manifest = manifest();
        manifest.catalog_set = Some(catalog::CatalogSet {
            catalogs: vec![catalog::CatalogProperties {
                handle: Some(catalog::CatalogHandle {
                    catalog_name: "warehouse".to_owned(),
                    version: vec![7; 32],
                }),
                provider_kind: catalog::CatalogProviderKind::Iceberg as i32,
                config_format_version: 1,
                execution_properties: vec![],
                credential_bindings: vec![catalog::CatalogCredentialBinding {
                    purpose: catalog::CatalogCredentialPurpose::ObjectStoreData as i32,
                    consumer_role: catalog::CredentialConsumerRole::FrontendAndBackend as i32,
                    mode: Some(catalog::catalog_credential_binding::Mode::VendedCredential(
                        catalog::VendedCredential {},
                    )),
                }],
            }],
        });
        manifest.credential_lease_descriptors = vec![novarocks::CredentialLeaseDescriptor {
            lease_id: vec![1; 16],
            epoch: 3,
            owner: Some(catalog::CatalogHandle {
                catalog_name: "warehouse".to_owned(),
                version: vec![7; 32],
            }),
            provider: novarocks::CredentialLeaseProvider::S3 as i32,
            prefixes: vec!["s3://bucket/data".to_owned()],
            not_after_unix_ms: 99,
            refresh_capable: true,
            storage_access_domain_id: vec![8; 32],
        }];
        novarocks::InitQueryRequest {
            manifest: Some(manifest),
            credential_lease_envelopes: vec![novarocks::CredentialLeaseSecretEnvelope {
                lease_id: vec![1; 16],
                epoch: 3,
                s3: Some(novarocks::CredentialLeaseS3SecretMaterial {
                    access_key_id: "cca-access-canary".to_owned(),
                    secret_access_key: "cca-secret-canary".to_owned(),
                    session_token: "cca-token-canary".to_owned(),
                    session_token_expires_at_unix_ms: 99,
                }),
            }],
        }
    }

    fn execution_id() -> novarocks::QueryExecutionId {
        novarocks::QueryExecutionId {
            query_id: Some(id(5, 6)),
            attempt_id: 1,
        }
    }

    fn participant() -> novarocks::ParticipantAttemptRef {
        novarocks::ParticipantAttemptRef {
            execution_id: Some(execution_id()),
            backend_process_id: manifest().backend.and_then(|backend| backend.process_id),
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
            credential_lease_envelopes: vec![],
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

        let error = QueryInitRequest::parse(novarocks::InitQueryRequest {
            manifest: None,
            credential_lease_envelopes: vec![],
        })
        .expect_err("the manifest is required");
        assert_eq!(error.detail(), "participant manifest is required");
    }

    #[test]
    fn confidential_lease_ingress_requires_the_explicit_tls_parser_and_redacts_debug() {
        let init = vended_lease_init();
        let error = QueryInitRequest::parse(init.clone())
            .expect_err("ordinary ingress must reject confidential material");
        assert!(error.detail().contains("TLS-aware"));
        let parsed = QueryInitRequest::parse_tls(init).expect("TLS ingress accepts valid envelope");
        let rendered = format!("{parsed:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("cca-secret-canary"));
        assert_eq!(
            parsed.credential_lease_envelopes().expect("envelope").len(),
            1
        );
        let digest = parsed
            .manifest()
            .expect("manifest")
            .digest()
            .expect("descriptor digest");
        let mut changed_value = parsed.as_proto().clone();
        changed_value.credential_lease_envelopes[0]
            .s3
            .as_mut()
            .expect("S3 material")
            .secret_access_key = "cca-secret-canary-replaced".to_owned();
        let changed_value = QueryInitRequest::parse_tls(changed_value)
            .expect("same descriptor with different secret remains structurally valid");
        assert_eq!(
            digest,
            changed_value
                .manifest()
                .expect("manifest")
                .digest()
                .expect("descriptor digest"),
            "confidential material must never enter the manifest digest"
        );

        let command = novarocks::QueryControlRequest {
            command: Some(
                novarocks::query_control_request::Command::CredentialLeasePrepare(
                    novarocks::CredentialLeasePrepare {
                        envelope: parsed
                            .as_proto()
                            .credential_lease_envelopes
                            .first()
                            .cloned(),
                    },
                ),
            ),
        };
        assert!(QueryControlCommand::parse(command.clone()).is_err());
        let parsed = QueryControlCommand::parse_tls(command).expect("TLS control ingress");
        assert!(format!("{parsed:?}").contains("[REDACTED]"));
        assert!(!format!("{parsed:?}").contains("cca-secret-canary"));
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
            participant: Some(participant()),
        })
        .expect("valid attach");
        assert_eq!(
            attach
                .participant()
                .expect("participant")
                .execution_id()
                .expect("execution id"),
            super::decode_query_execution_id(&execution_id()).expect("execution id")
        );

        let error = QueryAbortRequest::parse(novarocks::AbortQueryRequest {
            init_digest: manifest_digest(),
            reason: " ".into(),
            participant: Some(participant()),
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
    fn validates_terminal_runtime_filter_feedback_shape() {
        let event = QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(
                novarocks::query_control_response::Event::RuntimeFilterFeedback(
                    novarocks::RuntimeFilterFeedbackEvent {
                        participant_id: 1,
                        deployment_epoch: 1,
                        channel_id: 1,
                        contract_digest: vec![9; 32],
                        terminal_outcome: Some(
                            novarocks::runtime_filter_feedback_event::TerminalOutcome::CanonicalDomain(
                                b"NRFF\x01\x03".to_vec(),
                            ),
                        ),
                        participant_attempt: Some(participant()),
                    },
                ),
            ),
        })
        .expect("valid feedback event");
        assert!(matches!(
            event.as_proto().event,
            Some(novarocks::query_control_response::Event::RuntimeFilterFeedback(_))
        ));

        let error = QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(
                novarocks::query_control_response::Event::RuntimeFilterFeedback(
                    novarocks::RuntimeFilterFeedbackEvent {
                        participant_id: 1,
                        deployment_epoch: 1,
                        channel_id: 1,
                        contract_digest: vec![9; 31],
                        terminal_outcome: Some(
                            novarocks::runtime_filter_feedback_event::TerminalOutcome::UnavailableReason(0),
                        ),
                        participant_attempt: Some(participant()),
                    },
                ),
            ),
        })
        .expect_err("invalid feedback is rejected at the protocol boundary");
        assert_eq!(error.detail(), "feedback contract digest must be 32 bytes");
    }

    #[test]
    fn validates_terminal_ack_report_ack_and_fragment_observation() {
        let terminal_ack = QueryTerminalAck::parse(novarocks::QueryControlTerminalAck {
            participant: Some(novarocks::ParticipantAttemptRef {
                execution_id: Some(execution_id()),
                backend_process_id: manifest().backend.and_then(|backend| backend.process_id),
            }),
        })
        .expect("valid terminal ack");
        assert_eq!(
            terminal_ack
                .participant()
                .expect("participant")
                .execution_id()
                .expect("execution id")
                .query_id()
                .high(),
            5
        );

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
            fragment_instance_id: Some(id(11, 12)),
            sequence: 1,
            participant: Some(participant()),
            ..Default::default()
        })
        .expect("valid observation");
        assert_eq!(observation.sequence(), 1);

        let error = FragmentLiveObservation::parse(novarocks::FragmentLiveObservation {
            fragment_instance_id: Some(id(0, 0)),
            sequence: 1,
            participant: Some(participant()),
            ..Default::default()
        })
        .expect_err("zero fragment id");
        assert_eq!(
            error.detail(),
            "fragment observation instance id must be nonzero"
        );
    }
}
