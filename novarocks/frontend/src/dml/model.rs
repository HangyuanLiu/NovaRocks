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

use std::collections::BTreeMap;
use std::fmt;

use crate::state_store::coordination::FencingToken;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorWriteReceipt,
    ExternalMutationEvidence, LakePublicationId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Ordinary DML journal values are intentionally an atomic format. There is
/// no persisted predecessor to read or migrate.
///
/// CP-3D cuts v7 to v8. A v7 operation record predates the CTAS catalog-fence
/// and retention invariant, so it may describe a staged create, publish, or
/// abort whose external authority and historical obligation cannot be rebuilt
/// after restart. v8 code must never interpret such a record: the journal fails
/// the read explicitly instead of migrating or dual-reading it.
pub const DML_OPERATION_SCHEMA_VERSION: u8 = 8;
pub const DML_UNFINISHED_SCHEMA_VERSION: u8 = 1;
pub const DML_COORDINATION_RESOURCE_CODEC_VERSION: u8 = 1;
pub const DML_RECOVERY_DUE_SCHEMA_VERSION: u8 = 1;
pub const DML_RECOVERY_SHARD_COUNT: u8 = 16;
pub const DML_FOREGROUND_RECOVERY_VISIBILITY_MS: i64 = 18_000;
pub const DML_RECOVERY_PAGE_SIZE: usize = 128;
pub const DML_EXTERNAL_FACT_ENCODED_LIMIT: usize = 16 * 1024;
pub const DML_CONNECTOR_WRITE_WIRE_LIMIT: usize = 128 * 1024;
/// CP-3B external operation fence receipt codec. The frontend stores identity,
/// scalar generation values, digests, and a bounded opaque provider payload.
pub const DML_EXTERNAL_FENCE_CODEC_VERSION: u8 = 1;
/// CP-3B historical write recovery codec.
pub const DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION: u8 = 1;
/// Raw byte bound for one opaque provider payload. The durable form is
/// lowercase hex, so a payload contributes at most twice this many encoded
/// bytes plus JSON framing.
pub const DML_OPAQUE_PAYLOAD_LIMIT: usize = 4 * 1024;
/// Encoded bound for one complete external fence receipt record: one opaque
/// payload plus identity, generation, and digest framing.
pub const DML_EXTERNAL_FENCE_ENCODED_LIMIT: usize = 12 * 1024;
/// Encoded bound for one complete historical write recovery record: the sealed
/// request (with the old fence receipt), the raised fence receipt, and up to
/// three opaque payloads.
pub const DML_HISTORICAL_WRITE_RECOVERY_ENCODED_LIMIT: usize = 48 * 1024;
/// CP-3C direct data-mutation external fence receipt codec. The record wraps the
/// shared CP-3B fence carrier with the direct-mutation family and, for ADD
/// FILES, the immutable source scope the fence was minted for.
pub const DML_DIRECT_MUTATION_FENCE_CODEC_VERSION: u8 = 1;
/// CP-3C historical data-mutation recovery codec.
pub const DML_HISTORICAL_DATA_MUTATION_RECOVERY_CODEC_VERSION: u8 = 1;
/// Encoded bound for one complete direct-mutation fence receipt record: the
/// wrapped fence receipt plus family and source-scope framing.
pub const DML_DIRECT_MUTATION_FENCE_ENCODED_LIMIT: usize = DML_EXTERNAL_FENCE_ENCODED_LIMIT + 1024;
/// Encoded bound for one complete historical data-mutation recovery record: the
/// sealed request (with the old fence receipt), the raised fence receipt, and up
/// to three opaque payloads.
pub const DML_HISTORICAL_DATA_MUTATION_RECOVERY_ENCODED_LIMIT: usize = 48 * 1024;
/// CTAS retains four phase facts in one StateStore value. Bound each complete
/// fact envelope, not each individual string, so the four-fact maximum leaves
/// room for operation identity, target facts, digests, and JSON framing.
pub const DML_CTAS_FACT_ENCODED_LIMIT: usize = 8 * 1024;
pub const DML_CTAS_TOTAL_FACT_ENCODED_LIMIT: usize = 4 * DML_CTAS_FACT_ENCODED_LIMIT;
pub const CTAS_CREATE_POLICY_FAIL_IF_EXISTS: &str = "FAIL_IF_EXISTS";
pub const CTAS_CREATE_POLICY_NO_OP_IF_EXISTS: &str = "NO_OP_IF_EXISTS";

/// Historical DML naming for the single statement publication identity.
pub type DmlOperationId = LakePublicationId;

/// Canonical CP-1 fencing-token v1 bytes retained for durable audit and
/// recovery identity. This is deliberately not a serialized `LeaseFence`:
/// the exact StateStore record version remains a live-guard capability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DmlFencingTokenV1(Vec<u8>);

impl DmlFencingTokenV1 {
    pub fn try_from_token(token: &FencingToken) -> Result<Self, String> {
        token
            .encode_v1()
            .map(|bytes| Self(bytes.to_vec()))
            .map_err(|error| error.to_string())
    }

    pub fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, String> {
        let token = FencingToken::decode_v1(Bytes::copy_from_slice(&bytes))
            .map_err(|error| error.to_string())?;
        let canonical = token.encode_v1().map_err(|error| error.to_string())?;
        if canonical.as_ref() != bytes.as_slice() {
            return Err("DML fencing token is not canonical v1 encoding".to_string());
        }
        Ok(Self(bytes))
    }

    pub fn try_decode(&self) -> Result<FencingToken, String> {
        Self::try_from_bytes(self.0.clone()).and_then(|canonical| {
            FencingToken::decode_v1(Bytes::from(canonical.0)).map_err(|error| error.to_string())
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Durable identity of the live operation lease that last claimed an
/// operation. UUIDv7 holder/attempt identities are intentionally separate
/// from the statement's provider-facing `attempt_id` string.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlCoordinationProvenance {
    pub resource_codec_version: u8,
    pub holder_id: Uuid,
    pub coordination_attempt_id: Uuid,
    pub fencing_token: DmlFencingTokenV1,
    pub acquired_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlCoordinationClaimRequest {
    pub operation_id: DmlOperationId,
    pub expected_revision: u64,
    pub mutation_id: Uuid,
    pub provenance: DmlCoordinationProvenance,
    pub recovery_due_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlRecoveryDueRescheduleRequest {
    pub operation_id: DmlOperationId,
    pub expected_revision: u64,
    pub mutation_id: Uuid,
    pub recovery_due_at_ms: Option<i64>,
}

/// A bounded due-index result. Every field is copied from both the operation
/// and its index value so callers can reject a stale candidate after lease
/// acquisition without treating the scan as authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlRecoveryCandidate {
    pub operation_id: DmlOperationId,
    pub operation_revision: u64,
    pub last_mutation_id: Uuid,
    pub coordination_attempt_id: Option<Uuid>,
    pub recovery_due_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationKind {
    InsertAppend,
    InsertOverwrite,
    RowDelta,
    MvRefresh,
    Maintenance,
    CreateTableAsSelect,
    Truncate,
    AddFiles,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationState {
    Preparing,
    Writing,
    Collecting,
    Committing,
    Committed,
    CommitUnknown,
    Finalizing,
    Finalized,
    Aborting,
    Aborted,
    FailedKnownUncommitted,
    FinalizeFailedKnownCommitted,
}

impl OperationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Writing => "WRITING",
            Self::Collecting => "COLLECTING",
            Self::Committing => "COMMITTING",
            Self::Committed => "COMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::Finalizing => "FINALIZING",
            Self::Finalized => "FINALIZED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::FailedKnownUncommitted => "FAILED_KNOWN_UNCOMMITTED",
            Self::FinalizeFailedKnownCommitted => "FINALIZE_FAILED_KNOWN_COMMITTED",
        }
    }

    pub const fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::Aborted | Self::FailedKnownUncommitted
        )
    }
}

pub fn validate_operation_transition(
    from: OperationState,
    to: OperationState,
) -> Result<(), String> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (OperationState::Preparing, OperationState::Writing)
            | (OperationState::Preparing, OperationState::Committing)
            | (OperationState::Preparing, OperationState::Aborting)
            | (
                OperationState::Preparing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Writing, OperationState::Collecting)
            | (OperationState::Writing, OperationState::Committing)
            | (OperationState::Writing, OperationState::Aborting)
            | (
                OperationState::Writing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Collecting, OperationState::Committing)
            | (OperationState::Collecting, OperationState::Aborting)
            | (
                OperationState::Collecting,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Committing, OperationState::Committed)
            | (OperationState::Committing, OperationState::CommitUnknown)
            | (
                OperationState::Committing,
                OperationState::FinalizeFailedKnownCommitted
            )
            | (
                OperationState::Committing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::CommitUnknown, OperationState::Committed)
            | (
                OperationState::CommitUnknown,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Committed, OperationState::Finalizing)
            | (OperationState::Committed, OperationState::Finalized)
            | (OperationState::Finalizing, OperationState::Finalized)
            | (
                OperationState::Finalizing,
                OperationState::FinalizeFailedKnownCommitted
            )
            | (OperationState::Finalizing, OperationState::CommitUnknown)
            | (
                OperationState::FinalizeFailedKnownCommitted,
                OperationState::Finalizing
            )
            | (OperationState::Aborting, OperationState::Aborted)
            | (OperationState::Aborting, OperationState::Committed)
            | (OperationState::Aborting, OperationState::CommitUnknown)
            | (
                OperationState::Aborting,
                OperationState::FinalizeFailedKnownCommitted
            )
            | (
                OperationState::Aborting,
                OperationState::FailedKnownUncommitted
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(format!(
            "invalid DML operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        ))
    }
}

/// Validate a statement-payload transition without widening the lifecycle of
/// existing INSERT, DELETE, MV, or maintenance operations.
///
/// CTAS is a multi-effect saga. Its prepare, writer, publish, and staged-abort
/// effects can each become unknown, and a failed/conflicting publish must move
/// from publication into handle-scoped staging cleanup. Those edges are not
/// valid for the ordinary single-write transaction runner.
pub fn validate_statement_operation_transition(
    operation_kind: OperationKind,
    from: OperationState,
    to: OperationState,
) -> Result<(), String> {
    if validate_operation_transition(from, to).is_ok() {
        return Ok(());
    }
    let ctas_allowed = operation_kind == OperationKind::CreateTableAsSelect
        && matches!(
            (from, to),
            (OperationState::Preparing, OperationState::CommitUnknown)
                | (OperationState::Writing, OperationState::CommitUnknown)
                | (OperationState::Committing, OperationState::Aborting)
                | (OperationState::Aborting, OperationState::CommitUnknown)
                | (OperationState::CommitUnknown, OperationState::Aborting)
                | (OperationState::CommitUnknown, OperationState::Writing)
                | (OperationState::CommitUnknown, OperationState::Committing)
        );
    if ctas_allowed {
        Ok(())
    } else {
        Err(format!(
            "invalid DML operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    #[serde(default)]
    pub ref_name: Option<String>,
}

/// A bounded SPI-owned receipt envelope stored without inspecting its provider
/// payload. JSON serializes the opaque wire as a byte array; decoding uses the
/// SPI envelope codec only and never projects provider facts into the frontend.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectorWriteReceiptWire(pub Vec<u8>);

impl ConnectorWriteReceiptWire {
    pub fn try_from_receipt(receipt: &ConnectorWriteReceipt) -> Result<Self, String> {
        let wire = receipt
            .try_to_wire_v1()
            .map_err(|error| error.to_string())?;
        Self::try_from_wire(wire.to_vec())
    }

    pub fn try_from_wire(wire: Vec<u8>) -> Result<Self, String> {
        if wire.is_empty() || wire.len() > DML_CONNECTOR_WRITE_WIRE_LIMIT {
            return Err(
                "connector write receipt wire is empty or exceeds journal bound".to_string(),
            );
        }
        ConnectorWriteReceipt::try_from_wire_v1(&wire).map_err(|error| error.to_string())?;
        Ok(Self(wire))
    }

    pub fn try_decode(&self) -> Result<ConnectorWriteReceipt, String> {
        ConnectorWriteReceipt::try_from_wire_v1(&self.0).map_err(|error| error.to_string())
    }
}

/// A bounded SPI-owned reconciliation envelope stored without inspecting its
/// provider payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExternalMutationEvidenceWire(pub Vec<u8>);

impl ExternalMutationEvidenceWire {
    pub fn try_from_evidence(evidence: &ExternalMutationEvidence) -> Result<Self, String> {
        let wire = evidence
            .try_to_wire_v1()
            .map_err(|error| error.to_string())?;
        Self::try_from_wire(wire.to_vec())
    }

    pub fn try_from_wire(wire: Vec<u8>) -> Result<Self, String> {
        if wire.is_empty() || wire.len() > DML_CONNECTOR_WRITE_WIRE_LIMIT {
            return Err(
                "external mutation evidence wire is empty or exceeds journal bound".to_string(),
            );
        }
        ExternalMutationEvidence::try_from_wire_v1(&wire).map_err(|error| error.to_string())?;
        Ok(Self(wire))
    }

    pub fn try_decode(&self) -> Result<ExternalMutationEvidence, String> {
        ExternalMutationEvidence::try_from_wire_v1(&self.0).map_err(|error| error.to_string())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnectorWriteFailureKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unauthenticated,
    PermissionDenied,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    Unavailable,
    CorruptData,
    Internal,
}

impl From<ConnectorMutationFailureKind> for ConnectorWriteFailureKind {
    fn from(value: ConnectorMutationFailureKind) -> Self {
        match value {
            ConnectorMutationFailureKind::InvalidRequest => Self::InvalidRequest,
            ConnectorMutationFailureKind::NotFound => Self::NotFound,
            ConnectorMutationFailureKind::AlreadyExists => Self::AlreadyExists,
            ConnectorMutationFailureKind::Conflict => Self::Conflict,
            ConnectorMutationFailureKind::Unauthenticated => Self::Unauthenticated,
            ConnectorMutationFailureKind::PermissionDenied => Self::PermissionDenied,
            ConnectorMutationFailureKind::Unsupported => Self::Unsupported,
            ConnectorMutationFailureKind::Cancelled => Self::Cancelled,
            ConnectorMutationFailureKind::DeadlineExceeded => Self::DeadlineExceeded,
            ConnectorMutationFailureKind::ResourceExhausted => Self::ResourceExhausted,
            ConnectorMutationFailureKind::Unavailable => Self::Unavailable,
            ConnectorMutationFailureKind::CorruptData => Self::CorruptData,
            ConnectorMutationFailureKind::Internal => Self::Internal,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorWriteFailureRecord {
    pub kind: ConnectorWriteFailureKind,
    pub message: String,
}

impl From<&ConnectorMutationFailure> for ConnectorWriteFailureRecord {
    fn from(value: &ConnectorMutationFailure) -> Self {
        Self {
            kind: value.kind().into(),
            message: value.message().to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    content = "failure",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum ConnectorWriteFinalizationRecord {
    Complete,
    Failed(ConnectorWriteFailureRecord),
}

/// The sole ordinary-DML durable terminal fact. Its tagged form makes all
/// impossible receipt/evidence combinations unrepresentable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnectorWriteLifecycleRecord {
    Pending,
    KnownEmpty,
    KnownCommitted {
        receipt_wire: ConnectorWriteReceiptWire,
        finalization: ConnectorWriteFinalizationRecord,
    },
    KnownUncommitted {
        failure: ConnectorWriteFailureRecord,
    },
    CommitUnknown {
        evidence_wire: ExternalMutationEvidenceWire,
        failure: ConnectorWriteFailureRecord,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationFact {
    pub state: OperationState,
    pub lifecycle: ConnectorWriteLifecycleRecord,
}

// ---------------------------------------------------------------------------
// CP-3B: external operation fence and historical write recovery records.
//
// These records are deliberately provider-neutral: identity, scalar generation
// values, digests, and bounded opaque payloads only. They must not depend on
// any connector fence or historical-recovery type, so the frontend cannot start
// interpreting provider payloads. Mapping SPI values onto these fields belongs
// to the frontend fence projection and recovery profile, not to the journal.
// ---------------------------------------------------------------------------

/// A bounded provider payload the frontend stores without interpretation.
///
/// External fence receipts, historical proofs, evidence, and continuations are
/// opaque: the frontend never decodes them, and `Debug` never reveals their
/// contents. The durable form is canonical lowercase hex so a record's encoded
/// size stays a predictable multiple of its payload length.
#[derive(Clone, Eq, PartialEq)]
pub struct DmlOpaquePayload(Vec<u8>);

impl DmlOpaquePayload {
    pub fn try_new(bytes: Vec<u8>) -> Result<Self, String> {
        if bytes.is_empty() || bytes.len() > DML_OPAQUE_PAYLOAD_LIMIT {
            return Err(format!(
                "opaque DML payload must hold 1..={DML_OPAQUE_PAYLOAD_LIMIT} bytes, found {}",
                bytes.len()
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for DmlOpaquePayload {
    /// Redacted on purpose: an opaque provider payload must never reach a log.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DmlOpaquePayload")
            .field("len", &self.0.len())
            .finish()
    }
}

impl Serialize for DmlOpaquePayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for DmlOpaquePayload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let bytes = hex::decode(&text).map_err(serde::de::Error::custom)?;
        if hex::encode(&bytes) != text {
            return Err(serde::de::Error::custom(
                "opaque DML payload must use canonical lowercase hex",
            ));
        }
        Self::try_new(bytes).map_err(serde::de::Error::custom)
    }
}

/// Totally ordered generation of one external operation fence.
///
/// The order is lexicographic over the declared field order: a higher control
/// plane incarnation outranks any resource epoch, and a higher resource epoch
/// outranks any provider fence generation. That is exactly the comparison a new
/// owner needs to prove its fence is strictly above the old authority's fence
/// without reading any connector type.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DmlExternalFenceGeneration {
    /// CP-1 control plane incarnation the fence was minted under (nonzero).
    pub control_plane_incarnation: u64,
    /// CP-1 resource epoch of the fenced operation lease (nonzero).
    pub resource_epoch: u64,
    /// Provider-visible monotone fence generation (nonzero).
    pub fence_generation: u64,
}

/// Identity an external operation fence is bound to.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlExternalFenceIdentity {
    /// Digest of the cluster identity the fence was minted from.
    pub cluster_identity_digest: String,
    /// Digest of the fenced resource identity (table plus target ref).
    pub resource_digest: String,
    /// Stable write operation id every attempt of this DML statement shares.
    pub write_operation_id: Uuid,
    /// CP-3A coordination attempt that minted the fence.
    pub coordination_attempt_id: Uuid,
    pub generation: DmlExternalFenceGeneration,
}

/// Durable proof that one DML operation attempt confirmed an external
/// operation fence before any writer or commit dispatch could start.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlExternalFenceReceiptRecord {
    pub codec_version: u8,
    pub identity: DmlExternalFenceIdentity,
    /// Digest of the fence value the provider compared at its linearization
    /// point.
    pub fence_digest: String,
    /// Digest of the provider receipt that confirmed the fence.
    pub receipt_digest: String,
    pub receipt_payload: DmlOpaquePayload,
    pub established_at_ms: i64,
}

impl DmlExternalFenceReceiptRecord {
    pub const fn generation(&self) -> DmlExternalFenceGeneration {
        self.identity.generation
    }
}

/// What the frontend knew about the old attempt's dispatch when it sealed a
/// historical write recovery request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlHistoricalDispatchCertainty {
    ConfirmedNotDispatched,
    PossiblyDispatched,
    ConfirmedDispatched,
}

/// Typed provider disposition of a historical write operation.
///
/// The frontend classifies nothing itself: it records exactly what the current
/// provider generation proved and never downgrades a conflict to an unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlHistoricalWriteDisposition {
    /// Provider proved the target external truth carries this operation.
    Applied,
    /// Provider proved the old operation did not commit.
    NotApplied,
    /// Provider proved no writer or commit was ever dispatched.
    NotDispatched,
    /// Writer output exists but was never committed; cleanup is proof-bound.
    Staged,
    /// External base or fence moved on under another operation.
    Conflict,
    /// Evidence is insufficient; the recovery index must be retained.
    Ambiguous,
    /// The provider has no historical write recovery capability.
    Unsupported,
}

/// Retention state of the proof-bound cleanup a disposition may require.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlHistoricalCleanupState {
    NotRequired,
    Pending,
    Completed,
    /// Explicitly handed to an operator. The obligation is retained but the
    /// bounded automatic scan stops.
    ManualRetention,
}

/// Where one historical write recovery cycle stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlHistoricalRecoveryPhase {
    /// The immutable request is durable; no higher fence exists yet.
    Requested,
    /// The current generation established a strictly higher external fence.
    FenceRaised,
    /// The provider returned a conclusive typed disposition.
    Inspected,
    /// A proof-bound guarded cleanup is outstanding.
    CleanupPending,
    /// Terminal: finalized, cleaned up, or handed to an operator.
    Resolved,
    /// Evidence was insufficient; a later cycle may repeat inspection.
    Unresolved,
}

impl DmlHistoricalRecoveryPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "REQUESTED",
            Self::FenceRaised => "FENCE_RAISED",
            Self::Inspected => "INSPECTED",
            Self::CleanupPending => "CLEANUP_PENDING",
            Self::Resolved => "RESOLVED",
            Self::Unresolved => "UNRESOLVED",
        }
    }

    /// Monotone progress rank inside one recovery cycle. A retry that needs to
    /// move backwards must open a new cycle instead.
    const fn progress(self) -> u8 {
        match self {
            Self::Requested => 0,
            Self::FenceRaised => 1,
            Self::Inspected => 2,
            Self::CleanupPending => 3,
            Self::Unresolved => 4,
            Self::Resolved => 5,
        }
    }
}

/// The immutable, digest-sealed description of the historical write a new owner
/// asks the current provider generation to classify.
///
/// Every field is either neutral identity, a digest, or a bounded opaque
/// payload. The frontend never resolves the old owner into a live binding and
/// never replays the old operation through the ordinary write path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlHistoricalWriteRequestRecord {
    /// Old exact connector owner, kept as neutral strings for audit only.
    pub old_provider_id: String,
    pub old_connector_instance_id: String,
    /// 32 lowercase hexadecimal characters.
    pub old_connector_incarnation: String,
    pub old_coordination_attempt_id: Option<Uuid>,
    /// The fence the old attempt confirmed, when one was durable. This is the
    /// baseline a raised fence must be strictly above.
    pub old_fence: Option<DmlExternalFenceReceiptRecord>,
    /// Stable write operation id shared by every attempt of this statement.
    pub write_operation_id: Uuid,
    /// Sealed writer cohort set digest.
    pub cohort_set_digest: String,
    /// Sealed aggregate write digest, when the old attempt reached one.
    pub aggregate_write_digest: Option<String>,
    pub dispatch_certainty: DmlHistoricalDispatchCertainty,
    /// Whether writer output was durably checkpointed before the owner was
    /// lost. A checkpointed writer cohort is never adopted across generations.
    pub writer_output_checkpointed: bool,
    pub commit_dispatched_at_ms: Option<i64>,
    /// Digest over the complete immutable request as it was handed to the
    /// provider. The double check after a historical call compares this value.
    pub request_digest: String,
}

/// The typed provider outcome of one historical inspection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlHistoricalWriteResultRecord {
    pub disposition: DmlHistoricalWriteDisposition,
    /// Digest of the provider observation the disposition came from.
    pub observation_digest: String,
    pub evidence_payload: Option<DmlOpaquePayload>,
    pub proof_payload: Option<DmlOpaquePayload>,
    /// Only a proven `NotDispatched` disposition may carry a continuation, and
    /// only once a strictly higher fence closed the old authority.
    pub continuation_payload: Option<DmlOpaquePayload>,
    pub cleanup: DmlHistoricalCleanupState,
    /// Typed stale, conflict, or unsupported classification. A fence conflict
    /// is recorded here and is never downgraded to an unknown outcome.
    pub failure: Option<ConnectorWriteFailureRecord>,
    pub observed_at_ms: i64,
}

/// One durable historical write recovery record: a sealed request plus the
/// fence the current generation raised and the typed result it obtained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlHistoricalWriteRecoveryRecord {
    pub codec_version: u8,
    pub phase: DmlHistoricalRecoveryPhase,
    /// CP-3A coordination attempt that owns this cycle.
    pub recovery_attempt_id: Uuid,
    /// Monotone cycle counter over the same immutable request. Repeating an
    /// inspection opens a new cycle instead of rewinding the current one.
    pub recovery_cycle: u32,
    pub request: DmlHistoricalWriteRequestRecord,
    pub raised_fence: Option<DmlExternalFenceReceiptRecord>,
    pub result: Option<DmlHistoricalWriteResultRecord>,
    pub next_action: StatementNextAction,
    pub requested_at_ms: i64,
    pub updated_at_ms: i64,
}

impl DmlHistoricalWriteRecoveryRecord {
    /// True while the bounded recovery scan must keep visiting this operation.
    ///
    /// Only `Resolved` ends the scan, so a pending cleanup outcome cannot be
    /// dropped because the user-visible statement result became terminal.
    pub const fn requires_recovery_scan(&self) -> bool {
        !matches!(self.phase, DmlHistoricalRecoveryPhase::Resolved)
    }

    pub const fn cleanup(&self) -> Option<DmlHistoricalCleanupState> {
        match &self.result {
            Some(result) => Some(result.cleanup),
            None => None,
        }
    }
}

/// Authorized mutation that publishes a historical write recovery request or
/// its typed result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlHistoricalWriteRecoveryMutationRequest {
    pub operation_id: DmlOperationId,
    pub expected_revision: u64,
    pub mutation_id: Uuid,
    pub recovery: DmlHistoricalWriteRecoveryRecord,
}

fn is_lowercase_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(*byte, b'a'..=b'f'))
}

fn is_lowercase_incarnation(value: &str) -> bool {
    value.len() == 32
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(*byte, b'a'..=b'f'))
}

fn is_uuid_v7(value: Uuid) -> bool {
    value.get_version() == Some(uuid::Version::SortRand)
        && value.get_variant() == uuid::Variant::RFC4122
}

/// Validate the complete shape of an external fence receipt record.
pub fn validate_external_fence_receipt(
    record: &DmlExternalFenceReceiptRecord,
) -> Result<(), String> {
    if record.codec_version != DML_EXTERNAL_FENCE_CODEC_VERSION {
        return Err(format!(
            "unsupported DML external fence codec version: {}",
            record.codec_version
        ));
    }
    let generation = record.identity.generation;
    if generation.control_plane_incarnation == 0
        || generation.resource_epoch == 0
        || generation.fence_generation == 0
    {
        return Err("DML external fence generation components must all be nonzero".to_string());
    }
    for (label, digest) in [
        ("cluster identity", &record.identity.cluster_identity_digest),
        ("resource", &record.identity.resource_digest),
        ("fence", &record.fence_digest),
        ("receipt", &record.receipt_digest),
    ] {
        if !is_lowercase_digest(digest) {
            return Err(format!(
                "DML external fence {label} digest must be 64 lowercase hexadecimal characters"
            ));
        }
    }
    if record.identity.write_operation_id.is_nil() {
        return Err("DML external fence write operation id must not be nil".to_string());
    }
    if !is_uuid_v7(record.identity.coordination_attempt_id) {
        return Err("DML external fence coordination attempt id must be UUIDv7".to_string());
    }
    if record.established_at_ms < 0 {
        return Err("DML external fence timestamp must be nonnegative".to_string());
    }
    if record.receipt_payload.as_bytes().len() > DML_OPAQUE_PAYLOAD_LIMIT {
        return Err(format!(
            "DML external fence receipt payload exceeds {DML_OPAQUE_PAYLOAD_LIMIT} bytes"
        ));
    }
    let encoded = serde_json::to_vec(record).map_err(|error| error.to_string())?;
    if encoded.len() > DML_EXTERNAL_FENCE_ENCODED_LIMIT {
        return Err(format!(
            "DML external fence record encoded size {} exceeds limit {DML_EXTERNAL_FENCE_ENCODED_LIMIT}",
            encoded.len()
        ));
    }
    Ok(())
}

/// Validate that replacing `existing` with `next` keeps the external fence
/// generation monotone and cannot reuse a marker across operations.
pub fn validate_external_fence_transition(
    existing: Option<&DmlExternalFenceReceiptRecord>,
    next: &DmlExternalFenceReceiptRecord,
) -> Result<(), String> {
    validate_external_fence_receipt(next)?;
    let Some(existing) = existing else {
        return Ok(());
    };
    if existing.identity.write_operation_id != next.identity.write_operation_id {
        return Err(
            "DML external fence receipt cannot be reused across write operations".to_string(),
        );
    }
    if existing.identity.resource_digest != next.identity.resource_digest
        || existing.identity.cluster_identity_digest != next.identity.cluster_identity_digest
    {
        return Err("DML external fence receipt changed its fenced identity".to_string());
    }
    match next.generation().cmp(&existing.generation()) {
        std::cmp::Ordering::Less => {
            Err("DML external fence generation must not move backwards".to_string())
        }
        std::cmp::Ordering::Equal if existing != next => {
            Err("DML external fence receipt changed without advancing its generation".to_string())
        }
        _ => Ok(()),
    }
}

fn validate_historical_write_request(
    request: &DmlHistoricalWriteRequestRecord,
) -> Result<(), String> {
    if request.old_provider_id.is_empty() || request.old_connector_instance_id.is_empty() {
        return Err(
            "DML historical write recovery requires a non-empty old provider and instance"
                .to_string(),
        );
    }
    if !is_lowercase_incarnation(&request.old_connector_incarnation) {
        return Err(
            "DML historical write recovery old incarnation must be 32 lowercase hexadecimal characters"
                .to_string(),
        );
    }
    if request
        .old_coordination_attempt_id
        .is_some_and(|attempt| !is_uuid_v7(attempt))
    {
        return Err(
            "DML historical write recovery old coordination attempt id must be UUIDv7".to_string(),
        );
    }
    if request.write_operation_id.is_nil() {
        return Err("DML historical write recovery write operation id must not be nil".to_string());
    }
    for (label, digest) in [
        ("cohort set", Some(&request.cohort_set_digest)),
        ("aggregate write", request.aggregate_write_digest.as_ref()),
        ("request", Some(&request.request_digest)),
    ] {
        if let Some(digest) = digest
            && !is_lowercase_digest(digest)
        {
            return Err(format!(
                "DML historical write recovery {label} digest must be 64 lowercase hexadecimal characters"
            ));
        }
    }
    if request
        .commit_dispatched_at_ms
        .is_some_and(|timestamp| timestamp < 0)
    {
        return Err(
            "DML historical write recovery commit dispatch timestamp must be nonnegative"
                .to_string(),
        );
    }
    if request.dispatch_certainty == DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
        && (request.writer_output_checkpointed || request.commit_dispatched_at_ms.is_some())
    {
        return Err(
            "DML historical write recovery cannot claim not-dispatched with dispatch facts"
                .to_string(),
        );
    }
    if let Some(old_fence) = &request.old_fence {
        validate_external_fence_receipt(old_fence)?;
        if old_fence.identity.write_operation_id != request.write_operation_id {
            return Err(
                "DML historical write recovery old fence belongs to another write operation"
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn validate_historical_write_result(result: &DmlHistoricalWriteResultRecord) -> Result<(), String> {
    if !is_lowercase_digest(&result.observation_digest) {
        return Err(
            "DML historical write observation digest must be 64 lowercase hexadecimal characters"
                .to_string(),
        );
    }
    if result.observed_at_ms < 0 {
        return Err("DML historical write observation timestamp must be nonnegative".to_string());
    }
    if result
        .failure
        .as_ref()
        .is_some_and(|failure| failure.message.is_empty())
    {
        return Err("DML historical write failure message must not be empty".to_string());
    }
    if result.continuation_payload.is_some()
        && result.disposition != DmlHistoricalWriteDisposition::NotDispatched
    {
        return Err(
            "DML historical write continuation is only valid for a proven NOT_DISPATCHED disposition"
                .to_string(),
        );
    }
    match result.disposition {
        DmlHistoricalWriteDisposition::Applied
        | DmlHistoricalWriteDisposition::NotApplied
        | DmlHistoricalWriteDisposition::NotDispatched
        | DmlHistoricalWriteDisposition::Staged
        | DmlHistoricalWriteDisposition::Conflict
            if result.proof_payload.is_none() && result.evidence_payload.is_none() =>
        {
            return Err(
                "a conclusive DML historical write disposition requires provider proof or evidence"
                    .to_string(),
            );
        }
        DmlHistoricalWriteDisposition::Conflict if result.failure.is_none() => {
            return Err(
                "a DML historical write conflict requires a typed failure classification"
                    .to_string(),
            );
        }
        DmlHistoricalWriteDisposition::Staged
            if result.cleanup == DmlHistoricalCleanupState::NotRequired =>
        {
            return Err(
                "a staged DML historical write disposition requires proof-bound cleanup"
                    .to_string(),
            );
        }
        DmlHistoricalWriteDisposition::Ambiguous | DmlHistoricalWriteDisposition::Unsupported
            if result.cleanup == DmlHistoricalCleanupState::Completed =>
        {
            return Err(
                "an inconclusive DML historical write disposition cannot report completed cleanup"
                    .to_string(),
            );
        }
        _ => {}
    }
    for payload in [
        result.evidence_payload.as_ref(),
        result.proof_payload.as_ref(),
        result.continuation_payload.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if payload.as_bytes().len() > DML_OPAQUE_PAYLOAD_LIMIT {
            return Err(format!(
                "DML historical write payload exceeds {DML_OPAQUE_PAYLOAD_LIMIT} bytes"
            ));
        }
    }
    Ok(())
}

/// Validate the complete shape of a historical write recovery record.
pub fn validate_historical_write_recovery(
    record: &DmlHistoricalWriteRecoveryRecord,
) -> Result<(), String> {
    if record.codec_version != DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION {
        return Err(format!(
            "unsupported DML historical write recovery codec version: {}",
            record.codec_version
        ));
    }
    if !is_uuid_v7(record.recovery_attempt_id) {
        return Err("DML historical write recovery attempt id must be UUIDv7".to_string());
    }
    if record.recovery_cycle == 0 {
        return Err("DML historical write recovery cycle must be nonzero".to_string());
    }
    if record.requested_at_ms < 0 || record.updated_at_ms < record.requested_at_ms {
        return Err("DML historical write recovery has invalid timestamps".to_string());
    }
    validate_historical_write_request(&record.request)?;
    if let Some(raised) = &record.raised_fence {
        validate_external_fence_receipt(raised)?;
        if raised.identity.write_operation_id != record.request.write_operation_id {
            return Err("DML raised external fence belongs to another write operation".to_string());
        }
        if raised.identity.coordination_attempt_id != record.recovery_attempt_id {
            return Err(
                "DML raised external fence was not minted by the current recovery attempt"
                    .to_string(),
            );
        }
        if let Some(old_fence) = &record.request.old_fence
            && raised.generation() <= old_fence.generation()
        {
            return Err(
                "DML raised external fence must be strictly above the old attempt's fence"
                    .to_string(),
            );
        }
    }
    if let Some(result) = &record.result {
        validate_historical_write_result(result)?;
        if record.raised_fence.is_none() {
            return Err(
                "DML historical write inspection requires a durable raised external fence"
                    .to_string(),
            );
        }
    }
    validate_historical_phase_shape(record)?;
    let encoded = serde_json::to_vec(record).map_err(|error| error.to_string())?;
    if encoded.len() > DML_HISTORICAL_WRITE_RECOVERY_ENCODED_LIMIT {
        return Err(format!(
            "DML historical write recovery encoded size {} exceeds limit {DML_HISTORICAL_WRITE_RECOVERY_ENCODED_LIMIT}",
            encoded.len()
        ));
    }
    Ok(())
}

fn validate_historical_phase_shape(
    record: &DmlHistoricalWriteRecoveryRecord,
) -> Result<(), String> {
    let cleanup = record.cleanup();
    let phase = record.phase;
    let shape_ok = match phase {
        DmlHistoricalRecoveryPhase::Requested => {
            record.raised_fence.is_none() && record.result.is_none()
        }
        DmlHistoricalRecoveryPhase::FenceRaised => {
            record.raised_fence.is_some() && record.result.is_none()
        }
        DmlHistoricalRecoveryPhase::Inspected => {
            cleanup.is_some_and(|cleanup| cleanup != DmlHistoricalCleanupState::Pending)
        }
        DmlHistoricalRecoveryPhase::CleanupPending => {
            cleanup == Some(DmlHistoricalCleanupState::Pending)
        }
        DmlHistoricalRecoveryPhase::Resolved => {
            cleanup.is_some_and(|cleanup| cleanup != DmlHistoricalCleanupState::Pending)
        }
        DmlHistoricalRecoveryPhase::Unresolved => record.result.as_ref().is_none_or(|result| {
            matches!(
                result.disposition,
                DmlHistoricalWriteDisposition::Ambiguous
                    | DmlHistoricalWriteDisposition::Unsupported
            )
        }),
    };
    if !shape_ok {
        return Err(format!(
            "DML historical write recovery phase {} disagrees with its durable facts",
            phase.as_str()
        ));
    }
    let next_action_ok = if phase == DmlHistoricalRecoveryPhase::Resolved {
        if cleanup == Some(DmlHistoricalCleanupState::ManualRetention) {
            record.next_action == StatementNextAction::ManualInspect
        } else {
            record.next_action == StatementNextAction::None
        }
    } else {
        record.next_action != StatementNextAction::None
    };
    if !next_action_ok {
        return Err(format!(
            "DML historical write recovery phase {} disagrees with next action {:?}",
            phase.as_str(),
            record.next_action
        ));
    }
    Ok(())
}

/// Validate that replacing `existing` with `next` preserves the immutable
/// request, keeps the cycle and phase monotone, and never drops a retained
/// cleanup obligation.
pub fn validate_historical_write_recovery_transition(
    existing: Option<&DmlHistoricalWriteRecoveryRecord>,
    next: &DmlHistoricalWriteRecoveryRecord,
) -> Result<(), String> {
    validate_historical_write_recovery(next)?;
    let Some(existing) = existing else {
        if next.recovery_cycle != 1 || next.phase != DmlHistoricalRecoveryPhase::Requested {
            return Err(
                "a new DML historical write recovery must start at cycle 1 in REQUESTED phase"
                    .to_string(),
            );
        }
        return Ok(());
    };
    if existing.request != next.request {
        return Err(
            "a DML historical write recovery request is immutable once durable".to_string(),
        );
    }
    if existing.phase == DmlHistoricalRecoveryPhase::Resolved && existing != next {
        return Err("a resolved DML historical write recovery cannot be reopened".to_string());
    }
    if next.recovery_cycle < existing.recovery_cycle {
        return Err("DML historical write recovery cycle must not move backwards".to_string());
    }
    if next.recovery_cycle == existing.recovery_cycle {
        if next.recovery_attempt_id != existing.recovery_attempt_id {
            return Err(
                "a DML historical write recovery cycle is owned by one coordination attempt"
                    .to_string(),
            );
        }
        if next.phase.progress() < existing.phase.progress() {
            return Err(format!(
                "DML historical write recovery phase cannot move from {} back to {} inside one cycle",
                existing.phase.as_str(),
                next.phase.as_str()
            ));
        }
    }
    // A raised fence closed the old authority. Neither a later phase nor a new
    // cycle may forget or lower it, so `Requested` is only reachable before the
    // first raise.
    match (&existing.raised_fence, &next.raised_fence) {
        (Some(previous), Some(raised)) => {
            validate_external_fence_transition(Some(previous), raised)?;
        }
        (Some(_), None) => {
            return Err(
                "a raised DML external fence cannot be dropped from a historical write recovery"
                    .to_string(),
            );
        }
        _ => {}
    }
    if existing.cleanup() == Some(DmlHistoricalCleanupState::Pending)
        && !matches!(
            next.cleanup(),
            Some(
                DmlHistoricalCleanupState::Pending
                    | DmlHistoricalCleanupState::Completed
                    | DmlHistoricalCleanupState::ManualRetention
            )
        )
    {
        return Err(
            "a pending DML historical cleanup outcome must be retained until it completes or is explicitly kept for manual retention"
                .to_string(),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CP-3C: direct data-mutation external fence and historical data-mutation
// recovery records.
//
// TRUNCATE and ADD FILES never reach the distributed writer, but they do change
// external truth, so they need the same fence-before-dispatch proof and the same
// provider-owned historical classification. These records deliberately reuse the
// CP-3B fence carrier: spec D1 rules that direct mutation shares the external
// operation fence value instead of minting a second one.
//
// As in CP-3B every field is neutral identity, a scalar, a digest, or a bounded
// opaque payload. Mapping SPI values onto them belongs to the direct-mutation
// fence projection and the statement-family recovery profile, never here.
// ---------------------------------------------------------------------------

/// Which direct data-mutation family a durable CP-3C record belongs to.
///
/// The two families share a fence carrier but never share ownership semantics:
/// only ADD FILES reserves an immutable source scope, and TRUNCATE has no source
/// set at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlDirectMutationKind {
    Truncate,
    AddFiles,
}

impl DmlDirectMutationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Truncate => "TRUNCATE",
            Self::AddFiles => "ADD_FILES",
        }
    }

    /// True when the family owns an immutable source scope every durable record
    /// must bind.
    pub const fn binds_source_scope(self) -> bool {
        matches!(self, Self::AddFiles)
    }

    pub const fn operation_kind(self) -> OperationKind {
        match self {
            Self::Truncate => OperationKind::Truncate,
            Self::AddFiles => OperationKind::AddFiles,
        }
    }
}

/// Durable proof that one direct data-mutation attempt confirmed an external
/// operation fence before the irreversible external effect could start.
///
/// The fence value itself is the CP-3B carrier. `identity.write_operation_id`
/// holds this statement's stable direct-mutation operation id, and the fence ref
/// derives from it, so a TRUNCATE, an ADD FILES, and a row DML statement can
/// never reuse one another's marker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlDirectMutationFenceReceiptRecord {
    pub codec_version: u8,
    pub operation_kind: DmlDirectMutationKind,
    pub fence: DmlExternalFenceReceiptRecord,
    /// The immutable ADD FILES source scope this fence was minted for.
    /// Mandatory for ADD FILES, forbidden for TRUNCATE.
    pub source_scope_digest: Option<String>,
}

impl DmlDirectMutationFenceReceiptRecord {
    pub const fn generation(&self) -> DmlExternalFenceGeneration {
        self.fence.identity.generation
    }

    /// The stable direct-mutation operation id this fence is bound to.
    pub const fn mutation_operation_id(&self) -> Uuid {
        self.fence.identity.write_operation_id
    }
}

/// Typed provider disposition of one historical direct data mutation.
///
/// The frontend classifies nothing itself. A missing marker or a missing
/// artifact is never evidence of anything: it can only produce `Ambiguous`, and
/// never `NotApplied`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlHistoricalDataMutationDisposition {
    /// Provider proved the external table truth carries this mutation.
    Applied,
    /// Provider proved the mutation never changed external truth and a strictly
    /// higher fence already closed the old authority.
    NotApplied,
    /// External table identity, base state, or fence moved on under another
    /// operation. An ADD FILES source proof must not act on the new table.
    Conflict,
    /// Provider proved only part of the mutation reached external truth.
    PartiallyApplied,
    /// The disposition is determined but leaves proof-bound artifacts behind.
    CleanupRequired,
    /// Evidence was insufficient. The recovery index and any source-scope
    /// reservation must be retained.
    Ambiguous,
    /// The provider installs no historical data-mutation recovery capability.
    Unsupported,
}

impl DmlHistoricalDataMutationDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::NotApplied => "NOT_APPLIED",
            Self::Conflict => "CONFLICT",
            Self::PartiallyApplied => "PARTIALLY_APPLIED",
            Self::CleanupRequired => "CLEANUP_REQUIRED",
            Self::Ambiguous => "AMBIGUOUS",
            Self::Unsupported => "UNSUPPORTED",
        }
    }

    /// True when the provider returned a typed answer rather than admitting that
    /// evidence was insufficient. Only a determined disposition may carry proof.
    pub const fn is_determined(self) -> bool {
        !matches!(self, Self::Ambiguous | Self::Unsupported)
    }

    /// True when the provider proved a complete one-way answer for the whole
    /// mutation. Only these two dispositions may free an ADD FILES source-scope
    /// reservation; every other outcome keeps it held.
    pub const fn may_release_source_scope(self) -> bool {
        matches!(self, Self::Applied | Self::NotApplied)
    }
}

/// The immutable, digest-sealed description of the historical direct mutation a
/// new owner asks the current provider generation to classify.
///
/// The frontend never resolves the old owner into a live binding and never
/// replays the old operation through the ordinary execute/reconcile path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlHistoricalDataMutationRequestRecord {
    /// Old exact connector owner, kept as neutral strings for audit only.
    pub old_provider_id: String,
    pub old_connector_instance_id: String,
    /// 32 lowercase hexadecimal characters.
    pub old_connector_incarnation: String,
    pub old_coordination_attempt_id: Option<Uuid>,
    /// The fence the old attempt confirmed, when one was durable. This is the
    /// baseline a raised fence must be strictly above.
    pub old_fence: Option<DmlDirectMutationFenceReceiptRecord>,
    pub operation_kind: DmlDirectMutationKind,
    /// Stable direct-mutation operation id shared by every attempt of this
    /// statement.
    pub mutation_operation_id: Uuid,
    /// Digest over the complete immutable request as it was handed to the
    /// provider. The double check after a historical call compares this value.
    pub request_digest: String,
    pub plan_digest: Option<String>,
    pub state_digest: Option<String>,
    /// The immutable ADD FILES source scope. Mandatory for ADD FILES, forbidden
    /// for TRUNCATE. The source set never expands, so a result naming any other
    /// scope belongs to another operation and is refused.
    pub source_scope_digest: Option<String>,
    pub dispatch_certainty: DmlHistoricalDispatchCertainty,
    pub dispatched_at_ms: Option<i64>,
}

/// The typed provider outcome of one historical direct-mutation inspection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlHistoricalDataMutationResultRecord {
    pub disposition: DmlHistoricalDataMutationDisposition,
    /// Digest of the provider observation the disposition came from.
    pub observation_digest: String,
    /// The exact source scope the provider classified. It must equal the sealed
    /// request's scope, so a late or crossed ADD FILES result can never be
    /// applied to another operation's source set.
    pub source_scope_digest: Option<String>,
    pub evidence_payload: Option<DmlOpaquePayload>,
    pub proof_payload: Option<DmlOpaquePayload>,
    /// Only a proven `NOT_APPLIED` disposition may carry a continuation, and the
    /// continuation opens a new coordination attempt of the same durable
    /// operation. It never revives the old prepared handle.
    pub continuation_payload: Option<DmlOpaquePayload>,
    pub cleanup: DmlHistoricalCleanupState,
    /// Whether the ADD FILES source-scope reservation must stay held. Every
    /// disposition short of a complete provider proof forces this: evidence
    /// absence is not proof that the source set became free.
    pub source_scope_retained: bool,
    /// Typed stale, conflict, or unsupported classification. A fence conflict is
    /// recorded here and is never downgraded to an unknown outcome.
    pub failure: Option<ConnectorWriteFailureRecord>,
    pub observed_at_ms: i64,
}

/// One durable historical data-mutation recovery record: a sealed request plus
/// the fence the current generation raised and the typed result it obtained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DmlHistoricalDataMutationRecoveryRecord {
    pub codec_version: u8,
    pub phase: DmlHistoricalRecoveryPhase,
    /// CP-3A coordination attempt that owns this cycle.
    pub recovery_attempt_id: Uuid,
    /// Monotone cycle counter over the same immutable request. Repeating an
    /// inspection opens a new cycle instead of rewinding the current one.
    pub recovery_cycle: u32,
    pub request: DmlHistoricalDataMutationRequestRecord,
    pub raised_fence: Option<DmlDirectMutationFenceReceiptRecord>,
    pub result: Option<DmlHistoricalDataMutationResultRecord>,
    pub next_action: StatementNextAction,
    pub requested_at_ms: i64,
    pub updated_at_ms: i64,
}

impl DmlHistoricalDataMutationRecoveryRecord {
    /// True while the bounded recovery scan must keep visiting this operation.
    ///
    /// Only `Resolved` ends the scan, so a pending guarded cleanup cannot be
    /// dropped because the user-visible statement result became terminal.
    pub const fn requires_recovery_scan(&self) -> bool {
        !matches!(self.phase, DmlHistoricalRecoveryPhase::Resolved)
    }

    pub const fn cleanup(&self) -> Option<DmlHistoricalCleanupState> {
        match &self.result {
            Some(result) => Some(result.cleanup),
            None => None,
        }
    }

    /// True while the ADD FILES source-scope reservation must stay held.
    ///
    /// Without a provider result nothing has proved the reservation releasable,
    /// so an ADD FILES recovery retains it by construction.
    pub const fn retains_source_scope(&self) -> bool {
        match &self.result {
            Some(result) => result.source_scope_retained,
            None => self.request.operation_kind.binds_source_scope(),
        }
    }
}

/// Authorized mutation that attaches a confirmed direct-mutation fence receipt
/// to one TRUNCATE or ADD FILES attempt.
/// Authorized mutation that publishes a historical data-mutation recovery
/// request or its typed result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlHistoricalDataMutationRecoveryMutationRequest {
    pub operation_id: DmlOperationId,
    pub expected_revision: u64,
    pub mutation_id: Uuid,
    pub recovery: DmlHistoricalDataMutationRecoveryRecord,
}

/// A direct-mutation family either binds exactly one immutable source scope or
/// none at all. Both mismatches fail closed.
fn validate_source_scope_binding(
    operation_kind: DmlDirectMutationKind,
    source_scope_digest: Option<&String>,
    label: &str,
) -> Result<(), String> {
    match (operation_kind.binds_source_scope(), source_scope_digest) {
        (true, None) => Err(format!(
            "an ADD FILES {label} must bind its immutable source scope digest"
        )),
        (false, Some(_)) => Err(format!(
            "a TRUNCATE {label} must not bind a source scope digest"
        )),
        (true, Some(digest)) if !is_lowercase_digest(digest) => Err(format!(
            "DML {label} source scope digest must be 64 lowercase hexadecimal characters"
        )),
        _ => Ok(()),
    }
}

/// Validate the complete shape of a direct-mutation fence receipt record.
pub fn validate_direct_mutation_fence_receipt(
    record: &DmlDirectMutationFenceReceiptRecord,
) -> Result<(), String> {
    if record.codec_version != DML_DIRECT_MUTATION_FENCE_CODEC_VERSION {
        return Err(format!(
            "unsupported DML direct mutation fence codec version: {}",
            record.codec_version
        ));
    }
    validate_external_fence_receipt(&record.fence)?;
    validate_source_scope_binding(
        record.operation_kind,
        record.source_scope_digest.as_ref(),
        "direct mutation fence receipt",
    )?;
    let encoded = serde_json::to_vec(record).map_err(|error| error.to_string())?;
    if encoded.len() > DML_DIRECT_MUTATION_FENCE_ENCODED_LIMIT {
        return Err(format!(
            "DML direct mutation fence record encoded size {} exceeds limit {DML_DIRECT_MUTATION_FENCE_ENCODED_LIMIT}",
            encoded.len()
        ));
    }
    Ok(())
}

/// Validate that replacing `existing` with `next` keeps the direct-mutation
/// fence generation monotone and never rebinds another family or source scope.
pub fn validate_direct_mutation_fence_transition(
    existing: Option<&DmlDirectMutationFenceReceiptRecord>,
    next: &DmlDirectMutationFenceReceiptRecord,
) -> Result<(), String> {
    validate_direct_mutation_fence_receipt(next)?;
    let Some(existing) = existing else {
        return Ok(());
    };
    if existing.operation_kind != next.operation_kind {
        return Err(
            "DML direct mutation fence receipt cannot change its mutation family".to_string(),
        );
    }
    if existing.source_scope_digest != next.source_scope_digest {
        return Err(
            "an ADD FILES source scope is immutable: its fence receipt cannot rebind another scope"
                .to_string(),
        );
    }
    validate_external_fence_transition(Some(&existing.fence), &next.fence)
}

fn validate_historical_data_mutation_request(
    request: &DmlHistoricalDataMutationRequestRecord,
) -> Result<(), String> {
    if request.old_provider_id.is_empty() || request.old_connector_instance_id.is_empty() {
        return Err(
            "DML historical data mutation recovery requires a non-empty old provider and instance"
                .to_string(),
        );
    }
    if !is_lowercase_incarnation(&request.old_connector_incarnation) {
        return Err(
            "DML historical data mutation recovery old incarnation must be 32 lowercase hexadecimal characters"
                .to_string(),
        );
    }
    if request
        .old_coordination_attempt_id
        .is_some_and(|attempt| !is_uuid_v7(attempt))
    {
        return Err(
            "DML historical data mutation recovery old coordination attempt id must be UUIDv7"
                .to_string(),
        );
    }
    if request.mutation_operation_id.is_nil() {
        return Err(
            "DML historical data mutation recovery mutation operation id must not be nil"
                .to_string(),
        );
    }
    for (label, digest) in [
        ("request", Some(&request.request_digest)),
        ("plan", request.plan_digest.as_ref()),
        ("state", request.state_digest.as_ref()),
    ] {
        if let Some(digest) = digest
            && !is_lowercase_digest(digest)
        {
            return Err(format!(
                "DML historical data mutation recovery {label} digest must be 64 lowercase hexadecimal characters"
            ));
        }
    }
    validate_source_scope_binding(
        request.operation_kind,
        request.source_scope_digest.as_ref(),
        "historical data mutation recovery request",
    )?;
    if request
        .dispatched_at_ms
        .is_some_and(|timestamp| timestamp < 0)
    {
        return Err(
            "DML historical data mutation recovery dispatch timestamp must be nonnegative"
                .to_string(),
        );
    }
    if request.dispatch_certainty == DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
        && request.dispatched_at_ms.is_some()
    {
        return Err(
            "DML historical data mutation recovery cannot claim not-dispatched with a dispatch fact"
                .to_string(),
        );
    }
    if let Some(old_fence) = &request.old_fence {
        validate_direct_mutation_fence_receipt(old_fence)?;
        if old_fence.operation_kind != request.operation_kind {
            return Err(
                "DML historical data mutation recovery old fence belongs to another mutation family"
                    .to_string(),
            );
        }
        if old_fence.mutation_operation_id() != request.mutation_operation_id {
            return Err(
                "DML historical data mutation recovery old fence belongs to another mutation operation"
                    .to_string(),
            );
        }
        if old_fence.source_scope_digest != request.source_scope_digest {
            return Err(
                "DML historical data mutation recovery old fence binds another source scope"
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn validate_historical_data_mutation_result(
    request: &DmlHistoricalDataMutationRequestRecord,
    result: &DmlHistoricalDataMutationResultRecord,
) -> Result<(), String> {
    if !is_lowercase_digest(&result.observation_digest) {
        return Err(
            "DML historical data mutation observation digest must be 64 lowercase hexadecimal characters"
                .to_string(),
        );
    }
    if result.observed_at_ms < 0 {
        return Err(
            "DML historical data mutation observation timestamp must be nonnegative".to_string(),
        );
    }
    if result
        .failure
        .as_ref()
        .is_some_and(|failure| failure.message.is_empty())
    {
        return Err("DML historical data mutation failure message must not be empty".to_string());
    }
    // Spec D4: the ADD FILES source set never expands, so a result is only ever
    // valid for the exact scope the original operation reserved.
    if result.source_scope_digest != request.source_scope_digest {
        return Err(
            "DML historical data mutation result is bound to a different source scope than its sealed request"
                .to_string(),
        );
    }
    if result.continuation_payload.is_some()
        && result.disposition != DmlHistoricalDataMutationDisposition::NotApplied
    {
        return Err(
            "DML historical data mutation continuation is only valid for a proven NOT_APPLIED disposition"
                .to_string(),
        );
    }
    // Fail closed: only a complete provider proof may free an ADD FILES
    // source-scope reservation. A missing marker, a missing artifact, partially
    // visible sources, and a rebuilt table all keep it held.
    if request.operation_kind.binds_source_scope()
        && !result.disposition.may_release_source_scope()
        && !result.source_scope_retained
    {
        return Err(format!(
            "a DML historical data mutation result with disposition {} must retain its ADD FILES source scope",
            result.disposition.as_str()
        ));
    }
    if !request.operation_kind.binds_source_scope() && result.source_scope_retained {
        return Err(
            "a TRUNCATE historical data mutation result has no source scope to retain".to_string(),
        );
    }
    if result.disposition.is_determined()
        && result.proof_payload.is_none()
        && result.evidence_payload.is_none()
    {
        return Err(
            "a determined DML historical data mutation disposition requires provider proof or evidence"
                .to_string(),
        );
    }
    match result.disposition {
        DmlHistoricalDataMutationDisposition::Conflict if result.failure.is_none() => {
            return Err(
                "a DML historical data mutation conflict requires a typed failure classification"
                    .to_string(),
            );
        }
        DmlHistoricalDataMutationDisposition::PartiallyApplied
        | DmlHistoricalDataMutationDisposition::CleanupRequired
            if result.cleanup == DmlHistoricalCleanupState::NotRequired =>
        {
            return Err(format!(
                "a DML historical data mutation with disposition {} requires proof-bound cleanup",
                result.disposition.as_str()
            ));
        }
        DmlHistoricalDataMutationDisposition::Ambiguous
        | DmlHistoricalDataMutationDisposition::Unsupported
            if result.cleanup == DmlHistoricalCleanupState::Completed =>
        {
            return Err(
                "an inconclusive DML historical data mutation cannot report completed cleanup"
                    .to_string(),
            );
        }
        _ => {}
    }
    for payload in [
        result.evidence_payload.as_ref(),
        result.proof_payload.as_ref(),
        result.continuation_payload.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if payload.as_bytes().len() > DML_OPAQUE_PAYLOAD_LIMIT {
            return Err(format!(
                "DML historical data mutation payload exceeds {DML_OPAQUE_PAYLOAD_LIMIT} bytes"
            ));
        }
    }
    Ok(())
}

/// Validate the complete shape of a historical data-mutation recovery record.
pub fn validate_historical_data_mutation_recovery(
    record: &DmlHistoricalDataMutationRecoveryRecord,
) -> Result<(), String> {
    if record.codec_version != DML_HISTORICAL_DATA_MUTATION_RECOVERY_CODEC_VERSION {
        return Err(format!(
            "unsupported DML historical data mutation recovery codec version: {}",
            record.codec_version
        ));
    }
    if !is_uuid_v7(record.recovery_attempt_id) {
        return Err("DML historical data mutation recovery attempt id must be UUIDv7".to_string());
    }
    if record.recovery_cycle == 0 {
        return Err("DML historical data mutation recovery cycle must be nonzero".to_string());
    }
    if record.requested_at_ms < 0 || record.updated_at_ms < record.requested_at_ms {
        return Err("DML historical data mutation recovery has invalid timestamps".to_string());
    }
    validate_historical_data_mutation_request(&record.request)?;
    if let Some(raised) = &record.raised_fence {
        validate_direct_mutation_fence_receipt(raised)?;
        if raised.operation_kind != record.request.operation_kind {
            return Err(
                "DML raised direct mutation fence belongs to another mutation family".to_string(),
            );
        }
        if raised.mutation_operation_id() != record.request.mutation_operation_id {
            return Err(
                "DML raised direct mutation fence belongs to another mutation operation"
                    .to_string(),
            );
        }
        if raised.source_scope_digest != record.request.source_scope_digest {
            return Err(
                "DML raised direct mutation fence binds another source scope than its sealed request"
                    .to_string(),
            );
        }
        if raised.fence.identity.coordination_attempt_id != record.recovery_attempt_id {
            return Err(
                "DML raised direct mutation fence was not minted by the current recovery attempt"
                    .to_string(),
            );
        }
        if let Some(old_fence) = &record.request.old_fence
            && raised.generation() <= old_fence.generation()
        {
            return Err(
                "DML raised direct mutation fence must be strictly above the old attempt's fence"
                    .to_string(),
            );
        }
    }
    if let Some(result) = &record.result {
        validate_historical_data_mutation_result(&record.request, result)?;
        if record.raised_fence.is_none() {
            return Err(
                "DML historical data mutation inspection requires a durable raised external fence"
                    .to_string(),
            );
        }
    }
    validate_historical_data_mutation_phase_shape(record)?;
    let encoded = serde_json::to_vec(record).map_err(|error| error.to_string())?;
    if encoded.len() > DML_HISTORICAL_DATA_MUTATION_RECOVERY_ENCODED_LIMIT {
        return Err(format!(
            "DML historical data mutation recovery encoded size {} exceeds limit {DML_HISTORICAL_DATA_MUTATION_RECOVERY_ENCODED_LIMIT}",
            encoded.len()
        ));
    }
    Ok(())
}

fn validate_historical_data_mutation_phase_shape(
    record: &DmlHistoricalDataMutationRecoveryRecord,
) -> Result<(), String> {
    let cleanup = record.cleanup();
    let phase = record.phase;
    let shape_ok = match phase {
        DmlHistoricalRecoveryPhase::Requested => {
            record.raised_fence.is_none() && record.result.is_none()
        }
        DmlHistoricalRecoveryPhase::FenceRaised => {
            record.raised_fence.is_some() && record.result.is_none()
        }
        DmlHistoricalRecoveryPhase::Inspected => {
            cleanup.is_some_and(|cleanup| cleanup != DmlHistoricalCleanupState::Pending)
        }
        DmlHistoricalRecoveryPhase::CleanupPending => {
            cleanup == Some(DmlHistoricalCleanupState::Pending)
        }
        DmlHistoricalRecoveryPhase::Resolved => {
            cleanup.is_some_and(|cleanup| cleanup != DmlHistoricalCleanupState::Pending)
        }
        DmlHistoricalRecoveryPhase::Unresolved => record
            .result
            .as_ref()
            .is_none_or(|result| !result.disposition.is_determined()),
    };
    if !shape_ok {
        return Err(format!(
            "DML historical data mutation recovery phase {} disagrees with its durable facts",
            phase.as_str()
        ));
    }
    let next_action_ok = if phase == DmlHistoricalRecoveryPhase::Resolved {
        if cleanup == Some(DmlHistoricalCleanupState::ManualRetention) {
            record.next_action == StatementNextAction::ManualInspect
        } else {
            record.next_action == StatementNextAction::None
        }
    } else {
        record.next_action != StatementNextAction::None
    };
    if !next_action_ok {
        return Err(format!(
            "DML historical data mutation recovery phase {} disagrees with next action {:?}",
            phase.as_str(),
            record.next_action
        ));
    }
    Ok(())
}

/// Validate that replacing `existing` with `next` preserves the immutable
/// request, keeps the cycle and phase monotone, never lowers the raised fence,
/// and never drops a retained cleanup obligation.
pub fn validate_historical_data_mutation_recovery_transition(
    existing: Option<&DmlHistoricalDataMutationRecoveryRecord>,
    next: &DmlHistoricalDataMutationRecoveryRecord,
) -> Result<(), String> {
    validate_historical_data_mutation_recovery(next)?;
    let Some(existing) = existing else {
        if next.recovery_cycle != 1 || next.phase != DmlHistoricalRecoveryPhase::Requested {
            return Err(
                "a new DML historical data mutation recovery must start at cycle 1 in REQUESTED phase"
                    .to_string(),
            );
        }
        return Ok(());
    };
    if existing.request != next.request {
        return Err(
            "a DML historical data mutation recovery request is immutable once durable".to_string(),
        );
    }
    if existing.phase == DmlHistoricalRecoveryPhase::Resolved && existing != next {
        return Err(
            "a resolved DML historical data mutation recovery cannot be reopened".to_string(),
        );
    }
    if next.recovery_cycle < existing.recovery_cycle {
        return Err(
            "DML historical data mutation recovery cycle must not move backwards".to_string(),
        );
    }
    if next.recovery_cycle == existing.recovery_cycle {
        if next.recovery_attempt_id != existing.recovery_attempt_id {
            return Err(
                "a DML historical data mutation recovery cycle is owned by one coordination attempt"
                    .to_string(),
            );
        }
        if next.phase.progress() < existing.phase.progress() {
            return Err(format!(
                "DML historical data mutation recovery phase cannot move from {} back to {} inside one cycle",
                existing.phase.as_str(),
                next.phase.as_str()
            ));
        }
    }
    // A raised fence closed the old authority. Neither a later phase nor a new
    // cycle may forget or lower it.
    match (&existing.raised_fence, &next.raised_fence) {
        (Some(previous), Some(raised)) => {
            validate_direct_mutation_fence_transition(Some(previous), raised)?;
        }
        (Some(_), None) => {
            return Err(
                "a raised DML direct mutation fence cannot be dropped from a historical recovery"
                    .to_string(),
            );
        }
        _ => {}
    }
    if existing.cleanup() == Some(DmlHistoricalCleanupState::Pending)
        && !matches!(
            next.cleanup(),
            Some(
                DmlHistoricalCleanupState::Pending
                    | DmlHistoricalCleanupState::Completed
                    | DmlHistoricalCleanupState::ManualRetention
            )
        )
    {
        return Err(
            "a pending DML historical data mutation cleanup outcome must be retained until it completes or is explicitly kept for manual retention"
                .to_string(),
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExternalFactOutcome {
    KnownCommitted,
    NoOp,
    KnownUncommitted,
    CommitUnknown,
    Unsupported,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementNextAction {
    None,
    Reconcile,
    AbortStaging,
    RetryFinalize,
    ManualInspect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableExternalFact {
    pub outcome: ExternalFactOutcome,
    #[serde(default)]
    pub receipt: Option<String>,
    #[serde(default)]
    pub evidence: Option<String>,
    #[serde(default)]
    pub finalization_failure: Option<String>,
    #[serde(default)]
    pub failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableMutationSummary {
    pub file_count: u32,
    pub row_count: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CtasSagaPhase {
    PreparingSource,
    PreparingStagedTable,
    PrepareUnknown,
    Staged,
    Writing,
    WriteUnknown,
    Publishing,
    PublishUnknown,
    AbortingStaging,
    AbortUnknown,
    Committed,
    NoOp,
    Conflict,
    Failed,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CtasSagaRecord {
    pub phase: CtasSagaPhase,
    pub prepare_operation_id: Uuid,
    pub write_operation_id: Uuid,
    pub publish_operation_id: Uuid,
    pub abort_staging_operation_id: Uuid,
    pub create_policy: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub connector_instance_id: Option<String>,
    #[serde(default)]
    pub connector_incarnation: Option<String>,
    #[serde(default)]
    pub source_plan_digest: Option<String>,
    #[serde(default)]
    pub source_schema_digest: Option<String>,
    #[serde(default)]
    pub source_execution_identity: Option<String>,
    #[serde(default)]
    pub write_cohort_id: Option<String>,
    #[serde(default)]
    pub staged_handle_digest: Option<String>,
    /// Provider-neutral digest of the exact sealed distributed-writer cohort
    /// set. It is durable before writer dispatch and is required to inspect a
    /// historical CTAS write without decoding provider evidence.
    #[serde(default)]
    pub write_cohort_set_digest: Option<String>,
    #[serde(default)]
    pub aggregate_write_digest: Option<String>,
    #[serde(default)]
    pub prepare_fact: Option<DurableExternalFact>,
    #[serde(default)]
    pub write_fact: Option<DurableExternalFact>,
    #[serde(default)]
    pub publish_fact: Option<DurableExternalFact>,
    #[serde(default)]
    pub abort_staging_fact: Option<DurableExternalFact>,
    pub next_action: StatementNextAction,
}

impl CtasSagaRecord {
    /// A crash-only CTAS has one statement publication identity across stage,
    /// write and publication. It deliberately has no durable recovery action:
    /// an interrupted attempt is diagnosed manually and its owned residue is
    /// retired only by age-based lake GC.
    pub fn is_crash_only_publication(&self) -> bool {
        self.prepare_operation_id == self.write_operation_id
            && self.prepare_operation_id == self.publish_operation_id
            && self.prepare_operation_id == self.abort_staging_operation_id
    }
}

// ---------------------------------------------------------------------------
// CP-3D: catalog-fenced CTAS takeover and historical recovery.
//
// These records deliberately contain no provider metadata. Receipts and staged
// locators are bounded opaque strings; everything else is a stable identity,
// scalar, digest, dispatch checkpoint, or retention decision. The provider is
// the only component that may interpret an opaque value.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TruncateLifecyclePhase {
    Preparing,
    Planned,
    Executing,
    CommitUnknown,
    Reconciling,
    Committed,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddFilesLifecyclePhase {
    Preparing,
    Planned,
    Executing,
    CommitUnknown,
    Reconciling,
    Committed,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddFilesArtifactKind {
    Plan,
    Receipt,
    Evidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AddFilesArtifactDescriptor {
    pub kind: AddFilesArtifactKind,
    pub codec_version: u16,
    pub total_length: u32,
    pub chunk_count: u16,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddFilesDispatchCertainty {
    ConfirmedNotDispatched,
    PossiblyDispatched,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceScopeOwnership {
    Unclaimed,
    ReservedImmutable,
    Frozen,
    TableOwned,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AddFilesLifecycleRecord {
    pub phase: AddFilesLifecyclePhase,
    pub connector_operation_id: Uuid,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub connector_instance_id: Option<String>,
    #[serde(default)]
    pub connector_incarnation: Option<String>,
    pub source_location: String,
    #[serde(default)]
    pub source_scope_version: Option<u16>,
    #[serde(default)]
    pub source_scope_kind: Option<String>,
    #[serde(default)]
    pub source_scope_digest: Option<String>,
    #[serde(default)]
    pub request_digest: Option<String>,
    #[serde(default)]
    pub plan_digest: Option<String>,
    #[serde(default)]
    pub state_digest: Option<String>,
    #[serde(default)]
    pub plan_summary: Option<DurableMutationSummary>,
    #[serde(default)]
    pub plan_artifact: Option<AddFilesArtifactDescriptor>,
    #[serde(default)]
    pub receipt_artifact: Option<AddFilesArtifactDescriptor>,
    #[serde(default)]
    pub evidence_artifact: Option<AddFilesArtifactDescriptor>,
    pub dispatch_certainty: AddFilesDispatchCertainty,
    pub source_ownership: SourceScopeOwnership,
    #[serde(default)]
    pub outcome: Option<DurableExternalFact>,
    pub next_action: StatementNextAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TruncateLifecycleRecord {
    pub phase: TruncateLifecyclePhase,
    pub connector_operation_id: Uuid,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub connector_instance_id: Option<String>,
    #[serde(default)]
    pub connector_incarnation: Option<String>,
    pub target_ref: String,
    #[serde(default)]
    pub request_digest: Option<String>,
    #[serde(default)]
    pub plan_digest: Option<String>,
    #[serde(default)]
    pub state_digest: Option<String>,
    #[serde(default)]
    pub plan_summary: Option<DurableMutationSummary>,
    #[serde(default)]
    pub outcome: Option<DurableExternalFact>,
    pub next_action: StatementNextAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "details", rename_all = "SCREAMING_SNAKE_CASE")]
#[expect(
    clippy::large_enum_variant,
    reason = "The durable DML journal codec keeps its existing variant shape for backward-compatible persisted records."
)]
pub enum OperationPayload {
    ConnectorWriteLifecycle(ConnectorWriteLifecycleRecord),
    CtasSaga(CtasSagaRecord),
    TruncateLifecycle(TruncateLifecycleRecord),
    AddFilesLifecycle(AddFilesLifecycleRecord),
}

/// True when the bounded recovery scan must keep visiting this operation.
///
/// The user-visible statement state is not the whole truth. A durable CP-3B
/// historical write recovery keeps the obligation alive after the statement
/// result became terminal, so a pending external finalization or proof-bound
/// cleanup cannot be dropped just because the user already saw a failure.
///
/// Callers that compute a recovery due for an authorized mutation must use this
/// function, not the operation state alone.
///
/// This form cannot see a CP-3C historical data-mutation recovery, so it is a
/// lower bound for TRUNCATE and ADD FILES. Callers that can read that side
/// record must use [`operation_requires_recovery_scan_with_direct_mutation`].
pub fn operation_requires_recovery_scan(
    state: OperationState,
    payload: &OperationPayload,
    historical_write_recovery: Option<&DmlHistoricalWriteRecoveryRecord>,
) -> bool {
    if matches!(payload, OperationPayload::CtasSaga(_)) {
        return false;
    }
    if matches!(
        payload,
        OperationPayload::TruncateLifecycle(record)
            if record.next_action == StatementNextAction::ManualInspect
    ) || matches!(
        payload,
        OperationPayload::AddFilesLifecycle(record)
            if record.next_action == StatementNextAction::ManualInspect
    ) {
        // A crash-only direct-mutation unknown is terminal from the recovery
        // controller's perspective even when its publication result remains
        // unknown to the user. Manual inspection and age-gated GC own the
        // retained evidence; no process may re-drive the old attempt.
        return false;
    }
    if !state.is_finished() {
        return true;
    }
    if historical_write_recovery
        .is_some_and(DmlHistoricalWriteRecoveryRecord::requires_recovery_scan)
    {
        return true;
    }
    match payload {
        OperationPayload::ConnectorWriteLifecycle(_) => false,
        OperationPayload::CtasSaga(record) => record.next_action != StatementNextAction::None,
        // LNP-1 has no in-process recovery authority after a direct mutation
        // reaches a terminal result. `ManualInspect` is diagnostic guidance,
        // not a license to re-drive the old catalog attempt.
        OperationPayload::TruncateLifecycle(_) | OperationPayload::AddFilesLifecycle(_) => false,
    }
}

/// True when the bounded recovery scan must keep visiting this operation, with
/// both durable historical recovery records in view.
///
/// An open CP-3C historical data-mutation recovery keeps its due exactly like an
/// open CP-3B historical write recovery: a terminal TRUNCATE or ADD FILES result
/// must not discard an outstanding provider inspection or a proof-bound guarded
/// cleanup, and it must not release an ADD FILES source scope by forgetting it.
pub fn operation_requires_recovery_scan_with_direct_mutation(
    state: OperationState,
    payload: &OperationPayload,
    historical_write_recovery: Option<&DmlHistoricalWriteRecoveryRecord>,
    historical_data_mutation_recovery: Option<&DmlHistoricalDataMutationRecoveryRecord>,
) -> bool {
    if operation_requires_recovery_scan(state, payload, historical_write_recovery) {
        return true;
    }
    historical_data_mutation_recovery
        .is_some_and(DmlHistoricalDataMutationRecoveryRecord::requires_recovery_scan)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesArtifact {
    pub descriptor: AddFilesArtifactDescriptor,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddFilesSourceAction {
    Reserve {
        provider_id: String,
        scope_digest: String,
        ownership: SourceScopeOwnership,
    },
    Transition {
        provider_id: String,
        scope_digest: String,
        expected: SourceScopeOwnership,
        ownership: SourceScopeOwnership,
    },
    Release {
        provider_id: String,
        scope_digest: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddFilesMutationRequest {
    pub operation: OperationMutationRequest,
    pub artifacts: Vec<AddFilesArtifact>,
    pub source_action: Option<AddFilesSourceAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePreparingRequest {
    /// The one UUIDv7 publication identity allocated during typed statement
    /// admission. It is also the durable operation key.
    pub publication_id: DmlOperationId,
    pub operation_kind: OperationKind,
    pub operation_subkind: Option<String>,
    pub target: OperationTarget,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOperation {
    pub schema_version: u8,
    pub operation_id: DmlOperationId,
    pub revision: u64,
    pub last_mutation_id: Uuid,
    pub operation_kind: OperationKind,
    #[serde(default)]
    pub operation_subkind: Option<String>,
    pub target: OperationTarget,
    pub state: OperationState,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    pub payload: OperationPayload,
    pub coordination_provenance: Option<DmlCoordinationProvenance>,
    pub recovery_due_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateStatementOperationRequest {
    pub operation_id: DmlOperationId,
    pub mutation_id: Uuid,
    pub operation_kind: OperationKind,
    pub target: OperationTarget,
    pub attempt_id: String,
    pub payload: OperationPayload,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationMutationRequest {
    pub operation_id: DmlOperationId,
    pub expected_revision: u64,
    pub mutation_id: Uuid,
    pub state: OperationState,
    pub payload: OperationPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionSpec {
    /// The one UUIDv7 publication identity allocated during typed statement
    /// admission. It is reused by the connector request and durable journal.
    pub publication_id: DmlOperationId,
    pub target: OperationTarget,
    pub operation_kind: OperationKind,
    /// Stable statement refinement retained by the frontend lifecycle.
    pub operation_subkind: Option<String>,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionOutcome {
    pub operation_id: Option<DmlOperationId>,
    pub committed_receipt: Option<ConnectorWriteReceipt>,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn connector_write_lifecycle_uses_only_opaque_spi_wire() {
        let receipt =
            ConnectorWriteReceipt::try_new(Bytes::from_static(b"opaque")).expect("receipt");
        let lifecycle = ConnectorWriteLifecycleRecord::KnownCommitted {
            receipt_wire: ConnectorWriteReceiptWire::try_from_receipt(&receipt).expect("wire"),
            finalization: ConnectorWriteFinalizationRecord::Complete,
        };
        let encoded = serde_json::to_vec(&lifecycle).expect("JSON");
        assert!(!String::from_utf8_lossy(&encoded).contains("snapshot_id"));
        assert_eq!(
            serde_json::from_slice::<ConnectorWriteLifecycleRecord>(&encoded).expect("decode"),
            lifecycle
        );
    }

    #[test]
    fn lifecycle_tag_rejects_a_receipt_evidence_hybrid() {
        let hybrid = br#"{
            "outcome":"KNOWN_COMMITTED",
            "evidence_wire":[1,2,3]
        }"#;
        assert!(serde_json::from_slice::<ConnectorWriteLifecycleRecord>(hybrid).is_err());
    }

    #[test]
    fn statement_payload_json_round_trips() {
        let ctas = OperationPayload::CtasSaga(CtasSagaRecord {
            phase: CtasSagaPhase::PublishUnknown,
            prepare_operation_id: Uuid::now_v7(),
            write_operation_id: Uuid::now_v7(),
            publish_operation_id: Uuid::now_v7(),
            abort_staging_operation_id: Uuid::now_v7(),
            create_policy: "FAIL_IF_EXISTS".to_string(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("03".repeat(16)),
            source_plan_digest: Some("source".to_string()),
            source_schema_digest: Some("schema".to_string()),
            source_execution_identity: Some("execution".to_string()),
            write_cohort_id: Some("cohort".to_string()),
            staged_handle_digest: Some("staged".to_string()),
            write_cohort_set_digest: None,
            aggregate_write_digest: Some("write".to_string()),
            prepare_fact: None,
            write_fact: None,
            publish_fact: Some(DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: Some("evidence".to_string()),
                finalization_failure: None,
                failure: Some("unknown".to_string()),
            }),
            abort_staging_fact: None,
            next_action: StatementNextAction::Reconcile,
        });
        let encoded = serde_json::to_vec(&ctas).unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationPayload>(&encoded).unwrap(),
            ctas
        );

        let truncate = OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
            phase: TruncateLifecyclePhase::Executing,
            connector_operation_id: Uuid::now_v7(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("04".repeat(16)),
            target_ref: "main".to_string(),
            request_digest: Some("request".to_string()),
            plan_digest: Some("plan".to_string()),
            state_digest: Some("state".to_string()),
            plan_summary: Some(DurableMutationSummary {
                file_count: 3,
                row_count: 4,
                total_bytes: 5,
            }),
            outcome: None,
            next_action: StatementNextAction::None,
        });
        let encoded = serde_json::to_vec(&truncate).unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationPayload>(&encoded).unwrap(),
            truncate
        );

        let add_files = OperationPayload::AddFilesLifecycle(AddFilesLifecycleRecord {
            phase: AddFilesLifecyclePhase::CommitUnknown,
            connector_operation_id: Uuid::now_v7(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("05".repeat(16)),
            source_location: "s3://warehouse/import".to_string(),
            source_scope_version: Some(1),
            source_scope_kind: Some("DIRECTORY".to_string()),
            source_scope_digest: Some("06".repeat(32)),
            request_digest: Some("request".to_string()),
            plan_digest: Some("plan".to_string()),
            state_digest: Some("state".to_string()),
            plan_summary: Some(DurableMutationSummary {
                file_count: 1,
                row_count: 2,
                total_bytes: 3,
            }),
            plan_artifact: Some(AddFilesArtifactDescriptor {
                kind: AddFilesArtifactKind::Plan,
                codec_version: 1,
                total_length: 4,
                chunk_count: 1,
                sha256: "07".repeat(32),
            }),
            receipt_artifact: None,
            evidence_artifact: Some(AddFilesArtifactDescriptor {
                kind: AddFilesArtifactKind::Evidence,
                codec_version: 1,
                total_length: 5,
                chunk_count: 1,
                sha256: "08".repeat(32),
            }),
            dispatch_certainty: AddFilesDispatchCertainty::PossiblyDispatched,
            source_ownership: SourceScopeOwnership::Frozen,
            outcome: Some(DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: None,
                finalization_failure: None,
                failure: Some("provider response lost".to_string()),
            }),
            next_action: StatementNextAction::ManualInspect,
        });
        let encoded = serde_json::to_vec(&add_files).unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationPayload>(&encoded).unwrap(),
            add_files
        );
    }

    #[test]
    fn unsafe_statement_state_shortcuts_are_rejected() {
        assert!(
            validate_operation_transition(OperationState::Preparing, OperationState::Finalized)
                .is_err()
        );
        assert!(
            validate_operation_transition(
                OperationState::CommitUnknown,
                OperationState::FailedKnownUncommitted,
            )
            .is_ok()
        );
        assert!(OperationState::Aborted.is_finished());
        assert!(!OperationState::CommitUnknown.is_finished());
    }

    #[test]
    fn typed_abort_preserves_known_and_unknown_commit_certainty() {
        assert!(
            validate_operation_transition(OperationState::Aborting, OperationState::Committed,)
                .is_ok()
        );
        assert!(
            validate_operation_transition(OperationState::Aborting, OperationState::CommitUnknown,)
                .is_ok()
        );
        assert!(
            validate_operation_transition(OperationState::Aborting, OperationState::Finalized,)
                .is_err()
        );
    }

    #[test]
    fn ctas_unknown_and_cleanup_edges_do_not_widen_other_dml() {
        let ctas_edges = [
            (OperationState::Preparing, OperationState::CommitUnknown),
            (OperationState::Writing, OperationState::CommitUnknown),
            (OperationState::Committing, OperationState::Aborting),
            (OperationState::CommitUnknown, OperationState::Aborting),
            (OperationState::CommitUnknown, OperationState::Writing),
            (OperationState::CommitUnknown, OperationState::Committing),
        ];
        for (from, to) in ctas_edges {
            assert!(
                validate_statement_operation_transition(
                    OperationKind::CreateTableAsSelect,
                    from,
                    to,
                )
                .is_ok()
            );
            assert!(
                validate_statement_operation_transition(OperationKind::InsertAppend, from, to)
                    .is_err()
            );
            assert!(validate_operation_transition(from, to).is_err());
        }
    }

    #[test]
    fn manual_direct_mutation_unknown_never_schedules_recovery() {
        let payload = OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
            phase: TruncateLifecyclePhase::CommitUnknown,
            connector_operation_id: Uuid::now_v7(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("01".repeat(16)),
            target_ref: "main".to_string(),
            request_digest: None,
            plan_digest: None,
            state_digest: None,
            plan_summary: None,
            outcome: None,
            next_action: StatementNextAction::ManualInspect,
        });
        assert!(
            !operation_requires_recovery_scan(OperationState::CommitUnknown, &payload, None),
            "manual inspection must not make a fresh process recover an old direct mutation"
        );
        assert!(
            !operation_requires_recovery_scan_with_direct_mutation(
                OperationState::CommitUnknown,
                &payload,
                None,
                None,
            ),
            "the direct-mutation variant must preserve the same crash-only boundary"
        );
    }

    #[test]
    fn crash_only_ctas_unknown_never_schedules_recovery() {
        let publication_id = Uuid::now_v7();
        let payload = OperationPayload::CtasSaga(CtasSagaRecord {
            phase: CtasSagaPhase::PublishUnknown,
            prepare_operation_id: publication_id,
            write_operation_id: publication_id,
            publish_operation_id: publication_id,
            abort_staging_operation_id: publication_id,
            create_policy: CTAS_CREATE_POLICY_FAIL_IF_EXISTS.to_string(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some("01".repeat(16)),
            source_plan_digest: Some("source".to_string()),
            source_schema_digest: Some("schema".to_string()),
            source_execution_identity: Some("execution".to_string()),
            write_cohort_id: Some("cohort".to_string()),
            staged_handle_digest: Some("staged".to_string()),
            write_cohort_set_digest: None,
            aggregate_write_digest: Some("write".to_string()),
            prepare_fact: None,
            write_fact: None,
            publish_fact: Some(DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: Some("publication response lost".to_string()),
                finalization_failure: None,
                failure: Some("publication unknown".to_string()),
            }),
            abort_staging_fact: None,
            next_action: StatementNextAction::ManualInspect,
        });
        let OperationPayload::CtasSaga(saga) = &payload else {
            unreachable!("the test constructs a CTAS payload");
        };
        assert!(saga.is_crash_only_publication());
        assert!(
            !operation_requires_recovery_scan(OperationState::CommitUnknown, &payload, None),
            "manual inspection must not grant a new process authority to recover a CTAS attempt"
        );
        assert!(
            !operation_requires_recovery_scan_with_direct_mutation(
                OperationState::CommitUnknown,
                &payload,
                None,
                None,
            ),
            "the extended recovery scan must preserve the same crash-only CTAS boundary"
        );
    }
}
