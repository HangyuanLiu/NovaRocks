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

//! Provider-neutral, cross-incarnation inspection for a *direct* data mutation.
//!
//! TRUNCATE and ADD FILES do not run through the distributed writer. They are
//! planned once, executed once by one exact Connector generation, and can be
//! reconciled only while that generation is still alive. After a frontend
//! takeover the old generation is gone for good, so the only admissible
//! evidence about it is immutable external truth.
//!
//! This is deliberately not a second [`ConnectorDataMutation`] contract and it
//! is installed as its own capability. An ordinary execution path must never
//! reach it as a fallback, and it must never call an ordinary old-owner method
//! (`plan_mutation` / `execute` / `reconcile`), revive the historical binding,
//! or construct a historical runtime session. A destructive mutation that may
//! already have been dispatched is *classified*, never replayed.
//!
//! Two direct-mutation facts have no analogue in the distributed-write facet
//! and are frozen here:
//!
//! 1. **ADD FILES owns an immutable source scope.** The descriptor binds it,
//!    the provider never reasons about releasing it, and a source set that is
//!    only partially visible in the table is [`PartiallyApplied`] — never
//!    [`Applied`] and never [`NotApplied`].
//! 2. **TRUNCATE is destructive.** Its recovery may only ever classify. A
//!    continuation authorizes the *current* generation to run the statement
//!    again against a freshly proven base state; it never resurrects the old
//!    prepared handle.
//!
//! Every descriptor and observation is digest sealed and every provider payload
//! is a bounded opaque container. The frontend persists identity, generation
//! scalars, digests and opaque bytes only; it never decodes a payload and never
//! interprets a file list, object path, manifest or snapshot membership.
//!
//! [`ConnectorDataMutation`]: super::ConnectorDataMutation
//! [`PartiallyApplied`]: ConnectorHistoricalDataMutationDisposition::PartiallyApplied
//! [`Applied`]: ConnectorHistoricalDataMutationDisposition::Applied
//! [`NotApplied`]: ConnectorHistoricalDataMutationDisposition::NotApplied

// Design: ADR-0065 (docs/adr/ADR-0065-external-write-fence-as-catalog-linearization-point.md)

use std::fmt;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorCommittedVersion, ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt,
    ConnectorDataMutationSourceScope, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExternalFenceFailure, ConnectorExternalFenceReceipt,
    ConnectorExternalOperationFence, ConnectorInstanceDescriptor, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorWriteOperationId,
    ConnectorWriteTargetRef, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, REGISTER_EXISTING_FILES_KIND, TRUNCATE_KIND,
};

pub const MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_PROOF_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_CHECKPOINTS: usize = 4096;

const CONNECTOR_HISTORICAL_DATA_MUTATION_DESCRIPTOR_DOMAIN: &[u8] =
    b"novarocks.historical-data-mutation-descriptor.v1\0";
const CONNECTOR_HISTORICAL_DATA_MUTATION_OBSERVATION_DOMAIN: &[u8] =
    b"novarocks.historical-data-mutation-observation.v1\0";
const CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_DOMAIN: &[u8] =
    b"novarocks.historical-data-mutation-continuation.v1\0";

/// Which direct-mutation family a historical operation belongs to.
///
/// The family is not decoration: ADD FILES owns an immutable external source
/// scope and TRUNCATE is destructive, so the two have different safe answers
/// for the same external observation. Carrying the family as a closed enum —
/// rather than as a free-form operation-kind string — makes it structurally
/// impossible for a descriptor to claim one family's identity and another
/// family's semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalDataMutationFamily {
    Truncate,
    RegisterExistingFiles,
}

impl ConnectorHistoricalDataMutationFamily {
    /// The `ConnectorDataMutation` operation kind this family plans and
    /// executes under. It is derived, never supplied, so the sealed descriptor
    /// and the provider's own marker can only ever agree.
    pub const fn operation_kind(self) -> &'static str {
        match self {
            Self::Truncate => TRUNCATE_KIND,
            Self::RegisterExistingFiles => REGISTER_EXISTING_FILES_KIND,
        }
    }

    /// Whether this family owns a durable external source scope that a
    /// recovery must protect.
    pub const fn owns_source_scope(self) -> bool {
        matches!(self, Self::RegisterExistingFiles)
    }
}

/// The frontend-observed phase a historical direct mutation had reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalDataMutationPhase {
    Prepared,
    Planned,
    FenceEstablished,
    ExecuteDispatched,
    ExecuteCompleted,
}

/// Whether the historical owner is known to have dispatched a phase. This is a
/// frontend observation of its own journal, never a provider conclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalDataMutationDispatchState {
    NotDispatched,
    Dispatched,
    Completed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationCheckpoint {
    pub phase: ConnectorHistoricalDataMutationPhase,
    pub state: ConnectorHistoricalDataMutationDispatchState,
    pub evidence_digest: Option<[u8; 32]>,
}

/// The typed external fence fact of the historical attempt.
///
/// `NotEstablished` is a provable historical state, not a missing value: an
/// owner may crash after planning and before it establishes any fence.
// This is a sealed value fact, not a hot enum: keeping the fence inline matches
// the other sealed SPI request/preparation enums.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalDataMutationFence {
    NotEstablished,
    Established {
        fence: ConnectorExternalOperationFence,
        receipt_digest: [u8; 32],
    },
}

impl ConnectorHistoricalDataMutationFence {
    pub fn established(
        receipt: &ConnectorExternalFenceReceipt,
        fence: ConnectorExternalOperationFence,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        receipt.validate()?;
        if !receipt.matches(&fence) {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "historical data mutation fence receipt does not acknowledge its fence",
            ));
        }
        Ok(Self::Established {
            fence,
            receipt_digest: receipt.digest(),
        })
    }

    pub fn fence(&self) -> Option<&ConnectorExternalOperationFence> {
        match self {
            Self::NotEstablished => None,
            Self::Established { fence, .. } => Some(fence),
        }
    }

    pub const fn is_established(&self) -> bool {
        matches!(self, Self::Established { .. })
    }

    fn digest_into(&self, hasher: &mut Sha256) {
        match self {
            Self::NotEstablished => hasher.update([0u8]),
            Self::Established {
                fence,
                receipt_digest,
            } => {
                hasher.update([1u8]);
                hasher.update(fence.digest());
                hasher.update(receipt_digest);
            }
        }
    }
}

/// The immutable identity half of a historical direct-mutation descriptor.
///
/// Every field is a value the frontend already holds in its fenced journal.
/// Together they reproduce the exact provenance the ordinary direct-mutation
/// path stamps into external truth, so a provider can decide whether external
/// truth already contains this operation without any process-local memory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationIdentity {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub table: ConnectorTableIdentity,
    pub target_ref: ConnectorWriteTargetRef,
    pub operation_id: ConnectorMutationOperationId,
    pub family: ConnectorHistoricalDataMutationFamily,
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub plan_summary: ConnectorDataMutationPlanSummary,
    /// Required for ADD FILES, forbidden for TRUNCATE. The provider never
    /// decides to release it; it is bound here so an observation can only ever
    /// answer the exact immutable source set the historical plan owned.
    pub source_scope: Option<ConnectorDataMutationSourceScope>,
}

/// The fence half of a historical direct-mutation descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationFenceFacts {
    pub historical_fence: ConnectorHistoricalDataMutationFence,
    pub raised_fence: ConnectorExternalOperationFence,
    pub raised_fence_receipt_digest: [u8; 32],
}

/// Complete value-only description of one historical direct mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationDescriptor {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub table: ConnectorTableIdentity,
    pub target_ref: ConnectorWriteTargetRef,
    pub operation_id: ConnectorMutationOperationId,
    pub family: ConnectorHistoricalDataMutationFamily,
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub plan_summary: ConnectorDataMutationPlanSummary,
    pub source_scope: Option<ConnectorDataMutationSourceScope>,
    pub historical_fence: ConnectorHistoricalDataMutationFence,
    /// The strictly higher fence the current owner established before this
    /// inspection. Without it a historical operation can never be classified
    /// as safe to retry.
    pub raised_fence: ConnectorExternalOperationFence,
    pub raised_fence_receipt_digest: [u8; 32],
    pub checkpoints: Vec<ConnectorHistoricalDataMutationCheckpoint>,
    /// The opaque provider evidence the historical attempt returned when its
    /// commit outcome was unknown. It names the staged artifacts of that
    /// attempt and is used only for cross-checks; it never outranks external
    /// truth, and evidence that cannot be tied to this descriptor is a reason
    /// to refuse a conclusion.
    pub evidence: Option<ExternalMutationEvidence>,
    digest: [u8; 32],
}

impl ConnectorHistoricalDataMutationDescriptor {
    pub fn try_new(
        identity: ConnectorHistoricalDataMutationIdentity,
        fences: ConnectorHistoricalDataMutationFenceFacts,
        checkpoints: Vec<ConnectorHistoricalDataMutationCheckpoint>,
        evidence: Option<ExternalMutationEvidence>,
    ) -> Result<Self, ConnectorError> {
        let ConnectorHistoricalDataMutationIdentity {
            historical_binding,
            table,
            target_ref,
            operation_id,
            family,
            request_digest,
            plan_digest,
            state_digest,
            plan_summary,
            source_scope,
        } = identity;
        let ConnectorHistoricalDataMutationFenceFacts {
            historical_fence,
            raised_fence,
            raised_fence_receipt_digest,
        } = fences;
        if checkpoints.is_empty()
            || checkpoints.len() > MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_CHECKPOINTS
        {
            return Err(invalid(
                "historical data mutation descriptor must carry 1..=4096 dispatch checkpoints",
            ));
        }
        for (label, digest) in [
            ("request", request_digest),
            ("plan", plan_digest),
            ("state", state_digest),
        ] {
            if digest == [0; 32] {
                return Err(invalid(format!(
                    "historical data mutation descriptor must carry a sealed {label} digest"
                )));
            }
        }
        if raised_fence_receipt_digest == [0; 32] {
            return Err(invalid(
                "historical data mutation descriptor must carry the raised fence receipt digest",
            ));
        }
        // ADD FILES without its source scope could be classified as applied and
        // then have its source silently released; TRUNCATE with one would claim
        // ownership over an external location it never read.
        match (family.owns_source_scope(), &source_scope) {
            (true, Some(scope)) => scope.validate()?,
            (true, None) => {
                return Err(invalid(
                    "historical ADD FILES descriptor must bind its immutable source scope",
                ));
            }
            (false, Some(_)) => {
                return Err(invalid(
                    "historical TRUNCATE descriptor must not carry a source scope",
                ));
            }
            (false, None) => {}
        }
        // The fence is keyed by the same stable operation identity the direct
        // mutation is keyed by, so one operation can never borrow another's
        // fence marker.
        raised_fence.validate_for_operation(ConnectorWriteOperationId::from_bytes(
            operation_id.to_bytes(),
        ))?;
        if raised_fence.table() != &table || raised_fence.target_ref() != &target_ref {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "historical data mutation raised fence names another resource",
            ));
        }
        // The takeover order is fixed: the current owner must have raised a
        // strictly higher fence before it may inspect a historical operation.
        if let Some(historical) = historical_fence.fence()
            && !raised_fence.supersedes(historical)?
        {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "historical data mutation inspection requires a strictly higher raised external fence",
            ));
        }
        if let Some(evidence) = &evidence {
            if evidence.provider_payload().is_empty() {
                return Err(invalid(
                    "historical data mutation descriptor evidence must carry a provider payload",
                ));
            }
            if evidence.operation_id() != operation_id
                || evidence.operation_kind() != family.operation_kind()
            {
                return Err(invalid(
                    "historical data mutation descriptor evidence answers another operation",
                ));
            }
        }
        let digest = descriptor_digest(DescriptorDigestInput {
            historical_binding: &historical_binding,
            table: &table,
            target_ref: &target_ref,
            operation_id,
            family,
            request_digest,
            plan_digest,
            state_digest,
            plan_summary,
            source_scope,
            historical_fence: &historical_fence,
            raised_fence: &raised_fence,
            raised_fence_receipt_digest,
            checkpoints: &checkpoints,
            evidence: evidence.as_ref(),
        });
        Ok(Self {
            historical_binding,
            table,
            target_ref,
            operation_id,
            family,
            request_digest,
            plan_digest,
            state_digest,
            plan_summary,
            source_scope,
            historical_fence,
            raised_fence,
            raised_fence_receipt_digest,
            checkpoints,
            evidence,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            ConnectorHistoricalDataMutationIdentity {
                historical_binding: self.historical_binding.clone(),
                table: self.table.clone(),
                target_ref: self.target_ref.clone(),
                operation_id: self.operation_id,
                family: self.family,
                request_digest: self.request_digest,
                plan_digest: self.plan_digest,
                state_digest: self.state_digest,
                plan_summary: self.plan_summary,
                source_scope: self.source_scope,
            },
            ConnectorHistoricalDataMutationFenceFacts {
                historical_fence: self.historical_fence.clone(),
                raised_fence: self.raised_fence.clone(),
                raised_fence_receipt_digest: self.raised_fence_receipt_digest,
            },
            self.checkpoints.clone(),
            self.evidence.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical data mutation descriptor digest does not match its contents",
            ));
        }
        Ok(())
    }

    /// Whether the frontend journal proves the destructive `execute` was never
    /// dispatched. A provider may only issue a continuation for such an
    /// operation, which is what keeps "already dispatched is never replayed"
    /// a structural property rather than a convention.
    pub fn journal_proves_nothing_dispatched(&self) -> bool {
        self.checkpoints.iter().all(|checkpoint| {
            !matches!(
                checkpoint.phase,
                ConnectorHistoricalDataMutationPhase::ExecuteDispatched
                    | ConnectorHistoricalDataMutationPhase::ExecuteCompleted
            ) || checkpoint.state == ConnectorHistoricalDataMutationDispatchState::NotDispatched
        })
    }

    /// The stable write-operation identity the external fence is keyed by.
    pub fn fenced_operation_id(&self) -> ConnectorWriteOperationId {
        ConnectorWriteOperationId::from_bytes(self.operation_id.to_bytes())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Bounded opaque provider proof for one historical classification.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationProof {
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalDataMutationProof {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        if payload.is_empty() || payload.len() > MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_PROOF_BYTES
        {
            return Err(invalid(
                "historical data mutation proof exceeds its bounded payload limit",
            ));
        }
        Ok(Self {
            digest: Sha256::digest(&payload).into(),
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(self.payload.clone())?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical data mutation proof digest does not match its payload",
            ));
        }
        Ok(())
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorHistoricalDataMutationProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalDataMutationProof")
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// A provider-signed continuation authorizing the current generation to run the
/// same stable statement again.
///
/// It is only ever issued for a proven [`NotApplied`] operation whose journal
/// also proves nothing was dispatched. It never resurrects the historical
/// prepared handle: the current generation must plan again and re-prove table
/// identity and base state.
///
/// [`NotApplied`]: ConnectorHistoricalDataMutationDisposition::NotApplied
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationContinuation {
    raised_fence_digest: [u8; 32],
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalDataMutationContinuation {
    pub fn try_new(
        raised_fence: &ConnectorExternalOperationFence,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        raised_fence.validate()?;
        if payload.is_empty()
            || payload.len() > MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_BYTES
        {
            return Err(invalid(
                "historical data mutation continuation exceeds its bounded payload limit",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_DOMAIN);
        hasher.update(raised_fence.digest());
        hasher.update(payload.as_ref());
        Ok(Self {
            raised_fence_digest: raised_fence.digest(),
            payload,
            digest: hasher.finalize().into(),
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.payload.is_empty()
            || self.payload.len() > MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_BYTES
        {
            return Err(invalid(
                "historical data mutation continuation exceeds its bounded payload limit",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_DOMAIN);
        hasher.update(self.raised_fence_digest);
        hasher.update(self.payload.as_ref());
        let expected: [u8; 32] = hasher.finalize().into();
        if expected != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical data mutation continuation digest does not match its contents",
            ));
        }
        Ok(())
    }

    /// Whether this continuation is bound to the exact raised fence.
    pub fn is_bound_to(&self, raised_fence: &ConnectorExternalOperationFence) -> bool {
        self.raised_fence_digest == raised_fence.digest()
    }

    pub const fn raised_fence_digest(&self) -> [u8; 32] {
        self.raised_fence_digest
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorHistoricalDataMutationContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalDataMutationContinuation")
            .field("raised_fence_digest", &self.raised_fence_digest)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// The typed classification of one historical direct mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalDataMutationDisposition {
    /// The provider proved external truth already contains this operation in
    /// full: for ADD FILES, the whole sealed source set.
    Applied,
    /// The provider proved this operation never changed the table, that no
    /// historical authority can still change it, and that it left nothing
    /// behind to remove.
    NotApplied,
    /// The provider proved this operation never changed the table, but
    /// provider-owned artifacts of the historical attempt are still present.
    /// The frontend must drive a proof-bound guarded `cleanup` before the
    /// record is finished; the mutation itself did not apply.
    CleanupRequired,
    /// ADD FILES only: part of the sealed source set is provably inside the
    /// table and the rest cannot be proven either way. Never `Applied`, never
    /// `NotApplied`; the source scope stays owned by the operation.
    PartiallyApplied,
    /// The external fence has been advanced past this recovery attempt by
    /// another authority. Nothing here is ours to finish.
    Conflict,
    /// Evidence is insufficient. The recovery index must be retained and the
    /// ADD FILES source scope must not be released.
    Ambiguous,
    /// This provider cannot classify a historical direct mutation on this
    /// target.
    Unsupported,
}

impl ConnectorHistoricalDataMutationDisposition {
    /// Whether the current generation may issue a continuation. Only a proven
    /// `NotApplied` operation qualifies: every other disposition either changed
    /// the table, may still change it, or is unproven.
    pub const fn may_continue(self) -> bool {
        matches!(self, Self::NotApplied)
    }

    /// Whether this disposition is a resolved classification. `Ambiguous` and
    /// `Unsupported` are not: they keep the recovery record unresolved.
    pub const fn is_resolved(self) -> bool {
        !matches!(self, Self::Ambiguous | Self::Unsupported)
    }

    /// Whether this disposition permits the *frontend* to consider releasing an
    /// ADD FILES source scope.
    ///
    /// This is a necessary condition, never a sufficient one, and it is never a
    /// provider claim: the provider only reports what external truth proves,
    /// and the frontend releases a reservation solely inside the fenced journal
    /// transaction that also validates the immutable source-scope digest. A
    /// partially applied, conflicting, ambiguous or unsupported operation keeps
    /// its scope forever until an operator resolves it.
    pub const fn permits_source_scope_release(self) -> bool {
        matches!(self, Self::Applied | Self::NotApplied)
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::NotApplied => "not-applied",
            Self::CleanupRequired => "cleanup-required",
            Self::PartiallyApplied => "partially-applied",
            Self::Conflict => "conflict",
            Self::Ambiguous => "ambiguous",
            Self::Unsupported => "unsupported",
        }
    }
}

/// The finalization facts a provider must supply for an applied operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationApplication {
    pub committed_version: ConnectorCommittedVersion,
    pub receipt: ConnectorDataMutationReceipt,
    pub finalization: ExternalMutationFinalization,
}

/// The result half of a historical direct-mutation observation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationOutcomeFacts {
    pub application: Option<ConnectorHistoricalDataMutationApplication>,
    pub continuation: Option<ConnectorHistoricalDataMutationContinuation>,
    pub cleanup_required: bool,
}

/// A digest-sealed classification of one historical direct mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationObservation {
    pub disposition: ConnectorHistoricalDataMutationDisposition,
    pub operation_id: ConnectorMutationOperationId,
    pub family: ConnectorHistoricalDataMutationFamily,
    pub descriptor_digest: [u8; 32],
    pub raised_fence_digest: [u8; 32],
    /// Echoed verbatim from the descriptor so a persisted observation can never
    /// be paired with a different source set than the one it answered.
    pub source_scope: Option<ConnectorDataMutationSourceScope>,
    pub application: Option<ConnectorHistoricalDataMutationApplication>,
    pub continuation: Option<ConnectorHistoricalDataMutationContinuation>,
    pub cleanup_required: bool,
    pub proof: ConnectorHistoricalDataMutationProof,
    digest: [u8; 32],
}

impl ConnectorHistoricalDataMutationObservation {
    pub fn try_new(
        descriptor: &ConnectorHistoricalDataMutationDescriptor,
        disposition: ConnectorHistoricalDataMutationDisposition,
        outcome: ConnectorHistoricalDataMutationOutcomeFacts,
        proof: ConnectorHistoricalDataMutationProof,
    ) -> Result<Self, ConnectorError> {
        descriptor.validate()?;
        proof.validate()?;
        let ConnectorHistoricalDataMutationOutcomeFacts {
            application,
            continuation,
            cleanup_required,
        } = outcome;
        if let Some(application) = &application {
            application.committed_version.validate()?;
            application.receipt.validate()?;
            if application.receipt.operation_id() != descriptor.operation_id
                || application.receipt.operation_kind() != descriptor.family.operation_kind()
                || application.receipt.request_digest() != descriptor.request_digest
                || application.receipt.plan_digest() != descriptor.plan_digest
                || application.receipt.state_digest() != descriptor.state_digest
            {
                return Err(invalid(
                    "historical data mutation application receipt answers another operation",
                ));
            }
        }
        match disposition {
            ConnectorHistoricalDataMutationDisposition::Applied if application.is_none() => {
                return Err(invalid(
                    "an applied historical data mutation observation must carry its neutral receipt and finalization",
                ));
            }
            ConnectorHistoricalDataMutationDisposition::Applied => {}
            _ if application.is_some() => {
                return Err(invalid(
                    "only an applied historical data mutation observation may carry finalization facts",
                ));
            }
            _ => {}
        }
        if continuation.is_some() && !disposition.may_continue() {
            return Err(invalid(
                "only a proven not-applied historical data mutation observation may carry a continuation",
            ));
        }
        if let Some(continuation) = &continuation {
            continuation.validate()?;
            if !continuation.is_bound_to(&descriptor.raised_fence) {
                return Err(ConnectorError::external_fence(
                    ConnectorExternalFenceFailure::ForeignOperation,
                    "historical data mutation continuation is not bound to the raised external fence",
                ));
            }
            if !descriptor.journal_proves_nothing_dispatched() {
                return Err(invalid(
                    "historical data mutation continuation contradicts a dispatched journal checkpoint",
                ));
            }
            // Cleanup removes the very fence artifact the continuation's
            // authority rests on, so a caller must never be handed both and be
            // left to guess the order.
            if cleanup_required {
                return Err(invalid(
                    "a historical data mutation continuation cannot accompany a pending cleanup",
                ));
            }
        }
        if cleanup_required && !disposition.is_resolved() {
            return Err(invalid(
                "an unresolved historical data mutation observation cannot request cleanup",
            ));
        }
        // A partially applied operation has, by definition, no proof about
        // which artifacts belong only to it.
        if cleanup_required
            && matches!(
                disposition,
                ConnectorHistoricalDataMutationDisposition::PartiallyApplied
                    | ConnectorHistoricalDataMutationDisposition::Conflict
            )
        {
            return Err(invalid(
                "a partially applied or conflicting historical data mutation cannot request cleanup",
            ));
        }
        if disposition == ConnectorHistoricalDataMutationDisposition::CleanupRequired
            && !cleanup_required
        {
            return Err(invalid(
                "a cleanup-required historical data mutation observation must request cleanup",
            ));
        }
        if disposition == ConnectorHistoricalDataMutationDisposition::PartiallyApplied
            && !descriptor.family.owns_source_scope()
        {
            return Err(invalid(
                "only a source-owning historical data mutation family can be partially applied",
            ));
        }
        let digest = observation_digest(ObservationDigestInput {
            descriptor_digest: descriptor.digest(),
            raised_fence_digest: descriptor.raised_fence.digest(),
            operation_id: descriptor.operation_id,
            family: descriptor.family,
            source_scope: descriptor.source_scope,
            disposition,
            application: application.as_ref(),
            continuation: continuation.as_ref(),
            cleanup_required,
            proof_digest: proof.digest(),
        });
        Ok(Self {
            disposition,
            operation_id: descriptor.operation_id,
            family: descriptor.family,
            descriptor_digest: descriptor.digest(),
            raised_fence_digest: descriptor.raised_fence.digest(),
            source_scope: descriptor.source_scope,
            application,
            continuation,
            cleanup_required,
            proof,
            digest,
        })
    }

    /// Verify that this observation answers exactly the supplied descriptor and
    /// that its own digest still seals its contents.
    pub fn validate_for(
        &self,
        descriptor: &ConnectorHistoricalDataMutationDescriptor,
    ) -> Result<(), ConnectorError> {
        if self.descriptor_digest != descriptor.digest()
            || self.operation_id != descriptor.operation_id
            || self.family != descriptor.family
            || self.source_scope != descriptor.source_scope
            || self.raised_fence_digest != descriptor.raised_fence.digest()
        {
            return Err(invalid(
                "historical data mutation observation answers another descriptor",
            ));
        }
        let expected = Self::try_new(
            descriptor,
            self.disposition,
            ConnectorHistoricalDataMutationOutcomeFacts {
                application: self.application.clone(),
                continuation: self.continuation.clone(),
                cleanup_required: self.cleanup_required,
            },
            self.proof.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical data mutation observation digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Request the current generation to establish a strictly higher external fence
/// over one historical direct mutation.
///
/// TRUNCATE and ADD FILES share this one entry point: both are per-operation
/// direct mutations fenced by the same stable operation identity, so a second
/// entry point could only ever disagree with this one.
#[derive(Clone)]
pub struct ConnectorHistoricalDataMutationFenceRaiseRequest {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub family: ConnectorHistoricalDataMutationFamily,
    pub observed: ConnectorHistoricalDataMutationFence,
    pub raised: ConnectorExternalOperationFence,
    pub context: ConnectorRequestContext,
}

impl ConnectorHistoricalDataMutationFenceRaiseRequest {
    /// Fail closed unless the requested fence strictly supersedes the observed
    /// historical fence of the same authority. A raise that does not outrank
    /// the old authority cannot close it, so it is refused rather than accepted
    /// as a no-op.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.raised.validate()?;
        match &self.observed {
            ConnectorHistoricalDataMutationFence::NotEstablished => Ok(()),
            ConnectorHistoricalDataMutationFence::Established { fence, .. } => {
                if self.raised.supersedes(fence)? {
                    Ok(())
                } else {
                    Err(ConnectorError::external_fence(
                        ConnectorExternalFenceFailure::Stale,
                        "historical data mutation fence raise does not strictly supersede the observed fence",
                    ))
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnectorHistoricalDataMutationCleanupRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub descriptor_digest: [u8; 32],
    pub observation: ConnectorHistoricalDataMutationObservation,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDataMutationCleanupReceipt {
    pub descriptor_digest: [u8; 32],
    pub observation_digest: [u8; 32],
}

/// The narrow provider facet installed separately from the ordinary
/// data-mutation capability.
///
/// An ordinary execution path must never reach for this facet as a fallback,
/// and this facet must never call an ordinary old-owner method
/// (`plan_mutation` / `execute` / `reconcile`).
pub trait ConnectorHistoricalDataMutationRecovery: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    /// Atomically establish a strictly higher external fence so no historical
    /// authority can still execute the mutation. This must happen before
    /// `inspect`, and it must be strictly monotonic: a fence that does not
    /// outrank the marker already published is refused with a typed external
    /// fence failure, never as an unknown outcome and never as `Unsupported`.
    fn raise_external_fence(
        &self,
        request: ConnectorHistoricalDataMutationFenceRaiseRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError>;

    /// Classify one historical direct mutation against durable external truth.
    /// Repeating the same immutable descriptor must be idempotent.
    fn inspect(
        &self,
        descriptor: ConnectorHistoricalDataMutationDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalDataMutationObservation, ConnectorError>;

    /// Remove only the artifacts the supplied observation proves belong to the
    /// historical operation and are not referenced by live table state. A
    /// cleanup the provider cannot tie to an inspection it performed must be
    /// refused, never treated as a silent success.
    fn cleanup(
        &self,
        request: ConnectorHistoricalDataMutationCleanupRequest,
    ) -> Result<
        ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
        ConnectorError,
    >;

    /// Resolve a cleanup whose result was lost, using opaque evidence only.
    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<
        ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
        ConnectorError,
    >;
}

pub fn validate_historical_data_mutation_recovery_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: super::ConnectorInstanceIncarnation,
    recovery: &dyn ConnectorHistoricalDataMutationRecovery,
) -> Result<(), ConnectorError> {
    let key = recovery.binding_key();
    if key.instance_id != descriptor.instance_id || key.incarnation != incarnation {
        return Err(invalid(
            "historical data mutation recovery capability does not match its control binding generation",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

struct DescriptorDigestInput<'a> {
    historical_binding: &'a ConnectorExecutionBindingKey,
    table: &'a ConnectorTableIdentity,
    target_ref: &'a ConnectorWriteTargetRef,
    operation_id: ConnectorMutationOperationId,
    family: ConnectorHistoricalDataMutationFamily,
    request_digest: [u8; 32],
    plan_digest: [u8; 32],
    state_digest: [u8; 32],
    plan_summary: ConnectorDataMutationPlanSummary,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    historical_fence: &'a ConnectorHistoricalDataMutationFence,
    raised_fence: &'a ConnectorExternalOperationFence,
    raised_fence_receipt_digest: [u8; 32],
    checkpoints: &'a [ConnectorHistoricalDataMutationCheckpoint],
    evidence: Option<&'a ExternalMutationEvidence>,
}

fn descriptor_digest(input: DescriptorDigestInput<'_>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_HISTORICAL_DATA_MUTATION_DESCRIPTOR_DOMAIN);
    hasher.update(input.historical_binding.instance_id.as_str().as_bytes());
    hasher.update(input.historical_binding.incarnation.to_bytes());
    hasher.update(input.table.instance_id.as_str().as_bytes());
    hasher.update(input.table.namespace.as_bytes());
    hasher.update(input.table.table.as_bytes());
    hasher.update(input.target_ref.as_str().as_bytes());
    hasher.update(input.operation_id.to_bytes());
    hasher.update(input.family.operation_kind().as_bytes());
    hasher.update(input.request_digest);
    hasher.update(input.plan_digest);
    hasher.update(input.state_digest);
    hasher.update(input.plan_summary.file_count().to_be_bytes());
    hasher.update(input.plan_summary.row_count().to_be_bytes());
    hasher.update(input.plan_summary.total_bytes().to_be_bytes());
    match input.source_scope {
        None => hasher.update([0u8]),
        Some(scope) => {
            hasher.update([1u8]);
            hasher.update(scope.version().to_be_bytes());
            hasher.update(scope.digest());
        }
    }
    input.historical_fence.digest_into(&mut hasher);
    hasher.update(input.raised_fence.digest());
    hasher.update(input.raised_fence_receipt_digest);
    for checkpoint in input.checkpoints {
        hasher.update([checkpoint.phase as u8, checkpoint.state as u8]);
        hasher.update(checkpoint.evidence_digest.unwrap_or_default());
    }
    if let Some(evidence) = input.evidence {
        hasher.update(evidence.schema_version().to_be_bytes());
        hasher.update(evidence.operation_id().to_bytes());
        hasher.update(evidence.operation_kind().as_bytes());
        hasher.update(Sha256::digest(evidence.provider_payload()));
    }
    hasher.finalize().into()
}

struct ObservationDigestInput<'a> {
    descriptor_digest: [u8; 32],
    raised_fence_digest: [u8; 32],
    operation_id: ConnectorMutationOperationId,
    family: ConnectorHistoricalDataMutationFamily,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    disposition: ConnectorHistoricalDataMutationDisposition,
    application: Option<&'a ConnectorHistoricalDataMutationApplication>,
    continuation: Option<&'a ConnectorHistoricalDataMutationContinuation>,
    cleanup_required: bool,
    proof_digest: [u8; 32],
}

fn observation_digest(input: ObservationDigestInput<'_>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_HISTORICAL_DATA_MUTATION_OBSERVATION_DOMAIN);
    hasher.update(input.descriptor_digest);
    hasher.update(input.raised_fence_digest);
    hasher.update(input.operation_id.to_bytes());
    hasher.update([input.family as u8]);
    match input.source_scope {
        None => hasher.update([0u8]),
        Some(scope) => {
            hasher.update([1u8]);
            hasher.update(scope.digest());
        }
    }
    hasher.update([input.disposition as u8]);
    if let Some(application) = input.application {
        hasher.update(b"application\0");
        hasher.update(application.committed_version.digest());
        hasher.update(application.receipt.provider_payload_digest());
        hasher.update(application.receipt.incarnation().to_bytes());
        application_summary_into(&mut hasher, application);
        match &application.finalization {
            ExternalMutationFinalization::Complete => hasher.update([0u8]),
            ExternalMutationFinalization::Failed(failure) => {
                hasher.update([1u8]);
                hasher.update(failure.to_string().as_bytes());
            }
        }
    }
    if let Some(continuation) = input.continuation {
        hasher.update(b"continuation\0");
        hasher.update(continuation.digest());
    }
    hasher.update([input.cleanup_required as u8]);
    hasher.update(input.proof_digest);
    hasher.finalize().into()
}

fn application_summary_into(
    hasher: &mut Sha256,
    application: &ConnectorHistoricalDataMutationApplication,
) {
    let summary = application.receipt.summary();
    hasher.update(summary.file_count().to_be_bytes());
    hasher.update(summary.row_count().to_be_bytes());
    hasher.update(summary.total_bytes().to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::connector::external_write_fence::tests::fence;
    use crate::connector::{
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorProviderId,
    };

    fn binding() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("catalog.ice").expect("instance id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([5; 16]),
        }
    }

    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("catalog.ice").expect("instance id"),
            namespace: Arc::from("db"),
            table: Arc::from("orders"),
        }
    }

    fn op_id(byte: u8) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes([byte; 16])
    }

    fn source_scope() -> ConnectorDataMutationSourceScope {
        ConnectorDataMutationSourceScope::try_new_directory([6; 32]).expect("source scope")
    }

    fn identity(
        operation_id: ConnectorMutationOperationId,
        family: ConnectorHistoricalDataMutationFamily,
    ) -> ConnectorHistoricalDataMutationIdentity {
        ConnectorHistoricalDataMutationIdentity {
            historical_binding: binding(),
            table: table(),
            target_ref: ConnectorWriteTargetRef::main(),
            operation_id,
            family,
            request_digest: [1; 32],
            plan_digest: [2; 32],
            state_digest: [3; 32],
            plan_summary: match family {
                ConnectorHistoricalDataMutationFamily::Truncate => {
                    ConnectorDataMutationPlanSummary::default()
                }
                ConnectorHistoricalDataMutationFamily::RegisterExistingFiles => {
                    ConnectorDataMutationPlanSummary::try_new(4, 40, 400).expect("summary")
                }
            },
            source_scope: family.owns_source_scope().then(source_scope),
        }
    }

    fn checkpoints(
        state: ConnectorHistoricalDataMutationDispatchState,
    ) -> Vec<ConnectorHistoricalDataMutationCheckpoint> {
        vec![
            ConnectorHistoricalDataMutationCheckpoint {
                phase: ConnectorHistoricalDataMutationPhase::Planned,
                state: ConnectorHistoricalDataMutationDispatchState::Completed,
                evidence_digest: None,
            },
            ConnectorHistoricalDataMutationCheckpoint {
                phase: ConnectorHistoricalDataMutationPhase::ExecuteDispatched,
                state,
                evidence_digest: None,
            },
        ]
    }

    fn established(
        operation_id: ConnectorMutationOperationId,
        epoch: u64,
    ) -> ConnectorHistoricalDataMutationFence {
        let historical = fence(
            ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
            1,
            epoch,
            1,
        );
        let receipt =
            ConnectorExternalFenceReceipt::try_new(&historical, Bytes::from_static(b"marker"))
                .expect("receipt");
        ConnectorHistoricalDataMutationFence::established(&receipt, historical)
            .expect("established fence")
    }

    fn descriptor(
        operation_id: ConnectorMutationOperationId,
        family: ConnectorHistoricalDataMutationFamily,
        historical: ConnectorHistoricalDataMutationFence,
        state: ConnectorHistoricalDataMutationDispatchState,
    ) -> ConnectorHistoricalDataMutationDescriptor {
        ConnectorHistoricalDataMutationDescriptor::try_new(
            identity(operation_id, family),
            ConnectorHistoricalDataMutationFenceFacts {
                historical_fence: historical,
                raised_fence: fence(
                    ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
                    1,
                    3,
                    1,
                ),
                raised_fence_receipt_digest: [9; 32],
            },
            checkpoints(state),
            None,
        )
        .expect("historical data mutation descriptor")
    }

    fn proof() -> ConnectorHistoricalDataMutationProof {
        ConnectorHistoricalDataMutationProof::try_new(Bytes::from_static(b"provider-proof"))
            .expect("proof")
    }

    fn application(
        descriptor: &ConnectorHistoricalDataMutationDescriptor,
    ) -> ConnectorHistoricalDataMutationApplication {
        ConnectorHistoricalDataMutationApplication {
            committed_version: ConnectorCommittedVersion::try_new(
                Bytes::from_static(b"version"),
                Some(9),
            )
            .expect("committed version"),
            receipt: ConnectorDataMutationReceipt::try_new(
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
                    instance_id: descriptor.table.instance_id.clone(),
                },
                ConnectorInstanceIncarnation::from_bytes([7; 16]),
                descriptor.operation_id,
                descriptor.family.operation_kind(),
                descriptor.request_digest,
                descriptor.plan_digest,
                descriptor.state_digest,
                descriptor.plan_summary,
                Bytes::from_static(b"{\"version\":1,\"snapshot_id\":9}"),
            )
            .expect("receipt"),
            finalization: ExternalMutationFinalization::Complete,
        }
    }

    struct NeverCancelled;

    impl crate::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("request context")
    }

    #[test]
    fn proof_and_continuation_reject_unbounded_payloads_and_redact_debug() {
        assert!(ConnectorHistoricalDataMutationProof::try_new(Bytes::new()).is_err());
        assert!(
            ConnectorHistoricalDataMutationProof::try_new(Bytes::from(vec![
                0;
                MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_PROOF_BYTES
                    + 1
            ]))
            .is_err()
        );
        let debug = format!("{:?}", proof());
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("provider-proof"));

        let raised = fence(ConnectorWriteOperationId::from_bytes([1; 16]), 1, 3, 1);
        assert!(
            ConnectorHistoricalDataMutationContinuation::try_new(&raised, Bytes::new()).is_err()
        );
        assert!(
            ConnectorHistoricalDataMutationContinuation::try_new(
                &raised,
                Bytes::from(vec![
                    0;
                    MAX_CONNECTOR_HISTORICAL_DATA_MUTATION_CONTINUATION_BYTES
                        + 1
                ]),
            )
            .is_err()
        );
        let continuation = ConnectorHistoricalDataMutationContinuation::try_new(
            &raised,
            Bytes::from_static(b"signed-continuation"),
        )
        .expect("continuation");
        let debug = format!("{continuation:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("signed-continuation"));
    }

    #[test]
    fn add_files_must_bind_a_source_scope_and_truncate_must_not() {
        let operation_id = op_id(1);
        let mut add_files = identity(
            operation_id,
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
        );
        add_files.source_scope = None;
        assert!(
            ConnectorHistoricalDataMutationDescriptor::try_new(
                add_files,
                ConnectorHistoricalDataMutationFenceFacts {
                    historical_fence: ConnectorHistoricalDataMutationFence::NotEstablished,
                    raised_fence: fence(
                        ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
                        1,
                        3,
                        1,
                    ),
                    raised_fence_receipt_digest: [9; 32],
                },
                checkpoints(ConnectorHistoricalDataMutationDispatchState::NotDispatched),
                None,
            )
            .is_err(),
            "ADD FILES recovery without its immutable source scope must be refused"
        );

        let mut truncate = identity(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
        );
        truncate.source_scope = Some(source_scope());
        assert!(
            ConnectorHistoricalDataMutationDescriptor::try_new(
                truncate,
                ConnectorHistoricalDataMutationFenceFacts {
                    historical_fence: ConnectorHistoricalDataMutationFence::NotEstablished,
                    raised_fence: fence(
                        ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
                        1,
                        3,
                        1,
                    ),
                    raised_fence_receipt_digest: [9; 32],
                },
                checkpoints(ConnectorHistoricalDataMutationDispatchState::NotDispatched),
                None,
            )
            .is_err(),
            "TRUNCATE owns no external source location"
        );
    }

    #[test]
    fn descriptor_requires_a_strictly_higher_raised_fence() {
        let operation_id = op_id(1);
        let error = ConnectorHistoricalDataMutationDescriptor::try_new(
            identity(
                operation_id,
                ConnectorHistoricalDataMutationFamily::Truncate,
            ),
            ConnectorHistoricalDataMutationFenceFacts {
                historical_fence: established(operation_id, 3),
                raised_fence: fence(
                    ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
                    1,
                    3,
                    1,
                ),
                raised_fence_receipt_digest: [9; 32],
            },
            checkpoints(ConnectorHistoricalDataMutationDispatchState::Unknown),
            None,
        )
        .expect_err("an equal raised fence cannot fence out the historical authority");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
        descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        )
        .validate()
        .expect("a strictly higher raised fence is accepted");
    }

    #[test]
    fn descriptor_digest_detects_mutation_of_every_sealed_field() {
        let operation_id = op_id(1);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::NotDispatched,
        );
        sealed.validate().expect("sealed descriptor validates");

        let mut retargeted = sealed.clone();
        retargeted.state_digest = [11; 32];
        assert!(retargeted.validate().is_err());

        let mut rescoped = sealed.clone();
        rescoped.source_scope =
            Some(ConnectorDataMutationSourceScope::try_new_directory([12; 32]).expect("scope"));
        assert!(rescoped.validate().is_err());

        let mut resummarized = sealed.clone();
        resummarized.plan_summary =
            ConnectorDataMutationPlanSummary::try_new(5, 40, 400).expect("summary");
        assert!(resummarized.validate().is_err());

        let mut relabelled = sealed;
        relabelled.checkpoints[1].state = ConnectorHistoricalDataMutationDispatchState::Dispatched;
        assert!(relabelled.validate().is_err());
    }

    #[test]
    fn applied_observation_requires_finalization_bound_to_its_operation() {
        let operation_id = op_id(1);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Completed,
        );
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &sealed,
                ConnectorHistoricalDataMutationDisposition::Applied,
                ConnectorHistoricalDataMutationOutcomeFacts::default(),
                proof(),
            )
            .is_err()
        );
        let observation = ConnectorHistoricalDataMutationObservation::try_new(
            &sealed,
            ConnectorHistoricalDataMutationDisposition::Applied,
            ConnectorHistoricalDataMutationOutcomeFacts {
                application: Some(application(&sealed)),
                continuation: None,
                cleanup_required: true,
            },
            proof(),
        )
        .expect("applied observation");
        observation
            .validate_for(&sealed)
            .expect("applied observation validates");

        let mut corrupted = observation;
        corrupted.cleanup_required = false;
        assert!(corrupted.validate_for(&sealed).is_err());

        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &sealed,
                ConnectorHistoricalDataMutationDisposition::NotApplied,
                ConnectorHistoricalDataMutationOutcomeFacts {
                    application: Some(application(&sealed)),
                    continuation: None,
                    cleanup_required: false,
                },
                proof(),
            )
            .is_err(),
            "only an applied observation may carry finalization facts"
        );

        // A receipt for another operation cannot be smuggled into an applied
        // observation.
        let other = descriptor(
            op_id(2),
            ConnectorHistoricalDataMutationFamily::Truncate,
            established(op_id(2), 2),
            ConnectorHistoricalDataMutationDispatchState::Completed,
        );
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &sealed,
                ConnectorHistoricalDataMutationDisposition::Applied,
                ConnectorHistoricalDataMutationOutcomeFacts {
                    application: Some(application(&other)),
                    continuation: None,
                    cleanup_required: false,
                },
                proof(),
            )
            .is_err()
        );
    }

    #[test]
    fn continuation_is_only_legal_for_a_proven_not_applied_operation() {
        let operation_id = op_id(1);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::NotDispatched,
        );
        let continuation = ConnectorHistoricalDataMutationContinuation::try_new(
            &sealed.raised_fence,
            Bytes::from_static(b"signed"),
        )
        .expect("continuation");
        ConnectorHistoricalDataMutationObservation::try_new(
            &sealed,
            ConnectorHistoricalDataMutationDisposition::NotApplied,
            ConnectorHistoricalDataMutationOutcomeFacts {
                application: None,
                continuation: Some(continuation.clone()),
                cleanup_required: false,
            },
            proof(),
        )
        .expect("not-applied continuation");

        for disposition in [
            ConnectorHistoricalDataMutationDisposition::Applied,
            ConnectorHistoricalDataMutationDisposition::CleanupRequired,
            ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
            ConnectorHistoricalDataMutationDisposition::Conflict,
            ConnectorHistoricalDataMutationDisposition::Ambiguous,
            ConnectorHistoricalDataMutationDisposition::Unsupported,
        ] {
            assert!(
                ConnectorHistoricalDataMutationObservation::try_new(
                    &sealed,
                    disposition,
                    ConnectorHistoricalDataMutationOutcomeFacts {
                        application: None,
                        continuation: Some(continuation.clone()),
                        cleanup_required: false,
                    },
                    proof(),
                )
                .is_err(),
                "{disposition:?} must not carry a continuation"
            );
        }

        // A pending cleanup removes the fence artifact a continuation's
        // authority rests on, so the two are mutually exclusive.
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &sealed,
                ConnectorHistoricalDataMutationDisposition::NotApplied,
                ConnectorHistoricalDataMutationOutcomeFacts {
                    application: None,
                    continuation: Some(continuation.clone()),
                    cleanup_required: true,
                },
                proof(),
            )
            .is_err()
        );

        let dispatched = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::Dispatched,
        );
        let dispatched_continuation = ConnectorHistoricalDataMutationContinuation::try_new(
            &dispatched.raised_fence,
            Bytes::from_static(b"signed"),
        )
        .expect("continuation");
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &dispatched,
                ConnectorHistoricalDataMutationDisposition::NotApplied,
                ConnectorHistoricalDataMutationOutcomeFacts {
                    application: None,
                    continuation: Some(dispatched_continuation),
                    cleanup_required: false,
                },
                proof(),
            )
            .is_err(),
            "a dispatched journal checkpoint forbids replaying a destructive mutation"
        );

        let foreign = descriptor(
            op_id(2),
            ConnectorHistoricalDataMutationFamily::Truncate,
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::NotDispatched,
        );
        let error = ConnectorHistoricalDataMutationObservation::try_new(
            &foreign,
            ConnectorHistoricalDataMutationDisposition::NotApplied,
            ConnectorHistoricalDataMutationOutcomeFacts {
                application: None,
                continuation: Some(continuation),
                cleanup_required: false,
            },
            proof(),
        )
        .expect_err("a continuation must bind the raised fence");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::ForeignOperation)
        );
    }

    #[test]
    fn cleanup_is_refused_for_unresolved_partial_and_conflicting_observations() {
        let operation_id = op_id(1);
        for family in [
            ConnectorHistoricalDataMutationFamily::Truncate,
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
        ] {
            let sealed = descriptor(
                operation_id,
                family,
                established(operation_id, 2),
                ConnectorHistoricalDataMutationDispatchState::Unknown,
            );
            for disposition in [
                ConnectorHistoricalDataMutationDisposition::Ambiguous,
                ConnectorHistoricalDataMutationDisposition::Unsupported,
                ConnectorHistoricalDataMutationDisposition::Conflict,
            ] {
                assert!(
                    ConnectorHistoricalDataMutationObservation::try_new(
                        &sealed,
                        disposition,
                        ConnectorHistoricalDataMutationOutcomeFacts {
                            application: None,
                            continuation: None,
                            cleanup_required: true,
                        },
                        proof(),
                    )
                    .is_err(),
                    "{disposition:?} must not authorize a removal"
                );
                ConnectorHistoricalDataMutationObservation::try_new(
                    &sealed,
                    disposition,
                    ConnectorHistoricalDataMutationOutcomeFacts::default(),
                    proof(),
                )
                .expect("an unresolved observation keeps the recovery record");
            }
        }

        let add_files = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        );
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &add_files,
                ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
                ConnectorHistoricalDataMutationOutcomeFacts {
                    application: None,
                    continuation: None,
                    cleanup_required: true,
                },
                proof(),
            )
            .is_err(),
            "a partial source set proves nothing about artifact ownership"
        );
    }

    #[test]
    fn only_a_source_owning_family_can_be_partially_applied() {
        let operation_id = op_id(1);
        let truncate = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        );
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &truncate,
                ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
                ConnectorHistoricalDataMutationOutcomeFacts::default(),
                proof(),
            )
            .is_err()
        );
        let add_files = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        );
        ConnectorHistoricalDataMutationObservation::try_new(
            &add_files,
            ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
            ConnectorHistoricalDataMutationOutcomeFacts::default(),
            proof(),
        )
        .expect("ADD FILES can be partially applied");
    }

    #[test]
    fn cleanup_required_disposition_must_request_cleanup() {
        let operation_id = op_id(1);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        );
        assert!(
            ConnectorHistoricalDataMutationObservation::try_new(
                &sealed,
                ConnectorHistoricalDataMutationDisposition::CleanupRequired,
                ConnectorHistoricalDataMutationOutcomeFacts::default(),
                proof(),
            )
            .is_err()
        );
        ConnectorHistoricalDataMutationObservation::try_new(
            &sealed,
            ConnectorHistoricalDataMutationDisposition::CleanupRequired,
            ConnectorHistoricalDataMutationOutcomeFacts {
                application: None,
                continuation: None,
                cleanup_required: true,
            },
            proof(),
        )
        .expect("cleanup-required observation");
    }

    #[test]
    fn observation_rejects_a_foreign_descriptor() {
        let operation_id = op_id(1);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalDataMutationFamily::Truncate,
            established(operation_id, 2),
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        );
        let observation = ConnectorHistoricalDataMutationObservation::try_new(
            &sealed,
            ConnectorHistoricalDataMutationDisposition::Ambiguous,
            ConnectorHistoricalDataMutationOutcomeFacts::default(),
            proof(),
        )
        .expect("observation");
        let other = descriptor(
            op_id(2),
            ConnectorHistoricalDataMutationFamily::Truncate,
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::Unknown,
        );
        assert!(observation.validate_for(&other).is_err());
    }

    #[test]
    fn only_applied_and_not_applied_permit_a_source_scope_release() {
        for disposition in [
            ConnectorHistoricalDataMutationDisposition::Applied,
            ConnectorHistoricalDataMutationDisposition::NotApplied,
        ] {
            assert!(
                disposition.permits_source_scope_release(),
                "{disposition:?}"
            );
        }
        for disposition in [
            ConnectorHistoricalDataMutationDisposition::CleanupRequired,
            ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
            ConnectorHistoricalDataMutationDisposition::Conflict,
            ConnectorHistoricalDataMutationDisposition::Ambiguous,
            ConnectorHistoricalDataMutationDisposition::Unsupported,
        ] {
            assert!(
                !disposition.permits_source_scope_release(),
                "{disposition:?} must retain the ADD FILES source scope"
            );
        }
    }

    #[test]
    fn fence_raise_request_must_strictly_supersede_the_observed_fence() {
        let operation_id = op_id(1);
        let observed = established(operation_id, 3);
        let stale = ConnectorHistoricalDataMutationFenceRaiseRequest {
            historical_binding: binding(),
            family: ConnectorHistoricalDataMutationFamily::Truncate,
            observed: observed.clone(),
            raised: fence(
                ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
                1,
                2,
                1,
            ),
            context: context(),
        };
        let error = stale.validate().expect_err("a lower raise is stale");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
        assert_ne!(error.kind(), ConnectorErrorKind::Unsupported);
        let raised = ConnectorHistoricalDataMutationFenceRaiseRequest {
            historical_binding: binding(),
            family: ConnectorHistoricalDataMutationFamily::Truncate,
            observed,
            raised: fence(
                ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
                1,
                4,
                1,
            ),
            context: context(),
        };
        raised.validate().expect("a higher raise is accepted");
    }

    #[test]
    fn owner_validation_rejects_a_foreign_generation() {
        struct Recovery {
            key: ConnectorExecutionBindingKey,
        }

        impl ConnectorHistoricalDataMutationRecovery for Recovery {
            fn binding_key(&self) -> &ConnectorExecutionBindingKey {
                &self.key
            }

            fn raise_external_fence(
                &self,
                _request: ConnectorHistoricalDataMutationFenceRaiseRequest,
            ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
                Err(invalid("test facet does not raise fences"))
            }

            fn inspect(
                &self,
                _descriptor: ConnectorHistoricalDataMutationDescriptor,
                _context: ConnectorRequestContext,
            ) -> Result<ConnectorHistoricalDataMutationObservation, ConnectorError> {
                Err(invalid("test facet does not inspect"))
            }

            fn cleanup(
                &self,
                _request: ConnectorHistoricalDataMutationCleanupRequest,
            ) -> Result<
                ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
                ConnectorError,
            > {
                Err(invalid("test facet does not clean up"))
            }

            fn reconcile_cleanup(
                &self,
                _operation_id: ConnectorMutationOperationId,
                _evidence: ExternalMutationEvidence,
                _context: ConnectorRequestContext,
            ) -> Result<
                ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
                ConnectorError,
            > {
                Err(invalid("test facet does not reconcile cleanup"))
            }
        }

        let instance = ConnectorInstanceId::parse("catalog.ice").expect("instance id");
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            instance_id: instance.clone(),
        };
        let incarnation = ConnectorInstanceIncarnation::from_bytes([5; 16]);
        let recovery = Recovery {
            key: ConnectorExecutionBindingKey {
                instance_id: instance,
                incarnation,
            },
        };
        validate_historical_data_mutation_recovery_owner(&descriptor, incarnation, &recovery)
            .expect("matching generation");
        let foreign = ConnectorInstanceIncarnation::from_bytes([6; 16]);
        assert!(
            validate_historical_data_mutation_recovery_owner(&descriptor, foreign, &recovery)
                .is_err()
        );
    }
}
