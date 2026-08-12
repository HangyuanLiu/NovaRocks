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

//! Provider-neutral, cross-incarnation inspection for a distributed write.
//!
//! This is deliberately not a second write contract. The current connector
//! generation may raise the external fence over a historical write operation,
//! classify that operation against durable external truth, and then perform a
//! proof-bound guarded cleanup. It must never revive the historical binding,
//! construct a historical runtime session, call an ordinary old-owner method,
//! or replay an operation that was already dispatched.
//!
//! Every descriptor and observation is digest sealed and every provider payload
//! is a bounded opaque container. The frontend persists identity, generation
//! scalars, digests and opaque bytes only; it never decodes a payload.

use std::fmt;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExternalFenceFailure, ConnectorExternalFenceReceipt, ConnectorExternalOperationFence,
    ConnectorInstanceDescriptor, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteIntent, ConnectorWriteOperationId, ConnectorWriteReceipt,
    ConnectorWriteTargetRef, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome,
};

pub const MAX_CONNECTOR_HISTORICAL_WRITE_PROOF_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_HISTORICAL_WRITE_CONTINUATION_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_HISTORICAL_WRITE_CHECKPOINTS: usize = 4096;

const CONNECTOR_HISTORICAL_WRITE_DESCRIPTOR_DOMAIN: &[u8] =
    b"novarocks.historical-write-descriptor.v1\0";
const CONNECTOR_HISTORICAL_WRITE_OBSERVATION_DOMAIN: &[u8] =
    b"novarocks.historical-write-observation.v1\0";
const CONNECTOR_HISTORICAL_WRITE_CONTINUATION_DOMAIN: &[u8] =
    b"novarocks.historical-write-continuation.v1\0";

/// The frontend-observed phase a historical write operation had reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalWritePhase {
    Prepared,
    Activated,
    FenceEstablished,
    WritersDispatched,
    WritersCompleted,
    CommitDispatched,
}

/// Whether the historical owner is known to have dispatched a phase. This is a
/// frontend observation of its own journal, never a provider conclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalWriteDispatchState {
    NotDispatched,
    Dispatched,
    Completed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteCheckpoint {
    pub phase: ConnectorHistoricalWritePhase,
    pub state: ConnectorHistoricalWriteDispatchState,
    pub evidence_digest: Option<[u8; 32]>,
}

/// The typed external fence fact of the historical attempt.
///
/// `NotEstablished` is a provable historical state, not a missing value: an
/// owner may crash before it establishes any fence (spec CP-3B failure row 1).
// This is a sealed value fact, not a hot enum: keeping the fence inline
// matches the other sealed SPI request/preparation enums.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalWriteFence {
    NotEstablished,
    Established {
        fence: ConnectorExternalOperationFence,
        receipt_digest: [u8; 32],
    },
}

impl ConnectorHistoricalWriteFence {
    pub fn established(
        receipt: &ConnectorExternalFenceReceipt,
        fence: ConnectorExternalOperationFence,
    ) -> Result<Self, ConnectorError> {
        fence.validate()?;
        receipt.validate()?;
        if !receipt.matches(&fence) {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "historical write fence receipt does not acknowledge its fence",
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

/// Complete value-only description of one historical write operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteDescriptor {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub table: ConnectorTableIdentity,
    pub target_ref: ConnectorWriteTargetRef,
    pub operation_id: ConnectorWriteOperationId,
    pub intent: ConnectorWriteIntent,
    pub cohort_set_digest: [u8; 32],
    pub aggregate_digest: Option<[u8; 32]>,
    pub historical_fence: ConnectorHistoricalWriteFence,
    /// The strictly higher fence the current owner established before this
    /// inspection. Without it a historical operation can never be classified
    /// as safe to retry.
    pub raised_fence: ConnectorExternalOperationFence,
    pub raised_fence_receipt_digest: [u8; 32],
    pub checkpoints: Vec<ConnectorHistoricalWriteCheckpoint>,
    pub evidence: Option<ExternalMutationEvidence>,
    digest: [u8; 32],
}

impl ConnectorHistoricalWriteDescriptor {
    pub fn try_new(
        identity: ConnectorHistoricalWriteIdentity,
        fences: ConnectorHistoricalWriteFenceFacts,
        checkpoints: Vec<ConnectorHistoricalWriteCheckpoint>,
        evidence: Option<ExternalMutationEvidence>,
    ) -> Result<Self, ConnectorError> {
        let ConnectorHistoricalWriteIdentity {
            historical_binding,
            table,
            target_ref,
            operation_id,
            intent,
            cohort_set_digest,
            aggregate_digest,
        } = identity;
        let ConnectorHistoricalWriteFenceFacts {
            historical_fence,
            raised_fence,
            raised_fence_receipt_digest,
        } = fences;
        if checkpoints.is_empty() || checkpoints.len() > MAX_CONNECTOR_HISTORICAL_WRITE_CHECKPOINTS
        {
            return Err(invalid(
                "historical write descriptor must carry 1..=4096 dispatch checkpoints",
            ));
        }
        if cohort_set_digest == [0; 32] {
            return Err(invalid(
                "historical write descriptor must carry a sealed cohort set digest",
            ));
        }
        if raised_fence_receipt_digest == [0; 32] {
            return Err(invalid(
                "historical write descriptor must carry the raised fence receipt digest",
            ));
        }
        raised_fence.validate_for_operation(operation_id)?;
        if raised_fence.table() != &table || raised_fence.target_ref() != &target_ref {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::ForeignOperation,
                "historical write raised fence names another resource",
            ));
        }
        // The takeover order is fixed: the current owner must have raised a
        // strictly higher fence before it may inspect a historical operation.
        if let Some(historical) = historical_fence.fence()
            && !raised_fence.supersedes(historical)?
        {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "historical write inspection requires a strictly higher raised external fence",
            ));
        }
        if let Some(evidence) = &evidence
            && evidence.provider_payload().is_empty()
        {
            return Err(invalid(
                "historical write descriptor evidence must carry a provider payload",
            ));
        }
        let digest = descriptor_digest(
            &historical_binding,
            &table,
            &target_ref,
            operation_id,
            intent,
            cohort_set_digest,
            aggregate_digest,
            &historical_fence,
            &raised_fence,
            raised_fence_receipt_digest,
            &checkpoints,
            evidence.as_ref(),
        );
        Ok(Self {
            historical_binding,
            table,
            target_ref,
            operation_id,
            intent,
            cohort_set_digest,
            aggregate_digest,
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
            ConnectorHistoricalWriteIdentity {
                historical_binding: self.historical_binding.clone(),
                table: self.table.clone(),
                target_ref: self.target_ref.clone(),
                operation_id: self.operation_id,
                intent: self.intent,
                cohort_set_digest: self.cohort_set_digest,
                aggregate_digest: self.aggregate_digest,
            },
            ConnectorHistoricalWriteFenceFacts {
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
                "historical write descriptor digest does not match its contents",
            ));
        }
        Ok(())
    }

    /// Whether the frontend journal proves no writer or commit was dispatched.
    /// A provider may only issue a continuation for such an operation.
    pub fn journal_proves_nothing_dispatched(&self) -> bool {
        self.checkpoints.iter().all(|checkpoint| {
            !matches!(
                checkpoint.phase,
                ConnectorHistoricalWritePhase::WritersDispatched
                    | ConnectorHistoricalWritePhase::WritersCompleted
                    | ConnectorHistoricalWritePhase::CommitDispatched
            ) || checkpoint.state == ConnectorHistoricalWriteDispatchState::NotDispatched
        })
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// The immutable identity half of a historical write descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteIdentity {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub table: ConnectorTableIdentity,
    pub target_ref: ConnectorWriteTargetRef,
    pub operation_id: ConnectorWriteOperationId,
    pub intent: ConnectorWriteIntent,
    pub cohort_set_digest: [u8; 32],
    pub aggregate_digest: Option<[u8; 32]>,
}

/// The fence half of a historical write descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteFenceFacts {
    pub historical_fence: ConnectorHistoricalWriteFence,
    pub raised_fence: ConnectorExternalOperationFence,
    pub raised_fence_receipt_digest: [u8; 32],
}

/// Bounded opaque provider proof for one historical classification.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteProof {
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalWriteProof {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        if payload.is_empty() || payload.len() > MAX_CONNECTOR_HISTORICAL_WRITE_PROOF_BYTES {
            return Err(invalid(
                "historical write proof exceeds its bounded payload limit",
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
                "historical write proof digest does not match its payload",
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

impl fmt::Debug for ConnectorHistoricalWriteProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalWriteProof")
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// A provider-signed continuation authorizing the current generation to run the
/// same stable DML operation again. It is only ever issued for a `NotDispatched`
/// disposition after the historical authority has been fenced out.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteContinuation {
    raised_fence_digest: [u8; 32],
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalWriteContinuation {
    pub fn try_new(
        raised_fence: &ConnectorExternalOperationFence,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        raised_fence.validate()?;
        if payload.is_empty() || payload.len() > MAX_CONNECTOR_HISTORICAL_WRITE_CONTINUATION_BYTES {
            return Err(invalid(
                "historical write continuation exceeds its bounded payload limit",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_HISTORICAL_WRITE_CONTINUATION_DOMAIN);
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
            || self.payload.len() > MAX_CONNECTOR_HISTORICAL_WRITE_CONTINUATION_BYTES
        {
            return Err(invalid(
                "historical write continuation exceeds its bounded payload limit",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_HISTORICAL_WRITE_CONTINUATION_DOMAIN);
        hasher.update(self.raised_fence_digest);
        hasher.update(self.payload.as_ref());
        let expected: [u8; 32] = hasher.finalize().into();
        if expected != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical write continuation digest does not match its contents",
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

impl fmt::Debug for ConnectorHistoricalWriteContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalWriteContinuation")
            .field("raised_fence_digest", &self.raised_fence_digest)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// The typed classification of one historical write operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalWriteDisposition {
    /// The provider proved external truth already contains this operation.
    Applied,
    /// The provider proved this operation never committed and no historical
    /// authority can still commit it.
    NotApplied,
    /// The provider proved no writer or commit was ever dispatched.
    NotDispatched,
    /// Writer output exists but was never committed. Only a proof-bound abort
    /// or cleanup is allowed; staged output is never adopted across generations.
    Staged,
    /// The external base or fence has been advanced by another operation.
    Conflict,
    /// Evidence is insufficient. The recovery index must be retained.
    Ambiguous,
    /// This provider cannot classify a historical write operation.
    Unsupported,
}

impl ConnectorHistoricalWriteDisposition {
    /// Whether the current generation may issue a continuation. Only a proven
    /// `NotDispatched` operation qualifies.
    pub const fn may_continue(self) -> bool {
        matches!(self, Self::NotDispatched)
    }

    /// Whether this disposition is a resolved classification. `Ambiguous` and
    /// `Unsupported` are not: they keep the recovery record unresolved.
    pub const fn is_resolved(self) -> bool {
        !matches!(self, Self::Ambiguous | Self::Unsupported)
    }
}

/// The finalization facts a provider must supply for an applied operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteApplication {
    pub committed_version: ConnectorCommittedVersion,
    pub receipt: ConnectorWriteReceipt,
    pub finalization: ExternalMutationFinalization,
}

/// A digest-sealed classification of one historical write operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteObservation {
    pub disposition: ConnectorHistoricalWriteDisposition,
    pub operation_id: ConnectorWriteOperationId,
    pub descriptor_digest: [u8; 32],
    pub raised_fence_digest: [u8; 32],
    pub application: Option<ConnectorHistoricalWriteApplication>,
    pub continuation: Option<ConnectorHistoricalWriteContinuation>,
    pub cleanup_required: bool,
    pub proof: ConnectorHistoricalWriteProof,
    digest: [u8; 32],
}

impl ConnectorHistoricalWriteObservation {
    pub fn try_new(
        descriptor: &ConnectorHistoricalWriteDescriptor,
        disposition: ConnectorHistoricalWriteDisposition,
        outcome: ConnectorHistoricalWriteOutcomeFacts,
        proof: ConnectorHistoricalWriteProof,
    ) -> Result<Self, ConnectorError> {
        descriptor.validate()?;
        proof.validate()?;
        let ConnectorHistoricalWriteOutcomeFacts {
            application,
            continuation,
            cleanup_required,
        } = outcome;
        if let Some(application) = &application {
            application.committed_version.validate()?;
            application.receipt.validate()?;
        }
        match disposition {
            ConnectorHistoricalWriteDisposition::Applied if application.is_none() => {
                return Err(invalid(
                    "an applied historical write observation must carry its neutral receipt and finalization",
                ));
            }
            ConnectorHistoricalWriteDisposition::Applied => {}
            _ if application.is_some() => {
                return Err(invalid(
                    "only an applied historical write observation may carry finalization facts",
                ));
            }
            _ => {}
        }
        if continuation.is_some() && !disposition.may_continue() {
            return Err(invalid(
                "only a proven not-dispatched historical write observation may carry a continuation",
            ));
        }
        if let Some(continuation) = &continuation {
            continuation.validate()?;
            if !continuation.is_bound_to(&descriptor.raised_fence) {
                return Err(ConnectorError::external_fence(
                    ConnectorExternalFenceFailure::ForeignOperation,
                    "historical write continuation is not bound to the raised external fence",
                ));
            }
            if !descriptor.journal_proves_nothing_dispatched() {
                return Err(invalid(
                    "historical write continuation contradicts a dispatched journal checkpoint",
                ));
            }
        }
        if cleanup_required && !disposition.is_resolved() {
            return Err(invalid(
                "an unresolved historical write observation cannot request cleanup",
            ));
        }
        let digest = observation_digest(
            descriptor.digest(),
            descriptor.raised_fence.digest(),
            descriptor.operation_id,
            disposition,
            application.as_ref(),
            continuation.as_ref(),
            cleanup_required,
            proof.digest(),
        );
        Ok(Self {
            disposition,
            operation_id: descriptor.operation_id,
            descriptor_digest: descriptor.digest(),
            raised_fence_digest: descriptor.raised_fence.digest(),
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
        descriptor: &ConnectorHistoricalWriteDescriptor,
    ) -> Result<(), ConnectorError> {
        if self.descriptor_digest != descriptor.digest()
            || self.operation_id != descriptor.operation_id
            || self.raised_fence_digest != descriptor.raised_fence.digest()
        {
            return Err(invalid(
                "historical write observation answers another descriptor",
            ));
        }
        let expected = Self::try_new(
            descriptor,
            self.disposition,
            ConnectorHistoricalWriteOutcomeFacts {
                application: self.application.clone(),
                continuation: self.continuation.clone(),
                cleanup_required: self.cleanup_required,
            },
            self.proof.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical write observation digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// The result half of a historical write observation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteOutcomeFacts {
    pub application: Option<ConnectorHistoricalWriteApplication>,
    pub continuation: Option<ConnectorHistoricalWriteContinuation>,
    pub cleanup_required: bool,
}

/// Request the current generation to establish a strictly higher external fence
/// over one historical write operation.
#[derive(Clone)]
pub struct ConnectorHistoricalWriteFenceRaiseRequest {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub observed: ConnectorHistoricalWriteFence,
    pub raised: ConnectorExternalOperationFence,
    pub context: ConnectorRequestContext,
}

impl ConnectorHistoricalWriteFenceRaiseRequest {
    /// Fail closed unless the requested fence strictly supersedes the observed
    /// historical fence of the same authority.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.raised.validate()?;
        match &self.observed {
            ConnectorHistoricalWriteFence::NotEstablished => Ok(()),
            ConnectorHistoricalWriteFence::Established { fence, .. } => {
                if self.raised.supersedes(fence)? {
                    Ok(())
                } else {
                    Err(ConnectorError::external_fence(
                        ConnectorExternalFenceFailure::Stale,
                        "historical write fence raise does not strictly supersede the observed fence",
                    ))
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnectorHistoricalWriteCleanupRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub descriptor_digest: [u8; 32],
    pub observation: ConnectorHistoricalWriteObservation,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalWriteCleanupReceipt {
    pub descriptor_digest: [u8; 32],
    pub observation_digest: [u8; 32],
}

/// The narrow provider facet installed separately from ordinary write control.
///
/// An ordinary execution path must never reach for this facet as a fallback,
/// and this facet must never call an ordinary old-owner write method.
pub trait ConnectorHistoricalWriteRecovery: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    /// Atomically establish a strictly higher external fence so no historical
    /// authority can still commit. This must happen before `inspect`.
    fn raise_external_fence(
        &self,
        request: ConnectorHistoricalWriteFenceRaiseRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError>;

    /// Classify one historical write operation against durable external truth.
    /// Repeating the same immutable descriptor must be idempotent.
    fn inspect(
        &self,
        descriptor: ConnectorHistoricalWriteDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalWriteObservation, ConnectorError>;

    /// Remove only the artifacts proven by the supplied observation.
    fn cleanup(
        &self,
        request: ConnectorHistoricalWriteCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>;

    /// Resolve a cleanup whose result was lost, using opaque evidence only.
    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorWriteOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>;
}

pub fn validate_historical_write_recovery_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: super::ConnectorInstanceIncarnation,
    recovery: &dyn ConnectorHistoricalWriteRecovery,
) -> Result<(), ConnectorError> {
    let key = recovery.binding_key();
    if key.instance_id != descriptor.instance_id || key.incarnation != incarnation {
        return Err(invalid(
            "historical write recovery capability does not match its control binding generation",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

// Every argument is a sealed field of this digest; grouping them into a struct
// would add a type that exists only to be destructured here.
#[allow(clippy::too_many_arguments)]
fn descriptor_digest(
    historical_binding: &ConnectorExecutionBindingKey,
    table: &ConnectorTableIdentity,
    target_ref: &ConnectorWriteTargetRef,
    operation_id: ConnectorWriteOperationId,
    intent: ConnectorWriteIntent,
    cohort_set_digest: [u8; 32],
    aggregate_digest: Option<[u8; 32]>,
    historical_fence: &ConnectorHistoricalWriteFence,
    raised_fence: &ConnectorExternalOperationFence,
    raised_fence_receipt_digest: [u8; 32],
    checkpoints: &[ConnectorHistoricalWriteCheckpoint],
    evidence: Option<&ExternalMutationEvidence>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_HISTORICAL_WRITE_DESCRIPTOR_DOMAIN);
    hasher.update(historical_binding.instance_id.as_str().as_bytes());
    hasher.update(historical_binding.incarnation.to_bytes());
    hasher.update(table.instance_id.as_str().as_bytes());
    hasher.update(table.namespace.as_bytes());
    hasher.update(table.table.as_bytes());
    hasher.update(target_ref.as_str().as_bytes());
    hasher.update(operation_id.to_bytes());
    hasher.update([intent as u8]);
    hasher.update(cohort_set_digest);
    hasher.update(aggregate_digest.unwrap_or_default());
    historical_fence.digest_into(&mut hasher);
    hasher.update(raised_fence.digest());
    hasher.update(raised_fence_receipt_digest);
    for checkpoint in checkpoints {
        hasher.update([checkpoint.phase as u8, checkpoint.state as u8]);
        hasher.update(checkpoint.evidence_digest.unwrap_or_default());
    }
    if let Some(evidence) = evidence {
        hasher.update(evidence.schema_version().to_be_bytes());
        hasher.update(evidence.operation_id().to_bytes());
        hasher.update(evidence.operation_kind().as_bytes());
        hasher.update(Sha256::digest(evidence.provider_payload()));
    }
    hasher.finalize().into()
}

// Same reasoning as `descriptor_digest`.
#[allow(clippy::too_many_arguments)]
fn observation_digest(
    descriptor_digest: [u8; 32],
    raised_fence_digest: [u8; 32],
    operation_id: ConnectorWriteOperationId,
    disposition: ConnectorHistoricalWriteDisposition,
    application: Option<&ConnectorHistoricalWriteApplication>,
    continuation: Option<&ConnectorHistoricalWriteContinuation>,
    cleanup_required: bool,
    proof_digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_HISTORICAL_WRITE_OBSERVATION_DOMAIN);
    hasher.update(descriptor_digest);
    hasher.update(raised_fence_digest);
    hasher.update(operation_id.to_bytes());
    hasher.update([disposition as u8]);
    if let Some(application) = application {
        hasher.update(b"application\0");
        hasher.update(application.committed_version.digest());
        hasher.update(application.receipt.digest());
        match &application.finalization {
            ExternalMutationFinalization::Complete => hasher.update([0u8]),
            ExternalMutationFinalization::Failed(failure) => {
                hasher.update([1u8]);
                hasher.update(failure.to_string().as_bytes());
            }
        }
    }
    if let Some(continuation) = continuation {
        hasher.update(b"continuation\0");
        hasher.update(continuation.digest());
    }
    hasher.update([cleanup_required as u8]);
    hasher.update(proof_digest);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::connector::external_write_fence::tests::fence;
    use crate::connector::{ConnectorInstanceId, ConnectorInstanceIncarnation};

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

    fn identity(operation_id: ConnectorWriteOperationId) -> ConnectorHistoricalWriteIdentity {
        ConnectorHistoricalWriteIdentity {
            historical_binding: binding(),
            table: table(),
            target_ref: ConnectorWriteTargetRef::main(),
            operation_id,
            intent: ConnectorWriteIntent::Append,
            cohort_set_digest: [7; 32],
            aggregate_digest: Some([8; 32]),
        }
    }

    fn checkpoints(
        state: ConnectorHistoricalWriteDispatchState,
    ) -> Vec<ConnectorHistoricalWriteCheckpoint> {
        vec![
            ConnectorHistoricalWriteCheckpoint {
                phase: ConnectorHistoricalWritePhase::Activated,
                state: ConnectorHistoricalWriteDispatchState::Completed,
                evidence_digest: None,
            },
            ConnectorHistoricalWriteCheckpoint {
                phase: ConnectorHistoricalWritePhase::WritersDispatched,
                state,
                evidence_digest: None,
            },
        ]
    }

    fn descriptor(
        operation_id: ConnectorWriteOperationId,
        historical: ConnectorHistoricalWriteFence,
        state: ConnectorHistoricalWriteDispatchState,
    ) -> ConnectorHistoricalWriteDescriptor {
        ConnectorHistoricalWriteDescriptor::try_new(
            identity(operation_id),
            ConnectorHistoricalWriteFenceFacts {
                historical_fence: historical,
                raised_fence: fence(operation_id, 1, 3, 1),
                raised_fence_receipt_digest: [9; 32],
            },
            checkpoints(state),
            None,
        )
        .expect("historical write descriptor")
    }

    fn established(
        operation_id: ConnectorWriteOperationId,
        epoch: u64,
    ) -> ConnectorHistoricalWriteFence {
        let historical = fence(operation_id, 1, epoch, 1);
        let receipt =
            ConnectorExternalFenceReceipt::try_new(&historical, Bytes::from_static(b"marker"))
                .expect("receipt");
        ConnectorHistoricalWriteFence::established(&receipt, historical).expect("established fence")
    }

    fn proof() -> ConnectorHistoricalWriteProof {
        ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"provider-proof"))
            .expect("proof")
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
        assert!(ConnectorHistoricalWriteProof::try_new(Bytes::new()).is_err());
        assert!(
            ConnectorHistoricalWriteProof::try_new(Bytes::from(vec![
                0;
                MAX_CONNECTOR_HISTORICAL_WRITE_PROOF_BYTES
                    + 1
            ]))
            .is_err()
        );
        let debug = format!("{:?}", proof());
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("provider-proof"));

        let raised = fence(ConnectorWriteOperationId::from_bytes([1; 16]), 1, 3, 1);
        assert!(ConnectorHistoricalWriteContinuation::try_new(&raised, Bytes::new()).is_err());
        assert!(
            ConnectorHistoricalWriteContinuation::try_new(
                &raised,
                Bytes::from(vec![
                    0;
                    MAX_CONNECTOR_HISTORICAL_WRITE_CONTINUATION_BYTES + 1
                ]),
            )
            .is_err()
        );
        let continuation = ConnectorHistoricalWriteContinuation::try_new(
            &raised,
            Bytes::from_static(b"signed-continuation"),
        )
        .expect("continuation");
        let debug = format!("{continuation:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("signed-continuation"));
    }

    #[test]
    fn descriptor_requires_a_strictly_higher_raised_fence() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let error = ConnectorHistoricalWriteDescriptor::try_new(
            identity(operation_id),
            ConnectorHistoricalWriteFenceFacts {
                historical_fence: established(operation_id, 3),
                raised_fence: fence(operation_id, 1, 3, 1),
                raised_fence_receipt_digest: [9; 32],
            },
            checkpoints(ConnectorHistoricalWriteDispatchState::Unknown),
            None,
        )
        .expect_err("an equal raised fence cannot fence out the historical authority");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
        descriptor(
            operation_id,
            established(operation_id, 2),
            ConnectorHistoricalWriteDispatchState::Unknown,
        )
        .validate()
        .expect("a strictly higher raised fence is accepted");
    }

    #[test]
    fn descriptor_digest_detects_mutation() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        sealed.validate().expect("sealed descriptor validates");
        let mut corrupted = sealed.clone();
        corrupted.cohort_set_digest = [11; 32];
        assert!(corrupted.validate().is_err());
        let mut relabelled = sealed;
        relabelled.checkpoints[1].state = ConnectorHistoricalWriteDispatchState::Dispatched;
        assert!(relabelled.validate().is_err());
    }

    #[test]
    fn applied_observation_requires_finalization_and_forbids_continuation() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = descriptor(
            operation_id,
            established(operation_id, 2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        assert!(
            ConnectorHistoricalWriteObservation::try_new(
                &sealed,
                ConnectorHistoricalWriteDisposition::Applied,
                ConnectorHistoricalWriteOutcomeFacts::default(),
                proof(),
            )
            .is_err()
        );
        let application = ConnectorHistoricalWriteApplication {
            committed_version: ConnectorCommittedVersion::try_new(
                Bytes::from_static(b"version"),
                Some(9),
            )
            .expect("committed version"),
            receipt: ConnectorWriteReceipt::try_new(Bytes::from_static(b"receipt"))
                .expect("receipt"),
            finalization: ExternalMutationFinalization::Complete,
        };
        let observation = ConnectorHistoricalWriteObservation::try_new(
            &sealed,
            ConnectorHistoricalWriteDisposition::Applied,
            ConnectorHistoricalWriteOutcomeFacts {
                application: Some(application.clone()),
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
            ConnectorHistoricalWriteObservation::try_new(
                &sealed,
                ConnectorHistoricalWriteDisposition::Staged,
                ConnectorHistoricalWriteOutcomeFacts {
                    application: Some(application),
                    continuation: None,
                    cleanup_required: true,
                },
                proof(),
            )
            .is_err(),
            "only an applied observation may carry finalization facts"
        );
    }

    #[test]
    fn continuation_is_only_legal_for_a_proven_not_dispatched_operation() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = descriptor(
            operation_id,
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        let continuation = ConnectorHistoricalWriteContinuation::try_new(
            &sealed.raised_fence,
            Bytes::from_static(b"signed"),
        )
        .expect("continuation");
        ConnectorHistoricalWriteObservation::try_new(
            &sealed,
            ConnectorHistoricalWriteDisposition::NotDispatched,
            ConnectorHistoricalWriteOutcomeFacts {
                application: None,
                continuation: Some(continuation.clone()),
                cleanup_required: false,
            },
            proof(),
        )
        .expect("not-dispatched continuation");

        for disposition in [
            ConnectorHistoricalWriteDisposition::Applied,
            ConnectorHistoricalWriteDisposition::NotApplied,
            ConnectorHistoricalWriteDisposition::Staged,
            ConnectorHistoricalWriteDisposition::Conflict,
            ConnectorHistoricalWriteDisposition::Ambiguous,
            ConnectorHistoricalWriteDisposition::Unsupported,
        ] {
            assert!(
                ConnectorHistoricalWriteObservation::try_new(
                    &sealed,
                    disposition,
                    ConnectorHistoricalWriteOutcomeFacts {
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

        let dispatched = descriptor(
            operation_id,
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::Dispatched,
        );
        let dispatched_continuation = ConnectorHistoricalWriteContinuation::try_new(
            &dispatched.raised_fence,
            Bytes::from_static(b"signed"),
        )
        .expect("continuation");
        assert!(
            ConnectorHistoricalWriteObservation::try_new(
                &dispatched,
                ConnectorHistoricalWriteDisposition::NotDispatched,
                ConnectorHistoricalWriteOutcomeFacts {
                    application: None,
                    continuation: Some(dispatched_continuation),
                    cleanup_required: false,
                },
                proof(),
            )
            .is_err(),
            "a dispatched journal checkpoint forbids a continuation"
        );

        let foreign = descriptor(
            ConnectorWriteOperationId::from_bytes([2; 16]),
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        let error = ConnectorHistoricalWriteObservation::try_new(
            &foreign,
            ConnectorHistoricalWriteDisposition::NotDispatched,
            ConnectorHistoricalWriteOutcomeFacts {
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
    fn unresolved_observations_cannot_request_cleanup() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = descriptor(
            operation_id,
            established(operation_id, 2),
            ConnectorHistoricalWriteDispatchState::Unknown,
        );
        for disposition in [
            ConnectorHistoricalWriteDisposition::Ambiguous,
            ConnectorHistoricalWriteDisposition::Unsupported,
        ] {
            assert!(
                ConnectorHistoricalWriteObservation::try_new(
                    &sealed,
                    disposition,
                    ConnectorHistoricalWriteOutcomeFacts {
                        application: None,
                        continuation: None,
                        cleanup_required: true,
                    },
                    proof(),
                )
                .is_err()
            );
            ConnectorHistoricalWriteObservation::try_new(
                &sealed,
                disposition,
                ConnectorHistoricalWriteOutcomeFacts::default(),
                proof(),
            )
            .expect("an unresolved observation keeps the recovery record");
        }
    }

    #[test]
    fn observation_rejects_a_foreign_descriptor() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let sealed = descriptor(
            operation_id,
            established(operation_id, 2),
            ConnectorHistoricalWriteDispatchState::Unknown,
        );
        let observation = ConnectorHistoricalWriteObservation::try_new(
            &sealed,
            ConnectorHistoricalWriteDisposition::NotApplied,
            ConnectorHistoricalWriteOutcomeFacts::default(),
            proof(),
        )
        .expect("observation");
        let other = descriptor(
            ConnectorWriteOperationId::from_bytes([2; 16]),
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::Unknown,
        );
        assert!(observation.validate_for(&other).is_err());
    }

    #[test]
    fn fence_raise_request_must_strictly_supersede_the_observed_fence() {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let observed = established(operation_id, 3);
        let stale = ConnectorHistoricalWriteFenceRaiseRequest {
            historical_binding: binding(),
            observed: observed.clone(),
            raised: fence(operation_id, 1, 2, 1),
            context: context(),
        };
        let error = stale.validate().expect_err("a lower raise is stale");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
        let raised = ConnectorHistoricalWriteFenceRaiseRequest {
            historical_binding: binding(),
            observed,
            raised: fence(operation_id, 1, 4, 1),
            context: context(),
        };
        raised.validate().expect("a higher raise is accepted");
    }

    #[test]
    fn owner_validation_rejects_a_foreign_generation() {
        struct Recovery {
            key: ConnectorExecutionBindingKey,
        }

        impl ConnectorHistoricalWriteRecovery for Recovery {
            fn binding_key(&self) -> &ConnectorExecutionBindingKey {
                &self.key
            }

            fn raise_external_fence(
                &self,
                _request: ConnectorHistoricalWriteFenceRaiseRequest,
            ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
                Err(invalid("test facet does not raise fences"))
            }

            fn inspect(
                &self,
                _descriptor: ConnectorHistoricalWriteDescriptor,
                _context: ConnectorRequestContext,
            ) -> Result<ConnectorHistoricalWriteObservation, ConnectorError> {
                Err(invalid("test facet does not inspect"))
            }

            fn cleanup(
                &self,
                _request: ConnectorHistoricalWriteCleanupRequest,
            ) -> Result<
                ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>,
                ConnectorError,
            > {
                Err(invalid("test facet does not clean up"))
            }

            fn reconcile_cleanup(
                &self,
                _operation_id: ConnectorWriteOperationId,
                _evidence: ExternalMutationEvidence,
                _context: ConnectorRequestContext,
            ) -> Result<
                ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>,
                ConnectorError,
            > {
                Err(invalid("test facet does not reconcile cleanup"))
            }
        }

        let instance = ConnectorInstanceId::parse("catalog.ice").expect("instance id");
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: crate::connector::ConnectorProviderId::parse("iceberg")
                .expect("provider id"),
            instance_id: instance.clone(),
        };
        let incarnation = ConnectorInstanceIncarnation::from_bytes([5; 16]);
        let recovery = Recovery {
            key: ConnectorExecutionBindingKey {
                instance_id: instance,
                incarnation,
            },
        };
        validate_historical_write_recovery_owner(&descriptor, incarnation, &recovery)
            .expect("matching generation");
        let foreign = ConnectorInstanceIncarnation::from_bytes([6; 16]);
        assert!(validate_historical_write_recovery_owner(&descriptor, foreign, &recovery).is_err());
    }
}
