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

//! Cross-incarnation inspection of a historical table-maintenance operation.
//!
//! A frontend that takes over a table cannot reach the connector generation
//! that started the maintenance work: `ConnectorInstanceIncarnation` is
//! process-local, so the exact binding dies with the process that made it. This
//! capability lets the *current* generation read the durable lake truth an old
//! attempt left behind and classify it.
//!
//! It is deliberately not a second execution API. The current generation
//! interprets proof; it never inherits the old generation's authority. Old
//! binding keys appear here only as descriptor input, never as a live binding,
//! and a dispatched action is never replayed — at most it is reconciled, or its
//! provably-staged leftovers are cleaned up. Work that provably never
//! dispatched can be continued, but only through a fresh continuation the
//! current generation signs against current live truth.

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorTableIdentity, ExternalMutationEvidence,
    ExternalMutationOutcome,
};

pub const MAX_CONNECTOR_HISTORICAL_MAINTENANCE_PROOF_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_HISTORICAL_MAINTENANCE_CONTINUATION_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_HISTORICAL_MAINTENANCE_ARTIFACTS: usize = 4096;
pub const MAX_CONNECTOR_HISTORICAL_MAINTENANCE_ARTIFACT_BYTES: usize = 64 * 1024;

/// Which durable maintenance lifecycle a historical operation belongs to.
///
/// The families keep separate typed results. A single universal recovery
/// payload would force every consumer to accept the union of four state
/// machines it cannot validate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalMaintenanceFamily {
    MetadataMaintenance,
    DistributedRewrite,
    Cleanup,
}

/// One provider-owned artifact the historical operation recorded.
///
/// The handle is opaque: only the provider that wrote it can interpret it. The
/// digest is what makes a late or crossed response detectable.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalMaintenanceArtifact {
    kind: Arc<str>,
    handle: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalMaintenanceArtifact {
    pub fn try_new(kind: impl Into<Arc<str>>, handle: Bytes) -> Result<Self, ConnectorError> {
        let kind = kind.into();
        if kind.is_empty() {
            return Err(invalid("historical maintenance artifact kind is empty"));
        }
        if handle.is_empty() || handle.len() > MAX_CONNECTOR_HISTORICAL_MAINTENANCE_ARTIFACT_BYTES {
            return Err(invalid(
                "historical maintenance artifact exceeds its bounded payload limit",
            ));
        }
        Ok(Self {
            digest: Sha256::digest(&handle).into(),
            kind,
            handle,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub const fn handle(&self) -> &Bytes {
        &self.handle
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorHistoricalMaintenanceArtifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalMaintenanceArtifact")
            .field("kind", &self.kind)
            .field("handle_len", &self.handle.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// What the durable record knows about whether the old attempt reached the
/// external system.
///
/// `dispatch_started` is the load-bearing field: once it is true the provider
/// must never execute the action again, only reconcile it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalDispatchFacts {
    pub dispatch_started: bool,
    pub batch_ordinal: Option<u32>,
    pub receipt_digest: Option<[u8; 32]>,
}

/// Complete value-only description of one historical maintenance operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalMaintenanceDescriptor {
    /// Proof input only. This binding is dead; it is never registered.
    pub historical_binding: ConnectorExecutionBindingKey,
    pub table: ConnectorTableIdentity,
    pub family: ConnectorHistoricalMaintenanceFamily,
    pub operation_id: [u8; 16],
    pub request_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    pub base_state_digest: Option<[u8; 32]>,
    pub artifacts: Vec<ConnectorHistoricalMaintenanceArtifact>,
    pub dispatch: ConnectorHistoricalDispatchFacts,
    /// The current CP-4A attempt asking for this inspection.
    pub recovery_attempt: [u8; 16],
    digest: [u8; 32],
}

impl ConnectorHistoricalMaintenanceDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        historical_binding: ConnectorExecutionBindingKey,
        table: ConnectorTableIdentity,
        family: ConnectorHistoricalMaintenanceFamily,
        operation_id: [u8; 16],
        request_digest: [u8; 32],
        plan_digest: Option<[u8; 32]>,
        base_state_digest: Option<[u8; 32]>,
        artifacts: Vec<ConnectorHistoricalMaintenanceArtifact>,
        dispatch: ConnectorHistoricalDispatchFacts,
        recovery_attempt: [u8; 16],
    ) -> Result<Self, ConnectorError> {
        if operation_id == [0u8; 16] {
            return Err(invalid("historical maintenance operation id is empty"));
        }
        if recovery_attempt == [0u8; 16] {
            return Err(invalid("historical maintenance recovery attempt is empty"));
        }
        if artifacts.len() > MAX_CONNECTOR_HISTORICAL_MAINTENANCE_ARTIFACTS {
            return Err(invalid(
                "historical maintenance descriptor exceeds its artifact limit",
            ));
        }
        // A batch ordinal without a dispatch flag describes a batch that was
        // prepared but never sent; a receipt without one cannot exist.
        if dispatch.receipt_digest.is_some() && !dispatch.dispatch_started {
            return Err(invalid(
                "historical maintenance dispatch facts carry a receipt without a dispatch",
            ));
        }
        let digest = descriptor_digest(
            &historical_binding,
            &table,
            family,
            operation_id,
            request_digest,
            plan_digest.as_ref(),
            base_state_digest.as_ref(),
            &artifacts,
            &dispatch,
            recovery_attempt,
        );
        Ok(Self {
            historical_binding,
            table,
            family,
            operation_id,
            request_digest,
            plan_digest,
            base_state_digest,
            artifacts,
            dispatch,
            recovery_attempt,
            digest,
        })
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = descriptor_digest(
            &self.historical_binding,
            &self.table,
            self.family,
            self.operation_id,
            self.request_digest,
            self.plan_digest.as_ref(),
            self.base_state_digest.as_ref(),
            &self.artifacts,
            &self.dispatch,
            self.recovery_attempt,
        );
        if expected != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical maintenance descriptor digest does not match its fields",
            ));
        }
        Ok(())
    }
}

/// Opaque provider evidence backing an observation.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalMaintenanceProof {
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalMaintenanceProof {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        if payload.is_empty() || payload.len() > MAX_CONNECTOR_HISTORICAL_MAINTENANCE_PROOF_BYTES {
            return Err(invalid(
                "historical maintenance proof exceeds its bounded payload limit",
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
                "historical maintenance proof digest does not match its payload",
            ));
        }
        Ok(())
    }

    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorHistoricalMaintenanceProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalMaintenanceProof")
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// What the current generation could prove about the old attempt.
///
/// `Ambiguous` is a real answer, not a failure to try: it means the evidence
/// does not decide the question, and the operation must stay unresolved rather
/// than be guessed into a terminal state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalMaintenanceDisposition {
    NotDispatched,
    Applied,
    NotApplied,
    PartiallyApplied,
    Ambiguous,
}

/// Per-family classification detail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorHistoricalMaintenanceOutcome {
    MetadataMaintenance {
        committed_version: Option<ConnectorCommittedVersion>,
        marker_present: bool,
    },
    DistributedRewrite {
        committed_version: Option<ConnectorCommittedVersion>,
        staged_artifacts_present: bool,
        cleanup_required: bool,
    },
    Cleanup {
        deleted_count: u64,
        already_absent_count: u64,
        skipped_count: u64,
        failed_count: u64,
        unknown_count: u64,
    },
}

impl ConnectorHistoricalMaintenanceOutcome {
    const fn family(&self) -> ConnectorHistoricalMaintenanceFamily {
        match self {
            Self::MetadataMaintenance { .. } => {
                ConnectorHistoricalMaintenanceFamily::MetadataMaintenance
            }
            Self::DistributedRewrite { .. } => {
                ConnectorHistoricalMaintenanceFamily::DistributedRewrite
            }
            Self::Cleanup { .. } => ConnectorHistoricalMaintenanceFamily::Cleanup,
        }
    }
}

/// A fresh authorization to continue work that provably never dispatched.
///
/// This is not the old plan revived. The current generation signs it against
/// current live truth, binds it to the current binding, the stable operation and
/// the CP-4A attempt asking for it, and the frontend must persist its digest
/// under the fence before using it.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorHistoricalMaintenanceContinuation {
    binding: ConnectorExecutionBindingKey,
    operation_id: [u8; 16],
    recovery_attempt: [u8; 16],
    live_state_digest: [u8; 32],
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorHistoricalMaintenanceContinuation {
    pub fn try_new(
        binding: ConnectorExecutionBindingKey,
        operation_id: [u8; 16],
        recovery_attempt: [u8; 16],
        live_state_digest: [u8; 32],
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if payload.is_empty()
            || payload.len() > MAX_CONNECTOR_HISTORICAL_MAINTENANCE_CONTINUATION_BYTES
        {
            return Err(invalid(
                "historical maintenance continuation exceeds its bounded payload limit",
            ));
        }
        if operation_id == [0u8; 16] || recovery_attempt == [0u8; 16] {
            return Err(invalid(
                "historical maintenance continuation is not bound to an operation and attempt",
            ));
        }
        let digest = continuation_digest(
            &binding,
            operation_id,
            recovery_attempt,
            live_state_digest,
            &payload,
        );
        Ok(Self {
            binding,
            operation_id,
            recovery_attempt,
            live_state_digest,
            payload,
            digest,
        })
    }

    pub const fn binding(&self) -> &ConnectorExecutionBindingKey {
        &self.binding
    }

    pub const fn operation_id(&self) -> [u8; 16] {
        self.operation_id
    }

    pub const fn recovery_attempt(&self) -> [u8; 16] {
        self.recovery_attempt
    }

    pub const fn live_state_digest(&self) -> [u8; 32] {
        self.live_state_digest
    }

    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorHistoricalMaintenanceContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorHistoricalMaintenanceContinuation")
            .field("operation_id", &self.operation_id)
            .field("recovery_attempt", &self.recovery_attempt)
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

/// The provider's answer about one historical operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalMaintenanceObservation {
    pub descriptor_digest: [u8; 32],
    pub disposition: ConnectorHistoricalMaintenanceDisposition,
    pub outcome: ConnectorHistoricalMaintenanceOutcome,
    pub proof: ConnectorHistoricalMaintenanceProof,
    pub continuation: Option<ConnectorHistoricalMaintenanceContinuation>,
    digest: [u8; 32],
}

impl ConnectorHistoricalMaintenanceObservation {
    pub fn try_new(
        descriptor: &ConnectorHistoricalMaintenanceDescriptor,
        disposition: ConnectorHistoricalMaintenanceDisposition,
        outcome: ConnectorHistoricalMaintenanceOutcome,
        proof: ConnectorHistoricalMaintenanceProof,
        continuation: Option<ConnectorHistoricalMaintenanceContinuation>,
    ) -> Result<Self, ConnectorError> {
        if outcome.family() != descriptor.family {
            return Err(invalid(
                "historical maintenance observation answers a different family than it was asked",
            ));
        }
        proof.validate()?;
        // A continuation authorizes future work. Handing one out for an action
        // that already reached the external system would turn recovery into a
        // replay, which is the one thing this capability must never do.
        if let Some(continuation) = continuation.as_ref() {
            if disposition != ConnectorHistoricalMaintenanceDisposition::NotDispatched {
                return Err(invalid(
                    "historical maintenance continuation requires a not-dispatched disposition",
                ));
            }
            if descriptor.dispatch.dispatch_started {
                return Err(invalid(
                    "historical maintenance continuation contradicts a dispatched operation",
                ));
            }
            if continuation.operation_id != descriptor.operation_id
                || continuation.recovery_attempt != descriptor.recovery_attempt
            {
                return Err(invalid(
                    "historical maintenance continuation is bound to a different operation or attempt",
                ));
            }
        }
        let digest = observation_digest(
            descriptor.digest,
            disposition,
            &outcome,
            proof.digest(),
            continuation.as_ref().map(Self::continuation_digest),
        );
        Ok(Self {
            descriptor_digest: descriptor.digest,
            disposition,
            outcome,
            proof,
            continuation,
            digest,
        })
    }

    fn continuation_digest(continuation: &ConnectorHistoricalMaintenanceContinuation) -> [u8; 32] {
        continuation.digest()
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Remove leftovers the inspection proved belong to the old attempt.
#[derive(Clone)]
pub struct ConnectorHistoricalMaintenanceCleanupRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub descriptor_digest: [u8; 32],
    pub observation: ConnectorHistoricalMaintenanceObservation,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalMaintenanceCleanupReceipt {
    pub descriptor_digest: [u8; 32],
    pub observation_digest: [u8; 32],
}

/// Provider capability for reading a dead generation's maintenance work.
///
/// This is resolved separately from the ordinary exact-generation maintenance
/// capabilities. A provider that does not implement it makes the frontend keep
/// the operation unresolved; there is no fallback to ordinary reconcile, since
/// that path requires the exact generation which by definition is gone.
pub trait ConnectorHistoricalMaintenanceRecovery: Send + Sync {
    /// The *current* binding this capability speaks for.
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn inspect(
        &self,
        descriptor: ConnectorHistoricalMaintenanceDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalMaintenanceObservation, ConnectorError>;

    fn cleanup(
        &self,
        request: ConnectorHistoricalMaintenanceCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalMaintenanceCleanupReceipt>, ConnectorError>;

    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalMaintenanceCleanupReceipt>, ConnectorError>;
}

/// Resolve the *current* generation's historical inspector.
///
/// There is deliberately no exact-generation variant. Exact resolution is what
/// recovery has already lost; offering it here would invite a caller to pretend
/// the dead generation is still reachable.
pub trait ConnectorHistoricalMaintenanceResolver: Send + Sync {
    fn acquire_current_historical_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorHistoricalMaintenanceLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorHistoricalMaintenanceLease {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
    recovery: Arc<dyn ConnectorHistoricalMaintenanceRecovery>,
    _release: Arc<HistoricalMaintenanceRelease>,
}

struct HistoricalMaintenanceRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl Drop for HistoricalMaintenanceRelease {
    fn drop(&mut self) {
        if let Some(release) = self
            .release
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            release();
        }
    }
}

impl ConnectorHistoricalMaintenanceLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
        recovery: Arc<dyn ConnectorHistoricalMaintenanceRecovery>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if descriptor.instance_id != key.instance_id || recovery.binding_key() != &key {
            return Err(invalid(
                "historical maintenance capability does not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            key,
            recovery,
            _release: Arc::new(HistoricalMaintenanceRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    /// Inspect one historical operation under this live generation.
    ///
    /// The descriptor's own binding is the dead one being investigated; this
    /// lease is the live generation doing the investigating. They must differ,
    /// otherwise the caller still holds the original generation and should use
    /// the ordinary exact-generation reconcile instead of historical recovery.
    pub fn inspect(
        &self,
        descriptor: ConnectorHistoricalMaintenanceDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalMaintenanceObservation, ConnectorError> {
        descriptor.validate()?;
        if descriptor.table.instance_id != self.key.instance_id {
            return Err(invalid(
                "historical maintenance descriptor belongs to another connector instance",
            ));
        }
        if descriptor.historical_binding == self.key {
            return Err(invalid(
                "historical maintenance inspection was asked for the live generation itself",
            ));
        }
        let observation = self.recovery.inspect(descriptor.clone(), context)?;
        if observation.descriptor_digest != descriptor.digest() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "historical maintenance observation answers a different descriptor",
            ));
        }
        Ok(observation)
    }

    pub fn cleanup(
        &self,
        request: ConnectorHistoricalMaintenanceCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalMaintenanceCleanupReceipt>, ConnectorError>
    {
        self.recovery.cleanup(request)
    }

    pub fn reconcile_cleanup(
        &self,
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalMaintenanceCleanupReceipt>, ConnectorError>
    {
        self.recovery
            .reconcile_cleanup(operation_id, evidence, context)
    }
}

/// The capability must belong to the live control generation that offered it.
pub fn validate_historical_maintenance_recovery_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: super::ConnectorInstanceIncarnation,
    recovery: &dyn ConnectorHistoricalMaintenanceRecovery,
) -> Result<(), ConnectorError> {
    let key = recovery.binding_key();
    if key.instance_id != descriptor.instance_id || key.incarnation != incarnation {
        return Err(invalid(
            "historical maintenance recovery capability does not match its control binding generation",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

// Every argument is a field of the digest this function seals; grouping them
// into a struct would add a type that exists only to be destructured here.
#[allow(clippy::too_many_arguments)]
fn descriptor_digest(
    binding: &ConnectorExecutionBindingKey,
    table: &ConnectorTableIdentity,
    family: ConnectorHistoricalMaintenanceFamily,
    operation_id: [u8; 16],
    request_digest: [u8; 32],
    plan_digest: Option<&[u8; 32]>,
    base_state_digest: Option<&[u8; 32]>,
    artifacts: &[ConnectorHistoricalMaintenanceArtifact],
    dispatch: &ConnectorHistoricalDispatchFacts,
    recovery_attempt: [u8; 16],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks/connector/historical-maintenance/descriptor/v1\0");
    hasher.update(binding.instance_id.as_str().as_bytes());
    hasher.update([0u8]);
    hasher.update(binding.incarnation.to_bytes());
    hasher.update(table.instance_id.as_str().as_bytes());
    hasher.update([0u8]);
    hasher.update(table.namespace.as_ref().as_bytes());
    hasher.update([0u8]);
    hasher.update(table.table.as_ref().as_bytes());
    hasher.update([0u8]);
    hasher.update([family as u8]);
    hasher.update(operation_id);
    hasher.update(request_digest);
    match plan_digest {
        Some(digest) => {
            hasher.update([1u8]);
            hasher.update(digest);
        }
        None => hasher.update([0u8]),
    }
    match base_state_digest {
        Some(digest) => {
            hasher.update([1u8]);
            hasher.update(digest);
        }
        None => hasher.update([0u8]),
    }
    hasher.update((artifacts.len() as u64).to_be_bytes());
    for artifact in artifacts {
        hasher.update(artifact.kind().as_bytes());
        hasher.update([0u8]);
        hasher.update(artifact.digest());
    }
    hasher.update([u8::from(dispatch.dispatch_started)]);
    match dispatch.batch_ordinal {
        Some(ordinal) => {
            hasher.update([1u8]);
            hasher.update(ordinal.to_be_bytes());
        }
        None => hasher.update([0u8]),
    }
    match dispatch.receipt_digest {
        Some(digest) => {
            hasher.update([1u8]);
            hasher.update(digest);
        }
        None => hasher.update([0u8]),
    }
    hasher.update(recovery_attempt);
    hasher.finalize().into()
}

fn continuation_digest(
    binding: &ConnectorExecutionBindingKey,
    operation_id: [u8; 16],
    recovery_attempt: [u8; 16],
    live_state_digest: [u8; 32],
    payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks/connector/historical-maintenance/continuation/v1\0");
    hasher.update(binding.instance_id.as_str().as_bytes());
    hasher.update([0u8]);
    hasher.update(binding.incarnation.to_bytes());
    hasher.update(operation_id);
    hasher.update(recovery_attempt);
    hasher.update(live_state_digest);
    hasher.update(payload);
    hasher.finalize().into()
}

fn observation_digest(
    descriptor_digest: [u8; 32],
    disposition: ConnectorHistoricalMaintenanceDisposition,
    outcome: &ConnectorHistoricalMaintenanceOutcome,
    proof_digest: [u8; 32],
    continuation_digest: Option<[u8; 32]>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks/connector/historical-maintenance/observation/v1\0");
    hasher.update(descriptor_digest);
    hasher.update([disposition as u8]);
    match outcome {
        ConnectorHistoricalMaintenanceOutcome::MetadataMaintenance {
            committed_version,
            marker_present,
        } => {
            hasher.update([0u8]);
            hasher.update([u8::from(*marker_present)]);
            hash_version(&mut hasher, committed_version.as_ref());
        }
        ConnectorHistoricalMaintenanceOutcome::DistributedRewrite {
            committed_version,
            staged_artifacts_present,
            cleanup_required,
        } => {
            hasher.update([1u8]);
            hasher.update([u8::from(*staged_artifacts_present)]);
            hasher.update([u8::from(*cleanup_required)]);
            hash_version(&mut hasher, committed_version.as_ref());
        }
        ConnectorHistoricalMaintenanceOutcome::Cleanup {
            deleted_count,
            already_absent_count,
            skipped_count,
            failed_count,
            unknown_count,
        } => {
            hasher.update([2u8]);
            for count in [
                deleted_count,
                already_absent_count,
                skipped_count,
                failed_count,
                unknown_count,
            ] {
                hasher.update(count.to_be_bytes());
            }
        }
    }
    hasher.update(proof_digest);
    match continuation_digest {
        Some(digest) => {
            hasher.update([1u8]);
            hasher.update(digest);
        }
        None => hasher.update([0u8]),
    }
    hasher.finalize().into()
}

fn hash_version(hasher: &mut Sha256, version: Option<&ConnectorCommittedVersion>) {
    match version {
        Some(version) => {
            hasher.update([1u8]);
            hasher.update(version.digest());
        }
        None => hasher.update([0u8]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{ConnectorInstanceId, ConnectorInstanceIncarnation};

    fn descriptor(
        family: ConnectorHistoricalMaintenanceFamily,
        dispatch: ConnectorHistoricalDispatchFacts,
    ) -> ConnectorHistoricalMaintenanceDescriptor {
        let instance = ConnectorInstanceId::parse("catalog.ice").unwrap();
        ConnectorHistoricalMaintenanceDescriptor::try_new(
            ConnectorExecutionBindingKey {
                instance_id: instance.clone(),
                incarnation: ConnectorInstanceIncarnation::new(),
            },
            ConnectorTableIdentity {
                instance_id: instance,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            family,
            [7; 16],
            [1; 32],
            Some([2; 32]),
            Some([3; 32]),
            vec![
                ConnectorHistoricalMaintenanceArtifact::try_new(
                    "manifest",
                    Bytes::from_static(b"handle"),
                )
                .unwrap(),
            ],
            dispatch,
            [9; 16],
        )
        .unwrap()
    }

    fn undispatched() -> ConnectorHistoricalDispatchFacts {
        ConnectorHistoricalDispatchFacts {
            dispatch_started: false,
            batch_ordinal: None,
            receipt_digest: None,
        }
    }

    fn dispatched() -> ConnectorHistoricalDispatchFacts {
        ConnectorHistoricalDispatchFacts {
            dispatch_started: true,
            batch_ordinal: Some(0),
            receipt_digest: Some([4; 32]),
        }
    }

    fn proof() -> ConnectorHistoricalMaintenanceProof {
        ConnectorHistoricalMaintenanceProof::try_new(Bytes::from_static(b"opaque-evidence"))
            .unwrap()
    }

    #[test]
    fn proof_and_artifact_reject_unbounded_payloads_and_redact_debug() {
        assert!(ConnectorHistoricalMaintenanceProof::try_new(Bytes::new()).is_err());
        assert!(
            ConnectorHistoricalMaintenanceProof::try_new(Bytes::from(vec![
                0;
                MAX_CONNECTOR_HISTORICAL_MAINTENANCE_PROOF_BYTES
                    + 1
            ]))
            .is_err()
        );
        assert!(
            ConnectorHistoricalMaintenanceArtifact::try_new("kind", Bytes::new()).is_err(),
            "an empty artifact handle cannot identify anything"
        );

        let debug = format!("{:?}", proof());
        assert!(debug.contains("payload_len"));
        assert!(
            !debug.contains("opaque-evidence"),
            "provider evidence must not leak through Debug"
        );
    }

    #[test]
    fn a_receipt_without_a_dispatch_is_rejected() {
        let instance = ConnectorInstanceId::parse("catalog.ice").unwrap();
        let error = ConnectorHistoricalMaintenanceDescriptor::try_new(
            ConnectorExecutionBindingKey {
                instance_id: instance.clone(),
                incarnation: ConnectorInstanceIncarnation::new(),
            },
            ConnectorTableIdentity {
                instance_id: instance,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            ConnectorHistoricalMaintenanceFamily::Cleanup,
            [7; 16],
            [1; 32],
            None,
            None,
            Vec::new(),
            ConnectorHistoricalDispatchFacts {
                dispatch_started: false,
                batch_ordinal: Some(0),
                receipt_digest: Some([4; 32]),
            },
            [9; 16],
        )
        .expect_err("a receipt proves a dispatch happened");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn descriptor_digest_detects_mutation() {
        let mut descriptor = descriptor(
            ConnectorHistoricalMaintenanceFamily::Cleanup,
            undispatched(),
        );
        descriptor.validate().unwrap();
        descriptor.dispatch.dispatch_started = true;
        assert_eq!(
            descriptor.validate().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData,
            "flipping a dispatch fact after sealing must be detectable"
        );
    }

    #[test]
    fn an_observation_must_answer_the_family_it_was_asked() {
        let descriptor = descriptor(
            ConnectorHistoricalMaintenanceFamily::Cleanup,
            undispatched(),
        );
        let error = ConnectorHistoricalMaintenanceObservation::try_new(
            &descriptor,
            ConnectorHistoricalMaintenanceDisposition::Applied,
            ConnectorHistoricalMaintenanceOutcome::MetadataMaintenance {
                committed_version: None,
                marker_present: true,
            },
            proof(),
            None,
        )
        .expect_err("a cleanup question cannot be answered with a metadata result");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn a_dispatched_operation_can_never_receive_a_continuation() {
        let descriptor = descriptor(ConnectorHistoricalMaintenanceFamily::Cleanup, dispatched());
        let continuation = ConnectorHistoricalMaintenanceContinuation::try_new(
            descriptor.historical_binding.clone(),
            descriptor.operation_id,
            descriptor.recovery_attempt,
            [5; 32],
            Bytes::from_static(b"continue"),
        )
        .unwrap();

        // Both guards must hold independently: the disposition alone must not
        // be able to authorize continuing work that already reached the lake.
        let error = ConnectorHistoricalMaintenanceObservation::try_new(
            &descriptor,
            ConnectorHistoricalMaintenanceDisposition::NotDispatched,
            ConnectorHistoricalMaintenanceOutcome::Cleanup {
                deleted_count: 0,
                already_absent_count: 0,
                skipped_count: 0,
                failed_count: 0,
                unknown_count: 0,
            },
            proof(),
            Some(continuation),
        )
        .expect_err("a dispatched batch must never be continued");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn a_continuation_must_be_bound_to_the_asking_attempt() {
        let descriptor = descriptor(
            ConnectorHistoricalMaintenanceFamily::DistributedRewrite,
            undispatched(),
        );
        let foreign = ConnectorHistoricalMaintenanceContinuation::try_new(
            descriptor.historical_binding.clone(),
            descriptor.operation_id,
            [8; 16],
            [5; 32],
            Bytes::from_static(b"continue"),
        )
        .unwrap();
        let error = ConnectorHistoricalMaintenanceObservation::try_new(
            &descriptor,
            ConnectorHistoricalMaintenanceDisposition::NotDispatched,
            ConnectorHistoricalMaintenanceOutcome::DistributedRewrite {
                committed_version: None,
                staged_artifacts_present: false,
                cleanup_required: false,
            },
            proof(),
            Some(foreign),
        )
        .expect_err("a continuation signed for another attempt is not usable");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn an_undispatched_operation_may_be_continued() {
        let descriptor = descriptor(
            ConnectorHistoricalMaintenanceFamily::DistributedRewrite,
            undispatched(),
        );
        let continuation = ConnectorHistoricalMaintenanceContinuation::try_new(
            descriptor.historical_binding.clone(),
            descriptor.operation_id,
            descriptor.recovery_attempt,
            [5; 32],
            Bytes::from_static(b"continue"),
        )
        .unwrap();
        let observation = ConnectorHistoricalMaintenanceObservation::try_new(
            &descriptor,
            ConnectorHistoricalMaintenanceDisposition::NotDispatched,
            ConnectorHistoricalMaintenanceOutcome::DistributedRewrite {
                committed_version: None,
                staged_artifacts_present: false,
                cleanup_required: false,
            },
            proof(),
            Some(continuation),
        )
        .unwrap();
        assert_eq!(observation.descriptor_digest, descriptor.digest());
        assert!(observation.continuation.is_some());
    }
}
