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

//! CP-3C frontend historical direct data-mutation recovery profile.
//!
//! TRUNCATE and ADD FILES never run through the distributed writer. They are
//! planned once and executed once by one exact Connector generation, so after a
//! frontend takeover the old generation is gone and the only admissible
//! evidence about it is immutable external truth. This profile is what the
//! bounded CP-3A recovery controller does with such a claim; it replaces the
//! blanket deferral the controller used before CP-3C.
//!
//! The convergence order is fixed by spec CP-3C D2 and is enforced jointly with
//! the provider:
//!
//! 1. the operation is claimed under its exact operation lease (CP-3A);
//! 2. the immutable recovery request is fenced-persisted — it can never change
//!    once durable, so every later cycle inspects exactly the same request;
//! 3. `raise_external_fence` establishes a strictly higher external fence, so
//!    the historical authority is closed *before* anything is concluded;
//! 4. `inspect` runs outside any StateStore transaction;
//! 5. the typed result is fenced-persisted;
//! 6. only then may the profile finalize the statement, run a proof-bound
//!    guarded cleanup, or keep the record unresolved — strictly according to
//!    the disposition.
//!
//! Four rules shape everything below.
//!
//! *The frontend classifies nothing.* Every disposition, receipt, proof and
//! continuation is produced by the current provider generation and stored as
//! identity, digests and bounded opaque bytes. No provider payload is decoded,
//! and no file list, object path, manifest or snapshot membership is read.
//!
//! *No ordinary direct-mutation call ever happens here.* The profile only ever
//! reaches the separately installed historical facet. It never calls
//! `plan_mutation`, `execute` or `reconcile` on the historical operation, never
//! revives the historical binding, and never reuses an old prepared handle — a
//! destructive mutation that may already have been dispatched is *classified*,
//! never replayed (spec D3).
//!
//! *Absence is never proof.* A missing marker, a missing artifact, an
//! unreconstructable fenced identity, a fence that cannot be raised strictly
//! higher, or an `Ambiguous` disposition all reschedule the recovery due
//! instead of concluding anything.
//!
//! *An ADD FILES source scope is released only against proof.*
//! [`permits_source_scope_release`] is necessary but never sufficient: the
//! reservation is freed exclusively inside the fenced journal transaction that
//! also validates the immutable source-scope digest (spec D4/D5). Every
//! undetermined, partial, conflicting or unsupported outcome keeps it held.
//!
//! [`permits_source_scope_release`]: ConnectorHistoricalDataMutationDisposition::permits_source_scope_release

// Design: ADR-0068 (docs/adr/ADR-0068-external-write-fence-as-catalog-linearization-point.md)

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorClusterIdentity, ConnectorControlPlanningLease,
    ConnectorControlRegistry, ConnectorDataMutationPlanSummary, ConnectorDataMutationSourceScope,
    ConnectorDataMutationSourceScopeKind, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExternalFenceFailure, ConnectorExternalFenceGeneration,
    ConnectorExternalFenceReceipt, ConnectorExternalOperationFence,
    ConnectorHistoricalDataMutationCheckpoint, ConnectorHistoricalDataMutationCleanupReceipt,
    ConnectorHistoricalDataMutationCleanupRequest, ConnectorHistoricalDataMutationContinuation,
    ConnectorHistoricalDataMutationDescriptor, ConnectorHistoricalDataMutationDispatchState,
    ConnectorHistoricalDataMutationDisposition, ConnectorHistoricalDataMutationFamily,
    ConnectorHistoricalDataMutationFence, ConnectorHistoricalDataMutationFenceFacts,
    ConnectorHistoricalDataMutationFenceRaiseRequest, ConnectorHistoricalDataMutationIdentity,
    ConnectorHistoricalDataMutationObservation, ConnectorHistoricalDataMutationPhase,
    ConnectorHistoricalDataMutationRecovery, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteOperationId, ConnectorWriteTargetRef, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::coordination::{ActiveDmlOperation, DmlExternalFenceProposal};
use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::model::{
    AddFilesLifecyclePhase, AddFilesLifecycleRecord, AddFilesMutationRequest, AddFilesSourceAction,
    ConnectorWriteFailureKind, ConnectorWriteFailureRecord,
    DML_DIRECT_MUTATION_FENCE_CODEC_VERSION, DML_EXTERNAL_FENCE_CODEC_VERSION,
    DML_HISTORICAL_DATA_MUTATION_RECOVERY_CODEC_VERSION, DmlDirectMutationFenceReceiptRecord,
    DmlDirectMutationKind, DmlExternalFenceGeneration, DmlExternalFenceIdentity,
    DmlExternalFenceReceiptRecord, DmlHistoricalCleanupState, DmlHistoricalDataMutationDisposition,
    DmlHistoricalDataMutationRecoveryMutationRequest, DmlHistoricalDataMutationRecoveryRecord,
    DmlHistoricalDataMutationRequestRecord, DmlHistoricalDataMutationResultRecord,
    DmlHistoricalDispatchCertainty, DmlHistoricalRecoveryPhase, DmlOpaquePayload, DmlOperationId,
    DurableExternalFact, ExternalFactOutcome, OperationKind, OperationMutationRequest,
    OperationPayload, OperationState, OperationTarget, SourceScopeOwnership, StatementNextAction,
    StoredOperation, TruncateLifecyclePhase, TruncateLifecycleRecord,
    operation_requires_recovery_scan_with_direct_mutation, validate_operation_transition,
};

/// Backoff for an operation whose evidence is insufficient right now: an
/// unreconstructable fenced identity, an unsealed descriptor, or a transient
/// provider failure. Nothing has been concluded, so the due simply moves.
pub(crate) const DML_STATEMENT_RECOVERY_UNRESOLVED_DELAY_MS: i64 = 5_000;
/// Backoff for a proof-bound cleanup that did not complete. The obligation is
/// retained (spec D4) and retried on a later cycle.
pub(crate) const DML_STATEMENT_RECOVERY_CLEANUP_DELAY_MS: i64 = 15_000;
/// Backoff for a fact this cycle cannot change: a superseded recovery attempt,
/// unreadable external truth, a retained provider continuation, or a resolved
/// record that still needs an operator.
pub(crate) const DML_STATEMENT_RECOVERY_BLOCKED_DELAY_MS: i64 = 30_000;

/// Deadline for one historical provider action. Recovery is a background,
/// bounded activity; it must never wait on a provider indefinitely.
const DML_STATEMENT_RECOVERY_ACTION_DEADLINE: Duration = Duration::from_secs(30);

/// Domain separator for the frontend-owned digest of one immutable historical
/// direct-mutation request.
const DML_HISTORICAL_DATA_MUTATION_REQUEST_DOMAIN: &[u8] =
    b"novarocks.dml.historical-data-mutation-request.v1\0";
/// Domain separator for the fenced resource digest.
///
/// This is deliberately the same rule the ordinary fence receipt projection
/// uses: a raised fence names the same resource as the fence it supersedes, so
/// the two records must be comparable field by field.
const DML_EXTERNAL_FENCE_RESOURCE_DOMAIN: &[u8] = b"novarocks.dml.external-fence-resource.v1\0";

/// The historical connector generation the frontend never recorded.
///
/// An all-zero incarnation is a sentinel for "unknown", never a claim about a
/// real generation: it is deliberately not the current generation, so a
/// provider can never mistake the recovering owner for the historical owner.
const UNKNOWN_HISTORICAL_INCARNATION: [u8; 16] = [0; 16];

/// The direct data-mutation family this operation kind belongs to, if any.
///
/// Row DML and CTAS have their own historical reconciliation owners (CP-3B,
/// CP-3D) and must never be driven here.
pub(crate) const fn direct_mutation_kind(kind: OperationKind) -> Option<DmlDirectMutationKind> {
    match kind {
        OperationKind::Truncate => Some(DmlDirectMutationKind::Truncate),
        OperationKind::AddFiles => Some(DmlDirectMutationKind::AddFiles),
        _ => None,
    }
}

const fn family_of(kind: DmlDirectMutationKind) -> ConnectorHistoricalDataMutationFamily {
    match kind {
        DmlDirectMutationKind::Truncate => ConnectorHistoricalDataMutationFamily::Truncate,
        DmlDirectMutationKind::AddFiles => {
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles
        }
    }
}

// ---------------------------------------------------------------------------
// Frontend-visible projection of one historical observation
// ---------------------------------------------------------------------------

/// Everything the frontend is allowed to learn from one historical inspection.
///
/// Building this value is the only projection the profile performs, and it
/// reads no provider payload: proof, continuation and receipt are reduced to
/// their digests before they cross this boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoricalDataMutationOutcome {
    pub disposition: ConnectorHistoricalDataMutationDisposition,
    pub operation_id: ConnectorMutationOperationId,
    pub family: ConnectorHistoricalDataMutationFamily,
    pub descriptor_digest: [u8; 32],
    pub raised_fence_digest: [u8; 32],
    pub observation_digest: [u8; 32],
    pub proof_digest: [u8; 32],
    pub continuation_digest: Option<[u8; 32]>,
    pub source_scope_digest: Option<[u8; 32]>,
    pub finalization_complete: Option<bool>,
    pub cleanup_required: bool,
    pub determined: bool,
}

impl HistoricalDataMutationOutcome {
    pub fn project(observation: &ConnectorHistoricalDataMutationObservation) -> Self {
        Self {
            disposition: observation.disposition,
            operation_id: observation.operation_id,
            family: observation.family,
            descriptor_digest: observation.descriptor_digest,
            raised_fence_digest: observation.raised_fence_digest,
            observation_digest: observation.digest(),
            proof_digest: observation.proof.digest(),
            continuation_digest: observation
                .continuation
                .as_ref()
                .map(ConnectorHistoricalDataMutationContinuation::digest),
            source_scope_digest: observation
                .source_scope
                .map(ConnectorDataMutationSourceScope::digest),
            finalization_complete: observation.application.as_ref().map(|application| {
                matches!(
                    application.finalization,
                    ExternalMutationFinalization::Complete
                )
            }),
            cleanup_required: observation.cleanup_required,
            determined: observation.disposition.is_resolved(),
        }
    }

    /// Whether this outcome answers exactly the supplied descriptor under the
    /// supplied raised external fence.
    pub fn answers(&self, descriptor: &ConnectorHistoricalDataMutationDescriptor) -> bool {
        self.descriptor_digest == descriptor.digest()
            && self.operation_id == descriptor.operation_id
            && self.family == descriptor.family
            && self.raised_fence_digest == descriptor.raised_fence.digest()
            && self.source_scope_digest
                == descriptor
                    .source_scope
                    .map(ConnectorDataMutationSourceScope::digest)
    }
}

/// The CP-3C D5 double check performed on every historical provider response.
///
/// A response is only durable when it was produced under the external fence
/// this owner still holds *and* it answers exactly the immutable descriptor —
/// including its source scope — that this recovery record still owns. A
/// response from a superseded lease is refused as typed stale and changes
/// nothing.
pub fn validate_historical_data_mutation_response(
    observation: &ConnectorHistoricalDataMutationObservation,
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
    expected_raised_fence_digest: [u8; 32],
) -> Result<(), ConnectorError> {
    if observation.raised_fence_digest != expected_raised_fence_digest {
        return Err(ConnectorError::external_fence(
            ConnectorExternalFenceFailure::Stale,
            "historical data mutation response was produced under a superseded external fence",
        ));
    }
    if observation.source_scope != descriptor.source_scope {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "historical data mutation response answers another immutable source scope",
        ));
    }
    observation.validate_for(descriptor)
}

// ---------------------------------------------------------------------------
// Cleanup retention
// ---------------------------------------------------------------------------

/// What one guarded-cleanup attempt means for the retained cleanup obligation.
///
/// A pending cleanup outcome survives until it completes or is explicitly kept
/// for manual retention, even after the user-visible statement result has
/// already become terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalDataMutationCleanupProgress {
    pub state: DmlHistoricalCleanupState,
    /// Present when the cleanup result was lost. It is the only legal input to
    /// `reconcile_cleanup`, and it stays opaque.
    pub unresolved_evidence: Option<ExternalMutationEvidence>,
}

impl HistoricalDataMutationCleanupProgress {
    pub fn of(
        outcome: &ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
    ) -> Self {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { finalization, .. } => Self {
                state: if matches!(finalization, ExternalMutationFinalization::Complete) {
                    DmlHistoricalCleanupState::Completed
                } else {
                    DmlHistoricalCleanupState::Pending
                },
                unresolved_evidence: None,
            },
            ExternalMutationOutcome::KnownUncommitted { .. } => Self {
                state: DmlHistoricalCleanupState::Pending,
                unresolved_evidence: None,
            },
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => Self {
                state: DmlHistoricalCleanupState::Pending,
                unresolved_evidence: Some(evidence.clone()),
            },
        }
    }

    pub const fn is_pending(&self) -> bool {
        matches!(self.state, DmlHistoricalCleanupState::Pending)
    }
}

// ---------------------------------------------------------------------------
// Provider facet resolution
// ---------------------------------------------------------------------------

/// One resolved historical data-mutation recovery facet, with the control
/// generation it belongs to kept alive for as long as the facet is used.
pub struct HistoricalDataMutationRecoveryHandle {
    provider_id: String,
    recovery: Arc<dyn ConnectorHistoricalDataMutationRecovery>,
    _retained: Option<ConnectorControlPlanningLease>,
}

impl HistoricalDataMutationRecoveryHandle {
    pub fn new(
        provider_id: String,
        recovery: Arc<dyn ConnectorHistoricalDataMutationRecovery>,
    ) -> Self {
        Self {
            provider_id,
            recovery,
            _retained: None,
        }
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub fn facet(&self) -> &dyn ConnectorHistoricalDataMutationRecovery {
        self.recovery.as_ref()
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        self.recovery.binding_key()
    }
}

/// Narrow port that resolves the current generation's historical data-mutation
/// recovery facet for one connector instance.
///
/// The facet is installed separately from the ordinary data-mutation
/// capability, so a provider that owns TRUNCATE and ADD FILES without owning
/// historical recovery resolves to `Unsupported` here rather than silently
/// falling back to an ordinary `execute` or `reconcile`.
pub trait HistoricalDataMutationRecoveryResolver: Send + Sync {
    fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<HistoricalDataMutationRecoveryHandle, ConnectorError>;
}

struct ControlRegistryResolver {
    registry: Arc<dyn ConnectorControlRegistry>,
}

impl HistoricalDataMutationRecoveryResolver for ControlRegistryResolver {
    fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<HistoricalDataMutationRecoveryHandle, ConnectorError> {
        let lease = self.registry.acquire_current(instance_id)?;
        let recovery = lease
            .binding()
            .historical_data_mutation_recovery()
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no historical data mutation recovery capability",
                )
            })?;
        let provider_id = lease
            .binding()
            .descriptor()
            .provider_id
            .as_str()
            .to_string();
        Ok(HistoricalDataMutationRecoveryHandle {
            provider_id,
            recovery,
            _retained: Some(lease),
        })
    }
}

/// Build the production resolver from the frontend-owned control registry.
pub fn control_registry_resolver(
    registry: Arc<dyn ConnectorControlRegistry>,
) -> Arc<dyn HistoricalDataMutationRecoveryResolver> {
    Arc::new(ControlRegistryResolver { registry })
}

// ---------------------------------------------------------------------------
// Durable ledger seam
// ---------------------------------------------------------------------------

/// One statement-lifecycle mutation the profile wants to publish.
///
/// An ADD FILES source action always travels with the operation change it
/// belongs to, so ownership can only ever move inside the one fenced
/// transaction that also validates the immutable source-scope digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatementOutcomeMutation {
    pub state: OperationState,
    pub payload: OperationPayload,
    pub source_action: Option<AddFilesSourceAction>,
}

/// The durable half of one claimed operation, as the profile needs it.
///
/// Every method here is a fenced StateStore mutation or a read; none of them may
/// be held open across a provider call. The seam exists so the convergence
/// order can be verified against a recording ledger without a StateStore.
pub(crate) trait StatementRecoveryLedger {
    fn stored(&self) -> &StoredOperation;

    /// Re-assert that this owner still holds the exact operation lease.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn check_authority(&self) -> Result<(), DmlError>;

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn load_direct_mutation_fence(
        &self,
    ) -> Result<Option<DmlDirectMutationFenceReceiptRecord>, DmlError>;

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn load_recovery(&self) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError>;

    /// The opaque SPI evidence wire the historical attempt journalled when its
    /// commit outcome was unknown, if it has any.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn load_evidence_wire(&self) -> Result<Option<Vec<u8>>, DmlError>;

    /// Mint this recovery attempt's external fence proposal from the *live*
    /// lease guard. CP-3A rule 3 forbids capturing a one-shot snapshot.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn external_fence_proposal(&self) -> Result<DmlExternalFenceProposal, DmlError>;

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn persist_recovery(
        &mut self,
        recovery: DmlHistoricalDataMutationRecoveryRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError>;

    /// Publish one statement outcome under the same authority, with the due the
    /// open recovery still requires.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn publish_statement_outcome(
        &mut self,
        outcome: StatementOutcomeMutation,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError>;

    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn reschedule(&mut self, recovery_due_at_ms: Option<i64>) -> Result<(), DmlError>;
}

impl StatementRecoveryLedger for ActiveDmlOperation {
    fn stored(&self) -> &StoredOperation {
        &self.stored
    }

    fn check_authority(&self) -> Result<(), DmlError> {
        self.check_before_dispatch()
    }

    fn load_direct_mutation_fence(
        &self,
    ) -> Result<Option<DmlDirectMutationFenceReceiptRecord>, DmlError> {
        self.journal.load_direct_mutation_fence(self.operation_id())
    }

    fn load_recovery(&self) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError> {
        self.journal
            .load_historical_data_mutation_recovery(self.operation_id())
    }

    fn load_evidence_wire(&self) -> Result<Option<Vec<u8>>, DmlError> {
        match &self.stored.payload {
            OperationPayload::TruncateLifecycle(record) => {
                let Some(encoded) = record
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.evidence.as_ref())
                else {
                    return Ok(None);
                };
                crate::dml::truncate::decode_truncate_evidence_hex(encoded)
                    .map(Some)
                    .map_err(DmlError::journal_corruption)
            }
            OperationPayload::AddFilesLifecycle(record) => {
                let Some(descriptor) = record.evidence_artifact.as_ref() else {
                    return Ok(None);
                };
                self.journal
                    .load_add_files_artifact(self.operation_id(), descriptor)
                    .map(|artifact| Some(artifact.bytes))
            }
            _ => Ok(None),
        }
    }

    fn external_fence_proposal(&self) -> Result<DmlExternalFenceProposal, DmlError> {
        self.external_fence()
    }

    fn persist_recovery(
        &mut self,
        recovery: DmlHistoricalDataMutationRecoveryRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let request = DmlHistoricalDataMutationRecoveryMutationRequest {
            operation_id: self.operation_id(),
            expected_revision: self.stored.revision,
            mutation_id: Uuid::now_v7(),
            recovery,
        };
        // Refuse a record this journal could never hold before any provider
        // action treats it as durable.
        self.journal
            .preflight_historical_data_mutation_recovery(&request)
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        self.stored = self
            .journal
            .record_historical_data_mutation_recovery_authorized(
                request,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    fn publish_statement_outcome(
        &mut self,
        outcome: StatementOutcomeMutation,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let operation = OperationMutationRequest {
            operation_id: self.operation_id(),
            expected_revision: self.stored.revision,
            mutation_id: Uuid::now_v7(),
            state: outcome.state,
            payload: outcome.payload,
        };
        // The due is supplied explicitly rather than derived from the operation
        // alone: only this profile can see the open CP-3C recovery record that
        // keeps the bounded scan alive, and the journal refuses to drop a due
        // while that record is open.
        self.stored = match outcome.source_action {
            None => self
                .journal
                .mutate_statement_operation_authorized(
                    operation,
                    recovery_due_at_ms,
                    self.journal_authority()?,
                )
                .map_err(|error| error.with_operation_id(self.operation_id()))?,
            Some(source_action) => self
                .journal
                .apply_add_files_mutation_authorized(
                    AddFilesMutationRequest {
                        operation,
                        artifacts: Vec::new(),
                        source_action: Some(source_action),
                    },
                    recovery_due_at_ms,
                    self.journal_authority()?,
                )
                .map_err(|error| error.with_operation_id(self.operation_id()))?,
        };
        Ok(())
    }

    fn reschedule(&mut self, recovery_due_at_ms: Option<i64>) -> Result<(), DmlError> {
        self.reschedule_recovery_due(recovery_due_at_ms)
    }
}

// ---------------------------------------------------------------------------
// Profile
// ---------------------------------------------------------------------------

/// What one bounded recovery cycle achieved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatementRecoveryProgress {
    /// This operation is not driven by the direct data-mutation profile.
    NotApplicable,
    /// The recovery record reached its terminal phase.
    Resolved,
    /// A proof-bound guarded cleanup is still outstanding and retained.
    CleanupPending,
    /// The provider signed a continuation for a proven not-applied operation
    /// whose journal also proves nothing was dispatched. The profile stores it
    /// and does not resume the statement: a continuation authorizes a *new*
    /// coordination attempt of the same durable operation through the ordinary
    /// current-generation path, never a replay of the old prepared handle.
    ContinuationPending,
    /// This recovery attempt was superseded by another authority.
    Superseded,
    /// Evidence was insufficient. The due moved; nothing was concluded.
    Unresolved,
}

pub(crate) struct StatementRecoveryProfile {
    resolver: Arc<dyn HistoricalDataMutationRecoveryResolver>,
}

impl StatementRecoveryProfile {
    pub(crate) fn new(resolver: Arc<dyn HistoricalDataMutationRecoveryResolver>) -> Self {
        Self { resolver }
    }

    /// Drive one bounded historical direct data-mutation recovery cycle.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn drive(
        &self,
        ledger: &mut dyn StatementRecoveryLedger,
        now_ms: i64,
    ) -> Result<StatementRecoveryProgress, DmlError> {
        let operation_id = ledger.stored().operation_id;
        let Some(kind) = direct_mutation_kind(ledger.stored().operation_kind) else {
            return Ok(StatementRecoveryProgress::NotApplicable);
        };
        if !matches!(
            (kind, &ledger.stored().payload),
            (
                DmlDirectMutationKind::Truncate,
                OperationPayload::TruncateLifecycle(_)
            ) | (
                DmlDirectMutationKind::AddFiles,
                OperationPayload::AddFilesLifecycle(_)
            )
        ) {
            return Ok(StatementRecoveryProgress::NotApplicable);
        }
        // A provider is about to be asked to change external truth; prove this
        // owner still holds the exact operation lease first.
        ledger.check_authority()?;

        let durable = ledger.load_recovery()?;
        if durable
            .as_ref()
            .is_some_and(|record| record.phase == DmlHistoricalRecoveryPhase::Resolved)
        {
            // A resolved record is never reopened. The operation may still need
            // an operator (a partially applied ADD FILES, for example), so keep
            // it visible without touching the provider again.
            return self.park(ledger, now_ms, StatementRecoveryProgress::Resolved);
        }

        let mut facts = match self.historical_facts(ledger, kind, durable.as_ref())? {
            Ok(facts) => facts,
            Err(reason) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    reason = %reason,
                    "historical data mutation recovery has insufficient durable evidence; rescheduling"
                );
                return self.park(ledger, now_ms, StatementRecoveryProgress::Unresolved);
            }
        };

        // D2 step 2: the immutable request must be durable before any external
        // fence is raised. A later cycle finds it already durable and reuses it
        // verbatim, which is what makes repeated inspection legal.
        let mut cycle = match &durable {
            None => {
                let record = DmlHistoricalDataMutationRecoveryRecord {
                    codec_version: DML_HISTORICAL_DATA_MUTATION_RECOVERY_CODEC_VERSION,
                    phase: DmlHistoricalRecoveryPhase::Requested,
                    recovery_attempt_id: facts.recovery_attempt_id,
                    recovery_cycle: 1,
                    request: facts.request.clone(),
                    raised_fence: None,
                    result: None,
                    next_action: StatementNextAction::Reconcile,
                    requested_at_ms: now_ms,
                    updated_at_ms: now_ms,
                };
                self.persist(
                    ledger,
                    record,
                    now_ms,
                    DML_STATEMENT_RECOVERY_UNRESOLVED_DELAY_MS,
                )?
            }
            Some(existing) => existing.clone(),
        };

        // D2 step 3: close the historical authority before concluding anything.
        let context = request_context().map_err(DmlError::journal_unavailable)?;
        let handle = match self.resolver.resolve(&facts.table.instance_id) {
            Ok(handle) => handle,
            Err(error) => {
                let progress = self.classify_provider_failure(operation_id, "resolve", &error);
                return self.park(ledger, now_ms, progress);
            }
        };
        let raise_receipt = match handle.facet().raise_external_fence(
            ConnectorHistoricalDataMutationFenceRaiseRequest {
                historical_binding: facts.historical_binding.clone(),
                family: facts.family,
                observed: facts.historical_fence.clone(),
                raised: facts.raised_fence.clone(),
                context: context.clone(),
            },
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                let progress = self.classify_provider_failure(operation_id, "raise", &error);
                return self.park(ledger, now_ms, progress);
            }
        };
        if !raise_receipt.matches(&facts.raised_fence) {
            tracing::warn!(
                operation_id = %operation_id,
                "historical data mutation fence raise receipt acknowledges another fence; rescheduling"
            );
            return self.park(ledger, now_ms, StatementRecoveryProgress::Unresolved);
        }

        // The descriptor can only be sealed now: it binds the receipt digest of
        // the raise, which did not exist a moment ago. Sealing it earlier would
        // have bound a digest nothing had produced.
        facts.raised_fence_receipt_digest = raise_receipt.digest();
        let descriptor = match self.descriptor(&facts) {
            Ok(descriptor) => descriptor,
            Err(error) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    error = %error,
                    "historical data mutation recovery cannot seal its descriptor; rescheduling"
                );
                return self.park(ledger, now_ms, StatementRecoveryProgress::Unresolved);
            }
        };

        // Publish the raised fence before the inspection that depends on it, so
        // a crash in between can never leave a classification whose fence has
        // no durable proof.
        let raised_record = raised_fence_record(&facts, &raise_receipt, now_ms)?;
        let carried_result = cycle.result.clone();
        let post_raise_phase = phase_after_raise(carried_result.as_ref());
        cycle = self.persist(
            ledger,
            DmlHistoricalDataMutationRecoveryRecord {
                phase: post_raise_phase,
                recovery_attempt_id: facts.recovery_attempt_id,
                recovery_cycle: next_cycle(&cycle, facts.recovery_attempt_id),
                raised_fence: Some(raised_record),
                result: carried_result,
                next_action: next_action_for(post_raise_phase, None),
                updated_at_ms: now_ms,
                ..cycle
            },
            now_ms,
            DML_STATEMENT_RECOVERY_UNRESOLVED_DELAY_MS,
        )?;

        // D2 step 4: classify outside every StateStore transaction.
        let observation = match handle.facet().inspect(descriptor.clone(), context.clone()) {
            Ok(observation) => observation,
            Err(error) => {
                let progress = self.classify_provider_failure(operation_id, "inspect", &error);
                return self.park(ledger, now_ms, progress);
            }
        };
        if let Err(error) = validate_historical_data_mutation_response(
            &observation,
            &descriptor,
            descriptor.raised_fence.digest(),
        ) {
            // D5: a stale or crossed response changes nothing durable.
            let progress = self.classify_provider_failure(operation_id, "validate", &error);
            return self.park(ledger, now_ms, progress);
        }
        let outcome = HistoricalDataMutationOutcome::project(&observation);
        debug_assert!(outcome.answers(&descriptor));
        tracing::info!(
            operation_id = %operation_id,
            provider = handle.provider_id(),
            family = ?outcome.family,
            disposition = outcome.disposition.label(),
            cleanup_required = outcome.cleanup_required,
            determined = outcome.determined,
            "historical data mutation recovery classified a direct mutation"
        );

        // D2 step 5: publish the typed result before anything acts on it.
        let mut result = match historical_result_record(&facts, &observation, now_ms) {
            Ok(result) => result,
            Err(reason) => {
                // A result the journal could never hold is not a classification
                // the frontend may act on. Nothing is concluded.
                tracing::warn!(
                    operation_id = %operation_id,
                    reason = %reason,
                    "historical data mutation result cannot be made durable; keeping the record unresolved"
                );
                return self.park(ledger, now_ms, StatementRecoveryProgress::Unresolved);
            }
        };
        let inspected_phase = inspected_phase_for(&outcome);
        cycle = self.persist(
            ledger,
            DmlHistoricalDataMutationRecoveryRecord {
                phase: inspected_phase,
                result: Some(result.clone()),
                next_action: next_action_for(inspected_phase, Some(result.cleanup)),
                updated_at_ms: now_ms,
                ..cycle
            },
            now_ms,
            DML_STATEMENT_RECOVERY_CLEANUP_DELAY_MS,
        )?;

        // D2 step 6: finalize, clean up, or stay unresolved.
        match outcome.disposition {
            ConnectorHistoricalDataMutationDisposition::Ambiguous
            | ConnectorHistoricalDataMutationDisposition::Unsupported => {
                return self.park(ledger, now_ms, StatementRecoveryProgress::Unresolved);
            }
            ConnectorHistoricalDataMutationDisposition::Conflict => {
                // A `Conflict` is a statement about *this* recovery attempt: a
                // newer authority owns the external fence. The old operation is
                // not settled by it, so nothing terminal is published and the
                // record stays open to be re-driven under that new authority.
                return self.park(ledger, now_ms, StatementRecoveryProgress::Superseded);
            }
            _ => {}
        }

        // The statement outcome is published before the record resolves: a
        // crash in between leaves an open recovery whose next cycle re-inspects
        // and finds the statement already terminal, never a terminal statement
        // with a silently dropped obligation.
        let due = Some(now_ms.saturating_add(DML_STATEMENT_RECOVERY_CLEANUP_DELAY_MS));
        for mutation in statement_outcome_mutations(&facts, ledger.stored(), &result, &outcome)? {
            // The recovery record is still open, so every step keeps an
            // explicit due: the journal refuses to drop one while a CP-3C
            // recovery has not resolved.
            ledger.publish_statement_outcome(mutation, due)?;
        }
        let mut progress = if outcome.continuation_digest.is_some() {
            StatementRecoveryProgress::ContinuationPending
        } else {
            StatementRecoveryProgress::Resolved
        };

        if outcome.cleanup_required {
            // Only an observation this provider issued authorizes a cleanup,
            // and it must be the exact object `inspect` returned. A process
            // restart between the two forces a new cycle, which re-inspects.
            match self.run_cleanup(&handle, &descriptor, &observation, &context) {
                Ok(cleanup) => {
                    result.cleanup = cleanup.state;
                    if cleanup.is_pending() {
                        progress = StatementRecoveryProgress::CleanupPending;
                    }
                }
                Err(error) => {
                    let failure = self.classify_provider_failure(operation_id, "cleanup", &error);
                    tracing::debug!(
                        operation_id = %operation_id,
                        error = %error,
                        "historical data mutation guarded cleanup did not complete; obligation retained"
                    );
                    result.cleanup = DmlHistoricalCleanupState::Pending;
                    progress = match failure {
                        StatementRecoveryProgress::Superseded => {
                            StatementRecoveryProgress::Superseded
                        }
                        _ => StatementRecoveryProgress::CleanupPending,
                    };
                }
            }
        }

        let final_phase = final_phase_for(progress, result.cleanup);
        let delay = delay_for(progress);
        self.persist(
            ledger,
            DmlHistoricalDataMutationRecoveryRecord {
                phase: final_phase,
                result: Some(result.clone()),
                next_action: next_action_for(final_phase, Some(result.cleanup)),
                updated_at_ms: now_ms,
                ..cycle
            },
            now_ms,
            delay,
        )?;
        Ok(progress)
    }

    /// Run the guarded cleanup, resolving a lost result from opaque evidence.
    fn run_cleanup(
        &self,
        handle: &HistoricalDataMutationRecoveryHandle,
        descriptor: &ConnectorHistoricalDataMutationDescriptor,
        observation: &ConnectorHistoricalDataMutationObservation,
        context: &ConnectorRequestContext,
    ) -> Result<HistoricalDataMutationCleanupProgress, ConnectorError> {
        let outcome = handle
            .facet()
            .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
                operation_id: descriptor.operation_id,
                descriptor_digest: descriptor.digest(),
                observation: observation.clone(),
                context: context.clone(),
            })?;
        let progress = HistoricalDataMutationCleanupProgress::of(&outcome);
        let Some(evidence) = progress.unresolved_evidence.clone() else {
            return Ok(progress);
        };
        // One bounded reconciliation attempt on the same opaque evidence. It is
        // the historical facet's own cleanup reconciliation, never an ordinary
        // data-mutation reconcile.
        match handle
            .facet()
            .reconcile_cleanup(descriptor.operation_id, evidence, context.clone())
        {
            Ok(resolved) => Ok(HistoricalDataMutationCleanupProgress::of(&resolved)),
            Err(_) => Ok(progress),
        }
    }

    /// Persist one recovery record with the due its obligations require.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn persist(
        &self,
        ledger: &mut dyn StatementRecoveryLedger,
        record: DmlHistoricalDataMutationRecoveryRecord,
        now_ms: i64,
        delay_ms: i64,
    ) -> Result<DmlHistoricalDataMutationRecoveryRecord, DmlError> {
        let due = recovery_due_for(ledger.stored(), &record, now_ms.saturating_add(delay_ms));
        ledger.persist_recovery(record.clone(), due)?;
        Ok(record)
    }

    /// Move the due and report progress without concluding anything.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn park(
        &self,
        ledger: &mut dyn StatementRecoveryLedger,
        now_ms: i64,
        progress: StatementRecoveryProgress,
    ) -> Result<StatementRecoveryProgress, DmlError> {
        let stored = ledger.stored().clone();
        let durable = ledger.load_recovery()?;
        let due = if operation_requires_recovery_scan_with_direct_mutation(
            stored.state,
            &stored.payload,
            None,
            durable.as_ref(),
        ) {
            Some(now_ms.saturating_add(delay_for(progress)))
        } else {
            None
        };
        if stored.recovery_due_at_ms != due {
            ledger.reschedule(due)?;
        }
        Ok(progress)
    }

    /// Classify one typed provider failure for logging and backoff.
    ///
    /// A typed external fence failure is terminal for this attempt: it is a
    /// fact about authority, never a transient condition, and it is never
    /// retried under the same fence. Unreadable external truth keeps the record
    /// unresolved. Only the remaining classes may be retried on a later cycle.
    fn classify_provider_failure(
        &self,
        operation_id: DmlOperationId,
        stage: &str,
        error: &ConnectorError,
    ) -> StatementRecoveryProgress {
        if let Some(failure) = error.external_fence_failure() {
            tracing::debug!(
                operation_id = %operation_id,
                stage,
                failure = ?failure,
                error = %error,
                "historical data mutation recovery hit a typed external fence failure; not retried in this cycle"
            );
            return match failure {
                ConnectorExternalFenceFailure::Superseded
                | ConnectorExternalFenceFailure::Stale
                | ConnectorExternalFenceFailure::ForeignOperation => {
                    StatementRecoveryProgress::Superseded
                }
                ConnectorExternalFenceFailure::NotEstablished => {
                    StatementRecoveryProgress::Unresolved
                }
            };
        }
        match error.kind() {
            ConnectorErrorKind::CorruptData => {
                tracing::warn!(
                    operation_id = %operation_id,
                    stage,
                    error = %error,
                    "historical data mutation recovery could not read external truth; keeping the record unresolved"
                );
                StatementRecoveryProgress::Superseded
            }
            _ => {
                tracing::debug!(
                    operation_id = %operation_id,
                    stage,
                    error = %error,
                    "historical data mutation recovery could not complete this cycle; rescheduling"
                );
                StatementRecoveryProgress::Unresolved
            }
        }
    }

    /// Rebuild every fact one historical inspection needs from durable state.
    ///
    /// `Ok(Err(reason))` means the frontend does not hold enough durable
    /// evidence to ask the provider anything. That is deliberately not an
    /// error: nothing has gone wrong, and nothing may be concluded either.
    #[allow(clippy::type_complexity)]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn historical_facts(
        &self,
        ledger: &dyn StatementRecoveryLedger,
        kind: DmlDirectMutationKind,
        durable: Option<&DmlHistoricalDataMutationRecoveryRecord>,
    ) -> Result<Result<HistoricalDataMutationFacts, String>, DmlError> {
        let stored = ledger.stored().clone();
        let fence_record = ledger.load_direct_mutation_fence()?;
        let evidence_wire = ledger.load_evidence_wire().unwrap_or_default();
        let proposal = ledger.external_fence_proposal()?;
        Ok(historical_data_mutation_facts(
            &stored,
            kind,
            fence_record.as_ref(),
            evidence_wire.as_deref(),
            &proposal,
            durable,
        ))
    }

    fn descriptor(
        &self,
        facts: &HistoricalDataMutationFacts,
    ) -> Result<ConnectorHistoricalDataMutationDescriptor, ConnectorError> {
        ConnectorHistoricalDataMutationDescriptor::try_new(
            ConnectorHistoricalDataMutationIdentity {
                historical_binding: facts.historical_binding.clone(),
                table: facts.table.clone(),
                target_ref: facts.target_ref.clone(),
                operation_id: facts.connector_operation_id,
                family: facts.family,
                request_digest: facts.request_digest,
                plan_digest: facts.plan_digest,
                state_digest: facts.state_digest,
                plan_summary: facts.plan_summary,
                source_scope: facts.source_scope,
            },
            ConnectorHistoricalDataMutationFenceFacts {
                historical_fence: facts.historical_fence.clone(),
                raised_fence: facts.raised_fence.clone(),
                raised_fence_receipt_digest: facts.raised_fence_receipt_digest,
            },
            facts.checkpoints.clone(),
            facts.evidence.clone(),
        )
    }
}

// ---------------------------------------------------------------------------
// Durable fact reconstruction
// ---------------------------------------------------------------------------

/// The durable statement lifecycle facts both families expose, projected onto
/// one shape so the profile never branches on the payload again.
struct StatementFacts {
    provider_id: Option<String>,
    instance_id: Option<String>,
    incarnation: Option<String>,
    target_ref: String,
    request_digest: Option<String>,
    plan_digest: Option<String>,
    state_digest: Option<String>,
    plan_summary: ConnectorDataMutationPlanSummary,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    source_scope_digest: Option<String>,
    source_ownership: SourceScopeOwnership,
    connector_operation_id: Uuid,
    dispatch_certainty: DmlHistoricalDispatchCertainty,
}

/// Every fact one historical inspection needs, rebuilt from durable state only.
struct HistoricalDataMutationFacts {
    kind: DmlDirectMutationKind,
    family: ConnectorHistoricalDataMutationFamily,
    table: ConnectorTableIdentity,
    target_ref: ConnectorWriteTargetRef,
    connector_operation_id: ConnectorMutationOperationId,
    historical_binding: ConnectorExecutionBindingKey,
    request_digest: [u8; 32],
    plan_digest: [u8; 32],
    state_digest: [u8; 32],
    plan_summary: ConnectorDataMutationPlanSummary,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    source_ownership: SourceScopeOwnership,
    historical_fence: ConnectorHistoricalDataMutationFence,
    raised_fence: ConnectorExternalOperationFence,
    raised_fence_receipt_digest: [u8; 32],
    resource_digest: String,
    checkpoints: Vec<ConnectorHistoricalDataMutationCheckpoint>,
    evidence: Option<ExternalMutationEvidence>,
    request: DmlHistoricalDataMutationRequestRecord,
    recovery_attempt_id: Uuid,
}

/// Rebuild the historical facts, proving every reconstructed identity against
/// the digests the durable records already sealed.
fn historical_data_mutation_facts(
    stored: &StoredOperation,
    kind: DmlDirectMutationKind,
    fence_record: Option<&DmlDirectMutationFenceReceiptRecord>,
    evidence_wire: Option<&[u8]>,
    proposal: &DmlExternalFenceProposal,
    durable: Option<&DmlHistoricalDataMutationRecoveryRecord>,
) -> Result<HistoricalDataMutationFacts, String> {
    let statement = statement_facts(stored, kind)?;
    let family = family_of(kind);
    let connector_operation_id =
        ConnectorMutationOperationId::from_bytes(*statement.connector_operation_id.as_bytes());
    let table = table_identity(&stored.target)?;
    let target_ref = ConnectorWriteTargetRef::parse(statement.target_ref.clone())
        .map_err(|error| error.to_string())?;
    let resource_digest = fenced_resource_digest(&table, &target_ref);

    let request_digest = required_digest(statement.request_digest.as_deref(), "request")?;
    let plan_digest = required_digest(statement.plan_digest.as_deref(), "plan")?;
    let state_digest = required_digest(statement.state_digest.as_deref(), "state")?;

    // A durable fence receipt is not required: an owner may crash after
    // planning and before it establishes any fence, and `NotEstablished` is a
    // provable historical state rather than a missing value.
    let historical_fence = match fence_record {
        None => ConnectorHistoricalDataMutationFence::NotEstablished,
        Some(record) => {
            if record.operation_kind != kind {
                return Err(
                    "the durable direct mutation fence receipt belongs to another family"
                        .to_string(),
                );
            }
            if record.mutation_operation_id() != statement.connector_operation_id {
                return Err(
                    "the durable direct mutation fence receipt names another mutation operation"
                        .to_string(),
                );
            }
            if record.source_scope_digest != statement.source_scope_digest {
                return Err(
                    "the durable direct mutation fence receipt binds another source scope"
                        .to_string(),
                );
            }
            historical_fence(record, connector_operation_id, &table, &target_ref)?
        }
    };

    // The raised fence is minted from the live lease of *this* recovery attempt
    // and must strictly supersede the historical one; otherwise the historical
    // authority is still able to execute and nothing may be classified.
    let raised_fence = proposal
        .seal(
            ConnectorWriteOperationId::from_bytes(connector_operation_id.to_bytes()),
            table.clone(),
            target_ref.clone(),
        )
        .map_err(|error| error.to_string())?;
    if let Some(historical) = historical_fence.fence()
        && !raised_fence
            .supersedes(historical)
            .map_err(|error| error.to_string())?
    {
        return Err(
            "this recovery attempt cannot raise an external fence strictly above the historical one"
                .to_string(),
        );
    }

    let evidence = evidence_wire
        .map(|wire| {
            ExternalMutationEvidence::try_from_wire_v1(wire).map_err(|error| {
                format!("durable direct mutation evidence wire is unusable: {error}")
            })
        })
        .transpose()?
        .filter(|evidence| {
            // Evidence that cannot be tied to this descriptor is a reason to
            // refuse a conclusion, never a reason to widen one, so a mismatch
            // is dropped rather than sent.
            evidence.operation_id() == connector_operation_id
                && evidence.operation_kind() == family.operation_kind()
        });
    // The lifecycle records the connector instance the plan was produced
    // against. It must be the instance the fenced resource names; a disagreeing
    // pair means the frontend cannot name the historical owner at all.
    if let Some(instance_id) = statement.instance_id.as_deref()
        && instance_id != table.instance_id.as_str()
    {
        return Err(
            "the durable direct mutation names another connector instance than its target"
                .to_string(),
        );
    }
    let historical_binding = historical_binding(&table, statement.incarnation.as_deref());
    let request = match durable {
        // A durable request is immutable: a later cycle inspects exactly the
        // same immutable input, so it is reused verbatim rather than rederived
        // from a lifecycle that may have moved on.
        Some(existing) => existing.request.clone(),
        None => historical_request_record(kind, &statement, fence_record, &historical_binding)?,
    };
    if request.mutation_operation_id != statement.connector_operation_id {
        return Err(
            "the durable historical data mutation request names another mutation operation"
                .to_string(),
        );
    }
    if request.source_scope_digest != statement.source_scope_digest {
        return Err(
            "the durable historical data mutation request binds another immutable source scope"
                .to_string(),
        );
    }

    Ok(HistoricalDataMutationFacts {
        kind,
        family,
        table,
        target_ref,
        connector_operation_id,
        historical_binding: request_binding(&request)?,
        request_digest,
        plan_digest,
        state_digest,
        plan_summary: statement.plan_summary,
        source_scope: statement.source_scope,
        source_ownership: statement.source_ownership,
        historical_fence,
        raised_fence,
        raised_fence_receipt_digest: [0; 32],
        resource_digest,
        checkpoints: checkpoints_from_request(&request),
        evidence,
        recovery_attempt_id: proposal.coordination_attempt_id(),
        request,
    })
}

/// Project one durable statement lifecycle payload onto the shared shape.
fn statement_facts(
    stored: &StoredOperation,
    kind: DmlDirectMutationKind,
) -> Result<StatementFacts, String> {
    match (kind, &stored.payload) {
        (DmlDirectMutationKind::Truncate, OperationPayload::TruncateLifecycle(record)) => {
            Ok(StatementFacts {
                provider_id: record.provider_id.clone(),
                instance_id: record.connector_instance_id.clone(),
                incarnation: record.connector_incarnation.clone(),
                target_ref: record.target_ref.clone(),
                request_digest: record.request_digest.clone(),
                plan_digest: record.plan_digest.clone(),
                state_digest: record.state_digest.clone(),
                plan_summary: plan_summary(record.plan_summary.as_ref())?,
                source_scope: None,
                source_scope_digest: None,
                source_ownership: SourceScopeOwnership::Unclaimed,
                connector_operation_id: record.connector_operation_id,
                dispatch_certainty: truncate_dispatch_certainty(record.phase),
            })
        }
        (DmlDirectMutationKind::AddFiles, OperationPayload::AddFilesLifecycle(record)) => {
            Ok(StatementFacts {
                provider_id: record.provider_id.clone(),
                instance_id: record.connector_instance_id.clone(),
                incarnation: record.connector_incarnation.clone(),
                // ADD FILES always targets main; it has no branch-qualified
                // form, so the fenced resource is derived, never guessed.
                target_ref: ConnectorWriteTargetRef::main().as_str().to_string(),
                request_digest: record.request_digest.clone(),
                plan_digest: record.plan_digest.clone(),
                state_digest: record.state_digest.clone(),
                plan_summary: plan_summary(record.plan_summary.as_ref())?,
                source_scope: Some(source_scope(record)?),
                source_scope_digest: record.source_scope_digest.clone(),
                source_ownership: record.source_ownership,
                connector_operation_id: record.connector_operation_id,
                dispatch_certainty: add_files_dispatch_certainty(record),
            })
        }
        _ => Err("durable direct mutation operation has the wrong payload kind".to_string()),
    }
}

fn plan_summary(
    summary: Option<&crate::dml::model::DurableMutationSummary>,
) -> Result<ConnectorDataMutationPlanSummary, String> {
    let Some(summary) = summary else {
        return Err("the durable direct mutation has no plan summary".to_string());
    };
    ConnectorDataMutationPlanSummary::try_new(
        summary.file_count,
        summary.row_count,
        summary.total_bytes,
    )
    .map_err(|error| error.to_string())
}

fn source_scope(
    record: &AddFilesLifecycleRecord,
) -> Result<ConnectorDataMutationSourceScope, String> {
    let version = record
        .source_scope_version
        .ok_or_else(|| "the durable ADD FILES record has no source scope version".to_string())?;
    if version != ConnectorDataMutationSourceScope::VERSION {
        return Err(format!(
            "unsupported durable ADD FILES source scope version: {version}"
        ));
    }
    match record.source_scope_kind.as_deref() {
        Some("DIRECTORY") => {}
        Some(kind) => return Err(format!("unsupported ADD FILES source scope kind: {kind}")),
        None => return Err("the durable ADD FILES record has no source scope kind".to_string()),
    }
    let digest = required_digest(record.source_scope_digest.as_deref(), "source scope")?;
    let scope = ConnectorDataMutationSourceScope::try_new_directory(digest)
        .map_err(|error| error.to_string())?;
    debug_assert_eq!(
        scope.kind(),
        ConnectorDataMutationSourceScopeKind::Directory
    );
    Ok(scope)
}

/// Rebuild the historical fence value and prove it against the sealed digests.
///
/// Comparing the reconstructed value against the durable digest is what proves
/// the reconstructed table and target ref are the exact fenced resource. A
/// mismatch means the frontend cannot name what was fenced, and guessing is
/// forbidden.
fn historical_fence(
    record: &DmlDirectMutationFenceReceiptRecord,
    operation_id: ConnectorMutationOperationId,
    table: &ConnectorTableIdentity,
    target_ref: &ConnectorWriteTargetRef,
) -> Result<ConnectorHistoricalDataMutationFence, String> {
    let value = ConnectorExternalOperationFence::try_new(
        ConnectorClusterIdentity::try_from_digest(digest_bytes(
            &record.fence.identity.cluster_identity_digest,
            "cluster identity",
        )?)
        .map_err(|error| error.to_string())?,
        ConnectorExternalFenceGeneration::try_new(
            record.fence.identity.generation.control_plane_incarnation,
            record.fence.identity.generation.resource_epoch,
            record.fence.identity.generation.fence_generation,
        )
        .map_err(|error| error.to_string())?,
        ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
        *record.fence.identity.coordination_attempt_id.as_bytes(),
        table.clone(),
        target_ref.clone(),
    )
    .map_err(|error| error.to_string())?;
    if hex::encode(value.digest()) != record.fence.fence_digest {
        return Err(
            "the durable direct mutation fence receipt does not describe the reconstructed fenced resource"
                .to_string(),
        );
    }
    let receipt = ConnectorExternalFenceReceipt::try_new(
        &value,
        Bytes::from(record.fence.receipt_payload.as_bytes().to_vec()),
    )
    .map_err(|error| error.to_string())?;
    if hex::encode(receipt.digest()) != record.fence.receipt_digest {
        return Err(
            "the durable direct mutation fence receipt digest does not seal its payload"
                .to_string(),
        );
    }
    ConnectorHistoricalDataMutationFence::established(&receipt, value)
        .map_err(|error| error.to_string())
}

/// Build the immutable historical direct-mutation request record.
fn historical_request_record(
    kind: DmlDirectMutationKind,
    statement: &StatementFacts,
    fence_record: Option<&DmlDirectMutationFenceReceiptRecord>,
    historical_binding: &ConnectorExecutionBindingKey,
) -> Result<DmlHistoricalDataMutationRequestRecord, String> {
    let dispatch_certainty = statement.dispatch_certainty;
    let dispatched_at_ms =
        if dispatch_certainty == DmlHistoricalDispatchCertainty::ConfirmedNotDispatched {
            None
        } else {
            Some(0)
        };
    let mut request = DmlHistoricalDataMutationRequestRecord {
        old_provider_id: statement
            .provider_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        old_connector_instance_id: historical_binding.instance_id.as_str().to_string(),
        old_connector_incarnation: hex::encode(historical_binding.incarnation.to_bytes()),
        old_coordination_attempt_id: fence_record
            .map(|record| record.fence.identity.coordination_attempt_id),
        old_fence: fence_record.cloned(),
        operation_kind: kind,
        mutation_operation_id: statement.connector_operation_id,
        request_digest: String::new(),
        plan_digest: statement.plan_digest.clone(),
        state_digest: statement.state_digest.clone(),
        source_scope_digest: statement.source_scope_digest.clone(),
        dispatch_certainty,
        dispatched_at_ms,
    };
    request.request_digest = request_digest(&request)?;
    Ok(request)
}

/// The historical connector generation, taken from the durable statement
/// lifecycle when it recorded one.
///
/// The lifecycle stores the exact incarnation the plan was produced by as a
/// neutral hexadecimal string; reading it decodes no provider payload. An
/// operation that never reached planning leaves it unknown.
fn historical_binding(
    table: &ConnectorTableIdentity,
    incarnation: Option<&str>,
) -> ConnectorExecutionBindingKey {
    let mut bytes = UNKNOWN_HISTORICAL_INCARNATION;
    if let Some(incarnation) = incarnation {
        let mut decoded = [0u8; 16];
        if hex::decode_to_slice(incarnation, &mut decoded).is_ok() {
            bytes = decoded;
        }
    }
    ConnectorExecutionBindingKey {
        instance_id: table.instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes(bytes),
    }
}

fn request_binding(
    request: &DmlHistoricalDataMutationRequestRecord,
) -> Result<ConnectorExecutionBindingKey, String> {
    let mut incarnation = [0u8; 16];
    hex::decode_to_slice(&request.old_connector_incarnation, &mut incarnation)
        .map_err(|error| format!("historical connector incarnation is unusable: {error}"))?;
    Ok(ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(request.old_connector_instance_id.as_str())
            .map_err(|error| error.to_string())?,
        incarnation: ConnectorInstanceIncarnation::from_bytes(incarnation),
    })
}

/// Digest over the complete immutable request, excluding the digest field.
fn request_digest(request: &DmlHistoricalDataMutationRequestRecord) -> Result<String, String> {
    let mut hasher = Sha256::new();
    hasher.update(DML_HISTORICAL_DATA_MUTATION_REQUEST_DOMAIN);
    hasher.update(serde_json::to_vec(request).map_err(|error| error.to_string())?);
    Ok(hex::encode(hasher.finalize()))
}

/// Bounded digest of the fenced resource identity: the connector instance, the
/// table, and the write target ref the fence was minted for.
fn fenced_resource_digest(
    table: &ConnectorTableIdentity,
    target_ref: &ConnectorWriteTargetRef,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DML_EXTERNAL_FENCE_RESOURCE_DOMAIN);
    for component in [
        table.instance_id.as_str(),
        table.namespace.as_ref(),
        table.table.as_ref(),
        target_ref.as_str(),
    ] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Project the raised fence and its provider receipt into the durable record.
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn raised_fence_record(
    facts: &HistoricalDataMutationFacts,
    receipt: &ConnectorExternalFenceReceipt,
    now_ms: i64,
) -> Result<DmlDirectMutationFenceReceiptRecord, DmlError> {
    let generation = facts.raised_fence.generation();
    Ok(DmlDirectMutationFenceReceiptRecord {
        codec_version: DML_DIRECT_MUTATION_FENCE_CODEC_VERSION,
        operation_kind: facts.kind,
        fence: DmlExternalFenceReceiptRecord {
            codec_version: DML_EXTERNAL_FENCE_CODEC_VERSION,
            identity: DmlExternalFenceIdentity {
                cluster_identity_digest: hex::encode(facts.raised_fence.cluster().digest()),
                resource_digest: facts.resource_digest.clone(),
                write_operation_id: Uuid::from_bytes(facts.raised_fence.operation_id().to_bytes()),
                coordination_attempt_id: Uuid::from_bytes(
                    facts.raised_fence.coordination_attempt_id(),
                ),
                generation: DmlExternalFenceGeneration {
                    control_plane_incarnation: generation.control_plane_incarnation(),
                    resource_epoch: generation.resource_epoch(),
                    fence_generation: generation.coordination_attempt(),
                },
            },
            fence_digest: hex::encode(facts.raised_fence.digest()),
            receipt_digest: hex::encode(receipt.digest()),
            receipt_payload: DmlOpaquePayload::try_new(receipt.payload().to_vec())
                .map_err(DmlError::journal_corruption)?,
            established_at_ms: now_ms.max(0),
        },
        source_scope_digest: facts.request.source_scope_digest.clone(),
    })
}

fn table_identity(target: &OperationTarget) -> Result<ConnectorTableIdentity, String> {
    Ok(ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse(target.catalog.as_str())
            .map_err(|error| error.to_string())?,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    })
}

/// What the durable TRUNCATE lifecycle proves about the destructive dispatch.
///
/// The executing phase is journalled *before* the fence is established and the
/// destructive `execute` is dispatched, so only `Preparing` and `Planned`
/// prove nothing left the frontend. Everything from `Executing` on may have
/// produced an irreversible external effect, and nothing here may be softened
/// into "not dispatched": a plan failure and a dispatched-then-failed execute
/// share the same `Failed` phase.
const fn truncate_dispatch_certainty(
    phase: TruncateLifecyclePhase,
) -> DmlHistoricalDispatchCertainty {
    match phase {
        TruncateLifecyclePhase::Preparing | TruncateLifecyclePhase::Planned => {
            DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
        }
        TruncateLifecyclePhase::Committed => DmlHistoricalDispatchCertainty::ConfirmedDispatched,
        TruncateLifecyclePhase::Executing
        | TruncateLifecyclePhase::CommitUnknown
        | TruncateLifecyclePhase::Reconciling
        | TruncateLifecyclePhase::Failed => DmlHistoricalDispatchCertainty::PossiblyDispatched,
    }
}

/// What the durable ADD FILES lifecycle proves about the dispatch.
///
/// ADD FILES journals its own dispatch certainty before every step that can
/// reach the provider, so it is read rather than inferred.
const fn add_files_dispatch_certainty(
    record: &AddFilesLifecycleRecord,
) -> DmlHistoricalDispatchCertainty {
    match record.phase {
        AddFilesLifecyclePhase::Committed => DmlHistoricalDispatchCertainty::ConfirmedDispatched,
        _ => match record.dispatch_certainty {
            crate::dml::model::AddFilesDispatchCertainty::ConfirmedNotDispatched => {
                DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
            }
            crate::dml::model::AddFilesDispatchCertainty::PossiblyDispatched => {
                DmlHistoricalDispatchCertainty::PossiblyDispatched
            }
        },
    }
}

/// Project the immutable request onto the SPI dispatch checkpoints.
///
/// Deriving them from the durable request (not from the live operation state)
/// keeps the sealed request stable across recovery cycles even after the
/// statement result has been published. These checkpoints are the *only* input
/// to `journal_proves_nothing_dispatched`, which is the sole gate for a
/// provider continuation, so `NotDispatched` is stamped only when the journal
/// genuinely proves it.
fn checkpoints_from_request(
    request: &DmlHistoricalDataMutationRequestRecord,
) -> Vec<ConnectorHistoricalDataMutationCheckpoint> {
    let confirmed_not_dispatched =
        request.dispatch_certainty == DmlHistoricalDispatchCertainty::ConfirmedNotDispatched;
    let dispatched = match request.dispatch_certainty {
        DmlHistoricalDispatchCertainty::ConfirmedNotDispatched => {
            ConnectorHistoricalDataMutationDispatchState::NotDispatched
        }
        DmlHistoricalDispatchCertainty::PossiblyDispatched => {
            ConnectorHistoricalDataMutationDispatchState::Unknown
        }
        DmlHistoricalDispatchCertainty::ConfirmedDispatched => {
            ConnectorHistoricalDataMutationDispatchState::Dispatched
        }
    };
    let completed = match request.dispatch_certainty {
        DmlHistoricalDispatchCertainty::ConfirmedNotDispatched => {
            ConnectorHistoricalDataMutationDispatchState::NotDispatched
        }
        DmlHistoricalDispatchCertainty::PossiblyDispatched
        | DmlHistoricalDispatchCertainty::ConfirmedDispatched => {
            ConnectorHistoricalDataMutationDispatchState::Unknown
        }
    };
    vec![
        ConnectorHistoricalDataMutationCheckpoint {
            phase: ConnectorHistoricalDataMutationPhase::Prepared,
            state: ConnectorHistoricalDataMutationDispatchState::Completed,
            evidence_digest: None,
        },
        ConnectorHistoricalDataMutationCheckpoint {
            phase: ConnectorHistoricalDataMutationPhase::Planned,
            state: if request.plan_digest.is_some() {
                ConnectorHistoricalDataMutationDispatchState::Completed
            } else if confirmed_not_dispatched {
                ConnectorHistoricalDataMutationDispatchState::NotDispatched
            } else {
                ConnectorHistoricalDataMutationDispatchState::Unknown
            },
            evidence_digest: None,
        },
        ConnectorHistoricalDataMutationCheckpoint {
            phase: ConnectorHistoricalDataMutationPhase::FenceEstablished,
            state: if request.old_fence.is_some() {
                ConnectorHistoricalDataMutationDispatchState::Completed
            } else {
                ConnectorHistoricalDataMutationDispatchState::NotDispatched
            },
            evidence_digest: None,
        },
        ConnectorHistoricalDataMutationCheckpoint {
            phase: ConnectorHistoricalDataMutationPhase::ExecuteDispatched,
            state: dispatched,
            evidence_digest: None,
        },
        ConnectorHistoricalDataMutationCheckpoint {
            phase: ConnectorHistoricalDataMutationPhase::ExecuteCompleted,
            state: completed,
            evidence_digest: None,
        },
    ]
}

fn required_digest(value: Option<&str>, label: &str) -> Result<[u8; 32], String> {
    let Some(value) = value else {
        return Err(format!("the durable direct mutation has no {label} digest"));
    };
    digest_bytes(value, label)
}

fn digest_bytes(value: &str, label: &str) -> Result<[u8; 32], String> {
    let mut digest = [0u8; 32];
    hex::decode_to_slice(value, &mut digest)
        .map_err(|error| format!("DML {label} digest is unusable: {error}"))?;
    Ok(digest)
}

// ---------------------------------------------------------------------------
// Result and statement projection
// ---------------------------------------------------------------------------

const fn disposition_record(
    disposition: ConnectorHistoricalDataMutationDisposition,
) -> DmlHistoricalDataMutationDisposition {
    match disposition {
        ConnectorHistoricalDataMutationDisposition::Applied => {
            DmlHistoricalDataMutationDisposition::Applied
        }
        ConnectorHistoricalDataMutationDisposition::NotApplied => {
            DmlHistoricalDataMutationDisposition::NotApplied
        }
        ConnectorHistoricalDataMutationDisposition::CleanupRequired => {
            DmlHistoricalDataMutationDisposition::CleanupRequired
        }
        ConnectorHistoricalDataMutationDisposition::PartiallyApplied => {
            DmlHistoricalDataMutationDisposition::PartiallyApplied
        }
        ConnectorHistoricalDataMutationDisposition::Conflict => {
            DmlHistoricalDataMutationDisposition::Conflict
        }
        ConnectorHistoricalDataMutationDisposition::Ambiguous => {
            DmlHistoricalDataMutationDisposition::Ambiguous
        }
        ConnectorHistoricalDataMutationDisposition::Unsupported => {
            DmlHistoricalDataMutationDisposition::Unsupported
        }
    }
}

/// Whether this outcome may free the ADD FILES source-scope reservation.
///
/// The provider condition is necessary but never sufficient: the reservation
/// must also still be exactly the immutable one this operation reserved, and
/// the actual release happens only inside the fenced journal transaction that
/// re-validates that digest.
fn releases_source_scope(
    facts: &HistoricalDataMutationFacts,
    outcome: &HistoricalDataMutationOutcome,
) -> bool {
    facts.kind.binds_source_scope()
        && outcome.disposition.permits_source_scope_release()
        && matches!(
            facts.source_ownership,
            SourceScopeOwnership::ReservedImmutable | SourceScopeOwnership::Frozen
        )
        && outcome.source_scope_digest
            == facts
                .source_scope
                .map(ConnectorDataMutationSourceScope::digest)
}

/// Project one provider observation into the durable typed result.
fn historical_result_record(
    facts: &HistoricalDataMutationFacts,
    observation: &ConnectorHistoricalDataMutationObservation,
    now_ms: i64,
) -> Result<DmlHistoricalDataMutationResultRecord, String> {
    let outcome = HistoricalDataMutationOutcome::project(observation);
    let proof_payload = DmlOpaquePayload::try_new(observation.proof.payload().to_vec())?;
    let continuation_payload = observation
        .continuation
        .as_ref()
        .map(|continuation| DmlOpaquePayload::try_new(continuation.payload().to_vec()))
        .transpose()?;
    let retained = if facts.kind.binds_source_scope() {
        !releases_source_scope(facts, &outcome)
    } else {
        false
    };
    Ok(DmlHistoricalDataMutationResultRecord {
        disposition: disposition_record(outcome.disposition),
        observation_digest: hex::encode(outcome.observation_digest),
        source_scope_digest: facts.request.source_scope_digest.clone(),
        evidence_payload: None,
        proof_payload: Some(proof_payload),
        continuation_payload,
        cleanup: cleanup_state(&outcome),
        source_scope_retained: retained,
        failure: historical_failure_record(outcome.disposition),
        observed_at_ms: now_ms.max(0),
    })
}

/// The cleanup obligation one classification carries.
///
/// A partially applied mutation has, by definition, no proof about which
/// artifacts belong only to it, so the provider refuses to request an automatic
/// cleanup for it. The obligation is nevertheless real: it is handed to an
/// operator rather than declared absent.
const fn cleanup_state(outcome: &HistoricalDataMutationOutcome) -> DmlHistoricalCleanupState {
    if outcome.cleanup_required {
        DmlHistoricalCleanupState::Pending
    } else if matches!(
        outcome.disposition,
        ConnectorHistoricalDataMutationDisposition::PartiallyApplied
    ) {
        DmlHistoricalCleanupState::ManualRetention
    } else {
        DmlHistoricalCleanupState::NotRequired
    }
}

/// Keep a typed conflict classification. It is never widened into an unknown
/// outcome and never softened into an unsupported one.
fn historical_failure_record(
    disposition: ConnectorHistoricalDataMutationDisposition,
) -> Option<ConnectorWriteFailureRecord> {
    match disposition {
        ConnectorHistoricalDataMutationDisposition::Conflict => Some(ConnectorWriteFailureRecord {
            kind: ConnectorWriteFailureKind::Conflict,
            message: "another authority advanced the external fence past this recovery attempt"
                .to_string(),
        }),
        _ => None,
    }
}

/// The statement lifecycle mutations one determined disposition authorizes.
///
/// An empty result means nothing may be published: the durable statement
/// result already answers this operation, or the disposition proves nothing
/// one-way about it.
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn statement_outcome_mutations(
    facts: &HistoricalDataMutationFacts,
    stored: &StoredOperation,
    result: &DmlHistoricalDataMutationResultRecord,
    outcome: &HistoricalDataMutationOutcome,
) -> Result<Vec<StatementOutcomeMutation>, DmlError> {
    let note = format!(
        "recovered by historical data mutation inspection: {} (observation {})",
        outcome.disposition.label(),
        result.observation_digest
    );
    let (target, fact) = match outcome.disposition {
        ConnectorHistoricalDataMutationDisposition::Applied => (
            OperationState::Committed,
            DurableExternalFact {
                outcome: ExternalFactOutcome::KnownCommitted,
                // The provider receipt is a neutral SPI value the frontend
                // cannot re-encode into a family receipt without decoding it,
                // so only its sealed observation digest is retained.
                receipt: None,
                evidence: None,
                finalization_failure: match outcome.finalization_complete {
                    Some(false) => Some(note.clone()),
                    _ => None,
                },
                failure: Some(note.clone()),
            },
        ),
        // Spec D3: a cleanup-required historical mutation did not apply. It is
        // terminal and is never a retry signal; the cleanup obligation lives on
        // the recovery record, not on the statement result.
        ConnectorHistoricalDataMutationDisposition::NotApplied
        | ConnectorHistoricalDataMutationDisposition::CleanupRequired => (
            OperationState::FailedKnownUncommitted,
            DurableExternalFact {
                outcome: ExternalFactOutcome::KnownUncommitted,
                receipt: None,
                evidence: None,
                finalization_failure: None,
                failure: Some(note.clone()),
            },
        ),
        // Determined but not one-way: an operator has to look. The state is
        // deliberately left where it is.
        ConnectorHistoricalDataMutationDisposition::PartiallyApplied => (
            stored.state,
            DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: None,
                finalization_failure: None,
                failure: Some(note.clone()),
            },
        ),
        _ => return Ok(Vec::new()),
    };
    // A state the durable lifecycle cannot reach is never forced: the record is
    // left exactly where it is and handed to an operator instead.
    let manual = outcome.disposition
        == ConnectorHistoricalDataMutationDisposition::PartiallyApplied
        || validate_operation_transition(stored.state, target).is_err();
    let state = if manual { stored.state } else { target };
    let finalization = finalization_states(outcome, manual);
    let next_action = if manual {
        StatementNextAction::ManualInspect
    } else if finalization
        .last()
        .is_some_and(|state| *state == OperationState::FinalizeFailedKnownCommitted)
    {
        StatementNextAction::RetryFinalize
    } else {
        StatementNextAction::None
    };
    let payload = statement_payload(facts, stored, state, next_action, fact.clone())?;
    if payload == stored.payload && state == stored.state && finalization.is_empty() {
        return Ok(Vec::new());
    }
    let mut mutations = vec![StatementOutcomeMutation {
        state,
        payload,
        // The source action travels with the first mutation, so ownership can
        // only ever move together with the outcome that authorized it.
        source_action: source_action(facts, stored, result, outcome),
    }];
    // An applied mutation still has to finish finalizing: leaving it in
    // `Committed` would keep an already-settled operation permanently due.
    for state in finalization {
        let payload = statement_payload(facts, stored, state, next_action, fact.clone())?;
        mutations.push(StatementOutcomeMutation {
            state,
            payload,
            source_action: None,
        });
    }
    Ok(mutations)
}

/// The durable finalization the recovered commit still owes.
///
/// The provider reported whether external finalization completed, so the
/// lifecycle is walked to the matching terminal state rather than left in
/// `Committed`. A failed finalization stays visible instead of being hidden.
fn finalization_states(
    outcome: &HistoricalDataMutationOutcome,
    manual: bool,
) -> Vec<OperationState> {
    if manual {
        return Vec::new();
    }
    match outcome.finalization_complete {
        Some(true) => vec![OperationState::Finalized],
        Some(false) => vec![
            OperationState::Finalizing,
            OperationState::FinalizeFailedKnownCommitted,
        ],
        None => Vec::new(),
    }
}

/// Rebuild the statement payload with the recovered outcome attached.
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn statement_payload(
    facts: &HistoricalDataMutationFacts,
    stored: &StoredOperation,
    state: OperationState,
    next_action: StatementNextAction,
    fact: DurableExternalFact,
) -> Result<OperationPayload, DmlError> {
    match (&facts.kind, &stored.payload) {
        (DmlDirectMutationKind::Truncate, OperationPayload::TruncateLifecycle(record)) => Ok(
            OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
                phase: match state {
                    OperationState::Committed
                    | OperationState::Finalizing
                    | OperationState::Finalized
                    | OperationState::FinalizeFailedKnownCommitted => {
                        TruncateLifecyclePhase::Committed
                    }
                    OperationState::FailedKnownUncommitted => TruncateLifecyclePhase::Failed,
                    _ => record.phase,
                },
                outcome: Some(fact),
                next_action,
                ..record.clone()
            }),
        ),
        (DmlDirectMutationKind::AddFiles, OperationPayload::AddFilesLifecycle(record)) => {
            let mut next = record.clone();
            next.phase = match state {
                OperationState::Committed
                | OperationState::Finalizing
                | OperationState::Finalized
                | OperationState::FinalizeFailedKnownCommitted => AddFilesLifecyclePhase::Committed,
                OperationState::FailedKnownUncommitted => AddFilesLifecyclePhase::Failed,
                _ => record.phase,
            };
            next.outcome = Some(fact);
            next.next_action = next_action;
            Ok(OperationPayload::AddFilesLifecycle(next))
        }
        _ => Err(DmlError::journal_corruption(
            "durable direct mutation operation has the wrong payload kind",
        )
        .with_operation_id(stored.operation_id)),
    }
}

/// The ADD FILES source-scope action this determined disposition authorizes.
///
/// The action always travels with the operation mutation, so ownership can only
/// move inside the fenced transaction that also re-validates the immutable
/// source-scope digest against the durable reservation.
fn source_action(
    facts: &HistoricalDataMutationFacts,
    stored: &StoredOperation,
    result: &DmlHistoricalDataMutationResultRecord,
    outcome: &HistoricalDataMutationOutcome,
) -> Option<AddFilesSourceAction> {
    if result.source_scope_retained || !releases_source_scope(facts, outcome) {
        return None;
    }
    let (provider_id, scope_digest) = match &stored.payload {
        OperationPayload::AddFilesLifecycle(record) => (
            record.provider_id.clone()?,
            record.source_scope_digest.clone()?,
        ),
        _ => return None,
    };
    match outcome.disposition {
        // The provider proved external truth carries the whole sealed source
        // set, so the table owns it now.
        ConnectorHistoricalDataMutationDisposition::Applied => {
            Some(AddFilesSourceAction::Transition {
                provider_id,
                scope_digest,
                expected: facts.source_ownership,
                ownership: SourceScopeOwnership::TableOwned,
            })
        }
        // A proven not-applied operation never claimed the source, so the
        // reservation is freed. A frozen scope is deliberately not releasable:
        // it was frozen because an earlier attempt could not prove anything.
        ConnectorHistoricalDataMutationDisposition::NotApplied
            if facts.source_ownership == SourceScopeOwnership::ReservedImmutable =>
        {
            Some(AddFilesSourceAction::Release {
                provider_id,
                scope_digest,
            })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Phase and due arithmetic
// ---------------------------------------------------------------------------

/// The cycle a post-raise record belongs to.
///
/// A raise minted by a different coordination attempt opens a new cycle: one
/// cycle is owned by exactly one attempt, and a phase may never rewind inside
/// a cycle.
const fn next_cycle(existing: &DmlHistoricalDataMutationRecoveryRecord, attempt: Uuid) -> u32 {
    if existing.recovery_attempt_id.as_u128() == attempt.as_u128() {
        existing.recovery_cycle
    } else {
        existing.recovery_cycle.saturating_add(1)
    }
}

/// The phase a record reaches once this cycle has raised its external fence.
///
/// A carried result must keep its cleanup obligation visible, so a resumed
/// cycle over a pending cleanup stays in `CleanupPending` rather than
/// pretending it has no result yet.
const fn phase_after_raise(
    carried: Option<&DmlHistoricalDataMutationResultRecord>,
) -> DmlHistoricalRecoveryPhase {
    match carried {
        None => DmlHistoricalRecoveryPhase::FenceRaised,
        Some(result) => match result.cleanup {
            DmlHistoricalCleanupState::Pending => DmlHistoricalRecoveryPhase::CleanupPending,
            _ => DmlHistoricalRecoveryPhase::Inspected,
        },
    }
}

/// The phase that publishes one fresh classification.
///
/// Nothing terminal is claimed here: the record only becomes `Resolved` after
/// the statement outcome and the guarded cleanup have both been dealt with.
const fn inspected_phase_for(
    outcome: &HistoricalDataMutationOutcome,
) -> DmlHistoricalRecoveryPhase {
    if !outcome.determined {
        DmlHistoricalRecoveryPhase::Unresolved
    } else if outcome.cleanup_required {
        DmlHistoricalRecoveryPhase::CleanupPending
    } else {
        DmlHistoricalRecoveryPhase::Inspected
    }
}

const fn final_phase_for(
    progress: StatementRecoveryProgress,
    cleanup: DmlHistoricalCleanupState,
) -> DmlHistoricalRecoveryPhase {
    match cleanup {
        DmlHistoricalCleanupState::Pending => DmlHistoricalRecoveryPhase::CleanupPending,
        _ => match progress {
            StatementRecoveryProgress::Resolved => DmlHistoricalRecoveryPhase::Resolved,
            StatementRecoveryProgress::Unresolved => DmlHistoricalRecoveryPhase::Unresolved,
            _ => DmlHistoricalRecoveryPhase::Inspected,
        },
    }
}

/// The next action a non-terminal phase advertises.
///
/// Only a resolved record may advertise `None`, and a record kept for manual
/// retention must say so.
const fn next_action_for(
    phase: DmlHistoricalRecoveryPhase,
    cleanup: Option<DmlHistoricalCleanupState>,
) -> StatementNextAction {
    match phase {
        DmlHistoricalRecoveryPhase::Resolved => match cleanup {
            Some(DmlHistoricalCleanupState::ManualRetention) => StatementNextAction::ManualInspect,
            _ => StatementNextAction::None,
        },
        DmlHistoricalRecoveryPhase::CleanupPending => StatementNextAction::AbortStaging,
        _ => StatementNextAction::Reconcile,
    }
}

const fn delay_for(progress: StatementRecoveryProgress) -> i64 {
    match progress {
        StatementRecoveryProgress::CleanupPending => DML_STATEMENT_RECOVERY_CLEANUP_DELAY_MS,
        StatementRecoveryProgress::Superseded
        | StatementRecoveryProgress::ContinuationPending
        | StatementRecoveryProgress::Resolved
        | StatementRecoveryProgress::NotApplicable => DML_STATEMENT_RECOVERY_BLOCKED_DELAY_MS,
        StatementRecoveryProgress::Unresolved => DML_STATEMENT_RECOVERY_UNRESOLVED_DELAY_MS,
    }
}

/// The recovery due one record still requires.
///
/// The obligation is computed from the operation *and* its historical record,
/// so a pending cleanup or an open cycle cannot be dropped because the
/// user-visible statement result already became terminal (spec D5).
fn recovery_due_for(
    stored: &StoredOperation,
    record: &DmlHistoricalDataMutationRecoveryRecord,
    next_due_ms: i64,
) -> Option<i64> {
    if operation_requires_recovery_scan_with_direct_mutation(
        stored.state,
        &stored.payload,
        None,
        Some(record),
    ) {
        Some(next_due_ms.max(0))
    } else {
        None
    }
}

fn request_context() -> Result<ConnectorRequestContext, String> {
    ConnectorRequestContext::try_new(
        Instant::now() + DML_STATEMENT_RECOVERY_ACTION_DEADLINE,
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Whether this error means the profile lost its authority rather than hit a
/// transient failure. Used by the controller to decide how loudly to log.
pub(crate) fn is_authority_loss(error: &DmlError) -> bool {
    matches!(
        error.kind(),
        DmlErrorKind::CoordinationLost | DmlErrorKind::CoordinationContended
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorDataMutationReceipt,
        ConnectorHistoricalDataMutationApplication, ConnectorHistoricalDataMutationOutcomeFacts,
        ConnectorHistoricalDataMutationProof, ConnectorInstanceDescriptor,
        ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorProviderId,
        ExternalMutationEffect,
    };

    use super::*;
    use crate::dml::model::{
        AddFilesDispatchCertainty, DML_OPERATION_SCHEMA_VERSION, DurableMutationSummary,
        validate_historical_data_mutation_recovery_transition,
        validate_statement_operation_transition,
    };

    const CLUSTER: &str = "nova-cp3c-profile";
    const CATALOG: &str = "catalog.lake";
    const NAMESPACE: &str = "db";
    const TABLE: &str = "orders";
    const PROVIDER: &str = "iceberg";
    const HISTORICAL_INCARNATION: [u8; 16] = [4; 16];
    const CURRENT_INCARNATION: [u8; 16] = [9; 16];
    const PROVIDER_MARKER: &str = "PROVIDER-PRIVATE-BODY";
    const SOURCE_SCOPE_DIGEST: [u8; 32] = [6; 32];

    // -----------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------

    fn instance() -> ConnectorInstanceId {
        ConnectorInstanceId::parse(CATALOG).expect("instance id")
    }

    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: instance(),
            namespace: Arc::from(NAMESPACE),
            table: Arc::from(TABLE),
        }
    }

    fn mutation_operation_uuid() -> Uuid {
        Uuid::parse_str("018f0000-0000-7000-8000-0000000000cc").expect("uuid v7")
    }

    fn connector_operation_id() -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes(*mutation_operation_uuid().as_bytes())
    }

    fn recovery_attempt() -> Uuid {
        Uuid::parse_str("018f0000-0000-7000-8000-000000000002").expect("uuid v7")
    }

    fn historical_attempt() -> Uuid {
        Uuid::parse_str("018f0000-0000-7000-8000-000000000001").expect("uuid v7")
    }

    fn marker(fence: &ConnectorExternalOperationFence) -> Bytes {
        Bytes::from(format!(
            "{PROVIDER_MARKER}|marker|{}",
            hex::encode(fence.digest())
        ))
    }

    fn proposal(attempt: Uuid, epoch: u64, generation: u64) -> DmlExternalFenceProposal {
        DmlExternalFenceProposal::testing(
            DmlOperationId::from(operation_uuid()),
            CLUSTER,
            attempt,
            DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: epoch,
                fence_generation: generation,
            },
        )
        .expect("proposal")
    }

    fn sealed_fence(
        attempt: Uuid,
        epoch: u64,
        generation: u64,
        kind: DmlDirectMutationKind,
    ) -> ConnectorExternalOperationFence {
        proposal(attempt, epoch, generation)
            .seal(
                ConnectorWriteOperationId::from_bytes(connector_operation_id().to_bytes()),
                table(),
                target_ref(kind),
            )
            .expect("sealed fence")
    }

    fn target_ref(kind: DmlDirectMutationKind) -> ConnectorWriteTargetRef {
        match kind {
            DmlDirectMutationKind::Truncate => {
                ConnectorWriteTargetRef::parse("main".to_string()).expect("target ref")
            }
            DmlDirectMutationKind::AddFiles => ConnectorWriteTargetRef::main(),
        }
    }

    /// A durable fence receipt of the *historical* attempt.
    fn fence_record(kind: DmlDirectMutationKind) -> DmlDirectMutationFenceReceiptRecord {
        let value = sealed_fence(historical_attempt(), 2, 10, kind);
        let receipt =
            ConnectorExternalFenceReceipt::try_new(&value, marker(&value)).expect("receipt");
        let generation = value.generation();
        DmlDirectMutationFenceReceiptRecord {
            codec_version: DML_DIRECT_MUTATION_FENCE_CODEC_VERSION,
            operation_kind: kind,
            fence: DmlExternalFenceReceiptRecord {
                codec_version: DML_EXTERNAL_FENCE_CODEC_VERSION,
                identity: DmlExternalFenceIdentity {
                    cluster_identity_digest: hex::encode(value.cluster().digest()),
                    resource_digest: fenced_resource_digest(&table(), &target_ref(kind)),
                    write_operation_id: mutation_operation_uuid(),
                    coordination_attempt_id: historical_attempt(),
                    generation: DmlExternalFenceGeneration {
                        control_plane_incarnation: generation.control_plane_incarnation(),
                        resource_epoch: generation.resource_epoch(),
                        fence_generation: generation.coordination_attempt(),
                    },
                },
                fence_digest: hex::encode(value.digest()),
                receipt_digest: hex::encode(receipt.digest()),
                receipt_payload: DmlOpaquePayload::try_new(receipt.payload().to_vec())
                    .expect("bounded payload"),
                established_at_ms: 10,
            },
            source_scope_digest: kind
                .binds_source_scope()
                .then(|| hex::encode(SOURCE_SCOPE_DIGEST)),
        }
    }

    fn operation_uuid() -> Uuid {
        Uuid::parse_str("018f0000-0000-7000-8000-00000000000a").expect("uuid v7")
    }

    fn summary() -> DurableMutationSummary {
        DurableMutationSummary {
            file_count: 4,
            row_count: 40,
            total_bytes: 400,
        }
    }

    fn truncate_payload(phase: TruncateLifecyclePhase) -> OperationPayload {
        OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
            phase,
            connector_operation_id: mutation_operation_uuid(),
            provider_id: Some(PROVIDER.to_string()),
            connector_instance_id: Some(CATALOG.to_string()),
            connector_incarnation: Some(hex::encode(HISTORICAL_INCARNATION)),
            target_ref: "main".to_string(),
            request_digest: Some(hex::encode([1u8; 32])),
            plan_digest: Some(hex::encode([2u8; 32])),
            state_digest: Some(hex::encode([3u8; 32])),
            plan_summary: Some(summary()),
            outcome: None,
            next_action: StatementNextAction::None,
        })
    }

    fn add_files_payload(
        phase: AddFilesLifecyclePhase,
        certainty: AddFilesDispatchCertainty,
        ownership: SourceScopeOwnership,
    ) -> OperationPayload {
        OperationPayload::AddFilesLifecycle(AddFilesLifecycleRecord {
            phase,
            connector_operation_id: mutation_operation_uuid(),
            provider_id: Some(PROVIDER.to_string()),
            connector_instance_id: Some(CATALOG.to_string()),
            connector_incarnation: Some(hex::encode(HISTORICAL_INCARNATION)),
            source_location: "s3://bucket/incoming".to_string(),
            source_scope_version: Some(ConnectorDataMutationSourceScope::VERSION),
            source_scope_kind: Some("DIRECTORY".to_string()),
            source_scope_digest: Some(hex::encode(SOURCE_SCOPE_DIGEST)),
            request_digest: Some(hex::encode([1u8; 32])),
            plan_digest: Some(hex::encode([2u8; 32])),
            state_digest: Some(hex::encode([3u8; 32])),
            plan_summary: Some(summary()),
            plan_artifact: None,
            receipt_artifact: None,
            evidence_artifact: None,
            dispatch_certainty: certainty,
            source_ownership: ownership,
            outcome: None,
            next_action: StatementNextAction::None,
        })
    }

    fn stored(
        kind: DmlDirectMutationKind,
        state: OperationState,
        payload: OperationPayload,
    ) -> StoredOperation {
        StoredOperation {
            schema_version: DML_OPERATION_SCHEMA_VERSION,
            operation_id: DmlOperationId::from(operation_uuid()),
            revision: 4,
            last_mutation_id: Uuid::now_v7(),
            operation_kind: kind.operation_kind(),
            operation_subkind: None,
            target: OperationTarget {
                catalog: CATALOG.to_string(),
                namespace: NAMESPACE.to_string(),
                table: TABLE.to_string(),
                ref_name: None,
            },
            state,
            attempt_id: "attempt".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
            staged_artifacts: Vec::new(),
            payload,
            coordination_provenance: None,
            recovery_due_at_ms: Some(0),
            created_at_ms: 1,
            updated_at_ms: 2,
            finished_at_ms: None,
        }
    }

    // -----------------------------------------------------------------------
    // Recording ledger
    // -----------------------------------------------------------------------

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum LedgerEvent {
        Persist(
            DmlHistoricalRecoveryPhase,
            Option<DmlHistoricalCleanupState>,
            Option<i64>,
        ),
        Statement(OperationState, Option<AddFilesSourceAction>),
        Reschedule(Option<i64>),
    }

    struct FakeLedger {
        stored: StoredOperation,
        fence: Option<DmlDirectMutationFenceReceiptRecord>,
        recovery: Option<DmlHistoricalDataMutationRecoveryRecord>,
        proposal: DmlExternalFenceProposal,
        events: Vec<LedgerEvent>,
    }

    impl FakeLedger {
        fn truncate(state: OperationState, phase: TruncateLifecyclePhase) -> Self {
            Self {
                stored: stored(
                    DmlDirectMutationKind::Truncate,
                    state,
                    truncate_payload(phase),
                ),
                fence: Some(fence_record(DmlDirectMutationKind::Truncate)),
                recovery: None,
                proposal: proposal(recovery_attempt(), 3, 20),
                events: Vec::new(),
            }
        }

        fn add_files(
            state: OperationState,
            phase: AddFilesLifecyclePhase,
            certainty: AddFilesDispatchCertainty,
            ownership: SourceScopeOwnership,
        ) -> Self {
            Self {
                stored: stored(
                    DmlDirectMutationKind::AddFiles,
                    state,
                    add_files_payload(phase, certainty, ownership),
                ),
                fence: Some(fence_record(DmlDirectMutationKind::AddFiles)),
                recovery: None,
                proposal: proposal(recovery_attempt(), 3, 20),
                events: Vec::new(),
            }
        }

        fn phases(&self) -> Vec<DmlHistoricalRecoveryPhase> {
            self.events
                .iter()
                .filter_map(|event| match event {
                    LedgerEvent::Persist(phase, ..) => Some(*phase),
                    _ => None,
                })
                .collect()
        }

        fn source_actions(&self) -> Vec<AddFilesSourceAction> {
            self.events
                .iter()
                .filter_map(|event| match event {
                    LedgerEvent::Statement(_, action) => action.clone(),
                    _ => None,
                })
                .collect()
        }

        fn result(&self) -> Option<DmlHistoricalDataMutationResultRecord> {
            self.recovery
                .as_ref()
                .and_then(|record| record.result.clone())
        }
    }

    impl StatementRecoveryLedger for FakeLedger {
        fn stored(&self) -> &StoredOperation {
            &self.stored
        }

        fn check_authority(&self) -> Result<(), DmlError> {
            Ok(())
        }

        fn load_direct_mutation_fence(
            &self,
        ) -> Result<Option<DmlDirectMutationFenceReceiptRecord>, DmlError> {
            Ok(self.fence.clone())
        }

        fn load_recovery(
            &self,
        ) -> Result<Option<DmlHistoricalDataMutationRecoveryRecord>, DmlError> {
            Ok(self.recovery.clone())
        }

        fn load_evidence_wire(&self) -> Result<Option<Vec<u8>>, DmlError> {
            Ok(None)
        }

        fn external_fence_proposal(&self) -> Result<DmlExternalFenceProposal, DmlError> {
            Ok(self.proposal.clone())
        }

        fn persist_recovery(
            &mut self,
            recovery: DmlHistoricalDataMutationRecoveryRecord,
            recovery_due_at_ms: Option<i64>,
        ) -> Result<(), DmlError> {
            // Enforce the real journal's transition rules so a profile bug
            // cannot pass here and fail against SQLite.
            validate_historical_data_mutation_recovery_transition(
                self.recovery.as_ref(),
                &recovery,
            )
            .map_err(DmlError::journal_corruption)?;
            let expected = operation_requires_recovery_scan_with_direct_mutation(
                self.stored.state,
                &self.stored.payload,
                None,
                Some(&recovery),
            );
            assert_eq!(
                expected,
                recovery_due_at_ms.is_some(),
                "a historical recovery mutation must keep exactly the dues its obligations require"
            );
            self.events.push(LedgerEvent::Persist(
                recovery.phase,
                recovery.cleanup(),
                recovery_due_at_ms,
            ));
            self.stored.revision += 1;
            self.stored.recovery_due_at_ms = recovery_due_at_ms;
            self.recovery = Some(recovery);
            Ok(())
        }

        fn publish_statement_outcome(
            &mut self,
            outcome: StatementOutcomeMutation,
            recovery_due_at_ms: Option<i64>,
        ) -> Result<(), DmlError> {
            validate_statement_operation_transition(
                self.stored.operation_kind,
                self.stored.state,
                outcome.state,
            )
            .map_err(DmlError::journal_unresolved)?;
            // The journal refuses to drop a due while a CP-3C recovery is open,
            // so mirror that rule exactly.
            let expected = operation_requires_recovery_scan_with_direct_mutation(
                outcome.state,
                &outcome.payload,
                None,
                self.recovery.as_ref(),
            );
            assert_eq!(
                expected,
                recovery_due_at_ms.is_some(),
                "a statement mutation must keep the due its open recovery requires"
            );
            self.events
                .push(LedgerEvent::Statement(outcome.state, outcome.source_action));
            self.stored.revision += 1;
            self.stored.state = outcome.state;
            self.stored.payload = outcome.payload;
            self.stored.recovery_due_at_ms = recovery_due_at_ms;
            Ok(())
        }

        fn reschedule(&mut self, recovery_due_at_ms: Option<i64>) -> Result<(), DmlError> {
            self.events
                .push(LedgerEvent::Reschedule(recovery_due_at_ms));
            self.stored.revision += 1;
            self.stored.recovery_due_at_ms = recovery_due_at_ms;
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Fake historical facet
    // -----------------------------------------------------------------------

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FacetEvent {
        Raise([u8; 32]),
        Inspect([u8; 32]),
        Cleanup([u8; 32]),
        ReconcileCleanup,
    }

    #[derive(Clone, Copy)]
    enum FacetPlan {
        Applied,
        NotApplied {
            continuation: bool,
        },
        CleanupRequired,
        PartiallyApplied,
        Conflict,
        Ambiguous,
        RaiseSuperseded,
        /// Answers under a fence nobody raised: the CP-3C D5 stale case.
        StaleResponse,
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum FacetCleanup {
        Complete,
        Refused,
        Lost,
    }

    struct FakeFacet {
        key: ConnectorExecutionBindingKey,
        plan: FacetPlan,
        cleanup: FacetCleanup,
        state: Mutex<FakeFacetState>,
    }

    #[derive(Default)]
    struct FakeFacetState {
        events: Vec<FacetEvent>,
        raised: Option<ConnectorExternalOperationFence>,
        issued: Vec<[u8; 32]>,
    }

    impl FakeFacet {
        fn new(plan: FacetPlan, cleanup: FacetCleanup) -> Arc<Self> {
            Arc::new(Self {
                key: ConnectorExecutionBindingKey {
                    instance_id: instance(),
                    incarnation: ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
                },
                plan,
                cleanup,
                state: Mutex::new(FakeFacetState::default()),
            })
        }

        fn events(&self) -> Vec<FacetEvent> {
            self.state.lock().expect("facet state").events.clone()
        }
    }

    impl ConnectorHistoricalDataMutationRecovery for FakeFacet {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn raise_external_fence(
            &self,
            request: ConnectorHistoricalDataMutationFenceRaiseRequest,
        ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
            request.validate()?;
            if matches!(self.plan, FacetPlan::RaiseSuperseded) {
                return Err(ConnectorError::external_fence(
                    ConnectorExternalFenceFailure::Superseded,
                    "another authority already raised a higher fence",
                ));
            }
            let receipt =
                ConnectorExternalFenceReceipt::try_new(&request.raised, marker(&request.raised))?;
            let mut state = self.state.lock().expect("facet state");
            state.events.push(FacetEvent::Raise(receipt.digest()));
            state.raised = Some(request.raised);
            Ok(receipt)
        }

        fn inspect(
            &self,
            descriptor: ConnectorHistoricalDataMutationDescriptor,
            _context: ConnectorRequestContext,
        ) -> Result<ConnectorHistoricalDataMutationObservation, ConnectorError> {
            descriptor.validate()?;
            let mut state = self.state.lock().expect("facet state");
            state.events.push(FacetEvent::Inspect(descriptor.digest()));
            // The mandatory call order: without a raise this generation cannot
            // classify at all.
            let Some(raised) = state.raised.clone() else {
                return Err(ConnectorError::external_fence(
                    ConnectorExternalFenceFailure::NotEstablished,
                    "inspection requires this provider to have raised the fence first",
                ));
            };
            if raised.digest() != descriptor.raised_fence.digest() {
                return Err(ConnectorError::external_fence(
                    ConnectorExternalFenceFailure::Stale,
                    "inspection is behind the established external fence",
                ));
            }
            if matches!(self.plan, FacetPlan::StaleResponse) {
                // A response minted against a superseded descriptor: same
                // operation, different raised fence.
                let superseded = ConnectorHistoricalDataMutationDescriptor::try_new(
                    ConnectorHistoricalDataMutationIdentity {
                        historical_binding: descriptor.historical_binding.clone(),
                        table: descriptor.table.clone(),
                        target_ref: descriptor.target_ref.clone(),
                        operation_id: descriptor.operation_id,
                        family: descriptor.family,
                        request_digest: descriptor.request_digest,
                        plan_digest: descriptor.plan_digest,
                        state_digest: descriptor.state_digest,
                        plan_summary: descriptor.plan_summary,
                        source_scope: descriptor.source_scope,
                    },
                    ConnectorHistoricalDataMutationFenceFacts {
                        historical_fence: descriptor.historical_fence.clone(),
                        raised_fence: sealed_fence(
                            recovery_attempt(),
                            4,
                            21,
                            match descriptor.family {
                                ConnectorHistoricalDataMutationFamily::Truncate => {
                                    DmlDirectMutationKind::Truncate
                                }
                                ConnectorHistoricalDataMutationFamily::RegisterExistingFiles => {
                                    DmlDirectMutationKind::AddFiles
                                }
                            },
                        ),
                        raised_fence_receipt_digest: descriptor.raised_fence_receipt_digest,
                    },
                    descriptor.checkpoints.clone(),
                    descriptor.evidence.clone(),
                )?;
                return ConnectorHistoricalDataMutationObservation::try_new(
                    &superseded,
                    ConnectorHistoricalDataMutationDisposition::NotApplied,
                    ConnectorHistoricalDataMutationOutcomeFacts::default(),
                    ConnectorHistoricalDataMutationProof::try_new(Bytes::from_static(
                        b"stale-proof",
                    ))?,
                );
            }
            let (disposition, application, continuation, cleanup_required) = match self.plan {
                FacetPlan::Applied => (
                    ConnectorHistoricalDataMutationDisposition::Applied,
                    Some(ConnectorHistoricalDataMutationApplication {
                        committed_version: ConnectorCommittedVersion::try_new(
                            Bytes::from_static(b"committed-version"),
                            Some(42),
                        )?,
                        receipt: ConnectorDataMutationReceipt::try_new(
                            ConnectorInstanceDescriptor {
                                provider_id: ConnectorProviderId::parse(PROVIDER)
                                    .expect("provider id"),
                                instance_id: descriptor.table.instance_id.clone(),
                            },
                            ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
                            descriptor.operation_id,
                            descriptor.family.operation_kind(),
                            descriptor.request_digest,
                            descriptor.plan_digest,
                            descriptor.state_digest,
                            descriptor.plan_summary,
                            Bytes::from_static(b"{\"version\":1,\"snapshot_id\":9}"),
                        )?,
                        finalization: ExternalMutationFinalization::Complete,
                    }),
                    None,
                    false,
                ),
                FacetPlan::NotApplied { continuation } => (
                    ConnectorHistoricalDataMutationDisposition::NotApplied,
                    None,
                    continuation
                        .then(|| {
                            ConnectorHistoricalDataMutationContinuation::try_new(
                                &descriptor.raised_fence,
                                Bytes::from(format!("{PROVIDER_MARKER}|continuation")),
                            )
                        })
                        .transpose()?,
                    false,
                ),
                FacetPlan::CleanupRequired => (
                    ConnectorHistoricalDataMutationDisposition::CleanupRequired,
                    None,
                    None,
                    true,
                ),
                FacetPlan::PartiallyApplied => (
                    ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
                    None,
                    None,
                    false,
                ),
                FacetPlan::Conflict => (
                    ConnectorHistoricalDataMutationDisposition::Conflict,
                    None,
                    None,
                    false,
                ),
                FacetPlan::Ambiguous | FacetPlan::RaiseSuperseded | FacetPlan::StaleResponse => (
                    ConnectorHistoricalDataMutationDisposition::Ambiguous,
                    None,
                    None,
                    false,
                ),
            };
            let observation = ConnectorHistoricalDataMutationObservation::try_new(
                &descriptor,
                disposition,
                ConnectorHistoricalDataMutationOutcomeFacts {
                    application,
                    continuation,
                    cleanup_required,
                },
                ConnectorHistoricalDataMutationProof::try_new(Bytes::from(format!(
                    "{PROVIDER_MARKER}|proof|{}",
                    disposition.label()
                )))?,
            )?;
            state.issued.push(observation.digest());
            Ok(observation)
        }

        fn cleanup(
            &self,
            request: ConnectorHistoricalDataMutationCleanupRequest,
        ) -> Result<
            ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
            ConnectorError,
        > {
            let mut state = self.state.lock().expect("facet state");
            state
                .events
                .push(FacetEvent::Cleanup(request.observation.digest()));
            if !state.issued.contains(&request.observation.digest()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cleanup names an observation this provider never issued",
                ));
            }
            if !request.observation.cleanup_required {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cleanup was requested for an observation that requires none",
                ));
            }
            let receipt = ConnectorHistoricalDataMutationCleanupReceipt {
                descriptor_digest: request.descriptor_digest,
                observation_digest: request.observation.digest(),
            };
            match self.cleanup {
                FacetCleanup::Complete => Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                }),
                FacetCleanup::Refused => Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "cleanup could not run",
                    ),
                }),
                FacetCleanup::Lost => Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "cleanup result was lost",
                    ),
                    evidence: ExternalMutationEvidence::try_new(
                        1,
                        ConnectorInstanceDescriptor {
                            provider_id: ConnectorProviderId::parse(PROVIDER).expect("provider id"),
                            instance_id: instance(),
                        },
                        ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
                        request.operation_id,
                        "historical-data-mutation-cleanup",
                        Bytes::from(format!("{PROVIDER_MARKER}|cleanup-evidence")),
                    )?,
                }),
            }
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
            self.state
                .lock()
                .expect("facet state")
                .events
                .push(FacetEvent::ReconcileCleanup);
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: ConnectorHistoricalDataMutationCleanupReceipt {
                    descriptor_digest: [0; 32],
                    observation_digest: [0; 32],
                },
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    struct FakeResolver {
        facet: Arc<FakeFacet>,
    }

    impl HistoricalDataMutationRecoveryResolver for FakeResolver {
        fn resolve(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<HistoricalDataMutationRecoveryHandle, ConnectorError> {
            Ok(HistoricalDataMutationRecoveryHandle::new(
                PROVIDER.to_string(),
                Arc::clone(&self.facet) as Arc<dyn ConnectorHistoricalDataMutationRecovery>,
            ))
        }
    }

    fn profile(facet: &Arc<FakeFacet>) -> StatementRecoveryProfile {
        StatementRecoveryProfile::new(Arc::new(FakeResolver {
            facet: Arc::clone(facet),
        }))
    }

    /// Every facet call this profile is allowed to make. The ordinary
    /// `plan_mutation` / `execute` / `reconcile` surface is not reachable from
    /// [`ConnectorHistoricalDataMutationRecovery`] at all, so "zero ordinary
    /// calls" is a structural property; this asserts the recorded sequence
    /// stays inside the historical facet as well.
    fn assert_only_historical_calls(facet: &FakeFacet) {
        for event in facet.events() {
            assert!(
                matches!(
                    event,
                    FacetEvent::Raise(_)
                        | FacetEvent::Inspect(_)
                        | FacetEvent::Cleanup(_)
                        | FacetEvent::ReconcileCleanup
                ),
                "historical recovery reached a call outside the historical facet: {event:?}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Convergence order (spec D2)
    // -----------------------------------------------------------------------

    #[test]
    fn the_fence_is_raised_before_anything_is_inspected_or_concluded() {
        let facet = FakeFacet::new(FacetPlan::Applied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Resolved);

        let events = facet.events();
        let raise = events
            .iter()
            .position(|event| matches!(event, FacetEvent::Raise(_)))
            .expect("the fence was raised");
        let inspect = events
            .iter()
            .position(|event| matches!(event, FacetEvent::Inspect(_)))
            .expect("the operation was inspected");
        assert!(
            raise < inspect,
            "the historical authority must be closed before anything is classified"
        );
        assert_only_historical_calls(&facet);

        // The request is durable before the raise, and the raised fence is
        // durable before the classification it authorizes.
        assert_eq!(
            ledger.phases(),
            vec![
                DmlHistoricalRecoveryPhase::Requested,
                DmlHistoricalRecoveryPhase::FenceRaised,
                DmlHistoricalRecoveryPhase::Inspected,
                DmlHistoricalRecoveryPhase::Resolved,
            ]
        );
        // The recovered commit is walked all the way to its terminal state, so
        // an already-settled operation does not stay permanently due, and the
        // resolved record releases the bounded scan in the same step.
        assert_eq!(ledger.stored.state, OperationState::Finalized);
        assert!(ledger.stored.state.is_finished());
        assert_eq!(ledger.stored.recovery_due_at_ms, None);
        assert_eq!(
            ledger
                .events
                .iter()
                .filter_map(|event| match event {
                    LedgerEvent::Statement(state, _) => Some(*state),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![OperationState::Committed, OperationState::Finalized]
        );
    }

    #[test]
    fn a_typed_result_is_durable_before_any_guarded_cleanup_runs() {
        let facet = FakeFacet::new(FacetPlan::CleanupRequired, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Resolved);

        // The CleanupPending record is persisted before cleanup is attempted:
        // a crash mid-cleanup must never lose the obligation.
        let phases = ledger.phases();
        let pending = phases
            .iter()
            .position(|phase| *phase == DmlHistoricalRecoveryPhase::CleanupPending)
            .expect("a pending cleanup was published");
        assert!(pending < phases.len() - 1);
        assert!(
            facet
                .events()
                .iter()
                .any(|event| matches!(event, FacetEvent::Cleanup(_)))
        );
        // Cleanup-required is not-applied and terminal: it is never a retry.
        assert_eq!(ledger.stored.state, OperationState::FailedKnownUncommitted);
        assert_eq!(
            ledger.result().expect("result").cleanup,
            DmlHistoricalCleanupState::Completed
        );
        assert_only_historical_calls(&facet);
    }

    #[test]
    fn a_lost_cleanup_response_is_reconciled_through_the_historical_facet_only() {
        let facet = FakeFacet::new(FacetPlan::CleanupRequired, FacetCleanup::Lost);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");

        assert!(
            facet
                .events()
                .iter()
                .any(|event| matches!(event, FacetEvent::ReconcileCleanup)),
            "a lost cleanup result is resolved from opaque evidence"
        );
        assert_only_historical_calls(&facet);
    }

    #[test]
    fn a_retained_cleanup_obligation_survives_a_terminal_statement_result() {
        let facet = FakeFacet::new(FacetPlan::CleanupRequired, FacetCleanup::Refused);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::CleanupPending);

        // The user-visible statement result is terminal ...
        assert_eq!(ledger.stored.state, OperationState::FailedKnownUncommitted);
        assert!(ledger.stored.state.is_finished());
        // ... and the obligation still holds its due and its pending cleanup.
        let record = ledger.recovery.as_ref().expect("recovery record");
        assert_eq!(record.phase, DmlHistoricalRecoveryPhase::CleanupPending);
        assert_eq!(record.cleanup(), Some(DmlHistoricalCleanupState::Pending));
        assert!(record.requires_recovery_scan());
        assert!(ledger.stored.recovery_due_at_ms.is_some());
    }

    // -----------------------------------------------------------------------
    // Identity validation (spec D5)
    // -----------------------------------------------------------------------

    #[test]
    fn a_stale_response_changes_nothing_durable() {
        let facet = FakeFacet::new(FacetPlan::StaleResponse, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Superseded);

        // The fence was raised and the request published, but no result and no
        // statement outcome were ever recorded.
        let record = ledger.recovery.as_ref().expect("recovery record");
        assert_eq!(record.phase, DmlHistoricalRecoveryPhase::FenceRaised);
        assert!(record.result.is_none());
        assert_eq!(ledger.stored.state, OperationState::Committing);
        assert!(
            !ledger
                .events
                .iter()
                .any(|event| matches!(event, LedgerEvent::Statement(..)))
        );
    }

    #[test]
    fn a_response_naming_another_source_scope_is_refused() {
        let raised = sealed_fence(recovery_attempt(), 3, 20, DmlDirectMutationKind::AddFiles);
        let descriptor = ConnectorHistoricalDataMutationDescriptor::try_new(
            ConnectorHistoricalDataMutationIdentity {
                historical_binding: ConnectorExecutionBindingKey {
                    instance_id: instance(),
                    incarnation: ConnectorInstanceIncarnation::from_bytes(HISTORICAL_INCARNATION),
                },
                table: table(),
                target_ref: ConnectorWriteTargetRef::main(),
                operation_id: connector_operation_id(),
                family: ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
                request_digest: [1; 32],
                plan_digest: [2; 32],
                state_digest: [3; 32],
                plan_summary: ConnectorDataMutationPlanSummary::try_new(4, 40, 400)
                    .expect("summary"),
                source_scope: Some(
                    ConnectorDataMutationSourceScope::try_new_directory(SOURCE_SCOPE_DIGEST)
                        .expect("scope"),
                ),
            },
            ConnectorHistoricalDataMutationFenceFacts {
                historical_fence: ConnectorHistoricalDataMutationFence::NotEstablished,
                raised_fence: raised.clone(),
                raised_fence_receipt_digest: [9; 32],
            },
            vec![ConnectorHistoricalDataMutationCheckpoint {
                phase: ConnectorHistoricalDataMutationPhase::ExecuteDispatched,
                state: ConnectorHistoricalDataMutationDispatchState::Unknown,
                evidence_digest: None,
            }],
            None,
        )
        .expect("descriptor");
        let observation = ConnectorHistoricalDataMutationObservation::try_new(
            &descriptor,
            ConnectorHistoricalDataMutationDisposition::NotApplied,
            ConnectorHistoricalDataMutationOutcomeFacts::default(),
            ConnectorHistoricalDataMutationProof::try_new(Bytes::from_static(b"proof"))
                .expect("proof"),
        )
        .expect("observation");

        let mut crossed = observation.clone();
        crossed.source_scope = Some(
            ConnectorDataMutationSourceScope::try_new_directory([7; 32]).expect("other scope"),
        );
        assert!(
            validate_historical_data_mutation_response(&crossed, &descriptor, raised.digest())
                .is_err(),
            "an ADD FILES result must answer exactly the immutable source set it was asked about"
        );
        validate_historical_data_mutation_response(&observation, &descriptor, raised.digest())
            .expect("the matching response is accepted");
    }

    // -----------------------------------------------------------------------
    // ADD FILES source-scope ownership (spec D4)
    // -----------------------------------------------------------------------

    #[test]
    fn a_partially_applied_add_files_retains_its_source_scope() {
        let facet = FakeFacet::new(FacetPlan::PartiallyApplied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::add_files(
            OperationState::Committing,
            AddFilesLifecyclePhase::Executing,
            AddFilesDispatchCertainty::PossiblyDispatched,
            SourceScopeOwnership::ReservedImmutable,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Resolved);

        let result = ledger.result().expect("result");
        assert_eq!(
            result.disposition,
            DmlHistoricalDataMutationDisposition::PartiallyApplied
        );
        assert!(
            result.source_scope_retained,
            "a partially applied source set is never released"
        );
        assert!(
            ledger.source_actions().is_empty(),
            "no ownership action may be taken on an undetermined source set"
        );
        // The statement stays where it is and becomes operator-visible.
        assert_eq!(ledger.stored.state, OperationState::Committing);
        match &ledger.stored.payload {
            OperationPayload::AddFilesLifecycle(record) => {
                assert_eq!(record.next_action, StatementNextAction::ManualInspect);
                assert_eq!(
                    record.source_ownership,
                    SourceScopeOwnership::ReservedImmutable
                );
            }
            payload => panic!("unexpected payload {payload:?}"),
        }
    }

    #[test]
    fn an_undetermined_add_files_never_frees_its_source_scope() {
        for plan in [
            FacetPlan::Ambiguous,
            FacetPlan::Conflict,
            FacetPlan::RaiseSuperseded,
        ] {
            let facet = FakeFacet::new(plan, FacetCleanup::Complete);
            let mut ledger = FakeLedger::add_files(
                OperationState::Committing,
                AddFilesLifecyclePhase::Executing,
                AddFilesDispatchCertainty::PossiblyDispatched,
                SourceScopeOwnership::ReservedImmutable,
            );

            profile(&facet)
                .drive(&mut ledger, 1_000)
                .expect("one bounded cycle");

            assert!(
                ledger.source_actions().is_empty(),
                "an undetermined disposition must not touch source-scope ownership"
            );
            if let Some(result) = ledger.result() {
                assert!(result.source_scope_retained);
            }
            assert!(
                ledger
                    .recovery
                    .as_ref()
                    .expect("recovery record")
                    .retains_source_scope()
            );
        }
    }

    #[test]
    fn a_proven_not_applied_add_files_releases_its_reservation_in_the_fenced_transaction() {
        let facet = FakeFacet::new(
            FacetPlan::NotApplied {
                continuation: false,
            },
            FacetCleanup::Complete,
        );
        let mut ledger = FakeLedger::add_files(
            OperationState::Committing,
            AddFilesLifecyclePhase::Executing,
            AddFilesDispatchCertainty::PossiblyDispatched,
            SourceScopeOwnership::ReservedImmutable,
        );

        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");

        assert_eq!(
            ledger.source_actions(),
            vec![AddFilesSourceAction::Release {
                provider_id: PROVIDER.to_string(),
                scope_digest: hex::encode(SOURCE_SCOPE_DIGEST),
            }],
            "the release travels with the operation mutation that re-validates its digest"
        );
        assert!(!ledger.result().expect("result").source_scope_retained);
    }

    #[test]
    fn an_applied_add_files_hands_its_source_scope_to_the_table() {
        let facet = FakeFacet::new(FacetPlan::Applied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::add_files(
            OperationState::Committing,
            AddFilesLifecyclePhase::Executing,
            AddFilesDispatchCertainty::PossiblyDispatched,
            SourceScopeOwnership::ReservedImmutable,
        );

        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");

        assert_eq!(
            ledger.source_actions(),
            vec![AddFilesSourceAction::Transition {
                provider_id: PROVIDER.to_string(),
                scope_digest: hex::encode(SOURCE_SCOPE_DIGEST),
                expected: SourceScopeOwnership::ReservedImmutable,
                ownership: SourceScopeOwnership::TableOwned,
            }],
            "a proven applied source set is owned by the table, never released"
        );
        assert_eq!(ledger.stored.state, OperationState::Finalized);
    }

    #[test]
    fn a_frozen_add_files_source_scope_is_never_released() {
        let facet = FakeFacet::new(
            FacetPlan::NotApplied {
                continuation: false,
            },
            FacetCleanup::Complete,
        );
        let mut ledger = FakeLedger::add_files(
            OperationState::Committing,
            AddFilesLifecyclePhase::Executing,
            AddFilesDispatchCertainty::PossiblyDispatched,
            SourceScopeOwnership::Frozen,
        );

        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");

        assert!(
            ledger
                .source_actions()
                .iter()
                .all(|action| !matches!(action, AddFilesSourceAction::Release { .. })),
            "a frozen scope was frozen because nothing could be proven about it"
        );
    }

    // -----------------------------------------------------------------------
    // Continuations (spec D3)
    // -----------------------------------------------------------------------

    #[test]
    fn a_continuation_needs_a_journal_that_proves_nothing_was_dispatched() {
        // A possibly-dispatched TRUNCATE cannot carry a continuation at all:
        // the SPI refuses to seal one against its checkpoints.
        let facet = FakeFacet::new(
            FacetPlan::NotApplied { continuation: true },
            FacetCleanup::Complete,
        );
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );
        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(
            progress,
            StatementRecoveryProgress::Unresolved,
            "a continuation contradicting a dispatched journal is refused, not stored"
        );
        assert!(ledger.result().is_none());

        // A TRUNCATE that never left the planning phase proves non-dispatch,
        // so the same provider answer is accepted.
        let facet = FakeFacet::new(
            FacetPlan::NotApplied { continuation: true },
            FacetCleanup::Complete,
        );
        let mut ledger =
            FakeLedger::truncate(OperationState::Committing, TruncateLifecyclePhase::Planned);
        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::ContinuationPending);
        let result = ledger.result().expect("result");
        assert!(
            result.continuation_payload.is_some(),
            "the continuation authorizes a new attempt of the same durable operation"
        );
        assert_eq!(
            result.disposition,
            DmlHistoricalDataMutationDisposition::NotApplied
        );
        assert_only_historical_calls(&facet);
    }

    #[test]
    fn only_a_planning_phase_truncate_proves_nothing_was_dispatched() {
        for (phase, expected) in [
            (
                TruncateLifecyclePhase::Preparing,
                DmlHistoricalDispatchCertainty::ConfirmedNotDispatched,
            ),
            (
                TruncateLifecyclePhase::Planned,
                DmlHistoricalDispatchCertainty::ConfirmedNotDispatched,
            ),
            (
                TruncateLifecyclePhase::Executing,
                DmlHistoricalDispatchCertainty::PossiblyDispatched,
            ),
            (
                TruncateLifecyclePhase::CommitUnknown,
                DmlHistoricalDispatchCertainty::PossiblyDispatched,
            ),
            (
                // A plan failure and a dispatched-then-failed execute share
                // this phase, so it may never be softened.
                TruncateLifecyclePhase::Failed,
                DmlHistoricalDispatchCertainty::PossiblyDispatched,
            ),
            (
                TruncateLifecyclePhase::Committed,
                DmlHistoricalDispatchCertainty::ConfirmedDispatched,
            ),
        ] {
            assert_eq!(truncate_dispatch_certainty(phase), expected, "{phase:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Insufficient evidence
    // -----------------------------------------------------------------------

    #[test]
    fn an_ambiguous_disposition_publishes_nothing_and_reschedules() {
        let facet = FakeFacet::new(FacetPlan::Ambiguous, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Unresolved);
        assert_eq!(ledger.stored.state, OperationState::Committing);
        assert!(
            !ledger
                .events
                .iter()
                .any(|event| matches!(event, LedgerEvent::Statement(..)))
        );
        let record = ledger.recovery.as_ref().expect("recovery record");
        assert_eq!(record.phase, DmlHistoricalRecoveryPhase::Unresolved);
        assert!(record.requires_recovery_scan());
        assert_eq!(
            ledger.stored.recovery_due_at_ms,
            Some(1_000 + DML_STATEMENT_RECOVERY_UNRESOLVED_DELAY_MS)
        );
    }

    #[test]
    fn a_statement_without_durable_plan_digests_concludes_nothing() {
        let facet = FakeFacet::new(FacetPlan::Applied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Preparing,
        );
        match &mut ledger.stored.payload {
            OperationPayload::TruncateLifecycle(record) => {
                record.request_digest = None;
                record.plan_digest = None;
                record.state_digest = None;
            }
            payload => panic!("unexpected payload {payload:?}"),
        }

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Unresolved);
        assert!(
            facet.events().is_empty(),
            "an unnameable historical operation must not reach the provider at all"
        );
        assert!(ledger.recovery.is_none());
        assert_eq!(
            ledger.events,
            vec![LedgerEvent::Reschedule(Some(
                1_000 + DML_STATEMENT_RECOVERY_UNRESOLVED_DELAY_MS
            ))]
        );
    }

    #[test]
    fn a_fence_that_cannot_be_raised_concludes_nothing() {
        let facet = FakeFacet::new(FacetPlan::RaiseSuperseded, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Superseded);
        assert!(
            !facet
                .events()
                .iter()
                .any(|event| matches!(event, FacetEvent::Inspect(_))),
            "a fence that could not be raised must never be followed by an inspection"
        );
        let record = ledger.recovery.as_ref().expect("recovery record");
        assert_eq!(record.phase, DmlHistoricalRecoveryPhase::Requested);
        assert!(record.raised_fence.is_none());
    }

    #[test]
    fn a_recovery_attempt_that_cannot_outrank_the_old_fence_asks_the_provider_nothing() {
        let facet = FakeFacet::new(FacetPlan::Applied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );
        // The live lease mints a fence at or below the historical one.
        ledger.proposal = proposal(recovery_attempt(), 2, 10);

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::Unresolved);
        assert!(facet.events().is_empty());
        assert!(ledger.recovery.is_none());
    }

    // -----------------------------------------------------------------------
    // Terminal records
    // -----------------------------------------------------------------------

    #[test]
    fn a_resolved_record_is_never_reopened() {
        let facet = FakeFacet::new(FacetPlan::Applied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );
        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("first cycle");
        let resolved = ledger.recovery.clone().expect("recovery record");
        assert_eq!(resolved.phase, DmlHistoricalRecoveryPhase::Resolved);
        let calls = facet.events().len();

        let progress = profile(&facet)
            .drive(&mut ledger, 2_000)
            .expect("second cycle");
        assert_eq!(progress, StatementRecoveryProgress::Resolved);
        assert_eq!(
            facet.events().len(),
            calls,
            "a resolved record must not touch the provider again"
        );
        assert_eq!(ledger.recovery, Some(resolved));
    }

    #[test]
    fn a_row_dml_operation_is_not_driven_by_this_profile() {
        let facet = FakeFacet::new(FacetPlan::Applied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::truncate(
            OperationState::Committing,
            TruncateLifecyclePhase::Executing,
        );
        ledger.stored.operation_kind = OperationKind::RowDelta;

        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one bounded cycle");
        assert_eq!(progress, StatementRecoveryProgress::NotApplicable);
        assert!(facet.events().is_empty());
        assert!(ledger.events.is_empty());
    }
}
