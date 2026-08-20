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

//! CP-3B frontend historical write recovery profile.
//!
//! The bounded CP-3A recovery controller claims one distributed-write operation
//! under its exact operation lease; this profile is what it does with that
//! claim. The convergence order is fixed by spec CP-3B D2 and is enforced
//! jointly with the provider:
//!
//! 1. the operation is claimed under its exact operation lease (CP-3A);
//! 2. the immutable recovery request is fenced-persisted — it can never change
//!    once durable, so every later cycle inspects exactly the same request;
//! 3. `raise_external_fence` establishes a strictly higher external fence, so
//!    the historical authority is closed *before* anything is concluded;
//! 4. `inspect` runs outside any StateStore transaction;
//! 5. the typed result is fenced-persisted;
//! 6. only then may the profile finalize, run a proof-bound guarded cleanup, or
//!    keep the record unresolved — strictly according to the disposition.
//!
//! Three rules shape everything below.
//!
//! *The frontend classifies nothing.* Every disposition, receipt, proof and
//! continuation is produced by the current provider generation and stored as
//! identity, digests and bounded opaque bytes. No provider payload is decoded.
//!
//! *No ordinary write call ever happens here.* The profile only ever reaches
//! the separately installed historical facet. It never calls `commit`, `abort`
//! or `reconcile` on the historical operation, and it never revives the
//! historical binding.
//!
//! *Absence is never proof.* Missing evidence, an unreconstructable fenced
//! identity, a fence that cannot be raised strictly higher, or an `Ambiguous`
//! disposition all reschedule the recovery due instead of concluding anything.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorClusterIdentity, ConnectorControlPlanningLease,
    ConnectorControlRegistry, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExternalFenceFailure, ConnectorExternalFenceGeneration, ConnectorExternalFenceReceipt,
    ConnectorExternalOperationFence, ConnectorHistoricalWriteCheckpoint,
    ConnectorHistoricalWriteCleanupReceipt, ConnectorHistoricalWriteCleanupRequest,
    ConnectorHistoricalWriteContinuation, ConnectorHistoricalWriteDescriptor,
    ConnectorHistoricalWriteDispatchState, ConnectorHistoricalWriteDisposition,
    ConnectorHistoricalWriteFence, ConnectorHistoricalWriteFenceFacts,
    ConnectorHistoricalWriteFenceRaiseRequest, ConnectorHistoricalWriteIdentity,
    ConnectorHistoricalWriteObservation, ConnectorHistoricalWritePhase,
    ConnectorHistoricalWriteRecovery, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorWriteIntent,
    ConnectorWriteOperationId, ConnectorWriteTargetRef, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::coordination::{ActiveDmlOperation, DmlExternalFenceProposal};
use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::model::{
    ConnectorWriteFinalizationRecord, ConnectorWriteLifecycleRecord,
    DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION, DmlExternalFenceReceiptRecord,
    DmlHistoricalCleanupState, DmlHistoricalDispatchCertainty, DmlHistoricalRecoveryPhase,
    DmlHistoricalWriteRecoveryMutationRequest, DmlHistoricalWriteRecoveryRecord,
    DmlHistoricalWriteRequestRecord, DmlHistoricalWriteResultRecord, OperationFact, OperationKind,
    OperationPayload, OperationState, OperationTarget, StatementNextAction, StoredOperation,
    operation_requires_recovery_scan, validate_operation_transition,
};
use crate::dml::reconcile::{
    HistoricalWriteProjection, historical_write_projection, historical_write_result_record,
};

/// Backoff for an operation whose evidence is insufficient right now: a missing
/// fence receipt, an unreconstructable fenced identity, or a transient provider
/// failure. Nothing has been concluded, so the due simply moves.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
pub(crate) const DML_WRITE_RECOVERY_UNRESOLVED_DELAY_MS: i64 = 5_000;
/// Backoff for a proof-bound cleanup that did not complete. The obligation is
/// retained (spec D5) and retried on a later cycle.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
pub(crate) const DML_WRITE_RECOVERY_CLEANUP_DELAY_MS: i64 = 15_000;
/// Backoff for a fact this cycle cannot change: a superseded recovery attempt,
/// unreadable external truth, or a retained provider continuation.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
pub(crate) const DML_WRITE_RECOVERY_BLOCKED_DELAY_MS: i64 = 30_000;

/// Deadline for one historical provider action. Recovery is a background,
/// bounded activity; it must never wait on a provider indefinitely.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const DML_WRITE_RECOVERY_ACTION_DEADLINE: Duration = Duration::from_secs(30);

/// Domain separator for the frontend-owned digest of one immutable historical
/// write request.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const DML_HISTORICAL_WRITE_REQUEST_DOMAIN: &[u8] = b"novarocks.dml.historical-write-request.v1\0";
/// Domain separator for the frontend-owned digest of the old immutable input.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const DML_HISTORICAL_WRITE_INPUT_DOMAIN: &[u8] = b"novarocks.dml.historical-write-input.v1\0";

/// The historical connector generation the frontend never recorded.
///
/// A write-family operation payload carries no provider incarnation, so unless
/// the durable lifecycle holds SPI reconciliation evidence (which does carry the
/// exact historical generation as a neutral field) the frontend cannot name it.
/// An all-zero incarnation is a sentinel for "unknown", never a claim about a
/// real generation: it is deliberately not the current generation, so a
/// provider can never mistake the recovering owner for the historical owner.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const UNKNOWN_HISTORICAL_INCARNATION: [u8; 16] = [0; 16];

/// Whether this operation kind belongs to the CP-3B distributed-write family.
///
/// CTAS, TRUNCATE, ADD FILES and MV refresh have their own historical
/// reconciliation owners (CP-3C/3D, MVX-3) and must not be driven here.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
pub(crate) const fn is_write_family(kind: OperationKind) -> bool {
    matches!(
        kind,
        OperationKind::InsertAppend | OperationKind::InsertOverwrite | OperationKind::RowDelta
    )
}

// ---------------------------------------------------------------------------
// Frontend-visible projection of one historical observation
// ---------------------------------------------------------------------------

/// Everything the frontend is allowed to learn from one historical inspection.
///
/// Building this value is the only projection the profile performs, and it
/// reads no provider payload: proof, continuation, committed version and write
/// receipt are reduced to their digests before they cross this boundary. The
/// profile drives its state machine from these fields and performs the CP-3B D5
/// double check against them, so nothing here is decoration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoricalWriteOutcome {
    pub disposition: ConnectorHistoricalWriteDisposition,
    pub operation_id: ConnectorWriteOperationId,
    pub descriptor_digest: [u8; 32],
    pub raised_fence_digest: [u8; 32],
    pub proof_digest: [u8; 32],
    pub continuation_digest: Option<[u8; 32]>,
    pub committed_version_digest: Option<[u8; 32]>,
    pub write_receipt_digest: Option<[u8; 32]>,
    pub finalization_complete: Option<bool>,
    pub cleanup_required: bool,
    pub resolved: bool,
}

impl HistoricalWriteOutcome {
    pub fn project(observation: &ConnectorHistoricalWriteObservation) -> Self {
        Self {
            disposition: observation.disposition,
            operation_id: observation.operation_id,
            descriptor_digest: observation.descriptor_digest,
            raised_fence_digest: observation.raised_fence_digest,
            proof_digest: observation.proof.digest(),
            continuation_digest: observation
                .continuation
                .as_ref()
                .map(ConnectorHistoricalWriteContinuation::digest),
            committed_version_digest: observation
                .application
                .as_ref()
                .map(|application| application.committed_version.digest()),
            write_receipt_digest: observation
                .application
                .as_ref()
                .map(|application| application.receipt.digest()),
            finalization_complete: observation.application.as_ref().map(|application| {
                matches!(
                    application.finalization,
                    ExternalMutationFinalization::Complete
                )
            }),
            cleanup_required: observation.cleanup_required,
            resolved: observation.disposition.is_resolved(),
        }
    }

    /// Whether this outcome answers exactly the supplied descriptor under the
    /// supplied raised external fence.
    pub fn answers(&self, descriptor: &ConnectorHistoricalWriteDescriptor) -> bool {
        self.descriptor_digest == descriptor.digest()
            && self.operation_id == descriptor.operation_id
            && self.raised_fence_digest == descriptor.raised_fence.digest()
    }
}

/// The CP-3B D5 double check performed on every historical provider response.
///
/// A response is only durable when it was produced under the external fence
/// this owner still holds *and* it answers exactly the immutable descriptor this
/// recovery record still owns. A response from a superseded lease is refused as
/// typed stale and changes nothing.
pub fn validate_historical_response(
    observation: &ConnectorHistoricalWriteObservation,
    descriptor: &ConnectorHistoricalWriteDescriptor,
    expected_raised_fence_digest: [u8; 32],
) -> Result<(), ConnectorError> {
    if observation.raised_fence_digest != expected_raised_fence_digest {
        return Err(ConnectorError::external_fence(
            ConnectorExternalFenceFailure::Stale,
            "historical write response was produced under a superseded external fence",
        ));
    }
    observation.validate_for(descriptor)
}

// ---------------------------------------------------------------------------
// Cleanup retention
// ---------------------------------------------------------------------------

/// What one guarded-cleanup attempt means for the retained cleanup obligation.
///
/// Spec D5: a pending cleanup outcome survives until it completes or is
/// explicitly kept for manual retention, and a `KnownUncommitted` outcome never
/// drops its `ExternalMutationFinalization` just because the user-visible
/// statement result already became terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalCleanupProgress {
    pub state: DmlHistoricalCleanupState,
    pub retained_finalization: Option<ExternalMutationFinalization>,
    /// Present when the cleanup result was lost. It is the only legal input to
    /// `reconcile_cleanup`, and it stays opaque.
    pub unresolved_evidence: Option<ExternalMutationEvidence>,
}

impl HistoricalCleanupProgress {
    pub fn of(outcome: &ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>) -> Self {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { finalization, .. } => Self {
                state: if matches!(finalization, ExternalMutationFinalization::Complete) {
                    DmlHistoricalCleanupState::Completed
                } else {
                    DmlHistoricalCleanupState::Pending
                },
                retained_finalization: Some(finalization.clone()),
                unresolved_evidence: None,
            },
            ExternalMutationOutcome::KnownUncommitted { .. } => Self {
                state: DmlHistoricalCleanupState::Pending,
                retained_finalization: None,
                unresolved_evidence: None,
            },
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => Self {
                state: DmlHistoricalCleanupState::Pending,
                retained_finalization: None,
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

/// One resolved historical write recovery facet, with the control generation it
/// belongs to kept alive for as long as the facet is used.
pub struct HistoricalWriteRecoveryHandle {
    provider_id: String,
    recovery: Arc<dyn ConnectorHistoricalWriteRecovery>,
    _retained: Option<ConnectorControlPlanningLease>,
}

impl HistoricalWriteRecoveryHandle {
    pub fn new(provider_id: String, recovery: Arc<dyn ConnectorHistoricalWriteRecovery>) -> Self {
        Self {
            provider_id,
            recovery,
            _retained: None,
        }
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub fn facet(&self) -> &dyn ConnectorHistoricalWriteRecovery {
        self.recovery.as_ref()
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        self.recovery.binding_key()
    }
}

/// Narrow port that resolves the current generation's historical write recovery
/// facet for one connector instance.
///
/// The facet is installed separately from the ordinary write capability, so a
/// provider that owns ordinary writes without owning historical recovery
/// resolves to `Unsupported` here rather than silently falling back.
pub trait HistoricalWriteRecoveryResolver: Send + Sync {
    fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<HistoricalWriteRecoveryHandle, ConnectorError>;
}

struct ControlRegistryResolver {
    registry: Arc<dyn ConnectorControlRegistry>,
}

impl HistoricalWriteRecoveryResolver for ControlRegistryResolver {
    fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<HistoricalWriteRecoveryHandle, ConnectorError> {
        let lease = self.registry.acquire_current(instance_id)?;
        let recovery = lease
            .binding()
            .historical_write_recovery()
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no historical write recovery capability",
                )
            })?;
        let provider_id = lease
            .binding()
            .descriptor()
            .provider_id
            .as_str()
            .to_string();
        Ok(HistoricalWriteRecoveryHandle {
            provider_id,
            recovery,
            _retained: Some(lease),
        })
    }
}

/// Build the production resolver from the frontend-owned control registry.
pub fn control_registry_resolver(
    registry: Arc<dyn ConnectorControlRegistry>,
) -> Arc<dyn HistoricalWriteRecoveryResolver> {
    Arc::new(ControlRegistryResolver { registry })
}

// ---------------------------------------------------------------------------
// Durable ledger seam
// ---------------------------------------------------------------------------

/// The durable half of one claimed operation, as the profile needs it.
///
/// Every method here is a fenced StateStore mutation or a read; none of them may
/// be held open across a provider call. The seam exists so the convergence
/// order can be verified against a recording ledger without a StateStore.
pub(crate) trait HistoricalWriteRecoveryLedger {
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    fn stored(&self) -> &StoredOperation;

    /// Re-assert that this owner still holds the exact operation lease.
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn check_authority(&self) -> Result<(), DmlError>;

    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn load_external_fence(&self) -> Result<Option<DmlExternalFenceReceiptRecord>, DmlError>;

    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn load_recovery(&self) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError>;

    /// Mint this recovery attempt's external fence proposal from the *live*
    /// lease guard. CP-3A rule 3 forbids capturing a one-shot snapshot.
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
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
        recovery: DmlHistoricalWriteRecoveryRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError>;

    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn publish_fact(&mut self, fact: OperationFact) -> Result<(), DmlError>;

    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn transition(&mut self, to: OperationState) -> Result<(), DmlError>;

    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn reschedule(&mut self, recovery_due_at_ms: Option<i64>) -> Result<(), DmlError>;
}

impl HistoricalWriteRecoveryLedger for ActiveDmlOperation {
    fn stored(&self) -> &StoredOperation {
        &self.stored
    }

    fn check_authority(&self) -> Result<(), DmlError> {
        self.check_before_dispatch()
    }

    fn load_external_fence(&self) -> Result<Option<DmlExternalFenceReceiptRecord>, DmlError> {
        self.journal.load_external_fence(self.operation_id())
    }

    fn load_recovery(&self) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
        self.journal
            .load_historical_write_recovery(self.operation_id())
    }

    fn external_fence_proposal(&self) -> Result<DmlExternalFenceProposal, DmlError> {
        self.external_fence()
    }

    fn persist_recovery(
        &mut self,
        recovery: DmlHistoricalWriteRecoveryRecord,
        recovery_due_at_ms: Option<i64>,
    ) -> Result<(), DmlError> {
        let request = DmlHistoricalWriteRecoveryMutationRequest {
            operation_id: self.operation_id(),
            expected_revision: self.stored.revision,
            mutation_id: Uuid::now_v7(),
            recovery,
        };
        // Refuse a record this journal could never hold before any provider
        // action treats it as durable.
        self.journal
            .preflight_historical_write_recovery(&request)
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        self.stored = self
            .journal
            .record_historical_write_recovery_authorized(
                request,
                recovery_due_at_ms,
                self.journal_authority()?,
            )
            .map_err(|error| error.with_operation_id(self.operation_id()))?;
        Ok(())
    }

    fn publish_fact(&mut self, fact: OperationFact) -> Result<(), DmlError> {
        self.record_fact(fact, None)
    }

    fn transition(&mut self, to: OperationState) -> Result<(), DmlError> {
        ActiveDmlOperation::transition(self, to, None)
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
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
pub(crate) enum WriteRecoveryProgress {
    /// This operation is not driven by the distributed-write profile.
    NotApplicable,
    /// The recovery record reached its terminal phase.
    Resolved,
    /// A proof-bound guarded cleanup is still outstanding and retained.
    CleanupPending,
    /// The provider signed a continuation for a proven not-dispatched
    /// operation. The profile stores it and does not resume the statement; a
    /// continuation is resumed through the ordinary current-generation path.
    ContinuationPending,
    /// This recovery attempt was superseded by another authority.
    Superseded,
    /// Evidence was insufficient. The due moved; nothing was concluded.
    Unresolved,
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
pub(crate) struct WriteRecoveryProfile {
    resolver: Arc<dyn HistoricalWriteRecoveryResolver>,
}

impl WriteRecoveryProfile {
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    pub(crate) fn new(resolver: Arc<dyn HistoricalWriteRecoveryResolver>) -> Self {
        Self { resolver }
    }

    /// Drive one bounded historical write recovery cycle.
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn drive(
        &self,
        ledger: &mut dyn HistoricalWriteRecoveryLedger,
        now_ms: i64,
    ) -> Result<WriteRecoveryProgress, DmlError> {
        let operation_id = ledger.stored().operation_id;
        if !is_write_family(ledger.stored().operation_kind)
            || !matches!(
                ledger.stored().payload,
                OperationPayload::ConnectorWriteLifecycle(_)
            )
        {
            return Ok(WriteRecoveryProgress::NotApplicable);
        }
        // A provider is about to be asked to change external truth; prove this
        // owner still holds the exact operation lease first.
        ledger.check_authority()?;

        let durable = ledger.load_recovery()?;
        if durable
            .as_ref()
            .is_some_and(|record| record.phase == DmlHistoricalRecoveryPhase::Resolved)
        {
            // A resolved record is never reopened. The operation may still be
            // non-terminal (a retained finalization failure, for example), so
            // keep it visible without touching the provider again.
            return self.park(ledger, now_ms, WriteRecoveryProgress::Resolved);
        }

        let mut facts = match self.historical_facts(ledger, durable.as_ref())? {
            Ok(facts) => facts,
            Err(reason) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    reason = %reason,
                    "historical write recovery has insufficient durable evidence; rescheduling"
                );
                return self.park(ledger, now_ms, WriteRecoveryProgress::Unresolved);
            }
        };

        // D2 step 2: the immutable request must be durable before any external
        // fence is raised. A later cycle finds it already durable and reuses it
        // verbatim, which is what makes repeated inspection legal.
        let mut cycle = match &durable {
            None => {
                let record = DmlHistoricalWriteRecoveryRecord {
                    codec_version: DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION,
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
                    DML_WRITE_RECOVERY_UNRESOLVED_DELAY_MS,
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
        let raise_receipt =
            match handle
                .facet()
                .raise_external_fence(ConnectorHistoricalWriteFenceRaiseRequest {
                    historical_binding: facts.historical_binding.clone(),
                    observed: facts.historical_fence.clone(),
                    raised: facts.raised_fence.clone(),
                    context: context.clone(),
                }) {
                Ok(receipt) => receipt,
                Err(error) => {
                    let progress = self.classify_provider_failure(operation_id, "raise", &error);
                    return self.park(ledger, now_ms, progress);
                }
            };
        if !raise_receipt.matches(&facts.raised_fence) {
            tracing::warn!(
                operation_id = %operation_id,
                "historical write fence raise receipt acknowledges another fence; rescheduling"
            );
            return self.park(ledger, now_ms, WriteRecoveryProgress::Unresolved);
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
                    "historical write recovery cannot seal its descriptor; rescheduling"
                );
                return self.park(ledger, now_ms, WriteRecoveryProgress::Unresolved);
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
            DmlHistoricalWriteRecoveryRecord {
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
            DML_WRITE_RECOVERY_UNRESOLVED_DELAY_MS,
        )?;

        // D2 step 4: classify outside every StateStore transaction.
        let observation = match handle.facet().inspect(descriptor.clone(), context.clone()) {
            Ok(observation) => observation,
            Err(error) => {
                let progress = self.classify_provider_failure(operation_id, "inspect", &error);
                return self.park(ledger, now_ms, progress);
            }
        };
        if let Err(error) = validate_historical_response(
            &observation,
            &descriptor,
            descriptor.raised_fence.digest(),
        ) {
            // D5: a stale or foreign response changes nothing durable.
            let progress = self.classify_provider_failure(operation_id, "validate", &error);
            return self.park(ledger, now_ms, progress);
        }
        let outcome = HistoricalWriteOutcome::project(&observation);
        debug_assert!(outcome.answers(&descriptor));
        tracing::info!(
            operation_id = %operation_id,
            provider = handle.provider_id(),
            disposition = ?outcome.disposition,
            cleanup_required = outcome.cleanup_required,
            resolved = outcome.resolved,
            "historical write recovery classified a distributed write"
        );

        // D2 step 5: publish the typed result before anything acts on it.
        let mut result = historical_write_result_record(&observation)
            .map_err(DmlError::journal_corruption)
            .map_err(|error| error.with_operation_id(operation_id))?;
        let inspected_phase = inspected_phase_for(&outcome);
        cycle = self.persist(
            ledger,
            DmlHistoricalWriteRecoveryRecord {
                phase: inspected_phase,
                result: Some(result.clone()),
                next_action: next_action_for(inspected_phase, Some(result.cleanup)),
                updated_at_ms: now_ms,
                ..cycle
            },
            now_ms,
            DML_WRITE_RECOVERY_CLEANUP_DELAY_MS,
        )?;

        // D2 step 6: finalize, clean up, or stay unresolved.
        match outcome.disposition {
            ConnectorHistoricalWriteDisposition::Ambiguous
            | ConnectorHistoricalWriteDisposition::Unsupported => {
                return self.park(ledger, now_ms, WriteRecoveryProgress::Unresolved);
            }
            ConnectorHistoricalWriteDisposition::Conflict => {
                // A `Conflict` is a statement about *this* recovery attempt: a
                // newer authority owns the external fence. The old operation is
                // not settled by it, so nothing terminal is published and the
                // record stays open to be re-driven under that new authority.
                return self.park(ledger, now_ms, WriteRecoveryProgress::Superseded);
            }
            _ => {}
        }

        let projection = historical_write_projection(&observation)
            .map_err(DmlError::journal_corruption)
            .map_err(|error| error.with_operation_id(operation_id))?;
        let mut progress = match &projection {
            HistoricalWriteProjection::Continuation => WriteRecoveryProgress::ContinuationPending,
            HistoricalWriteProjection::Terminal(fact)
            | HistoricalWriteProjection::CleanupRequired(fact) => {
                publish_terminal(ledger, fact.clone())?;
                WriteRecoveryProgress::Resolved
            }
            HistoricalWriteProjection::Unresolved => WriteRecoveryProgress::Unresolved,
        };

        if outcome.cleanup_required {
            // Only an observation this provider issued authorizes a cleanup, and
            // it must be the exact object `inspect` returned. A process restart
            // between the two forces a new cycle, which re-inspects.
            let cleanup = self.run_cleanup(&handle, &descriptor, &observation, &context);
            match cleanup {
                Ok(cleanup) => {
                    result.cleanup = cleanup.state;
                    if cleanup.is_pending() {
                        progress = WriteRecoveryProgress::CleanupPending;
                    }
                }
                Err(error) => {
                    let failure = self.classify_provider_failure(operation_id, "cleanup", &error);
                    tracing::debug!(
                        operation_id = %operation_id,
                        error = %error,
                        "historical write guarded cleanup did not complete; obligation retained"
                    );
                    result.cleanup = DmlHistoricalCleanupState::Pending;
                    progress = match failure {
                        WriteRecoveryProgress::Superseded => WriteRecoveryProgress::Superseded,
                        _ => WriteRecoveryProgress::CleanupPending,
                    };
                }
            }
        }

        let final_phase = final_phase_for(progress, result.cleanup);
        let delay = delay_for(progress);
        self.persist(
            ledger,
            DmlHistoricalWriteRecoveryRecord {
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
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    fn run_cleanup(
        &self,
        handle: &HistoricalWriteRecoveryHandle,
        descriptor: &ConnectorHistoricalWriteDescriptor,
        observation: &ConnectorHistoricalWriteObservation,
        context: &ConnectorRequestContext,
    ) -> Result<HistoricalCleanupProgress, ConnectorError> {
        let outcome = handle
            .facet()
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id: descriptor.operation_id,
                descriptor_digest: descriptor.digest(),
                observation: observation.clone(),
                context: context.clone(),
            })?;
        let progress = HistoricalCleanupProgress::of(&outcome);
        let Some(evidence) = progress.unresolved_evidence.clone() else {
            return Ok(progress);
        };
        // One bounded reconciliation attempt on the same opaque evidence. It is
        // the historical facet's own cleanup reconciliation, never an ordinary
        // write reconcile.
        match handle
            .facet()
            .reconcile_cleanup(descriptor.operation_id, evidence, context.clone())
        {
            Ok(resolved) => Ok(HistoricalCleanupProgress::of(&resolved)),
            Err(_) => Ok(progress),
        }
    }

    /// Persist one recovery record with the due its obligations require.
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn persist(
        &self,
        ledger: &mut dyn HistoricalWriteRecoveryLedger,
        record: DmlHistoricalWriteRecoveryRecord,
        now_ms: i64,
        delay_ms: i64,
    ) -> Result<DmlHistoricalWriteRecoveryRecord, DmlError> {
        let due = recovery_due_for(ledger.stored(), &record, now_ms.saturating_add(delay_ms));
        ledger.persist_recovery(record.clone(), due)?;
        Ok(record)
    }

    /// Move the due and report progress without concluding anything.
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn park(
        &self,
        ledger: &mut dyn HistoricalWriteRecoveryLedger,
        now_ms: i64,
        progress: WriteRecoveryProgress,
    ) -> Result<WriteRecoveryProgress, DmlError> {
        let stored = ledger.stored().clone();
        let durable = ledger.load_recovery()?;
        let due =
            if operation_requires_recovery_scan(stored.state, &stored.payload, durable.as_ref()) {
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
    /// A fence failure and unreadable external truth are never retried inside
    /// this cycle: they are facts about authority or evidence, not transient
    /// conditions. Everything else may be retried on a later cycle.
    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    fn classify_provider_failure(
        &self,
        operation_id: crate::dml::model::DmlOperationId,
        stage: &str,
        error: &ConnectorError,
    ) -> WriteRecoveryProgress {
        if let Some(failure) = error.external_fence_failure() {
            tracing::debug!(
                operation_id = %operation_id,
                stage,
                failure = ?failure,
                error = %error,
                "historical write recovery hit a typed external fence failure; not retried in this cycle"
            );
            return match failure {
                ConnectorExternalFenceFailure::Superseded
                | ConnectorExternalFenceFailure::Stale
                | ConnectorExternalFenceFailure::ForeignOperation => {
                    WriteRecoveryProgress::Superseded
                }
                ConnectorExternalFenceFailure::NotEstablished => WriteRecoveryProgress::Unresolved,
            };
        }
        match error.kind() {
            ConnectorErrorKind::CorruptData => {
                tracing::warn!(
                    operation_id = %operation_id,
                    stage,
                    error = %error,
                    "historical write recovery could not read external truth; keeping the record unresolved"
                );
                WriteRecoveryProgress::Superseded
            }
            _ => {
                tracing::debug!(
                    operation_id = %operation_id,
                    stage,
                    error = %error,
                    "historical write recovery could not complete this cycle; rescheduling"
                );
                WriteRecoveryProgress::Unresolved
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
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn historical_facts(
        &self,
        ledger: &dyn HistoricalWriteRecoveryLedger,
        durable: Option<&DmlHistoricalWriteRecoveryRecord>,
    ) -> Result<Result<HistoricalWriteFacts, String>, DmlError> {
        let stored = ledger.stored().clone();
        let Some(fence_record) = ledger.load_external_fence()? else {
            // Without a confirmed fence receipt the frontend cannot name the
            // connector write operation the historical attempt used, so it can
            // ask the provider nothing at all. This is not evidence of
            // non-commit: the record stays unresolved.
            return Ok(Err(
                "no durable external fence receipt names the historical connector write operation"
                    .to_string(),
            ));
        };
        let proposal = ledger.external_fence_proposal()?;
        Ok(historical_write_facts(
            &stored,
            &fence_record,
            &proposal,
            durable,
        ))
    }

    #[allow(
        dead_code,
        reason = "Retained for staged DML recovery and durable-journal integration."
    )]
    fn descriptor(
        &self,
        facts: &HistoricalWriteFacts,
    ) -> Result<ConnectorHistoricalWriteDescriptor, ConnectorError> {
        ConnectorHistoricalWriteDescriptor::try_new(
            ConnectorHistoricalWriteIdentity {
                historical_binding: facts.historical_binding.clone(),
                table: facts.table.clone(),
                target_ref: facts.target_ref.clone(),
                operation_id: facts.connector_operation_id,
                intent: facts.intent,
                cohort_set_digest: facts.cohort_set_digest,
                aggregate_digest: facts.aggregate_digest,
            },
            ConnectorHistoricalWriteFenceFacts {
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

/// Every fact one historical inspection needs, rebuilt from durable state only.
#[derive(Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
struct HistoricalWriteFacts {
    table: ConnectorTableIdentity,
    target_ref: ConnectorWriteTargetRef,
    connector_operation_id: ConnectorWriteOperationId,
    intent: ConnectorWriteIntent,
    historical_binding: ConnectorExecutionBindingKey,
    historical_fence: ConnectorHistoricalWriteFence,
    raised_fence: ConnectorExternalOperationFence,
    raised_fence_receipt_digest: [u8; 32],
    cohort_set_digest: [u8; 32],
    aggregate_digest: Option<[u8; 32]>,
    checkpoints: Vec<ConnectorHistoricalWriteCheckpoint>,
    evidence: Option<ExternalMutationEvidence>,
    request: DmlHistoricalWriteRequestRecord,
    recovery_attempt_id: Uuid,
}

pub(crate) fn reconstruct_historical_write_fence(
    fence_record: &DmlExternalFenceReceiptRecord,
    connector_operation_id: ConnectorWriteOperationId,
    table: ConnectorTableIdentity,
    target_ref: ConnectorWriteTargetRef,
) -> Result<ConnectorHistoricalWriteFence, String> {
    if fence_record.identity.write_operation_id
        != Uuid::from_bytes(connector_operation_id.to_bytes())
    {
        return Err("the durable fence receipt belongs to another write operation".to_string());
    }
    let historical_fence_value = ConnectorExternalOperationFence::try_new(
        ConnectorClusterIdentity::try_from_digest(digest_bytes(
            &fence_record.identity.cluster_identity_digest,
            "cluster identity",
        )?)
        .map_err(|error| error.to_string())?,
        ConnectorExternalFenceGeneration::try_new(
            fence_record.identity.generation.control_plane_incarnation,
            fence_record.identity.generation.resource_epoch,
            fence_record.identity.generation.fence_generation,
        )
        .map_err(|error| error.to_string())?,
        connector_operation_id,
        *fence_record.identity.coordination_attempt_id.as_bytes(),
        table,
        target_ref,
    )
    .map_err(|error| error.to_string())?;
    if hex::encode(historical_fence_value.digest()) != fence_record.fence_digest {
        return Err(
            "the durable fence receipt does not describe the reconstructed fenced resource"
                .to_string(),
        );
    }
    let historical_receipt = ConnectorExternalFenceReceipt::try_new(
        &historical_fence_value,
        Bytes::from(fence_record.receipt_payload.as_bytes().to_vec()),
    )
    .map_err(|error| error.to_string())?;
    if hex::encode(historical_receipt.digest()) != fence_record.receipt_digest {
        return Err(
            "the durable external fence receipt digest does not seal its payload".to_string(),
        );
    }
    ConnectorHistoricalWriteFence::established(&historical_receipt, historical_fence_value)
        .map_err(|error| error.to_string())
}

/// Rebuild the historical facts, proving every reconstructed identity against
/// the digests the durable fence receipt already sealed.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn historical_write_facts(
    stored: &StoredOperation,
    fence_record: &DmlExternalFenceReceiptRecord,
    proposal: &DmlExternalFenceProposal,
    durable: Option<&DmlHistoricalWriteRecoveryRecord>,
) -> Result<HistoricalWriteFacts, String> {
    let connector_operation_id =
        ConnectorWriteOperationId::from_bytes(*fence_record.identity.write_operation_id.as_bytes());
    let table = table_identity(&stored.target)?;
    let target_ref = target_ref(&stored.target)?;

    // Rebuilding the fence value and comparing it against the sealed digest is
    // what proves the reconstructed table and target ref are the exact fenced
    // resource. A mismatch means the frontend cannot name what was fenced, and
    // guessing is forbidden.
    let historical_fence = reconstruct_historical_write_fence(
        fence_record,
        connector_operation_id,
        table.clone(),
        target_ref.clone(),
    )?;

    // The raised fence is minted from the live lease of *this* recovery attempt
    // and must strictly supersede the historical one; otherwise the historical
    // authority is still able to commit and nothing may be classified.
    let raised_fence = proposal
        .seal(connector_operation_id, table.clone(), target_ref.clone())
        .map_err(|error| error.to_string())?;
    let historical_value = historical_fence
        .fence()
        .ok_or_else(|| "the historical fence value is missing".to_string())?;
    if !raised_fence
        .supersedes(historical_value)
        .map_err(|error| error.to_string())?
    {
        return Err(
            "this recovery attempt cannot raise an external fence strictly above the historical one"
                .to_string(),
        );
    }

    let evidence = durable_evidence(stored)?;
    let historical_binding = historical_binding(&table, evidence.as_ref());
    let request = match durable {
        // A durable request is immutable: a later cycle inspects exactly the
        // same immutable input, so it is reused verbatim rather than rederived
        // from a lifecycle that may have moved on.
        Some(existing) => existing.request.clone(),
        None => {
            historical_request_record(stored, fence_record, &historical_binding, evidence.as_ref())?
        }
    };
    if request.write_operation_id != fence_record.identity.write_operation_id {
        return Err(
            "the durable historical write request names another connector write operation"
                .to_string(),
        );
    }

    Ok(HistoricalWriteFacts {
        table,
        target_ref,
        connector_operation_id,
        intent: write_intent(stored.operation_kind),
        historical_binding: request_binding(&request)?,
        historical_fence,
        raised_fence,
        raised_fence_receipt_digest: [0; 32],
        cohort_set_digest: digest_bytes(&request.cohort_set_digest, "cohort set")?,
        aggregate_digest: request
            .aggregate_write_digest
            .as_deref()
            .map(|digest| digest_bytes(digest, "aggregate write"))
            .transpose()?,
        checkpoints: checkpoints_from_request(&request),
        evidence,
        recovery_attempt_id: proposal.coordination_attempt_id(),
        request,
    })
}

/// Build the immutable historical write request record.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn historical_request_record(
    stored: &StoredOperation,
    fence_record: &DmlExternalFenceReceiptRecord,
    historical_binding: &ConnectorExecutionBindingKey,
    evidence: Option<&ExternalMutationEvidence>,
) -> Result<DmlHistoricalWriteRequestRecord, String> {
    let dispatch_certainty = dispatch_certainty(stored.state);
    let writer_output_checkpointed = dispatch_certainty
        != DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
        && writer_output_checkpointed(stored.state);
    let commit_dispatched_at_ms =
        if dispatch_certainty == DmlHistoricalDispatchCertainty::ConfirmedNotDispatched {
            None
        } else if commit_dispatched(stored.state) {
            Some(stored.updated_at_ms.max(0))
        } else {
            None
        };
    let mut request = DmlHistoricalWriteRequestRecord {
        old_provider_id: evidence
            .map(|evidence| evidence.descriptor().provider_id.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        old_connector_instance_id: historical_binding.instance_id.as_str().to_string(),
        old_connector_incarnation: hex::encode(historical_binding.incarnation.to_bytes()),
        old_coordination_attempt_id: Some(fence_record.identity.coordination_attempt_id),
        old_fence: Some(fence_record.clone()),
        write_operation_id: fence_record.identity.write_operation_id,
        cohort_set_digest: historical_input_digest(stored, fence_record),
        aggregate_write_digest: None,
        dispatch_certainty,
        writer_output_checkpointed,
        commit_dispatched_at_ms,
        request_digest: String::new(),
    };
    request.request_digest = request_digest(&request)?;
    Ok(request)
}

/// The historical connector generation, taken from durable SPI evidence when the
/// lifecycle holds it.
///
/// A `CommitUnknown` lifecycle carries an `ExternalMutationEvidence` whose
/// provider descriptor and incarnation are *neutral SPI fields*, not payload:
/// reading them decodes nothing. Every other lifecycle leaves the historical
/// generation unknown.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn historical_binding(
    table: &ConnectorTableIdentity,
    evidence: Option<&ExternalMutationEvidence>,
) -> ConnectorExecutionBindingKey {
    let incarnation = evidence
        .filter(|evidence| evidence.descriptor().instance_id == table.instance_id)
        .map_or_else(
            || ConnectorInstanceIncarnation::from_bytes(UNKNOWN_HISTORICAL_INCARNATION),
            ExternalMutationEvidence::incarnation,
        );
    ConnectorExecutionBindingKey {
        instance_id: table.instance_id.clone(),
        incarnation,
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn request_binding(
    request: &DmlHistoricalWriteRequestRecord,
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

/// The durable SPI reconciliation evidence of this operation, if it has any.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn durable_evidence(stored: &StoredOperation) -> Result<Option<ExternalMutationEvidence>, String> {
    let OperationPayload::ConnectorWriteLifecycle(ConnectorWriteLifecycleRecord::CommitUnknown {
        evidence_wire,
        ..
    }) = &stored.payload
    else {
        return Ok(None);
    };
    evidence_wire.try_decode().map(Some)
}

/// The frontend-owned sealed digest of the old immutable input.
///
/// The frontend does not durably hold the connector's sealed writer cohort set
/// digest — it is sealed inside the provider at commit time — so this is the
/// strongest immutable-input digest the frontend owns: the stable operation
/// identity plus the fenced resource and the exact fence the historical attempt
/// confirmed. A provider that needs its own cohort digest to prove `Applied`
/// answers `Ambiguous` here, which keeps the record unresolved rather than
/// producing a wrong classification.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn historical_input_digest(
    stored: &StoredOperation,
    fence_record: &DmlExternalFenceReceiptRecord,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DML_HISTORICAL_WRITE_INPUT_DOMAIN);
    hasher.update(stored.operation_id.as_uuid().as_bytes());
    hasher.update(fence_record.identity.write_operation_id.as_bytes());
    hasher.update(fence_record.identity.cluster_identity_digest.as_bytes());
    hasher.update(fence_record.identity.resource_digest.as_bytes());
    hasher.update(fence_record.fence_digest.as_bytes());
    hex::encode(hasher.finalize())
}

/// Digest over the complete immutable request, excluding the digest field.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn request_digest(request: &DmlHistoricalWriteRequestRecord) -> Result<String, String> {
    let mut hasher = Sha256::new();
    hasher.update(DML_HISTORICAL_WRITE_REQUEST_DOMAIN);
    hasher.update(serde_json::to_vec(request).map_err(|error| error.to_string())?);
    Ok(hex::encode(hasher.finalize()))
}

/// Project the raised fence and its provider receipt into the durable record.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn raised_fence_record(
    facts: &HistoricalWriteFacts,
    receipt: &ConnectorExternalFenceReceipt,
    now_ms: i64,
) -> Result<DmlExternalFenceReceiptRecord, DmlError> {
    let generation = facts.raised_fence.generation();
    Ok(DmlExternalFenceReceiptRecord {
        codec_version: crate::dml::model::DML_EXTERNAL_FENCE_CODEC_VERSION,
        identity: crate::dml::model::DmlExternalFenceIdentity {
            cluster_identity_digest: hex::encode(facts.raised_fence.cluster().digest()),
            // The fenced resource never changes across a takeover, so the
            // sealed resource digest of the historical receipt is reused rather
            // than recomputed from a second hashing rule.
            resource_digest: facts.request.old_fence.as_ref().map_or_else(
                || hex::encode([0u8; 32]),
                |fence| fence.identity.resource_digest.clone(),
            ),
            write_operation_id: Uuid::from_bytes(facts.raised_fence.operation_id().to_bytes()),
            coordination_attempt_id: Uuid::from_bytes(facts.raised_fence.coordination_attempt_id()),
            generation: crate::dml::model::DmlExternalFenceGeneration {
                control_plane_incarnation: generation.control_plane_incarnation(),
                resource_epoch: generation.resource_epoch(),
                fence_generation: generation.coordination_attempt(),
            },
        },
        fence_digest: hex::encode(facts.raised_fence.digest()),
        receipt_digest: hex::encode(receipt.digest()),
        receipt_payload: crate::dml::model::DmlOpaquePayload::try_new(receipt.payload().to_vec())
            .map_err(DmlError::journal_corruption)?,
        established_at_ms: now_ms.max(0),
    })
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn table_identity(target: &OperationTarget) -> Result<ConnectorTableIdentity, String> {
    Ok(ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse(target.catalog.as_str())
            .map_err(|error| error.to_string())?,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    })
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn target_ref(target: &OperationTarget) -> Result<ConnectorWriteTargetRef, String> {
    match target.ref_name.as_deref() {
        None => Ok(ConnectorWriteTargetRef::main()),
        Some(name) => {
            ConnectorWriteTargetRef::parse(name.to_string()).map_err(|error| error.to_string())
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn write_intent(kind: OperationKind) -> ConnectorWriteIntent {
    // The journal records the statement family, not the physical
    // partition-overwrite refinement. A provider classifies by stable operation
    // identity and external fence, so the refinement is not load bearing here.
    match kind {
        OperationKind::InsertOverwrite => ConnectorWriteIntent::Overwrite,
        OperationKind::RowDelta => ConnectorWriteIntent::RowDelta,
        _ => ConnectorWriteIntent::Append,
    }
}

/// What the durable operation state proves about the historical dispatch.
///
/// Only `Preparing` proves nothing left the frontend. Everything from `Writing`
/// on may have produced an irreversible external effect, and nothing here may
/// be softened into "not dispatched".
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn dispatch_certainty(state: OperationState) -> DmlHistoricalDispatchCertainty {
    match state {
        OperationState::Preparing => DmlHistoricalDispatchCertainty::ConfirmedNotDispatched,
        OperationState::Writing
        | OperationState::Collecting
        | OperationState::Committing
        | OperationState::CommitUnknown
        | OperationState::Aborting => DmlHistoricalDispatchCertainty::PossiblyDispatched,
        OperationState::Committed
        | OperationState::Finalizing
        | OperationState::Finalized
        | OperationState::Aborted
        | OperationState::FailedKnownUncommitted
        | OperationState::FinalizeFailedKnownCommitted => {
            DmlHistoricalDispatchCertainty::ConfirmedDispatched
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn writer_output_checkpointed(state: OperationState) -> bool {
    matches!(
        state,
        OperationState::Collecting
            | OperationState::Committing
            | OperationState::CommitUnknown
            | OperationState::Committed
            | OperationState::Finalizing
            | OperationState::Finalized
            | OperationState::FinalizeFailedKnownCommitted
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn commit_dispatched(state: OperationState) -> bool {
    matches!(
        state,
        OperationState::Committing
            | OperationState::CommitUnknown
            | OperationState::Committed
            | OperationState::Finalizing
            | OperationState::Finalized
            | OperationState::Aborting
            | OperationState::Aborted
            | OperationState::FailedKnownUncommitted
            | OperationState::FinalizeFailedKnownCommitted
    )
}

/// Project the immutable request onto the SPI dispatch checkpoints.
///
/// Deriving them from the durable request (not from the live operation state)
/// keeps the sealed request stable across recovery cycles even after the
/// statement result has been published.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn checkpoints_from_request(
    request: &DmlHistoricalWriteRequestRecord,
) -> Vec<ConnectorHistoricalWriteCheckpoint> {
    let confirmed_not_dispatched =
        request.dispatch_certainty == DmlHistoricalDispatchCertainty::ConfirmedNotDispatched;
    let unknown_or_not_dispatched = if confirmed_not_dispatched {
        ConnectorHistoricalWriteDispatchState::NotDispatched
    } else {
        ConnectorHistoricalWriteDispatchState::Unknown
    };
    let writers = match request.dispatch_certainty {
        DmlHistoricalDispatchCertainty::ConfirmedNotDispatched => {
            ConnectorHistoricalWriteDispatchState::NotDispatched
        }
        DmlHistoricalDispatchCertainty::PossiblyDispatched => {
            ConnectorHistoricalWriteDispatchState::Unknown
        }
        DmlHistoricalDispatchCertainty::ConfirmedDispatched => {
            ConnectorHistoricalWriteDispatchState::Dispatched
        }
    };
    vec![
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::Activated,
            state: ConnectorHistoricalWriteDispatchState::Completed,
            evidence_digest: None,
        },
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::FenceEstablished,
            state: if request.old_fence.is_some() {
                ConnectorHistoricalWriteDispatchState::Completed
            } else {
                ConnectorHistoricalWriteDispatchState::NotDispatched
            },
            evidence_digest: None,
        },
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::WritersDispatched,
            state: writers,
            evidence_digest: None,
        },
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::WritersCompleted,
            state: if request.writer_output_checkpointed {
                ConnectorHistoricalWriteDispatchState::Completed
            } else {
                unknown_or_not_dispatched
            },
            evidence_digest: None,
        },
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::CommitDispatched,
            state: if request.commit_dispatched_at_ms.is_some() {
                ConnectorHistoricalWriteDispatchState::Dispatched
            } else {
                unknown_or_not_dispatched
            },
            evidence_digest: None,
        },
    ]
}

fn digest_bytes(value: &str, label: &str) -> Result<[u8; 32], String> {
    let mut digest = [0u8; 32];
    hex::decode_to_slice(value, &mut digest)
        .map_err(|error| format!("DML {label} digest is unusable: {error}"))?;
    Ok(digest)
}

// ---------------------------------------------------------------------------
// Phase and due arithmetic
// ---------------------------------------------------------------------------

/// The cycle a post-raise write belongs to.
///
/// A raise minted by a different coordination attempt opens a new cycle: one
/// cycle is owned by exactly one attempt, and a phase may never rewind inside
/// a cycle.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn next_cycle(existing: &DmlHistoricalWriteRecoveryRecord, attempt: Uuid) -> u32 {
    if existing.recovery_attempt_id.as_u128() == attempt.as_u128() {
        existing.recovery_cycle
    } else {
        existing.recovery_cycle.saturating_add(1)
    }
}

/// The phase a record reaches once this cycle has raised its external fence.
///
/// A carried result must keep its cleanup obligation visible, so a resumed
/// cycle over a pending cleanup stays in `CleanupPending` rather than pretending
/// it has no result yet.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn phase_after_raise(
    carried: Option<&DmlHistoricalWriteResultRecord>,
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
/// the statement fact and the guarded cleanup have both been dealt with.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn inspected_phase_for(outcome: &HistoricalWriteOutcome) -> DmlHistoricalRecoveryPhase {
    if !outcome.resolved {
        DmlHistoricalRecoveryPhase::Unresolved
    } else if outcome.cleanup_required {
        DmlHistoricalRecoveryPhase::CleanupPending
    } else {
        DmlHistoricalRecoveryPhase::Inspected
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn final_phase_for(
    progress: WriteRecoveryProgress,
    cleanup: DmlHistoricalCleanupState,
) -> DmlHistoricalRecoveryPhase {
    match cleanup {
        DmlHistoricalCleanupState::Pending => DmlHistoricalRecoveryPhase::CleanupPending,
        _ => match progress {
            WriteRecoveryProgress::Resolved => DmlHistoricalRecoveryPhase::Resolved,
            WriteRecoveryProgress::Unresolved => DmlHistoricalRecoveryPhase::Unresolved,
            _ => DmlHistoricalRecoveryPhase::Inspected,
        },
    }
}

/// The next action a non-terminal phase advertises.
///
/// Only a resolved record may advertise `None`, and a record kept for manual
/// retention must say so.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
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

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
const fn delay_for(progress: WriteRecoveryProgress) -> i64 {
    match progress {
        WriteRecoveryProgress::CleanupPending => DML_WRITE_RECOVERY_CLEANUP_DELAY_MS,
        WriteRecoveryProgress::Superseded | WriteRecoveryProgress::ContinuationPending => {
            DML_WRITE_RECOVERY_BLOCKED_DELAY_MS
        }
        _ => DML_WRITE_RECOVERY_UNRESOLVED_DELAY_MS,
    }
}

/// The recovery due one record still requires.
///
/// The obligation is computed from the operation *and* its historical record, so
/// a pending cleanup or an open cycle cannot be dropped because the
/// user-visible statement result already became terminal (spec D5).
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn recovery_due_for(
    stored: &StoredOperation,
    record: &DmlHistoricalWriteRecoveryRecord,
    next_due_ms: i64,
) -> Option<i64> {
    if operation_requires_recovery_scan(stored.state, &stored.payload, Some(record)) {
        Some(next_due_ms.max(0))
    } else {
        None
    }
}

/// Publish one terminal statement fact for a recovered operation.
///
/// The lifecycle state machine is never widened: when the current state cannot
/// reach the fact directly, the only bridge taken is the ordinary
/// `-> Committing` edge that already exists for every pre-commit state.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn publish_terminal(
    ledger: &mut dyn HistoricalWriteRecoveryLedger,
    fact: OperationFact,
) -> Result<(), DmlError> {
    let current = ledger.stored().state;
    let target = fact.state;
    if validate_operation_transition(current, target).is_err() {
        if validate_operation_transition(current, OperationState::Committing).is_ok()
            && validate_operation_transition(OperationState::Committing, target).is_ok()
        {
            ledger.transition(OperationState::Committing)?;
        } else {
            return Err(DmlError::journal_unresolved(format!(
                "historical write recovery cannot publish state {} from {}",
                target.as_str(),
                current.as_str()
            ))
            .with_operation_id(ledger.stored().operation_id));
        }
    }
    let finalize = matches!(
        fact.lifecycle,
        ConnectorWriteLifecycleRecord::KnownEmpty
            | ConnectorWriteLifecycleRecord::KnownCommitted {
                finalization: ConnectorWriteFinalizationRecord::Complete,
                ..
            }
    );
    ledger.publish_fact(fact)?;
    if finalize && ledger.stored().state == OperationState::Committed {
        // The provider already reported a complete external finalization, so
        // there is nothing left to finalize; only the durable state has to
        // catch up. A failed finalization is deliberately left visible.
        ledger.transition(OperationState::Finalized)?;
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
fn request_context() -> Result<ConnectorRequestContext, String> {
    ConnectorRequestContext::try_new(
        Instant::now() + DML_WRITE_RECOVERY_ACTION_DEADLINE,
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Whether this error means the profile lost its authority rather than hit a
/// transient failure. Used by the controller to decide how loudly to log.
#[allow(
    dead_code,
    reason = "Retained for staged DML recovery and durable-journal integration."
)]
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
        ConnectorCommittedVersion, ConnectorHistoricalWriteApplication,
        ConnectorHistoricalWriteOutcomeFacts, ConnectorHistoricalWriteProof,
        ConnectorInstanceDescriptor, ConnectorMutationFailure, ConnectorMutationFailureKind,
        ConnectorProviderId, ConnectorWriteReceipt, ExternalMutationEffect,
    };

    use super::*;
    use crate::dml::model::{
        DML_EXTERNAL_FENCE_CODEC_VERSION, DML_OPERATION_SCHEMA_VERSION, DmlExternalFenceGeneration,
        DmlExternalFenceIdentity, DmlOpaquePayload, DmlOperationId,
    };

    const CLUSTER: &str = "nova-cp3b-profile";
    const CATALOG: &str = "catalog.lake";
    const NAMESPACE: &str = "db";
    const TABLE: &str = "orders";
    const HISTORICAL_INCARNATION: [u8; 16] = [4; 16];
    const CURRENT_INCARNATION: [u8; 16] = [9; 16];
    const PROVIDER_MARKER: &str = "PROVIDER-PRIVATE-BODY";

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

    fn connector_operation_id() -> ConnectorWriteOperationId {
        ConnectorWriteOperationId::from_bytes([6; 16])
    }

    fn write_operation_uuid() -> Uuid {
        Uuid::from_bytes(connector_operation_id().to_bytes())
    }

    fn marker(fence: &ConnectorExternalOperationFence) -> Bytes {
        Bytes::from(format!(
            "{PROVIDER_MARKER}|marker|{}",
            hex::encode(fence.digest())
        ))
    }

    fn fence(epoch: u64, generation: u64, attempt: [u8; 16]) -> ConnectorExternalOperationFence {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive(CLUSTER).expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(1, epoch, generation).expect("generation"),
            connector_operation_id(),
            attempt,
            table(),
            ConnectorWriteTargetRef::main(),
        )
        .expect("fence")
    }

    fn historical_fence_value() -> ConnectorExternalOperationFence {
        fence(2, 10, *historical_attempt().as_bytes())
    }

    fn historical_attempt() -> Uuid {
        // A fixed UUIDv7 so the durable record shape stays deterministic.
        Uuid::parse_str("018f0000-0000-7000-8000-000000000001").expect("uuid v7")
    }

    fn recovery_attempt() -> Uuid {
        Uuid::parse_str("018f0000-0000-7000-8000-000000000002").expect("uuid v7")
    }

    fn fence_record() -> DmlExternalFenceReceiptRecord {
        let value = historical_fence_value();
        let receipt = ConnectorExternalFenceReceipt::try_new(&value, marker(&value))
            .expect("historical receipt");
        let generation = value.generation();
        DmlExternalFenceReceiptRecord {
            codec_version: DML_EXTERNAL_FENCE_CODEC_VERSION,
            identity: DmlExternalFenceIdentity {
                cluster_identity_digest: hex::encode(value.cluster().digest()),
                resource_digest: hex::encode([3u8; 32]),
                write_operation_id: write_operation_uuid(),
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
        }
    }

    fn proposal(epoch: u64, generation: u64) -> DmlExternalFenceProposal {
        DmlExternalFenceProposal::testing(
            DmlOperationId::from(Uuid::now_v7()),
            CLUSTER,
            recovery_attempt(),
            DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: epoch,
                fence_generation: generation,
            },
        )
        .expect("proposal")
    }

    fn stored(state: OperationState) -> StoredOperation {
        StoredOperation {
            schema_version: DML_OPERATION_SCHEMA_VERSION,
            operation_id: DmlOperationId::from(
                Uuid::parse_str("018f0000-0000-7000-8000-00000000000a").expect("uuid v7"),
            ),
            revision: 4,
            last_mutation_id: Uuid::now_v7(),
            operation_kind: OperationKind::RowDelta,
            operation_subkind: Some("UPDATE".to_string()),
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
            payload: OperationPayload::ConnectorWriteLifecycle(
                ConnectorWriteLifecycleRecord::Pending,
            ),
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
        Fact(OperationState),
        Transition(OperationState),
        Reschedule(Option<i64>),
    }

    struct FakeLedger {
        stored: StoredOperation,
        fence: Option<DmlExternalFenceReceiptRecord>,
        recovery: Option<DmlHistoricalWriteRecoveryRecord>,
        proposal: DmlExternalFenceProposal,
        events: Vec<LedgerEvent>,
        reject_persist: bool,
    }

    impl FakeLedger {
        fn new(state: OperationState) -> Self {
            Self {
                stored: stored(state),
                fence: Some(fence_record()),
                recovery: None,
                proposal: proposal(3, 20),
                events: Vec::new(),
                reject_persist: false,
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
    }

    impl HistoricalWriteRecoveryLedger for FakeLedger {
        fn stored(&self) -> &StoredOperation {
            &self.stored
        }

        fn check_authority(&self) -> Result<(), DmlError> {
            Ok(())
        }

        fn load_external_fence(&self) -> Result<Option<DmlExternalFenceReceiptRecord>, DmlError> {
            Ok(self.fence.clone())
        }

        fn load_recovery(&self) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
            Ok(self.recovery.clone())
        }

        fn external_fence_proposal(&self) -> Result<DmlExternalFenceProposal, DmlError> {
            Ok(self.proposal.clone())
        }

        fn persist_recovery(
            &mut self,
            recovery: DmlHistoricalWriteRecoveryRecord,
            recovery_due_at_ms: Option<i64>,
        ) -> Result<(), DmlError> {
            if self.reject_persist {
                return Err(DmlError::journal_unresolved("persist refused"));
            }
            // Enforce the real journal's transition rules so a profile bug
            // cannot pass here and fail against SQLite.
            crate::dml::model::validate_historical_write_recovery_transition(
                self.recovery.as_ref(),
                &recovery,
            )
            .map_err(DmlError::journal_corruption)?;
            let expected = operation_requires_recovery_scan(
                self.stored.state,
                &self.stored.payload,
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

        fn publish_fact(&mut self, fact: OperationFact) -> Result<(), DmlError> {
            validate_operation_transition(self.stored.state, fact.state)
                .map_err(DmlError::journal_unresolved)?;
            self.events.push(LedgerEvent::Fact(fact.state));
            self.stored.revision += 1;
            self.stored.state = fact.state;
            self.stored.payload = OperationPayload::ConnectorWriteLifecycle(fact.lifecycle);
            Ok(())
        }

        fn transition(&mut self, to: OperationState) -> Result<(), DmlError> {
            validate_operation_transition(self.stored.state, to)
                .map_err(DmlError::journal_unresolved)?;
            self.events.push(LedgerEvent::Transition(to));
            self.stored.revision += 1;
            self.stored.state = to;
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
        Applied { cleanup_required: bool },
        NotApplied,
        NotDispatched,
        Staged,
        Conflict,
        Ambiguous,
        RaiseSuperseded,
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

    impl ConnectorHistoricalWriteRecovery for FakeFacet {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn raise_external_fence(
            &self,
            request: ConnectorHistoricalWriteFenceRaiseRequest,
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
            descriptor: ConnectorHistoricalWriteDescriptor,
            _context: ConnectorRequestContext,
        ) -> Result<ConnectorHistoricalWriteObservation, ConnectorError> {
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
            let (disposition, application, continuation, cleanup_required) = match self.plan {
                FacetPlan::Applied { cleanup_required } => (
                    ConnectorHistoricalWriteDisposition::Applied,
                    Some(ConnectorHistoricalWriteApplication {
                        committed_version: ConnectorCommittedVersion::try_new(
                            Bytes::from_static(b"committed-version"),
                            Some(42),
                        )?,
                        receipt: ConnectorWriteReceipt::try_new(Bytes::from_static(
                            b"historical-receipt",
                        ))?,
                        finalization: ExternalMutationFinalization::Complete,
                    }),
                    None,
                    cleanup_required,
                ),
                FacetPlan::NotApplied => (
                    ConnectorHistoricalWriteDisposition::NotApplied,
                    None,
                    None,
                    true,
                ),
                FacetPlan::NotDispatched => (
                    ConnectorHistoricalWriteDisposition::NotDispatched,
                    None,
                    Some(ConnectorHistoricalWriteContinuation::try_new(
                        &descriptor.raised_fence,
                        Bytes::from(format!("{PROVIDER_MARKER}|continuation")),
                    )?),
                    false,
                ),
                FacetPlan::Staged => (
                    ConnectorHistoricalWriteDisposition::Staged,
                    None,
                    None,
                    true,
                ),
                FacetPlan::Conflict => (
                    ConnectorHistoricalWriteDisposition::Conflict,
                    None,
                    None,
                    false,
                ),
                FacetPlan::Ambiguous | FacetPlan::RaiseSuperseded => (
                    ConnectorHistoricalWriteDisposition::Ambiguous,
                    None,
                    None,
                    false,
                ),
            };
            let observation = ConnectorHistoricalWriteObservation::try_new(
                &descriptor,
                disposition,
                ConnectorHistoricalWriteOutcomeFacts {
                    application,
                    continuation,
                    cleanup_required,
                },
                ConnectorHistoricalWriteProof::try_new(Bytes::from(format!(
                    "{PROVIDER_MARKER}|proof|{disposition:?}"
                )))?,
            )?;
            state.issued.push(observation.digest());
            Ok(observation)
        }

        fn cleanup(
            &self,
            request: ConnectorHistoricalWriteCleanupRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
        {
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
            let receipt = ConnectorHistoricalWriteCleanupReceipt {
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
                            provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                            instance_id: instance(),
                        },
                        ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
                        novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                            request.operation_id.to_bytes(),
                        ),
                        "historical-write-cleanup",
                        Bytes::from(format!("{PROVIDER_MARKER}|cleanup-evidence")),
                    )?,
                }),
            }
        }

        fn reconcile_cleanup(
            &self,
            _operation_id: ConnectorWriteOperationId,
            _evidence: ExternalMutationEvidence,
            _context: ConnectorRequestContext,
        ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
        {
            self.state
                .lock()
                .expect("facet state")
                .events
                .push(FacetEvent::ReconcileCleanup);
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: ConnectorHistoricalWriteCleanupReceipt {
                    descriptor_digest: [0; 32],
                    observation_digest: [0; 32],
                },
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    struct FixedResolver {
        facet: Arc<FakeFacet>,
    }

    impl HistoricalWriteRecoveryResolver for FixedResolver {
        fn resolve(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<HistoricalWriteRecoveryHandle, ConnectorError> {
            Ok(HistoricalWriteRecoveryHandle::new(
                "fake".to_string(),
                Arc::clone(&self.facet) as Arc<dyn ConnectorHistoricalWriteRecovery>,
            ))
        }
    }

    struct MissingFacetResolver;

    impl HistoricalWriteRecoveryResolver for MissingFacetResolver {
        fn resolve(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<HistoricalWriteRecoveryHandle, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation has no historical write recovery capability",
            ))
        }
    }

    fn profile(facet: &Arc<FakeFacet>) -> WriteRecoveryProfile {
        WriteRecoveryProfile::new(Arc::new(FixedResolver {
            facet: Arc::clone(facet),
        }))
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn the_takeover_order_raises_the_fence_before_it_inspects_anything() {
        let facet = FakeFacet::new(FacetPlan::Staged, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Committing);
        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Resolved);

        let events = facet.events();
        let raise = events
            .iter()
            .position(|event| matches!(event, FacetEvent::Raise(_)))
            .expect("the profile must raise the external fence");
        let inspect = events
            .iter()
            .position(|event| matches!(event, FacetEvent::Inspect(_)))
            .expect("the profile must inspect the historical operation");
        let cleanup = events
            .iter()
            .position(|event| matches!(event, FacetEvent::Cleanup(_)))
            .expect("a staged disposition must run its guarded cleanup");
        assert!(
            raise < inspect,
            "spec D2: the raised fence must close the historical authority strictly before any \
             classification, but the provider observed {events:?}"
        );
        assert!(
            inspect < cleanup,
            "cleanup must act on the observation the inspection returned: {events:?}"
        );

        // The request is durable before the fence is raised, and the typed
        // result is durable before the cleanup runs.
        assert_eq!(
            ledger.phases(),
            vec![
                DmlHistoricalRecoveryPhase::Requested,
                DmlHistoricalRecoveryPhase::FenceRaised,
                DmlHistoricalRecoveryPhase::CleanupPending,
                DmlHistoricalRecoveryPhase::Resolved,
            ],
            "the durable phases must follow the D2 order exactly"
        );
        let persisted_before_cleanup = ledger
            .events
            .iter()
            .filter(|event| matches!(event, LedgerEvent::Persist(..)))
            .count();
        assert!(
            persisted_before_cleanup >= 3,
            "the typed result must be persisted before the guarded cleanup is requested"
        );
    }

    #[test]
    fn a_missing_fence_receipt_reschedules_instead_of_concluding() {
        let facet = FakeFacet::new(FacetPlan::NotApplied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Writing);
        ledger.fence = None;
        let progress = profile(&facet)
            .drive(&mut ledger, 5_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Unresolved);
        assert!(
            facet.events().is_empty(),
            "without a durable fence receipt the frontend cannot name the historical write \
             operation, so it must not reach the provider at all"
        );
        assert_eq!(
            ledger.events,
            vec![LedgerEvent::Reschedule(Some(
                5_000 + DML_WRITE_RECOVERY_UNRESOLVED_DELAY_MS
            ))],
            "insufficient evidence must move the due and change nothing else"
        );
        assert!(
            ledger.recovery.is_none(),
            "no classification may be recorded from missing evidence"
        );
    }

    #[test]
    fn a_fence_that_cannot_be_raised_higher_never_classifies_the_operation() {
        let facet = FakeFacet::new(FacetPlan::NotApplied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Committing);
        // Same generation as the historical fence: the historical authority
        // would still be able to commit.
        ledger.proposal = proposal(2, 10);
        let progress = profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Unresolved);
        assert!(
            facet.events().is_empty(),
            "a takeover that cannot fence out the historical authority must not inspect anything"
        );
    }

    #[test]
    fn a_stale_response_leaves_the_journal_untouched() {
        // The D5 double check itself: a response minted under another fence
        // must be refused, and the refusal must be typed stale.
        let historical = historical_fence_value();
        let receipt = ConnectorExternalFenceReceipt::try_new(&historical, marker(&historical))
            .expect("receipt");
        let established =
            ConnectorHistoricalWriteFence::established(&receipt, historical).expect("established");
        let raised = fence(3, 20, *recovery_attempt().as_bytes());
        let raised_receipt =
            ConnectorExternalFenceReceipt::try_new(&raised, marker(&raised)).expect("receipt");
        let descriptor = ConnectorHistoricalWriteDescriptor::try_new(
            ConnectorHistoricalWriteIdentity {
                historical_binding: ConnectorExecutionBindingKey {
                    instance_id: instance(),
                    incarnation: ConnectorInstanceIncarnation::from_bytes(HISTORICAL_INCARNATION),
                },
                table: table(),
                target_ref: ConnectorWriteTargetRef::main(),
                operation_id: connector_operation_id(),
                intent: ConnectorWriteIntent::RowDelta,
                cohort_set_digest: [7; 32],
                aggregate_digest: None,
            },
            ConnectorHistoricalWriteFenceFacts {
                historical_fence: established,
                raised_fence: raised.clone(),
                raised_fence_receipt_digest: raised_receipt.digest(),
            },
            vec![ConnectorHistoricalWriteCheckpoint {
                phase: ConnectorHistoricalWritePhase::CommitDispatched,
                state: ConnectorHistoricalWriteDispatchState::Unknown,
                evidence_digest: None,
            }],
            None,
        )
        .expect("descriptor");
        let observation = ConnectorHistoricalWriteObservation::try_new(
            &descriptor,
            ConnectorHistoricalWriteDisposition::NotApplied,
            ConnectorHistoricalWriteOutcomeFacts {
                application: None,
                continuation: None,
                cleanup_required: true,
            },
            ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"proof")).expect("proof"),
        )
        .expect("observation");

        validate_historical_response(&observation, &descriptor, raised.digest())
            .expect("the current response applies");
        let error = validate_historical_response(&observation, &descriptor, [1; 32])
            .expect_err("a response under a superseded fence must be refused");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
        assert!(!error.retryable_before_progress());
    }

    #[test]
    fn a_superseded_recovery_attempt_records_no_terminal_statement_result() {
        for plan in [FacetPlan::RaiseSuperseded, FacetPlan::Conflict] {
            let facet = FakeFacet::new(plan, FacetCleanup::Complete);
            let mut ledger = FakeLedger::new(OperationState::Committing);
            let progress = profile(&facet)
                .drive(&mut ledger, 2_000)
                .expect("one recovery cycle");
            assert_eq!(
                progress,
                WriteRecoveryProgress::Superseded,
                "a superseded recovery attempt must be re-driven under the new authority"
            );
            assert!(
                !ledger
                    .events
                    .iter()
                    .any(|event| matches!(event, LedgerEvent::Fact(_))),
                "a superseded recovery attempt must not settle the statement"
            );
            assert_ne!(
                ledger.recovery.as_ref().map(|record| record.phase),
                Some(DmlHistoricalRecoveryPhase::Resolved),
                "a superseded recovery attempt must leave the record open"
            );
        }
    }

    /// A provider proof of non-dispatch is necessary but not sufficient.
    ///
    /// The external fence is only established after the operation has already
    /// transitioned to `Writing`, so any operation that owns a fence receipt is
    /// recorded as *possibly* dispatched. `journal_proves_nothing_dispatched`
    /// therefore cannot hold, and the SPI refuses the continuation the provider
    /// offered -- deliberately, because replaying a possibly-dispatched write is
    /// exactly what the design forbids.
    ///
    /// The safe convergence is to keep the record open with the fence intact.
    /// Issuing continuations for real would need a durable "writer dispatch
    /// started" checkpoint that this change does not add; see ADR-0068.
    #[test]
    fn a_provider_proof_of_non_dispatch_alone_does_not_earn_a_continuation() {
        let facet = FakeFacet::new(FacetPlan::NotDispatched, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Writing);
        let progress = profile(&facet)
            .drive(&mut ledger, 3_000)
            .expect("one recovery cycle");
        assert_eq!(
            progress,
            WriteRecoveryProgress::Unresolved,
            "a journal that cannot prove non-dispatch must leave the operation unresolved"
        );
        assert!(
            !facet
                .events()
                .iter()
                .any(|event| matches!(event, FacetEvent::Cleanup(_))),
            "a not-dispatched operation must never have its fence retired: that would reopen the \
             historical authority"
        );
        let record = ledger.recovery.expect("durable record");
        assert_ne!(
            record.phase,
            DmlHistoricalRecoveryPhase::Resolved,
            "an unresolved record stays open for a later cycle"
        );
        assert!(
            record.result.is_none(),
            "a refused observation must not be published as a typed result"
        );
        assert!(
            !ledger
                .events
                .iter()
                .any(|event| matches!(event, LedgerEvent::Fact(_))),
            "nothing was concluded, so no statement fact may be settled"
        );
    }

    #[test]
    fn an_ambiguous_disposition_reschedules_and_publishes_no_fact() {
        let facet = FakeFacet::new(FacetPlan::Ambiguous, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::CommitUnknown);
        let progress = profile(&facet)
            .drive(&mut ledger, 7_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Unresolved);
        let record = ledger.recovery.clone().expect("durable record");
        assert_eq!(record.phase, DmlHistoricalRecoveryPhase::Unresolved);
        assert!(record.requires_recovery_scan());
        assert!(
            !ledger
                .events
                .iter()
                .any(|event| matches!(event, LedgerEvent::Fact(_))),
            "unreadable external truth must never settle the statement"
        );
    }

    #[test]
    fn a_pending_cleanup_survives_a_terminal_user_visible_result() {
        {
            let cleanup = FacetCleanup::Refused;
            let facet = FakeFacet::new(FacetPlan::NotApplied, cleanup);
            let mut ledger = FakeLedger::new(OperationState::Committing);
            let progress = profile(&facet)
                .drive(&mut ledger, 4_000)
                .expect("one recovery cycle");
            assert_eq!(progress, WriteRecoveryProgress::CleanupPending);
            assert_eq!(
                ledger.stored.state,
                OperationState::FailedKnownUncommitted,
                "a provably uncommitted operation settles as known uncommitted"
            );
            assert!(
                ledger.stored.state.is_finished(),
                "the user-visible result is terminal"
            );
            let record = ledger.recovery.clone().expect("durable record");
            assert_eq!(
                record.cleanup(),
                Some(DmlHistoricalCleanupState::Pending),
                "a terminal user-visible result must not drop the pending cleanup obligation"
            );
            assert!(
                record.requires_recovery_scan(),
                "the bounded scan must keep visiting an operation with a pending cleanup"
            );
            assert_eq!(
                ledger.stored.recovery_due_at_ms,
                Some(4_000 + DML_WRITE_RECOVERY_CLEANUP_DELAY_MS),
                "the retained obligation must keep a due"
            );
        }
    }

    #[test]
    fn a_lost_cleanup_result_is_reconciled_from_opaque_evidence_only() {
        let facet = FakeFacet::new(FacetPlan::Staged, FacetCleanup::Lost);
        let mut ledger = FakeLedger::new(OperationState::Committing);
        let progress = profile(&facet)
            .drive(&mut ledger, 6_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Resolved);
        assert!(
            facet.events().contains(&FacetEvent::ReconcileCleanup),
            "a lost cleanup result must be resolved through the historical facet's own cleanup \
             reconciliation"
        );
        let record = ledger.recovery.expect("durable record");
        assert_eq!(record.phase, DmlHistoricalRecoveryPhase::Resolved);
        assert_eq!(record.cleanup(), Some(DmlHistoricalCleanupState::Completed));
    }

    #[test]
    fn an_applied_operation_finalizes_with_the_provider_receipt() {
        let facet = FakeFacet::new(
            FacetPlan::Applied {
                cleanup_required: true,
            },
            FacetCleanup::Complete,
        );
        let mut ledger = FakeLedger::new(OperationState::CommitUnknown);
        let progress = profile(&facet)
            .drive(&mut ledger, 8_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Resolved);
        assert_eq!(ledger.stored.state, OperationState::Finalized);
        let OperationPayload::ConnectorWriteLifecycle(
            ConnectorWriteLifecycleRecord::KnownCommitted { receipt_wire, .. },
        ) = &ledger.stored.payload
        else {
            panic!("an applied historical write keeps the provider receipt");
        };
        assert_eq!(
            receipt_wire.try_decode().expect("receipt"),
            ConnectorWriteReceipt::try_new(Bytes::from_static(b"historical-receipt"))
                .expect("receipt")
        );
        assert_eq!(
            ledger.stored.recovery_due_at_ms, None,
            "a fully resolved operation releases the bounded scan"
        );
    }

    #[test]
    fn a_resolved_record_is_never_reopened_and_never_reaches_the_provider() {
        let facet = FakeFacet::new(FacetPlan::NotApplied, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Committing);
        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("first cycle");
        let resolved = ledger.recovery.clone().expect("durable record");
        assert_eq!(resolved.phase, DmlHistoricalRecoveryPhase::Resolved);
        let calls = facet.events().len();

        let progress = profile(&facet)
            .drive(&mut ledger, 2_000)
            .expect("second cycle");
        assert_eq!(progress, WriteRecoveryProgress::Resolved);
        assert_eq!(
            facet.events().len(),
            calls,
            "a resolved record must not reach the provider again"
        );
        assert_eq!(
            ledger.recovery,
            Some(resolved),
            "a resolved record is immutable"
        );
    }

    #[test]
    fn a_second_cycle_reuses_the_immutable_request_and_re_raises_the_fence() {
        let facet = FakeFacet::new(FacetPlan::Ambiguous, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Committing);
        profile(&facet)
            .drive(&mut ledger, 1_000)
            .expect("first cycle");
        let first = ledger.recovery.clone().expect("durable record");
        assert_eq!(first.recovery_cycle, 1);

        // A new owner claims the same operation under a later attempt.
        let later_attempt =
            Uuid::parse_str("018f0000-0000-7000-8000-000000000003").expect("uuid v7");
        ledger.proposal = DmlExternalFenceProposal::testing(
            DmlOperationId::from(Uuid::now_v7()),
            CLUSTER,
            later_attempt,
            DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 4,
                fence_generation: 30,
            },
        )
        .expect("proposal");
        profile(&facet)
            .drive(&mut ledger, 2_000)
            .expect("second cycle");
        let second = ledger.recovery.clone().expect("durable record");
        assert_eq!(
            second.request, first.request,
            "the historical write request is immutable once durable"
        );
        assert_eq!(second.recovery_cycle, 2);
        assert_eq!(second.recovery_attempt_id, later_attempt);
        let raises = facet
            .events()
            .iter()
            .filter(|event| matches!(event, FacetEvent::Raise(_)))
            .count();
        assert_eq!(
            raises, 2,
            "every cycle must raise its own strictly higher external fence before inspecting"
        );
    }

    #[test]
    fn a_missing_historical_facet_keeps_the_record_unresolved() {
        let profile = WriteRecoveryProfile::new(Arc::new(MissingFacetResolver));
        let mut ledger = FakeLedger::new(OperationState::Committing);
        let progress = profile
            .drive(&mut ledger, 1_000)
            .expect("one recovery cycle");
        assert_eq!(progress, WriteRecoveryProgress::Unresolved);
        assert_eq!(
            ledger.recovery.as_ref().map(|record| record.phase),
            Some(DmlHistoricalRecoveryPhase::Requested),
            "the immutable request is durable, but nothing was classified"
        );
    }

    #[test]
    fn only_the_distributed_write_family_is_driven_by_this_profile() {
        let facet = FakeFacet::new(FacetPlan::NotApplied, FacetCleanup::Complete);
        for kind in [
            OperationKind::MvRefresh,
            OperationKind::Maintenance,
            OperationKind::CreateTableAsSelect,
            OperationKind::Truncate,
            OperationKind::AddFiles,
        ] {
            assert!(
                !is_write_family(kind),
                "{kind:?} has its own recovery owner"
            );
            let mut ledger = FakeLedger::new(OperationState::Committing);
            ledger.stored.operation_kind = kind;
            assert_eq!(
                profile(&facet)
                    .drive(&mut ledger, 1_000)
                    .expect("one recovery cycle"),
                WriteRecoveryProgress::NotApplicable
            );
        }
        for kind in [
            OperationKind::InsertAppend,
            OperationKind::InsertOverwrite,
            OperationKind::RowDelta,
        ] {
            assert!(is_write_family(kind));
        }
    }

    #[test]
    fn the_frontend_visible_projection_never_carries_a_provider_payload() {
        let facet = FakeFacet::new(FacetPlan::NotDispatched, FacetCleanup::Complete);
        let mut ledger = FakeLedger::new(OperationState::Writing);
        profile(&facet).drive(&mut ledger, 1_000).expect("cycle");
        let record = ledger.recovery.expect("durable record");
        let rendered = format!("{record:?}");
        assert!(
            !rendered.contains(PROVIDER_MARKER),
            "the durable record must never render a provider payload body: {rendered}"
        );
    }

    #[test]
    fn cleanup_progress_retains_a_failed_finalization() {
        let receipt = ConnectorHistoricalWriteCleanupReceipt {
            descriptor_digest: [1; 32],
            observation_digest: [2; 32],
        };
        let complete = HistoricalCleanupProgress::of(&ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt.clone(),
            finalization: ExternalMutationFinalization::Complete,
        });
        assert_eq!(complete.state, DmlHistoricalCleanupState::Completed);
        assert!(!complete.is_pending());

        let failed = HistoricalCleanupProgress::of(&ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "finalization did not complete",
            )),
        });
        assert!(failed.is_pending());
        assert!(
            matches!(
                failed.retained_finalization,
                Some(ExternalMutationFinalization::Failed(_))
            ),
            "a failed finalization must be retained, never discarded"
        );

        let refused = HistoricalCleanupProgress::of(&ExternalMutationOutcome::<
            ConnectorHistoricalWriteCleanupReceipt,
        >::KnownUncommitted {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "cleanup could not run",
            ),
        });
        assert!(refused.is_pending());
        assert!(refused.unresolved_evidence.is_none());
    }

    #[test]
    fn a_confirmed_not_dispatched_request_proves_nothing_was_dispatched() {
        let record = historical_request_record(
            &stored(OperationState::Preparing),
            &fence_record(),
            &ConnectorExecutionBindingKey {
                instance_id: instance(),
                incarnation: ConnectorInstanceIncarnation::from_bytes(HISTORICAL_INCARNATION),
            },
            None,
        )
        .expect("request");
        assert_eq!(
            record.dispatch_certainty,
            DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
        );
        assert!(!record.writer_output_checkpointed);
        assert!(record.commit_dispatched_at_ms.is_none());
        let checkpoints = checkpoints_from_request(&record);
        assert!(checkpoints.iter().all(|checkpoint| !matches!(
            checkpoint.phase,
            ConnectorHistoricalWritePhase::WritersDispatched
                | ConnectorHistoricalWritePhase::WritersCompleted
                | ConnectorHistoricalWritePhase::CommitDispatched
        ) || checkpoint.state
            == ConnectorHistoricalWriteDispatchState::NotDispatched));

        // Everything from `Writing` on may already have left this cluster and
        // must never be softened into "nothing dispatched".
        for state in [
            OperationState::Writing,
            OperationState::Collecting,
            OperationState::Committing,
            OperationState::CommitUnknown,
            OperationState::Committed,
            OperationState::FailedKnownUncommitted,
        ] {
            let record = historical_request_record(
                &stored(state),
                &fence_record(),
                &ConnectorExecutionBindingKey {
                    instance_id: instance(),
                    incarnation: ConnectorInstanceIncarnation::from_bytes(HISTORICAL_INCARNATION),
                },
                None,
            )
            .expect("request");
            let checkpoints = checkpoints_from_request(&record);
            assert!(
                checkpoints.iter().any(|checkpoint| matches!(
                    checkpoint.phase,
                    ConnectorHistoricalWritePhase::WritersDispatched
                ) && checkpoint.state
                    != ConnectorHistoricalWriteDispatchState::NotDispatched),
                "{state:?} must not be reported as proof that nothing was dispatched"
            );
        }
    }

    #[test]
    fn an_unreconstructable_fenced_resource_is_never_guessed() {
        let mut record = fence_record();
        record.fence_digest = hex::encode([0x11u8; 32]);
        let error = historical_write_facts(
            &stored(OperationState::Committing),
            &record,
            &proposal(3, 20),
            None,
        )
        .expect_err("a fence digest that does not seal the reconstructed resource is fatal");
        assert!(error.contains("reconstructed fenced resource"));
    }
}
