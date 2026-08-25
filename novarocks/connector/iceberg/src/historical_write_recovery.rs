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

//! Provider-owned historical inspection for a distributed Iceberg write.
//!
//! # What this facet is
//!
//! After a frontend takeover the *current* Iceberg control generation is asked
//! to classify an *older* attempt of the same stable write operation. The old
//! generation is gone: its runtime session, its writer cohort and its lease no
//! longer exist and can never be reconstructed. The only admissible evidence is
//! therefore immutable external truth in the catalog.
//!
//! This facet is installed separately from the ordinary write capability
//! (`control_factory.rs`). An ordinary execution path must never reach it as a
//! fallback, and it must never call an ordinary old-owner method
//! (`commit` / `reconcile` / `abort`) on the historical operation. It registers
//! no binding, constructs no historical runtime session and never replays an
//! operation that was already dispatched.
//!
//! # Proof sources, in order of authority
//!
//! 1. **The fence branch marker** on `novarocks-write-fence-<operation-id>`
//!    ([`crate::commit::write_fence`]). It proves which authority currently owns
//!    the operation and, once this generation has raised it, that no historical
//!    authority can still commit.
//! 2. **The target data ref's snapshot provenance.** The ordinary commit path
//!    stamps a write-operation marker into the snapshot summary; finding it in
//!    the target ref lineage is the only proof that the operation applied.
//! 3. **Bounded opaque evidence** carried by the descriptor, used only for
//!    cross-checks. It is never trusted over external truth.
//!
//! A provider-private operation repository is deliberately *not* consulted:
//! process-local records do not survive the owner that wrote them, so they can
//! never prove anything about a historical attempt.
//!
//! # The one rule that matters most
//!
//! Absent evidence is never read as "did not commit". A missing marker, a
//! missing artifact, a digest mismatch, an unknown marker layout or a lineage
//! that cannot be walked to a proven end are all [`Ambiguous`]. Guessing
//! [`NotApplied`] from absence would silently lose a committed write.
//!
//! [`Ambiguous`]: ConnectorHistoricalWriteDisposition::Ambiguous
//! [`NotApplied`]: ConnectorHistoricalWriteDisposition::NotApplied

// Design: ADR-0068 (docs/adr/ADR-0068-external-write-fence-as-catalog-linearization-point.md)

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExternalFenceFailure, ConnectorExternalFenceReceipt, ConnectorExternalOperationFence,
    ConnectorHistoricalWriteApplication, ConnectorHistoricalWriteCleanupReceipt,
    ConnectorHistoricalWriteCleanupRequest, ConnectorHistoricalWriteContinuation,
    ConnectorHistoricalWriteDescriptor, ConnectorHistoricalWriteDisposition,
    ConnectorHistoricalWriteFence, ConnectorHistoricalWriteFenceRaiseRequest,
    ConnectorHistoricalWriteObservation, ConnectorHistoricalWriteOutcomeFacts,
    ConnectorHistoricalWriteProof, ConnectorHistoricalWriteRecovery, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteOperationId, ConnectorWriteReceipt, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::commit::write_control::{
    ICEBERG_WRITE_OPERATION_MARKER_PROPERTY, ICEBERG_WRITE_OPERATION_MARKER_VERSION,
    IcebergWriteOperationMarkerV1,
};
use crate::commit::write_fence::{
    EstablishedFence, FenceError, FenceGeneration, IcebergWriteFenceFacts, ObservedFence,
    fence_facts_from_spi, is_fence_ref, observe_fence, raise_fence, retire_fence_ref,
};
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::spec::{Summary, TableMetadata};
use crate::loaded_table::IcebergPhysicalTable;

/// Wire version of the opaque proof, receipt and continuation payloads minted
/// here. All three are provider-private: the frontend stores bytes and digests.
const ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION: u16 = 1;
/// Operation kind reported on cleanup reconciliation evidence.
const ICEBERG_HISTORICAL_WRITE_CLEANUP_KIND: &str = "iceberg.historical_write_cleanup.v1";
/// Schema version of that evidence envelope.
const ICEBERG_HISTORICAL_WRITE_EVIDENCE_VERSION: u16 = 1;

/// Snapshot summary key the ordinary write path stamps onto the snapshot it
/// commits, and the marker layout version this build understands.
///
/// The producing side owns both in `commit/write_control.rs`; they are mirrored
/// here read-only because that module keeps its marker type private. A marker
/// whose `version` is anything else is reported as ambiguous rather than
/// reinterpreted, so a future producer layout degrades to "unresolved" instead
/// of to a wrong classification.
/// Upper bound on the snapshot ancestry this facet will walk. A lineage that
/// does not end within the bound is reported as unproven, never as absence.
const MAX_TARGET_LINEAGE_WALK: usize = 50_000;

/// Upper bound on retained cleanup outcomes. Retention is what lets a lost
/// cleanup response be reconciled; it is bounded so a long-lived generation
/// cannot grow without limit.
const MAX_RETAINED_CLEANUP_OUTCOMES: usize = 4_096;

/// A snapshot id value no Iceberg snapshot can have, used in a proof that does
/// not pin a real fence marker so it can never anchor a cleanup.
const NO_FENCE_SNAPSHOT: i64 = -1;

/// Opaque provider proof returned with every classification.
///
/// The frontend persists it verbatim and never decodes it. This facet decodes
/// it again in `cleanup` so a cleanup can only ever act on the exact external
/// state a previous `inspect` proved.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalWriteProofV1 {
    version: u16,
    descriptor_digest: Vec<u8>,
    raised_fence_digest: Vec<u8>,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    target_snapshot_id: Option<i64>,
    fence_ref: String,
    /// The marker this generation holds on the fence ref, or [`NO_FENCE_SNAPSHOT`].
    fence_snapshot_id: i64,
    fence_generation: [u64; 3],
    /// Snapshot proving the operation applied, when one was found.
    applied_snapshot_id: Option<i64>,
    /// Whether the target lineage could be walked to a proven end.
    lineage_complete: bool,
    disposition: String,
}

/// Opaque acknowledgement of one raised fence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalWriteFenceReceiptV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    fence_ref: String,
    fence_snapshot_id: i64,
    /// True when this generation observed its own identical marker already
    /// published and reused it rather than publishing a second one.
    reused: bool,
}

/// Provider-signed authorization to run the same stable operation again under
/// the current generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalWriteContinuationV1 {
    version: u16,
    operation_id_base64: String,
    cohort_set_digest_base64: String,
    aggregate_digest_base64: Option<String>,
    target_ref: String,
    raised_fence_digest: Vec<u8>,
    /// Base state this generation proved before authorizing the continuation.
    table_uuid: String,
    target_snapshot_id: Option<i64>,
}

/// Neutral receipt payload for an operation proven to have applied.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalWriteReceiptV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    applied_snapshot_id: i64,
    operation_id_base64: String,
}

/// A retained cleanup outcome, kept so a lost response can be reconciled.
#[derive(Clone)]
struct CleanupRecord {
    outcome: ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>,
    proof: IcebergHistoricalWriteProofV1,
    descriptor_digest: [u8; 32],
    observation_digest: [u8; 32],
}

/// Bounded, insertion-ordered retention of cleanup outcomes.
#[derive(Default)]
struct CleanupRetention {
    records: HashMap<ConnectorWriteOperationId, CleanupRecord>,
    order: VecDeque<ConnectorWriteOperationId>,
}

impl CleanupRetention {
    fn get(&self, operation_id: &ConnectorWriteOperationId) -> Option<CleanupRecord> {
        self.records.get(operation_id).cloned()
    }

    fn insert(&mut self, operation_id: ConnectorWriteOperationId, record: CleanupRecord) {
        if self.records.insert(operation_id, record).is_none() {
            self.order.push_back(operation_id);
        }
        while self.order.len() > MAX_RETAINED_CLEANUP_OUTCOMES {
            if let Some(evicted) = self.order.pop_front() {
                self.records.remove(&evicted);
            }
        }
    }
}

/// Bounded record of the cleanup-authorizing observations this generation
/// actually issued.
///
/// A cleanup request carries the observation and its descriptor digest but not
/// the descriptor itself, so a provider cannot re-run the SPI's
/// `validate_for(descriptor)` seal at cleanup time. Without this set a
/// well-formed but never-issued observation would authorize a removal, which
/// would defeat "cleanup only ever touches proof-bound artifacts".
///
/// Generation-local memory is the correct scope rather than a limitation: the
/// takeover order requires the *current* owner to raise the fence and inspect
/// before it may clean up, so an observation from an earlier generation must be
/// re-derived anyway. Eviction can only cause a refusal, never an unauthorized
/// removal, and a refused cleanup is retried after a fresh inspection.
#[derive(Default)]
struct IssuedObservations {
    digests: HashSet<[u8; 32]>,
    order: VecDeque<[u8; 32]>,
}

impl IssuedObservations {
    fn record(&mut self, digest: [u8; 32]) {
        if self.digests.insert(digest) {
            self.order.push_back(digest);
        }
        while self.order.len() > MAX_RETAINED_CLEANUP_OUTCOMES {
            if let Some(evicted) = self.order.pop_front() {
                self.digests.remove(&evicted);
            }
        }
    }

    fn contains(&self, digest: &[u8; 32]) -> bool {
        self.digests.contains(digest)
    }
}

/// The narrow historical facet of one Iceberg control generation.
#[derive(Clone)]
pub struct IcebergHistoricalWriteRecovery {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    binding_key: ConnectorExecutionBindingKey,
    runtime: Arc<IcebergControlRuntime>,
    cleanup_outcomes: Arc<Mutex<CleanupRetention>>,
    issued_observations: Arc<Mutex<IssuedObservations>>,
}

impl IcebergHistoricalWriteRecovery {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        runtime: Arc<IcebergControlRuntime>,
    ) -> Self {
        let binding_key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        Self {
            descriptor,
            incarnation,
            binding_key,
            runtime,
            cleanup_outcomes: Arc::new(Mutex::new(CleanupRetention::default())),
            issued_observations: Arc::new(Mutex::new(IssuedObservations::default())),
        }
    }

    fn validate_context(&self, context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "Iceberg historical write recovery request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg historical write recovery deadline elapsed",
            ));
        }
        Ok(())
    }

    /// Load the table bypassing the generation-local cache.
    ///
    /// Historical classification reasons about the newest durable catalog state
    /// by definition, so a cached metadata snapshot would be exactly the wrong
    /// input.
    fn load_fresh(
        &self,
        table: &ConnectorTableIdentity,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        if table.instance_id != self.descriptor.instance_id {
            return Err(invalid(
                "Iceberg historical write descriptor belongs to another connector instance",
            ));
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        self.runtime
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)
    }

    /// Publish a strictly higher marker on this operation's fence ref.
    fn publish_raised_marker(
        &self,
        loaded: &IcebergPhysicalTable,
        facts: &IcebergWriteFenceFacts,
    ) -> Result<EstablishedFence, ConnectorError> {
        let catalog = Arc::clone(self.runtime.catalog());
        let table = loaded.table.clone();
        let file_io = loaded.table.file_io().clone();
        let facts = facts.clone();
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { raise_fence(catalog.as_ref(), &table, &file_io, &facts).await })
            .map_err(unavailable)?
            .map_err(fence_error_to_connector_error)
    }

    fn retire(
        &self,
        loaded: &IcebergPhysicalTable,
        fence_ref: &str,
        fence_snapshot_id: i64,
    ) -> Result<Result<(), FenceError>, ConnectorError> {
        let catalog = Arc::clone(self.runtime.catalog());
        let table = loaded.table.clone();
        let fence_ref = fence_ref.to_string();
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                retire_fence_ref(catalog.as_ref(), &table, &fence_ref, fence_snapshot_id).await
            })
            .map_err(unavailable)
    }
}

impl ConnectorHistoricalWriteRecovery for IcebergHistoricalWriteRecovery {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn raise_external_fence(
        &self,
        request: ConnectorHistoricalWriteFenceRaiseRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        self.validate_context(&request.context)?;
        // Fails closed unless the requested fence strictly supersedes the
        // historical one. A raise that does not outrank the old authority
        // cannot close it, so it is refused rather than accepted as a no-op.
        request.validate()?;
        if request.historical_binding.instance_id != self.descriptor.instance_id {
            return Err(invalid(
                "Iceberg historical write fence raise names another connector instance",
            ));
        }
        // The historical *incarnation* is deliberately not required to match:
        // fencing an older generation of this instance is the entire point.
        let raised = &request.raised;
        let facts = fence_facts_from_spi(raised);
        let loaded = self.load_fresh(raised.table())?;
        let fence_ref = facts.fence_ref();

        // A lost response must not turn our own established fence into a
        // superseded one. If the ref already carries exactly this fence value,
        // this is a replay of our own raise: reuse it. Monotonicity is intact
        // because the observed marker *is* the requested one.
        if let Some(existing) = observe_fence(loaded.table.metadata(), &fence_ref)
            .map_err(fence_error_to_connector_error)?
            && existing.facts == facts
        {
            return fence_receipt(&loaded, raised, &fence_ref, existing.snapshot_id, true);
        }

        let established = self.publish_raised_marker(&loaded, &facts)?;
        // The marker moved the fence ref, so the cached metadata is stale.
        self.runtime
            .control_state()
            .invalidate_table_cache(&raised.table().namespace, &raised.table().table);
        fence_receipt(
            &loaded,
            raised,
            established.assertion.fence_ref(),
            established.assertion.fence_snapshot_id(),
            established.reused,
        )
    }

    fn inspect(
        &self,
        descriptor: ConnectorHistoricalWriteDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalWriteObservation, ConnectorError> {
        self.validate_context(&context)?;
        descriptor.validate()?;
        if descriptor.historical_binding.instance_id != self.descriptor.instance_id {
            return Err(invalid(
                "Iceberg historical write descriptor names another connector instance",
            ));
        }
        let loaded = self.load_fresh(&descriptor.table)?;
        let metadata = loaded.table.metadata();
        let facts = fence_facts_from_spi(&descriptor.raised_fence);
        let fence_ref = facts.fence_ref();

        let fence = observe_raised_fence(metadata, &fence_ref, &facts);
        let target = observe_target_ref(metadata, &descriptor);
        let outcome = classify(&fence, &descriptor, &target);

        let proof = IcebergHistoricalWriteProofV1 {
            version: ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            raised_fence_digest: descriptor.raised_fence.digest().to_vec(),
            namespace: descriptor.table.namespace.to_string(),
            table: descriptor.table.table.to_string(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: descriptor.target_ref.as_str().to_string(),
            target_snapshot_id: target.head_snapshot_id,
            fence_ref,
            fence_snapshot_id: match &fence {
                RaisedFenceObservation::Held { snapshot_id, .. } => *snapshot_id,
                // A fence this generation does not hold can never anchor a
                // cleanup, so the proof refuses to name one.
                _ => NO_FENCE_SNAPSHOT,
            },
            fence_generation: [
                facts.control_plane_incarnation,
                facts.resource_epoch,
                facts.coordination_attempt,
            ],
            applied_snapshot_id: target.matched_snapshot_id,
            lineage_complete: target.lineage_complete,
            disposition: disposition_label(outcome.disposition).to_string(),
        };
        let proof =
            ConnectorHistoricalWriteProof::try_new(encode(&proof, "historical write proof")?)?;

        let application = match outcome.disposition {
            ConnectorHistoricalWriteDisposition::Applied => {
                let snapshot_id = target.matched_snapshot_id.ok_or_else(|| {
                    corrupt("Iceberg historical write applied classification has no snapshot")
                })?;
                Some(applied_facts(&descriptor, metadata, snapshot_id)?)
            }
            _ => None,
        };
        let continuation = match outcome.disposition {
            ConnectorHistoricalWriteDisposition::NotDispatched => {
                Some(ConnectorHistoricalWriteContinuation::try_new(
                    &descriptor.raised_fence,
                    continuation_payload(&descriptor, metadata, &target)?,
                )?)
            }
            _ => None,
        };
        let observation = ConnectorHistoricalWriteObservation::try_new(
            &descriptor,
            outcome.disposition,
            ConnectorHistoricalWriteOutcomeFacts {
                application,
                continuation,
                cleanup_required: outcome.cleanup_required,
            },
            proof,
        )?;
        if observation.cleanup_required {
            // Only an observation that asks for cleanup can authorize one, and
            // only this generation may issue it. Repeating the same immutable
            // descriptor produces the same digest, so this stays idempotent.
            self.issued_observations
                .lock()
                .map_err(cleanup_lock_error)?
                .record(observation.digest());
        }
        Ok(observation)
    }

    fn cleanup(
        &self,
        request: ConnectorHistoricalWriteCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
    {
        self.validate_context(&request.context)?;
        // A cleanup request carries no descriptor, so the SPI observation seal
        // cannot be re-checked here. Refusing every observation this generation
        // did not itself issue is what keeps cleanup proof-bound: a fabricated
        // but well-formed observation must never authorize a removal.
        if !self
            .issued_observations
            .lock()
            .map_err(cleanup_lock_error)?
            .contains(&request.observation.digest())
        {
            return Err(invalid(
                "Iceberg historical write cleanup presents an observation this generation did not issue",
            ));
        }
        if let Some(record) = self
            .cleanup_outcomes
            .lock()
            .map_err(cleanup_lock_error)?
            .get(&request.operation_id)
        {
            if record.descriptor_digest != request.descriptor_digest
                || record.observation_digest != request.observation.digest()
            {
                return Err(invalid(
                    "Iceberg historical write cleanup replays another observation for this operation",
                ));
            }
            return Ok(record.outcome);
        }
        request.observation.proof.validate()?;
        let proof = decode_proof(request.observation.proof.payload())?;
        if proof.descriptor_digest.as_slice() != request.descriptor_digest
            || proof.descriptor_digest.as_slice() != request.observation.descriptor_digest
            || proof.raised_fence_digest.as_slice() != request.observation.raised_fence_digest
            || request.observation.operation_id != request.operation_id
        {
            return Err(corrupt(
                "Iceberg historical write cleanup proof conflicts with its observation",
            ));
        }
        if !request.observation.cleanup_required {
            return Err(invalid(
                "Iceberg historical write cleanup was not requested by its observation",
            ));
        }
        if !fence_ref_is_retirable_for(request.observation.disposition) {
            return Err(invalid(format!(
                "Iceberg historical write cleanup refuses a {} observation",
                disposition_label(request.observation.disposition)
            )));
        }
        if !is_fence_ref(&proof.fence_ref) || proof.fence_snapshot_id <= 0 {
            return Err(corrupt(
                "Iceberg historical write cleanup proof does not name a provider-owned fence marker",
            ));
        }

        let table = ConnectorTableIdentity {
            instance_id: self.descriptor.instance_id.clone(),
            namespace: Arc::from(proof.namespace.clone()),
            table: Arc::from(proof.table.clone()),
        };
        let loaded = self.load_fresh(&table)?;
        let outcome = if loaded.table.metadata().uuid().to_string() != proof.table_uuid {
            // The table this proof describes is not the table that exists now.
            // Nothing here is provably ours, so nothing is removed.
            known_uncommitted("Iceberg historical write cleanup table UUID drifted")
        } else {
            match observe_fence(loaded.table.metadata(), &proof.fence_ref) {
                Err(error) => return Err(fence_error_to_connector_error(error)),
                // Already retired by an earlier attempt of this cleanup.
                Ok(None) => ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::NoOp,
                    receipt: cleanup_receipt(&request),
                    finalization: ExternalMutationFinalization::Complete,
                },
                Ok(Some(existing)) if existing.snapshot_id != proof.fence_snapshot_id => {
                    known_uncommitted(
                        "Iceberg historical write cleanup no longer holds the current fence",
                    )
                }
                Ok(Some(_)) => {
                    match self.retire(&loaded, &proof.fence_ref, proof.fence_snapshot_id)? {
                        Ok(()) => ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            receipt: cleanup_receipt(&request),
                            finalization: ExternalMutationFinalization::Complete,
                        },
                        // The ref vanished between the observation and the
                        // retirement: the artifact is already gone.
                        Err(FenceError::NotEstablished { .. }) => {
                            ExternalMutationOutcome::KnownCommitted {
                                effect: ExternalMutationEffect::NoOp,
                                receipt: cleanup_receipt(&request),
                                finalization: ExternalMutationFinalization::Complete,
                            }
                        }
                        Err(FenceError::Superseded { .. } | FenceError::MarkerConflict { .. }) => {
                            known_uncommitted(
                                "Iceberg historical write cleanup lost the fence before retirement",
                            )
                        }
                        Err(error) => ExternalMutationOutcome::CommitUnknown {
                            failure: ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Unavailable,
                                format!("Iceberg historical write cleanup: {error}"),
                            ),
                            evidence: self.cleanup_evidence(request.operation_id, &proof)?,
                        },
                    }
                }
            }
        };
        self.cleanup_outcomes
            .lock()
            .map_err(cleanup_lock_error)?
            .insert(
                request.operation_id,
                CleanupRecord {
                    outcome: outcome.clone(),
                    proof,
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
            );
        Ok(outcome)
    }

    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorWriteOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
    {
        self.validate_context(&context)?;
        if evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.incarnation
            || evidence.operation_id().to_bytes() != operation_id.to_bytes()
        {
            return Err(invalid(
                "Iceberg historical write cleanup evidence does not match this generation",
            ));
        }
        let record = self
            .cleanup_outcomes
            .lock()
            .map_err(cleanup_lock_error)?
            .get(&operation_id)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "Iceberg historical write cleanup has no retained outcome to reconcile",
                )
            })?;
        if !matches!(
            record.outcome,
            ExternalMutationOutcome::CommitUnknown { .. }
        ) {
            return Ok(record.outcome);
        }
        let table = ConnectorTableIdentity {
            instance_id: self.descriptor.instance_id.clone(),
            namespace: Arc::from(record.proof.namespace.clone()),
            table: Arc::from(record.proof.table.clone()),
        };
        let loaded = self.load_fresh(&table)?;
        let receipt = ConnectorHistoricalWriteCleanupReceipt {
            descriptor_digest: record.descriptor_digest,
            observation_digest: record.observation_digest,
        };
        let outcome = if loaded.table.metadata().uuid().to_string() != record.proof.table_uuid {
            known_uncommitted(
                "Iceberg historical write cleanup table UUID drifted during reconciliation",
            )
        } else {
            match observe_fence(loaded.table.metadata(), &record.proof.fence_ref) {
                Err(error) => return Err(fence_error_to_connector_error(error)),
                Ok(None) => ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                },
                Ok(Some(existing)) if existing.snapshot_id == record.proof.fence_snapshot_id => {
                    known_uncommitted(
                        "Iceberg historical write cleanup fence ref still points at the inspected marker",
                    )
                }
                Ok(Some(_)) => known_uncommitted(
                    "Iceberg historical write cleanup fence ref moved to another marker",
                ),
            }
        };
        self.cleanup_outcomes
            .lock()
            .map_err(cleanup_lock_error)?
            .insert(
                operation_id,
                CleanupRecord {
                    outcome: outcome.clone(),
                    ..record
                },
            );
        Ok(outcome)
    }
}

impl IcebergHistoricalWriteRecovery {
    fn cleanup_evidence(
        &self,
        operation_id: ConnectorWriteOperationId,
        proof: &IcebergHistoricalWriteProofV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        ExternalMutationEvidence::try_new(
            ICEBERG_HISTORICAL_WRITE_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            ConnectorMutationOperationId::from_bytes(operation_id.to_bytes()),
            ICEBERG_HISTORICAL_WRITE_CLEANUP_KIND,
            encode(proof, "historical write cleanup evidence")?,
        )
    }
}

/// What the fence ref says about *this* recovery attempt's authority.
#[derive(Clone, Debug, Eq, PartialEq)]
enum RaisedFenceObservation {
    /// The fence ref carries exactly the marker this attempt raised.
    Held {
        snapshot_id: i64,
        /// Whether that marker has a predecessor on the same ref, which is the
        /// only public evidence that an earlier fence of this operation is
        /// still present in the table it was published into.
        has_predecessor: bool,
    },
    /// The fence ref moved past this attempt: another authority took over.
    Superseded,
    /// The fence ref cannot support any conclusion.
    Unproven { detail: String },
}

/// What the target data ref says about the historical operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TargetRefObservation {
    head_snapshot_id: Option<i64>,
    /// Snapshot in the target lineage carrying this operation's provenance.
    matched_snapshot_id: Option<i64>,
    /// Whether the lineage was walked to a proven end (root reached, no
    /// truncation, bound not exhausted).
    lineage_complete: bool,
    /// The provenance exists in table metadata but not in the target lineage.
    off_lineage_match: bool,
    /// A marker named this operation but carried different sealed facts.
    digest_mismatch: bool,
    /// A marker used a layout this build does not understand.
    unknown_marker_layout: bool,
    /// More than one snapshot in the lineage claimed this operation.
    multiple_matches: bool,
    /// The target ref exists but is not a branch.
    non_branch_target: bool,
    /// A named non-main target ref does not exist at all.
    missing_target_ref: bool,
}

struct Classification {
    disposition: ConnectorHistoricalWriteDisposition,
    cleanup_required: bool,
}

/// Read the fence ref and decide whether this attempt still owns the fence.
fn observe_raised_fence(
    metadata: &TableMetadata,
    fence_ref: &str,
    raised: &IcebergWriteFenceFacts,
) -> RaisedFenceObservation {
    let observed = match observe_fence(metadata, fence_ref) {
        Ok(observed) => observed,
        Err(error) => {
            return RaisedFenceObservation::Unproven {
                detail: error.to_string(),
            };
        }
    };
    let Some(existing) = observed else {
        return RaisedFenceObservation::Unproven {
            detail: format!("fence ref '{fence_ref}' carries no marker for this recovery attempt"),
        };
    };
    if existing.facts.write_operation_id != raised.write_operation_id {
        return RaisedFenceObservation::Unproven {
            detail: format!(
                "fence ref '{fence_ref}' carries operation {} but recovery is for operation {}",
                existing.facts.write_operation_id, raised.write_operation_id
            ),
        };
    }
    match compare_to_raised(&existing, raised) {
        RaisedFenceRank::Ours => {
            let has_predecessor = metadata
                .snapshot_by_id(existing.snapshot_id)
                .is_some_and(|snapshot| snapshot.parent_snapshot_id().is_some());
            RaisedFenceObservation::Held {
                snapshot_id: existing.snapshot_id,
                has_predecessor,
            }
        }
        RaisedFenceRank::Higher | RaisedFenceRank::SameGenerationOtherValue => {
            RaisedFenceObservation::Superseded
        }
        RaisedFenceRank::Lower => RaisedFenceObservation::Unproven {
            detail: format!(
                "fence ref '{fence_ref}' is behind the fence this recovery attempt raised"
            ),
        },
    }
}

enum RaisedFenceRank {
    Ours,
    Higher,
    SameGenerationOtherValue,
    Lower,
}

fn compare_to_raised(existing: &ObservedFence, raised: &IcebergWriteFenceFacts) -> RaisedFenceRank {
    let observed: FenceGeneration = existing.generation();
    let requested = raised.generation();
    if observed > requested {
        return RaisedFenceRank::Higher;
    }
    if observed < requested {
        return RaisedFenceRank::Lower;
    }
    if existing.facts == *raised {
        RaisedFenceRank::Ours
    } else {
        RaisedFenceRank::SameGenerationOtherValue
    }
}

/// Walk the target data ref and look for this operation's write provenance.
fn observe_target_ref(
    metadata: &TableMetadata,
    descriptor: &ConnectorHistoricalWriteDescriptor,
) -> TargetRefObservation {
    let target_ref = descriptor.target_ref.as_str();
    let mut observation = TargetRefObservation::default();
    let reference = metadata.refs().get(target_ref);
    if let Some(reference) = reference
        && !reference.is_branch()
    {
        observation.non_branch_target = true;
        return observation;
    }
    observation.head_snapshot_id = match reference {
        Some(reference) => Some(reference.snapshot_id),
        // An empty table has no `main` ref yet; that is a real, complete
        // "nothing committed here" state rather than missing evidence.
        None if target_ref == "main" => metadata.current_snapshot_id(),
        None => {
            observation.missing_target_ref = true;
            return observation;
        }
    };

    let mut walked = HashSet::new();
    let mut cursor = observation.head_snapshot_id;
    let mut complete = true;
    for _ in 0..MAX_TARGET_LINEAGE_WALK {
        let Some(snapshot_id) = cursor else {
            // Reached the root of the lineage: the walk covered every commit
            // that can exist on this ref.
            break;
        };
        let Some(snapshot) = metadata.snapshot_by_id(snapshot_id) else {
            // The ancestry was truncated (expired snapshots). A commit could
            // have existed here and been removed, so absence proves nothing.
            complete = false;
            break;
        };
        walked.insert(snapshot_id);
        match provenance_match(snapshot.summary(), descriptor) {
            ProvenanceMatch::None => {}
            ProvenanceMatch::UnknownLayout => observation.unknown_marker_layout = true,
            ProvenanceMatch::DigestMismatch => observation.digest_mismatch = true,
            ProvenanceMatch::Matched => {
                if observation
                    .matched_snapshot_id
                    .replace(snapshot_id)
                    .is_some()
                {
                    observation.multiple_matches = true;
                }
            }
        }
        cursor = snapshot.parent_snapshot_id();
        if cursor.is_some_and(|parent| walked.contains(&parent)) {
            // A cycle is structurally impossible in Iceberg lineage; treating
            // it as unproven keeps the walk terminating without guessing.
            complete = false;
            break;
        }
    }
    if cursor.is_some() && complete {
        // The bound was exhausted before the root: the walk is not a proof.
        complete = false;
    }
    observation.lineage_complete = complete;

    if observation.matched_snapshot_id.is_none() {
        // A commit that landed and was then rolled back or moved off this ref
        // still happened externally. Finding it outside the lineage is a reason
        // to refuse a conclusion, not to report absence.
        observation.off_lineage_match = metadata.snapshots().any(|snapshot| {
            !walked.contains(&snapshot.snapshot_id())
                && matches!(
                    provenance_match(snapshot.summary(), descriptor),
                    ProvenanceMatch::Matched
                )
        });
    }
    observation
}

enum ProvenanceMatch {
    None,
    Matched,
    DigestMismatch,
    UnknownLayout,
}

/// Compare one snapshot summary against the historical operation identity.
fn provenance_match(
    summary: &Summary,
    descriptor: &ConnectorHistoricalWriteDescriptor,
) -> ProvenanceMatch {
    let Some(raw) = summary
        .additional_properties
        .get(ICEBERG_WRITE_OPERATION_MARKER_PROPERTY)
    else {
        return ProvenanceMatch::None;
    };
    let Ok(marker) = serde_json::from_str::<IcebergWriteOperationMarkerV1>(raw) else {
        return ProvenanceMatch::UnknownLayout;
    };
    if marker.version != ICEBERG_WRITE_OPERATION_MARKER_VERSION {
        return ProvenanceMatch::UnknownLayout;
    }
    if marker.publication.publication_id().to_bytes() != descriptor.operation_id.to_bytes() {
        return ProvenanceMatch::None;
    }
    // From here the marker names this operation, so any disagreement is a
    // mismatch to report rather than a non-match to skip.
    if marker.instance_id != descriptor.historical_binding.instance_id.as_str()
        || decode_base64(&marker.incarnation_base64)
            != Some(descriptor.historical_binding.incarnation.to_bytes().into())
        || marker.target_ref != descriptor.target_ref.as_str()
        || decode_base64(&marker.cohort_set_digest_base64)
            != Some(descriptor.cohort_set_digest.into())
    {
        return ProvenanceMatch::DigestMismatch;
    }
    if let Some(aggregate) = descriptor.aggregate_digest
        && decode_base64(&marker.aggregate_digest_base64) != Some(aggregate.into())
    {
        return ProvenanceMatch::DigestMismatch;
    }
    ProvenanceMatch::Matched
}

/// The whole classification decision, expressed over proven facts only.
fn classify(
    fence: &RaisedFenceObservation,
    descriptor: &ConnectorHistoricalWriteDescriptor,
    target: &TargetRefObservation,
) -> Classification {
    // Applied is a monotonic fact about immutable history: once a snapshot in
    // the target lineage carries this operation's sealed provenance, no later
    // fence movement can undo it. It is therefore decided before any fence
    // question. `unknown_marker_layout` still blocks it, because a marker this
    // build cannot read might be a second commit of the same operation.
    if target.matched_snapshot_id.is_some()
        && !target.multiple_matches
        && !target.digest_mismatch
        && !target.unknown_marker_layout
    {
        return Classification {
            disposition: ConnectorHistoricalWriteDisposition::Applied,
            // The fence ref of an applied operation is retired only while this
            // generation still holds it.
            cleanup_required: matches!(fence, RaisedFenceObservation::Held { .. }),
        };
    }
    if target.non_branch_target {
        // A tag cannot receive DML, so this provider has no lineage semantics
        // to classify a historical write against.
        return unresolved(ConnectorHistoricalWriteDisposition::Unsupported);
    }
    if target.multiple_matches
        || target.digest_mismatch
        || target.unknown_marker_layout
        || target.off_lineage_match
        || target.missing_target_ref
    {
        return unresolved(ConnectorHistoricalWriteDisposition::Ambiguous);
    }
    match fence {
        RaisedFenceObservation::Superseded => Classification {
            disposition: ConnectorHistoricalWriteDisposition::Conflict,
            // Another authority owns the fence; removing it is not ours to do.
            cleanup_required: false,
        },
        RaisedFenceObservation::Unproven { .. } => {
            unresolved(ConnectorHistoricalWriteDisposition::Ambiguous)
        }
        RaisedFenceObservation::Held {
            has_predecessor, ..
        } => {
            if !target.lineage_complete {
                // The operation may have committed into a part of the history
                // that no longer exists. Absence is not proof.
                return unresolved(ConnectorHistoricalWriteDisposition::Ambiguous);
            }
            let historical_established = matches!(
                descriptor.historical_fence,
                ConnectorHistoricalWriteFence::Established { .. }
            );
            if historical_established && !*has_predecessor {
                // The historical attempt published a marker, but the fence ref
                // this generation raised has nothing behind it: the marker that
                // would carry the old attempt's provenance is gone (the table
                // was dropped and recreated, or the ref was rebuilt). Nothing
                // can be concluded from its absence.
                return unresolved(ConnectorHistoricalWriteDisposition::Ambiguous);
            }
            if !historical_established && descriptor.journal_proves_nothing_dispatched() {
                // The fence is established before any writer or commit that can
                // produce an irreversible external effect. No fence therefore
                // means no dispatch, and the raised fence has closed that
                // authority, so this operation may be continued.
                return Classification {
                    disposition: ConnectorHistoricalWriteDisposition::NotDispatched,
                    // Retiring the fence here would let the old authority
                    // establish one again at its old generation and commit.
                    cleanup_required: false,
                };
            }
            if descriptor.journal_proves_nothing_dispatched() {
                // A fence exists but nothing was dispatched: no writer output
                // can exist, and the old authority is closed.
                return Classification {
                    disposition: ConnectorHistoricalWriteDisposition::NotApplied,
                    cleanup_required: true,
                };
            }
            // Writers ran but no commit provenance exists and none can appear.
            // The output is orphaned; it is never adopted across generations.
            Classification {
                disposition: ConnectorHistoricalWriteDisposition::Staged,
                cleanup_required: true,
            }
        }
    }
}

/// Whether an operation in this disposition may have its fence ref retired.
///
/// Retiring the fence of an operation that was never dispatched would reopen
/// the historical authority: that owner holds no fence assertion yet, so it
/// could establish one again at its old generation and commit. Every other
/// terminal disposition already holds an assertion pinned to a marker whose
/// removal makes it permanently unsatisfiable. `Conflict`, `Ambiguous` and
/// `Unsupported` are refused because this generation either does not own the
/// fence or has proven nothing at all.
const fn fence_ref_is_retirable_for(disposition: ConnectorHistoricalWriteDisposition) -> bool {
    matches!(
        disposition,
        ConnectorHistoricalWriteDisposition::Applied
            | ConnectorHistoricalWriteDisposition::NotApplied
            | ConnectorHistoricalWriteDisposition::Staged
    )
}

fn unresolved(disposition: ConnectorHistoricalWriteDisposition) -> Classification {
    Classification {
        disposition,
        cleanup_required: false,
    }
}

/// Neutral finalization facts for an operation proven to have applied.
///
/// No row count is reported: the snapshot summary counts rows added by the
/// physical commit, which is not the statement-level answer for every write
/// intent, and a recovered write must never report a guessed number.
fn applied_facts(
    descriptor: &ConnectorHistoricalWriteDescriptor,
    metadata: &TableMetadata,
    snapshot_id: i64,
) -> Result<ConnectorHistoricalWriteApplication, ConnectorError> {
    let committed_version = ConnectorCommittedVersion::try_new(
        Bytes::from(format!("iceberg/historical-write/v1/{snapshot_id}")),
        Some(snapshot_id),
    )?;
    let payload = encode(
        &IcebergHistoricalWriteReceiptV1 {
            version: ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION,
            namespace: descriptor.table.namespace.to_string(),
            table: descriptor.table.table.to_string(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: descriptor.target_ref.as_str().to_string(),
            applied_snapshot_id: snapshot_id,
            operation_id_base64: encode_base64(descriptor.operation_id.to_bytes()),
        },
        "historical write receipt",
    )?;
    Ok(ConnectorHistoricalWriteApplication {
        committed_version: committed_version.clone(),
        receipt: ConnectorWriteReceipt::try_new_with_committed_version(payload, committed_version)?,
        finalization: ExternalMutationFinalization::Complete,
    })
}

/// Bind a continuation to the raised fence, the stable operation, the sealed
/// historical input digests and the base state this generation proved.
fn continuation_payload(
    descriptor: &ConnectorHistoricalWriteDescriptor,
    metadata: &TableMetadata,
    target: &TargetRefObservation,
) -> Result<Bytes, ConnectorError> {
    encode(
        &IcebergHistoricalWriteContinuationV1 {
            version: ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION,
            operation_id_base64: encode_base64(descriptor.operation_id.to_bytes()),
            cohort_set_digest_base64: encode_base64(descriptor.cohort_set_digest),
            aggregate_digest_base64: descriptor.aggregate_digest.map(encode_base64),
            target_ref: descriptor.target_ref.as_str().to_string(),
            raised_fence_digest: descriptor.raised_fence.digest().to_vec(),
            table_uuid: metadata.uuid().to_string(),
            target_snapshot_id: target.head_snapshot_id,
        },
        "historical write continuation",
    )
}

fn fence_receipt(
    loaded: &IcebergPhysicalTable,
    fence: &ConnectorExternalOperationFence,
    fence_ref: &str,
    fence_snapshot_id: i64,
    reused: bool,
) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
    let payload = encode(
        &IcebergHistoricalWriteFenceReceiptV1 {
            version: ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION,
            namespace: fence.table().namespace.to_string(),
            table: fence.table().table.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            fence_ref: fence_ref.to_string(),
            fence_snapshot_id,
            reused,
        },
        "historical write fence receipt",
    )?;
    ConnectorExternalFenceReceipt::try_new(fence, payload)
}

fn cleanup_receipt(
    request: &ConnectorHistoricalWriteCleanupRequest,
) -> ConnectorHistoricalWriteCleanupReceipt {
    ConnectorHistoricalWriteCleanupReceipt {
        descriptor_digest: request.descriptor_digest,
        observation_digest: request.observation.digest(),
    }
}

fn decode_proof(payload: &Bytes) -> Result<IcebergHistoricalWriteProofV1, ConnectorError> {
    let proof: IcebergHistoricalWriteProofV1 = serde_json::from_slice(payload)
        .map_err(|error| corrupt(format!("decode Iceberg historical write proof: {error}")))?;
    if proof.version != ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION {
        return Err(corrupt(format!(
            "Iceberg historical write proof has layout version {}; this build understands {}",
            proof.version, ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION
        )));
    }
    Ok(proof)
}

/// Map a fence failure onto a typed connector error.
///
/// A fence conflict is terminal by construction: it is never reported as an
/// unknown outcome and never as an unsupported capability, because either would
/// let a caller retry an authority that has already been closed.
fn fence_error_to_connector_error(error: FenceError) -> ConnectorError {
    match error {
        FenceError::Superseded { .. } => ConnectorError::external_fence(
            ConnectorExternalFenceFailure::Superseded,
            error.to_string(),
        ),
        FenceError::MarkerConflict { .. } => ConnectorError::external_fence(
            ConnectorExternalFenceFailure::ForeignOperation,
            error.to_string(),
        ),
        FenceError::NotEstablished { .. } => ConnectorError::external_fence(
            ConnectorExternalFenceFailure::NotEstablished,
            error.to_string(),
        ),
        FenceError::Ambiguous { .. } => corrupt(error.to_string()),
        FenceError::Failed { .. } => {
            ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
        }
    }
}

fn disposition_label(disposition: ConnectorHistoricalWriteDisposition) -> &'static str {
    match disposition {
        ConnectorHistoricalWriteDisposition::Applied => "applied",
        ConnectorHistoricalWriteDisposition::NotApplied => "not-applied",
        ConnectorHistoricalWriteDisposition::NotDispatched => "not-dispatched",
        ConnectorHistoricalWriteDisposition::Staged => "staged",
        ConnectorHistoricalWriteDisposition::Conflict => "conflict",
        ConnectorHistoricalWriteDisposition::Ambiguous => "ambiguous",
        ConnectorHistoricalWriteDisposition::Unsupported => "unsupported",
    }
}

fn known_uncommitted<T>(message: &str) -> ExternalMutationOutcome<T> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(ConnectorMutationFailureKind::Conflict, message),
    }
}

fn encode<T: Serialize>(value: &T, subject: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg {subject}: {error}"),
        )
    })
}

fn encode_base64(bytes: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(value).ok()
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn cleanup_lock_error<T: std::fmt::Display>(error: T) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!("Iceberg historical write cleanup retention lock: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorClusterIdentity, ConnectorExternalFenceGeneration,
        ConnectorHistoricalWriteCheckpoint, ConnectorHistoricalWriteDispatchState,
        ConnectorHistoricalWriteFenceFacts, ConnectorHistoricalWriteIdentity,
        ConnectorHistoricalWritePhase, ConnectorInstanceId, ConnectorProviderId,
        ConnectorWriteIntent, ConnectorWriteTargetRef,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Type,
    };
    use crate::iceberg::{NamespaceIdent, TableCommit, TableCreation, TableRequirement};
    use crate::resources::IcebergControlResources;

    use super::*;

    const INSTANCE: &str = "ice";
    const NAMESPACE: &str = "db";
    const TABLE: &str = "t";

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    fn instance_descriptor() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            instance_id: ConnectorInstanceId::parse(INSTANCE).expect("instance id"),
        }
    }

    fn current_incarnation() -> ConnectorInstanceIncarnation {
        ConnectorInstanceIncarnation::from_bytes([3; 16])
    }

    fn table_identity() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(INSTANCE).expect("instance id"),
            namespace: Arc::from(NAMESPACE),
            table: Arc::from(TABLE),
        }
    }

    fn operation_id() -> ConnectorWriteOperationId {
        ConnectorWriteOperationId::from_bytes([4; 16])
    }

    fn historical_incarnation() -> ConnectorInstanceIncarnation {
        ConnectorInstanceIncarnation::from_bytes([9; 16])
    }

    fn historical_binding() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse(INSTANCE).expect("instance id"),
            incarnation: historical_incarnation(),
        }
    }

    fn spi_fence(epoch: u64, attempt: u64) -> ConnectorExternalOperationFence {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive("nova-historical-test").expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(1, epoch, attempt).expect("generation"),
            operation_id(),
            [6; 16],
            table_identity(),
            ConnectorWriteTargetRef::main(),
        )
        .expect("external operation fence")
    }

    fn established_historical(epoch: u64) -> ConnectorHistoricalWriteFence {
        let fence = spi_fence(epoch, 1);
        let receipt = ConnectorExternalFenceReceipt::try_new(
            &fence,
            Bytes::from_static(b"historical-marker"),
        )
        .expect("receipt");
        ConnectorHistoricalWriteFence::established(&receipt, fence).expect("established fence")
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
        historical_fence: ConnectorHistoricalWriteFence,
        state: ConnectorHistoricalWriteDispatchState,
    ) -> ConnectorHistoricalWriteDescriptor {
        ConnectorHistoricalWriteDescriptor::try_new(
            ConnectorHistoricalWriteIdentity {
                historical_binding: historical_binding(),
                table: table_identity(),
                target_ref: ConnectorWriteTargetRef::main(),
                operation_id: operation_id(),
                intent: ConnectorWriteIntent::Append,
                cohort_set_digest: [7; 32],
                aggregate_digest: Some([8; 32]),
            },
            ConnectorHistoricalWriteFenceFacts {
                historical_fence,
                raised_fence: spi_fence(3, 1),
                raised_fence_receipt_digest: [5; 32],
            },
            checkpoints(state),
            None,
        )
        .expect("historical write descriptor")
    }

    fn held_fence() -> RaisedFenceObservation {
        RaisedFenceObservation::Held {
            snapshot_id: 42,
            has_predecessor: true,
        }
    }

    fn clean_target() -> TargetRefObservation {
        TargetRefObservation {
            head_snapshot_id: Some(11),
            lineage_complete: true,
            ..TargetRefObservation::default()
        }
    }

    // ----------------------------------------------------------------------
    // Classification rules. These drive the decision directly so every
    // disposition is covered including the ones a local warehouse cannot
    // reproduce.
    // ----------------------------------------------------------------------

    #[test]
    fn applied_is_decided_from_target_lineage_provenance() {
        let target = TargetRefObservation {
            matched_snapshot_id: Some(77),
            ..clean_target()
        };
        let outcome = classify(
            &held_fence(),
            &descriptor(
                established_historical(2),
                ConnectorHistoricalWriteDispatchState::Completed,
            ),
            &target,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::Applied
        );
        assert!(
            outcome.cleanup_required,
            "an applied operation retires its fence ref"
        );
    }

    #[test]
    fn not_dispatched_requires_no_historical_fence_and_a_clean_journal() {
        let outcome = classify(
            &held_fence(),
            &descriptor(
                ConnectorHistoricalWriteFence::NotEstablished,
                ConnectorHistoricalWriteDispatchState::NotDispatched,
            ),
            &clean_target(),
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::NotDispatched
        );
        assert!(
            !outcome.cleanup_required,
            "retiring the fence of a never-dispatched attempt would reopen its authority"
        );

        // The same journal state with an established historical fence is not
        // "never dispatched": the old owner reached the point where a writer
        // could have run.
        let outcome = classify(
            &held_fence(),
            &descriptor(
                established_historical(2),
                ConnectorHistoricalWriteDispatchState::NotDispatched,
            ),
            &clean_target(),
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::NotApplied
        );
        assert!(outcome.cleanup_required);

        // A dispatched journal checkpoint forbids not-dispatched even with no
        // fence at all.
        let outcome = classify(
            &held_fence(),
            &descriptor(
                ConnectorHistoricalWriteFence::NotEstablished,
                ConnectorHistoricalWriteDispatchState::Dispatched,
            ),
            &clean_target(),
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::Staged
        );
    }

    #[test]
    fn staged_output_is_cleaned_up_and_never_adopted() {
        let outcome = classify(
            &held_fence(),
            &descriptor(
                established_historical(2),
                ConnectorHistoricalWriteDispatchState::Completed,
            ),
            &clean_target(),
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::Staged
        );
        assert!(outcome.cleanup_required);
    }

    #[test]
    fn a_superseded_fence_is_a_conflict_and_never_a_cleanup() {
        let outcome = classify(
            &RaisedFenceObservation::Superseded,
            &descriptor(
                established_historical(2),
                ConnectorHistoricalWriteDispatchState::Completed,
            ),
            &clean_target(),
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::Conflict
        );
        assert!(
            !outcome.cleanup_required,
            "another authority owns the fence ref"
        );
    }

    #[test]
    fn a_non_branch_target_is_unsupported() {
        let target = TargetRefObservation {
            non_branch_target: true,
            ..TargetRefObservation::default()
        };
        let outcome = classify(
            &held_fence(),
            &descriptor(
                established_historical(2),
                ConnectorHistoricalWriteDispatchState::Completed,
            ),
            &target,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::Unsupported
        );
        assert!(!outcome.cleanup_required);
    }

    #[test]
    fn every_kind_of_missing_or_conflicting_evidence_is_ambiguous_never_not_applied() {
        let never_dispatched = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        let cases: Vec<(&str, RaisedFenceObservation, TargetRefObservation)> = vec![
            (
                "the raised fence marker is missing",
                RaisedFenceObservation::Unproven {
                    detail: "no marker".to_string(),
                },
                clean_target(),
            ),
            (
                "the historical marker is absent from the fence lineage",
                RaisedFenceObservation::Held {
                    snapshot_id: 42,
                    has_predecessor: false,
                },
                clean_target(),
            ),
            (
                "the target lineage was truncated",
                held_fence(),
                TargetRefObservation {
                    lineage_complete: false,
                    ..clean_target()
                },
            ),
            (
                "a marker carried different sealed facts",
                held_fence(),
                TargetRefObservation {
                    digest_mismatch: true,
                    ..clean_target()
                },
            ),
            (
                "a marker used an unknown layout version",
                held_fence(),
                TargetRefObservation {
                    unknown_marker_layout: true,
                    ..clean_target()
                },
            ),
            (
                "the provenance exists outside the target lineage",
                held_fence(),
                TargetRefObservation {
                    off_lineage_match: true,
                    ..clean_target()
                },
            ),
            (
                "the named target ref does not exist",
                held_fence(),
                TargetRefObservation {
                    missing_target_ref: true,
                    ..TargetRefObservation::default()
                },
            ),
            (
                "more than one snapshot claimed this operation",
                held_fence(),
                TargetRefObservation {
                    matched_snapshot_id: Some(1),
                    multiple_matches: true,
                    ..clean_target()
                },
            ),
            (
                "a matched snapshot disagreed with the sealed digests",
                held_fence(),
                TargetRefObservation {
                    matched_snapshot_id: Some(1),
                    digest_mismatch: true,
                    ..clean_target()
                },
            ),
        ];
        for (reason, fence, target) in cases {
            let outcome = classify(&fence, &never_dispatched, &target);
            assert_eq!(
                outcome.disposition,
                ConnectorHistoricalWriteDisposition::Ambiguous,
                "{reason} must be ambiguous"
            );
            assert!(!outcome.cleanup_required, "{reason} must not clean up");
        }
    }

    #[test]
    fn provenance_matching_binds_the_exact_historical_generation_and_digests() {
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        let summary = |mutate: fn(&mut serde_json::Value)| {
            let mut value = provenance_marker();
            mutate(&mut value);
            let mut additional_properties = HashMap::new();
            additional_properties.insert(
                ICEBERG_WRITE_OPERATION_MARKER_PROPERTY.to_string(),
                value.to_string(),
            );
            Summary {
                operation: Operation::Append,
                additional_properties,
            }
        };

        assert!(matches!(
            provenance_match(&summary(|_| {}), &descriptor),
            ProvenanceMatch::Matched
        ));
        assert!(
            matches!(
                provenance_match(
                    &summary(|value| value["version"] = serde_json::json!(9)),
                    &descriptor
                ),
                ProvenanceMatch::UnknownLayout
            ),
            "an unknown marker layout must never be read as a non-match"
        );
        for field in [
            "cohort_set_digest_base64",
            "aggregate_digest_base64",
            "incarnation_base64",
        ] {
            let mut value = provenance_marker();
            value[field] = serde_json::json!(encode_base64([1u8; 32]));
            let mut additional_properties = HashMap::new();
            additional_properties.insert(
                ICEBERG_WRITE_OPERATION_MARKER_PROPERTY.to_string(),
                value.to_string(),
            );
            assert!(
                matches!(
                    provenance_match(
                        &Summary {
                            operation: Operation::Append,
                            additional_properties,
                        },
                        &descriptor
                    ),
                    ProvenanceMatch::DigestMismatch
                ),
                "{field} must be a mismatch, not a silent non-match"
            );
        }
        assert!(
            matches!(
                provenance_match(
                    &summary(|value| value["operation_id_base64"] =
                        serde_json::json!(encode_base64([1u8; 16]))),
                    &descriptor
                ),
                ProvenanceMatch::None
            ),
            "another operation's marker is simply not this operation"
        );
        assert!(matches!(
            provenance_match(
                &Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                },
                &descriptor
            ),
            ProvenanceMatch::None
        ));
    }

    fn provenance_marker() -> serde_json::Value {
        serde_json::json!({
            "version": 1,
            "instance_id": INSTANCE,
            "incarnation_base64": encode_base64(historical_incarnation().to_bytes()),
            "operation_id_base64": encode_base64(operation_id().to_bytes()),
            "target_ref": "main",
            "cohort_set_digest_base64": encode_base64([7u8; 32]),
            "aggregate_digest_base64": encode_base64([8u8; 32]),
        })
    }

    #[test]
    fn retention_stays_bounded() {
        let mut retention = CleanupRetention::default();
        let record = CleanupRecord {
            outcome: known_uncommitted("retained"),
            proof: IcebergHistoricalWriteProofV1 {
                version: ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION,
                descriptor_digest: vec![1; 32],
                raised_fence_digest: vec![1; 32],
                namespace: NAMESPACE.to_string(),
                table: TABLE.to_string(),
                table_uuid: "uuid".to_string(),
                target_ref: "main".to_string(),
                target_snapshot_id: None,
                fence_ref: "novarocks-write-fence-op".to_string(),
                fence_snapshot_id: 1,
                fence_generation: [1, 1, 1],
                applied_snapshot_id: None,
                lineage_complete: true,
                disposition: "not-applied".to_string(),
            },
            descriptor_digest: [1; 32],
            observation_digest: [1; 32],
        };
        for index in 0..(MAX_RETAINED_CLEANUP_OUTCOMES + 16) {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
            retention.insert(ConnectorWriteOperationId::from_bytes(bytes), record.clone());
        }
        assert_eq!(retention.records.len(), MAX_RETAINED_CLEANUP_OUTCOMES);
        assert_eq!(retention.order.len(), MAX_RETAINED_CLEANUP_OUTCOMES);
    }

    // ----------------------------------------------------------------------
    // Catalog-backed coverage. These use a local filesystem warehouse so the
    // fence ref, its marker snapshot and the atomic conditional update are the
    // real ones rather than a simulation.
    // ----------------------------------------------------------------------

    struct Fixture {
        executor: tokio::runtime::Runtime,
        _warehouse: tempfile::TempDir,
        runtime: Arc<IcebergControlRuntime>,
        recovery: IcebergHistoricalWriteRecovery,
    }

    fn fixture() -> Fixture {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            INSTANCE,
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let catalog = Arc::clone(runtime.catalog());
        executor.block_on(async move {
            let namespace = NamespaceIdent::new(NAMESPACE.to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema");
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name(TABLE.to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                )
                .await
                .expect("create table");
        });
        let recovery = IcebergHistoricalWriteRecovery::new(
            instance_descriptor(),
            current_incarnation(),
            Arc::clone(&runtime),
        );
        Fixture {
            executor,
            _warehouse: warehouse,
            runtime,
            recovery,
        }
    }

    impl Fixture {
        fn raise(
            &self,
            observed: ConnectorHistoricalWriteFence,
            raised: ConnectorExternalOperationFence,
        ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
            self.recovery
                .raise_external_fence(ConnectorHistoricalWriteFenceRaiseRequest {
                    historical_binding: historical_binding(),
                    observed,
                    raised,
                    context: context(),
                })
        }

        /// Publish the marker the historical owner would have established, so
        /// the fence ref has the same shape a real takeover observes.
        fn establish_historical(&self, epoch: u64) {
            self.raise(
                ConnectorHistoricalWriteFence::NotEstablished,
                spi_fence(epoch, 1),
            )
            .expect("establish the historical fence marker");
        }

        fn reload(&self) -> IcebergPhysicalTable {
            self.runtime
                .control_state()
                .invalidate_table_cache(NAMESPACE, TABLE);
            self.runtime.load_table(NAMESPACE, TABLE).expect("table")
        }

        /// Commit one snapshot on `main` carrying the ordinary write path's
        /// provenance marker. The manifest list is never opened by this facet,
        /// which reads snapshot summaries only.
        fn commit_provenance_snapshot(&self, marker: serde_json::Value) -> i64 {
            let loaded = self.reload();
            let metadata = loaded.table.metadata();
            let parent = metadata.current_snapshot_id();
            let snapshot_id = 987_654_321;
            let mut additional_properties = HashMap::new();
            additional_properties.insert(
                ICEBERG_WRITE_OPERATION_MARKER_PROPERTY.to_string(),
                marker.to_string(),
            );
            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(metadata.last_sequence_number() + 1)
                .with_timestamp_ms(metadata.last_updated_ms() + 1)
                .with_manifest_list(format!(
                    "{}/metadata/historical-write-test-{snapshot_id}.avro",
                    metadata.location().trim_end_matches('/')
                ))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties,
                })
                .with_schema_id(metadata.current_schema_id())
                .build();
            let commit = TableCommit::builder()
                .ident(loaded.table.identifier().clone())
                .updates(vec![
                    crate::iceberg::TableUpdate::AddSnapshot { snapshot },
                    crate::iceberg::TableUpdate::SetSnapshotRef {
                        ref_name: "main".to_string(),
                        reference: SnapshotReference {
                            snapshot_id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    },
                ])
                .requirements(vec![TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                }])
                .build();
            let catalog = Arc::clone(self.runtime.catalog());
            self.executor
                .block_on(async move { catalog.update_table(commit).await })
                .expect("commit provenance snapshot");
            self.runtime
                .control_state()
                .invalidate_table_cache(NAMESPACE, TABLE);
            snapshot_id
        }

        fn fence_ref(&self) -> String {
            fence_facts_from_spi(&spi_fence(3, 1)).fence_ref()
        }
    }

    #[test]
    fn raising_the_fence_is_monotonic_and_idempotent_on_replay() {
        let fixture = fixture();
        fixture.establish_historical(2);
        let receipt = fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("a strictly higher raise establishes the fence");
        assert!(receipt.matches(&spi_fence(3, 1)));

        // Replaying the identical raise after a lost response must return the
        // same established fence rather than declaring us superseded.
        let replay = fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("an identical raise replay is idempotent");
        assert_eq!(replay.fence_digest(), receipt.fence_digest());

        // A raise that does not outrank the marker on the ref is refused as a
        // typed superseded failure, never as unknown or unsupported.
        let error = fixture
            .raise(established_historical(1), spi_fence(2, 1))
            .expect_err("a lower generation cannot raise the fence");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Superseded)
        );
        assert!(!error.retryable_before_progress());
        assert_ne!(error.kind(), ConnectorErrorKind::Unsupported);

        // The SPI request itself refuses a raise that cannot close the
        // historical authority.
        let error = fixture
            .raise(established_historical(4), spi_fence(3, 1))
            .expect_err("a raise below the historical fence is stale");
        assert_eq!(
            error.external_fence_failure(),
            Some(ConnectorExternalFenceFailure::Stale)
        );
    }

    #[test]
    fn inspect_proves_not_dispatched_and_issues_a_bound_continuation() {
        let fixture = fixture();
        fixture
            .raise(
                ConnectorHistoricalWriteFence::NotEstablished,
                spi_fence(3, 1),
            )
            .expect("raise");
        let descriptor = descriptor(
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        observation
            .validate_for(&descriptor)
            .expect("observation is sealed against its descriptor");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::NotDispatched
        );
        let continuation = observation.continuation.expect("continuation");
        assert!(continuation.is_bound_to(&descriptor.raised_fence));
        assert!(!observation.cleanup_required);
        assert!(observation.application.is_none());
    }

    #[test]
    fn inspect_proves_applied_from_target_ref_provenance() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let snapshot_id = fixture.commit_provenance_snapshot(provenance_marker());
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        observation.validate_for(&descriptor).expect("sealed");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::Applied
        );
        let application = observation.application.expect("finalization facts");
        assert_eq!(
            application.committed_version.snapshot_id(),
            Some(snapshot_id)
        );
        assert_eq!(
            application.finalization,
            ExternalMutationFinalization::Complete
        );
        assert!(observation.continuation.is_none());
        assert!(observation.cleanup_required);
    }

    #[test]
    fn inspect_reports_ambiguous_when_a_marker_carries_other_digests() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let mut marker = provenance_marker();
        marker["cohort_set_digest_base64"] = serde_json::json!(encode_base64([1u8; 32]));
        fixture.commit_provenance_snapshot(marker);
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor, context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::Ambiguous
        );
        assert!(!observation.cleanup_required);
    }

    #[test]
    fn inspect_is_ambiguous_before_the_fence_is_raised_and_when_the_historical_marker_is_gone() {
        let fixture = fixture();
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::Ambiguous,
            "a historical operation can never be classified before the fence is raised"
        );

        // Raising without the historical marker ever having been published
        // (a dropped and recreated table) leaves nothing behind our own marker.
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let observation = fixture
            .recovery
            .inspect(descriptor, context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::Ambiguous,
            "an absent historical marker must never be read as not-applied"
        );
    }

    #[test]
    fn cleanup_retires_only_a_proven_fence_ref() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::Staged
        );
        assert!(observation.cleanup_required);

        let outcome = fixture
            .recovery
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: descriptor.digest(),
                observation: observation.clone(),
                context: context(),
            })
            .expect("cleanup");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                ..
            }
        ));
        assert!(
            observe_fence(fixture.reload().table.metadata(), &fixture.fence_ref())
                .expect("observe")
                .is_none(),
            "the operation's fence ref is retired at terminal"
        );

        // The retained outcome answers a repeated cleanup.
        assert!(matches!(
            fixture
                .recovery
                .cleanup(ConnectorHistoricalWriteCleanupRequest {
                    operation_id: operation_id(),
                    descriptor_digest: descriptor.digest(),
                    observation,
                    context: context(),
                })
                .expect("repeat cleanup"),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
    }

    #[test]
    fn cleanup_refuses_an_observation_it_cannot_prove_owns_the_artifact() {
        let fixture = fixture();
        fixture
            .raise(
                ConnectorHistoricalWriteFence::NotEstablished,
                spi_fence(3, 1),
            )
            .expect("raise");
        let descriptor = descriptor(
            ConnectorHistoricalWriteFence::NotEstablished,
            ConnectorHistoricalWriteDispatchState::NotDispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::NotDispatched
        );
        assert!(!observation.cleanup_required);

        // A not-dispatched operation never asks for cleanup, so no cleanup was
        // ever authorized for it. The request is refused rather than silently
        // succeeding, and its fence stays in place.
        let error = fixture
            .recovery
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: descriptor.digest(),
                observation,
                context: context(),
            })
            .expect_err("cleanup must refuse a not-dispatched observation");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.message().contains("did not issue"));

        assert!(
            observe_fence(fixture.reload().table.metadata(), &fixture.fence_ref())
                .expect("observe")
                .is_some(),
            "a refused cleanup must leave the fence in place"
        );
    }

    #[test]
    fn only_a_terminal_disposition_may_retire_a_fence_ref() {
        for retirable in [
            ConnectorHistoricalWriteDisposition::Applied,
            ConnectorHistoricalWriteDisposition::NotApplied,
            ConnectorHistoricalWriteDisposition::Staged,
        ] {
            assert!(fence_ref_is_retirable_for(retirable), "{retirable:?}");
        }
        for refused in [
            // Retiring here would let the historical owner establish a fence
            // again at its old generation and commit.
            ConnectorHistoricalWriteDisposition::NotDispatched,
            // This generation either does not own the fence or proved nothing.
            ConnectorHistoricalWriteDisposition::Conflict,
            ConnectorHistoricalWriteDisposition::Ambiguous,
            ConnectorHistoricalWriteDisposition::Unsupported,
        ] {
            assert!(!fence_ref_is_retirable_for(refused), "{refused:?}");
        }
    }

    #[test]
    fn cleanup_refuses_a_well_formed_observation_this_generation_never_issued() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let loaded = fixture.reload();
        let held = observe_fence(loaded.table.metadata(), &fixture.fence_ref())
            .expect("observe")
            .expect("the raised marker");
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );

        // A caller can mint a structurally perfect observation: the descriptor
        // seal holds, the proof decodes, and it names the real fence marker.
        // Only "this generation issued it" separates it from a real one, and a
        // cleanup request carries no descriptor for the SPI to re-seal against.
        let forged_proof = IcebergHistoricalWriteProofV1 {
            version: ICEBERG_HISTORICAL_WRITE_PAYLOAD_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            raised_fence_digest: descriptor.raised_fence.digest().to_vec(),
            namespace: NAMESPACE.to_string(),
            table: TABLE.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            target_ref: "main".to_string(),
            target_snapshot_id: None,
            fence_ref: fixture.fence_ref(),
            fence_snapshot_id: held.snapshot_id,
            fence_generation: [1, 3, 1],
            applied_snapshot_id: None,
            lineage_complete: true,
            disposition: "staged".to_string(),
        };
        let forged = ConnectorHistoricalWriteObservation::try_new(
            &descriptor,
            ConnectorHistoricalWriteDisposition::Staged,
            ConnectorHistoricalWriteOutcomeFacts {
                application: None,
                continuation: None,
                cleanup_required: true,
            },
            ConnectorHistoricalWriteProof::try_new(
                encode(&forged_proof, "forged proof").expect("encode"),
            )
            .expect("proof"),
        )
        .expect("a forged observation is still well formed");
        forged
            .validate_for(&descriptor)
            .expect("and it satisfies the SPI seal");

        let error = fixture
            .recovery
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: descriptor.digest(),
                observation: forged,
                context: context(),
            })
            .expect_err("a never-issued observation must not authorize a removal");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.message().contains("did not issue"));
        assert!(
            observe_fence(fixture.reload().table.metadata(), &fixture.fence_ref())
                .expect("observe")
                .is_some(),
            "the fence ref survives a refused cleanup"
        );
    }

    #[test]
    fn cleanup_refuses_a_descriptor_digest_that_conflicts_with_the_sealed_proof() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor, context())
            .expect("inspect");
        assert!(observation.cleanup_required);

        let error = fixture
            .recovery
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: [0; 32],
                observation,
                context: context(),
            })
            .expect_err("cleanup must refuse a foreign descriptor digest");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(
            observe_fence(fixture.reload().table.metadata(), &fixture.fence_ref())
                .expect("observe")
                .is_some(),
            "a refused cleanup must leave the fence in place"
        );
    }

    #[test]
    fn reconcile_cleanup_requires_matching_evidence_and_a_retained_outcome() {
        let fixture = fixture();
        let evidence = |incarnation: ConnectorInstanceIncarnation| {
            ExternalMutationEvidence::try_new(
                ICEBERG_HISTORICAL_WRITE_EVIDENCE_VERSION,
                instance_descriptor(),
                incarnation,
                ConnectorMutationOperationId::from_bytes(operation_id().to_bytes()),
                ICEBERG_HISTORICAL_WRITE_CLEANUP_KIND,
                Bytes::from_static(b"{}"),
            )
            .expect("evidence")
        };
        let error = fixture
            .recovery
            .reconcile_cleanup(operation_id(), evidence(current_incarnation()), context())
            .expect_err("no retained outcome");
        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);

        let error = fixture
            .recovery
            .reconcile_cleanup(
                operation_id(),
                evidence(ConnectorInstanceIncarnation::from_bytes([4; 16])),
                context(),
            )
            .expect_err("foreign generation");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn reconcile_cleanup_replays_a_retained_terminal_outcome() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let descriptor = descriptor(
            established_historical(2),
            ConnectorHistoricalWriteDispatchState::Completed,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        fixture
            .recovery
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: descriptor.digest(),
                observation,
                context: context(),
            })
            .expect("cleanup");
        let evidence = ExternalMutationEvidence::try_new(
            ICEBERG_HISTORICAL_WRITE_EVIDENCE_VERSION,
            instance_descriptor(),
            current_incarnation(),
            ConnectorMutationOperationId::from_bytes(operation_id().to_bytes()),
            ICEBERG_HISTORICAL_WRITE_CLEANUP_KIND,
            Bytes::from_static(b"{}"),
        )
        .expect("evidence");
        assert!(matches!(
            fixture
                .recovery
                .reconcile_cleanup(operation_id(), evidence, context())
                .expect("reconcile"),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
    }
}
