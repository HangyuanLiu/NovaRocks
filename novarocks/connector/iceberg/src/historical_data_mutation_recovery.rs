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

//! Provider-owned historical inspection for a *direct* Iceberg data mutation.
//!
//! # What this facet is
//!
//! TRUNCATE and ADD FILES are planned and executed once by one exact Iceberg
//! control generation. After a frontend takeover that generation is gone: its
//! cached plan, its terminal record and its lease no longer exist and can never
//! be reconstructed. The current generation is therefore asked to classify the
//! old attempt from immutable external truth alone.
//!
//! This facet is installed separately from the ordinary data-mutation
//! capability (`control_factory.rs`). An ordinary execution path must never
//! reach it as a fallback, and it never calls an ordinary old-owner method
//! (`plan_mutation` / `execute` / `reconcile`). It registers no binding,
//! constructs no historical runtime session and never replays a destructive
//! mutation that was already dispatched.
//!
//! # Proof sources, in order of authority
//!
//! 1. **The fence ref marker** on `novarocks-write-fence-<operation-id>`
//!    ([`crate::commit::write_fence`]). It proves which authority currently
//!    owns the operation and, once this generation has raised it, that no
//!    historical authority can still execute.
//! 2. **The target ref's own data-mutation marker.** The ordinary direct
//!    mutation stamps [`MARKER_PROPERTY`] into the snapshot summary it commits,
//!    carrying the operation id and the identity digest of the exact generation
//!    that produced it. Finding it in the target ref lineage is the only proof
//!    that the mutation applied.
//! 3. **The staged artifacts named by the bounded opaque evidence** the
//!    historical attempt returned when its commit outcome was unknown. It is
//!    used only for cross-checks and never outranks external truth; evidence
//!    that cannot be read or that names another operation forces a refusal.
//!
//! A provider-private operation repository is deliberately *not* consulted:
//! process-local records do not survive the owner that wrote them, so they can
//! never prove anything about a historical attempt.
//!
//! # The rules that matter most
//!
//! Absent evidence is never read as "did not apply". A missing marker, a
//! missing or unreadable artifact, a digest mismatch, an unknown marker layout,
//! a truncated ancestry or a table that may have been dropped and recreated are
//! all [`Ambiguous`].
//!
//! For ADD FILES a source set that is only partially visible is
//! [`PartiallyApplied`] — never [`Applied`] and never [`NotApplied`] — and the
//! source scope stays owned by the operation. This facet never reasons about
//! releasing a source scope; that is the frontend's decision inside its own
//! fenced journal transaction.
//!
//! [`Ambiguous`]: ConnectorHistoricalDataMutationDisposition::Ambiguous
//! [`PartiallyApplied`]: ConnectorHistoricalDataMutationDisposition::PartiallyApplied
//! [`Applied`]: ConnectorHistoricalDataMutationDisposition::Applied
//! [`NotApplied`]: ConnectorHistoricalDataMutationDisposition::NotApplied

// Design: ADR-0068 (docs/adr/ADR-0068-external-write-fence-as-catalog-linearization-point.md)

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt,
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExternalFenceFailure, ConnectorExternalFenceReceipt, ConnectorExternalOperationFence,
    ConnectorHistoricalDataMutationApplication, ConnectorHistoricalDataMutationCleanupReceipt,
    ConnectorHistoricalDataMutationCleanupRequest, ConnectorHistoricalDataMutationContinuation,
    ConnectorHistoricalDataMutationDescriptor, ConnectorHistoricalDataMutationDisposition,
    ConnectorHistoricalDataMutationFenceRaiseRequest, ConnectorHistoricalDataMutationObservation,
    ConnectorHistoricalDataMutationOutcomeFacts, ConnectorHistoricalDataMutationProof,
    ConnectorHistoricalDataMutationRecovery, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext,
    ConnectorTableIdentity, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
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
const ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION: u16 = 1;
/// Operation kind reported on cleanup reconciliation evidence.
const ICEBERG_HISTORICAL_DATA_MUTATION_CLEANUP_KIND: &str =
    "iceberg.historical_data_mutation_cleanup.v1";
/// Schema version of that evidence envelope.
const ICEBERG_HISTORICAL_DATA_MUTATION_EVIDENCE_VERSION: u16 = 1;

/// Snapshot summary key the ordinary direct-mutation path stamps onto the
/// snapshot it commits, and the marker layout version this build understands.
///
/// The producing side owns both in `catalog_control/data_mutation.rs`, where
/// the constant and the marker type are module-private. They are mirrored here
/// read-only, and any drift degrades to "unresolved" rather than to a wrong
/// classification: a marker this build cannot decode, or whose `version` is
/// anything else, is reported as ambiguous instead of reinterpreted.
const MARKER_PROPERTY: &str = "novarocks.connector.data-mutation.v1";
const MARKER_VALUE_VERSION: u16 = 1;
/// Digest domain of the ordinary path's `identity_digest`, mirrored so this
/// facet can recompute the exact value a historical generation stamped.
const IDENTITY_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.data-mutation.identity.v1\0";
/// Payload version of the ordinary path's receipt and unknown-commit evidence.
const ORDINARY_RECEIPT_PAYLOAD_VERSION: u16 = 1;
const ORDINARY_EVIDENCE_PAYLOAD_VERSION: u16 = 1;

/// Upper bound on the snapshot ancestry this facet will walk. A lineage that
/// does not end within the bound is reported as unproven, never as absence.
const MAX_TARGET_LINEAGE_WALK: usize = 50_000;

/// Upper bound on retained cleanup outcomes and issued observation digests.
/// Retention is what lets a lost cleanup response be reconciled; it is bounded
/// so a long-lived generation cannot grow without limit.
const MAX_RETAINED_CLEANUP_OUTCOMES: usize = 4_096;

/// A snapshot id value no Iceberg snapshot can have, used in a proof that does
/// not pin a real fence marker so it can never anchor a cleanup.
const NO_FENCE_SNAPSHOT: i64 = -1;

/// Read-only mirror of the ordinary direct-mutation marker.
///
/// `deny_unknown_fields` is deliberate: a producer that adds a field is a
/// layout this build does not understand, and the only safe answer to that is
/// ambiguity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationMarkerV1 {
    version: u16,
    identity_digest_hex: String,
    incarnation_hex: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

/// Read-only mirror of the ordinary path's unknown-commit evidence payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationEvidenceV1 {
    version: u16,
    namespace: String,
    table: String,
    target_ref: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    identity_digest_hex: String,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

/// Read-only mirror of the ordinary path's durable receipt payload, so a
/// recovered application reports exactly the shape the live path would have.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationReceiptV1 {
    version: u16,
    snapshot_id: i64,
}

/// Opaque provider proof returned with every classification.
///
/// The frontend persists it verbatim and never decodes it. This facet decodes
/// it again in `cleanup` so a cleanup can only ever act on the exact external
/// state a previous `inspect` proved.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalDataMutationProofV1 {
    version: u16,
    descriptor_digest: Vec<u8>,
    raised_fence_digest: Vec<u8>,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    operation_id_hex: String,
    operation_kind: String,
    /// The identity this facet recomputed for the historical generation, and
    /// the value a matching marker must carry.
    identity_digest_hex: String,
    target_snapshot_id: Option<i64>,
    fence_ref: String,
    /// The marker this generation holds on the fence ref, or [`NO_FENCE_SNAPSHOT`].
    fence_snapshot_id: i64,
    fence_generation: [u64; 3],
    /// Snapshot proving the mutation applied, when one was found.
    applied_snapshot_id: Option<i64>,
    /// Whether the target lineage could be walked to a proven end.
    lineage_complete: bool,
    /// Echoed for ADD FILES so a cleanup can never be paired with another
    /// source set. This facet never releases it.
    source_scope_digest_hex: Option<String>,
    disposition: String,
}

/// Opaque acknowledgement of one raised fence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalDataMutationFenceReceiptV1 {
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

/// Provider-signed authorization to run the same stable statement again under
/// the current generation.
///
/// It carries the base state this generation proved, not the historical
/// prepared handle: a continued TRUNCATE must be planned again and must
/// re-verify table identity and base state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IcebergHistoricalDataMutationContinuationV1 {
    version: u16,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    source_scope_digest_hex: Option<String>,
    target_ref: String,
    raised_fence_digest: Vec<u8>,
    table_uuid: String,
    target_snapshot_id: Option<i64>,
}

/// A retained cleanup outcome, kept so a lost response can be reconciled.
#[derive(Clone)]
struct CleanupRecord {
    outcome: ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
    proof: IcebergHistoricalDataMutationProofV1,
    descriptor_digest: [u8; 32],
    observation_digest: [u8; 32],
}

/// Bounded, insertion-ordered retention of cleanup outcomes.
#[derive(Default)]
struct CleanupRetention {
    records: HashMap<ConnectorMutationOperationId, CleanupRecord>,
    order: VecDeque<ConnectorMutationOperationId>,
}

impl CleanupRetention {
    fn get(&self, operation_id: &ConnectorMutationOperationId) -> Option<CleanupRecord> {
        self.records.get(operation_id).cloned()
    }

    fn insert(&mut self, operation_id: ConnectorMutationOperationId, record: CleanupRecord) {
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

/// The narrow historical direct-mutation facet of one Iceberg control
/// generation.
#[derive(Clone)]
pub struct IcebergHistoricalDataMutationRecovery {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    binding_key: ConnectorExecutionBindingKey,
    runtime: Arc<IcebergControlRuntime>,
    cleanup_outcomes: Arc<Mutex<CleanupRetention>>,
    issued_observations: Arc<Mutex<IssuedObservations>>,
}

impl IcebergHistoricalDataMutationRecovery {
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
                "Iceberg historical data mutation recovery request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg historical data mutation recovery deadline elapsed",
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
                "Iceberg historical data mutation descriptor belongs to another connector instance",
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

    fn cleanup_evidence(
        &self,
        operation_id: ConnectorMutationOperationId,
        proof: &IcebergHistoricalDataMutationProofV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        ExternalMutationEvidence::try_new(
            ICEBERG_HISTORICAL_DATA_MUTATION_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            ICEBERG_HISTORICAL_DATA_MUTATION_CLEANUP_KIND,
            encode(proof, "historical data mutation cleanup evidence")?,
        )
    }

    /// Neutral finalization facts for a mutation proven to have applied.
    ///
    /// The receipt mirrors the shape the ordinary path mints, so a frontend
    /// that finalizes a recovered operation stores exactly the durable payload
    /// it would have stored had the reply not been lost.
    fn applied_facts(
        &self,
        descriptor: &ConnectorHistoricalDataMutationDescriptor,
        snapshot_id: i64,
    ) -> Result<ConnectorHistoricalDataMutationApplication, ConnectorError> {
        let committed_version = ConnectorCommittedVersion::try_new(
            Bytes::from(format!("iceberg/historical-data-mutation/v1/{snapshot_id}")),
            Some(snapshot_id),
        )?;
        let payload = encode(
            &IcebergDataMutationReceiptV1 {
                version: ORDINARY_RECEIPT_PAYLOAD_VERSION,
                snapshot_id,
            },
            "historical data mutation receipt",
        )?;
        Ok(ConnectorHistoricalDataMutationApplication {
            committed_version,
            receipt: ConnectorDataMutationReceipt::try_new(
                self.descriptor.clone(),
                self.incarnation,
                descriptor.operation_id,
                descriptor.family.operation_kind(),
                descriptor.request_digest,
                descriptor.plan_digest,
                descriptor.state_digest,
                descriptor.plan_summary,
                payload,
            )?,
            finalization: ExternalMutationFinalization::Complete,
        })
    }
}

impl ConnectorHistoricalDataMutationRecovery for IcebergHistoricalDataMutationRecovery {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn raise_external_fence(
        &self,
        request: ConnectorHistoricalDataMutationFenceRaiseRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        self.validate_context(&request.context)?;
        // Fails closed unless the requested fence strictly supersedes the
        // historical one. A raise that does not outrank the old authority
        // cannot close it, so it is refused rather than accepted as a no-op.
        request.validate()?;
        if request.historical_binding.instance_id != self.descriptor.instance_id {
            return Err(invalid(
                "Iceberg historical data mutation fence raise names another connector instance",
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
        descriptor: ConnectorHistoricalDataMutationDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalDataMutationObservation, ConnectorError> {
        self.validate_context(&context)?;
        descriptor.validate()?;
        if descriptor.historical_binding.instance_id != self.descriptor.instance_id {
            return Err(invalid(
                "Iceberg historical data mutation descriptor names another connector instance",
            ));
        }
        let identity_digest_hex =
            expected_identity_digest_hex(&self.descriptor.provider_id, &descriptor);
        let artifacts = inspect_evidence(&descriptor, &identity_digest_hex);
        let loaded = self.load_fresh(&descriptor.table)?;
        let metadata = loaded.table.metadata();
        let facts = fence_facts_from_spi(&descriptor.raised_fence);
        let fence_ref = facts.fence_ref();

        let fence = observe_raised_fence(metadata, &fence_ref, &facts);
        let target = observe_target_ref(metadata, &descriptor, &identity_digest_hex);
        let outcome = classify(&fence, &descriptor, &target, artifacts);

        let proof = IcebergHistoricalDataMutationProofV1 {
            version: ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            raised_fence_digest: descriptor.raised_fence.digest().to_vec(),
            namespace: descriptor.table.namespace.to_string(),
            table: descriptor.table.table.to_string(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: descriptor.target_ref.as_str().to_string(),
            operation_id_hex: hex_encode(descriptor.operation_id.to_bytes()),
            operation_kind: descriptor.family.operation_kind().to_string(),
            identity_digest_hex,
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
            source_scope_digest_hex: descriptor
                .source_scope
                .map(|scope| hex_encode(scope.digest())),
            disposition: outcome.disposition.label().to_string(),
        };
        let proof = ConnectorHistoricalDataMutationProof::try_new(encode(
            &proof,
            "historical data mutation proof",
        )?)?;

        let application = match outcome.disposition {
            ConnectorHistoricalDataMutationDisposition::Applied => {
                let snapshot_id = target.matched_snapshot_id.ok_or_else(|| {
                    corrupt(
                        "Iceberg historical data mutation applied classification has no snapshot",
                    )
                })?;
                Some(self.applied_facts(&descriptor, snapshot_id)?)
            }
            _ => None,
        };
        let continuation = if outcome.continuation_allowed {
            Some(ConnectorHistoricalDataMutationContinuation::try_new(
                &descriptor.raised_fence,
                continuation_payload(&descriptor, metadata, &target)?,
            )?)
        } else {
            None
        };
        let observation = ConnectorHistoricalDataMutationObservation::try_new(
            &descriptor,
            outcome.disposition,
            ConnectorHistoricalDataMutationOutcomeFacts {
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
        request: ConnectorHistoricalDataMutationCleanupRequest,
    ) -> Result<
        ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
        ConnectorError,
    > {
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
                "Iceberg historical data mutation cleanup presents an observation this generation did not issue",
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
                    "Iceberg historical data mutation cleanup replays another observation for this operation",
                ));
            }
            return Ok(record.outcome);
        }
        request.observation.proof.validate()?;
        let proof = decode_proof(request.observation.proof.payload())?;
        if proof.descriptor_digest.as_slice() != request.descriptor_digest
            || proof.descriptor_digest.as_slice() != request.observation.descriptor_digest
            || proof.raised_fence_digest.as_slice() != request.observation.raised_fence_digest
            || proof.operation_id_hex != hex_encode(request.operation_id.to_bytes())
            || proof.operation_kind != request.observation.family.operation_kind()
            || proof.source_scope_digest_hex
                != request
                    .observation
                    .source_scope
                    .map(|scope| hex_encode(scope.digest()))
            || request.observation.operation_id != request.operation_id
        {
            return Err(corrupt(
                "Iceberg historical data mutation cleanup proof conflicts with its observation",
            ));
        }
        if !request.observation.cleanup_required {
            return Err(invalid(
                "Iceberg historical data mutation cleanup was not requested by its observation",
            ));
        }
        if !fence_ref_is_retirable_for(request.observation.disposition) {
            return Err(invalid(format!(
                "Iceberg historical data mutation cleanup refuses a {} observation",
                request.observation.disposition.label()
            )));
        }
        if !is_fence_ref(&proof.fence_ref) || proof.fence_snapshot_id <= 0 {
            return Err(corrupt(
                "Iceberg historical data mutation cleanup proof does not name a provider-owned fence marker",
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
            known_uncommitted("Iceberg historical data mutation cleanup table UUID drifted")
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
                        "Iceberg historical data mutation cleanup no longer holds the current fence",
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
                                "Iceberg historical data mutation cleanup lost the fence before retirement",
                            )
                        }
                        Err(error) => ExternalMutationOutcome::CommitUnknown {
                            failure: ConnectorMutationFailure::new(
                                ConnectorMutationFailureKind::Unavailable,
                                format!("Iceberg historical data mutation cleanup: {error}"),
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
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<
        ExternalMutationOutcome<ConnectorHistoricalDataMutationCleanupReceipt>,
        ConnectorError,
    > {
        self.validate_context(&context)?;
        if evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.incarnation
            || evidence.operation_id() != operation_id
            || evidence.operation_kind() != ICEBERG_HISTORICAL_DATA_MUTATION_CLEANUP_KIND
        {
            return Err(invalid(
                "Iceberg historical data mutation cleanup evidence does not match this generation",
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
                    "Iceberg historical data mutation cleanup has no retained outcome to reconcile",
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
        let receipt = ConnectorHistoricalDataMutationCleanupReceipt {
            descriptor_digest: record.descriptor_digest,
            observation_digest: record.observation_digest,
        };
        let outcome = if loaded.table.metadata().uuid().to_string() != record.proof.table_uuid {
            known_uncommitted(
                "Iceberg historical data mutation cleanup table UUID drifted during reconciliation",
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
                        "Iceberg historical data mutation cleanup fence ref still points at the inspected marker",
                    )
                }
                Ok(Some(_)) => known_uncommitted(
                    "Iceberg historical data mutation cleanup fence ref moved to another marker",
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

/// What the bounded opaque evidence says about the historical attempt's staged
/// artifacts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactEvidence {
    /// The historical attempt reached a terminal outcome without minting
    /// unknown-commit evidence. There is no artifact claim to check.
    NotCarried,
    /// The evidence decodes and names exactly this operation and generation.
    Agrees,
    /// The evidence is carried but this build cannot read it.
    Unreadable,
    /// The evidence decodes but describes a different operation, table or plan.
    Disagrees,
}

/// What the target data ref says about the historical mutation.
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
    /// A marker proved this operation's identity but registered strictly fewer
    /// files, rows and bytes than the sealed plan. Only ADD FILES can be in
    /// this state and still be classified.
    summary_shortfall: bool,
    /// More than one snapshot in the lineage claimed this operation.
    multiple_matches: bool,
    /// The target ref exists but is not a branch.
    non_branch_target: bool,
    /// A named non-main target ref does not exist at all.
    missing_target_ref: bool,
}

struct Classification {
    disposition: ConnectorHistoricalDataMutationDisposition,
    cleanup_required: bool,
    continuation_allowed: bool,
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

/// Recompute the identity digest a historical generation would have stamped.
///
/// Every input is a sealed descriptor field plus this provider's own id, so a
/// marker can only satisfy it if it was produced by exactly that generation for
/// exactly that plan. This is what turns "a marker names this operation" into
/// "a marker proves this operation", and it is why the frontend never has to
/// carry a provider-private identity value.
fn expected_identity_digest_hex(
    provider_id: &ConnectorProviderId,
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, provider_id.as_str().as_bytes());
    digest_bytes(
        &mut hasher,
        descriptor
            .historical_binding
            .instance_id
            .as_str()
            .as_bytes(),
    );
    digest_bytes(
        &mut hasher,
        &descriptor.historical_binding.incarnation.to_bytes(),
    );
    digest_bytes(&mut hasher, &descriptor.operation_id.to_bytes());
    digest_bytes(&mut hasher, descriptor.family.operation_kind().as_bytes());
    digest_bytes(&mut hasher, &descriptor.request_digest);
    digest_bytes(&mut hasher, &descriptor.plan_digest);
    digest_bytes(&mut hasher, &descriptor.state_digest);
    hex_encode(<[u8; 32]>::from(hasher.finalize()))
}

/// Cross-check the bounded opaque evidence the historical attempt returned.
///
/// The evidence is the third proof source and the only one that names the
/// staged artifacts of the old attempt. It is never trusted over external
/// truth, but evidence this build cannot read, or that describes another
/// operation, means the recovery record itself cannot be tied to the artifacts
/// it claims — so no conclusion may be drawn from it.
fn inspect_evidence(
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
    identity_digest_hex: &str,
) -> ArtifactEvidence {
    let Some(evidence) = &descriptor.evidence else {
        return ArtifactEvidence::NotCarried;
    };
    let Ok(decoded) =
        serde_json::from_slice::<IcebergDataMutationEvidenceV1>(evidence.provider_payload())
    else {
        return ArtifactEvidence::Unreadable;
    };
    if decoded.version != ORDINARY_EVIDENCE_PAYLOAD_VERSION {
        return ArtifactEvidence::Unreadable;
    }
    if decoded.namespace != descriptor.table.namespace.as_ref()
        || decoded.table != descriptor.table.table.as_ref()
        || decoded.target_ref != descriptor.target_ref.as_str()
        || decoded.operation_id_hex != hex_encode(descriptor.operation_id.to_bytes())
        || decoded.operation_kind != descriptor.family.operation_kind()
        || decoded.request_digest_hex != hex_encode(descriptor.request_digest)
        || decoded.plan_digest_hex != hex_encode(descriptor.plan_digest)
        || decoded.state_digest_hex != hex_encode(descriptor.state_digest)
        || decoded.identity_digest_hex != identity_digest_hex
        || !summary_equals(
            descriptor.plan_summary,
            decoded.file_count,
            decoded.row_count,
            decoded.total_bytes,
        )
    {
        return ArtifactEvidence::Disagrees;
    }
    ArtifactEvidence::Agrees
}

/// Walk the target data ref and look for this operation's mutation provenance.
fn observe_target_ref(
    metadata: &TableMetadata,
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
    identity_digest_hex: &str,
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
        // "nothing committed here" state rather than missing evidence. This
        // mirrors how the ordinary path resolves the target snapshot.
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
        match provenance_match(snapshot.summary(), descriptor, identity_digest_hex) {
            ProvenanceMatch::None => {}
            ProvenanceMatch::UnknownLayout => observation.unknown_marker_layout = true,
            ProvenanceMatch::DigestMismatch => observation.digest_mismatch = true,
            ProvenanceMatch::SummaryShortfall => observation.summary_shortfall = true,
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
                    provenance_match(snapshot.summary(), descriptor, identity_digest_hex),
                    ProvenanceMatch::Matched | ProvenanceMatch::SummaryShortfall
                )
        });
    }
    observation
}

enum ProvenanceMatch {
    None,
    Matched,
    SummaryShortfall,
    DigestMismatch,
    UnknownLayout,
}

/// Compare one snapshot summary against the historical operation identity.
fn provenance_match(
    summary: &Summary,
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
    identity_digest_hex: &str,
) -> ProvenanceMatch {
    let Some(raw) = summary.additional_properties.get(MARKER_PROPERTY) else {
        return ProvenanceMatch::None;
    };
    let Ok(marker) = serde_json::from_str::<IcebergDataMutationMarkerV1>(raw) else {
        return ProvenanceMatch::UnknownLayout;
    };
    if marker.version != MARKER_VALUE_VERSION {
        return ProvenanceMatch::UnknownLayout;
    }
    if marker.operation_id_hex != hex_encode(descriptor.operation_id.to_bytes()) {
        return ProvenanceMatch::None;
    }
    // From here the marker names this operation, so any disagreement is a
    // mismatch to report rather than a non-match to skip.
    if marker.identity_digest_hex != identity_digest_hex
        || marker.incarnation_hex
            != hex_encode(descriptor.historical_binding.incarnation.to_bytes())
        || marker.operation_kind != descriptor.family.operation_kind()
        || marker.request_digest_hex != hex_encode(descriptor.request_digest)
        || marker.plan_digest_hex != hex_encode(descriptor.plan_digest)
        || marker.state_digest_hex != hex_encode(descriptor.state_digest)
        || marker.target_ref != descriptor.target_ref.as_str()
    {
        return ProvenanceMatch::DigestMismatch;
    }
    // The marker records what the commit actually registered. Equality is the
    // only proof that the whole sealed plan landed; a strict subset is a
    // partially visible source set, and anything larger is unexplainable.
    if summary_equals(
        descriptor.plan_summary,
        marker.file_count,
        marker.row_count,
        marker.total_bytes,
    ) {
        return ProvenanceMatch::Matched;
    }
    if summary_is_strict_subset(
        descriptor.plan_summary,
        marker.file_count,
        marker.row_count,
        marker.total_bytes,
    ) {
        return ProvenanceMatch::SummaryShortfall;
    }
    ProvenanceMatch::DigestMismatch
}

fn summary_equals(
    summary: ConnectorDataMutationPlanSummary,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
) -> bool {
    summary.file_count() == file_count
        && summary.row_count() == row_count
        && summary.total_bytes() == total_bytes
}

fn summary_is_strict_subset(
    summary: ConnectorDataMutationPlanSummary,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
) -> bool {
    file_count <= summary.file_count()
        && row_count <= summary.row_count()
        && total_bytes <= summary.total_bytes()
        && (file_count < summary.file_count()
            || row_count < summary.row_count()
            || total_bytes < summary.total_bytes())
}

/// The whole classification decision, expressed over proven facts only.
fn classify(
    fence: &RaisedFenceObservation,
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
    target: &TargetRefObservation,
    artifacts: ArtifactEvidence,
) -> Classification {
    // The recovery record claims artifacts it cannot name. Nothing about this
    // operation is provable, including whether it applied.
    if matches!(
        artifacts,
        ArtifactEvidence::Unreadable | ArtifactEvidence::Disagrees
    ) {
        return unresolved(ConnectorHistoricalDataMutationDisposition::Ambiguous);
    }
    // Applied is a monotonic fact about immutable history: once a snapshot in
    // the target lineage carries this operation's sealed provenance and the
    // whole sealed plan, no later fence movement can undo it. It is therefore
    // decided before any fence question. `unknown_marker_layout` still blocks
    // it, because a marker this build cannot read might be a second commit of
    // the same operation.
    if target.matched_snapshot_id.is_some()
        && !target.multiple_matches
        && !target.digest_mismatch
        && !target.unknown_marker_layout
        && !target.summary_shortfall
    {
        return Classification {
            disposition: ConnectorHistoricalDataMutationDisposition::Applied,
            // The fence ref of an applied operation is retired only while this
            // generation still holds it, and only when the historical owner had
            // established one: retiring a ref the old owner never published
            // would let it publish one at its old generation and execute.
            cleanup_required: matches!(fence, RaisedFenceObservation::Held { .. })
                && descriptor.historical_fence.is_established(),
            continuation_allowed: false,
        };
    }
    if target.non_branch_target {
        // A tag cannot receive a data mutation, so this provider has no lineage
        // semantics to classify a historical direct mutation against.
        return unresolved(ConnectorHistoricalDataMutationDisposition::Unsupported);
    }
    // ADD FILES owns an immutable external source set. When the operation's own
    // provenance is in the lineage but does not account for the whole set, part
    // of the source is provably inside the table: that is neither applied nor
    // not applied, and the source scope must stay owned by the operation.
    if descriptor.family.owns_source_scope()
        && (target.multiple_matches || target.summary_shortfall)
        && !target.unknown_marker_layout
        && !target.digest_mismatch
    {
        return unresolved(ConnectorHistoricalDataMutationDisposition::PartiallyApplied);
    }
    if target.multiple_matches
        || target.digest_mismatch
        || target.unknown_marker_layout
        || target.summary_shortfall
        || target.off_lineage_match
        || target.missing_target_ref
    {
        return unresolved(ConnectorHistoricalDataMutationDisposition::Ambiguous);
    }
    match fence {
        RaisedFenceObservation::Superseded => Classification {
            disposition: ConnectorHistoricalDataMutationDisposition::Conflict,
            // Another authority owns the fence; removing it is not ours to do.
            cleanup_required: false,
            continuation_allowed: false,
        },
        RaisedFenceObservation::Unproven { .. } => {
            unresolved(ConnectorHistoricalDataMutationDisposition::Ambiguous)
        }
        RaisedFenceObservation::Held {
            has_predecessor, ..
        } => {
            if !target.lineage_complete {
                // The mutation may have committed into a part of the history
                // that no longer exists. Absence is not proof.
                return unresolved(ConnectorHistoricalDataMutationDisposition::Ambiguous);
            }
            if descriptor.historical_fence.is_established() {
                if !*has_predecessor {
                    // The historical attempt published a marker, but the fence
                    // ref this generation raised has nothing behind it: the
                    // marker that would carry the old attempt's provenance is
                    // gone (the table was dropped and recreated, or the ref was
                    // rebuilt). Nothing can be concluded from its absence.
                    return unresolved(ConnectorHistoricalDataMutationDisposition::Ambiguous);
                }
                // The mutation provably did not change the table, and the old
                // authority is closed: its commit pins a marker this generation
                // has already replaced. What remains is the operation's own
                // fence ref, an artifact nothing else can reference.
                return Classification {
                    disposition: ConnectorHistoricalDataMutationDisposition::CleanupRequired,
                    cleanup_required: true,
                    continuation_allowed: false,
                };
            }
            // No historical fence was ever established, so the destructive
            // execute could not have been dispatched under one, and the raised
            // fence has closed that authority for good. Nothing was left
            // behind, and retiring our own marker here would reopen the ref for
            // the old owner to establish at its old generation.
            Classification {
                disposition: ConnectorHistoricalDataMutationDisposition::NotApplied,
                cleanup_required: false,
                continuation_allowed: descriptor.journal_proves_nothing_dispatched(),
            }
        }
    }
}

/// Whether an operation in this disposition may have its fence ref retired.
///
/// Only a terminal classification whose historical authority already holds an
/// assertion pinned to a marker qualifies: removing that marker makes the
/// assertion permanently unsatisfiable. `NotApplied` is refused because this
/// facet only reaches it when the historical owner never established a fence —
/// retiring would let that owner establish one at its old generation and
/// execute. `PartiallyApplied`, `Conflict`, `Ambiguous` and `Unsupported` are
/// refused because this generation either does not own the fence or has proven
/// nothing about artifact ownership.
const fn fence_ref_is_retirable_for(
    disposition: ConnectorHistoricalDataMutationDisposition,
) -> bool {
    matches!(
        disposition,
        ConnectorHistoricalDataMutationDisposition::Applied
            | ConnectorHistoricalDataMutationDisposition::CleanupRequired
    )
}

fn unresolved(disposition: ConnectorHistoricalDataMutationDisposition) -> Classification {
    Classification {
        disposition,
        cleanup_required: false,
        continuation_allowed: false,
    }
}

/// Bind a continuation to the raised fence, the stable operation, the sealed
/// historical input digests and the base state this generation proved.
fn continuation_payload(
    descriptor: &ConnectorHistoricalDataMutationDescriptor,
    metadata: &TableMetadata,
    target: &TargetRefObservation,
) -> Result<Bytes, ConnectorError> {
    encode(
        &IcebergHistoricalDataMutationContinuationV1 {
            version: ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION,
            operation_id_hex: hex_encode(descriptor.operation_id.to_bytes()),
            operation_kind: descriptor.family.operation_kind().to_string(),
            request_digest_hex: hex_encode(descriptor.request_digest),
            plan_digest_hex: hex_encode(descriptor.plan_digest),
            state_digest_hex: hex_encode(descriptor.state_digest),
            source_scope_digest_hex: descriptor
                .source_scope
                .map(|scope| hex_encode(scope.digest())),
            target_ref: descriptor.target_ref.as_str().to_string(),
            raised_fence_digest: descriptor.raised_fence.digest().to_vec(),
            table_uuid: metadata.uuid().to_string(),
            target_snapshot_id: target.head_snapshot_id,
        },
        "historical data mutation continuation",
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
        &IcebergHistoricalDataMutationFenceReceiptV1 {
            version: ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION,
            namespace: fence.table().namespace.to_string(),
            table: fence.table().table.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            fence_ref: fence_ref.to_string(),
            fence_snapshot_id,
            reused,
        },
        "historical data mutation fence receipt",
    )?;
    ConnectorExternalFenceReceipt::try_new(fence, payload)
}

fn cleanup_receipt(
    request: &ConnectorHistoricalDataMutationCleanupRequest,
) -> ConnectorHistoricalDataMutationCleanupReceipt {
    ConnectorHistoricalDataMutationCleanupReceipt {
        descriptor_digest: request.descriptor_digest,
        observation_digest: request.observation.digest(),
    }
}

fn decode_proof(payload: &Bytes) -> Result<IcebergHistoricalDataMutationProofV1, ConnectorError> {
    let proof: IcebergHistoricalDataMutationProofV1 =
        serde_json::from_slice(payload).map_err(|error| {
            corrupt(format!(
                "decode Iceberg historical data mutation proof: {error}"
            ))
        })?;
    if proof.version != ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION {
        return Err(corrupt(format!(
            "Iceberg historical data mutation proof has layout version {}; this build understands {}",
            proof.version, ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION
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

/// Length-prefixed field feed, mirroring the ordinary path's digest helper so
/// the recomputed identity is byte-identical.
fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(ALPHABET[(byte >> 4) as usize] as char);
        encoded.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    encoded
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
        format!("Iceberg historical data mutation cleanup retention lock: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorClusterIdentity, ConnectorDataMutationSourceScope,
        ConnectorExternalFenceGeneration, ConnectorHistoricalDataMutationCheckpoint,
        ConnectorHistoricalDataMutationDispatchState, ConnectorHistoricalDataMutationFamily,
        ConnectorHistoricalDataMutationFence, ConnectorHistoricalDataMutationFenceFacts,
        ConnectorHistoricalDataMutationIdentity, ConnectorHistoricalDataMutationPhase,
        ConnectorInstanceId, ConnectorProviderId, ConnectorWriteOperationId,
        ConnectorWriteTargetRef,
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

    fn historical_incarnation() -> ConnectorInstanceIncarnation {
        ConnectorInstanceIncarnation::from_bytes([9; 16])
    }

    fn table_identity() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(INSTANCE).expect("instance id"),
            namespace: Arc::from(NAMESPACE),
            table: Arc::from(TABLE),
        }
    }

    fn operation_id() -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes([4; 16])
    }

    fn historical_binding() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse(INSTANCE).expect("instance id"),
            incarnation: historical_incarnation(),
        }
    }

    fn add_files_summary() -> ConnectorDataMutationPlanSummary {
        ConnectorDataMutationPlanSummary::try_new(3, 30, 300).expect("summary")
    }

    fn source_scope() -> ConnectorDataMutationSourceScope {
        ConnectorDataMutationSourceScope::try_new_directory([6; 32]).expect("source scope")
    }

    fn spi_fence(epoch: u64, attempt: u64) -> ConnectorExternalOperationFence {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive("nova-historical-test").expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(1, epoch, attempt).expect("generation"),
            ConnectorWriteOperationId::from_bytes(operation_id().to_bytes()),
            [6; 16],
            table_identity(),
            ConnectorWriteTargetRef::main(),
        )
        .expect("external operation fence")
    }

    fn established_historical(epoch: u64) -> ConnectorHistoricalDataMutationFence {
        let fence = spi_fence(epoch, 1);
        let receipt = ConnectorExternalFenceReceipt::try_new(
            &fence,
            Bytes::from_static(b"historical-marker"),
        )
        .expect("receipt");
        ConnectorHistoricalDataMutationFence::established(&receipt, fence)
            .expect("established fence")
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

    fn descriptor_for(
        family: ConnectorHistoricalDataMutationFamily,
        historical_fence: ConnectorHistoricalDataMutationFence,
        state: ConnectorHistoricalDataMutationDispatchState,
        evidence: Option<ExternalMutationEvidence>,
    ) -> ConnectorHistoricalDataMutationDescriptor {
        ConnectorHistoricalDataMutationDescriptor::try_new(
            ConnectorHistoricalDataMutationIdentity {
                historical_binding: historical_binding(),
                table: table_identity(),
                target_ref: ConnectorWriteTargetRef::main(),
                operation_id: operation_id(),
                family,
                request_digest: [1; 32],
                plan_digest: [2; 32],
                state_digest: [3; 32],
                plan_summary: match family {
                    ConnectorHistoricalDataMutationFamily::Truncate => {
                        ConnectorDataMutationPlanSummary::default()
                    }
                    ConnectorHistoricalDataMutationFamily::RegisterExistingFiles => {
                        add_files_summary()
                    }
                },
                source_scope: family.owns_source_scope().then(source_scope),
            },
            ConnectorHistoricalDataMutationFenceFacts {
                historical_fence,
                raised_fence: spi_fence(3, 1),
                raised_fence_receipt_digest: [5; 32],
            },
            checkpoints(state),
            evidence,
        )
        .expect("historical data mutation descriptor")
    }

    fn truncate_descriptor(
        historical_fence: ConnectorHistoricalDataMutationFence,
        state: ConnectorHistoricalDataMutationDispatchState,
    ) -> ConnectorHistoricalDataMutationDescriptor {
        descriptor_for(
            ConnectorHistoricalDataMutationFamily::Truncate,
            historical_fence,
            state,
            None,
        )
    }

    fn add_files_descriptor(
        historical_fence: ConnectorHistoricalDataMutationFence,
        state: ConnectorHistoricalDataMutationDispatchState,
    ) -> ConnectorHistoricalDataMutationDescriptor {
        descriptor_for(
            ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
            historical_fence,
            state,
            None,
        )
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
            &truncate_descriptor(
                established_historical(2),
                ConnectorHistoricalDataMutationDispatchState::Completed,
            ),
            &target,
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::Applied
        );
        assert!(
            outcome.cleanup_required,
            "an applied operation retires its fence ref"
        );
        assert!(!outcome.continuation_allowed);

        // An applied operation whose historical owner never published a fence
        // marker must not have the ref retired: doing so would reopen it.
        let outcome = classify(
            &held_fence(),
            &truncate_descriptor(
                ConnectorHistoricalDataMutationFence::NotEstablished,
                ConnectorHistoricalDataMutationDispatchState::Completed,
            ),
            &target,
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::Applied
        );
        assert!(!outcome.cleanup_required);
    }

    #[test]
    fn not_applied_requires_a_held_fence_a_complete_lineage_and_no_historical_fence() {
        let outcome = classify(
            &held_fence(),
            &truncate_descriptor(
                ConnectorHistoricalDataMutationFence::NotEstablished,
                ConnectorHistoricalDataMutationDispatchState::NotDispatched,
            ),
            &clean_target(),
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::NotApplied
        );
        assert!(
            !outcome.cleanup_required,
            "retiring here would let the old owner establish a fence at its old generation"
        );
        assert!(outcome.continuation_allowed);

        // A dispatched journal checkpoint still proves the mutation did not
        // apply, but a destructive statement is never replayed on that basis.
        let outcome = classify(
            &held_fence(),
            &truncate_descriptor(
                ConnectorHistoricalDataMutationFence::NotEstablished,
                ConnectorHistoricalDataMutationDispatchState::Dispatched,
            ),
            &clean_target(),
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::NotApplied
        );
        assert!(!outcome.continuation_allowed);
    }

    #[test]
    fn an_established_historical_fence_with_no_commit_leaves_artifacts_to_clean_up() {
        let outcome = classify(
            &held_fence(),
            &truncate_descriptor(
                established_historical(2),
                ConnectorHistoricalDataMutationDispatchState::Dispatched,
            ),
            &clean_target(),
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::CleanupRequired
        );
        assert!(outcome.cleanup_required);
        assert!(!outcome.continuation_allowed);
    }

    #[test]
    fn a_superseded_fence_is_a_conflict_and_never_a_cleanup() {
        let outcome = classify(
            &RaisedFenceObservation::Superseded,
            &truncate_descriptor(
                established_historical(2),
                ConnectorHistoricalDataMutationDispatchState::Completed,
            ),
            &clean_target(),
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::Conflict
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
            &truncate_descriptor(
                established_historical(2),
                ConnectorHistoricalDataMutationDispatchState::Completed,
            ),
            &target,
            ArtifactEvidence::NotCarried,
        );
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalDataMutationDisposition::Unsupported
        );
        assert!(!outcome.cleanup_required);
    }

    #[test]
    fn a_partially_visible_add_files_source_set_is_never_applied_or_not_applied() {
        for (reason, target) in [
            (
                "the commit registered strictly fewer files than the sealed plan",
                TargetRefObservation {
                    summary_shortfall: true,
                    ..clean_target()
                },
            ),
            (
                "the source set is spread across more than one commit",
                TargetRefObservation {
                    matched_snapshot_id: Some(5),
                    multiple_matches: true,
                    ..clean_target()
                },
            ),
        ] {
            let outcome = classify(
                &held_fence(),
                &add_files_descriptor(
                    established_historical(2),
                    ConnectorHistoricalDataMutationDispatchState::Completed,
                ),
                &target,
                ArtifactEvidence::NotCarried,
            );
            assert_eq!(
                outcome.disposition,
                ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
                "{reason}"
            );
            assert!(
                !outcome.cleanup_required,
                "{reason} proves nothing about artifact ownership"
            );
            assert!(!outcome.continuation_allowed);

            // The same observation for TRUNCATE has no source semantics to
            // report, so it stays unresolved.
            let outcome = classify(
                &held_fence(),
                &truncate_descriptor(
                    established_historical(2),
                    ConnectorHistoricalDataMutationDispatchState::Completed,
                ),
                &target,
                ArtifactEvidence::NotCarried,
            );
            assert_eq!(
                outcome.disposition,
                ConnectorHistoricalDataMutationDisposition::Ambiguous,
                "{reason}"
            );
        }
    }

    #[test]
    fn every_kind_of_missing_or_conflicting_evidence_is_ambiguous_never_not_applied() {
        let cases: Vec<(
            &str,
            RaisedFenceObservation,
            TargetRefObservation,
            ArtifactEvidence,
        )> = vec![
            (
                "the raised fence marker is missing",
                RaisedFenceObservation::Unproven {
                    detail: "no marker".to_string(),
                },
                clean_target(),
                ArtifactEvidence::NotCarried,
            ),
            (
                "the historical marker is absent from the fence lineage",
                RaisedFenceObservation::Held {
                    snapshot_id: 42,
                    has_predecessor: false,
                },
                clean_target(),
                ArtifactEvidence::NotCarried,
            ),
            (
                "the target lineage was truncated",
                held_fence(),
                TargetRefObservation {
                    lineage_complete: false,
                    ..clean_target()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "a marker carried different sealed facts",
                held_fence(),
                TargetRefObservation {
                    digest_mismatch: true,
                    ..clean_target()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "a marker used an unknown layout version",
                held_fence(),
                TargetRefObservation {
                    unknown_marker_layout: true,
                    ..clean_target()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "the provenance exists outside the target lineage",
                held_fence(),
                TargetRefObservation {
                    off_lineage_match: true,
                    ..clean_target()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "the named target ref does not exist",
                held_fence(),
                TargetRefObservation {
                    missing_target_ref: true,
                    ..TargetRefObservation::default()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "more than one snapshot claimed this operation",
                held_fence(),
                TargetRefObservation {
                    matched_snapshot_id: Some(1),
                    multiple_matches: true,
                    ..clean_target()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "a matched snapshot disagreed with the sealed digests",
                held_fence(),
                TargetRefObservation {
                    matched_snapshot_id: Some(1),
                    digest_mismatch: true,
                    ..clean_target()
                },
                ArtifactEvidence::NotCarried,
            ),
            (
                "the staged artifact evidence cannot be read",
                held_fence(),
                TargetRefObservation {
                    matched_snapshot_id: Some(1),
                    ..clean_target()
                },
                ArtifactEvidence::Unreadable,
            ),
            (
                "the staged artifact evidence names another operation",
                held_fence(),
                clean_target(),
                ArtifactEvidence::Disagrees,
            ),
        ];
        for (reason, fence, target, artifacts) in cases {
            for family in [
                ConnectorHistoricalDataMutationFamily::Truncate,
                ConnectorHistoricalDataMutationFamily::RegisterExistingFiles,
            ] {
                let outcome = classify(
                    &fence,
                    &descriptor_for(
                        family,
                        established_historical(2),
                        ConnectorHistoricalDataMutationDispatchState::NotDispatched,
                        None,
                    ),
                    &target,
                    artifacts,
                );
                // The ADD FILES partial rules deliberately answer two of these
                // cases with a stricter, still-non-destructive disposition.
                let expected = if family.owns_source_scope()
                    && matches!(reason, "more than one snapshot claimed this operation")
                {
                    ConnectorHistoricalDataMutationDisposition::PartiallyApplied
                } else {
                    ConnectorHistoricalDataMutationDisposition::Ambiguous
                };
                assert_eq!(outcome.disposition, expected, "{reason} ({family:?})");
                assert!(!outcome.cleanup_required, "{reason} must not clean up");
                assert!(
                    !outcome.continuation_allowed,
                    "{reason} must not authorize a rerun"
                );
                assert_ne!(
                    outcome.disposition,
                    ConnectorHistoricalDataMutationDisposition::NotApplied,
                    "{reason} must never be read as a negative proof"
                );
            }
        }
    }

    #[test]
    fn provenance_matching_binds_the_exact_historical_generation_and_plan() {
        let descriptor = add_files_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Completed,
        );
        let identity = identity_digest_hex_for(&descriptor);
        let summary = |mutate: fn(&mut serde_json::Value)| {
            let mut value = provenance_marker(&identity);
            mutate(&mut value);
            let mut additional_properties = HashMap::new();
            additional_properties.insert(MARKER_PROPERTY.to_string(), value.to_string());
            Summary {
                operation: Operation::Append,
                additional_properties,
            }
        };

        assert!(matches!(
            provenance_match(&summary(|_| {}), &descriptor, &identity),
            ProvenanceMatch::Matched
        ));
        assert!(
            matches!(
                provenance_match(
                    &summary(|value| value["version"] = serde_json::json!(9)),
                    &descriptor,
                    &identity
                ),
                ProvenanceMatch::UnknownLayout
            ),
            "an unknown marker layout must never be read as a non-match"
        );
        assert!(
            matches!(
                provenance_match(
                    &summary(|value| value["extra"] = serde_json::json!(1)),
                    &descriptor,
                    &identity
                ),
                ProvenanceMatch::UnknownLayout
            ),
            "a producer field this build cannot interpret must be ambiguous"
        );
        for field in [
            "identity_digest_hex",
            "incarnation_hex",
            "request_digest_hex",
            "plan_digest_hex",
            "state_digest_hex",
        ] {
            assert!(
                matches!(
                    provenance_match(
                        &summary_with(&identity, field, serde_json::json!(hex_encode([0xAA; 32]))),
                        &descriptor,
                        &identity
                    ),
                    ProvenanceMatch::DigestMismatch
                ),
                "{field} must be a mismatch, not a silent non-match"
            );
        }
        assert!(matches!(
            provenance_match(
                &summary_with(&identity, "operation_kind", serde_json::json!("truncate")),
                &descriptor,
                &identity
            ),
            ProvenanceMatch::DigestMismatch
        ));
        assert!(matches!(
            provenance_match(
                &summary_with(&identity, "target_ref", serde_json::json!("audit")),
                &descriptor,
                &identity
            ),
            ProvenanceMatch::DigestMismatch
        ));
        assert!(
            matches!(
                provenance_match(
                    &summary_with(&identity, "file_count", serde_json::json!(2)),
                    &descriptor,
                    &identity
                ),
                ProvenanceMatch::SummaryShortfall
            ),
            "a strict subset of the sealed plan is a partial source set"
        );
        assert!(
            matches!(
                provenance_match(
                    &summary_with(&identity, "file_count", serde_json::json!(9)),
                    &descriptor,
                    &identity
                ),
                ProvenanceMatch::DigestMismatch
            ),
            "registering more than the sealed plan is unexplainable"
        );
        assert!(
            matches!(
                provenance_match(
                    &summary_with(
                        &identity,
                        "operation_id_hex",
                        serde_json::json!(hex_encode([1u8; 16]))
                    ),
                    &descriptor,
                    &identity
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
                &descriptor,
                &identity
            ),
            ProvenanceMatch::None
        ));
    }

    fn identity_digest_hex_for(descriptor: &ConnectorHistoricalDataMutationDescriptor) -> String {
        expected_identity_digest_hex(&instance_descriptor().provider_id, descriptor)
    }

    fn summary_with(identity: &str, field: &str, value: serde_json::Value) -> Summary {
        let mut marker = provenance_marker(identity);
        marker[field] = value;
        let mut additional_properties = HashMap::new();
        additional_properties.insert(MARKER_PROPERTY.to_string(), marker.to_string());
        Summary {
            operation: Operation::Append,
            additional_properties,
        }
    }

    fn provenance_marker(identity_digest_hex: &str) -> serde_json::Value {
        serde_json::json!({
            "version": 1,
            "identity_digest_hex": identity_digest_hex,
            "incarnation_hex": hex_encode(historical_incarnation().to_bytes()),
            "operation_id_hex": hex_encode(operation_id().to_bytes()),
            "operation_kind": "register-existing-files",
            "request_digest_hex": hex_encode([1u8; 32]),
            "plan_digest_hex": hex_encode([2u8; 32]),
            "state_digest_hex": hex_encode([3u8; 32]),
            "target_ref": "main",
            "base_snapshot_id": serde_json::Value::Null,
            "file_count": 3,
            "row_count": 30,
            "total_bytes": 300,
        })
    }

    #[test]
    fn evidence_cross_check_refuses_an_unreadable_or_foreign_artifact_claim() {
        let base = truncate_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Dispatched,
        );
        let identity = identity_digest_hex_for(&base);
        let agreeing = IcebergDataMutationEvidenceV1 {
            version: ORDINARY_EVIDENCE_PAYLOAD_VERSION,
            namespace: NAMESPACE.to_string(),
            table: TABLE.to_string(),
            target_ref: "main".to_string(),
            operation_id_hex: hex_encode(operation_id().to_bytes()),
            operation_kind: "truncate".to_string(),
            request_digest_hex: hex_encode([1u8; 32]),
            plan_digest_hex: hex_encode([2u8; 32]),
            state_digest_hex: hex_encode([3u8; 32]),
            identity_digest_hex: identity.clone(),
            file_count: 0,
            row_count: 0,
            total_bytes: 0,
        };
        let evidence = |payload: Bytes| {
            ExternalMutationEvidence::try_new(
                ORDINARY_EVIDENCE_PAYLOAD_VERSION,
                instance_descriptor(),
                historical_incarnation(),
                operation_id(),
                "truncate",
                payload,
            )
            .expect("evidence")
        };

        let with_evidence = |payload: Bytes| {
            descriptor_for(
                ConnectorHistoricalDataMutationFamily::Truncate,
                established_historical(2),
                ConnectorHistoricalDataMutationDispatchState::Dispatched,
                Some(evidence(payload)),
            )
        };

        let good = with_evidence(encode(&agreeing, "evidence").expect("encode"));
        assert_eq!(inspect_evidence(&good, &identity), ArtifactEvidence::Agrees);

        let unreadable = with_evidence(Bytes::from_static(b"{\"nope\":1}"));
        assert_eq!(
            inspect_evidence(&unreadable, &identity),
            ArtifactEvidence::Unreadable
        );

        let mut foreign = agreeing;
        foreign.identity_digest_hex = hex_encode([7u8; 32]);
        let foreign = with_evidence(encode(&foreign, "evidence").expect("encode"));
        assert_eq!(
            inspect_evidence(&foreign, &identity),
            ArtifactEvidence::Disagrees
        );
    }

    #[test]
    fn retention_stays_bounded() {
        let mut retention = CleanupRetention::default();
        let record = CleanupRecord {
            outcome: known_uncommitted("retained"),
            proof: proof_fixture(),
            descriptor_digest: [1; 32],
            observation_digest: [1; 32],
        };
        for index in 0..(MAX_RETAINED_CLEANUP_OUTCOMES + 16) {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
            retention.insert(
                ConnectorMutationOperationId::from_bytes(bytes),
                record.clone(),
            );
        }
        assert_eq!(retention.records.len(), MAX_RETAINED_CLEANUP_OUTCOMES);
        assert_eq!(retention.order.len(), MAX_RETAINED_CLEANUP_OUTCOMES);

        let mut issued = IssuedObservations::default();
        for index in 0..(MAX_RETAINED_CLEANUP_OUTCOMES + 16) {
            let mut digest = [0; 32];
            digest[..8].copy_from_slice(&(index as u64).to_be_bytes());
            issued.record(digest);
        }
        assert_eq!(issued.digests.len(), MAX_RETAINED_CLEANUP_OUTCOMES);
    }

    fn proof_fixture() -> IcebergHistoricalDataMutationProofV1 {
        IcebergHistoricalDataMutationProofV1 {
            version: ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION,
            descriptor_digest: vec![1; 32],
            raised_fence_digest: vec![1; 32],
            namespace: NAMESPACE.to_string(),
            table: TABLE.to_string(),
            table_uuid: "uuid".to_string(),
            target_ref: "main".to_string(),
            operation_id_hex: hex_encode(operation_id().to_bytes()),
            operation_kind: "truncate".to_string(),
            identity_digest_hex: hex_encode([2u8; 32]),
            target_snapshot_id: None,
            fence_ref: "novarocks-write-fence-op".to_string(),
            fence_snapshot_id: 1,
            fence_generation: [1, 1, 1],
            applied_snapshot_id: None,
            lineage_complete: true,
            source_scope_digest_hex: None,
            disposition: "cleanup-required".to_string(),
        }
    }

    #[test]
    fn only_a_terminal_disposition_holding_a_pinned_marker_may_retire_a_fence_ref() {
        for retirable in [
            ConnectorHistoricalDataMutationDisposition::Applied,
            ConnectorHistoricalDataMutationDisposition::CleanupRequired,
        ] {
            assert!(fence_ref_is_retirable_for(retirable), "{retirable:?}");
        }
        for refused in [
            // This facet only reaches NotApplied when the historical owner
            // never established a fence: retiring would reopen its authority.
            ConnectorHistoricalDataMutationDisposition::NotApplied,
            ConnectorHistoricalDataMutationDisposition::PartiallyApplied,
            ConnectorHistoricalDataMutationDisposition::Conflict,
            ConnectorHistoricalDataMutationDisposition::Ambiguous,
            ConnectorHistoricalDataMutationDisposition::Unsupported,
        ] {
            assert!(!fence_ref_is_retirable_for(refused), "{refused:?}");
        }
    }

    // ----------------------------------------------------------------------
    // Catalog-backed coverage. These use a local filesystem warehouse so the
    // fence ref, its marker snapshot and the atomic conditional update are the
    // real ones rather than a simulation.
    // ----------------------------------------------------------------------

    fn control_runtime(
        warehouse: &std::path::Path,
        handle: tokio::runtime::Handle,
    ) -> IcebergControlRuntime {
        let configuration = crate::catalog_config::parse_catalog_configuration(
            INSTANCE,
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(handle.clone())),
            Arc::new(TokioFileTaskSpawner::new(handle.clone())),
        );
        let resources = IcebergControlResources::new(binding, handle);
        IcebergControlRuntime::try_new(IcebergCatalogControlState::new(configuration), resources)
            .expect("control runtime")
    }

    struct Fixture {
        executor: tokio::runtime::Runtime,
        _warehouse: tempfile::TempDir,
        runtime: Arc<IcebergControlRuntime>,
        recovery: IcebergHistoricalDataMutationRecovery,
    }

    fn fixture() -> Fixture {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let runtime = Arc::new(control_runtime(warehouse.path(), executor.handle().clone()));
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
        let recovery = IcebergHistoricalDataMutationRecovery::new(
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
            observed: ConnectorHistoricalDataMutationFence,
            raised: ConnectorExternalOperationFence,
        ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
            self.recovery
                .raise_external_fence(ConnectorHistoricalDataMutationFenceRaiseRequest {
                    historical_binding: historical_binding(),
                    family: ConnectorHistoricalDataMutationFamily::Truncate,
                    observed,
                    raised,
                    context: context(),
                })
        }

        /// Publish the marker the historical owner would have established, so
        /// the fence ref has the same shape a real takeover observes.
        fn establish_historical(&self, epoch: u64) {
            self.raise(
                ConnectorHistoricalDataMutationFence::NotEstablished,
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

        /// Commit one snapshot on `main` carrying the ordinary direct-mutation
        /// provenance marker. The manifest list is never opened by this facet,
        /// which reads snapshot summaries only.
        fn commit_provenance_snapshot(&self, marker: serde_json::Value) -> i64 {
            let loaded = self.reload();
            let metadata = loaded.table.metadata();
            let parent = metadata.current_snapshot_id();
            let snapshot_id = 987_654_321;
            let mut additional_properties = HashMap::new();
            additional_properties.insert(MARKER_PROPERTY.to_string(), marker.to_string());
            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(metadata.last_sequence_number() + 1)
                .with_timestamp_ms(metadata.last_updated_ms() + 1)
                .with_manifest_list(format!(
                    "{}/metadata/historical-data-mutation-test-{snapshot_id}.avro",
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
        assert_ne!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn inspect_proves_not_applied_and_issues_a_bound_continuation() {
        let fixture = fixture();
        fixture
            .raise(
                ConnectorHistoricalDataMutationFence::NotEstablished,
                spi_fence(3, 1),
            )
            .expect("raise");
        let descriptor = truncate_descriptor(
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::NotDispatched,
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
            ConnectorHistoricalDataMutationDisposition::NotApplied
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
        let descriptor = add_files_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Completed,
        );
        let identity = identity_digest_hex_for(&descriptor);
        let snapshot_id = fixture.commit_provenance_snapshot(provenance_marker(&identity));
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        observation.validate_for(&descriptor).expect("sealed");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalDataMutationDisposition::Applied
        );
        let application = observation.application.expect("finalization facts");
        assert_eq!(
            application.committed_version.snapshot_id(),
            Some(snapshot_id)
        );
        assert_eq!(
            application.receipt.operation_kind(),
            "register-existing-files"
        );
        assert_eq!(application.receipt.summary(), add_files_summary());
        assert_eq!(
            application.finalization,
            ExternalMutationFinalization::Complete
        );
        assert!(observation.continuation.is_none());
        assert!(observation.cleanup_required);
        assert_eq!(observation.source_scope, Some(source_scope()));
    }

    #[test]
    fn inspect_reports_a_partial_add_files_source_set_from_a_short_marker() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let descriptor = add_files_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Completed,
        );
        let identity = identity_digest_hex_for(&descriptor);
        let mut marker = provenance_marker(&identity);
        marker["file_count"] = serde_json::json!(1);
        marker["row_count"] = serde_json::json!(10);
        marker["total_bytes"] = serde_json::json!(100);
        fixture.commit_provenance_snapshot(marker);
        let observation = fixture
            .recovery
            .inspect(descriptor, context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalDataMutationDisposition::PartiallyApplied
        );
        assert!(!observation.cleanup_required);
        assert!(
            !observation.disposition.permits_source_scope_release(),
            "a partial source set keeps its scope"
        );
    }

    #[test]
    fn inspect_reports_ambiguous_when_a_marker_carries_other_digests() {
        let fixture = fixture();
        fixture.establish_historical(2);
        fixture
            .raise(established_historical(2), spi_fence(3, 1))
            .expect("raise");
        let descriptor = add_files_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Completed,
        );
        let identity = identity_digest_hex_for(&descriptor);
        let mut marker = provenance_marker(&identity);
        marker["identity_digest_hex"] = serde_json::json!(hex_encode([1u8; 32]));
        fixture.commit_provenance_snapshot(marker);
        let observation = fixture
            .recovery
            .inspect(descriptor, context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalDataMutationDisposition::Ambiguous
        );
        assert!(!observation.cleanup_required);
    }

    #[test]
    fn inspect_is_ambiguous_before_the_fence_is_raised_and_when_the_historical_marker_is_gone() {
        let fixture = fixture();
        let descriptor = truncate_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::NotDispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalDataMutationDisposition::Ambiguous,
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
            ConnectorHistoricalDataMutationDisposition::Ambiguous,
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
        let descriptor = truncate_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Dispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalDataMutationDisposition::CleanupRequired
        );
        assert!(observation.cleanup_required);

        let outcome = fixture
            .recovery
            .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
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
                .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
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
    fn cleanup_refuses_an_observation_that_never_asked_for_one() {
        let fixture = fixture();
        fixture
            .raise(
                ConnectorHistoricalDataMutationFence::NotEstablished,
                spi_fence(3, 1),
            )
            .expect("raise");
        let descriptor = truncate_descriptor(
            ConnectorHistoricalDataMutationFence::NotEstablished,
            ConnectorHistoricalDataMutationDispatchState::NotDispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalDataMutationDisposition::NotApplied
        );
        assert!(!observation.cleanup_required);

        let error = fixture
            .recovery
            .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: descriptor.digest(),
                observation,
                context: context(),
            })
            .expect_err("cleanup must refuse an observation that requested none");
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
        let descriptor = truncate_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Dispatched,
        );

        // A caller can mint a structurally perfect observation: the descriptor
        // seal holds, the proof decodes, and it names the real fence marker.
        // Only "this generation issued it" separates it from a real one, and a
        // cleanup request carries no descriptor for the SPI to re-seal against.
        let forged_proof = IcebergHistoricalDataMutationProofV1 {
            version: ICEBERG_HISTORICAL_DATA_MUTATION_PAYLOAD_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            raised_fence_digest: descriptor.raised_fence.digest().to_vec(),
            namespace: NAMESPACE.to_string(),
            table: TABLE.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            target_ref: "main".to_string(),
            operation_id_hex: hex_encode(operation_id().to_bytes()),
            operation_kind: "truncate".to_string(),
            identity_digest_hex: identity_digest_hex_for(&descriptor),
            target_snapshot_id: None,
            fence_ref: fixture.fence_ref(),
            fence_snapshot_id: held.snapshot_id,
            fence_generation: [1, 3, 1],
            applied_snapshot_id: None,
            lineage_complete: true,
            source_scope_digest_hex: None,
            disposition: "cleanup-required".to_string(),
        };
        let forged = ConnectorHistoricalDataMutationObservation::try_new(
            &descriptor,
            ConnectorHistoricalDataMutationDisposition::CleanupRequired,
            ConnectorHistoricalDataMutationOutcomeFacts {
                application: None,
                continuation: None,
                cleanup_required: true,
            },
            ConnectorHistoricalDataMutationProof::try_new(
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
            .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
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
        let descriptor = truncate_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Dispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor, context())
            .expect("inspect");
        assert!(observation.cleanup_required);

        let error = fixture
            .recovery
            .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
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
                ICEBERG_HISTORICAL_DATA_MUTATION_EVIDENCE_VERSION,
                instance_descriptor(),
                incarnation,
                operation_id(),
                ICEBERG_HISTORICAL_DATA_MUTATION_CLEANUP_KIND,
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
        let descriptor = truncate_descriptor(
            established_historical(2),
            ConnectorHistoricalDataMutationDispatchState::Dispatched,
        );
        let observation = fixture
            .recovery
            .inspect(descriptor.clone(), context())
            .expect("inspect");
        fixture
            .recovery
            .cleanup(ConnectorHistoricalDataMutationCleanupRequest {
                operation_id: operation_id(),
                descriptor_digest: descriptor.digest(),
                observation,
                context: context(),
            })
            .expect("cleanup");
        let evidence = ExternalMutationEvidence::try_new(
            ICEBERG_HISTORICAL_DATA_MUTATION_EVIDENCE_VERSION,
            instance_descriptor(),
            current_incarnation(),
            operation_id(),
            ICEBERG_HISTORICAL_DATA_MUTATION_CLEANUP_KIND,
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
