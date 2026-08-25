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

//! CP-3B contract tests for external write fencing and historical write
//! recovery, driven by a fake Connector provider.
//!
//! There is deliberately no Iceberg, catalog, or object-store dependency here.
//! The fake provider owns a tiny model of "durable external truth" plus a
//! provider-private proof artifact, and every classification it returns is
//! computed from that model. That keeps the safety properties under test real
//! rather than tabulated: a truncated or digest-mismatched artifact is detected
//! by re-hashing it, not by a flag that says "pretend this is corrupt".
//!
//! The invariants asserted here are:
//!
//! 1. every disposition (`Applied`, `NotApplied`, `NotDispatched`, `Staged`,
//!    `Conflict`, `Ambiguous`, `Unsupported`) is reachable for every row-DML
//!    statement family, and reaches the frontend as a typed neutral result;
//! 2. corrupt or missing evidence classifies `Ambiguous`, never `NotApplied`;
//! 3. historical recovery makes zero ordinary `commit`/`abort`/`reconcile`
//!    calls, even for an operation that may already have been dispatched;
//! 4. a continuation is issued only for a proven `NotDispatched` operation
//!    whose historical authority was already fenced out;
//! 5. a historical response that arrives after the lease moved on cannot change
//!    durable state;
//! 6. a cleanup requirement and its finalization record survive a terminal
//!    user-visible result;
//! 7. the external fence is established before any writer or commit dispatch.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::conformance::{
    ConnectorExternalFenceConformanceInput, assert_external_write_fence_contract,
    assert_historical_write_recovery_contract, assert_typed_fence_conflict,
};
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorCancellation, ConnectorClusterIdentity,
    ConnectorCommittedVersion, ConnectorControlBinding, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorExecutionDistribution,
    ConnectorExternalFenceFailure, ConnectorExternalFenceGeneration, ConnectorExternalFenceReceipt,
    ConnectorExternalFenceRequest, ConnectorExternalOperationFence,
    ConnectorHistoricalWriteApplication, ConnectorHistoricalWriteCheckpoint,
    ConnectorHistoricalWriteCleanupReceipt, ConnectorHistoricalWriteCleanupRequest,
    ConnectorHistoricalWriteContinuation, ConnectorHistoricalWriteDescriptor,
    ConnectorHistoricalWriteDispatchState, ConnectorHistoricalWriteDisposition,
    ConnectorHistoricalWriteFence, ConnectorHistoricalWriteFenceFacts,
    ConnectorHistoricalWriteFenceRaiseRequest, ConnectorHistoricalWriteIdentity,
    ConnectorHistoricalWriteObservation, ConnectorHistoricalWriteOutcomeFacts,
    ConnectorHistoricalWritePhase, ConnectorHistoricalWriteProof, ConnectorHistoricalWriteRecovery,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorNamespaceRequest,
    ConnectorProviderId, ConnectorRequestContext, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest,
    ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest, ConnectorWriteCommitRequest,
    ConnectorWriteControl, ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationId,
    ConnectorWritePlan, ConnectorWritePlanningRequest, ConnectorWriteReceipt,
    ConnectorWriteReconcileRequest, ConnectorWriteTargetRef, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Fixed identities
// ---------------------------------------------------------------------------

const CLUSTER_ID: &str = "nova-cp3b-cluster";
const INSTANCE_ID: &str = "catalog.lake";
const PROVIDER_ID: &str = "fake";
const NAMESPACE: &str = "db";
const TABLE: &str = "orders";

/// The incarnation of the control generation that crashed mid-operation.
const HISTORICAL_INCARNATION: [u8; 16] = [4; 16];
/// The incarnation of the control generation performing recovery.
const CURRENT_INCARNATION: [u8; 16] = [9; 16];

/// A marker embedded in every provider-private payload. The frontend-visible
/// projection must never contain it: if it does, the frontend decoded a payload
/// it is only allowed to store and hand back.
const PROVIDER_PRIVATE_MARKER: &str = "PROVIDER-PRIVATE-PROOF-BODY";

fn instance_id() -> ConnectorInstanceId {
    ConnectorInstanceId::parse(INSTANCE_ID).expect("instance id")
}

fn instance_descriptor() -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse(PROVIDER_ID).expect("provider id"),
        instance_id: instance_id(),
    }
}

fn binding_key(incarnation: [u8; 16]) -> ConnectorExecutionBindingKey {
    ConnectorExecutionBindingKey {
        instance_id: instance_id(),
        incarnation: ConnectorInstanceIncarnation::from_bytes(incarnation),
    }
}

fn table_identity() -> ConnectorTableIdentity {
    ConnectorTableIdentity {
        instance_id: instance_id(),
        namespace: Arc::from(NAMESPACE),
        table: Arc::from(TABLE),
    }
}

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
        1024,
        8192,
    )
    .expect("request context")
}

// ---------------------------------------------------------------------------
// Fence construction
// ---------------------------------------------------------------------------

/// The three-component fence generation plus the attempt identity that a
/// coordination attempt projects from its CP-3A fencing token.
#[derive(Clone, Copy)]
struct FenceSpec {
    operation_id: ConnectorWriteOperationId,
    control_plane_incarnation: u64,
    resource_epoch: u64,
    coordination_attempt: u64,
    coordination_attempt_id: [u8; 16],
}

fn fence(spec: FenceSpec) -> ConnectorExternalOperationFence {
    ConnectorExternalOperationFence::try_new(
        ConnectorClusterIdentity::derive(CLUSTER_ID).expect("cluster identity"),
        ConnectorExternalFenceGeneration::try_new(
            spec.control_plane_incarnation,
            spec.resource_epoch,
            spec.coordination_attempt,
        )
        .expect("fence generation"),
        spec.operation_id,
        spec.coordination_attempt_id,
        table_identity(),
        ConnectorWriteTargetRef::main(),
    )
    .expect("external operation fence")
}

/// The fence the crashed owner had established, at resource epoch 2.
fn historical_fence(operation_id: ConnectorWriteOperationId) -> ConnectorExternalOperationFence {
    fence(FenceSpec {
        operation_id,
        control_plane_incarnation: 1,
        resource_epoch: 2,
        coordination_attempt: 1,
        coordination_attempt_id: [2; 16],
    })
}

/// The strictly higher fence the recovering owner raises, at resource epoch 3.
fn raised_fence(operation_id: ConnectorWriteOperationId) -> ConnectorExternalOperationFence {
    fence(FenceSpec {
        operation_id,
        control_plane_incarnation: 1,
        resource_epoch: 3,
        coordination_attempt: 1,
        coordination_attempt_id: [3; 16],
    })
}

fn established_historical_fence(
    operation_id: ConnectorWriteOperationId,
) -> ConnectorHistoricalWriteFence {
    let observed = historical_fence(operation_id);
    let receipt = ConnectorExternalFenceReceipt::try_new(&observed, marker_payload(&observed))
        .expect("historical fence receipt");
    ConnectorHistoricalWriteFence::established(&receipt, observed)
        .expect("established historical fence")
}

/// A deterministic provider-private fence marker body. Determinism is what
/// makes replaying one fence generation idempotent down to the receipt bytes.
fn marker_payload(fence: &ConnectorExternalOperationFence) -> Bytes {
    Bytes::from(format!(
        "{PROVIDER_PRIVATE_MARKER}|fence-marker|{}",
        hex::encode(fence.digest())
    ))
}

// ---------------------------------------------------------------------------
// The fake provider's model of durable external truth
// ---------------------------------------------------------------------------

/// What durable external truth says about one historical write operation.
///
/// This is the only input the fake provider may consult to classify an
/// operation; it stands in for a fence-branch marker plus target-ref commit
/// provenance without any table-format detail crossing the test boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExternalTruth {
    /// Commit provenance for exactly this operation is present.
    OperationCommitted,
    /// External truth proves this operation never committed, and the raised
    /// fence proves no historical authority can still commit it.
    ProvenUncommitted,
    /// No writer and no commit were ever dispatched under this operation.
    NothingDispatched,
    /// Writer output exists but carries no commit provenance.
    StagedOutputOnly,
    /// Another operation advanced the external base past this one.
    SupersededByAnotherOperation,
    /// This provider cannot classify a historical write operation at all.
    Unclassifiable,
}

impl ExternalTruth {
    const ALL: [Self; 6] = [
        Self::OperationCommitted,
        Self::ProvenUncommitted,
        Self::NothingDispatched,
        Self::StagedOutputOnly,
        Self::SupersededByAnotherOperation,
        Self::Unclassifiable,
    ];
}

/// How the provider-private proof artifact is damaged, if at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactDamage {
    None,
    /// Fewer bytes are readable than the sealed length promised.
    Truncated,
    /// The bytes are complete but no longer hash to the sealed digest.
    DigestMismatch,
    /// The artifact has been garbage collected.
    Absent,
}

impl ArtifactDamage {
    const CORRUPTIONS: [Self; 3] = [Self::Truncated, Self::DigestMismatch, Self::Absent];
}

/// Why the provider could not read its own proof artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactFault {
    Truncated,
    DigestMismatch,
    Absent,
}

impl ArtifactFault {
    fn as_str(self) -> &'static str {
        match self {
            Self::Truncated => "truncated",
            Self::DigestMismatch => "digest-mismatch",
            Self::Absent => "absent",
        }
    }
}

/// A digest-sealed provider-private artifact. `declared_len` and
/// `declared_digest` are what the provider recorded when it wrote the artifact;
/// `body` is what it can read back now.
#[derive(Clone)]
struct ProofArtifact {
    body: Bytes,
    declared_len: usize,
    declared_digest: [u8; 32],
}

impl ProofArtifact {
    fn intact(tag: &str) -> Self {
        let body = Bytes::from(format!("{PROVIDER_PRIVATE_MARKER}|artifact|{tag}"));
        Self {
            declared_len: body.len(),
            declared_digest: Sha256::digest(&body).into(),
            body,
        }
    }

    fn damaged(tag: &str, damage: ArtifactDamage) -> Option<Self> {
        let intact = Self::intact(tag);
        match damage {
            ArtifactDamage::None => Some(intact),
            ArtifactDamage::Truncated => Some(Self {
                body: intact.body.slice(..intact.body.len() / 2),
                ..intact
            }),
            ArtifactDamage::DigestMismatch => Some(Self {
                body: Bytes::from(format!(
                    "{PROVIDER_PRIVATE_MARKER}|artifact|rewritten-{tag}"
                )),
                ..intact
            }),
            ArtifactDamage::Absent => None,
        }
    }

    /// Re-derive integrity instead of trusting a label. Only an artifact that
    /// still has its full sealed length and still hashes to its sealed digest
    /// may be used as proof.
    fn read(&self) -> Result<&Bytes, ArtifactFault> {
        if self.body.len() < self.declared_len {
            return Err(ArtifactFault::Truncated);
        }
        let actual: [u8; 32] = Sha256::digest(&self.body).into();
        if actual != self.declared_digest {
            return Err(ArtifactFault::DigestMismatch);
        }
        Ok(&self.body)
    }
}

/// What the fake provider should do when asked to clean up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupBehavior {
    /// Cleanup completed.
    Complete,
    /// Cleanup completed but its finalization failed and must be retained.
    FinalizationFailed,
    /// Cleanup did not happen; nothing was removed.
    Refused,
    /// The cleanup result was lost and only opaque evidence remains.
    Lost,
}

/// Everything the provider knows about one historical operation.
#[derive(Clone)]
struct OperationTruth {
    truth: ExternalTruth,
    artifact: Option<ProofArtifact>,
    cleanup: CleanupBehavior,
}

fn artifact_id(operation_id: ConnectorWriteOperationId) -> String {
    format!("artifact/{}", hex::encode(operation_id.to_bytes()))
}

// ---------------------------------------------------------------------------
// The fake provider
// ---------------------------------------------------------------------------

/// One provider event, in the order the provider observed it. The ordering
/// assertions in this file read this log rather than a bare counter so a
/// "fence before dispatch" violation is visible as an ordering, not a total.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ProviderEvent {
    OrdinaryFenceEstablished([u8; 32]),
    OrdinaryPlanWrite,
    OrdinaryCommit,
    OrdinaryAbort,
    OrdinaryReconcile,
    WriterDispatched,
    HistoricalFenceRaised([u8; 32]),
    HistoricalInspect([u8; 32]),
    HistoricalCleanup([u8; 32]),
    HistoricalReconcileCleanup,
}

#[derive(Default)]
struct FakeState {
    events: Vec<ProviderEvent>,
    /// Highest external fence this provider established, per operation.
    established: BTreeMap<[u8; 16], ConnectorExternalOperationFence>,
    /// Durable external truth, per operation.
    truth: BTreeMap<[u8; 16], OperationTruth>,
    /// Observations this provider actually issued, by observation digest. A
    /// cleanup request naming anything else is not proof bound.
    issued: BTreeMap<[u8; 32], ConnectorWriteOperationId>,
    /// Provider-private artifacts that still exist.
    artifacts: BTreeSet<String>,
    /// Artifacts removed by a guarded cleanup, in order.
    removed: Vec<String>,
}

/// A fake Connector provider that owns both the ordinary write capability and
/// the historical write recovery facet.
///
/// Owning both on one object is deliberate: it means the historical facet
/// *could* call an ordinary old-owner method, so asserting that the ordinary
/// call counters stay at zero during recovery is a real observation rather than
/// a consequence of the ordinary methods being out of reach.
struct FakeProvider {
    binding_key: ConnectorExecutionBindingKey,
    instance_id: ConnectorInstanceId,
    descriptor: ConnectorInstanceDescriptor,
    state: Mutex<FakeState>,
    ordinary_plan_calls: AtomicUsize,
    ordinary_commit_calls: AtomicUsize,
    ordinary_abort_calls: AtomicUsize,
    ordinary_reconcile_calls: AtomicUsize,
}

impl FakeProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            binding_key: binding_key(CURRENT_INCARNATION),
            instance_id: instance_id(),
            descriptor: instance_descriptor(),
            state: Mutex::new(FakeState::default()),
            ordinary_plan_calls: AtomicUsize::new(0),
            ordinary_commit_calls: AtomicUsize::new(0),
            ordinary_abort_calls: AtomicUsize::new(0),
            ordinary_reconcile_calls: AtomicUsize::new(0),
        })
    }

    fn lock(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().expect("fake provider state")
    }

    /// Seed durable external truth for one operation.
    fn seed(
        &self,
        operation_id: ConnectorWriteOperationId,
        truth: ExternalTruth,
        damage: ArtifactDamage,
        cleanup: CleanupBehavior,
    ) {
        let id = artifact_id(operation_id);
        let artifact = ProofArtifact::damaged(&id, damage);
        let mut state = self.lock();
        if artifact.is_some() {
            state.artifacts.insert(id);
        }
        state.truth.insert(
            operation_id.to_bytes(),
            OperationTruth {
                truth,
                artifact,
                cleanup,
            },
        );
    }

    /// Add an artifact belonging to some other operation, so a guarded cleanup
    /// can be shown not to touch it.
    fn seed_unrelated_artifact(&self, name: &str) {
        self.lock().artifacts.insert(name.to_string());
    }

    fn events(&self) -> Vec<ProviderEvent> {
        self.lock().events.clone()
    }

    fn artifacts(&self) -> BTreeSet<String> {
        self.lock().artifacts.clone()
    }

    fn removed(&self) -> Vec<String> {
        self.lock().removed.clone()
    }

    fn ordinary_calls(&self) -> OrdinaryCallCounts {
        OrdinaryCallCounts {
            plan: self.ordinary_plan_calls.load(AtomicOrdering::SeqCst),
            commit: self.ordinary_commit_calls.load(AtomicOrdering::SeqCst),
            abort: self.ordinary_abort_calls.load(AtomicOrdering::SeqCst),
            reconcile: self.ordinary_reconcile_calls.load(AtomicOrdering::SeqCst),
        }
    }

    /// Record that a writer was dispatched. The test only reaches this through
    /// `guarded_writer_dispatch`, which requires an established fence first.
    fn record_writer_dispatch(&self) {
        self.lock().events.push(ProviderEvent::WriterDispatched);
    }

    /// Establish or raise the external fence of one operation, comparing
    /// generations at the same linearization point a later commit would use.
    fn establish_fence_locked(
        state: &mut FakeState,
        fence: &ConnectorExternalOperationFence,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        fence.validate()?;
        if let Some(existing) = state.established.get(&fence.operation_id().to_bytes()) {
            fence.validate_monotonic_successor_of(existing)?;
        }
        state
            .established
            .insert(fence.operation_id().to_bytes(), fence.clone());
        ConnectorExternalFenceReceipt::try_new(fence, marker_payload(fence))
    }

    /// The typed classification of one historical operation.
    ///
    /// The artifact gate comes first and is unconditional: without a readable,
    /// digest-matching proof the provider cannot conclude anything, so it must
    /// answer `Ambiguous`. Nothing below may translate a missing artifact into
    /// "not committed".
    fn classify(
        state: &FakeState,
        descriptor: &ConnectorHistoricalWriteDescriptor,
    ) -> Result<(ConnectorHistoricalWriteDisposition, ProofBody), ConnectorError> {
        let Some(known) = state.truth.get(&descriptor.operation_id.to_bytes()) else {
            return Ok((
                ConnectorHistoricalWriteDisposition::Ambiguous,
                ProofBody::unresolved("unknown-operation"),
            ));
        };
        let readable = match &known.artifact {
            None => Err(ArtifactFault::Absent),
            Some(artifact) => artifact.read(),
        };
        let body = match readable {
            Err(fault) => {
                return Ok((
                    ConnectorHistoricalWriteDisposition::Ambiguous,
                    ProofBody::unresolved(fault.as_str()),
                ));
            }
            Ok(body) => body.clone(),
        };
        let disposition = match known.truth {
            ExternalTruth::OperationCommitted => ConnectorHistoricalWriteDisposition::Applied,
            ExternalTruth::ProvenUncommitted => ConnectorHistoricalWriteDisposition::NotApplied,
            ExternalTruth::StagedOutputOnly => ConnectorHistoricalWriteDisposition::Staged,
            ExternalTruth::SupersededByAnotherOperation => {
                ConnectorHistoricalWriteDisposition::Conflict
            }
            ExternalTruth::Unclassifiable => ConnectorHistoricalWriteDisposition::Unsupported,
            // A provider may only claim nothing was dispatched when the
            // frontend journal agrees. An unknown checkpoint keeps the record
            // unresolved instead of unlocking a continuation.
            ExternalTruth::NothingDispatched => {
                if descriptor.journal_proves_nothing_dispatched() {
                    ConnectorHistoricalWriteDisposition::NotDispatched
                } else {
                    ConnectorHistoricalWriteDisposition::Ambiguous
                }
            }
        };
        Ok((disposition, ProofBody::resolved(&body, disposition)))
    }
}

/// The opaque proof body a provider hands back with one classification.
struct ProofBody(Bytes);

impl ProofBody {
    fn unresolved(reason: &str) -> Self {
        Self(Bytes::from(format!(
            "{PROVIDER_PRIVATE_MARKER}|unresolved|{reason}"
        )))
    }

    fn resolved(body: &Bytes, disposition: ConnectorHistoricalWriteDisposition) -> Self {
        let mut payload = Vec::from(body.as_ref());
        payload.extend_from_slice(format!("|disposition={disposition:?}").as_bytes());
        Self(Bytes::from(payload))
    }

    fn into_proof(self) -> Result<ConnectorHistoricalWriteProof, ConnectorError> {
        ConnectorHistoricalWriteProof::try_new(self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct OrdinaryCallCounts {
    plan: usize,
    commit: usize,
    abort: usize,
    reconcile: usize,
}

// ---------------------------------------------------------------------------
// Ordinary control-plane capabilities (only needed to build a real binding)
// ---------------------------------------------------------------------------

impl ConnectorMetadata for FakeProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(unsupported(
            "fake provider does not answer namespace existence",
        ))
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported("fake provider does not answer table existence"))
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported("fake provider does not list tables"))
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        Err(unsupported("fake provider does not load tables"))
    }
}

impl ConnectorScanPlanning for FakeProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unsupported("fake provider does not begin scans"))
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported("fake provider does not plan splits"))
    }
}

impl ConnectorExecutionDistribution for FakeProvider {
    fn declaration(
        &self,
        _context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        ConnectorExecutionDeclaration::iceberg(
            self.descriptor.instance_id.as_str(),
            self.binding_key.incarnation.to_bytes(),
            "fake",
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Ordinary write capability: every terminal method is counted
// ---------------------------------------------------------------------------

impl ConnectorWriteControl for FakeProvider {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn establish_external_fence(
        &self,
        request: ConnectorExternalFenceRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        request.validate(&self.binding_key)?;
        let mut state = self.lock();
        let receipt = Self::establish_fence_locked(&mut state, &request.fence)?;
        state
            .events
            .push(ProviderEvent::OrdinaryFenceEstablished(receipt.digest()));
        Ok(receipt)
    }

    fn plan_write(
        &self,
        _request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        self.ordinary_plan_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        self.lock().events.push(ProviderEvent::OrdinaryPlanWrite);
        Err(unsupported("fake provider does not plan ordinary writes"))
    }

    fn commit(
        &self,
        _request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.ordinary_commit_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        self.lock().events.push(ProviderEvent::OrdinaryCommit);
        Err(unsupported("fake provider does not commit ordinary writes"))
    }

    fn abort(
        &self,
        _request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.ordinary_abort_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        self.lock().events.push(ProviderEvent::OrdinaryAbort);
        Err(unsupported("fake provider does not abort ordinary writes"))
    }

    fn reconcile(
        &self,
        _request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.ordinary_reconcile_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        self.lock().events.push(ProviderEvent::OrdinaryReconcile);
        Err(unsupported(
            "fake provider does not reconcile ordinary writes",
        ))
    }
}

// ---------------------------------------------------------------------------
// Historical write recovery facet
// ---------------------------------------------------------------------------

impl ConnectorHistoricalWriteRecovery for FakeProvider {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn raise_external_fence(
        &self,
        request: ConnectorHistoricalWriteFenceRaiseRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        request.validate()?;
        let mut state = self.lock();
        let receipt = Self::establish_fence_locked(&mut state, &request.raised)?;
        state
            .events
            .push(ProviderEvent::HistoricalFenceRaised(receipt.digest()));
        Ok(receipt)
    }

    fn inspect(
        &self,
        descriptor: ConnectorHistoricalWriteDescriptor,
        _context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalWriteObservation, ConnectorError> {
        descriptor.validate()?;
        let mut state = self.lock();
        state
            .events
            .push(ProviderEvent::HistoricalInspect(descriptor.digest()));
        // Takeover order (spec D2): a historical operation may only be
        // classified once this provider has itself established the exact raised
        // fence named by the descriptor.
        match state.established.get(&descriptor.operation_id.to_bytes()) {
            None => {
                return Err(ConnectorError::external_fence(
                    ConnectorExternalFenceFailure::NotEstablished,
                    "historical write inspection requires this provider to have raised the external fence first",
                ));
            }
            Some(current) => match current.compare_generation(&descriptor.raised_fence)? {
                std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Less => {
                    return Err(ConnectorError::external_fence(
                        ConnectorExternalFenceFailure::NotEstablished,
                        "historical write inspection names a raised fence this provider never established",
                    ));
                }
                std::cmp::Ordering::Greater => {
                    return Err(ConnectorError::external_fence(
                        ConnectorExternalFenceFailure::Stale,
                        "historical write inspection is behind the established external fence",
                    ));
                }
            },
        }

        let (disposition, body) = Self::classify(&state, &descriptor)?;
        let application = match disposition {
            ConnectorHistoricalWriteDisposition::Applied => {
                Some(ConnectorHistoricalWriteApplication {
                    committed_version: ConnectorCommittedVersion::try_new(
                        Bytes::from_static(b"committed-version"),
                        Some(42),
                    )?,
                    receipt: ConnectorWriteReceipt::try_new(Bytes::from_static(
                        b"historical-write-receipt",
                    ))?,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            _ => None,
        };
        let continuation = match disposition {
            ConnectorHistoricalWriteDisposition::NotDispatched => {
                Some(ConnectorHistoricalWriteContinuation::try_new(
                    &descriptor.raised_fence,
                    continuation_payload(&descriptor),
                )?)
            }
            _ => None,
        };
        let cleanup_required = matches!(
            disposition,
            ConnectorHistoricalWriteDisposition::Applied
                | ConnectorHistoricalWriteDisposition::NotApplied
                | ConnectorHistoricalWriteDisposition::Staged
                | ConnectorHistoricalWriteDisposition::Conflict
        );
        let observation = ConnectorHistoricalWriteObservation::try_new(
            &descriptor,
            disposition,
            ConnectorHistoricalWriteOutcomeFacts {
                application,
                continuation,
                cleanup_required,
            },
            body.into_proof()?,
        )?;
        state
            .issued
            .insert(observation.digest(), descriptor.operation_id);
        Ok(observation)
    }

    fn cleanup(
        &self,
        request: ConnectorHistoricalWriteCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
    {
        let mut state = self.lock();
        state.events.push(ProviderEvent::HistoricalCleanup(
            request.observation.digest(),
        ));
        if request.descriptor_digest != request.observation.descriptor_digest
            || request.operation_id != request.observation.operation_id
        {
            return Err(invalid(
                "historical write cleanup request does not name its own observation",
            ));
        }
        // Proof bound: only an observation this provider issued may authorize
        // the removal of an artifact.
        if state.issued.get(&request.observation.digest()) != Some(&request.operation_id) {
            return Err(invalid(
                "historical write cleanup names an observation this provider never issued",
            ));
        }
        if !request.observation.cleanup_required {
            return Err(invalid(
                "historical write cleanup was requested for an observation that requires none",
            ));
        }
        let behavior = state
            .truth
            .get(&request.operation_id.to_bytes())
            .map(|known| known.cleanup)
            .ok_or_else(|| invalid("historical write cleanup names an unknown operation"))?;
        let id = artifact_id(request.operation_id);
        let receipt = ConnectorHistoricalWriteCleanupReceipt {
            descriptor_digest: request.descriptor_digest,
            observation_digest: request.observation.digest(),
        };
        match behavior {
            CleanupBehavior::Complete | CleanupBehavior::FinalizationFailed => {
                let effect = if state.artifacts.remove(&id) {
                    state.removed.push(id);
                    ExternalMutationEffect::Applied
                } else {
                    ExternalMutationEffect::NoOp
                };
                let finalization = if behavior == CleanupBehavior::Complete {
                    ExternalMutationFinalization::Complete
                } else {
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "historical write cleanup finalization did not complete",
                    ))
                };
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect,
                    receipt,
                    finalization,
                })
            }
            CleanupBehavior::Refused => Ok(ExternalMutationOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "historical write cleanup could not run",
                ),
            }),
            CleanupBehavior::Lost => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "historical write cleanup result was lost",
                ),
                evidence: cleanup_evidence(
                    &self.descriptor,
                    self.binding_key.incarnation,
                    request.operation_id,
                )?,
            }),
        }
    }

    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorWriteOperationId,
        evidence: ExternalMutationEvidence,
        _context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
    {
        let mut state = self.lock();
        state.events.push(ProviderEvent::HistoricalReconcileCleanup);
        if evidence.operation_id().to_bytes() != operation_id.to_bytes() {
            return Err(invalid(
                "historical write cleanup evidence names another operation",
            ));
        }
        let id = artifact_id(operation_id);
        let effect = if state.artifacts.remove(&id) {
            state.removed.push(id);
            ExternalMutationEffect::Applied
        } else {
            ExternalMutationEffect::NoOp
        };
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt: ConnectorHistoricalWriteCleanupReceipt {
                descriptor_digest: [0; 32],
                observation_digest: [0; 32],
            },
            finalization: ExternalMutationFinalization::Complete,
        })
    }
}

fn continuation_payload(descriptor: &ConnectorHistoricalWriteDescriptor) -> Bytes {
    Bytes::from(format!(
        "{PROVIDER_PRIVATE_MARKER}|continuation|operation={}|input={}|fence={}",
        hex::encode(descriptor.operation_id.to_bytes()),
        hex::encode(descriptor.cohort_set_digest),
        hex::encode(descriptor.raised_fence.digest())
    ))
}

fn cleanup_evidence(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    operation_id: ConnectorWriteOperationId,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    ExternalMutationEvidence::try_new(
        1,
        descriptor.clone(),
        incarnation,
        ConnectorMutationOperationId::from_bytes(operation_id.to_bytes()),
        "historical-write-cleanup",
        Bytes::from(format!("{PROVIDER_PRIVATE_MARKER}|cleanup-evidence")),
    )
}

fn unsupported(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

// ---------------------------------------------------------------------------
// Frontend-visible projection
// ---------------------------------------------------------------------------

/// Everything the frontend is allowed to learn from one historical inspection.
///
/// Building this value is the only projection performed here, and it reads no
/// provider payload: proof, continuation, committed version and write receipt
/// are reduced to their digests before they cross this boundary. The frontend
/// persists identity, generation scalars, digests and opaque bytes only.
#[derive(Clone, Debug, Eq, PartialEq)]
struct FrontendVisibleOutcome {
    disposition: ConnectorHistoricalWriteDisposition,
    operation_id: ConnectorWriteOperationId,
    descriptor_digest: [u8; 32],
    raised_fence_digest: [u8; 32],
    proof_digest: [u8; 32],
    continuation_digest: Option<[u8; 32]>,
    committed_version_digest: Option<[u8; 32]>,
    write_receipt_digest: Option<[u8; 32]>,
    finalization_complete: Option<bool>,
    cleanup_required: bool,
    resolved: bool,
}

impl FrontendVisibleOutcome {
    fn project(observation: &ConnectorHistoricalWriteObservation) -> Self {
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
}

// ---------------------------------------------------------------------------
// Descriptor construction
// ---------------------------------------------------------------------------

/// A row-DML statement family and the physical write intent it lowers to. The
/// same operation-level protocol must cover all of them.
#[derive(Clone, Copy)]
struct StatementFamily {
    label: &'static str,
    intent: ConnectorWriteIntent,
    operation_byte: u8,
}

const STATEMENT_FAMILIES: [StatementFamily; 6] = [
    StatementFamily {
        label: "INSERT",
        intent: ConnectorWriteIntent::Append,
        operation_byte: 1,
    },
    StatementFamily {
        label: "INSERT OVERWRITE",
        intent: ConnectorWriteIntent::Overwrite,
        operation_byte: 2,
    },
    StatementFamily {
        label: "INSERT OVERWRITE PARTITIONS",
        intent: ConnectorWriteIntent::PartitionOverwrite,
        operation_byte: 3,
    },
    StatementFamily {
        label: "DELETE",
        intent: ConnectorWriteIntent::RowDelta,
        operation_byte: 4,
    },
    StatementFamily {
        label: "UPDATE",
        intent: ConnectorWriteIntent::RowDelta,
        operation_byte: 5,
    },
    StatementFamily {
        label: "MERGE",
        intent: ConnectorWriteIntent::RowDelta,
        operation_byte: 6,
    },
];

impl StatementFamily {
    fn operation_id(self) -> ConnectorWriteOperationId {
        ConnectorWriteOperationId::from_bytes([self.operation_byte; 16])
    }
}

/// The journal shape of the historical attempt.
#[derive(Clone, Copy)]
struct JournalShape {
    phase: ConnectorHistoricalWritePhase,
    state: ConnectorHistoricalWriteDispatchState,
}

impl JournalShape {
    /// Nothing beyond activation ever left the frontend.
    const NOTHING_DISPATCHED: Self = Self {
        phase: ConnectorHistoricalWritePhase::WritersDispatched,
        state: ConnectorHistoricalWriteDispatchState::NotDispatched,
    };

    /// A commit was dispatched and its reply was lost: the operation may
    /// already have taken effect externally.
    const COMMIT_MAY_HAVE_LANDED: Self = Self {
        phase: ConnectorHistoricalWritePhase::CommitDispatched,
        state: ConnectorHistoricalWriteDispatchState::Unknown,
    };

    fn checkpoints(self) -> Vec<ConnectorHistoricalWriteCheckpoint> {
        vec![
            ConnectorHistoricalWriteCheckpoint {
                phase: ConnectorHistoricalWritePhase::Activated,
                state: ConnectorHistoricalWriteDispatchState::Completed,
                evidence_digest: None,
            },
            ConnectorHistoricalWriteCheckpoint {
                phase: ConnectorHistoricalWritePhase::FenceEstablished,
                state: ConnectorHistoricalWriteDispatchState::Completed,
                evidence_digest: None,
            },
            ConnectorHistoricalWriteCheckpoint {
                phase: self.phase,
                state: self.state,
                evidence_digest: None,
            },
        ]
    }
}

/// Build the immutable historical descriptor the recovering owner submits.
fn descriptor(
    family: StatementFamily,
    journal: JournalShape,
    historical: ConnectorHistoricalWriteFence,
) -> ConnectorHistoricalWriteDescriptor {
    descriptor_with_input(family, journal, historical, [7; 32])
}

/// Same as `descriptor`, with an explicit old immutable input digest so a
/// continuation can be shown not to transplant onto a different input.
fn descriptor_with_input(
    family: StatementFamily,
    journal: JournalShape,
    historical: ConnectorHistoricalWriteFence,
    cohort_set_digest: [u8; 32],
) -> ConnectorHistoricalWriteDescriptor {
    let operation_id = family.operation_id();
    let raised = raised_fence(operation_id);
    let receipt = ConnectorExternalFenceReceipt::try_new(&raised, marker_payload(&raised))
        .expect("raised fence receipt");
    ConnectorHistoricalWriteDescriptor::try_new(
        ConnectorHistoricalWriteIdentity {
            historical_binding: binding_key(HISTORICAL_INCARNATION),
            table: table_identity(),
            target_ref: ConnectorWriteTargetRef::main(),
            operation_id,
            intent: family.intent,
            cohort_set_digest,
            aggregate_digest: Some([8; 32]),
        },
        ConnectorHistoricalWriteFenceFacts {
            historical_fence: historical,
            raised_fence: raised,
            raised_fence_receipt_digest: receipt.digest(),
        },
        journal.checkpoints(),
        None,
    )
    .expect("historical write descriptor")
}

/// Perform the takeover fence raise the spec requires before any inspection.
fn raise_fence(
    provider: &Arc<FakeProvider>,
    descriptor: &ConnectorHistoricalWriteDescriptor,
) -> ConnectorExternalFenceReceipt {
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
    let receipt = recovery
        .raise_external_fence(ConnectorHistoricalWriteFenceRaiseRequest {
            historical_binding: descriptor.historical_binding.clone(),
            observed: descriptor.historical_fence.clone(),
            raised: descriptor.raised_fence.clone(),
            context: context(),
        })
        .expect("the recovering owner must be able to raise a strictly higher external fence");
    assert!(
        receipt.matches(&descriptor.raised_fence),
        "the raise receipt must acknowledge exactly the raised fence"
    );
    receipt
}

/// A control binding that owns the ordinary write capability *and* the
/// historical facet, installed separately.
fn binding_with_both_facets(provider: &Arc<FakeProvider>) -> ConnectorControlBinding {
    let write: Arc<dyn ConnectorWriteControl> = provider.clone();
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
    ConnectorControlBinding::try_new_with_capabilities(
        instance_descriptor(),
        ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
        provider.clone(),
        provider.clone(),
        provider.clone(),
        None,
        Some(write),
        None,
    )
    .expect("control binding with ordinary write capability")
    .try_with_historical_write_recovery(Some(recovery))
    .expect("historical write recovery facet installs on its own generation")
}

// ---------------------------------------------------------------------------
// 1. Full disposition coverage for every row-DML statement family
// ---------------------------------------------------------------------------

/// The typed neutral shape the frontend must see for one disposition.
struct ExpectedNeutralShape {
    disposition: ConnectorHistoricalWriteDisposition,
    resolved: bool,
    cleanup_required: bool,
    carries_application: bool,
    carries_continuation: bool,
}

fn expected_shape(truth: ExternalTruth) -> ExpectedNeutralShape {
    match truth {
        ExternalTruth::OperationCommitted => ExpectedNeutralShape {
            disposition: ConnectorHistoricalWriteDisposition::Applied,
            resolved: true,
            cleanup_required: true,
            carries_application: true,
            carries_continuation: false,
        },
        ExternalTruth::ProvenUncommitted => ExpectedNeutralShape {
            disposition: ConnectorHistoricalWriteDisposition::NotApplied,
            resolved: true,
            cleanup_required: true,
            carries_application: false,
            carries_continuation: false,
        },
        ExternalTruth::NothingDispatched => ExpectedNeutralShape {
            disposition: ConnectorHistoricalWriteDisposition::NotDispatched,
            resolved: true,
            cleanup_required: false,
            carries_application: false,
            carries_continuation: true,
        },
        ExternalTruth::StagedOutputOnly => ExpectedNeutralShape {
            disposition: ConnectorHistoricalWriteDisposition::Staged,
            resolved: true,
            cleanup_required: true,
            carries_application: false,
            carries_continuation: false,
        },
        ExternalTruth::SupersededByAnotherOperation => ExpectedNeutralShape {
            disposition: ConnectorHistoricalWriteDisposition::Conflict,
            resolved: true,
            cleanup_required: true,
            carries_application: false,
            carries_continuation: false,
        },
        ExternalTruth::Unclassifiable => ExpectedNeutralShape {
            disposition: ConnectorHistoricalWriteDisposition::Unsupported,
            resolved: false,
            cleanup_required: false,
            carries_application: false,
            carries_continuation: false,
        },
    }
}

#[test]
fn every_disposition_reaches_the_frontend_as_a_typed_neutral_result() {
    // `Ambiguous` is covered by the corruption matrix below; here every truth a
    // provider can actually prove must produce its own typed disposition, for
    // every row-DML statement family.
    for family in STATEMENT_FAMILIES {
        for truth in ExternalTruth::ALL {
            let expected = expected_shape(truth);
            // A proven not-dispatched operation must be paired with a journal
            // that agrees; anything else legitimately degrades to Ambiguous and
            // is asserted separately.
            let journal = if truth == ExternalTruth::NothingDispatched {
                JournalShape::NOTHING_DISPATCHED
            } else {
                JournalShape::COMMIT_MAY_HAVE_LANDED
            };
            let provider = FakeProvider::new();
            let operation_id = family.operation_id();
            provider.seed(
                operation_id,
                truth,
                ArtifactDamage::None,
                CleanupBehavior::Complete,
            );
            let descriptor =
                descriptor(family, journal, established_historical_fence(operation_id));
            raise_fence(&provider, &descriptor);

            let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
            let observation = assert_historical_write_recovery_contract(
                recovery.as_ref(),
                descriptor.clone(),
                context(),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "{} / {truth:?}: the historical recovery contract must hold for every \
                     disposition, but the provider violated it: {error}",
                    family.label
                )
            });

            let outcome = FrontendVisibleOutcome::project(&observation);
            assert_eq!(
                outcome.disposition, expected.disposition,
                "{} / {truth:?}: durable external truth must map to exactly one typed \
                 disposition; a provider may not answer with a different classification",
                family.label
            );
            assert_eq!(
                outcome.resolved,
                expected.resolved,
                "{} / {truth:?}: `{:?}` must{} be a resolved classification",
                family.label,
                expected.disposition,
                if expected.resolved { "" } else { " not" }
            );
            assert_eq!(
                outcome.cleanup_required, expected.cleanup_required,
                "{} / {truth:?}: `{:?}` carries the wrong cleanup requirement",
                family.label, expected.disposition
            );
            assert_eq!(
                outcome.committed_version_digest.is_some(),
                expected.carries_application,
                "{} / {truth:?}: only an applied operation may carry finalization facts",
                family.label
            );
            assert_eq!(
                outcome.continuation_digest.is_some(),
                expected.carries_continuation,
                "{} / {truth:?}: only a proven not-dispatched operation may carry a continuation",
                family.label
            );
            assert_eq!(
                outcome.operation_id, operation_id,
                "{} / {truth:?}: the observation must answer the same stable write operation",
                family.label
            );
            assert_eq!(
                outcome.descriptor_digest,
                descriptor.digest(),
                "{} / {truth:?}: the observation must answer exactly the submitted descriptor",
                family.label
            );
            assert_eq!(
                outcome.raised_fence_digest,
                descriptor.raised_fence.digest(),
                "{} / {truth:?}: the observation must be bound to the raised external fence",
                family.label
            );

            // The frontend never decodes an opaque provider payload: the
            // projection it keeps must not contain any payload body.
            let rendered = format!("{outcome:?}");
            assert!(
                !rendered.contains(PROVIDER_PRIVATE_MARKER),
                "{} / {truth:?}: the frontend-visible projection leaked a provider-private \
                 payload; the frontend must persist digests and opaque bytes only",
                family.label
            );
            assert!(
                !format!("{:?}", observation.proof).contains(PROVIDER_PRIVATE_MARKER),
                "{} / {truth:?}: the historical write proof Debug rendering must stay redacted",
                family.label
            );
            if let Some(continuation) = &observation.continuation {
                assert!(
                    !format!("{continuation:?}").contains(PROVIDER_PRIVATE_MARKER),
                    "{} / {truth:?}: the continuation Debug rendering must stay redacted",
                    family.label
                );
            }

            assert_eq!(
                provider.ordinary_calls(),
                OrdinaryCallCounts::default(),
                "{} / {truth:?}: historical recovery must not make any ordinary write call",
                family.label
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Corrupt or missing evidence -> Ambiguous, never NotApplied
// ---------------------------------------------------------------------------

#[test]
fn corrupt_or_missing_evidence_classifies_ambiguous_and_never_not_applied() {
    for family in STATEMENT_FAMILIES {
        for truth in ExternalTruth::ALL {
            for damage in ArtifactDamage::CORRUPTIONS {
                let provider = FakeProvider::new();
                let operation_id = family.operation_id();
                provider.seed(operation_id, truth, damage, CleanupBehavior::Complete);
                let descriptor = descriptor(
                    family,
                    JournalShape::COMMIT_MAY_HAVE_LANDED,
                    established_historical_fence(operation_id),
                );
                raise_fence(&provider, &descriptor);

                let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
                let observation = assert_historical_write_recovery_contract(
                    recovery.as_ref(),
                    descriptor.clone(),
                    context(),
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "{} / {truth:?} / {damage:?}: damaged evidence must still produce a \
                         contract-conforming observation, not an error: {error}",
                        family.label
                    )
                });
                let outcome = FrontendVisibleOutcome::project(&observation);

                assert_ne!(
                    outcome.disposition,
                    ConnectorHistoricalWriteDisposition::NotApplied,
                    "{} / {truth:?} / {damage:?}: a missing or corrupt artifact must NEVER be \
                     read as `NotApplied`; an absent proof is not a proof of non-commit",
                    family.label
                );
                assert_eq!(
                    outcome.disposition,
                    ConnectorHistoricalWriteDisposition::Ambiguous,
                    "{} / {truth:?} / {damage:?}: unreadable evidence must classify as \
                     `Ambiguous` so the recovery record stays unresolved",
                    family.label
                );
                assert!(
                    !outcome.resolved,
                    "{} / {truth:?} / {damage:?}: `Ambiguous` must keep the recovery record \
                     unresolved",
                    family.label
                );
                assert!(
                    !outcome.cleanup_required,
                    "{} / {truth:?} / {damage:?}: an unresolved observation must not request \
                     cleanup",
                    family.label
                );
                assert!(
                    outcome.continuation_digest.is_none(),
                    "{} / {truth:?} / {damage:?}: an unresolved observation must never unlock a \
                     continuation",
                    family.label
                );
                assert!(
                    outcome.committed_version_digest.is_none(),
                    "{} / {truth:?} / {damage:?}: an unresolved observation must not carry \
                     finalization facts",
                    family.label
                );
                assert_eq!(
                    provider.ordinary_calls(),
                    OrdinaryCallCounts::default(),
                    "{} / {truth:?} / {damage:?}: classifying damaged evidence must not reach \
                     any ordinary write method",
                    family.label
                );
            }
        }
    }
}

#[test]
fn a_provider_claim_of_not_dispatched_is_vetoed_by_an_unknown_journal_checkpoint() {
    // The second, independent source of `Ambiguous`: the artifact is intact and
    // the provider believes nothing was dispatched, but the frontend journal
    // cannot rule out that a commit already left this cluster. The operation
    // must stay unresolved rather than become continuable.
    for family in STATEMENT_FAMILIES {
        let operation_id = family.operation_id();
        let provider = FakeProvider::new();
        provider.seed(
            operation_id,
            ExternalTruth::NothingDispatched,
            ArtifactDamage::None,
            CleanupBehavior::Complete,
        );
        let sealed = descriptor(
            family,
            JournalShape::COMMIT_MAY_HAVE_LANDED,
            established_historical_fence(operation_id),
        );
        raise_fence(&provider, &sealed);
        let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
        let observation = recovery
            .inspect(sealed.clone(), context())
            .expect("historical inspection");
        let outcome = FrontendVisibleOutcome::project(&observation);
        assert_eq!(
            outcome.disposition,
            ConnectorHistoricalWriteDisposition::Ambiguous,
            "{}: a provider may not classify `NotDispatched` while the journal reports an \
             unknown commit dispatch; the record must stay unresolved",
            family.label
        );
        assert!(
            outcome.continuation_digest.is_none() && !outcome.resolved,
            "{}: an operation that may already have been dispatched must never become \
             continuable",
            family.label
        );
        assert_eq!(
            provider.ordinary_calls(),
            OrdinaryCallCounts::default(),
            "{}: vetoing a continuation must not reach any ordinary write method",
            family.label
        );
    }
}

#[test]
fn an_unresolved_observation_cannot_be_made_to_request_cleanup() {
    // The SPI, not the provider, is the authority here: an `Ambiguous` or
    // `Unsupported` observation that asks for cleanup must not be constructible.
    let family = STATEMENT_FAMILIES[0];
    let operation_id = family.operation_id();
    let sealed = descriptor(
        family,
        JournalShape::COMMIT_MAY_HAVE_LANDED,
        established_historical_fence(operation_id),
    );
    let proof = ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"unresolved-proof"))
        .expect("proof");
    for disposition in [
        ConnectorHistoricalWriteDisposition::Ambiguous,
        ConnectorHistoricalWriteDisposition::Unsupported,
    ] {
        let error = ConnectorHistoricalWriteObservation::try_new(
            &sealed,
            disposition,
            ConnectorHistoricalWriteOutcomeFacts {
                application: None,
                continuation: None,
                cleanup_required: true,
            },
            proof.clone(),
        )
        .err();
        assert!(
            error.is_some(),
            "{disposition:?}: an unresolved historical observation must not be able to request \
             cleanup, because cleanup would act on artifacts nothing has proven"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. An unknown dispatch state is never read as "nothing dispatched"
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_dispatch_checkpoint_never_proves_nothing_was_dispatched() {
    const IRREVERSIBLE_PHASES: [ConnectorHistoricalWritePhase; 3] = [
        ConnectorHistoricalWritePhase::WritersDispatched,
        ConnectorHistoricalWritePhase::WritersCompleted,
        ConnectorHistoricalWritePhase::CommitDispatched,
    ];
    const HARMLESS_PHASES: [ConnectorHistoricalWritePhase; 3] = [
        ConnectorHistoricalWritePhase::Prepared,
        ConnectorHistoricalWritePhase::Activated,
        ConnectorHistoricalWritePhase::FenceEstablished,
    ];
    const STATES: [ConnectorHistoricalWriteDispatchState; 4] = [
        ConnectorHistoricalWriteDispatchState::NotDispatched,
        ConnectorHistoricalWriteDispatchState::Dispatched,
        ConnectorHistoricalWriteDispatchState::Completed,
        ConnectorHistoricalWriteDispatchState::Unknown,
    ];

    let family = STATEMENT_FAMILIES[0];
    for phase in IRREVERSIBLE_PHASES {
        for state in STATES {
            let sealed = descriptor(
                family,
                JournalShape { phase, state },
                ConnectorHistoricalWriteFence::NotEstablished,
            );
            let expected = state == ConnectorHistoricalWriteDispatchState::NotDispatched;
            assert_eq!(
                sealed.journal_proves_nothing_dispatched(),
                expected,
                "{phase:?}/{state:?}: only an explicit `NotDispatched` checkpoint proves nothing \
                 was dispatched; `Unknown` or `Dispatched` must never be inferred as safe"
            );
        }
    }
    for phase in HARMLESS_PHASES {
        for state in STATES {
            let sealed = descriptor(
                family,
                JournalShape { phase, state },
                ConnectorHistoricalWriteFence::NotEstablished,
            );
            assert!(
                sealed.journal_proves_nothing_dispatched(),
                "{phase:?}/{state:?}: a phase that cannot produce an irreversible external \
                 effect must not block a continuation"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 4. Zero ordinary write calls during historical recovery (spec criterion 4)
// ---------------------------------------------------------------------------

#[test]
fn historical_recovery_of_a_dispatched_operation_makes_zero_ordinary_write_calls() {
    let family = STATEMENT_FAMILIES[5]; // MERGE: the widest row-DML form.
    let operation_id = family.operation_id();
    let provider = FakeProvider::new();
    provider.seed(
        operation_id,
        ExternalTruth::OperationCommitted,
        ArtifactDamage::None,
        CleanupBehavior::Complete,
    );
    let binding = binding_with_both_facets(&provider);

    // The ordinary write capability is installed and reachable on this exact
    // binding, so "zero ordinary calls" below is a real observation.
    assert!(
        binding.write().is_some(),
        "the fixture must install the ordinary write capability, otherwise a zero ordinary call \
         count proves nothing"
    );
    let recovery = binding
        .historical_write_recovery()
        .expect("the historical facet must be installed on the current control binding")
        .clone();

    // The operation was dispatched and its reply lost: replaying an ordinary
    // commit or reconcile here is exactly what must not happen.
    let sealed = descriptor(
        family,
        JournalShape::COMMIT_MAY_HAVE_LANDED,
        established_historical_fence(operation_id),
    );
    assert!(
        !sealed.journal_proves_nothing_dispatched(),
        "the fixture must describe an operation that may already have been dispatched"
    );

    raise_fence(&provider, &sealed);
    let observation = recovery
        .inspect(sealed.clone(), context())
        .expect("historical inspection");
    observation
        .validate_for(&sealed)
        .expect("the observation must answer exactly this descriptor");
    let cleanup = recovery
        .cleanup(ConnectorHistoricalWriteCleanupRequest {
            operation_id,
            descriptor_digest: sealed.digest(),
            observation: observation.clone(),
            context: context(),
        })
        .expect("guarded cleanup");
    assert!(
        matches!(cleanup, ExternalMutationOutcome::KnownCommitted { .. }),
        "a proof-bound cleanup of an applied operation must reach a known outcome"
    );
    let reconciled = recovery
        .reconcile_cleanup(
            operation_id,
            cleanup_evidence(
                &instance_descriptor(),
                ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
                operation_id,
            )
            .expect("cleanup evidence"),
            context(),
        )
        .expect("cleanup reconciliation from opaque evidence only");
    assert!(matches!(
        reconciled,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));

    assert_eq!(
        provider.ordinary_calls(),
        OrdinaryCallCounts::default(),
        "historical recovery of an already dispatched operation called an ordinary write method; \
         spec CP-3B forbids replaying a dispatched operation through ordinary commit/reconcile"
    );
    let ordinary = provider
        .events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                ProviderEvent::OrdinaryCommit
                    | ProviderEvent::OrdinaryAbort
                    | ProviderEvent::OrdinaryReconcile
                    | ProviderEvent::OrdinaryPlanWrite
            )
        })
        .count();
    assert_eq!(
        ordinary, 0,
        "the provider observed an ordinary write event during historical recovery"
    );
}

#[test]
fn guarded_cleanup_touches_only_the_proof_bound_artifact() {
    let family = STATEMENT_FAMILIES[3]; // DELETE
    let operation_id = family.operation_id();
    let provider = FakeProvider::new();
    provider.seed(
        operation_id,
        ExternalTruth::StagedOutputOnly,
        ArtifactDamage::None,
        CleanupBehavior::Complete,
    );
    provider.seed_unrelated_artifact("artifact/sibling-operation");
    let sealed = descriptor(
        family,
        JournalShape::COMMIT_MAY_HAVE_LANDED,
        established_historical_fence(operation_id),
    );
    raise_fence(&provider, &sealed);
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
    let observation = recovery
        .inspect(sealed.clone(), context())
        .expect("historical inspection");
    assert_eq!(
        observation.disposition,
        ConnectorHistoricalWriteDisposition::Staged,
        "writer output without commit provenance must classify as `Staged`"
    );

    recovery
        .cleanup(ConnectorHistoricalWriteCleanupRequest {
            operation_id,
            descriptor_digest: sealed.digest(),
            observation: observation.clone(),
            context: context(),
        })
        .expect("guarded cleanup of staged output");

    assert_eq!(
        provider.removed(),
        vec![artifact_id(operation_id)],
        "a guarded cleanup must remove exactly the artifact its observation proves"
    );
    assert!(
        provider.artifacts().contains("artifact/sibling-operation"),
        "a guarded cleanup must not touch an artifact belonging to another operation"
    );

    // An observation this provider never issued is not proof bound.
    let forged = ConnectorHistoricalWriteObservation::try_new(
        &sealed,
        ConnectorHistoricalWriteDisposition::Staged,
        ConnectorHistoricalWriteOutcomeFacts {
            application: None,
            continuation: None,
            cleanup_required: true,
        },
        ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"forged-proof"))
            .expect("forged proof"),
    )
    .expect("a well-formed but never-issued observation");
    let error = recovery
        .cleanup(ConnectorHistoricalWriteCleanupRequest {
            operation_id,
            descriptor_digest: sealed.digest(),
            observation: forged,
            context: context(),
        })
        .expect_err("cleanup must refuse an observation this provider never issued");
    assert_eq!(
        error.kind(),
        ConnectorErrorKind::InvalidRequest,
        "a non-proof-bound cleanup must be refused as an invalid request, not attempted"
    );
    assert_eq!(
        provider.removed(),
        vec![artifact_id(operation_id)],
        "a refused cleanup must not remove anything further"
    );
}

// ---------------------------------------------------------------------------
// 5. Continuation only on NotDispatched, only after the higher fence
// ---------------------------------------------------------------------------

#[test]
fn a_continuation_is_issued_only_for_a_proven_not_dispatched_operation() {
    let family = STATEMENT_FAMILIES[4]; // UPDATE
    let operation_id = family.operation_id();
    let provider = FakeProvider::new();
    provider.seed(
        operation_id,
        ExternalTruth::NothingDispatched,
        ArtifactDamage::None,
        CleanupBehavior::Complete,
    );
    let historical = established_historical_fence(operation_id);
    let sealed = descriptor(family, JournalShape::NOTHING_DISPATCHED, historical.clone());
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();

    // Before the higher fence is established the provider must refuse to
    // classify at all, so no continuation can exist.
    let error = recovery
        .inspect(sealed.clone(), context())
        .expect_err("inspection before the fence raise must be refused");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::NotEstablished),
        "an inspection without an established higher fence must fail with a typed \
         `NotEstablished` fence failure, never with a disposition"
    );
    assert_typed_fence_conflict(&error)
        .expect("a fence refusal must stay typed, non-retryable, and not downgraded");

    raise_fence(&provider, &sealed);
    let observation = recovery
        .inspect(sealed.clone(), context())
        .expect("historical inspection after the fence raise");
    assert_eq!(
        observation.disposition,
        ConnectorHistoricalWriteDisposition::NotDispatched
    );
    let continuation = observation
        .continuation
        .clone()
        .expect("a proven not-dispatched operation must carry a continuation");

    // The continuation binds the current generation, the same stable operation,
    // a strictly newer attempt/fence, and the old immutable input.
    assert!(
        continuation.is_bound_to(&sealed.raised_fence),
        "the continuation must be bound to the raised external fence"
    );
    assert_eq!(
        recovery.binding_key(),
        &binding_key(CURRENT_INCARNATION),
        "the continuation must be issued by the current control generation"
    );
    assert_ne!(
        &sealed.historical_binding,
        recovery.binding_key(),
        "the historical binding must not be revived as the issuing generation"
    );
    assert_eq!(
        sealed.raised_fence.operation_id(),
        operation_id,
        "the continuation must name the same stable DML operation"
    );
    let historical_fence_value = historical
        .fence()
        .expect("the fixture establishes a historical fence")
        .clone();
    assert!(
        sealed
            .raised_fence
            .supersedes(&historical_fence_value)
            .expect("same authority"),
        "the continuation's fence must strictly supersede the historical fence"
    );
    assert_ne!(
        sealed.raised_fence.coordination_attempt_id(),
        historical_fence_value.coordination_attempt_id(),
        "the continuation must belong to a new coordination attempt"
    );
    assert_eq!(
        observation.descriptor_digest,
        sealed.digest(),
        "the continuation is only valid against the old immutable input digest it was issued for"
    );

    // The same continuation must not transplant onto another old input digest.
    let other_input = descriptor_with_input(
        family,
        JournalShape::NOTHING_DISPATCHED,
        historical,
        [77; 32],
    );
    assert!(
        continuation.is_bound_to(&other_input.raised_fence),
        "the fixture must share one raised fence so the input digest is the only difference"
    );
    assert!(
        observation.validate_for(&other_input).is_err(),
        "a continuation issued for one old immutable input must not validate against another"
    );

    // No other disposition may carry a continuation.
    let proof =
        ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"other-proof")).expect("proof");
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
                proof.clone(),
            )
            .is_err(),
            "{disposition:?} must not be able to carry a continuation; only a proven \
             `NotDispatched` operation may be continued"
        );
        assert!(
            !disposition.may_continue(),
            "{disposition:?} must not report itself as continuable"
        );
    }

    // A dispatched journal checkpoint forbids a continuation even when the
    // provider believes nothing was dispatched.
    let dispatched = descriptor(
        family,
        JournalShape::COMMIT_MAY_HAVE_LANDED,
        established_historical_fence(operation_id),
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
            proof,
        )
        .is_err(),
        "a journal checkpoint that may have dispatched must veto a continuation"
    );

    assert_eq!(
        provider.ordinary_calls(),
        OrdinaryCallCounts::default(),
        "issuing a continuation must not touch any ordinary write method"
    );
}

#[test]
fn a_raised_fence_that_does_not_supersede_the_historical_fence_is_refused() {
    let family = STATEMENT_FAMILIES[0];
    let operation_id = family.operation_id();
    // The historical fence already sits at the generation the recovering owner
    // would raise, so the takeover cannot fence out the old authority.
    let equal = fence(FenceSpec {
        operation_id,
        control_plane_incarnation: 1,
        resource_epoch: 3,
        coordination_attempt: 1,
        coordination_attempt_id: [3; 16],
    });
    let receipt =
        ConnectorExternalFenceReceipt::try_new(&equal, marker_payload(&equal)).expect("receipt");
    let historical =
        ConnectorHistoricalWriteFence::established(&receipt, equal).expect("established");
    let raised = raised_fence(operation_id);
    let raised_receipt =
        ConnectorExternalFenceReceipt::try_new(&raised, marker_payload(&raised)).expect("receipt");
    let error = ConnectorHistoricalWriteDescriptor::try_new(
        ConnectorHistoricalWriteIdentity {
            historical_binding: binding_key(HISTORICAL_INCARNATION),
            table: table_identity(),
            target_ref: ConnectorWriteTargetRef::main(),
            operation_id,
            intent: family.intent,
            cohort_set_digest: [7; 32],
            aggregate_digest: Some([8; 32]),
        },
        ConnectorHistoricalWriteFenceFacts {
            historical_fence: historical.clone(),
            raised_fence: raised,
            raised_fence_receipt_digest: raised_receipt.digest(),
        },
        JournalShape::NOTHING_DISPATCHED.checkpoints(),
        None,
    )
    .expect_err("a non-superseding raised fence must not produce a descriptor");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::Stale),
        "inspecting without a strictly higher raised fence must fail as typed stale; the old \
         authority would still be able to commit"
    );

    // The same rule applies to the raise request itself.
    let provider = FakeProvider::new();
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
    let error = recovery
        .raise_external_fence(ConnectorHistoricalWriteFenceRaiseRequest {
            historical_binding: binding_key(HISTORICAL_INCARNATION),
            observed: historical,
            raised: historical_fence(operation_id),
            context: context(),
        })
        .expect_err("a raise below the observed fence must be refused");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::Stale)
    );
    assert_typed_fence_conflict(&error).expect("a stale raise must stay typed and non-retryable");
}

// ---------------------------------------------------------------------------
// 6. A stale historical response must not change durable state
// ---------------------------------------------------------------------------

/// A minimal stand-in for the fenced durable record the frontend keeps for one
/// historical write operation.
///
/// Every accept/refuse decision is delegated to product code
/// (`ConnectorHistoricalWriteObservation::validate_for` plus the raised fence
/// digest recorded by the current lease); the ledger exists only so a test can
/// observe that a refused response left durable state untouched. These
/// observations must be re-pointed at the real journal once the T6 frontend
/// recovery profile lands.
struct RecoveryLedger {
    descriptor: ConnectorHistoricalWriteDescriptor,
    expected_raised_fence_digest: [u8; 32],
    resolution: Option<ConnectorHistoricalWriteDisposition>,
    cleanup_pending: bool,
    retained_finalization: Option<ExternalMutationFinalization>,
    user_result_terminal: bool,
}

impl RecoveryLedger {
    fn open(descriptor: ConnectorHistoricalWriteDescriptor) -> Self {
        let expected_raised_fence_digest = descriptor.raised_fence.digest();
        Self {
            descriptor,
            expected_raised_fence_digest,
            resolution: None,
            cleanup_pending: false,
            retained_finalization: None,
            user_result_terminal: false,
        }
    }

    /// The CP-3B D5 double check: the response must answer the descriptor this
    /// record still owns, under the fence this owner still holds.
    fn apply(
        &mut self,
        observation: &ConnectorHistoricalWriteObservation,
    ) -> Result<(), ConnectorError> {
        if observation.raised_fence_digest != self.expected_raised_fence_digest {
            return Err(ConnectorError::external_fence(
                ConnectorExternalFenceFailure::Stale,
                "historical write response was produced under a superseded external fence",
            ));
        }
        observation.validate_for(&self.descriptor)?;
        self.resolution = Some(observation.disposition);
        self.cleanup_pending = observation.cleanup_required;
        if let Some(application) = &observation.application {
            self.retained_finalization = Some(application.finalization.clone());
        }
        Ok(())
    }

    fn record_cleanup(
        &mut self,
        outcome: &ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>,
    ) {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { finalization, .. } => {
                self.retained_finalization = Some(finalization.clone());
                self.cleanup_pending =
                    !matches!(finalization, ExternalMutationFinalization::Complete);
            }
            ExternalMutationOutcome::KnownUncommitted { .. }
            | ExternalMutationOutcome::CommitUnknown { .. } => {
                self.cleanup_pending = true;
            }
        }
    }

    fn terminalize_user_result(&mut self) {
        self.user_result_terminal = true;
    }
}

#[test]
fn a_historical_response_from_a_superseded_lease_cannot_change_durable_state() {
    let family = STATEMENT_FAMILIES[1]; // INSERT OVERWRITE
    let operation_id = family.operation_id();
    let provider = FakeProvider::new();
    provider.seed(
        operation_id,
        ExternalTruth::OperationCommitted,
        ArtifactDamage::None,
        CleanupBehavior::Complete,
    );
    let historical = established_historical_fence(operation_id);
    let first = descriptor(
        family,
        JournalShape::COMMIT_MAY_HAVE_LANDED,
        historical.clone(),
    );
    raise_fence(&provider, &first);
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
    let stale_response = recovery
        .inspect(first.clone(), context())
        .expect("first inspection");

    // The lease moves on: a new owner raises a strictly higher fence and opens
    // its own recovery record.
    let higher = fence(FenceSpec {
        operation_id,
        control_plane_incarnation: 1,
        resource_epoch: 4,
        coordination_attempt: 1,
        coordination_attempt_id: [4; 16],
    });
    let higher_receipt = ConnectorExternalFenceReceipt::try_new(&higher, marker_payload(&higher))
        .expect("higher receipt");
    let second = ConnectorHistoricalWriteDescriptor::try_new(
        ConnectorHistoricalWriteIdentity {
            historical_binding: binding_key(HISTORICAL_INCARNATION),
            table: table_identity(),
            target_ref: ConnectorWriteTargetRef::main(),
            operation_id,
            intent: family.intent,
            cohort_set_digest: [7; 32],
            aggregate_digest: Some([8; 32]),
        },
        ConnectorHistoricalWriteFenceFacts {
            historical_fence: historical,
            raised_fence: higher.clone(),
            raised_fence_receipt_digest: higher_receipt.digest(),
        },
        JournalShape::COMMIT_MAY_HAVE_LANDED.checkpoints(),
        None,
    )
    .expect("second recovery attempt descriptor");
    let mut ledger = RecoveryLedger::open(second.clone());

    let error = ledger
        .apply(&stale_response)
        .expect_err("a response produced under a superseded fence must be refused");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::Stale),
        "a late historical response must be refused as typed stale"
    );
    assert!(
        ledger.resolution.is_none() && !ledger.cleanup_pending,
        "a stale historical response must leave durable state untouched"
    );

    // The old response is also structurally unable to answer the new descriptor.
    assert!(
        stale_response.validate_for(&second).is_err(),
        "an observation must not validate against a descriptor it did not answer"
    );

    // The current owner repeats the inspection on the same immutable request
    // and only that response is durable.
    raise_fence(&provider, &second);
    let fresh = recovery
        .inspect(second.clone(), context())
        .expect("repeated inspection under the current fence");
    ledger.apply(&fresh).expect("the current response applies");
    assert_eq!(
        ledger.resolution,
        Some(ConnectorHistoricalWriteDisposition::Applied),
        "the response produced under the current fence must be the one that lands"
    );

    // Once the fence has moved on, the superseded generation cannot be
    // re-inspected either.
    let error = recovery
        .inspect(first, context())
        .expect_err("inspection under a superseded fence must be refused");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::Stale),
        "the provider must refuse an inspection that is behind its established fence"
    );

    assert_eq!(
        provider.ordinary_calls(),
        OrdinaryCallCounts::default(),
        "refusing a stale response must not reach any ordinary write method"
    );
}

// ---------------------------------------------------------------------------
// 7. Cleanup retention
// ---------------------------------------------------------------------------

#[test]
fn a_cleanup_requirement_and_its_finalization_survive_a_terminal_user_result() {
    for behavior in [
        CleanupBehavior::Refused,
        CleanupBehavior::Lost,
        CleanupBehavior::FinalizationFailed,
    ] {
        let family = STATEMENT_FAMILIES[3]; // DELETE
        let operation_id = family.operation_id();
        let provider = FakeProvider::new();
        provider.seed(
            operation_id,
            ExternalTruth::ProvenUncommitted,
            ArtifactDamage::None,
            behavior,
        );
        let sealed = descriptor(
            family,
            JournalShape::COMMIT_MAY_HAVE_LANDED,
            established_historical_fence(operation_id),
        );
        raise_fence(&provider, &sealed);
        let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
        let observation = recovery
            .inspect(sealed.clone(), context())
            .expect("historical inspection");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalWriteDisposition::NotApplied,
            "{behavior:?}: a provably uncommitted operation must classify as `NotApplied`"
        );
        assert!(
            observation.cleanup_required,
            "{behavior:?}: a `NotApplied` operation still owns leftover artifacts to clean up"
        );

        let mut ledger = RecoveryLedger::open(sealed.clone());
        ledger.apply(&observation).expect("observation applies");
        let outcome = recovery
            .cleanup(ConnectorHistoricalWriteCleanupRequest {
                operation_id,
                descriptor_digest: sealed.digest(),
                observation: observation.clone(),
                context: context(),
            })
            .expect("cleanup attempt");
        ledger.record_cleanup(&outcome);
        assert!(
            ledger.cleanup_pending,
            "{behavior:?}: an incomplete cleanup must stay pending"
        );

        // The user-visible statement result becoming terminal must not discard
        // the cleanup record.
        ledger.terminalize_user_result();
        assert!(
            ledger.user_result_terminal && ledger.cleanup_pending,
            "{behavior:?}: a terminal user-visible result must not drop a pending cleanup record"
        );
        if behavior == CleanupBehavior::FinalizationFailed {
            assert!(
                matches!(
                    ledger.retained_finalization,
                    Some(ExternalMutationFinalization::Failed(_))
                ),
                "{behavior:?}: a failed finalization must be retained, not discarded once the \
                 user-visible result is terminal"
            );
        } else {
            assert!(
                provider.removed().is_empty(),
                "{behavior:?}: a cleanup that did not run must not have removed any artifact"
            );
        }

        if let ExternalMutationOutcome::CommitUnknown { evidence, .. } = &outcome {
            let resolved = recovery
                .reconcile_cleanup(operation_id, evidence.clone(), context())
                .expect("a lost cleanup result must be resolvable from opaque evidence");
            assert!(
                matches!(resolved, ExternalMutationOutcome::KnownCommitted { .. }),
                "{behavior:?}: reconciling a lost cleanup must reach a known outcome"
            );
        }

        assert_eq!(
            provider.ordinary_calls(),
            OrdinaryCallCounts::default(),
            "{behavior:?}: cleanup retention must not reach any ordinary write method"
        );
    }
}

#[test]
fn a_sealed_observation_cannot_silently_drop_its_cleanup_or_finalization_record() {
    let family = STATEMENT_FAMILIES[0];
    let operation_id = family.operation_id();
    let sealed = descriptor(
        family,
        JournalShape::COMMIT_MAY_HAVE_LANDED,
        established_historical_fence(operation_id),
    );
    let application = ConnectorHistoricalWriteApplication {
        committed_version: ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"committed-version"),
            Some(42),
        )
        .expect("committed version"),
        receipt: ConnectorWriteReceipt::try_new(Bytes::from_static(b"receipt")).expect("receipt"),
        finalization: ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "external mutation finalization did not complete",
        )),
    };
    let observation = ConnectorHistoricalWriteObservation::try_new(
        &sealed,
        ConnectorHistoricalWriteDisposition::Applied,
        ConnectorHistoricalWriteOutcomeFacts {
            application: Some(application),
            continuation: None,
            cleanup_required: true,
        },
        ConnectorHistoricalWriteProof::try_new(Bytes::from_static(b"applied-proof"))
            .expect("proof"),
    )
    .expect("applied observation with a failed finalization");
    observation
        .validate_for(&sealed)
        .expect("the sealed observation validates");

    let mut cleared = observation.clone();
    cleared.cleanup_required = false;
    assert!(
        cleared.validate_for(&sealed).is_err(),
        "clearing `cleanup_required` must break the observation seal; a cleanup requirement \
         cannot be dropped because the user-visible result is terminal"
    );

    let mut downgraded = observation;
    downgraded
        .application
        .as_mut()
        .expect("application")
        .finalization = ExternalMutationFinalization::Complete;
    assert!(
        downgraded.validate_for(&sealed).is_err(),
        "downgrading a failed finalization to `Complete` must break the observation seal; a \
         `KnownUncommitted` finalization record cannot be discarded"
    );
}

// ---------------------------------------------------------------------------
// 8. Fence establishment precedes any writer or commit dispatch
// ---------------------------------------------------------------------------

/// Mirror of the production dispatch ordering rule: nothing that can produce an
/// irreversible external effect may run until the write authority proves it
/// holds an established external fence.
fn guarded_writer_dispatch(
    lease: &ConnectorWriteLease,
    provider: &FakeProvider,
) -> Result<(), ConnectorError> {
    lease.require_external_fence()?;
    provider.record_writer_dispatch();
    Ok(())
}

#[test]
fn the_external_fence_is_established_before_any_writer_or_commit_dispatch() {
    let family = STATEMENT_FAMILIES[0];
    let operation_id = family.operation_id();
    let provider = FakeProvider::new();
    let control: Arc<dyn ConnectorWriteControl> = provider.clone();
    let lease = ConnectorWriteLease::new(binding_key(CURRENT_INCARNATION), control, || {})
        .expect("write lease");

    // There is no unfenced terminal path.
    let error = lease
        .require_external_fence()
        .expect_err("a write authority without an established fence must fail closed");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::NotEstablished),
        "an unfenced authority must report a typed `NotEstablished` fence failure"
    );
    assert!(
        guarded_writer_dispatch(&lease, &provider).is_err(),
        "no writer may be dispatched before the external fence is established"
    );
    assert!(
        provider.events().is_empty(),
        "the provider must observe nothing at all before the fence is established, but saw {:?}",
        provider.events()
    );
    assert_eq!(
        provider.ordinary_calls(),
        OrdinaryCallCounts::default(),
        "a refused pre-fence dispatch must not reach any ordinary write method"
    );

    let attempt = fence(FenceSpec {
        operation_id,
        control_plane_incarnation: 1,
        resource_epoch: 3,
        coordination_attempt: 1,
        coordination_attempt_id: [3; 16],
    });
    let established = lease
        .establish_external_fence(attempt.clone(), context())
        .expect("the owner establishes its attempt fence before dispatch");
    assert!(
        established.receipt().matches(&attempt),
        "the receipt must acknowledge exactly the established fence"
    );
    guarded_writer_dispatch(&lease, &provider).expect("a fenced authority may dispatch a writer");
    assert_eq!(
        lease.require_external_fence().expect("fenced").digest(),
        attempt.digest(),
        "every terminal provider call must carry the same established fence"
    );

    let events = provider.events();
    let fence_at = events
        .iter()
        .position(|event| matches!(event, ProviderEvent::OrdinaryFenceEstablished(_)))
        .expect("the provider must observe the fence establishment");
    let writer_at = events
        .iter()
        .position(|event| matches!(event, ProviderEvent::WriterDispatched))
        .expect("the provider must observe the writer dispatch");
    assert!(
        fence_at < writer_at,
        "the provider observed the writer dispatch before the fence establishment: {events:?}; \
         the fence must be the linearization point that precedes every irreversible effect"
    );
    assert_eq!(
        provider.ordinary_calls(),
        OrdinaryCallCounts::default(),
        "establishing a fence must not be counted as an ordinary terminal write call"
    );
}

#[test]
fn the_provider_honours_all_four_external_fence_invariants() {
    let family = STATEMENT_FAMILIES[2];
    let operation_id = family.operation_id();
    let provider = FakeProvider::new();
    let control: Arc<dyn ConnectorWriteControl> = provider.clone();
    assert_external_write_fence_contract(
        control.as_ref(),
        &binding_key(CURRENT_INCARNATION),
        ConnectorExternalFenceConformanceInput {
            established: historical_fence(operation_id),
            raised: raised_fence(operation_id),
            context: context(),
        },
    )
    .expect("the fake provider must satisfy the frozen external write fence contract");

    // A different operation can never reuse another operation's fence receipt.
    let foreign = fence(FenceSpec {
        operation_id: ConnectorWriteOperationId::from_bytes([99; 16]),
        control_plane_incarnation: 1,
        resource_epoch: 9,
        coordination_attempt: 9,
        coordination_attempt_id: [9; 16],
    });
    let receipt = ConnectorExternalFenceReceipt::try_new(&foreign, marker_payload(&foreign))
        .expect("receipt");
    let error = ConnectorHistoricalWriteFence::established(&receipt, raised_fence(operation_id))
        .expect_err("a receipt from another operation must not establish this fence");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::ForeignOperation),
        "reusing another operation's fence receipt must be a typed foreign-operation conflict"
    );
}

// ---------------------------------------------------------------------------
// 9. Facet installation is separate from the ordinary write capability
// ---------------------------------------------------------------------------

#[test]
fn the_historical_facet_is_installed_separately_and_bound_to_its_own_generation() {
    let provider = FakeProvider::new();
    let binding = binding_with_both_facets(&provider);
    assert!(
        binding.write().is_some() && binding.historical_write_recovery().is_some(),
        "both facets must be installable on one control generation"
    );

    // An ordinary write capability alone must never expose the historical facet:
    // an ordinary execution path cannot reach it as a fallback.
    let write: Arc<dyn ConnectorWriteControl> = provider.clone();
    let ordinary_only = ConnectorControlBinding::try_new_with_capabilities(
        instance_descriptor(),
        ConnectorInstanceIncarnation::from_bytes(CURRENT_INCARNATION),
        provider.clone(),
        provider.clone(),
        provider.clone(),
        None,
        Some(write),
        None,
    )
    .expect("control binding with ordinary write capability");
    assert!(
        ordinary_only.historical_write_recovery().is_none(),
        "installing the ordinary write capability must not install historical recovery; the \
         historical facet must never be reachable as an ordinary fallback"
    );

    // The facet is bound to its own control generation.
    let foreign_generation = ConnectorInstanceIncarnation::from_bytes(HISTORICAL_INCARNATION);
    let recovery: Arc<dyn ConnectorHistoricalWriteRecovery> = provider.clone();
    let error = ConnectorControlBinding::try_new_with_capabilities(
        instance_descriptor(),
        foreign_generation,
        provider.clone(),
        provider.clone(),
        provider.clone(),
        None,
        None,
        None,
    )
    .expect("control binding")
    .try_with_historical_write_recovery(Some(recovery))
    .err();
    assert!(
        error.is_some(),
        "a historical recovery facet from another generation must not install on this binding"
    );
}
