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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_frontend::dml::journal::{
    DmlIntentAdmissionValidator, DmlMutationAuthority, DmlMutationAuthorityValidator,
    dml_operation_resource_key,
};
use novarocks_frontend::dml::model::{
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    DML_COORDINATION_RESOURCE_CODEC_VERSION, DML_CTAS_FACT_ENCODED_LIMIT,
    DML_CTAS_RECOVERY_CODEC_VERSION, DML_CTAS_RECOVERY_ENCODED_LIMIT,
    DML_CTAS_TOTAL_FACT_ENCODED_LIMIT, DML_DIRECT_MUTATION_FENCE_CODEC_VERSION,
    DML_EXTERNAL_FACT_ENCODED_LIMIT, DML_EXTERNAL_FENCE_CODEC_VERSION,
    DML_HISTORICAL_DATA_MUTATION_RECOVERY_CODEC_VERSION,
    DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION, DML_OPAQUE_PAYLOAD_LIMIT,
    DML_OPERATION_SCHEMA_VERSION, DML_RECOVERY_PAGE_SIZE,
};
use novarocks_frontend::dml::model::{
    DmlCoordinationClaimRequest, DmlCoordinationProvenance, DmlCtasActionKind,
    DmlCtasCatalogFenceRecord, DmlCtasChildSupersessionRecord, DmlCtasCleanupReceiptRecord,
    DmlCtasCleanupRetention, DmlCtasConflictKind, DmlCtasDispatchCertainty,
    DmlCtasDispatchCheckpointRecord, DmlCtasHistoricalDisposition,
    DmlCtasHistoricalObservationRecord, DmlCtasRecoveryMutationRequest, DmlCtasRecoveryRecord,
    DmlDirectMutationFenceMutationRequest, DmlDirectMutationFenceReceiptRecord,
    DmlDirectMutationKind, DmlExternalFenceGeneration, DmlExternalFenceIdentity,
    DmlExternalFenceMutationRequest, DmlExternalFenceReceiptRecord, DmlFencingTokenV1,
    DmlHistoricalCleanupState, DmlHistoricalDataMutationDisposition,
    DmlHistoricalDataMutationRecoveryMutationRequest, DmlHistoricalDataMutationRecoveryRecord,
    DmlHistoricalDataMutationRequestRecord, DmlHistoricalDataMutationResultRecord,
    DmlHistoricalDispatchCertainty, DmlHistoricalRecoveryPhase, DmlHistoricalWriteDisposition,
    DmlHistoricalWriteRecoveryMutationRequest, DmlHistoricalWriteRecoveryRecord,
    DmlHistoricalWriteRequestRecord, DmlHistoricalWriteResultRecord, DmlOpaquePayload,
    DmlRecoveryDueRescheduleRequest, validate_ctas_recovery, validate_ctas_recovery_transition,
};
use novarocks_frontend::dml::{
    AddFilesArtifact, AddFilesArtifactDescriptor, AddFilesArtifactKind, AddFilesDispatchCertainty,
    AddFilesLifecyclePhase, AddFilesLifecycleRecord, AddFilesMutationRequest, AddFilesSourceAction,
    ConnectorWriteFailureKind, ConnectorWriteFailureRecord, ConnectorWriteLifecycleRecord,
    CreatePreparingRequest, CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord,
    DmlError, DmlErrorKind, DmlOperationId, DurableExternalFact, DurableMutationSummary,
    ExternalFactOutcome, OperationFact, OperationJournal, OperationKind, OperationMutationRequest,
    OperationPayload, OperationState, OperationTarget, SourceScopeOwnership,
    StateStoreOperationJournal, StatementNextAction, StoredOperation, TruncateLifecyclePhase,
    TruncateLifecycleRecord,
};
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome as StateStoreCommitOutcome, CommitResolution, Key,
    Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, Value, WriteTransaction,
};
mod common;
use common::state_store_fixture;
use novarocks_frontend::OperationId;
use novarocks_frontend::StateStoreHost;
use novarocks_frontend::state_store::coordination::{
    AcquireOutcome, AttemptId, ClockHealth, ControlPlaneIncarnation, CoordinationError,
    CoordinationErrorKind, FencingToken, HolderId, IncarnationGate, LeaseClock, LeaseFence,
    LeaseGuard, LeaseManager, LeaseSettings, ResourceEpoch, WriteAdmission,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;
use uuid::{Uuid, Version};

const OPERATION_PREFIX: &str = "novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &str = "novarocks/frontend/dml/v1/unfinished/";
const CTAS_RECOVERY_PREFIX: &str = "novarocks/frontend/dml/v1/ctas-recoveries/";

#[allow(
    dead_code,
    reason = "Retained for state-store journal fixture construction."
)]
fn limits_with_max_value_bytes(max_value_bytes: Option<usize>) -> StateStoreLimits {
    let mut limits = StateStoreLimits::default();
    if let Some(max_value_bytes) = max_value_bytes {
        limits.max_value_bytes = max_value_bytes;
    }
    limits
}

async fn open_store(
    path: &std::path::Path,
) -> (
    StateStoreHost,
    Arc<dyn StateStore>,
    StateStoreOperationJournal,
) {
    open_store_with_max_value_bytes(path, None).await
}

async fn open_store_with_max_value_bytes(
    path: &std::path::Path,
    max_value_bytes: Option<usize>,
) -> (
    StateStoreHost,
    Arc<dyn StateStore>,
    StateStoreOperationJournal,
) {
    let cluster_id = format!("dml-journal-test-{}", path.display());
    let host = state_store_fixture::open_with_input(state_store_fixture::input_with_limits(
        cluster_id,
        limits_with_max_value_bytes(max_value_bytes),
    ))
    .await;
    let store = host.state_store().expect("StateStore exposure");
    let journal =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("open DML journal");
    (host, store, journal)
}

fn request() -> CreatePreparingRequest {
    CreatePreparingRequest {
        publication_id: DmlOperationId::new_v7(),
        operation_kind: OperationKind::InsertAppend,
        operation_subkind: None,
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: None,
        base_snapshot_map: BTreeMap::new(),
        staged_artifacts: Vec::new(),
        created_at_ms: 100,
    }
}

fn key(prefix: &str, operation_id: Uuid) -> Key {
    Key::try_from(Bytes::from(format!("{prefix}{}", operation_id.simple()))).unwrap()
}

async fn raw_put(store: &dyn StateStore, key: Key, value: Value) {
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "inject DML journal test record",
        )
        .await
        .unwrap();
    transaction
        .put(key, value, Precondition::Absent)
        .await
        .unwrap();
    assert!(matches!(
        transaction.commit().await,
        StateStoreCommitOutcome::Committed(_)
    ));
}

fn raw_operation(operation_id: Uuid, schema_version: u8) -> Value {
    raw_operation_with_kind(operation_id, schema_version, "INSERT_APPEND")
}

fn raw_operation_with_kind(operation_id: Uuid, schema_version: u8, operation_kind: &str) -> Value {
    let value = json!({
        "schema_version": schema_version,
        "operation_id": operation_id,
        "revision": 1,
        "last_mutation_id": Uuid::now_v7(),
        "operation_kind": operation_kind,
        "operation_subkind": null,
        "target": {
            "catalog": "cat",
            "namespace": "ns",
            "table": "tbl",
            "ref_name": null
        },
        "state": "PREPARING",
        "attempt_id": "attempt-1",
        "base_snapshot_id": null,
        "base_snapshot_map": {},
        "staged_artifacts": [],
        "payload": {"kind": "CONNECTOR_WRITE_LIFECYCLE", "details": {"outcome": "PENDING"}},
        "coordination_provenance": null,
        "recovery_due_at_ms": 1,
        "created_at_ms": 1,
        "updated_at_ms": 1,
        "finished_at_ms": null
    });
    Value::try_from(Bytes::from(serde_json::to_vec(&value).unwrap())).unwrap()
}

fn raw_unfinished(operation_id: Uuid) -> Value {
    let value = json!({
        "schema_version": 1,
        "operation_id": operation_id,
    });
    Value::try_from(Bytes::from(serde_json::to_vec(&value).unwrap())).unwrap()
}

#[derive(Default)]
struct TestTransactionValidator {
    calls: AtomicUsize,
}

impl TestTransactionValidator {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    #[expect(
        clippy::result_large_err,
        reason = "The journal test double preserves the production DML error contract."
    )]
    fn validate(&self) -> Result<(), DmlError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl DmlIntentAdmissionValidator for TestTransactionValidator {
    async fn validate_in(&self, _transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        self.validate()
    }
}

#[async_trait]
impl DmlMutationAuthorityValidator for TestTransactionValidator {
    async fn validate_in(&self, _transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        self.validate()
    }
}

struct ManualDmlLeaseClock {
    wall_ms: AtomicU64,
    monotonic_ms: AtomicU64,
    health: AtomicU8,
    wall_readable: AtomicBool,
}

impl ManualDmlLeaseClock {
    fn new(wall_ms: u64, monotonic_ms: u64) -> Self {
        Self {
            wall_ms: AtomicU64::new(wall_ms),
            monotonic_ms: AtomicU64::new(monotonic_ms),
            health: AtomicU8::new(0),
            wall_readable: AtomicBool::new(true),
        }
    }

    fn wall_ms(&self) -> u64 {
        self.wall_ms.load(Ordering::SeqCst)
    }

    fn advance_wall(&self, millis: u64) {
        self.wall_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(millis)
            })
            .expect("manual DML wall clock overflow");
    }

    fn advance_monotonic(&self, millis: u64) {
        self.monotonic_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(millis)
            })
            .expect("manual DML monotonic clock overflow");
    }

    fn set_health(&self, health: ClockHealth) {
        let encoded = match health {
            ClockHealth::Healthy => 0,
            ClockHealth::Unsafe => 1,
            ClockHealth::Unknown => 2,
        };
        self.health.store(encoded, Ordering::SeqCst);
    }
}

impl LeaseClock for ManualDmlLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        if !self.wall_readable.load(Ordering::SeqCst) {
            return Err(CoordinationError::clock_unsafe());
        }
        Ok(self.wall_ms())
    }

    fn monotonic_time_millis(&self) -> u64 {
        self.monotonic_ms.load(Ordering::SeqCst)
    }

    fn health(&self) -> ClockHealth {
        match self.health.load(Ordering::SeqCst) {
            0 => ClockHealth::Healthy,
            1 => ClockHealth::Unsafe,
            _ => ClockHealth::Unknown,
        }
    }
}

type CoordinationRejection = Arc<Mutex<Option<CoordinationErrorKind>>>;

fn dml_validator_rejection(
    kind: CoordinationErrorKind,
    observed: &CoordinationRejection,
) -> DmlError {
    *observed.lock().expect("coordination rejection lock") = Some(kind);
    match DmlMutationAuthority::try_new(Uuid::nil(), Arc::new(TestTransactionValidator::default()))
    {
        Err(error) => error,
        Ok(_) => panic!("nil UUID must not construct DML authority"),
    }
}

struct Cp1AdmissionValidator {
    admission: WriteAdmission,
    observed: CoordinationRejection,
}

#[async_trait]
impl DmlIntentAdmissionValidator for Cp1AdmissionValidator {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        self.admission
            .validate_in(transaction)
            .await
            .map_err(|error| dml_validator_rejection(error.kind(), &self.observed))
    }
}

enum Cp1FenceSource {
    Current(Arc<AsyncMutex<LeaseGuard>>),
    Static(LeaseFence),
}

struct Cp1AuthorityValidator {
    source: Cp1FenceSource,
    observed: CoordinationRejection,
}

#[async_trait]
impl DmlMutationAuthorityValidator for Cp1AuthorityValidator {
    async fn validate_in(&self, transaction: &mut dyn WriteTransaction) -> Result<(), DmlError> {
        let result = match &self.source {
            Cp1FenceSource::Current(guard) => {
                guard.lock().await.fence().validate_in(transaction).await
            }
            Cp1FenceSource::Static(fence) => fence.validate_in(transaction).await,
        };
        result.map_err(|error| dml_validator_rejection(error.kind(), &self.observed))
    }
}

fn lease_settings() -> LeaseSettings {
    LeaseSettings::new(
        Duration::from_secs(15),
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(2),
    )
    .unwrap()
}

fn holder_id(holder_uuid: Uuid) -> HolderId {
    HolderId::try_from(Bytes::from(format!(
        "novarocks/frontend/test-holder/v1/{holder_uuid}"
    )))
    .unwrap()
}

fn acquired(outcome: AcquireOutcome) -> LeaseGuard {
    match outcome {
        AcquireOutcome::Acquired(guard) => guard,
        AcquireOutcome::Contended(_) => panic!("expected acquired lease, found contention"),
        AcquireOutcome::AwaitingTakeover(_) => {
            panic!("expected acquired lease, found takeover observation")
        }
    }
}

fn current_authority(
    attempt_uuid: Uuid,
    guard: Arc<AsyncMutex<LeaseGuard>>,
    observed: CoordinationRejection,
) -> DmlMutationAuthority {
    DmlMutationAuthority::try_new(
        attempt_uuid,
        Arc::new(Cp1AuthorityValidator {
            source: Cp1FenceSource::Current(guard),
            observed,
        }),
    )
    .unwrap()
}

async fn real_provenance(
    holder_uuid: Uuid,
    attempt_uuid: Uuid,
    guard: &Arc<AsyncMutex<LeaseGuard>>,
    acquired_at_ms: i64,
) -> DmlCoordinationProvenance {
    let token = guard.lock().await.token().clone();
    DmlCoordinationProvenance {
        resource_codec_version: DML_COORDINATION_RESOURCE_CODEC_VERSION,
        holder_id: holder_uuid,
        coordination_attempt_id: attempt_uuid,
        fencing_token: DmlFencingTokenV1::try_from_token(&token).unwrap(),
        acquired_at_ms,
    }
}

fn coordination_provenance(
    holder_id: Uuid,
    coordination_attempt_id: Uuid,
    acquired_at_ms: i64,
) -> DmlCoordinationProvenance {
    let token = FencingToken::new(
        "dml-journal-test",
        ControlPlaneIncarnation::new(1).unwrap(),
        ResourceEpoch::new(1).unwrap(),
    )
    .unwrap();
    DmlCoordinationProvenance {
        resource_codec_version: DML_COORDINATION_RESOURCE_CODEC_VERSION,
        holder_id,
        coordination_attempt_id,
        fencing_token: DmlFencingTokenV1::try_from_token(&token).unwrap(),
        acquired_at_ms,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn admitted_claim_and_authority_validation_share_state_store_transactions() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission.clone())
        .unwrap();
    assert_eq!(admission.calls(), 1);

    let attempt = Uuid::now_v7();
    let claim_admission = Arc::new(TestTransactionValidator::default());
    let authority_validator = Arc::new(TestTransactionValidator::default());
    let authority = DmlMutationAuthority::try_new(attempt, authority_validator.clone()).unwrap();
    let claimed = journal
        .claim_operation_admitted(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: coordination_provenance(Uuid::now_v7(), attempt, 100),
                recovery_due_at_ms: 500,
            },
            claim_admission.clone(),
            authority,
        )
        .unwrap();
    assert_eq!(claimed.schema_version, DML_OPERATION_SCHEMA_VERSION);
    assert_eq!(DML_OPERATION_SCHEMA_VERSION, 8);
    assert_eq!(claimed.revision, 2);
    assert_eq!(claimed.recovery_due_at_ms, Some(500));
    assert_eq!(
        claimed
            .coordination_provenance
            .as_ref()
            .unwrap()
            .coordination_attempt_id,
        attempt
    );
    assert_eq!(authority_validator.calls(), 1);
    assert_eq!(claim_admission.calls(), 1);

    let stale_attempt = Uuid::now_v7();
    let stale_validator = Arc::new(TestTransactionValidator::default());
    let stale_authority =
        DmlMutationAuthority::try_new(stale_attempt, stale_validator.clone()).unwrap();
    let error = journal
        .reschedule_recovery_due(
            DmlRecoveryDueRescheduleRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                recovery_due_at_ms: Some(600),
            },
            stale_authority,
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnresolved);
    assert_eq!(stale_validator.calls(), 1);
    assert_eq!(journal.load(operation_id).unwrap(), Some(claimed));
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_due_index_is_sortable_fenced_and_removed_only_after_convergence() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();
    let shard = (0..journal.recovery_shard_count())
        .find(|shard| {
            journal
                .recovery_candidates(*shard, 18_100)
                .unwrap()
                .iter()
                .any(|candidate| candidate.operation_id == operation_id)
        })
        .unwrap();
    assert!(
        journal
            .recovery_candidates(shard, 18_099)
            .unwrap()
            .is_empty()
    );

    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let claimed = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: coordination_provenance(Uuid::now_v7(), attempt, 100),
                recovery_due_at_ms: 500,
            },
            authority(),
        )
        .unwrap();
    assert!(journal.recovery_candidates(shard, 499).unwrap().is_empty());
    let candidates = journal.recovery_candidates(shard, 500).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].operation_revision, claimed.revision);
    assert_eq!(candidates[0].last_mutation_id, claimed.last_mutation_id);
    assert_eq!(candidates[0].coordination_attempt_id, Some(attempt));

    let aborting = journal
        .transition_authorized(
            operation_id,
            claimed.revision,
            Uuid::now_v7(),
            OperationState::Aborting,
            Some(600),
            authority(),
        )
        .unwrap();
    let aborted = journal
        .transition_authorized(
            operation_id,
            aborting.revision,
            Uuid::now_v7(),
            OperationState::Aborted,
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(aborted.recovery_due_at_ms, None);
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty()
    );
    assert_eq!(validator.calls(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_due_candidate_page_is_bounded_to_128() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let target_shard = 0_u8;
    let mut operation_ids = Vec::new();
    while operation_ids.len() <= DML_RECOVERY_PAGE_SIZE {
        let operation_id = DmlOperationId::new_v7();
        if Sha256::digest(operation_id.as_uuid().as_bytes())[0] & 15 == target_shard {
            operation_ids.push(operation_id);
        }
    }
    for operation_id in operation_ids {
        journal
            .create_statement_operation_admitted(
                statement_request(
                    operation_id,
                    Uuid::now_v7(),
                    OperationKind::CreateTableAsSelect,
                    ctas_payload(CtasSagaPhase::PreparingSource),
                ),
                admission.clone(),
            )
            .unwrap();
    }
    let candidates = journal.recovery_candidates(target_shard, 18_200).unwrap();
    assert_eq!(candidates.len(), DML_RECOVERY_PAGE_SIZE);
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_cp1_dynamic_current_fence_survives_renew_and_release_fences_journal() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let gate = IncarnationGate::new(Arc::clone(&store));
    gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let admission_rejection = Arc::new(Mutex::new(None));
    let operation_id = journal
        .create_preparing_admitted(
            request(),
            Arc::new(Cp1AdmissionValidator {
                admission: gate.admit_writes().await.unwrap(),
                observed: Arc::clone(&admission_rejection),
            }),
        )
        .unwrap();
    assert_eq!(*admission_rejection.lock().unwrap(), None);

    let clock = Arc::new(ManualDmlLeaseClock::new(100_000, 10_000));
    let holder_a = Uuid::now_v7();
    let holder_b = Uuid::now_v7();
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_a),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let manager_b = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_b),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let resource = dml_operation_resource_key(operation_id).unwrap();
    let attempt_a_uuid = Uuid::now_v7();
    let attempt_a = AttemptId::try_from(attempt_a_uuid).unwrap();
    let guard = Arc::new(AsyncMutex::new(acquired(
        manager_a
            .acquire(resource.clone(), attempt_a, OperationId::new_v7())
            .await
            .unwrap(),
    )));
    let current_rejection = Arc::new(Mutex::new(None));
    let claimed = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(
                    holder_a,
                    attempt_a_uuid,
                    &guard,
                    clock.wall_ms() as i64,
                )
                .await,
                recovery_due_at_ms: 100_100,
            },
            current_authority(
                attempt_a_uuid,
                Arc::clone(&guard),
                Arc::clone(&current_rejection),
            ),
        )
        .unwrap();

    assert!(matches!(
        manager_b
            .acquire(
                resource.clone(),
                AttemptId::try_from(Uuid::now_v7()).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
        AcquireOutcome::Contended(_)
    ));

    let old_fence = guard.lock().await.fence();
    clock.advance_wall(5_000);
    guard
        .lock()
        .await
        .renew(OperationId::new_v7())
        .await
        .unwrap();

    let static_rejection = Arc::new(Mutex::new(None));
    let static_authority = DmlMutationAuthority::try_new(
        attempt_a_uuid,
        Arc::new(Cp1AuthorityValidator {
            source: Cp1FenceSource::Static(old_fence),
            observed: Arc::clone(&static_rejection),
        }),
    )
    .unwrap();
    let static_error = journal
        .transition_authorized(
            operation_id,
            claimed.revision,
            Uuid::now_v7(),
            OperationState::Writing,
            Some(105_100),
            static_authority,
        )
        .unwrap_err();
    assert_eq!(static_error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *static_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed);

    let writing = journal
        .transition_authorized(
            operation_id,
            claimed.revision,
            Uuid::now_v7(),
            OperationState::Writing,
            Some(105_100),
            current_authority(
                attempt_a_uuid,
                Arc::clone(&guard),
                Arc::clone(&current_rejection),
            ),
        )
        .unwrap();
    assert_eq!(*current_rejection.lock().unwrap(), None);

    let stale_revision = journal
        .transition_authorized(
            operation_id,
            claimed.revision,
            Uuid::now_v7(),
            OperationState::CommitUnknown,
            Some(105_200),
            current_authority(
                attempt_a_uuid,
                Arc::clone(&guard),
                Arc::new(Mutex::new(None)),
            ),
        )
        .unwrap_err();
    assert_eq!(stale_revision.kind(), DmlErrorKind::JournalUnresolved);

    let stale_attempt = journal
        .transition_authorized(
            operation_id,
            writing.revision,
            Uuid::now_v7(),
            OperationState::CommitUnknown,
            Some(105_200),
            current_authority(
                Uuid::now_v7(),
                Arc::clone(&guard),
                Arc::new(Mutex::new(None)),
            ),
        )
        .unwrap_err();
    assert_eq!(stale_attempt.kind(), DmlErrorKind::JournalUnresolved);

    guard
        .lock()
        .await
        .release(OperationId::new_v7())
        .await
        .unwrap();
    let released_rejection = Arc::new(Mutex::new(None));
    let released_error = journal
        .transition_authorized(
            operation_id,
            writing.revision,
            Uuid::now_v7(),
            OperationState::CommitUnknown,
            Some(105_200),
            current_authority(
                attempt_a_uuid,
                Arc::clone(&guard),
                Arc::clone(&released_rejection),
            ),
        )
        .unwrap_err();
    assert_eq!(released_error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *released_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), writing);
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_cp1_two_holder_takeover_and_restore_invalidate_old_journal_authority() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let gate = IncarnationGate::new(Arc::clone(&store));
    let open = gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let admission_rejection = Arc::new(Mutex::new(None));
    let admission = Arc::new(Cp1AdmissionValidator {
        admission: gate.admit_writes().await.unwrap(),
        observed: Arc::clone(&admission_rejection),
    });
    let operation_id = journal
        .create_preparing_admitted(request(), admission.clone())
        .unwrap();

    let clock = Arc::new(ManualDmlLeaseClock::new(500_000, 20_000));
    let holder_a = Uuid::now_v7();
    let holder_b = Uuid::now_v7();
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_a),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let manager_b = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_b),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let resource = dml_operation_resource_key(operation_id).unwrap();
    let attempt_a_uuid = Uuid::now_v7();
    let attempt_b_uuid = Uuid::now_v7();
    let guard_a = Arc::new(AsyncMutex::new(acquired(
        manager_a
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_a_uuid).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));
    let claimed_a = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(
                    holder_a,
                    attempt_a_uuid,
                    &guard_a,
                    clock.wall_ms() as i64,
                )
                .await,
                recovery_due_at_ms: 500_100,
            },
            current_authority(
                attempt_a_uuid,
                Arc::clone(&guard_a),
                Arc::new(Mutex::new(None)),
            ),
        )
        .unwrap();

    clock.advance_wall(16_001);
    assert!(matches!(
        manager_b
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_b_uuid).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(1_999);
    assert!(matches!(
        manager_b
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_b_uuid).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(1);
    let guard_b = Arc::new(AsyncMutex::new(acquired(
        manager_b
            .acquire(
                resource,
                AttemptId::try_from(attempt_b_uuid).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));

    let old_holder_rejection = Arc::new(Mutex::new(None));
    let old_holder_error = journal
        .transition_authorized(
            operation_id,
            claimed_a.revision,
            Uuid::now_v7(),
            OperationState::Writing,
            Some(516_100),
            current_authority(
                attempt_a_uuid,
                Arc::clone(&guard_a),
                Arc::clone(&old_holder_rejection),
            ),
        )
        .unwrap_err();
    assert_eq!(old_holder_error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *old_holder_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed_a);

    let claimed_b = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(
                    holder_b,
                    attempt_b_uuid,
                    &guard_b,
                    clock.wall_ms() as i64,
                )
                .await,
                recovery_due_at_ms: 516_100,
            },
            current_authority(
                attempt_b_uuid,
                Arc::clone(&guard_b),
                Arc::new(Mutex::new(None)),
            ),
        )
        .unwrap();
    assert_eq!(
        claimed_b
            .coordination_provenance
            .as_ref()
            .unwrap()
            .holder_id,
        holder_b
    );

    let restoring = gate
        .begin_restore(&open, OperationId::new_v7())
        .await
        .unwrap();
    let restore_rejection = Arc::new(Mutex::new(None));
    let restore_error = journal
        .transition_authorized(
            operation_id,
            claimed_b.revision,
            Uuid::now_v7(),
            OperationState::Writing,
            Some(516_200),
            current_authority(
                attempt_b_uuid,
                Arc::clone(&guard_b),
                Arc::clone(&restore_rejection),
            ),
        )
        .unwrap_err();
    assert_eq!(restore_error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *restore_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::IncarnationChanged)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed_b);

    let admission_error = journal
        .create_preparing_admitted(request(), admission)
        .unwrap_err();
    assert_eq!(admission_error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *admission_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::IncarnationChanged)
    );
    assert_eq!(journal.list_operations().unwrap().len(), 1);

    let release_error = guard_b
        .lock()
        .await
        .release(OperationId::new_v7())
        .await
        .unwrap_err();
    assert_eq!(
        release_error.kind(),
        CoordinationErrorKind::IncarnationChanged
    );
    gate.open_writes(&restoring, OperationId::new_v7())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_cp1_foreground_claim_revalidates_intent_admission_after_restore_starts() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let gate = IncarnationGate::new(Arc::clone(&store));
    let open = gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let admission_rejection = Arc::new(Mutex::new(None));
    let admission = Arc::new(Cp1AdmissionValidator {
        admission: gate.admit_writes().await.unwrap(),
        observed: Arc::clone(&admission_rejection),
    });
    let operation_id = journal
        .create_preparing_admitted(request(), admission.clone())
        .unwrap();
    let before_claim = journal.load(operation_id).unwrap().unwrap();

    gate.begin_restore(&open, OperationId::new_v7())
        .await
        .unwrap();
    let clock = Arc::new(ManualDmlLeaseClock::new(600_000, 30_000));
    let holder = Uuid::now_v7();
    let manager = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let attempt_uuid = Uuid::now_v7();
    let guard = Arc::new(AsyncMutex::new(acquired(
        manager
            .acquire(
                dml_operation_resource_key(operation_id).unwrap(),
                AttemptId::try_from(attempt_uuid).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));

    let error = journal
        .claim_operation_admitted(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: before_claim.revision,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder, attempt_uuid, &guard, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 600_100,
            },
            admission,
            current_authority(attempt_uuid, guard, Arc::new(Mutex::new(None))),
        )
        .unwrap_err();

    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *admission_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::IncarnationChanged)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), before_claim);
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_cp1_clock_unsafe_fails_renew_and_new_acquire_closed() {
    let temp = TempDir::new().unwrap();
    let (_host, store, _journal) = open_store(&temp.path().join("state.sqlite")).await;
    IncarnationGate::new(Arc::clone(&store))
        .bootstrap(OperationId::new_v7())
        .await
        .unwrap();
    let clock = Arc::new(ManualDmlLeaseClock::new(900_000, 30_000));
    let manager = LeaseManager::new(
        store,
        holder_id(Uuid::now_v7()),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let mut guard = acquired(
        manager
            .acquire(
                dml_operation_resource_key(DmlOperationId::new_v7()).unwrap(),
                AttemptId::try_from(Uuid::now_v7()).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    );
    clock.set_health(ClockHealth::Unsafe);
    assert_eq!(
        guard.renew(OperationId::new_v7()).await.unwrap_err().kind(),
        CoordinationErrorKind::ClockUnsafe
    );
    assert_eq!(
        manager
            .acquire(
                dml_operation_resource_key(DmlOperationId::new_v7()).unwrap(),
                AttemptId::try_from(Uuid::now_v7()).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap_err()
            .kind(),
        CoordinationErrorKind::ClockUnsafe
    );
}

#[derive(Clone, Copy)]
enum CommitUnknownMode {
    AfterCommit,
    BeforeCommit,
}

struct CommitUnknownStore {
    inner: Arc<dyn StateStore>,
    mode: CommitUnknownMode,
}

struct CommitUnknownTransaction {
    inner: Option<Box<dyn WriteTransaction>>,
    mode: CommitUnknownMode,
}

impl CommitUnknownTransaction {
    fn inner(&mut self) -> &mut dyn WriteTransaction {
        self.inner.as_deref_mut().expect("transaction is active")
    }
}

#[async_trait]
impl ReadTransaction for CommitUnknownTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner().get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.inner().range(request).await
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner
            .take()
            .expect("transaction is active")
            .abort()
            .await
    }
}

#[async_trait]
impl WriteTransaction for CommitUnknownTransaction {
    fn transaction_id(&self) -> &TransactionId {
        self.inner
            .as_deref()
            .expect("transaction is active")
            .transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner().put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner().delete(key, precondition).await
    }

    async fn commit(mut self: Box<Self>) -> StateStoreCommitOutcome {
        if matches!(self.mode, CommitUnknownMode::AfterCommit) {
            let outcome = self
                .inner
                .take()
                .expect("transaction is active")
                .commit()
                .await;
            if !matches!(outcome, StateStoreCommitOutcome::Committed(_)) {
                return outcome;
            }
        }
        StateStoreCommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "injected commit unknown",
        ))
    }
}

#[async_trait]
impl StateStore for CommitUnknownStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        Ok(Box::new(CommitUnknownTransaction {
            inner: Some(self.inner.begin_write(transaction_id, purpose).await?),
            mode: self.mode,
        }))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        self.inner.poll_changes(request).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        self.inner.resolve_commit(transaction_id).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn creates_uuid_v7_operation_and_unfinished_index_atomically() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    assert_eq!(
        operation_id.as_uuid().get_version(),
        Some(Version::SortRand)
    );

    let operation_key = key(OPERATION_PREFIX, *operation_id.as_uuid());
    let unfinished_key = key(UNFINISHED_PREFIX, *operation_id.as_uuid());
    let mut read = store.begin_read().await.unwrap();
    assert!(read.get(&operation_key).await.unwrap().is_some());
    assert!(read.get(&unfinished_key).await.unwrap().is_some());
    read.abort().await.unwrap();

    let stored = journal.load(operation_id).unwrap().unwrap();
    assert_ne!(*stored.operation_id.as_uuid(), stored.last_mutation_id);
    assert_eq!(stored.revision, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_transition_removes_unfinished_index() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Aborting)
        .unwrap();
    journal
        .transition(operation_id, OperationState::Aborted)
        .unwrap();

    assert!(journal.list_unfinished().unwrap().is_empty());
    let mut read = store.begin_read().await.unwrap();
    assert!(
        read.get(&key(UNFINISHED_PREFIX, *operation_id.as_uuid()))
            .await
            .unwrap()
            .is_none()
    );
    read.abort().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_loads_unfinished_operations() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (mut host, store, journal) = open_store(&path).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    drop(journal);
    drop(store);
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    let (_host, _store, reopened) = open_store(&path).await;
    let unfinished = reopened.list_unfinished().unwrap();
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].operation_id, operation_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_subkind_survives_state_store_restart() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (mut host, store, journal) = open_store(&path).await;
    let mut update = request();
    update.operation_kind = OperationKind::RowDelta;
    update.operation_subkind = Some("UPDATE".to_string());
    let operation_id = journal.create_preparing(update).unwrap();
    drop(journal);
    drop(store);
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    let (_host, _store, reopened) = open_store(&path).await;
    let stored = reopened.load(operation_id).unwrap().unwrap();
    assert_eq!(stored.operation_kind, OperationKind::RowDelta);
    assert_eq!(stored.operation_subkind.as_deref(), Some("UPDATE"));
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_abort_known_committed_removes_unfinished_index() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Aborting)
        .unwrap();
    journal
        .record_fact(
            operation_id,
            OperationFact {
                state: OperationState::Committed,
                lifecycle: ConnectorWriteLifecycleRecord::KnownEmpty,
            },
        )
        .unwrap();
    journal
        .transition(operation_id, OperationState::Finalizing)
        .unwrap();
    journal
        .transition(operation_id, OperationState::Finalized)
        .unwrap();

    assert!(journal.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_abort_known_uncommitted_removes_unfinished_index_after_restart() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (mut host, store, journal) = open_store(&path).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Aborting)
        .unwrap();
    journal
        .record_fact(
            operation_id,
            OperationFact {
                state: OperationState::FailedKnownUncommitted,
                lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                    failure: ConnectorWriteFailureRecord {
                        kind: ConnectorWriteFailureKind::Internal,
                        message: "connector outcome unavailable".to_string(),
                    },
                },
            },
        )
        .unwrap();
    drop(journal);
    drop(store);
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    let (_host, _store, reopened) = open_store(&path).await;
    assert!(reopened.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_fact_replay_is_idempotent() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Committing)
        .unwrap();
    let fact = OperationFact {
        state: OperationState::Committed,
        lifecycle: ConnectorWriteLifecycleRecord::KnownEmpty,
    };
    journal.record_fact(operation_id, fact.clone()).unwrap();
    journal.record_fact(operation_id, fact.clone()).unwrap();
    assert_eq!(
        journal.load(operation_id).unwrap().unwrap().payload,
        OperationPayload::ConnectorWriteLifecycle(fact.lifecycle),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicting_fact_replay_is_rejected() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Committing)
        .unwrap();
    let fact = OperationFact {
        state: OperationState::Committing,
        lifecycle: ConnectorWriteLifecycleRecord::Pending,
    };
    journal.record_fact(operation_id, fact).unwrap();
    let error = journal
        .record_fact(
            operation_id,
            OperationFact {
                state: OperationState::Committing,
                lifecycle: ConnectorWriteLifecycleRecord::KnownEmpty,
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.to_string().contains("conflicting"));
}

#[tokio::test(flavor = "multi_thread")]
async fn illegal_transition_is_rejected() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    let error = journal
        .transition(operation_id, OperationState::Finalized)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_schema_version_is_rejected_on_read_without_open_scan() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (_host, store, journal) = open_store(&path).await;
    drop(journal);
    let operation_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        raw_operation(operation_id, 99),
    )
    .await;
    let reopened =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("journal open must not scan operation records");
    let error = reopened
        .load(DmlOperationId::from(operation_id))
        .expect_err("unknown schema must fail when read");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
}

#[tokio::test(flavor = "multi_thread")]
async fn key_value_operation_identity_mismatch_is_corruption() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (_host, store, journal) = open_store(&path).await;
    drop(journal);
    let key_id = Uuid::now_v7();
    let value_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, key_id),
        raw_operation(value_id, DML_OPERATION_SCHEMA_VERSION),
    )
    .await;
    let reopened =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("journal open must not scan operation records");
    let error = reopened
        .load(DmlOperationId::from(key_id))
        .expect_err("identity mismatch must fail when read");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(error.to_string().contains("identity mismatch"));
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_unknown_matching_last_mutation_is_success() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    drop(journal);
    let wrapped: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: store,
        mode: CommitUnknownMode::AfterCommit,
    });
    let journal = StateStoreOperationJournal::open(wrapped, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let operation_id = journal.create_preparing(request()).unwrap();
    assert!(journal.load(operation_id).unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_unknown_without_matching_record_is_unresolved() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    drop(journal);
    let wrapped: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: Arc::clone(&store),
        mode: CommitUnknownMode::BeforeCommit,
    });
    let journal = StateStoreOperationJournal::open(wrapped, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let error = journal.create_preparing(request()).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnresolved);
    let clean = StateStoreOperationJournal::open(store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    assert!(clean.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_record_is_rejected_without_truncation() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let mut oversized = request();
    oversized.attempt_id = "x".repeat(70 * 1024);
    let error = journal.create_preparing(oversized).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(journal.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_preflight_uses_real_state_store_value_limit_at_exact_boundary() {
    let temp = TempDir::new().unwrap();
    let (_source_host, _source_store, source_journal) =
        open_store(&temp.path().join("source.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let mut operation = source_journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::Truncate,
            OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
                phase: TruncateLifecyclePhase::Preparing,
                connector_operation_id: Uuid::now_v7(),
                provider_id: Some("iceberg".to_string()),
                connector_instance_id: Some("iceberg-rest".to_string()),
                connector_incarnation: Some("09".repeat(16)),
                target_ref: "main".to_string(),
                request_digest: Some("request-digest".to_string()),
                plan_digest: Some("plan-digest".to_string()),
                state_digest: Some("state-digest".to_string()),
                plan_summary: Some(DurableMutationSummary {
                    file_count: 0,
                    row_count: 0,
                    total_bytes: 0,
                }),
                outcome: None,
                next_action: StatementNextAction::None,
            }),
        ))
        .unwrap();
    operation.state = OperationState::CommitUnknown;
    operation.payload = OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
        phase: TruncateLifecyclePhase::CommitUnknown,
        connector_operation_id: match &operation.payload {
            OperationPayload::TruncateLifecycle(record) => record.connector_operation_id,
            _ => unreachable!(),
        },
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some("09".repeat(16)),
        target_ref: "main".to_string(),
        request_digest: Some("request-digest".to_string()),
        plan_digest: Some("plan-digest".to_string()),
        state_digest: Some("state-digest".to_string()),
        plan_summary: Some(DurableMutationSummary {
            file_count: 0,
            row_count: 0,
            total_bytes: 0,
        }),
        outcome: Some(DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence: Some("ab".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT / 2)),
            finalization_failure: None,
            failure: Some(
                json!({
                    "version": 1,
                    "kind": "UNAVAILABLE",
                    "message_prefix": "x".repeat(2 * 1024),
                    "message_truncated": true,
                    "original_message_bytes": 128 * 1024,
                    "original_message_sha256": "cd".repeat(32),
                })
                .to_string(),
            ),
        }),
        next_action: StatementNextAction::Reconcile,
    });

    let encoded_len = serde_json::to_vec(&operation).unwrap().len();
    let (_exact_host, exact_store, exact_journal) =
        open_store_with_max_value_bytes(&temp.path().join("exact.sqlite"), Some(encoded_len)).await;
    assert_eq!(exact_store.limits().max_value_bytes, encoded_len);
    exact_journal
        .preflight_statement_operation(&operation)
        .unwrap();

    let (_short_host, short_store, short_journal) =
        open_store_with_max_value_bytes(&temp.path().join("short.sqlite"), Some(encoded_len - 1))
            .await;
    assert_eq!(short_store.limits().max_value_bytes, encoded_len - 1);
    let error = short_journal
        .preflight_statement_operation(&operation)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.to_string().contains(&format!(
        "encoded size {encoded_len} exceeds StateStore value limit {}",
        encoded_len - 1
    )));
}

fn ctas_payload(phase: CtasSagaPhase) -> OperationPayload {
    let committed_fact = || DurableExternalFact {
        outcome: ExternalFactOutcome::KnownCommitted,
        receipt: Some("receipt".to_string()),
        evidence: None,
        finalization_failure: None,
        failure: None,
    };
    let unknown_fact = || DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: Some("evidence".to_string()),
        finalization_failure: None,
        failure: Some("unknown".to_string()),
    };
    let (prepare_fact, write_fact, publish_fact, abort_staging_fact) = match phase {
        CtasSagaPhase::PreparingSource | CtasSagaPhase::PreparingStagedTable => {
            (None, None, None, None)
        }
        CtasSagaPhase::PrepareUnknown => (Some(unknown_fact()), None, None, None),
        CtasSagaPhase::Staged | CtasSagaPhase::Writing => {
            (Some(committed_fact()), None, None, None)
        }
        CtasSagaPhase::WriteUnknown => (Some(committed_fact()), Some(unknown_fact()), None, None),
        CtasSagaPhase::Publishing => (Some(committed_fact()), Some(committed_fact()), None, None),
        CtasSagaPhase::PublishUnknown => (
            Some(committed_fact()),
            Some(committed_fact()),
            Some(unknown_fact()),
            None,
        ),
        CtasSagaPhase::AbortingStaging => (Some(committed_fact()), None, None, None),
        CtasSagaPhase::AbortUnknown => (Some(committed_fact()), None, None, Some(unknown_fact())),
        CtasSagaPhase::Committed => (
            Some(committed_fact()),
            Some(committed_fact()),
            Some(committed_fact()),
            None,
        ),
        CtasSagaPhase::NoOp => (
            Some(committed_fact()),
            Some(committed_fact()),
            Some(DurableExternalFact {
                outcome: ExternalFactOutcome::NoOp,
                receipt: Some("receipt".to_string()),
                evidence: None,
                finalization_failure: None,
                failure: None,
            }),
            None,
        ),
        CtasSagaPhase::Conflict => (
            Some(committed_fact()),
            Some(committed_fact()),
            Some(DurableExternalFact {
                outcome: ExternalFactOutcome::Conflict,
                receipt: None,
                evidence: None,
                finalization_failure: None,
                failure: Some("conflict".to_string()),
            }),
            None,
        ),
        CtasSagaPhase::Failed | CtasSagaPhase::Unsupported => (None, None, None, None),
    };
    OperationPayload::CtasSaga(CtasSagaRecord {
        phase,
        prepare_operation_id: Uuid::now_v7(),
        write_operation_id: Uuid::now_v7(),
        publish_operation_id: Uuid::now_v7(),
        abort_staging_operation_id: Uuid::now_v7(),
        create_policy: CTAS_CREATE_POLICY_FAIL_IF_EXISTS.to_string(),
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some("07".repeat(16)),
        source_plan_digest: Some("source-digest".to_string()),
        source_schema_digest: Some("schema-digest".to_string()),
        source_execution_identity: Some("execution-identity".to_string()),
        write_cohort_id: Some("write-cohort".to_string()),
        staged_handle_digest: None,
        write_cohort_set_digest: None,
        aggregate_write_digest: None,
        prepare_fact,
        write_fact,
        publish_fact,
        abort_staging_fact,
        next_action: StatementNextAction::None,
    })
}

fn ctas_saga(payload: &OperationPayload) -> &CtasSagaRecord {
    let OperationPayload::CtasSaga(saga) = payload else {
        panic!("expected CTAS saga payload");
    };
    saga
}

fn ctas_recovery(recovery_attempt_id: Uuid, saga: &CtasSagaRecord) -> DmlCtasRecoveryRecord {
    DmlCtasRecoveryRecord {
        codec_version: DML_CTAS_RECOVERY_CODEC_VERSION,
        capability_version: 1,
        recovery_attempt_id,
        recovery_cycle: 1,
        catalog_fence_history: Vec::new(),
        catalog_fence: Some(DmlCtasCatalogFenceRecord {
            generation: DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 1,
                fence_generation: 1,
            },
            action_id: Uuid::now_v7(),
            request_digest: "10".repeat(32),
            dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
            dispatched_at_ms: Some(290),
            fence_digest: Some("11".repeat(32)),
            receipt_digest: Some("12".repeat(32)),
            receipt_payload: Some(
                DmlOpaquePayload::try_new(b"opaque catalog fence receipt".to_vec()).unwrap(),
            ),
            established_at_ms: Some(295),
        }),
        staged_target_digest: Some("11".repeat(32)),
        staged_locator: None,
        staged_locator_digest: None,
        staged_proof_digest: None,
        staged_proof: None,
        dispatch_checkpoints: vec![DmlCtasDispatchCheckpointRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: saga.prepare_operation_id,
            request_digest: "22".repeat(32),
            dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
            dispatched_at_ms: None,
        }],
        historical_observations: Vec::new(),
        child_supersessions: Vec::new(),
        cleanup_retention: DmlCtasCleanupRetention::NotRequired,
        cleanup_receipt: None,
        next_action: StatementNextAction::Reconcile,
        updated_at_ms: 300,
    }
}

fn claim_ctas(
    journal: &StateStoreOperationJournal,
    operation_id: DmlOperationId,
    payload: OperationPayload,
    attempt: Uuid,
    validator: Arc<TestTransactionValidator>,
) -> StoredOperation {
    journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::CreateTableAsSelect,
            payload,
        ))
        .unwrap();
    journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: coordination_provenance(Uuid::now_v7(), attempt, 200),
                recovery_due_at_ms: 500,
            },
            DmlMutationAuthority::try_new(attempt, validator).unwrap(),
        )
        .unwrap()
}

fn statement_request(
    operation_id: DmlOperationId,
    mutation_id: Uuid,
    operation_kind: OperationKind,
    payload: OperationPayload,
) -> CreateStatementOperationRequest {
    CreateStatementOperationRequest {
        operation_id,
        mutation_id,
        operation_kind,
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: Some("main".to_string()),
        },
        attempt_id: "attempt-statement".to_string(),
        payload,
        created_at_ms: 200,
    }
}

fn stored_statement_operation(
    operation_id: DmlOperationId,
    operation_kind: OperationKind,
    payload: OperationPayload,
) -> StoredOperation {
    StoredOperation {
        schema_version: DML_OPERATION_SCHEMA_VERSION,
        operation_id,
        revision: 1,
        last_mutation_id: Uuid::now_v7(),
        operation_kind,
        operation_subkind: None,
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: Some("main".to_string()),
        },
        state: OperationState::Preparing,
        attempt_id: "attempt-statement".to_string(),
        base_snapshot_id: None,
        base_snapshot_map: BTreeMap::new(),
        staged_artifacts: Vec::new(),
        payload,
        coordination_provenance: None,
        recovery_due_at_ms: Some(200),
        created_at_ms: 200,
        updated_at_ms: 200,
        finished_at_ms: None,
    }
}

fn add_files_artifact(kind: AddFilesArtifactKind, bytes: &[u8]) -> AddFilesArtifact {
    AddFilesArtifact {
        descriptor: AddFilesArtifactDescriptor {
            kind,
            codec_version: 1,
            total_length: u32::try_from(bytes.len()).unwrap(),
            chunk_count: u16::try_from(bytes.len().div_ceil(8 * 1024)).unwrap(),
            sha256: hex::encode(Sha256::digest(bytes)),
        },
        bytes: bytes.to_vec(),
    }
}

fn add_files_preparing() -> OperationPayload {
    OperationPayload::AddFilesLifecycle(AddFilesLifecycleRecord {
        phase: AddFilesLifecyclePhase::Preparing,
        connector_operation_id: Uuid::now_v7(),
        provider_id: None,
        connector_instance_id: None,
        connector_incarnation: None,
        source_location: "s3://warehouse/staged".to_string(),
        source_scope_version: None,
        source_scope_kind: None,
        source_scope_digest: None,
        request_digest: None,
        plan_digest: None,
        state_digest: None,
        plan_summary: None,
        plan_artifact: None,
        receipt_artifact: None,
        evidence_artifact: None,
        dispatch_certainty: AddFilesDispatchCertainty::ConfirmedNotDispatched,
        source_ownership: SourceScopeOwnership::Unclaimed,
        outcome: None,
        next_action: StatementNextAction::None,
    })
}

fn add_files_planned(plan: &AddFilesArtifactDescriptor, scope_digest: &str) -> OperationPayload {
    OperationPayload::AddFilesLifecycle(AddFilesLifecycleRecord {
        phase: AddFilesLifecyclePhase::Planned,
        connector_operation_id: Uuid::now_v7(),
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("rest".to_string()),
        connector_incarnation: Some("11".repeat(16)),
        source_location: "s3://warehouse/staged".to_string(),
        source_scope_version: Some(1),
        source_scope_kind: Some("DIRECTORY".to_string()),
        source_scope_digest: Some(scope_digest.to_string()),
        request_digest: Some("22".repeat(32)),
        plan_digest: Some("33".repeat(32)),
        state_digest: Some("44".repeat(32)),
        plan_summary: Some(DurableMutationSummary {
            file_count: 1,
            row_count: 1,
            total_bytes: 1,
        }),
        plan_artifact: Some(plan.clone()),
        receipt_artifact: None,
        evidence_artifact: None,
        dispatch_certainty: AddFilesDispatchCertainty::ConfirmedNotDispatched,
        source_ownership: SourceScopeOwnership::ReservedImmutable,
        outcome: None,
        next_action: StatementNextAction::None,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn add_files_restart_open_is_read_only_and_preserves_reservation() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    let scope_digest = "aa".repeat(32);
    let plan = add_files_artifact(AddFilesArtifactKind::Plan, b"durable public plan");

    let first_id = DmlOperationId::new_v7();
    let first = journal
        .create_statement_operation(statement_request(
            first_id,
            Uuid::now_v7(),
            OperationKind::AddFiles,
            add_files_preparing(),
        ))
        .unwrap();
    let first_reservation = AddFilesMutationRequest {
        operation: OperationMutationRequest {
            operation_id: first_id,
            expected_revision: first.revision,
            mutation_id: Uuid::now_v7(),
            state: OperationState::Committing,
            payload: add_files_planned(&plan.descriptor, &scope_digest),
        },
        artifacts: vec![plan.clone()],
        source_action: Some(AddFilesSourceAction::Reserve {
            provider_id: "iceberg".to_string(),
            scope_digest: scope_digest.clone(),
            ownership: SourceScopeOwnership::ReservedImmutable,
        }),
    };
    journal
        .preflight_add_files_mutation(&first_reservation)
        .unwrap();
    let first_planned = journal.apply_add_files_mutation(first_reservation).unwrap();
    assert_eq!(first_planned.state, OperationState::Committing);
    assert_eq!(
        journal
            .load_add_files_artifact(first_id, &plan.descriptor)
            .unwrap()
            .bytes,
        plan.bytes
    );

    let second_id = DmlOperationId::new_v7();
    let second = journal
        .create_statement_operation(statement_request(
            second_id,
            Uuid::now_v7(),
            OperationKind::AddFiles,
            add_files_preparing(),
        ))
        .unwrap();
    let conflict = journal
        .apply_add_files_mutation(AddFilesMutationRequest {
            operation: OperationMutationRequest {
                operation_id: second_id,
                expected_revision: second.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::Committing,
                payload: add_files_planned(&plan.descriptor, &scope_digest),
            },
            artifacts: vec![plan.clone()],
            source_action: Some(AddFilesSourceAction::Reserve {
                provider_id: "iceberg".to_string(),
                scope_digest: scope_digest.clone(),
                ownership: SourceScopeOwnership::ReservedImmutable,
            }),
        })
        .unwrap_err();
    assert_eq!(conflict.kind(), DmlErrorKind::JournalUnresolved);

    drop(journal);
    drop(store);
    drop(host);
    let (_host, _store, recovered) = open_store(&path).await;
    let recovered_first = recovered.load(first_id).unwrap().unwrap();
    assert_eq!(recovered_first, first_planned);
    assert!(
        recovered
            .list_unfinished()
            .unwrap()
            .iter()
            .any(|operation| operation.operation_id == first_id)
    );

    let second_after_recovery = recovered.load(second_id).unwrap().unwrap();
    assert_eq!(second_after_recovery, second);
    let third_id = DmlOperationId::new_v7();
    let third = recovered
        .create_statement_operation(statement_request(
            third_id,
            Uuid::now_v7(),
            OperationKind::AddFiles,
            add_files_preparing(),
        ))
        .unwrap();
    let conflict_after_restart = recovered
        .apply_add_files_mutation(AddFilesMutationRequest {
            operation: OperationMutationRequest {
                operation_id: third_id,
                expected_revision: third.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::Committing,
                payload: add_files_planned(&plan.descriptor, &scope_digest),
            },
            artifacts: vec![plan],
            source_action: Some(AddFilesSourceAction::Reserve {
                provider_id: "iceberg".to_string(),
                scope_digest,
                ownership: SourceScopeOwnership::ReservedImmutable,
            }),
        })
        .unwrap_err();
    assert_eq!(
        conflict_after_restart.kind(),
        DmlErrorKind::JournalUnresolved
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn prior_schema_record_is_rejected_without_compatibility_decode() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        raw_operation(operation_id, DML_OPERATION_SCHEMA_VERSION - 1),
    )
    .await;

    let error = journal
        .load(DmlOperationId::from(operation_id))
        .expect_err("prior schema must not be decoded");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_v2_mutation_is_revision_cas_and_idempotent() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let created = journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::CreateTableAsSelect,
            ctas_payload(CtasSagaPhase::PreparingSource),
        ))
        .unwrap();
    assert_eq!(created.revision, 1);

    let mutation_id = Uuid::now_v7();
    let mutation = OperationMutationRequest {
        operation_id,
        expected_revision: created.revision,
        mutation_id,
        state: OperationState::Writing,
        payload: ctas_payload(CtasSagaPhase::Staged),
    };
    let applied = journal
        .mutate_statement_operation(mutation.clone())
        .unwrap();
    assert_eq!(applied.revision, 2);
    assert_eq!(applied.last_mutation_id, mutation_id);
    assert_eq!(
        journal.mutate_statement_operation(mutation).unwrap(),
        applied
    );

    let stale = OperationMutationRequest {
        operation_id,
        expected_revision: 1,
        mutation_id: Uuid::now_v7(),
        state: OperationState::Collecting,
        payload: ctas_payload(CtasSagaPhase::Writing),
    };
    assert_eq!(
        journal
            .mutate_statement_operation(stale)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalUnresolved
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_v8_recovery_facts_are_authority_revision_bound_and_restart_durable() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    let operation_id = DmlOperationId::new_v7();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let recovery = ctas_recovery(attempt, ctas_saga(&payload));
    let mut reply_lost = recovery.clone();
    let stable_fence_action = reply_lost.catalog_fence.as_ref().unwrap().action_id;
    let fence = reply_lost.catalog_fence.as_mut().unwrap();
    fence.fence_digest = None;
    fence.receipt_digest = None;
    fence.receipt_payload = None;
    fence.established_at_ms = None;
    // Fence reply-loss may only retry/inspect the fence itself. No stage,
    // publish, abort, locator, or historical checkpoint exists yet.
    reply_lost.dispatch_checkpoints.clear();
    let claimed = claim_ctas(&journal, operation_id, payload, attempt, validator.clone());
    let mutation = DmlCtasRecoveryMutationRequest {
        operation_id,
        expected_revision: claimed.revision,
        mutation_id: Uuid::now_v7(),
        recovery: reply_lost.clone(),
    };
    journal.preflight_ctas_recovery(&mutation).unwrap();
    let reply_lost_recorded = journal
        .record_ctas_recovery_authorized(mutation.clone(), Some(600), authority())
        .unwrap();
    assert_eq!(reply_lost_recorded.revision, claimed.revision + 1);
    assert_eq!(
        journal
            .load_ctas_recovery(operation_id)
            .unwrap()
            .unwrap()
            .catalog_fence
            .unwrap()
            .action_id,
        stable_fence_action
    );
    assert_eq!(
        journal
            .record_ctas_recovery_authorized(mutation, Some(600), authority())
            .unwrap(),
        reply_lost_recorded
    );
    let recorded = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: reply_lost_recorded.revision,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(650),
            authority(),
        )
        .unwrap();
    assert_eq!(
        journal.load_ctas_recovery(operation_id).unwrap(),
        Some(recovery.clone())
    );

    let mut drifted_action = recovery.clone();
    drifted_action.catalog_fence.as_mut().unwrap().action_id = Uuid::now_v7();
    let action_error = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: recorded.revision,
                mutation_id: Uuid::now_v7(),
                recovery: drifted_action,
            },
            Some(675),
            authority(),
        )
        .unwrap_err();
    assert_eq!(action_error.kind(), DmlErrorKind::JournalCorruption);

    let expected_after_restart = recovery.clone();
    let stale = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: reply_lost_recorded.revision,
                mutation_id: Uuid::now_v7(),
                recovery,
            },
            Some(700),
            authority(),
        )
        .unwrap_err();
    assert_eq!(stale.kind(), DmlErrorKind::JournalUnresolved);

    drop(journal);
    drop(store);
    drop(host);
    let (_host, _store, reopened) = open_store(&path).await;
    assert_eq!(
        reopened.load_ctas_recovery(operation_id).unwrap(),
        Some(expected_after_restart)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_superseded_ctas_holder_cannot_persist_late_side_or_statement_writes() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let gate = IncarnationGate::new(Arc::clone(&store));
    gate.bootstrap(OperationId::new_v7()).await.unwrap();

    let operation_id = DmlOperationId::new_v7();
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    journal
        .create_statement_operation_admitted(
            statement_request(
                operation_id,
                Uuid::now_v7(),
                OperationKind::CreateTableAsSelect,
                payload.clone(),
            ),
            Arc::new(TestTransactionValidator::default()),
        )
        .unwrap();

    let clock = Arc::new(ManualDmlLeaseClock::new(700_000, 30_000));
    let holder_a = Uuid::now_v7();
    let holder_b = Uuid::now_v7();
    let attempt_a = Uuid::now_v7();
    let attempt_b = Uuid::now_v7();
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_a),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let manager_b = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_b),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let resource = dml_operation_resource_key(operation_id).unwrap();
    let guard_a = Arc::new(AsyncMutex::new(acquired(
        manager_a
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_a).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));
    let claimed_a = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder_a, attempt_a, &guard_a, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 700_100,
            },
            current_authority(attempt_a, Arc::clone(&guard_a), Arc::new(Mutex::new(None))),
        )
        .unwrap();

    // The provider reply represented by this recovery record arrives only
    // after holder B has taken the real SQLite-backed operation lease.
    let mut late_recovery = ctas_recovery(attempt_a, ctas_saga(&payload));
    late_recovery.catalog_fence.as_mut().unwrap().action_id = attempt_a;

    clock.advance_wall(16_001);
    assert!(matches!(
        manager_b
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_b).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(2_000);
    let guard_b = Arc::new(AsyncMutex::new(acquired(
        manager_b
            .acquire(
                resource,
                AttemptId::try_from(attempt_b).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));
    let claimed_b = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder_b, attempt_b, &guard_b, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 716_100,
            },
            current_authority(attempt_b, Arc::clone(&guard_b), Arc::new(Mutex::new(None))),
        )
        .unwrap();
    let mut current_recovery = ctas_recovery(attempt_b, ctas_saga(&payload));
    current_recovery.catalog_fence.as_mut().unwrap().action_id = attempt_b;
    let current = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed_b.revision,
                mutation_id: Uuid::now_v7(),
                recovery: current_recovery.clone(),
            },
            Some(716_200),
            current_authority(attempt_b, Arc::clone(&guard_b), Arc::new(Mutex::new(None))),
        )
        .unwrap();

    let side_rejection = Arc::new(Mutex::new(None));
    let error = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                recovery: late_recovery,
            },
            Some(716_300),
            current_authority(attempt_a, Arc::clone(&guard_a), Arc::clone(&side_rejection)),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *side_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );

    let statement_rejection = Arc::new(Mutex::new(None));
    let error = journal
        .mutate_statement_operation_authorized(
            OperationMutationRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::CommitUnknown,
                payload: payload.clone(),
            },
            Some(716_300),
            current_authority(
                attempt_a,
                Arc::clone(&guard_a),
                Arc::clone(&statement_rejection),
            ),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *statement_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap(), Some(current));
    assert_eq!(
        journal.load_ctas_recovery(operation_id).unwrap(),
        Some(current_recovery)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_ctas_keeps_recovery_due_until_proof_bound_cleanup_converges() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let mut recovery = ctas_recovery(attempt, ctas_saga(&payload));
    recovery.dispatch_checkpoints[0].dispatch_certainty =
        DmlCtasDispatchCertainty::PossiblyDispatched;
    recovery.dispatch_checkpoints[0].dispatched_at_ms = Some(301);
    let claimed = claim_ctas(&journal, operation_id, payload, attempt, validator.clone());
    let recorded = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(600),
            authority(),
        )
        .unwrap();

    let dropped = journal
        .transition_authorized(
            operation_id,
            recorded.revision,
            Uuid::now_v7(),
            OperationState::FailedKnownUncommitted,
            None,
            authority(),
        )
        .unwrap_err();
    assert_eq!(dropped.kind(), DmlErrorKind::JournalUnresolved);
    let terminal = journal
        .transition_authorized(
            operation_id,
            recorded.revision,
            Uuid::now_v7(),
            OperationState::FailedKnownUncommitted,
            Some(700),
            authority(),
        )
        .unwrap();
    assert!(terminal.state.is_finished());
    assert_eq!(terminal.recovery_due_at_ms, Some(700));

    let stage_child_operation_id = recovery.dispatch_checkpoints[0].child_operation_id;
    recovery.staged_locator =
        Some(DmlOpaquePayload::try_new(b"staged locator wire".to_vec()).unwrap());
    recovery.staged_locator_digest = Some("31".repeat(32));
    recovery.staged_proof_digest = Some("32".repeat(32));
    recovery.staged_proof = Some(DmlOpaquePayload::try_new(b"staged proof wire".to_vec()).unwrap());
    recovery
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: stage_child_operation_id,
            disposition: DmlCtasHistoricalDisposition::Staged,
            descriptor_digest: "30".repeat(32),
            descriptor_locator_digest: Some("31".repeat(32)),
            observation_digest: "33".repeat(32),
            locator_digest: Some("31".repeat(32)),
            proof_digest: Some("44".repeat(32)),
            proof_payload: Some(
                DmlOpaquePayload::try_new(b"opaque historical cleanup proof".to_vec()).unwrap(),
            ),
            conflict_kind: None,
            failure: None,
            observed_at_ms: 800,
        });
    recovery.cleanup_retention = DmlCtasCleanupRetention::Completed;
    recovery.cleanup_receipt = Some(DmlCtasCleanupReceiptRecord {
        descriptor_digest: "30".repeat(32),
        observation_digest: "33".repeat(32),
        locator_digest: "31".repeat(32),
        receipt_digest: "34".repeat(32),
        proof_digest: "35".repeat(32),
        proof_payload: DmlOpaquePayload::try_new(b"cleanup receipt proof wire".to_vec()).unwrap(),
        completed_at_ms: 800,
    });
    recovery.next_action = StatementNextAction::None;
    recovery.updated_at_ms = 800;
    let resolved = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: terminal.revision,
                mutation_id: Uuid::now_v7(),
                recovery,
            },
            None,
            authority(),
        )
        .unwrap();
    assert!(resolved.state.is_finished());
    assert_eq!(resolved.recovery_due_at_ms, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_child_supersession_is_append_only_and_checkpointed_to_the_new_child() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let saga = ctas_saga(&payload).clone();
    let claimed = claim_ctas(&journal, operation_id, payload, attempt, validator.clone());
    let successor = Uuid::now_v7();
    let mut recovery = ctas_recovery(attempt, &saga);
    let initial = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(550),
            authority(),
        )
        .unwrap();
    recovery.child_supersessions = vec![DmlCtasChildSupersessionRecord {
        action: DmlCtasActionKind::Stage,
        predecessor_child_operation_id: saga.prepare_operation_id,
        successor_child_operation_id: successor,
        reason_digest: "55".repeat(32),
        created_at_ms: 310,
    }];
    recovery
        .dispatch_checkpoints
        .push(DmlCtasDispatchCheckpointRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: successor,
            request_digest: "66".repeat(32),
            dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
            dispatched_at_ms: None,
        });
    let stored = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: initial.revision,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(600),
            authority(),
        )
        .unwrap();
    assert_eq!(
        journal.load_ctas_recovery(operation_id).unwrap(),
        Some(recovery.clone())
    );

    let mut crossed = recovery;
    crossed.dispatch_checkpoints[1].child_operation_id = Uuid::now_v7();
    let error = journal
        .record_ctas_recovery_authorized(
            DmlCtasRecoveryMutationRequest {
                operation_id,
                expected_revision: stored.revision,
                mutation_id: Uuid::now_v7(),
                recovery: crossed,
            },
            Some(700),
            authority(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_or_corrupt_ctas_recovery_is_never_decoded_as_absent_target_truth() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let missing = DmlOperationId::new_v7();
    assert_eq!(journal.load_ctas_recovery(missing).unwrap(), None);

    let corrupt = DmlOperationId::new_v7();
    raw_put(
        store.as_ref(),
        key(CTAS_RECOVERY_PREFIX, *corrupt.as_uuid()),
        Value::try_from(Bytes::from_static(b"{\"codec_version\":1}")).unwrap(),
    )
    .await;
    let error = journal
        .load_ctas_recovery(corrupt)
        .expect_err("corrupt CTAS facts must fail instead of implying NOT_CREATED");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
}

#[test]
fn ctas_recovery_preflight_enforces_opaque_and_total_bounds() {
    const { assert!(DML_CTAS_RECOVERY_ENCODED_LIMIT > DML_OPAQUE_PAYLOAD_LIMIT) };
    let temp = TempDir::new().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
        let payload = ctas_payload(CtasSagaPhase::PreparingSource);
        let mut recovery = ctas_recovery(Uuid::now_v7(), ctas_saga(&payload));
        assert!(DmlOpaquePayload::try_new(vec![b'x'; DML_OPAQUE_PAYLOAD_LIMIT + 1]).is_err());
        recovery.child_supersessions = (0..600)
            .map(|_| DmlCtasChildSupersessionRecord {
                action: DmlCtasActionKind::Stage,
                predecessor_child_operation_id: Uuid::now_v7(),
                successor_child_operation_id: Uuid::now_v7(),
                reason_digest: "77".repeat(32),
                created_at_ms: 400,
            })
            .collect();
        let error = journal
            .preflight_ctas_recovery(&DmlCtasRecoveryMutationRequest {
                operation_id: DmlOperationId::new_v7(),
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                recovery,
            })
            .unwrap_err();
        assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    });
}

#[test]
fn ctas_opaque_receipt_locator_and_cleanup_proof_are_redacted_from_debug() {
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let mut recovery = ctas_recovery(Uuid::now_v7(), ctas_saga(&payload));
    recovery.staged_locator =
        Some(DmlOpaquePayload::try_new(b"secret staged locator".to_vec()).unwrap());
    recovery.staged_locator_digest = Some("87".repeat(32));
    recovery.staged_proof_digest = Some("88".repeat(32));
    recovery.staged_proof =
        Some(DmlOpaquePayload::try_new(b"secret staged proof wire".to_vec()).unwrap());
    recovery
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: recovery.dispatch_checkpoints[0].child_operation_id,
            disposition: DmlCtasHistoricalDisposition::Staged,
            descriptor_digest: "98".repeat(32),
            descriptor_locator_digest: Some("87".repeat(32)),
            observation_digest: "99".repeat(32),
            locator_digest: Some("87".repeat(32)),
            proof_digest: Some("aa".repeat(32)),
            proof_payload: Some(
                DmlOpaquePayload::try_new(b"secret guarded cleanup proof".to_vec()).unwrap(),
            ),
            conflict_kind: None,
            failure: None,
            observed_at_ms: 900,
        });
    let debug = format!("{recovery:?}");
    assert!(!debug.contains("secret staged locator"));
    assert!(!debug.contains("secret staged proof wire"));
    assert!(!debug.contains("secret guarded cleanup proof"));
    assert!(!debug.contains("opaque catalog fence receipt"));
    assert!(debug.contains("DmlOpaquePayload"));
}

#[test]
fn ctas_historical_ambiguous_proof_is_optional_but_unsupported_never_has_one() {
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let mut recovery = ctas_recovery(Uuid::now_v7(), ctas_saga(&payload));
    let child_operation_id = recovery.dispatch_checkpoints[0].child_operation_id;
    recovery
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id,
            disposition: DmlCtasHistoricalDisposition::Ambiguous,
            descriptor_digest: "aa".repeat(32),
            descriptor_locator_digest: None,
            observation_digest: "ab".repeat(32),
            locator_digest: None,
            proof_digest: None,
            proof_payload: None,
            conflict_kind: None,
            failure: Some("catalog inspection reply was lost".to_string()),
            observed_at_ms: 901,
        });
    validate_ctas_recovery(&recovery).unwrap();

    let observation = recovery.historical_observations.last_mut().unwrap();
    observation.disposition = DmlCtasHistoricalDisposition::Unsupported;
    observation.proof_digest = Some("cd".repeat(32));
    observation.proof_payload =
        Some(DmlOpaquePayload::try_new(b"forbidden proof".to_vec()).unwrap());
    assert!(validate_ctas_recovery(&recovery).is_err());
}

#[test]
fn ctas_advance_fence_observation_remains_bound_after_takeover_archives_its_fence() {
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let mut previous = ctas_recovery(Uuid::now_v7(), ctas_saga(&payload));
    let previous_fence = previous.catalog_fence.as_ref().unwrap().clone();
    previous
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::AdvanceFence,
            child_operation_id: previous_fence.action_id,
            disposition: DmlCtasHistoricalDisposition::Ambiguous,
            descriptor_digest: "31".repeat(32),
            descriptor_locator_digest: None,
            observation_digest: "32".repeat(32),
            locator_digest: None,
            proof_digest: None,
            proof_payload: None,
            conflict_kind: None,
            failure: Some("catalog inspection reply was lost".to_string()),
            observed_at_ms: 905,
        });
    validate_ctas_recovery(&previous).unwrap();

    let mut next = previous.clone();
    let next_attempt = Uuid::now_v7();
    next.recovery_attempt_id = next_attempt;
    next.recovery_cycle += 1;
    next.catalog_fence_history.push(previous_fence.clone());
    let current = next.catalog_fence.as_mut().unwrap();
    current.generation.fence_generation += 1;
    current.action_id = next_attempt;
    current.request_digest = "33".repeat(32);
    current.fence_digest = Some("34".repeat(32));
    current.receipt_digest = Some("35".repeat(32));
    current.receipt_payload =
        Some(DmlOpaquePayload::try_new(b"next catalog fence receipt".to_vec()).unwrap());
    current.dispatched_at_ms = Some(910);
    current.established_at_ms = Some(911);
    next.updated_at_ms = 912;

    validate_ctas_recovery_transition(Some(&previous), &next).unwrap();
    validate_ctas_recovery(&next).unwrap();

    next.catalog_fence_history.clear();
    assert!(validate_ctas_recovery(&next).is_err());
}

#[test]
fn ctas_catalog_fence_conflict_and_cleanup_facts_fail_closed() {
    let payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let saga = ctas_saga(&payload);

    let mut incomplete_fence = ctas_recovery(Uuid::now_v7(), saga);
    let fence = incomplete_fence.catalog_fence.as_mut().unwrap();
    fence.fence_digest = None;
    fence.receipt_digest = None;
    fence.receipt_payload = None;
    fence.established_at_ms = None;
    assert!(validate_ctas_recovery(&incomplete_fence).is_err());
    incomplete_fence.dispatch_checkpoints.clear();
    validate_ctas_recovery(&incomplete_fence).unwrap();

    let mut conflict = ctas_recovery(Uuid::now_v7(), saga);
    conflict
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: conflict.dispatch_checkpoints[0].child_operation_id,
            disposition: DmlCtasHistoricalDisposition::Conflict,
            descriptor_digest: "41".repeat(32),
            descriptor_locator_digest: None,
            observation_digest: "42".repeat(32),
            locator_digest: None,
            proof_digest: Some("43".repeat(32)),
            proof_payload: Some(DmlOpaquePayload::try_new(b"conflict proof".to_vec()).unwrap()),
            conflict_kind: None,
            failure: Some("catalog digest conflict".to_string()),
            observed_at_ms: 902,
        });
    assert!(validate_ctas_recovery(&conflict).is_err());
    conflict.historical_observations[0].conflict_kind = Some(DmlCtasConflictKind::DigestConflict);
    validate_ctas_recovery(&conflict).unwrap();

    let mut cleanup = ctas_recovery(Uuid::now_v7(), saga);
    cleanup.staged_locator = Some(DmlOpaquePayload::try_new(b"locator wire".to_vec()).unwrap());
    cleanup.staged_locator_digest = Some("51".repeat(32));
    cleanup.staged_proof_digest = Some("52".repeat(32));
    cleanup.staged_proof = Some(DmlOpaquePayload::try_new(b"stage proof wire".to_vec()).unwrap());
    cleanup
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: cleanup.dispatch_checkpoints[0].child_operation_id,
            disposition: DmlCtasHistoricalDisposition::Ambiguous,
            descriptor_digest: "53".repeat(32),
            descriptor_locator_digest: Some("51".repeat(32)),
            observation_digest: "50".repeat(32),
            locator_digest: None,
            proof_digest: None,
            proof_payload: None,
            conflict_kind: None,
            failure: Some("inspection reply lost".to_string()),
            observed_at_ms: 902,
        });
    cleanup
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Stage,
            child_operation_id: cleanup.dispatch_checkpoints[0].child_operation_id,
            disposition: DmlCtasHistoricalDisposition::Staged,
            descriptor_digest: "53".repeat(32),
            descriptor_locator_digest: Some("51".repeat(32)),
            observation_digest: "54".repeat(32),
            locator_digest: Some("51".repeat(32)),
            proof_digest: Some("55".repeat(32)),
            proof_payload: Some(
                DmlOpaquePayload::try_new(b"inspection proof wire".to_vec()).unwrap(),
            ),
            conflict_kind: None,
            failure: None,
            observed_at_ms: 903,
        });
    cleanup.cleanup_retention = DmlCtasCleanupRetention::Pending;
    validate_ctas_recovery(&cleanup).unwrap();
    cleanup.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
    assert!(validate_ctas_recovery(&cleanup).is_err());
    cleanup.cleanup_retention = DmlCtasCleanupRetention::Completed;
    cleanup.next_action = StatementNextAction::None;
    assert!(validate_ctas_recovery(&cleanup).is_err());
    cleanup.cleanup_receipt = Some(DmlCtasCleanupReceiptRecord {
        descriptor_digest: "53".repeat(32),
        observation_digest: "54".repeat(32),
        locator_digest: "51".repeat(32),
        receipt_digest: "56".repeat(32),
        proof_digest: "57".repeat(32),
        proof_payload: DmlOpaquePayload::try_new(b"cleanup proof wire".to_vec()).unwrap(),
        completed_at_ms: 904,
    });
    validate_ctas_recovery(&cleanup).unwrap();
    cleanup.cleanup_receipt.as_mut().unwrap().locator_digest = "58".repeat(32);
    assert!(validate_ctas_recovery(&cleanup).is_err());

    let mut published = cleanup;
    published.cleanup_receipt.as_mut().unwrap().locator_digest = "51".repeat(32);
    published.historical_observations[1].disposition = DmlCtasHistoricalDisposition::Published;
    published.historical_observations[1].locator_digest = None;
    assert!(validate_ctas_recovery(&published).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_reconcile_transitions_are_statement_specific() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;

    for (resolved_state, resolved_phase) in [
        (OperationState::Writing, CtasSagaPhase::Staged),
        (OperationState::Committing, CtasSagaPhase::Publishing),
    ] {
        let operation_id = DmlOperationId::new_v7();
        let created = journal
            .create_statement_operation(statement_request(
                operation_id,
                Uuid::now_v7(),
                OperationKind::CreateTableAsSelect,
                ctas_payload(CtasSagaPhase::PreparingSource),
            ))
            .unwrap();
        let unknown = journal
            .mutate_statement_operation(OperationMutationRequest {
                operation_id,
                expected_revision: created.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::CommitUnknown,
                payload: ctas_payload(CtasSagaPhase::PrepareUnknown),
            })
            .unwrap();
        journal
            .mutate_statement_operation(OperationMutationRequest {
                operation_id,
                expected_revision: unknown.revision,
                mutation_id: Uuid::now_v7(),
                state: resolved_state,
                payload: ctas_payload(resolved_phase),
            })
            .unwrap();
    }

    let operation_id = DmlOperationId::new_v7();
    let created = journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::Truncate,
            OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
                phase: TruncateLifecyclePhase::Preparing,
                connector_operation_id: Uuid::now_v7(),
                provider_id: None,
                connector_instance_id: None,
                connector_incarnation: None,
                target_ref: "main".to_string(),
                request_digest: None,
                plan_digest: None,
                state_digest: None,
                plan_summary: None,
                outcome: None,
                next_action: StatementNextAction::None,
            }),
        ))
        .unwrap();
    let error = journal
        .mutate_statement_operation(OperationMutationRequest {
            operation_id,
            expected_revision: created.revision,
            mutation_id: Uuid::now_v7(),
            state: OperationState::CommitUnknown,
            payload: created.payload,
        })
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(
        error
            .to_string()
            .contains("invalid DML operation state transition")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_create_replay_is_idempotent_and_conflict_is_unresolved() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let request = statement_request(
        operation_id,
        Uuid::now_v7(),
        OperationKind::CreateTableAsSelect,
        ctas_payload(CtasSagaPhase::PreparingSource),
    );
    let created = journal.create_statement_operation(request.clone()).unwrap();
    assert_eq!(
        journal.create_statement_operation(request).unwrap(),
        created
    );

    let conflict = statement_request(
        operation_id,
        Uuid::now_v7(),
        OperationKind::CreateTableAsSelect,
        ctas_payload(CtasSagaPhase::PreparingSource),
    );
    assert_eq!(
        journal
            .create_statement_operation(conflict)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalUnresolved
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_commit_unknown_is_resolved_by_authoritative_mutation_id() {
    let temp = TempDir::new().unwrap();
    let (_host, store, direct) = open_store(&temp.path().join("state.sqlite")).await;
    drop(direct);
    let wrapped: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: Arc::clone(&store),
        mode: CommitUnknownMode::AfterCommit,
    });
    let journal = StateStoreOperationJournal::open(wrapped, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let operation_id = DmlOperationId::new_v7();
    let create_mutation_id = Uuid::now_v7();
    let created = journal
        .create_statement_operation(statement_request(
            operation_id,
            create_mutation_id,
            OperationKind::CreateTableAsSelect,
            ctas_payload(CtasSagaPhase::PreparingSource),
        ))
        .unwrap();
    assert_eq!(created.last_mutation_id, create_mutation_id);

    let mutation_id = Uuid::now_v7();
    let applied = journal
        .mutate_statement_operation(OperationMutationRequest {
            operation_id,
            expected_revision: created.revision,
            mutation_id,
            state: OperationState::Writing,
            payload: ctas_payload(CtasSagaPhase::Staged),
        })
        .unwrap();
    assert_eq!(applied.revision, 2);
    assert_eq!(applied.last_mutation_id, mutation_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_create_unknown_without_authoritative_record_is_unresolved() {
    let temp = TempDir::new().unwrap();
    let (_host, store, direct) = open_store(&temp.path().join("state.sqlite")).await;
    drop(direct);
    let wrapped: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: Arc::clone(&store),
        mode: CommitUnknownMode::BeforeCommit,
    });
    let journal = StateStoreOperationJournal::open(wrapped, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let error = journal
        .create_statement_operation(statement_request(
            DmlOperationId::new_v7(),
            Uuid::now_v7(),
            OperationKind::Truncate,
            OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
                phase: TruncateLifecyclePhase::Preparing,
                connector_operation_id: Uuid::now_v7(),
                provider_id: None,
                connector_instance_id: None,
                connector_incarnation: None,
                target_ref: "main".to_string(),
                request_digest: None,
                plan_digest: None,
                state_digest: None,
                plan_summary: None,
                outcome: None,
                next_action: StatementNextAction::None,
            }),
        ))
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnresolved);
    let clean = StateStoreOperationJournal::open(store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    assert!(clean.list_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_v2_payload_round_trips_and_terminal_removes_index() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let payload = OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
        phase: TruncateLifecyclePhase::Preparing,
        connector_operation_id: Uuid::now_v7(),
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some("09".repeat(16)),
        target_ref: "main".to_string(),
        request_digest: Some("request".to_string()),
        plan_digest: None,
        state_digest: None,
        plan_summary: Some(DurableMutationSummary {
            file_count: 0,
            row_count: 0,
            total_bytes: 0,
        }),
        outcome: None,
        next_action: StatementNextAction::None,
    });
    let created = journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::Truncate,
            payload,
        ))
        .unwrap();
    let terminal_payload = OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
        phase: TruncateLifecyclePhase::Failed,
        connector_operation_id: match &created.payload {
            OperationPayload::TruncateLifecycle(record) => record.connector_operation_id,
            _ => unreachable!(),
        },
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some("09".repeat(16)),
        target_ref: "main".to_string(),
        request_digest: Some("request".to_string()),
        plan_digest: None,
        state_digest: None,
        plan_summary: Some(DurableMutationSummary {
            file_count: 0,
            row_count: 0,
            total_bytes: 0,
        }),
        outcome: Some(DurableExternalFact {
            outcome: ExternalFactOutcome::KnownUncommitted,
            receipt: None,
            evidence: None,
            finalization_failure: None,
            failure: Some("planned failure".to_string()),
        }),
        next_action: StatementNextAction::None,
    });
    let terminal = journal
        .mutate_statement_operation(OperationMutationRequest {
            operation_id,
            expected_revision: 1,
            mutation_id: Uuid::now_v7(),
            state: OperationState::FailedKnownUncommitted,
            payload: terminal_payload.clone(),
        })
        .unwrap();
    assert_eq!(terminal.payload, terminal_payload);
    assert!(journal.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_external_fact_budget_is_enforced_before_create() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let mut payload = ctas_payload(CtasSagaPhase::PreparingStagedTable);
    let OperationPayload::CtasSaga(record) = &mut payload else {
        unreachable!();
    };
    record.prepare_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: Some("e".repeat(DML_CTAS_FACT_ENCODED_LIMIT + 1)),
        finalization_failure: None,
        failure: Some("unknown".to_string()),
    });
    let error = journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::CreateTableAsSelect,
            payload,
        ))
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(journal.list_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_ctas_fact_below_encoded_limit_is_preserved() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let evidence = "e".repeat(DML_CTAS_FACT_ENCODED_LIMIT - 512);
    let mut payload = ctas_payload(CtasSagaPhase::PreparingStagedTable);
    let OperationPayload::CtasSaga(record) = &mut payload else {
        unreachable!();
    };
    record.prepare_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: Some(evidence.clone()),
        finalization_failure: None,
        failure: Some("unknown".to_string()),
    });
    let stored = journal
        .create_statement_operation(statement_request(
            operation_id,
            Uuid::now_v7(),
            OperationKind::CreateTableAsSelect,
            payload,
        ))
        .unwrap();
    let OperationPayload::CtasSaga(record) = stored.payload else {
        unreachable!();
    };
    assert_eq!(record.prepare_fact.unwrap().evidence.unwrap(), evidence);
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_shape_rejects_invalid_child_ids_and_phase_fact_mismatch() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;

    let mut nil_payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let OperationPayload::CtasSaga(record) = &mut nil_payload else {
        unreachable!();
    };
    record.prepare_operation_id = Uuid::nil();
    let nil = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        nil_payload,
    );
    assert_eq!(
        journal
            .preflight_statement_operation(&nil)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalCorruption
    );

    let mut duplicate_payload = ctas_payload(CtasSagaPhase::PreparingSource);
    let OperationPayload::CtasSaga(record) = &mut duplicate_payload else {
        unreachable!();
    };
    record.write_operation_id = record.prepare_operation_id;
    let duplicate = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        duplicate_payload,
    );
    assert_eq!(
        journal
            .preflight_statement_operation(&duplicate)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalCorruption
    );

    let mut mismatched_payload = ctas_payload(CtasSagaPhase::WriteUnknown);
    let OperationPayload::CtasSaga(record) = &mut mismatched_payload else {
        unreachable!();
    };
    record.write_fact = None;
    let mismatched = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        mismatched_payload,
    );
    assert_eq!(
        journal
            .preflight_statement_operation(&mismatched)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalCorruption
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_unknown_without_provider_evidence_requires_manual_inspect() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let mut payload = ctas_payload(CtasSagaPhase::PrepareUnknown);
    let OperationPayload::CtasSaga(record) = &mut payload else {
        unreachable!();
    };
    let fact = record.prepare_fact.as_mut().unwrap();
    fact.evidence = None;
    fact.failure = Some("possibly dispatched contract failure".to_string());
    record.next_action = StatementNextAction::Reconcile;
    let operation = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        payload.clone(),
    );
    assert_eq!(
        journal
            .preflight_statement_operation(&operation)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalCorruption
    );

    let OperationPayload::CtasSaga(record) = &mut payload else {
        unreachable!();
    };
    record.next_action = StatementNextAction::ManualInspect;
    let manual = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        payload,
    );
    journal.preflight_statement_operation(&manual).unwrap();

    let mut evidenced_payload = ctas_payload(CtasSagaPhase::PrepareUnknown);
    let OperationPayload::CtasSaga(record) = &mut evidenced_payload else {
        unreachable!();
    };
    record.next_action = StatementNextAction::Reconcile;
    let evidenced = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        evidenced_payload,
    );
    journal.preflight_statement_operation(&evidenced).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_prepare_conflict_respects_canonical_create_policy() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let conflict_fact = DurableExternalFact {
        outcome: ExternalFactOutcome::Conflict,
        receipt: None,
        evidence: None,
        finalization_failure: None,
        failure: Some("target already exists".to_string()),
    };

    let mut no_op_payload = ctas_payload(CtasSagaPhase::NoOp);
    let OperationPayload::CtasSaga(record) = &mut no_op_payload else {
        unreachable!();
    };
    record.create_policy = CTAS_CREATE_POLICY_NO_OP_IF_EXISTS.to_string();
    record.prepare_fact = Some(conflict_fact.clone());
    record.publish_fact = None;
    let no_op = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        no_op_payload.clone(),
    );
    journal.preflight_statement_operation(&no_op).unwrap();

    let OperationPayload::CtasSaga(record) = &mut no_op_payload else {
        unreachable!();
    };
    record.create_policy = CTAS_CREATE_POLICY_FAIL_IF_EXISTS.to_string();
    let strict_no_op = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        no_op_payload,
    );
    assert_eq!(
        journal
            .preflight_statement_operation(&strict_no_op)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalCorruption
    );

    let mut conflict_payload = ctas_payload(CtasSagaPhase::Conflict);
    let OperationPayload::CtasSaga(record) = &mut conflict_payload else {
        unreachable!();
    };
    record.prepare_fact = Some(conflict_fact);
    record.publish_fact = None;
    let strict_conflict = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        conflict_payload.clone(),
    );
    journal
        .preflight_statement_operation(&strict_conflict)
        .unwrap();

    let OperationPayload::CtasSaga(record) = &mut conflict_payload else {
        unreachable!();
    };
    record.create_policy = "BEST_EFFORT".to_string();
    let malformed = stored_statement_operation(
        DmlOperationId::new_v7(),
        OperationKind::CreateTableAsSelect,
        conflict_payload,
    );
    assert_eq!(
        journal
            .preflight_statement_operation(&malformed)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalCorruption
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_four_maximum_facts_fit_one_real_state_store_record() {
    fn maximum_fact() -> DurableExternalFact {
        let mut fact = DurableExternalFact {
            outcome: ExternalFactOutcome::KnownCommitted,
            receipt: Some(String::new()),
            evidence: None,
            finalization_failure: None,
            failure: None,
        };
        let framing = serde_json::to_vec(&fact).unwrap().len();
        fact.receipt = Some("r".repeat(DML_CTAS_FACT_ENCODED_LIMIT - framing));
        assert_eq!(
            serde_json::to_vec(&fact).unwrap().len(),
            DML_CTAS_FACT_ENCODED_LIMIT
        );
        fact
    }

    let operation_id = DmlOperationId::new_v7();
    let mut payload = ctas_payload(CtasSagaPhase::Committed);
    let OperationPayload::CtasSaga(record) = &mut payload else {
        unreachable!();
    };
    record.prepare_fact = Some(maximum_fact());
    record.write_fact = Some(maximum_fact());
    record.publish_fact = Some(maximum_fact());
    record.abort_staging_fact = Some(maximum_fact());
    let operation = stored_statement_operation(
        operation_id,
        OperationKind::CreateTableAsSelect,
        payload.clone(),
    );
    let fact_bytes = match &payload {
        OperationPayload::CtasSaga(record) => [
            record.prepare_fact.as_ref().unwrap(),
            record.write_fact.as_ref().unwrap(),
            record.publish_fact.as_ref().unwrap(),
            record.abort_staging_fact.as_ref().unwrap(),
        ]
        .iter()
        .map(|fact| serde_json::to_vec(fact).unwrap().len())
        .sum::<usize>(),
        _ => unreachable!(),
    };
    assert_eq!(fact_bytes, DML_CTAS_TOTAL_FACT_ENCODED_LIMIT);

    let encoded_len = serde_json::to_vec(&operation).unwrap().len();
    assert!(encoded_len < 64 * 1024);
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) =
        open_store_with_max_value_bytes(&temp.path().join("ctas-max.sqlite"), Some(encoded_len))
            .await;
    assert_eq!(store.limits().max_value_bytes, encoded_len);
    journal.preflight_statement_operation(&operation).unwrap();

    let (_short_host, _short_store, short_journal) = open_store_with_max_value_bytes(
        &temp.path().join("ctas-max-short.sqlite"),
        Some(encoded_len - 1),
    )
    .await;
    assert_eq!(
        short_journal
            .preflight_statement_operation(&operation)
            .unwrap_err()
            .kind(),
        DmlErrorKind::JournalUnavailable
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_connector_owner_requires_lossless_lowercase_incarnation() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let mut payload = ctas_payload(CtasSagaPhase::PreparingStagedTable);
    let OperationPayload::CtasSaga(record) = &mut payload else {
        unreachable!();
    };
    record.connector_incarnation = Some("AA".repeat(16));
    let error = journal
        .create_statement_operation(statement_request(
            DmlOperationId::new_v7(),
            Uuid::now_v7(),
            OperationKind::CreateTableAsSelect,
            payload,
        ))
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);

    let mut partial = ctas_payload(CtasSagaPhase::PreparingSource);
    let OperationPayload::CtasSaga(record) = &mut partial else {
        unreachable!();
    };
    record.provider_id = None;
    let error = journal
        .create_statement_operation(statement_request(
            DmlOperationId::new_v7(),
            Uuid::now_v7(),
            OperationKind::CreateTableAsSelect,
            partial,
        ))
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(journal.list_operations().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// CP-3B: external operation fence receipts and historical write recovery.
// ---------------------------------------------------------------------------

fn fence_generation(
    control_plane_incarnation: u64,
    resource_epoch: u64,
    fence_generation: u64,
) -> DmlExternalFenceGeneration {
    DmlExternalFenceGeneration {
        control_plane_incarnation,
        resource_epoch,
        fence_generation,
    }
}

fn external_fence(
    write_operation_id: Uuid,
    coordination_attempt_id: Uuid,
    generation: DmlExternalFenceGeneration,
) -> DmlExternalFenceReceiptRecord {
    DmlExternalFenceReceiptRecord {
        codec_version: DML_EXTERNAL_FENCE_CODEC_VERSION,
        identity: DmlExternalFenceIdentity {
            cluster_identity_digest: "1a".repeat(32),
            resource_digest: "2b".repeat(32),
            write_operation_id,
            coordination_attempt_id,
            generation,
        },
        fence_digest: format!("{:062x}{:02x}", generation.fence_generation, 0xab_u8),
        receipt_digest: format!("{:062x}{:02x}", generation.fence_generation, 0xcd_u8),
        receipt_payload: DmlOpaquePayload::try_new(
            b"opaque provider external fence receipt".to_vec(),
        )
        .unwrap(),
        established_at_ms: 1_000,
    }
}

fn historical_request(
    write_operation_id: Uuid,
    old_coordination_attempt_id: Uuid,
    old_fence: Option<DmlExternalFenceReceiptRecord>,
) -> DmlHistoricalWriteRequestRecord {
    DmlHistoricalWriteRequestRecord {
        old_provider_id: "iceberg".to_string(),
        old_connector_instance_id: "iceberg-rest".to_string(),
        old_connector_incarnation: "5e".repeat(16),
        old_coordination_attempt_id: Some(old_coordination_attempt_id),
        old_fence,
        write_operation_id,
        cohort_set_digest: "6f".repeat(32),
        aggregate_write_digest: Some("7a".repeat(32)),
        dispatch_certainty: DmlHistoricalDispatchCertainty::PossiblyDispatched,
        writer_output_checkpointed: true,
        commit_dispatched_at_ms: Some(900),
        request_digest: "8b".repeat(32),
    }
}

fn historical_requested(
    recovery_attempt_id: Uuid,
    request: DmlHistoricalWriteRequestRecord,
) -> DmlHistoricalWriteRecoveryRecord {
    DmlHistoricalWriteRecoveryRecord {
        codec_version: DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION,
        phase: DmlHistoricalRecoveryPhase::Requested,
        recovery_attempt_id,
        recovery_cycle: 1,
        request,
        raised_fence: None,
        result: None,
        next_action: StatementNextAction::Reconcile,
        requested_at_ms: 1_000,
        updated_at_ms: 1_000,
    }
}

fn staged_result(cleanup: DmlHistoricalCleanupState) -> DmlHistoricalWriteResultRecord {
    DmlHistoricalWriteResultRecord {
        disposition: DmlHistoricalWriteDisposition::Staged,
        observation_digest: "9c".repeat(32),
        evidence_payload: None,
        proof_payload: Some(
            DmlOpaquePayload::try_new(b"opaque provider staged proof".to_vec()).unwrap(),
        ),
        continuation_payload: None,
        cleanup,
        failure: None,
        observed_at_ms: 1_200,
    }
}

/// Claim `operation_id` under `attempt` and move it into `Writing` so it can
/// accept an external fence receipt.
fn claim_and_start_writing(
    journal: &StateStoreOperationJournal,
    operation_id: DmlOperationId,
    attempt: Uuid,
    validator: Arc<TestTransactionValidator>,
) -> StoredOperation {
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let claimed = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: coordination_provenance(Uuid::now_v7(), attempt, 100),
                recovery_due_at_ms: 500,
            },
            authority(),
        )
        .unwrap();
    journal
        .transition_authorized(
            operation_id,
            claimed.revision,
            Uuid::now_v7(),
            OperationState::Writing,
            Some(500),
            authority(),
        )
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn external_fence_receipt_round_trips_and_only_advances_its_generation() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let writing = claim_and_start_writing(&journal, operation_id, attempt, validator.clone());

    let write_operation_id = Uuid::now_v7();
    let fence = external_fence(write_operation_id, attempt, fence_generation(1, 7, 1));
    let fenced_request = DmlExternalFenceMutationRequest {
        operation_id,
        expected_revision: writing.revision,
        mutation_id: Uuid::now_v7(),
        fence: fence.clone(),
    };
    journal.preflight_external_fence(&fenced_request).unwrap();
    let fenced = journal
        .record_external_fence_authorized(fenced_request, Some(600), authority())
        .unwrap();
    assert_eq!(fenced.revision, writing.revision + 1);
    assert_eq!(fenced.state, OperationState::Writing);
    assert_eq!(fenced.recovery_due_at_ms, Some(600));
    assert_eq!(
        journal.load_external_fence(operation_id).unwrap(),
        Some(fence.clone())
    );

    // A lower generation must never replace a confirmed fence.
    let lower = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: external_fence(write_operation_id, attempt, fence_generation(1, 6, 9)),
            },
            Some(600),
            authority(),
        )
        .unwrap_err();
    assert_eq!(lower.kind(), DmlErrorKind::JournalCorruption);
    assert!(lower.to_string().contains("must not move backwards"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), fenced);
    assert_eq!(
        journal.load_external_fence(operation_id).unwrap(),
        Some(fence.clone())
    );

    // A different receipt at the same generation cannot reuse the marker.
    let mut same_generation =
        external_fence(write_operation_id, attempt, fence_generation(1, 7, 1));
    same_generation.receipt_digest = "0f".repeat(32);
    let reused = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: same_generation,
            },
            Some(600),
            authority(),
        )
        .unwrap_err();
    assert_eq!(reused.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        reused
            .to_string()
            .contains("without advancing its generation")
    );

    // A strictly higher generation is the only legal replacement.
    let raised = external_fence(write_operation_id, attempt, fence_generation(1, 7, 2));
    let advanced = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: raised.clone(),
            },
            Some(700),
            authority(),
        )
        .unwrap();
    assert_eq!(advanced.revision, fenced.revision + 1);

    // The receipt survives a real StateStore restart byte for byte, and `open`
    // must not mutate anything it reads back.
    drop(journal);
    drop(store);
    drop(host);
    let (_host, _store, recovered) = open_store(&path).await;
    assert_eq!(recovered.load(operation_id).unwrap().unwrap(), advanced);
    assert_eq!(
        recovered.load_external_fence(operation_id).unwrap(),
        Some(raised)
    );
    assert_eq!(
        recovered
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn external_fence_receipt_is_rejected_after_the_operation_has_an_external_outcome() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let writing = claim_and_start_writing(&journal, operation_id, attempt, validator.clone());
    let terminal = journal
        .record_fact_authorized(
            operation_id,
            writing.revision,
            Uuid::now_v7(),
            OperationFact {
                state: OperationState::FailedKnownUncommitted,
                lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                    failure: ConnectorWriteFailureRecord {
                        kind: ConnectorWriteFailureKind::Unavailable,
                        message: "writer never dispatched".to_string(),
                    },
                },
            },
            None,
            authority(),
        )
        .unwrap();

    let error = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: terminal.revision,
                mutation_id: Uuid::now_v7(),
                fence: external_fence(Uuid::now_v7(), attempt, fence_generation(1, 7, 1)),
            },
            None,
            authority(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        error
            .to_string()
            .contains("cannot accept an external fence receipt in state")
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), terminal);
    assert_eq!(journal.load_external_fence(operation_id).unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_write_recovery_round_trips_and_keeps_its_due_until_resolved() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();
    let old_attempt = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let writing = claim_and_start_writing(&journal, operation_id, attempt, validator.clone());
    let shard = (0..journal.recovery_shard_count())
        .find(|shard| {
            journal
                .recovery_candidates(*shard, i64::MAX)
                .unwrap()
                .iter()
                .any(|candidate| candidate.operation_id == operation_id)
        })
        .unwrap();

    let write_operation_id = Uuid::now_v7();
    let old_fence = external_fence(write_operation_id, old_attempt, fence_generation(1, 7, 3));
    let request_record = historical_request(write_operation_id, old_attempt, Some(old_fence));
    let requested = historical_requested(attempt, request_record.clone());
    let mutation = DmlHistoricalWriteRecoveryMutationRequest {
        operation_id,
        expected_revision: writing.revision,
        mutation_id: Uuid::now_v7(),
        recovery: requested.clone(),
    };
    journal
        .preflight_historical_write_recovery(&mutation)
        .unwrap();
    let opened = journal
        .record_historical_write_recovery_authorized(mutation, Some(1_500), authority())
        .unwrap();
    assert_eq!(opened.revision, writing.revision + 1);
    assert_eq!(
        journal
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        Some(requested.clone())
    );

    // A raised fence must be strictly above the old attempt's fence.
    let too_low = DmlHistoricalWriteRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::FenceRaised,
        raised_fence: Some(external_fence(
            write_operation_id,
            attempt,
            fence_generation(1, 7, 3),
        )),
        updated_at_ms: 1_100,
        ..requested.clone()
    };
    let error = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: opened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: too_low,
            },
            Some(1_500),
            authority(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(error.to_string().contains("strictly above"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), opened);

    let raised_fence = external_fence(write_operation_id, attempt, fence_generation(1, 8, 1));
    let fence_raised = DmlHistoricalWriteRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::FenceRaised,
        raised_fence: Some(raised_fence.clone()),
        updated_at_ms: 1_100,
        ..requested.clone()
    };
    let raised = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: opened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: fence_raised.clone(),
            },
            Some(1_500),
            authority(),
        )
        .unwrap();

    let cleanup_pending = DmlHistoricalWriteRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::CleanupPending,
        result: Some(staged_result(DmlHistoricalCleanupState::Pending)),
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    };
    let pending = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: raised.revision,
                mutation_id: Uuid::now_v7(),
                recovery: cleanup_pending.clone(),
            },
            Some(1_500),
            authority(),
        )
        .unwrap();
    assert_eq!(
        journal
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        Some(cleanup_pending.clone())
    );

    // A terminal user-visible result must not drop the pending cleanup
    // obligation by clearing the recovery due.
    let dropped = journal
        .record_fact_authorized(
            operation_id,
            pending.revision,
            Uuid::now_v7(),
            OperationFact {
                state: OperationState::FailedKnownUncommitted,
                lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                    failure: ConnectorWriteFailureRecord {
                        kind: ConnectorWriteFailureKind::Conflict,
                        message: "old generation commit was fenced out".to_string(),
                    },
                },
            },
            None,
            authority(),
        )
        .unwrap_err();
    assert_eq!(dropped.kind(), DmlErrorKind::JournalUnresolved);
    assert!(dropped.to_string().contains("CLEANUP_PENDING"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), pending);

    // Forgetting the cleanup outcome inside the recovery record is refused too.
    let forgotten = DmlHistoricalWriteRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        result: None,
        next_action: StatementNextAction::None,
        updated_at_ms: 1_300,
        ..fence_raised.clone()
    };
    let error = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: pending.revision,
                mutation_id: Uuid::now_v7(),
                recovery: forgotten,
            },
            Some(1_500),
            authority(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), pending);

    // Completing the cleanup resolves the record and releases the scan.
    let resolved_record = DmlHistoricalWriteRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        result: Some(staged_result(DmlHistoricalCleanupState::Completed)),
        next_action: StatementNextAction::None,
        updated_at_ms: 1_400,
        ..cleanup_pending.clone()
    };
    let resolved = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: pending.revision,
                mutation_id: Uuid::now_v7(),
                recovery: resolved_record.clone(),
            },
            Some(1_500),
            authority(),
        )
        .unwrap();
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .iter()
            .any(|candidate| candidate.operation_id == operation_id)
    );

    let finished = journal
        .record_fact_authorized(
            operation_id,
            resolved.revision,
            Uuid::now_v7(),
            OperationFact {
                state: OperationState::FailedKnownUncommitted,
                lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                    failure: ConnectorWriteFailureRecord {
                        kind: ConnectorWriteFailureKind::Conflict,
                        message: "old generation commit was fenced out".to_string(),
                    },
                },
            },
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(finished.recovery_due_at_ms, None);
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty()
    );

    // A resolved recovery cannot be reopened, and it survives a restart.
    let reopened = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: finished.revision,
                mutation_id: Uuid::now_v7(),
                recovery: DmlHistoricalWriteRecoveryRecord {
                    phase: DmlHistoricalRecoveryPhase::CleanupPending,
                    result: Some(staged_result(DmlHistoricalCleanupState::Pending)),
                    next_action: StatementNextAction::Reconcile,
                    recovery_cycle: 2,
                    updated_at_ms: 1_500,
                    ..cleanup_pending
                },
            },
            Some(1_600),
            authority(),
        )
        .unwrap_err();
    assert_eq!(reopened.kind(), DmlErrorKind::JournalCorruption);
    assert!(reopened.to_string().contains("cannot be reopened"));

    drop(journal);
    drop(store);
    drop(host);
    let (_host, _store, recovered) = open_store(&path).await;
    assert_eq!(recovered.load(operation_id).unwrap().unwrap(), finished);
    assert_eq!(
        recovered
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        Some(resolved_record)
    );
    assert_eq!(
        recovered.load_external_fence(operation_id).unwrap(),
        None,
        "the raised recovery fence must not be mistaken for the attempt's own fence"
    );
    assert!(
        recovered
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty(),
        "reopening the journal must not resurrect a resolved recovery"
    );
    assert!(recovered.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn cp3b_side_records_reject_a_stale_attempt_and_a_wrong_expected_revision() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let writing = claim_and_start_writing(&journal, operation_id, attempt, validator.clone());
    let write_operation_id = Uuid::now_v7();

    // A stale coordination attempt cannot install a fence receipt.
    let stale_attempt = Uuid::now_v7();
    let stale_validator = Arc::new(TestTransactionValidator::default());
    let stale_authority =
        DmlMutationAuthority::try_new(stale_attempt, stale_validator.clone()).unwrap();
    let stale = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: writing.revision,
                mutation_id: Uuid::now_v7(),
                fence: external_fence(write_operation_id, stale_attempt, fence_generation(1, 7, 1)),
            },
            Some(600),
            stale_authority,
        )
        .unwrap_err();
    assert_eq!(stale.kind(), DmlErrorKind::JournalUnresolved);
    assert!(stale.to_string().contains("another coordination attempt"));
    assert_eq!(stale_validator.calls(), 1);
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), writing);
    assert_eq!(journal.load_external_fence(operation_id).unwrap(), None);

    // A fence minted by a foreign attempt is refused even under live authority.
    let foreign = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: writing.revision,
                mutation_id: Uuid::now_v7(),
                fence: external_fence(
                    write_operation_id,
                    Uuid::now_v7(),
                    fence_generation(1, 7, 1),
                ),
            },
            Some(600),
            authority(),
        )
        .unwrap_err();
    assert_eq!(foreign.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        foreign
            .to_string()
            .contains("was minted by another coordination attempt")
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), writing);
    assert_eq!(journal.load_external_fence(operation_id).unwrap(), None);

    // A wrong expected revision cannot change durable state either.
    let wrong_revision = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: writing.revision + 5,
                mutation_id: Uuid::now_v7(),
                fence: external_fence(write_operation_id, attempt, fence_generation(1, 7, 1)),
            },
            Some(600),
            authority(),
        )
        .unwrap_err();
    assert_eq!(wrong_revision.kind(), DmlErrorKind::JournalUnresolved);
    assert!(wrong_revision.to_string().contains("revision changed"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), writing);
    assert_eq!(journal.load_external_fence(operation_id).unwrap(), None);

    // The same two rejections hold for a historical write recovery record.
    let recovery = historical_requested(
        attempt,
        historical_request(write_operation_id, Uuid::now_v7(), None),
    );
    let wrong_revision = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: writing.revision + 5,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(1_500),
            authority(),
        )
        .unwrap_err();
    assert_eq!(wrong_revision.kind(), DmlErrorKind::JournalUnresolved);
    assert_eq!(
        journal
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        None
    );

    let foreign_recovery = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: writing.revision,
                mutation_id: Uuid::now_v7(),
                recovery: DmlHistoricalWriteRecoveryRecord {
                    recovery_attempt_id: Uuid::now_v7(),
                    ..recovery
                },
            },
            Some(1_500),
            authority(),
        )
        .unwrap_err();
    assert_eq!(foreign_recovery.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        foreign_recovery
            .to_string()
            .contains("belongs to another coordination attempt")
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), writing);
    assert_eq!(
        journal
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_cp1_superseded_holder_cannot_write_cp3b_side_records() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let gate = IncarnationGate::new(Arc::clone(&store));
    gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let operation_id = journal
        .create_preparing_admitted(
            request(),
            Arc::new(Cp1AdmissionValidator {
                admission: gate.admit_writes().await.unwrap(),
                observed: Arc::new(Mutex::new(None)),
            }),
        )
        .unwrap();

    let clock = Arc::new(ManualDmlLeaseClock::new(500_000, 20_000));
    let holder_a = Uuid::now_v7();
    let holder_b = Uuid::now_v7();
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_a),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let manager_b = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_b),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let resource = dml_operation_resource_key(operation_id).unwrap();
    let attempt_a = Uuid::now_v7();
    let attempt_b = Uuid::now_v7();
    let guard_a = Arc::new(AsyncMutex::new(acquired(
        manager_a
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_a).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));
    let claimed_a = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder_a, attempt_a, &guard_a, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 500_100,
            },
            current_authority(attempt_a, Arc::clone(&guard_a), Arc::new(Mutex::new(None))),
        )
        .unwrap();

    clock.advance_wall(16_001);
    assert!(matches!(
        manager_b
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_b).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(2_000);
    let guard_b = Arc::new(AsyncMutex::new(acquired(
        manager_b
            .acquire(
                resource,
                AttemptId::try_from(attempt_b).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));

    // Holder A still believes it owns the operation. Its fence receipt must be
    // refused by the latest live fence, not by a captured snapshot.
    let rejection = Arc::new(Mutex::new(None));
    let write_operation_id = Uuid::now_v7();
    let error = journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                fence: external_fence(write_operation_id, attempt_a, fence_generation(1, 7, 1)),
            },
            Some(500_200),
            current_authority(attempt_a, Arc::clone(&guard_a), Arc::clone(&rejection)),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed_a);
    assert_eq!(journal.load_external_fence(operation_id).unwrap(), None);

    let recovery_rejection = Arc::new(Mutex::new(None));
    let error = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                recovery: historical_requested(
                    attempt_a,
                    historical_request(write_operation_id, Uuid::now_v7(), None),
                ),
            },
            Some(500_200),
            current_authority(
                attempt_a,
                Arc::clone(&guard_a),
                Arc::clone(&recovery_rejection),
            ),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *recovery_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed_a);
    assert_eq!(
        journal
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        None
    );

    // The new owner re-claims and installs its own strictly higher fence.
    let claimed_b = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder_b, attempt_b, &guard_b, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 516_100,
            },
            current_authority(attempt_b, Arc::clone(&guard_b), Arc::new(Mutex::new(None))),
        )
        .unwrap();
    let fence_b = external_fence(write_operation_id, attempt_b, fence_generation(1, 8, 1));
    journal
        .record_external_fence_authorized(
            DmlExternalFenceMutationRequest {
                operation_id,
                expected_revision: claimed_b.revision,
                mutation_id: Uuid::now_v7(),
                fence: fence_b.clone(),
            },
            Some(516_200),
            current_authority(attempt_b, Arc::clone(&guard_b), Arc::new(Mutex::new(None))),
        )
        .unwrap();
    assert_eq!(
        journal.load_external_fence(operation_id).unwrap(),
        Some(fence_b)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn v7_operation_record_read_fails_without_migration_or_dual_format_decode() {
    assert_eq!(
        DML_OPERATION_SCHEMA_VERSION, 8,
        "CP-3D cuts the DML operation schema from v7 to v8"
    );
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (_host, store, journal) = open_store(&path).await;
    drop(journal);
    let operation_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        raw_operation(operation_id, 7),
    )
    .await;
    raw_put(
        store.as_ref(),
        key(UNFINISHED_PREFIX, operation_id),
        raw_unfinished(operation_id),
    )
    .await;

    let reopened =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("journal open must not scan or migrate operation records");
    let error = reopened
        .load(DmlOperationId::from(operation_id))
        .expect_err("a v7 operation record must not be decoded by v8 code");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        error
            .to_string()
            .contains("unsupported frontend DML operation schema version: 7"),
        "the hard cut must name the rejected version: {error}"
    );
    let scan_error = reopened
        .list_unfinished()
        .expect_err("a v7 record must not be silently skipped by the unfinished scan");
    assert_eq!(scan_error.kind(), DmlErrorKind::JournalCorruption);
}

#[tokio::test(flavor = "multi_thread")]
async fn cp3b_side_record_codecs_reject_corrupt_durable_values() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();

    // An unknown codec version must fail the read instead of being upgraded.
    raw_put(
        store.as_ref(),
        key(
            "novarocks/frontend/dml/v1/external-fences/",
            *operation_id.as_uuid(),
        ),
        Value::try_from(Bytes::from(
            serde_json::to_vec(&json!({
                "codec_version": 99,
                "identity": {
                    "cluster_identity_digest": "1a".repeat(32),
                    "resource_digest": "2b".repeat(32),
                    "write_operation_id": Uuid::now_v7(),
                    "coordination_attempt_id": Uuid::now_v7(),
                    "generation": {
                        "control_plane_incarnation": 1,
                        "resource_epoch": 1,
                        "fence_generation": 1
                    }
                },
                "fence_digest": "3c".repeat(32),
                "receipt_digest": "4d".repeat(32),
                "receipt_payload": "aabb",
                "established_at_ms": 1
            }))
            .unwrap(),
        ))
        .unwrap(),
    )
    .await;
    let error = journal.load_external_fence(operation_id).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        error
            .to_string()
            .contains("unsupported DML external fence codec version: 99")
    );

    // Non-canonical opaque payload encodings are refused by the codec itself.
    let second_id = journal
        .create_preparing_admitted(request(), Arc::new(TestTransactionValidator::default()))
        .unwrap();
    raw_put(
        store.as_ref(),
        key(
            "novarocks/frontend/dml/v1/external-fences/",
            *second_id.as_uuid(),
        ),
        Value::try_from(Bytes::from(
            serde_json::to_vec(&json!({
                "codec_version": DML_EXTERNAL_FENCE_CODEC_VERSION,
                "identity": {
                    "cluster_identity_digest": "1a".repeat(32),
                    "resource_digest": "2b".repeat(32),
                    "write_operation_id": Uuid::now_v7(),
                    "coordination_attempt_id": Uuid::now_v7(),
                    "generation": {
                        "control_plane_incarnation": 1,
                        "resource_epoch": 1,
                        "fence_generation": 1
                    }
                },
                "fence_digest": "3c".repeat(32),
                "receipt_digest": "4d".repeat(32),
                "receipt_payload": "AABB",
                "established_at_ms": 1
            }))
            .unwrap(),
        ))
        .unwrap(),
    )
    .await;
    let error = journal.load_external_fence(second_id).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(error.to_string().contains("canonical lowercase hex"));
}

#[test]
fn opaque_payload_is_bounded_and_never_leaks_into_debug_output() {
    assert!(DmlOpaquePayload::try_new(Vec::new()).is_err());
    assert!(DmlOpaquePayload::try_new(vec![7; DML_OPAQUE_PAYLOAD_LIMIT]).is_ok());
    assert!(DmlOpaquePayload::try_new(vec![7; DML_OPAQUE_PAYLOAD_LIMIT + 1]).is_err());

    let payload = DmlOpaquePayload::try_new(b"provider-private continuation".to_vec()).unwrap();
    let rendered = format!("{payload:?}");
    assert!(!rendered.contains("continuation"), "{rendered}");
    assert!(rendered.contains("len"), "{rendered}");

    let encoded = serde_json::to_string(&payload).unwrap();
    assert_eq!(encoded, format!("\"{}\"", hex::encode(payload.as_bytes())));
    assert_eq!(
        serde_json::from_str::<DmlOpaquePayload>(&encoded).unwrap(),
        payload
    );
}

#[test]
fn external_fence_generation_is_totally_ordered_by_its_scalar_components() {
    let base = fence_generation(1, 7, 5);
    assert!(fence_generation(1, 7, 6) > base);
    assert!(fence_generation(1, 8, 1) > base);
    assert!(fence_generation(2, 1, 1) > base);
    assert!(fence_generation(1, 7, 4) < base);
    assert!(fence_generation(1, 6, 9) < base);
    assert_eq!(fence_generation(1, 7, 5), base);
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_write_recovery_reopens_the_scan_for_an_already_terminal_operation() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let admission = Arc::new(TestTransactionValidator::default());
    let operation_id = journal
        .create_preparing_admitted(request(), admission)
        .unwrap();
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let writing = claim_and_start_writing(&journal, operation_id, attempt, validator.clone());

    // The statement already failed and left the recovery scan. This is the
    // KNOWN_UNCOMMITTED case whose external finalization must survive.
    let terminal = journal
        .record_fact_authorized(
            operation_id,
            writing.revision,
            Uuid::now_v7(),
            OperationFact {
                state: OperationState::FailedKnownUncommitted,
                lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                    failure: ConnectorWriteFailureRecord {
                        kind: ConnectorWriteFailureKind::Unavailable,
                        message: "old owner disappeared".to_string(),
                    },
                },
            },
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(terminal.recovery_due_at_ms, None);
    let shard = (0..journal.recovery_shard_count()).find(|shard| {
        journal
            .recovery_candidates(*shard, i64::MAX)
            .unwrap()
            .iter()
            .any(|candidate| candidate.operation_id == operation_id)
    });
    assert_eq!(shard, None, "a terminal write operation leaves the scan");

    // Opening a historical write recovery must put it back into the scan even
    // though the user-visible statement result is already terminal.
    let write_operation_id = Uuid::now_v7();
    let recovery = historical_requested(
        attempt,
        historical_request(write_operation_id, Uuid::now_v7(), None),
    );
    let reopened = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: terminal.revision,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(2_000),
            authority(),
        )
        .unwrap();
    assert_eq!(reopened.state, OperationState::FailedKnownUncommitted);
    assert_eq!(reopened.recovery_due_at_ms, Some(2_000));
    let shard = (0..journal.recovery_shard_count())
        .find(|shard| {
            journal
                .recovery_candidates(*shard, i64::MAX)
                .unwrap()
                .iter()
                .any(|candidate| candidate.operation_id == operation_id)
        })
        .expect("an open historical recovery must keep the operation scannable");
    assert!(journal.list_unfinished().unwrap().is_empty());

    // Opening it without a due is refused: the obligation cannot be invisible.
    let unscanned = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: reopened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: DmlHistoricalWriteRecoveryRecord {
                    updated_at_ms: 1_100,
                    ..recovery.clone()
                },
            },
            None,
            authority(),
        )
        .unwrap_err();
    assert_eq!(unscanned.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        unscanned.to_string().contains(
            "cannot drop its recovery due while historical write recovery phase REQUESTED"
        )
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), reopened);

    // Resolving it releases the scan in the same fenced mutation.
    let raised_fence = external_fence(write_operation_id, attempt, fence_generation(1, 9, 1));
    let resolved_record = DmlHistoricalWriteRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        raised_fence: Some(raised_fence),
        result: Some(DmlHistoricalWriteResultRecord {
            disposition: DmlHistoricalWriteDisposition::NotApplied,
            observation_digest: "9c".repeat(32),
            evidence_payload: None,
            proof_payload: Some(
                DmlOpaquePayload::try_new(b"opaque provider not-applied proof".to_vec()).unwrap(),
            ),
            continuation_payload: None,
            cleanup: DmlHistoricalCleanupState::NotRequired,
            failure: None,
            observed_at_ms: 1_300,
        }),
        next_action: StatementNextAction::None,
        updated_at_ms: 1_400,
        ..recovery
    };
    let resolved = journal
        .record_historical_write_recovery_authorized(
            DmlHistoricalWriteRecoveryMutationRequest {
                operation_id,
                expected_revision: reopened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: resolved_record.clone(),
            },
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(resolved.recovery_due_at_ms, None);
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        journal
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        Some(resolved_record)
    );
}

// ---------------------------------------------------------------------------
// CP-3C: direct data-mutation fence receipts and historical data-mutation
// recovery.
// ---------------------------------------------------------------------------

fn add_files_scope() -> String {
    "3d".repeat(32)
}

fn truncate_preparing() -> OperationPayload {
    OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
        phase: TruncateLifecyclePhase::Preparing,
        connector_operation_id: Uuid::now_v7(),
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some("09".repeat(16)),
        target_ref: "main".to_string(),
        request_digest: Some("request".to_string()),
        plan_digest: None,
        state_digest: None,
        plan_summary: None,
        outcome: None,
        next_action: StatementNextAction::None,
    })
}

fn truncate_failed() -> OperationPayload {
    OperationPayload::TruncateLifecycle(TruncateLifecycleRecord {
        phase: TruncateLifecyclePhase::Failed,
        connector_operation_id: Uuid::now_v7(),
        provider_id: Some("iceberg".to_string()),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some("09".repeat(16)),
        target_ref: "main".to_string(),
        request_digest: Some("request".to_string()),
        plan_digest: None,
        state_digest: None,
        plan_summary: None,
        outcome: Some(DurableExternalFact {
            outcome: ExternalFactOutcome::KnownUncommitted,
            receipt: None,
            evidence: None,
            finalization_failure: None,
            failure: Some("old owner disappeared".to_string()),
        }),
        next_action: StatementNextAction::None,
    })
}

fn direct_mutation_payload(kind: DmlDirectMutationKind) -> OperationPayload {
    match kind {
        DmlDirectMutationKind::Truncate => truncate_preparing(),
        DmlDirectMutationKind::AddFiles => add_files_preparing(),
    }
}

/// Create and claim a TRUNCATE or ADD FILES operation so it can accept a
/// direct-mutation fence receipt. Both families seal their fence in `Preparing`.
fn claim_direct_mutation(
    journal: &StateStoreOperationJournal,
    kind: DmlDirectMutationKind,
    attempt: Uuid,
    validator: Arc<TestTransactionValidator>,
) -> (DmlOperationId, StoredOperation) {
    let operation_id = DmlOperationId::new_v7();
    let created = journal
        .create_statement_operation_admitted(
            statement_request(
                operation_id,
                Uuid::now_v7(),
                kind.operation_kind(),
                direct_mutation_payload(kind),
            ),
            Arc::new(TestTransactionValidator::default()),
        )
        .unwrap();
    let claimed = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: created.revision,
                mutation_id: Uuid::now_v7(),
                provenance: coordination_provenance(Uuid::now_v7(), attempt, 300),
                recovery_due_at_ms: 18_300,
            },
            DmlMutationAuthority::try_new(attempt, validator).unwrap(),
        )
        .unwrap();
    (operation_id, claimed)
}

fn direct_mutation_fence(
    kind: DmlDirectMutationKind,
    mutation_operation_id: Uuid,
    coordination_attempt_id: Uuid,
    generation: DmlExternalFenceGeneration,
    source_scope_digest: Option<String>,
) -> DmlDirectMutationFenceReceiptRecord {
    DmlDirectMutationFenceReceiptRecord {
        codec_version: DML_DIRECT_MUTATION_FENCE_CODEC_VERSION,
        operation_kind: kind,
        fence: external_fence(mutation_operation_id, coordination_attempt_id, generation),
        source_scope_digest,
    }
}

fn data_mutation_request(
    kind: DmlDirectMutationKind,
    mutation_operation_id: Uuid,
    old_coordination_attempt_id: Uuid,
    old_fence: Option<DmlDirectMutationFenceReceiptRecord>,
    source_scope_digest: Option<String>,
) -> DmlHistoricalDataMutationRequestRecord {
    DmlHistoricalDataMutationRequestRecord {
        old_provider_id: "iceberg".to_string(),
        old_connector_instance_id: "iceberg-rest".to_string(),
        old_connector_incarnation: "5e".repeat(16),
        old_coordination_attempt_id: Some(old_coordination_attempt_id),
        old_fence,
        operation_kind: kind,
        mutation_operation_id,
        request_digest: "8b".repeat(32),
        plan_digest: Some("7a".repeat(32)),
        state_digest: Some("6f".repeat(32)),
        source_scope_digest,
        dispatch_certainty: DmlHistoricalDispatchCertainty::PossiblyDispatched,
        dispatched_at_ms: Some(900),
    }
}

fn data_mutation_requested(
    recovery_attempt_id: Uuid,
    request: DmlHistoricalDataMutationRequestRecord,
) -> DmlHistoricalDataMutationRecoveryRecord {
    DmlHistoricalDataMutationRecoveryRecord {
        codec_version: DML_HISTORICAL_DATA_MUTATION_RECOVERY_CODEC_VERSION,
        phase: DmlHistoricalRecoveryPhase::Requested,
        recovery_attempt_id,
        recovery_cycle: 1,
        request,
        raised_fence: None,
        result: None,
        next_action: StatementNextAction::Reconcile,
        requested_at_ms: 1_000,
        updated_at_ms: 1_000,
    }
}

fn data_mutation_result(
    disposition: DmlHistoricalDataMutationDisposition,
    source_scope_digest: Option<String>,
    cleanup: DmlHistoricalCleanupState,
    source_scope_retained: bool,
) -> DmlHistoricalDataMutationResultRecord {
    DmlHistoricalDataMutationResultRecord {
        disposition,
        observation_digest: "9c".repeat(32),
        source_scope_digest,
        evidence_payload: None,
        proof_payload: Some(
            DmlOpaquePayload::try_new(b"opaque provider direct mutation proof".to_vec()).unwrap(),
        ),
        continuation_payload: None,
        cleanup,
        source_scope_retained,
        failure: None,
        observed_at_ms: 1_200,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_mutation_fence_receipt_round_trips_and_only_advances_its_generation() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let (operation_id, claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::Truncate,
        attempt,
        validator.clone(),
    );

    let mutation_operation_id = Uuid::now_v7();
    let fence = direct_mutation_fence(
        DmlDirectMutationKind::Truncate,
        mutation_operation_id,
        attempt,
        fence_generation(1, 7, 1),
        None,
    );
    let fenced_request = DmlDirectMutationFenceMutationRequest {
        operation_id,
        expected_revision: claimed.revision,
        mutation_id: Uuid::now_v7(),
        fence: fence.clone(),
    };
    journal
        .preflight_direct_mutation_fence(&fenced_request)
        .unwrap();
    let fenced = journal
        .record_direct_mutation_fence_authorized(fenced_request, Some(18_400), authority())
        .unwrap();
    assert_eq!(fenced.revision, claimed.revision + 1);
    assert_eq!(fenced.state, OperationState::Preparing);
    assert_eq!(fenced.recovery_due_at_ms, Some(18_400));
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        Some(fence.clone())
    );
    // The direct-mutation receipt owns its own durable key: it must never be
    // mistaken for a distributed-write fence.
    assert_eq!(journal.load_external_fence(operation_id).unwrap(), None);

    // A lower generation must never replace a confirmed fence.
    let lower = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    mutation_operation_id,
                    attempt,
                    fence_generation(1, 6, 9),
                    None,
                ),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(lower.kind(), DmlErrorKind::JournalCorruption);
    assert!(lower.to_string().contains("must not move backwards"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), fenced);

    // A different receipt at the same generation cannot reuse the marker.
    let mut same_generation = direct_mutation_fence(
        DmlDirectMutationKind::Truncate,
        mutation_operation_id,
        attempt,
        fence_generation(1, 7, 1),
        None,
    );
    same_generation.fence.receipt_digest = "0f".repeat(32);
    let reused = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: same_generation,
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(reused.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        reused
            .to_string()
            .contains("without advancing its generation")
    );

    // A marker minted for another direct mutation cannot be adopted either.
    let crossed = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    Uuid::now_v7(),
                    attempt,
                    fence_generation(1, 7, 2),
                    None,
                ),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(crossed.kind(), DmlErrorKind::JournalCorruption);
    assert!(crossed.to_string().contains("cannot be reused across"));

    // A strictly higher generation is the only legal replacement.
    let raised = direct_mutation_fence(
        DmlDirectMutationKind::Truncate,
        mutation_operation_id,
        attempt,
        fence_generation(1, 7, 2),
        None,
    );
    let advanced = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: raised.clone(),
            },
            Some(18_500),
            authority(),
        )
        .unwrap();
    assert_eq!(advanced.revision, fenced.revision + 1);

    // The receipt survives a real StateStore restart byte for byte, and `open`
    // must not mutate anything it reads back.
    drop(journal);
    drop(store);
    drop(host);
    let (_host, _store, recovered) = open_store(&path).await;
    assert_eq!(recovered.load(operation_id).unwrap().unwrap(), advanced);
    assert_eq!(
        recovered.load_direct_mutation_fence(operation_id).unwrap(),
        Some(raised)
    );
    assert_eq!(
        recovered
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn add_files_direct_mutation_fence_binds_its_immutable_source_scope() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let (operation_id, claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::AddFiles,
        attempt,
        validator.clone(),
    );
    let mutation_operation_id = Uuid::now_v7();
    let scope = add_files_scope();

    // ADD FILES without a source scope binding fails closed at preflight.
    let unbound = DmlDirectMutationFenceMutationRequest {
        operation_id,
        expected_revision: claimed.revision,
        mutation_id: Uuid::now_v7(),
        fence: direct_mutation_fence(
            DmlDirectMutationKind::AddFiles,
            mutation_operation_id,
            attempt,
            fence_generation(1, 7, 1),
            None,
        ),
    };
    let error = journal
        .preflight_direct_mutation_fence(&unbound)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        error
            .to_string()
            .contains("must bind its immutable source scope digest")
    );
    let error = journal
        .record_direct_mutation_fence_authorized(unbound, Some(18_400), authority())
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        None
    );

    // A TRUNCATE-family receipt cannot land on an ADD FILES operation.
    let wrong_family = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    mutation_operation_id,
                    attempt,
                    fence_generation(1, 7, 1),
                    None,
                ),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(wrong_family.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        wrong_family
            .to_string()
            .contains("cannot accept a TRUNCATE fence receipt")
    );

    // A TRUNCATE receipt has no source set at all, so binding one is refused.
    let (truncate_id, truncate_claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::Truncate,
        attempt,
        validator.clone(),
    );
    let error = journal
        .preflight_direct_mutation_fence(&DmlDirectMutationFenceMutationRequest {
            operation_id: truncate_id,
            expected_revision: truncate_claimed.revision,
            mutation_id: Uuid::now_v7(),
            fence: direct_mutation_fence(
                DmlDirectMutationKind::Truncate,
                Uuid::now_v7(),
                attempt,
                fence_generation(1, 7, 1),
                Some(scope.clone()),
            ),
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must not bind a source scope digest")
    );

    // The bound receipt is durable.
    let fence = direct_mutation_fence(
        DmlDirectMutationKind::AddFiles,
        mutation_operation_id,
        attempt,
        fence_generation(1, 7, 1),
        Some(scope.clone()),
    );
    let fenced = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                fence: fence.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        Some(fence)
    );

    // Even a strictly higher generation cannot rebind another source scope: the
    // ADD FILES source set never expands or moves.
    let rebound = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: fenced.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::AddFiles,
                    mutation_operation_id,
                    attempt,
                    fence_generation(1, 8, 1),
                    Some("4e".repeat(32)),
                ),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(rebound.kind(), DmlErrorKind::JournalCorruption);
    assert!(rebound.to_string().contains("cannot rebind another scope"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), fenced);
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_data_mutation_recovery_round_trips_and_keeps_its_due_until_resolved() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    let attempt = Uuid::now_v7();
    let old_attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let (operation_id, claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::AddFiles,
        attempt,
        validator.clone(),
    );
    let shard = (0..journal.recovery_shard_count())
        .find(|shard| {
            journal
                .recovery_candidates(*shard, i64::MAX)
                .unwrap()
                .iter()
                .any(|candidate| candidate.operation_id == operation_id)
        })
        .unwrap();

    let mutation_operation_id = Uuid::now_v7();
    let scope = add_files_scope();
    let old_fence = direct_mutation_fence(
        DmlDirectMutationKind::AddFiles,
        mutation_operation_id,
        old_attempt,
        fence_generation(1, 7, 3),
        Some(scope.clone()),
    );
    let request_record = data_mutation_request(
        DmlDirectMutationKind::AddFiles,
        mutation_operation_id,
        old_attempt,
        Some(old_fence),
        Some(scope.clone()),
    );
    let requested = data_mutation_requested(attempt, request_record);
    let mutation = DmlHistoricalDataMutationRecoveryMutationRequest {
        operation_id,
        expected_revision: claimed.revision,
        mutation_id: Uuid::now_v7(),
        recovery: requested.clone(),
    };
    journal
        .preflight_historical_data_mutation_recovery(&mutation)
        .unwrap();
    let opened = journal
        .record_historical_data_mutation_recovery_authorized(mutation, Some(18_400), authority())
        .unwrap();
    assert_eq!(opened.revision, claimed.revision + 1);
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        Some(requested.clone())
    );
    assert!(
        requested.retains_source_scope(),
        "an ADD FILES recovery without a provider result must retain its source scope"
    );

    // A raised fence must be strictly above the old attempt's fence.
    let too_low = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::FenceRaised,
        raised_fence: Some(direct_mutation_fence(
            DmlDirectMutationKind::AddFiles,
            mutation_operation_id,
            attempt,
            fence_generation(1, 7, 3),
            Some(scope.clone()),
        )),
        updated_at_ms: 1_100,
        ..requested.clone()
    };
    let error = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: opened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: too_low,
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(error.to_string().contains("strictly above"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), opened);

    let fence_raised = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::FenceRaised,
        raised_fence: Some(direct_mutation_fence(
            DmlDirectMutationKind::AddFiles,
            mutation_operation_id,
            attempt,
            fence_generation(1, 8, 1),
            Some(scope.clone()),
        )),
        updated_at_ms: 1_100,
        ..requested.clone()
    };
    let raised = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: opened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: fence_raised.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();

    let cleanup_pending = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::CleanupPending,
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::CleanupRequired,
            Some(scope.clone()),
            DmlHistoricalCleanupState::Pending,
            true,
        )),
        next_action: StatementNextAction::AbortStaging,
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    };
    let pending = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: raised.revision,
                mutation_id: Uuid::now_v7(),
                recovery: cleanup_pending.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        Some(cleanup_pending.clone())
    );

    // A terminal user-visible ADD FILES result must not drop the pending
    // guarded cleanup by clearing the recovery due.
    let dropped = journal
        .mutate_statement_operation_authorized(
            OperationMutationRequest {
                operation_id,
                expected_revision: pending.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::FailedKnownUncommitted,
                payload: add_files_preparing(),
            },
            None,
            authority(),
        )
        .unwrap_err();
    assert_eq!(dropped.kind(), DmlErrorKind::JournalUnresolved);
    assert!(dropped.to_string().contains("CLEANUP_PENDING"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), pending);

    // Forgetting the cleanup outcome inside the recovery record is refused too.
    let forgotten = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        result: None,
        next_action: StatementNextAction::None,
        updated_at_ms: 1_300,
        ..fence_raised.clone()
    };
    let error = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: pending.revision,
                mutation_id: Uuid::now_v7(),
                recovery: forgotten,
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), pending);

    // Completing the guarded cleanup resolves the record and releases the scan.
    let resolved_record = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::CleanupRequired,
            Some(scope.clone()),
            DmlHistoricalCleanupState::Completed,
            true,
        )),
        next_action: StatementNextAction::None,
        updated_at_ms: 1_400,
        ..cleanup_pending.clone()
    };
    let resolved = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: pending.revision,
                mutation_id: Uuid::now_v7(),
                recovery: resolved_record.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .iter()
            .any(|candidate| candidate.operation_id == operation_id)
    );

    let finished = journal
        .mutate_statement_operation_authorized(
            OperationMutationRequest {
                operation_id,
                expected_revision: resolved.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::FailedKnownUncommitted,
                payload: add_files_preparing(),
            },
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(finished.recovery_due_at_ms, None);
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty()
    );

    // A resolved recovery cannot be reopened, and it survives a restart.
    let reopened = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: finished.revision,
                mutation_id: Uuid::now_v7(),
                recovery: DmlHistoricalDataMutationRecoveryRecord {
                    recovery_cycle: 2,
                    updated_at_ms: 1_500,
                    ..cleanup_pending
                },
            },
            Some(18_600),
            authority(),
        )
        .unwrap_err();
    assert_eq!(reopened.kind(), DmlErrorKind::JournalCorruption);
    assert!(reopened.to_string().contains("cannot be reopened"));

    drop(journal);
    drop(store);
    drop(host);
    let (_host, _store, recovered) = open_store(&path).await;
    assert_eq!(recovered.load(operation_id).unwrap().unwrap(), finished);
    assert_eq!(
        recovered
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        Some(resolved_record)
    );
    assert_eq!(
        recovered
            .load_historical_write_recovery(operation_id)
            .unwrap(),
        None,
        "a direct mutation recovery must not be read back as a distributed write recovery"
    );
    assert_eq!(
        recovered.load_direct_mutation_fence(operation_id).unwrap(),
        None,
        "the raised recovery fence must not be mistaken for the attempt's own fence"
    );
    assert!(
        recovered
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty(),
        "reopening the journal must not resurrect a resolved recovery"
    );
    assert!(recovered.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_data_mutation_result_is_bound_to_its_immutable_add_files_source_scope() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let (operation_id, claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::AddFiles,
        attempt,
        validator.clone(),
    );
    let mutation_operation_id = Uuid::now_v7();
    let scope = add_files_scope();
    let requested = data_mutation_requested(
        attempt,
        data_mutation_request(
            DmlDirectMutationKind::AddFiles,
            mutation_operation_id,
            Uuid::now_v7(),
            None,
            Some(scope.clone()),
        ),
    );
    let opened = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                recovery: requested.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();
    let fence_raised = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::FenceRaised,
        raised_fence: Some(direct_mutation_fence(
            DmlDirectMutationKind::AddFiles,
            mutation_operation_id,
            attempt,
            fence_generation(1, 8, 1),
            Some(scope.clone()),
        )),
        updated_at_ms: 1_100,
        ..requested
    };
    let raised = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: opened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: fence_raised.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();

    let write_result = |recovery: DmlHistoricalDataMutationRecoveryRecord| {
        journal.record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: raised.revision,
                mutation_id: Uuid::now_v7(),
                recovery,
            },
            Some(18_400),
            authority(),
        )
    };

    // A result bound to another source scope belongs to another operation.
    let crossed = write_result(DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Inspected,
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::Applied,
            Some("4e".repeat(32)),
            DmlHistoricalCleanupState::NotRequired,
            false,
        )),
        next_action: StatementNextAction::RetryFinalize,
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    })
    .unwrap_err();
    assert_eq!(crossed.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        crossed
            .to_string()
            .contains("bound to a different source scope than its sealed request"),
        "{crossed}"
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), raised);
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        Some(fence_raised.clone())
    );

    // Dropping the binding entirely is the same refusal.
    let unbound = write_result(DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Inspected,
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::Applied,
            None,
            DmlHistoricalCleanupState::NotRequired,
            false,
        )),
        next_action: StatementNextAction::RetryFinalize,
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    })
    .unwrap_err();
    assert_eq!(unbound.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        unbound
            .to_string()
            .contains("bound to a different source scope than its sealed request")
    );

    // Evidence absence is never proof: an inconclusive disposition must keep the
    // source-scope reservation held.
    let released = write_result(DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Unresolved,
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::Ambiguous,
            Some(scope.clone()),
            DmlHistoricalCleanupState::NotRequired,
            false,
        )),
        next_action: StatementNextAction::ManualInspect,
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    })
    .unwrap_err();
    assert_eq!(released.kind(), DmlErrorKind::JournalCorruption);
    assert!(
        released
            .to_string()
            .contains("must retain its ADD FILES source scope"),
        "{released}"
    );

    // A continuation is only meaningful once the mutation is proven NOT_APPLIED.
    let mut ambiguous_result = data_mutation_result(
        DmlHistoricalDataMutationDisposition::Ambiguous,
        Some(scope.clone()),
        DmlHistoricalCleanupState::NotRequired,
        true,
    );
    ambiguous_result.continuation_payload =
        Some(DmlOpaquePayload::try_new(b"opaque continuation".to_vec()).unwrap());
    let premature = write_result(DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Unresolved,
        result: Some(ambiguous_result),
        next_action: StatementNextAction::ManualInspect,
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    })
    .unwrap_err();
    assert!(
        premature
            .to_string()
            .contains("only valid for a proven NOT_APPLIED disposition")
    );

    // The unresolved answer with the scope retained is durable and keeps the
    // recovery scannable.
    let unresolved_record = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Unresolved,
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::Ambiguous,
            Some(scope.clone()),
            DmlHistoricalCleanupState::NotRequired,
            true,
        )),
        next_action: StatementNextAction::ManualInspect,
        updated_at_ms: 1_200,
        ..fence_raised.clone()
    };
    let unresolved = write_result(unresolved_record.clone()).unwrap();
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        Some(unresolved_record.clone())
    );
    assert!(unresolved_record.retains_source_scope());
    assert!(unresolved_record.requires_recovery_scan());

    // A later cycle may prove NOT_APPLIED, which is the only inconclusive-free
    // way to release the reservation and to hand out a continuation.
    let mut proven = data_mutation_result(
        DmlHistoricalDataMutationDisposition::NotApplied,
        Some(scope),
        DmlHistoricalCleanupState::NotRequired,
        false,
    );
    proven.continuation_payload =
        Some(DmlOpaquePayload::try_new(b"opaque continuation".to_vec()).unwrap());
    let resolved_record = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        result: Some(proven),
        next_action: StatementNextAction::None,
        updated_at_ms: 1_300,
        ..unresolved_record
    };
    let resolved = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: unresolved.revision,
                mutation_id: Uuid::now_v7(),
                recovery: resolved_record.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap();
    assert_eq!(resolved.revision, unresolved.revision + 1);
    assert!(!resolved_record.retains_source_scope());
    assert!(!resolved_record.requires_recovery_scan());
}

#[tokio::test(flavor = "multi_thread")]
async fn cp3c_side_records_reject_a_stale_attempt_and_a_wrong_expected_revision() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let (operation_id, claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::Truncate,
        attempt,
        validator.clone(),
    );
    let mutation_operation_id = Uuid::now_v7();

    // A stale coordination attempt cannot install a fence receipt.
    let stale_attempt = Uuid::now_v7();
    let stale_validator = Arc::new(TestTransactionValidator::default());
    let stale = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    mutation_operation_id,
                    stale_attempt,
                    fence_generation(1, 7, 1),
                    None,
                ),
            },
            Some(18_400),
            DmlMutationAuthority::try_new(stale_attempt, stale_validator.clone()).unwrap(),
        )
        .unwrap_err();
    assert_eq!(stale.kind(), DmlErrorKind::JournalUnresolved);
    assert!(stale.to_string().contains("another coordination attempt"));
    assert_eq!(stale_validator.calls(), 1);
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed);
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        None
    );

    // A fence minted by a foreign attempt is refused even under live authority.
    let foreign = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    mutation_operation_id,
                    Uuid::now_v7(),
                    fence_generation(1, 7, 1),
                    None,
                ),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(foreign.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        foreign
            .to_string()
            .contains("was minted by another coordination attempt")
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed);

    // A wrong expected revision cannot change durable state either.
    let wrong_revision = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed.revision + 5,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    mutation_operation_id,
                    attempt,
                    fence_generation(1, 7, 1),
                    None,
                ),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(wrong_revision.kind(), DmlErrorKind::JournalUnresolved);
    assert!(wrong_revision.to_string().contains("revision changed"));
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed);
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        None
    );

    // The same two rejections hold for a historical data-mutation recovery.
    let recovery = data_mutation_requested(
        attempt,
        data_mutation_request(
            DmlDirectMutationKind::Truncate,
            mutation_operation_id,
            Uuid::now_v7(),
            None,
            None,
        ),
    );
    let wrong_revision = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed.revision + 5,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(wrong_revision.kind(), DmlErrorKind::JournalUnresolved);
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        None
    );

    let foreign_recovery = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                recovery: DmlHistoricalDataMutationRecoveryRecord {
                    recovery_attempt_id: Uuid::now_v7(),
                    ..recovery
                },
            },
            Some(18_400),
            authority(),
        )
        .unwrap_err();
    assert_eq!(foreign_recovery.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        foreign_recovery
            .to_string()
            .contains("belongs to another coordination attempt")
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed);
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn historical_data_mutation_recovery_reopens_the_scan_for_a_terminal_truncate() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let attempt = Uuid::now_v7();
    let validator = Arc::new(TestTransactionValidator::default());
    let authority = || DmlMutationAuthority::try_new(attempt, validator.clone()).unwrap();
    let (operation_id, claimed) = claim_direct_mutation(
        &journal,
        DmlDirectMutationKind::Truncate,
        attempt,
        validator.clone(),
    );

    // The statement already failed and left the recovery scan.
    let terminal = journal
        .mutate_statement_operation_authorized(
            OperationMutationRequest {
                operation_id,
                expected_revision: claimed.revision,
                mutation_id: Uuid::now_v7(),
                state: OperationState::FailedKnownUncommitted,
                payload: truncate_failed(),
            },
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(terminal.recovery_due_at_ms, None);
    assert!(
        (0..journal.recovery_shard_count()).all(|shard| journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty()),
        "a terminal TRUNCATE leaves the scan"
    );

    // Opening a historical data-mutation recovery puts it back into the scan
    // even though the user-visible statement result is already terminal.
    let recovery = data_mutation_requested(
        attempt,
        data_mutation_request(
            DmlDirectMutationKind::Truncate,
            Uuid::now_v7(),
            Uuid::now_v7(),
            None,
            None,
        ),
    );
    let reopened = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: terminal.revision,
                mutation_id: Uuid::now_v7(),
                recovery: recovery.clone(),
            },
            Some(20_000),
            authority(),
        )
        .unwrap();
    assert_eq!(reopened.state, OperationState::FailedKnownUncommitted);
    assert_eq!(reopened.recovery_due_at_ms, Some(20_000));
    let shard = (0..journal.recovery_shard_count())
        .find(|shard| {
            journal
                .recovery_candidates(*shard, i64::MAX)
                .unwrap()
                .iter()
                .any(|candidate| candidate.operation_id == operation_id)
        })
        .expect("an open historical data mutation recovery must keep the operation scannable");
    assert!(journal.list_unfinished().unwrap().is_empty());

    // Opening it without a due is refused: the obligation cannot be invisible.
    let unscanned = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: reopened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: DmlHistoricalDataMutationRecoveryRecord {
                    updated_at_ms: 1_100,
                    ..recovery.clone()
                },
            },
            None,
            authority(),
        )
        .unwrap_err();
    assert_eq!(unscanned.kind(), DmlErrorKind::JournalUnresolved);
    assert!(
        unscanned.to_string().contains(
            "cannot drop its recovery due while historical data mutation recovery phase REQUESTED"
        ),
        "{unscanned}"
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), reopened);

    // Resolving it releases the scan in the same fenced mutation.
    let resolved_record = DmlHistoricalDataMutationRecoveryRecord {
        phase: DmlHistoricalRecoveryPhase::Resolved,
        raised_fence: Some(direct_mutation_fence(
            DmlDirectMutationKind::Truncate,
            recovery.request.mutation_operation_id,
            attempt,
            fence_generation(1, 9, 1),
            None,
        )),
        result: Some(data_mutation_result(
            DmlHistoricalDataMutationDisposition::NotApplied,
            None,
            DmlHistoricalCleanupState::NotRequired,
            false,
        )),
        next_action: StatementNextAction::None,
        updated_at_ms: 1_400,
        ..recovery
    };
    let resolved = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: reopened.revision,
                mutation_id: Uuid::now_v7(),
                recovery: resolved_record.clone(),
            },
            None,
            authority(),
        )
        .unwrap();
    assert_eq!(resolved.recovery_due_at_ms, None);
    assert!(
        journal
            .recovery_candidates(shard, i64::MAX)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        Some(resolved_record)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_cp1_superseded_holder_cannot_write_cp3c_side_records() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let gate = IncarnationGate::new(Arc::clone(&store));
    gate.bootstrap(OperationId::new_v7()).await.unwrap();
    let operation_id = DmlOperationId::new_v7();
    journal
        .create_statement_operation_admitted(
            statement_request(
                operation_id,
                Uuid::now_v7(),
                OperationKind::Truncate,
                truncate_preparing(),
            ),
            Arc::new(TestTransactionValidator::default()),
        )
        .unwrap();

    let clock = Arc::new(ManualDmlLeaseClock::new(500_000, 20_000));
    let holder_a = Uuid::now_v7();
    let holder_b = Uuid::now_v7();
    let manager_a = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_a),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let manager_b = LeaseManager::new(
        Arc::clone(&store),
        holder_id(holder_b),
        clock.clone(),
        lease_settings(),
    )
    .unwrap();
    let resource = dml_operation_resource_key(operation_id).unwrap();
    let attempt_a = Uuid::now_v7();
    let attempt_b = Uuid::now_v7();
    let guard_a = Arc::new(AsyncMutex::new(acquired(
        manager_a
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_a).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));
    let claimed_a = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: 1,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder_a, attempt_a, &guard_a, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 500_100,
            },
            current_authority(attempt_a, Arc::clone(&guard_a), Arc::new(Mutex::new(None))),
        )
        .unwrap();

    clock.advance_wall(16_001);
    assert!(matches!(
        manager_b
            .acquire(
                resource.clone(),
                AttemptId::try_from(attempt_b).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
        AcquireOutcome::AwaitingTakeover(_)
    ));
    clock.advance_monotonic(2_000);
    let guard_b = Arc::new(AsyncMutex::new(acquired(
        manager_b
            .acquire(
                resource,
                AttemptId::try_from(attempt_b).unwrap(),
                OperationId::new_v7(),
            )
            .await
            .unwrap(),
    )));

    // Holder A still believes it owns the TRUNCATE. Its direct-mutation fence
    // receipt must be refused by the latest live fence, not by a snapshot it
    // captured when it acquired the lease.
    let mutation_operation_id = Uuid::now_v7();
    let rejection = Arc::new(Mutex::new(None));
    let error = journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                fence: direct_mutation_fence(
                    DmlDirectMutationKind::Truncate,
                    mutation_operation_id,
                    attempt_a,
                    fence_generation(1, 7, 1),
                    None,
                ),
            },
            Some(500_200),
            current_authority(attempt_a, Arc::clone(&guard_a), Arc::clone(&rejection)),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed_a);
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        None
    );

    let recovery_rejection = Arc::new(Mutex::new(None));
    let error = journal
        .record_historical_data_mutation_recovery_authorized(
            DmlHistoricalDataMutationRecoveryMutationRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                recovery: data_mutation_requested(
                    attempt_a,
                    data_mutation_request(
                        DmlDirectMutationKind::Truncate,
                        mutation_operation_id,
                        Uuid::now_v7(),
                        None,
                        None,
                    ),
                ),
            },
            Some(500_200),
            current_authority(
                attempt_a,
                Arc::clone(&guard_a),
                Arc::clone(&recovery_rejection),
            ),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert_eq!(
        *recovery_rejection.lock().unwrap(),
        Some(CoordinationErrorKind::FenceLost)
    );
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), claimed_a);
    assert_eq!(
        journal
            .load_historical_data_mutation_recovery(operation_id)
            .unwrap(),
        None
    );

    // The new owner re-claims and installs its own strictly higher fence.
    let claimed_b = journal
        .claim_operation(
            DmlCoordinationClaimRequest {
                operation_id,
                expected_revision: claimed_a.revision,
                mutation_id: Uuid::now_v7(),
                provenance: real_provenance(holder_b, attempt_b, &guard_b, clock.wall_ms() as i64)
                    .await,
                recovery_due_at_ms: 516_100,
            },
            current_authority(attempt_b, Arc::clone(&guard_b), Arc::new(Mutex::new(None))),
        )
        .unwrap();
    let fence_b = direct_mutation_fence(
        DmlDirectMutationKind::Truncate,
        mutation_operation_id,
        attempt_b,
        fence_generation(1, 8, 1),
        None,
    );
    journal
        .record_direct_mutation_fence_authorized(
            DmlDirectMutationFenceMutationRequest {
                operation_id,
                expected_revision: claimed_b.revision,
                mutation_id: Uuid::now_v7(),
                fence: fence_b.clone(),
            },
            Some(516_200),
            current_authority(attempt_b, Arc::clone(&guard_b), Arc::new(Mutex::new(None))),
        )
        .unwrap();
    assert_eq!(
        journal.load_direct_mutation_fence(operation_id).unwrap(),
        Some(fence_b)
    );
}
