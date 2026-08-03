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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_frontend::dml::model::{
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    DML_CTAS_FACT_ENCODED_LIMIT, DML_CTAS_TOTAL_FACT_ENCODED_LIMIT,
    DML_EXTERNAL_FACT_ENCODED_LIMIT, DML_LEGACY_OPERATION_SCHEMA_VERSION,
    DML_OPERATION_SCHEMA_VERSION,
};
use novarocks_frontend::dml::{
    AddFilesArtifact, AddFilesArtifactDescriptor, AddFilesArtifactKind, AddFilesDispatchCertainty,
    AddFilesLifecyclePhase, AddFilesLifecycleRecord, AddFilesMutationRequest, AddFilesSourceAction,
    CreatePreparingRequest, CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord,
    DmlErrorKind, DmlOperationId, DurableExternalFact, DurableMutationSummary, ExternalFactOutcome,
    IcebergCommitOutcomeRecord, OperationFact, OperationJournal, OperationKind,
    OperationMutationRequest, OperationPayload, OperationState, OperationTarget,
    SourceScopeOwnership, StateStoreOperationJournal, StatementNextAction, StoredOperation,
    TruncateLifecyclePhase, TruncateLifecycleRecord,
};
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome as StateStoreCommitOutcome, CommitResolution,
    FeDeploymentView, Key, Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord,
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, Value, WriteTransaction,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::{Uuid, Version};

const OPERATION_PREFIX: &str = "novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &str = "novarocks/frontend/dml/v1/unfinished/";

fn config(path: &std::path::Path) -> StateStoreHostConfig {
    config_with_max_value_bytes(path, None)
}

fn config_with_max_value_bytes(
    path: &std::path::Path,
    max_value_bytes: Option<usize>,
) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "dml-journal-test".to_string(),
                limits: StateStoreLimitOverrides {
                    max_value_bytes,
                    ..StateStoreLimitOverrides::default()
                },
                provider: StateStoreProviderConfig::Sqlite {
                    path: path.to_path_buf(),
                    deployment_owner: "dml-journal-fe".to_string(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
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
    let registry = builtin_state_store_provider_registry().expect("provider registry");
    let host = StateStoreHost::open(
        &registry,
        config_with_max_value_bytes(path, max_value_bytes),
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"dml-journal-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite StateStore");
    let store = host.state_store().expect("StateStore exposure");
    let journal =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("open DML journal");
    (host, store, journal)
}

fn request() -> CreatePreparingRequest {
    CreatePreparingRequest {
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
        "commit_outcome": null,
        "cleanup_outcome": null,
        "recovery_evidence": null,
        "failure": null,
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
                commit_outcome: Some(IcebergCommitOutcomeRecord {
                    snapshot_id: 11,
                    written_manifest_paths: vec![],
                }),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
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
async fn typed_abort_unknown_remains_in_unfinished_index_after_restart() {
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
                state: OperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
            },
        )
        .unwrap();
    drop(journal);
    drop(store);
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    let (_host, _store, reopened) = open_store(&path).await;
    let unfinished = reopened.list_unfinished().unwrap();
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].operation_id, operation_id);
    assert_eq!(unfinished[0].state, OperationState::CommitUnknown);
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
        commit_outcome: Some(IcebergCommitOutcomeRecord {
            snapshot_id: 7,
            written_manifest_paths: vec!["m.avro".to_string()],
        }),
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: None,
    };
    journal.record_fact(operation_id, fact.clone()).unwrap();
    journal.record_fact(operation_id, fact).unwrap();
    assert_eq!(
        journal
            .load(operation_id)
            .unwrap()
            .unwrap()
            .commit_outcome
            .unwrap()
            .snapshot_id,
        7
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
    let fact = |snapshot_id| OperationFact {
        state: OperationState::Committed,
        commit_outcome: Some(IcebergCommitOutcomeRecord {
            snapshot_id,
            written_manifest_paths: Vec::new(),
        }),
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: None,
    };
    journal.record_fact(operation_id, fact(7)).unwrap();
    let error = journal.record_fact(operation_id, fact(8)).unwrap_err();
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
async fn unknown_schema_version_fails_open() {
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
    let error =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .err()
            .expect("unknown schema must fail open");
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
        raw_operation(value_id, 1),
    )
    .await;
    let error =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .err()
            .expect("identity mismatch must fail open");
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
        aggregate_write_digest: None,
        prepare_fact,
        write_fact,
        publish_fact,
        abort_staging_fact,
        next_action: StatementNextAction::None,
    })
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
        commit_outcome: None,
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: None,
        payload,
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
async fn add_files_atomic_reservation_rejects_conflicts_and_restart_releases_undispatched_work() {
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
    assert_eq!(
        recovered_first.state,
        OperationState::FailedKnownUncommitted
    );
    assert!(
        recovered
            .list_unfinished()
            .unwrap()
            .iter()
            .all(|op| op.operation_id != first_id)
    );

    let second_after_recovery = recovered.load(second_id).unwrap().unwrap();
    assert_eq!(
        second_after_recovery.state,
        OperationState::FailedKnownUncommitted
    );
    let third_id = DmlOperationId::new_v7();
    let third = recovered
        .create_statement_operation(statement_request(
            third_id,
            Uuid::now_v7(),
            OperationKind::AddFiles,
            add_files_preparing(),
        ))
        .unwrap();
    recovered
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
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn literal_v1_record_decodes_to_normalized_write_payload() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        raw_operation(operation_id, DML_LEGACY_OPERATION_SCHEMA_VERSION),
    )
    .await;

    let stored = journal
        .load(DmlOperationId::from(operation_id))
        .unwrap()
        .unwrap();
    assert_eq!(stored.schema_version, DML_LEGACY_OPERATION_SCHEMA_VERSION);
    assert_eq!(stored.payload, OperationPayload::WriteV1);
}

#[tokio::test(flavor = "multi_thread")]
async fn literal_v1_record_without_subkind_decodes_as_none() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = Uuid::now_v7();
    let value = json!({
        "schema_version": DML_LEGACY_OPERATION_SCHEMA_VERSION,
        "operation_id": operation_id,
        "revision": 1,
        "last_mutation_id": Uuid::now_v7(),
        "operation_kind": "ROW_DELTA",
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
        "commit_outcome": null,
        "cleanup_outcome": null,
        "recovery_evidence": null,
        "failure": null,
        "created_at_ms": 1,
        "updated_at_ms": 1,
        "finished_at_ms": null
    });
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        Value::try_from(Bytes::from(serde_json::to_vec(&value).unwrap())).unwrap(),
    )
    .await;

    let stored = journal
        .load(DmlOperationId::from(operation_id))
        .unwrap()
        .unwrap();
    assert_eq!(stored.operation_kind, OperationKind::RowDelta);
    assert_eq!(stored.operation_subkind, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn literal_v1_delete_record_decodes_to_normalized_write_payload() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        raw_operation_with_kind(
            operation_id,
            DML_LEGACY_OPERATION_SCHEMA_VERSION,
            "ROW_DELTA",
        ),
    )
    .await;

    let stored = journal
        .load(DmlOperationId::from(operation_id))
        .unwrap()
        .unwrap();
    assert_eq!(stored.operation_kind, OperationKind::RowDelta);
    assert_eq!(stored.payload, OperationPayload::WriteV1);
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
async fn literal_v1_mutation_upgrades_record_to_current_schema() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_uuid = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_uuid),
        raw_operation(operation_uuid, DML_LEGACY_OPERATION_SCHEMA_VERSION),
    )
    .await;
    raw_put(
        store.as_ref(),
        key(UNFINISHED_PREFIX, operation_uuid),
        raw_unfinished(operation_uuid),
    )
    .await;
    let operation_id = DmlOperationId::from(operation_uuid);
    let upgraded = journal
        .mutate_statement_operation(OperationMutationRequest {
            operation_id,
            expected_revision: 1,
            mutation_id: Uuid::now_v7(),
            state: OperationState::Writing,
            payload: OperationPayload::WriteV1,
        })
        .unwrap();
    assert_eq!(upgraded.schema_version, DML_OPERATION_SCHEMA_VERSION);
    assert_eq!(journal.load(operation_id).unwrap().unwrap(), upgraded);
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
