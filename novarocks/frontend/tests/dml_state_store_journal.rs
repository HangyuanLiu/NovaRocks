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
    DML_EXTERNAL_FACT_ENCODED_LIMIT, DML_LEGACY_OPERATION_SCHEMA_VERSION,
};
use novarocks_frontend::dml::{
    CreatePreparingRequest, CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord,
    DmlErrorKind, DmlOperationId, DurableExternalFact, ExternalFactOutcome,
    IcebergCommitOutcomeRecord, OperationFact, OperationJournal, OperationKind,
    OperationMutationRequest, OperationPayload, OperationState, OperationTarget,
    StateStoreOperationJournal, StatementNextAction, TruncateLifecyclePhase,
    TruncateLifecycleRecord,
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
use tempfile::TempDir;
use uuid::{Uuid, Version};

const OPERATION_PREFIX: &str = "novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &str = "novarocks/frontend/dml/v1/unfinished/";

fn config(path: &std::path::Path) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "dml-journal-test".to_string(),
                limits: StateStoreLimitOverrides::default(),
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
    let registry = builtin_state_store_provider_registry().expect("provider registry");
    let host = StateStoreHost::open(
        &registry,
        config(path),
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
    let (host, store, journal) = open_store(&path).await;
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
    drop(host);
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

fn ctas_payload(phase: CtasSagaPhase) -> OperationPayload {
    OperationPayload::CtasSaga(CtasSagaRecord {
        phase,
        prepare_operation_id: Uuid::now_v7(),
        write_operation_id: Uuid::now_v7(),
        publish_operation_id: Uuid::now_v7(),
        abort_staging_operation_id: Uuid::now_v7(),
        create_policy: "FAIL_IF_EXISTS".to_string(),
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some(7),
        source_plan_digest: Some("source-digest".to_string()),
        staged_handle_digest: None,
        aggregate_write_digest: None,
        prepare_fact: None,
        publish_fact: None,
        abort_staging_fact: None,
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
async fn literal_v1_mutation_upgrades_record_to_v2() {
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
    assert_eq!(upgraded.schema_version, 2);
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
                connector_instance_id: None,
                connector_incarnation: None,
                target_ref: "main".to_string(),
                request_digest: None,
                plan_digest: None,
                state_digest: None,
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
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some(9),
        target_ref: "main".to_string(),
        request_digest: Some("request".to_string()),
        plan_digest: None,
        state_digest: None,
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
        connector_instance_id: Some("iceberg-rest".to_string()),
        connector_incarnation: Some(9),
        target_ref: "main".to_string(),
        request_digest: Some("request".to_string()),
        plan_digest: None,
        state_digest: None,
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
        evidence: Some("e".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT + 1)),
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
async fn statement_external_fact_at_encoded_limit_is_preserved() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = DmlOperationId::new_v7();
    let evidence = "e".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT);
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
