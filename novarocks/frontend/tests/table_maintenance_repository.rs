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

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks::engine::table_maintenance::{MaintenanceTarget, OptimizeJobState};
use novarocks_frontend::table_maintenance::model::{OptimizeJobCreate, OptimizeJobOutcome};
use novarocks_frontend::table_maintenance::repository::{
    OptimizeJobRepository, RepositoryErrorKind,
};
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitResolution, Direction, Key, KeyRange,
    Precondition, RangePage, RangeRequest, ReadTransaction, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot, StoreIdentity, TransactionId,
    Value, WriteTransaction,
};
use novarocks_state_store::{
    FeDeploymentView, StateStoreConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
    StateStoreRuntime, open_state_store,
};
use tempfile::TempDir;
use tokio::sync::Notify;
use uuid::Uuid;

const PREFIX: &str = "novarocks/frontend/table-maintenance/v1/";

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "table-maintenance-repository-test".to_string(),
        limits: StateStoreLimitOverrides {
            max_page_size: Some(1),
            ..StateStoreLimitOverrides::default()
        },
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "table-maintenance-repository-fe".to_string(),
        },
    }
}

async fn open_sqlite(path: &Path) -> Arc<dyn StateStore> {
    let runtime = StateStoreRuntime::local().expect("local state-store runtime");
    open_state_store(
        &runtime,
        sqlite_config(path),
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"table-maintenance-repository-topology"),
        },
    )
    .await
    .expect("open SQLite state store")
}

async fn fixture() -> (TempDir, Arc<dyn StateStore>, OptimizeJobRepository) {
    let temp = TempDir::new().expect("create temp directory");
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let repository = OptimizeJobRepository::open(Arc::clone(&store))
        .await
        .expect("open optimize job repository");
    (temp, store, repository)
}

fn target(catalog: &str, namespace: &str, table: &str) -> MaintenanceTarget {
    MaintenanceTarget {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
    }
}

fn create_request(
    catalog: &str,
    namespace: &str,
    table: &str,
    base_snapshot_id: i64,
    created_at_ms: i64,
) -> OptimizeJobCreate {
    OptimizeJobCreate {
        target: target(catalog, namespace, table),
        base_snapshot_id,
        created_at_ms,
    }
}

fn outcome(target_snapshot_id: i64) -> OptimizeJobOutcome {
    OptimizeJobOutcome {
        target_snapshot_id: Some(target_snapshot_id),
        rewritten_data_files: 4,
        deleted_data_files: 4,
        added_data_files: 2,
        output_record_count: 88,
    }
}

fn raw_key(suffix: &str) -> Key {
    Key::try_from(Bytes::from(format!("{PREFIX}{suffix}"))).expect("valid raw key")
}

fn active_key(target: &MaintenanceTarget) -> Key {
    raw_key(&format!(
        "active/{}/{}/{}",
        hex::encode(target.catalog.as_bytes()),
        hex::encode(target.namespace.as_bytes()),
        hex::encode(target.table.as_bytes())
    ))
}

async fn raw_key_exists(store: &dyn StateStore, key: &Key) -> bool {
    let mut transaction = store.begin_read().await.expect("begin raw key read");
    let exists = transaction.get(key).await.expect("read raw key").is_some();
    transaction.abort().await.expect("finish raw key read");
    exists
}

async fn raw_prefix_count(store: &dyn StateStore, suffix: &str) -> usize {
    let prefix = raw_key(suffix);
    let range = KeyRange::for_prefix(prefix).expect("valid raw prefix");
    let mut transaction = store.begin_read().await.expect("begin raw prefix read");
    let mut request = RangeRequest {
        range,
        direction: Direction::Forward,
        page_size: store.limits().max_page_size,
        continuation: None,
    };
    let mut count = 0;
    loop {
        let page = transaction
            .range(&request)
            .await
            .expect("read raw prefix page");
        count += page.records.len();
        let Some(continuation) = page.continuation else {
            break;
        };
        request.continuation = Some(continuation);
    }
    transaction.abort().await.expect("finish raw prefix read");
    count
}

async fn write_raw(store: &dyn StateStore, key: Key, value: Value) {
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "write optimize repository test record",
        )
        .await
        .expect("begin raw write");
    transaction
        .put(key, value, Precondition::Absent)
        .await
        .expect("put raw record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
}

#[tokio::test]
async fn create_allocates_monotonic_ids_and_writes_job_pending_active_and_operation_records() {
    let (_temp, store, repository) = fixture().await;
    let first_target = target("ice", "db", "t1");
    let first = repository
        .create(create_request("ice", "db", "t1", 10, 100))
        .await
        .expect("create first optimize job");
    let second = repository
        .create(create_request("ice", "db", "t2", 20, 101))
        .await
        .expect("create second optimize job");

    assert_eq!(first.job_id, 1);
    assert_eq!(second.job_id, 2);
    assert_eq!(first.state, OptimizeJobState::Pending);
    assert_eq!(second.state, OptimizeJobState::Pending);
    assert_eq!(
        repository.list_pending().await.unwrap(),
        vec![first, second]
    );
    assert!(raw_key_exists(store.as_ref(), &raw_key("jobs/0000000000000001")).await);
    assert!(raw_key_exists(store.as_ref(), &raw_key("state/pending/0000000000000001")).await);
    assert!(raw_key_exists(store.as_ref(), &active_key(&first_target)).await);
    assert_eq!(raw_prefix_count(store.as_ref(), "operations/").await, 2);
}

#[tokio::test]
async fn duplicate_target_create_is_typed_and_does_not_create_a_second_job() {
    let (_temp, store, repository) = fixture().await;
    repository
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .expect("create optimize job");

    let duplicate = repository
        .create(create_request("ice", "db", "t", 11, 101))
        .await
        .unwrap_err();

    assert_eq!(duplicate.kind(), RepositoryErrorKind::AlreadyActive);
    assert!(duplicate.to_string().contains("ice.db.t"));
    assert_eq!(repository.list().await.unwrap().len(), 1);
    assert_eq!(raw_prefix_count(store.as_ref(), "jobs/").await, 1);
    assert_eq!(raw_prefix_count(store.as_ref(), "operations/").await, 1);
}

#[tokio::test]
async fn claim_atomically_moves_pending_job_to_running() {
    let (_temp, store, repository) = fixture().await;
    let created = repository
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .expect("create optimize job");

    let claimed = repository
        .claim(created.job_id, 200)
        .await
        .expect("claim optimize job")
        .expect("pending job is claimable");

    assert_eq!(claimed.state, OptimizeJobState::Running);
    assert_eq!(claimed.started_at_ms, Some(200));
    assert!(repository.list_pending().await.unwrap().is_empty());
    assert!(!raw_key_exists(store.as_ref(), &raw_key("state/pending/0000000000000001")).await);
    assert!(raw_key_exists(store.as_ref(), &raw_key("state/running/0000000000000001")).await);
    assert!(raw_key_exists(store.as_ref(), &active_key(&created.target)).await);
    assert_eq!(raw_prefix_count(store.as_ref(), "operations/").await, 2);
}

#[tokio::test]
async fn recorded_outcome_is_preserved_when_finish_clears_running_and_active_indexes() {
    let (_temp, store, repository) = fixture().await;
    let created = repository
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .expect("create optimize job");
    repository
        .claim(created.job_id, 200)
        .await
        .unwrap()
        .unwrap();
    let expected_outcome = outcome(11);

    repository
        .record_outcome(created.job_id, expected_outcome.clone())
        .await
        .expect("record optimize outcome");
    repository
        .finish(created.job_id, 300)
        .await
        .expect("finish optimize job");

    let finished = repository.list().await.unwrap().remove(0);
    assert_eq!(finished.state, OptimizeJobState::Finished);
    assert_eq!(finished.outcome, Some(expected_outcome));
    assert_eq!(finished.finished_at_ms, Some(300));
    assert_eq!(finished.error_message, None);
    assert!(!raw_key_exists(store.as_ref(), &raw_key("state/running/0000000000000001")).await);
    assert!(!raw_key_exists(store.as_ref(), &active_key(&created.target)).await);
    let replacement = repository
        .create(create_request("ice", "db", "t", 11, 301))
        .await
        .expect("finished target may be optimized again");
    assert_eq!(replacement.job_id, 2);
}

#[tokio::test]
async fn fail_preserves_error_and_clears_running_and_active_indexes() {
    let (_temp, store, repository) = fixture().await;
    let created = repository
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .expect("create optimize job");
    repository
        .claim(created.job_id, 200)
        .await
        .unwrap()
        .unwrap();

    repository
        .fail(created.job_id, 300, "rewrite failed".to_string())
        .await
        .expect("fail optimize job");

    let failed = repository.list().await.unwrap().remove(0);
    assert_eq!(failed.state, OptimizeJobState::Failed);
    assert_eq!(failed.error_message.as_deref(), Some("rewrite failed"));
    assert_eq!(failed.finished_at_ms, Some(300));
    assert!(!raw_key_exists(store.as_ref(), &raw_key("state/running/0000000000000001")).await);
    assert!(!raw_key_exists(store.as_ref(), &active_key(&created.target)).await);
    repository
        .create(create_request("ice", "db", "t", 11, 301))
        .await
        .expect("failed target may be optimized again");
}

#[tokio::test]
async fn restart_reconcile_fails_running_jobs_and_leaves_pending_jobs_claimable() {
    let (_temp, store, repository) = fixture().await;
    let running = repository
        .create(create_request("ice", "db", "running", 10, 100))
        .await
        .expect("create running job");
    let pending = repository
        .create(create_request("ice", "db", "pending", 20, 101))
        .await
        .expect("create pending job");
    repository
        .claim(running.job_id, 200)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(repository.reconcile_startup(300).await.unwrap(), 1);

    let jobs = repository.list().await.unwrap();
    let reconciled = jobs
        .iter()
        .find(|job| job.job_id == running.job_id)
        .unwrap();
    assert_eq!(reconciled.state, OptimizeJobState::Failed);
    assert!(
        reconciled
            .error_message
            .as_deref()
            .unwrap()
            .contains("restart")
    );
    let still_pending = jobs
        .iter()
        .find(|job| job.job_id == pending.job_id)
        .unwrap();
    assert_eq!(still_pending.state, OptimizeJobState::Pending);
    assert!(
        repository
            .claim(pending.job_id, 400)
            .await
            .unwrap()
            .is_some()
    );
    assert!(!raw_key_exists(store.as_ref(), &raw_key("state/running/0000000000000001")).await);
}

#[tokio::test]
async fn encoded_target_keys_do_not_collide_for_slashes_spaces_or_non_ascii() {
    let (_temp, _store, repository) = fixture().await;
    let first = repository
        .create(create_request("ice/a", "name space", "表", 10, 100))
        .await
        .expect("create first specially named target");
    let second = repository
        .create(create_request("ice", "a/name space", "表", 20, 101))
        .await
        .expect("create second specially named target");

    assert_ne!(first.target, second.target);
    assert_eq!(repository.list().await.unwrap(), vec![first, second]);
}

struct CommitUnknownStore {
    inner: Arc<dyn StateStore>,
    apply_before_unknown: bool,
    begin_write_count: Arc<AtomicUsize>,
    recovery_gate: Option<Arc<RecoveryGate>>,
}

#[async_trait]
impl StateStore for CommitUnknownStore {
    fn provider_name(&self) -> &'static str {
        "optimize-commit-unknown-test"
    }

    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        if let Some(gate) = &self.recovery_gate {
            gate.pause_if_armed().await;
        }
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.begin_write_count.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(CommitUnknownTransaction {
            inner: self.inner.begin_write(transaction_id, purpose).await?,
            apply_before_unknown: self.apply_before_unknown,
            recovery_gate: self.recovery_gate.clone(),
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

struct CommitUnknownTransaction {
    inner: Box<dyn WriteTransaction>,
    apply_before_unknown: bool,
    recovery_gate: Option<Arc<RecoveryGate>>,
}

#[async_trait]
impl ReadTransaction for CommitUnknownTransaction {
    async fn get(
        &mut self,
        key: &Key,
    ) -> Result<Option<novarocks_spi::state_store::StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner.abort().await
    }
}

#[async_trait]
impl WriteTransaction for CommitUnknownTransaction {
    fn transaction_id(&self) -> &TransactionId {
        self.inner.transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.delete(key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        if self.apply_before_unknown {
            assert!(matches!(
                self.inner.commit().await,
                CommitOutcome::Committed(_)
            ));
            if let Some(gate) = &self.recovery_gate {
                gate.arm();
            }
        }
        CommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Transient,
            "scripted optimize job commit outcome is unknown",
        ))
    }
}

#[derive(Default)]
struct RecoveryGate {
    armed: std::sync::atomic::AtomicBool,
    reached: Notify,
    release: Notify,
}

impl RecoveryGate {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn pause_if_armed(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    async fn wait_until_recovery(&self) {
        self.reached.notified().await;
    }

    fn allow_recovery(&self) {
        self.release.notify_one();
    }
}

#[tokio::test]
async fn commit_unknown_uses_authoritative_operation_marker_without_blind_retry() {
    let committed_temp = TempDir::new().unwrap();
    let committed_inner = open_sqlite(&committed_temp.path().join("state.sqlite")).await;
    let committed_writes = Arc::new(AtomicUsize::new(0));
    let committed_store: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: committed_inner,
        apply_before_unknown: true,
        begin_write_count: Arc::clone(&committed_writes),
        recovery_gate: None,
    });
    let committed = OptimizeJobRepository::open(Arc::clone(&committed_store))
        .await
        .unwrap();
    let created = committed
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .expect("authoritative marker proves commit");
    assert_eq!(created.job_id, 1);
    assert_eq!(committed.list().await.unwrap().len(), 1);
    assert_eq!(committed_writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        raw_prefix_count(committed_store.as_ref(), "operations/").await,
        1
    );

    let unresolved_temp = TempDir::new().unwrap();
    let unresolved_inner = open_sqlite(&unresolved_temp.path().join("state.sqlite")).await;
    let unresolved_writes = Arc::new(AtomicUsize::new(0));
    let unresolved_store: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: unresolved_inner,
        apply_before_unknown: false,
        begin_write_count: Arc::clone(&unresolved_writes),
        recovery_gate: None,
    });
    let unresolved = OptimizeJobRepository::open(Arc::clone(&unresolved_store))
        .await
        .unwrap();
    let error = unresolved
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), RepositoryErrorKind::CommitUnknown);
    assert!(error.to_string().contains("unresolved"));
    assert!(unresolved.list().await.unwrap().is_empty());
    assert_eq!(unresolved_writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        raw_prefix_count(unresolved_store.as_ref(), "operations/").await,
        0
    );
}

#[tokio::test]
async fn commit_unknown_recovery_accepts_legal_successor_without_mutation_retry() {
    let temp = TempDir::new().unwrap();
    let inner = open_sqlite(&temp.path().join("state.sqlite")).await;
    let advancing = OptimizeJobRepository::open(Arc::clone(&inner))
        .await
        .unwrap();
    let created = advancing
        .create(create_request("ice", "db", "t", 10, 100))
        .await
        .unwrap();

    let recovery_gate = Arc::new(RecoveryGate::default());
    let recovering_writes = Arc::new(AtomicUsize::new(0));
    let recovering_store: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: Arc::clone(&inner),
        apply_before_unknown: true,
        begin_write_count: Arc::clone(&recovering_writes),
        recovery_gate: Some(Arc::clone(&recovery_gate)),
    });
    let recovering = OptimizeJobRepository::open(recovering_store).await.unwrap();
    let claim = tokio::spawn(async move { recovering.claim(created.job_id, 200).await });

    recovery_gate.wait_until_recovery().await;
    advancing
        .record_outcome(created.job_id, outcome(11))
        .await
        .unwrap();
    advancing.finish(created.job_id, 300).await.unwrap();
    recovery_gate.allow_recovery();

    let recovered = claim
        .await
        .expect("join claim recovery")
        .expect("authoritative marker proves claim")
        .expect("claim returned its post-mutation result");
    assert_eq!(recovered.state, OptimizeJobState::Running);
    assert_eq!(recovered.started_at_ms, Some(200));
    assert_eq!(
        advancing.list().await.unwrap()[0].state,
        OptimizeJobState::Finished
    );
    assert_eq!(recovering_writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn repository_open_fails_fast_on_unknown_job_schema_version() {
    let temp = TempDir::new().unwrap();
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let payload = serde_json::json!({
        "schema_version": 2,
        "job_id": 1,
        "target": {
            "catalog": "ice",
            "namespace": "db",
            "table": "t"
        },
        "base_snapshot_id": 10,
        "state": "PENDING",
        "outcome": null,
        "error_message": null,
        "created_at_ms": 100,
        "started_at_ms": null,
        "finished_at_ms": null,
        "last_operation_id": Uuid::now_v7()
    });
    write_raw(
        store.as_ref(),
        raw_key("jobs/0000000000000001"),
        Value::try_from(Bytes::from(serde_json::to_vec(&payload).unwrap())).unwrap(),
    )
    .await;

    let error = OptimizeJobRepository::open(store).await.unwrap_err();
    assert_eq!(error.kind(), RepositoryErrorKind::Corruption);
    assert!(error.to_string().contains("schema version"));
}
