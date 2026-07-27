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
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_frontend::view::repository::{
    DatabaseMutation, StoredDatabaseViewsV1, ViewRepository, database_key, decode_record,
    encode_record,
};
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitResolution, Key, Precondition, RangePage,
    RangeRequest, ReadTransaction, StateRecord, StateStore, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StateStoreMetricsSnapshot, StoreIdentity, TransactionId, Value,
    WriteTransaction,
};
use novarocks_state_store::{
    FeDeploymentView, StateStoreConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
    StateStoreRuntime, open_state_store,
};
use tempfile::TempDir;
use uuid::Uuid;

fn record(catalog: &str, database: &str) -> StoredDatabaseViewsV1 {
    StoredDatabaseViewsV1 {
        schema_version: 1,
        catalog: catalog.to_string(),
        database: database.to_string(),
        last_operation_id: Uuid::now_v7(),
        views: BTreeMap::new(),
    }
}

#[test]
fn database_record_key_is_versioned_and_unambiguous() {
    let key = database_key("default_catalog", "db/a").unwrap();
    assert_eq!(
        key.as_bytes(),
        b"novarocks/frontend/views/v1/64656661756c745f636174616c6f67/64622f61"
    );
}

#[test]
fn decode_rejects_key_value_identity_mismatch() {
    let value = encode_record(record("default_catalog", "other")).unwrap();
    assert!(
        decode_record(database_key("default_catalog", "db").unwrap(), value)
            .unwrap_err()
            .contains("view record identity mismatch")
    );
}

#[test]
fn codec_rejects_unknown_schema_non_normalized_names_non_queries_and_oversized_values() {
    let mut unknown_schema = record("default_catalog", "db");
    unknown_schema.schema_version = 2;
    assert!(
        encode_record(unknown_schema)
            .unwrap_err()
            .contains("unsupported frontend view database schema version")
    );

    let mut non_normalized = record("default_catalog", "DB");
    non_normalized.views.insert("v".into(), "SELECT 1".into());
    assert!(
        encode_record(non_normalized)
            .unwrap_err()
            .contains("database is not normalized")
    );

    let mut non_query = record("default_catalog", "db");
    non_query.views.insert("v".into(), "DROP TABLE t".into());
    assert!(
        encode_record(non_query)
            .unwrap_err()
            .contains("exactly one query statement")
    );

    let mut oversized = record("default_catalog", "db");
    oversized
        .views
        .insert("v".into(), format!("SELECT '{}'", "x".repeat(70 * 1024)));
    assert!(
        encode_record(oversized)
            .unwrap_err()
            .contains("encode frontend view database default_catalog.db failed")
    );
}

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "view-repository-test".to_string(),
        limits: StateStoreLimitOverrides {
            max_page_size: Some(1),
            ..StateStoreLimitOverrides::default()
        },
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "view-repository-fe".to_string(),
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
            topology_revision: Bytes::from_static(b"view-repository-topology"),
        },
    )
    .await
    .expect("open SQLite state store")
}

fn create(view: &str, sql: &str, or_replace: bool) -> DatabaseMutation {
    DatabaseMutation::Create {
        view: view.to_string(),
        sql: sql.to_string(),
        or_replace,
    }
}

#[tokio::test]
async fn repository_mutations_are_atomic_and_catalog_isolated() {
    let temp = TempDir::new().unwrap();
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let repository = ViewRepository::open(Arc::clone(&store), tokio::runtime::Handle::current())
        .await
        .unwrap();

    let created = repository
        .mutate_database("default_catalog", "db", create("v", "select 1", false))
        .await
        .unwrap();
    assert_eq!(created.views.get("v").map(String::as_str), Some("SELECT 1"));

    let duplicate = repository
        .mutate_database("default_catalog", "db", create("v", "SELECT 2", false))
        .await
        .unwrap_err();
    assert_eq!(duplicate, "view already exists: db.v");
    assert_eq!(
        repository.load_all().await.unwrap()[0]
            .views
            .get("v")
            .map(String::as_str),
        Some("SELECT 1")
    );

    repository
        .mutate_database("default_catalog", "db", create("v", "SELECT 2", true))
        .await
        .unwrap();
    repository
        .mutate_database("other_catalog", "db", create("v", "SELECT 3", false))
        .await
        .unwrap();

    repository
        .mutate_database(
            "default_catalog",
            "db",
            DatabaseMutation::DropView {
                view: "missing".to_string(),
            },
        )
        .await
        .unwrap();
    let dropped = repository
        .mutate_database("default_catalog", "db", DatabaseMutation::DropDatabase)
        .await
        .unwrap();
    assert!(dropped.views.is_empty());

    let loaded = repository.load_all().await.unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(
        loaded
            .iter()
            .find(|record| record.catalog == "default_catalog")
            .unwrap()
            .views
            .is_empty()
    );
    assert_eq!(
        loaded
            .iter()
            .find(|record| record.catalog == "other_catalog")
            .unwrap()
            .views
            .get("v")
            .map(String::as_str),
        Some("SELECT 3")
    );
}

#[tokio::test]
async fn repository_reopens_from_durable_records() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let store = open_sqlite(&path).await;
    let repository = ViewRepository::open(store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    repository
        .mutate_database("default_catalog", "db", create("v", "SELECT 42", false))
        .await
        .unwrap();
    drop(repository);

    let reopened_store = open_sqlite(&path).await;
    let reopened = ViewRepository::open(reopened_store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let loaded = reopened.load_all().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(
        loaded[0].views.get("v").map(String::as_str),
        Some("SELECT 42")
    );
}

async fn write_raw(store: &dyn StateStore, key: Key, value: Value) {
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "write corrupt frontend view record",
        )
        .await
        .unwrap();
    transaction
        .put(key, value, Precondition::Absent)
        .await
        .unwrap();
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
}

struct CommitUnknownStore {
    inner: Arc<dyn StateStore>,
    apply_before_unknown: bool,
}

#[async_trait]
impl StateStore for CommitUnknownStore {
    fn provider_name(&self) -> &'static str {
        "commit-unknown-test"
    }

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
            inner: self.inner.begin_write(transaction_id, purpose).await?,
            apply_before_unknown: self.apply_before_unknown,
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
}

#[async_trait]
impl ReadTransaction for CommitUnknownTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
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
        }
        CommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Transient,
            "scripted frontend view commit outcome is unknown",
        ))
    }
}

#[tokio::test]
async fn repository_resolves_commit_unknown_only_from_authoritative_operation_id() {
    let committed_temp = TempDir::new().unwrap();
    let committed_inner = open_sqlite(&committed_temp.path().join("state.sqlite")).await;
    let committed_store: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: committed_inner,
        apply_before_unknown: true,
    });
    let committed = ViewRepository::open(committed_store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let record = committed
        .mutate_database("default_catalog", "db", create("v", "SELECT 7", false))
        .await
        .unwrap();
    assert_eq!(record.views.get("v").map(String::as_str), Some("SELECT 7"));

    let unresolved_temp = TempDir::new().unwrap();
    let unresolved_inner = open_sqlite(&unresolved_temp.path().join("state.sqlite")).await;
    let unresolved_store: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: unresolved_inner,
        apply_before_unknown: false,
    });
    let unresolved = ViewRepository::open(unresolved_store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let error = unresolved
        .mutate_database("default_catalog", "db", create("v", "SELECT 8", false))
        .await
        .unwrap_err();
    assert!(error.contains("commit outcome is unresolved"));
    assert!(unresolved.load_all().await.unwrap().is_empty());
}

#[tokio::test]
async fn repository_open_fails_fast_on_corrupt_records() {
    let temp = TempDir::new().unwrap();
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    write_raw(
        store.as_ref(),
        database_key("default_catalog", "db").unwrap(),
        Value::try_from(Bytes::from_static(b"{not-json")).unwrap(),
    )
    .await;

    let error = ViewRepository::open(store, tokio::runtime::Handle::current())
        .await
        .unwrap_err();
    assert!(error.contains("decode frontend view database default_catalog.db failed"));
}
