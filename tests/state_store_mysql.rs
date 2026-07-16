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

#![cfg(feature = "mysql-state-store-provider")]

use std::num::NonZeroUsize;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "state-store-test-hooks")]
use novarocks::state_store::mysql::test_support::{MysqlOpenGatePhase, arm_mysql_open_gate};
use novarocks::state_store::mysql::test_support::{
    MysqlSchemaColumnSnapshot, MysqlSchemaMutation, MysqlSchemaTableSnapshot,
    acquire_schema_advisory_lock, active_readiness, advisory_lock_name, apply_schema_mutation,
    is_schema_advisory_lock_free, schema_snapshot, schema_timeout_connection_is_destroyed,
    store_readiness_snapshot,
};
use novarocks::state_store::{
    FeDeploymentView, MySqlClientConfig, MySqlTlsMode, StateStore, StateStoreConfig,
    StateStoreErrorKind, StateStoreLimitOverrides, StateStoreProviderConfig, StateStoreRuntime,
    open_state_store,
};
use sha2::{Digest, Sha256};
use uuid::Version;

const CLUSTER_ID: &str = "mysql-schema-test-cluster";
const EXPECTED_SCHEMA_DIGEST: &str =
    "ddc1a524fb8fe17b143b3783d105267187e4a0d0019556ac0825cfa4c2a9faf7";

struct TestDatabase {
    name: String,
}

impl TestDatabase {
    fn provision(test_name: &str, suffix: &str) -> Self {
        let digest = Sha256::digest([test_name.as_bytes(), suffix.as_bytes()].concat());
        let prefix = test_name
            .chars()
            .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
            .take(12)
            .collect::<String>();
        let case_id = format!("ss3t4_{prefix}_{}", hex::encode(&digest[..4]));
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("docker/mysql-state-store/provision-test-database.sh");
        let output = Command::new(script)
            .args(["create", &case_id])
            .output()
            .expect("run MySQL test database provisioner child process");
        assert!(
            output.status.success(),
            "MySQL test database provisioner create failed"
        );
        let name = String::from_utf8(output.stdout)
            .expect("provisioner database output is UTF-8")
            .trim()
            .to_owned();
        assert!(!name.is_empty(), "provisioner returned an empty database");
        Self { name }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("docker/mysql-state-store/provision-test-database.sh");
        let status = Command::new(script).args(["drop", &self.name]).status();
        if !status.is_ok_and(|status| status.success()) && !std::thread::panicking() {
            panic!("MySQL test database provisioner drop failed");
        }
    }
}

fn fixture_client_config() -> MySqlClientConfig {
    MySqlClientConfig {
        host: std::env::var("NOVAROCKS_MYSQL_HOST").expect("fixture host"),
        port: std::env::var("NOVAROCKS_MYSQL_PORT")
            .expect("fixture port")
            .parse()
            .expect("numeric fixture port"),
        username: std::env::var("NOVAROCKS_MYSQL_USERNAME").expect("fixture username"),
        password_env: std::env::var("NOVAROCKS_MYSQL_PASSWORD_ENV")
            .expect("fixture password environment name"),
        tls_mode: MySqlTlsMode::Disabled,
        tls_ca_path: None,
        tls_cert_path: None,
        tls_key_path: None,
        connect_timeout_ms: 1_000,
        pool_min: 1,
        pool_max: 4,
        inactive_connection_ttl_ms: 1_000,
    }
}

fn store_config(database: &str, cluster_id: &str, deadline_ms: u64) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: cluster_id.to_owned(),
        limits: StateStoreLimitOverrides {
            transaction_deadline_ms: Some(deadline_ms),
            ..StateStoreLimitOverrides::default()
        },
        provider: StateStoreProviderConfig::Mysql {
            database: database.to_owned(),
        },
    }
}

fn deployment() -> FeDeploymentView {
    FeDeploymentView {
        active_fe_count: NonZeroUsize::new(2).expect("two is non-zero"),
        topology_revision: Bytes::from_static(b"mysql-schema-test-topology"),
    }
}

async fn open_store(
    runtime: &StateStoreRuntime,
    database: &str,
    cluster_id: &str,
    deadline_ms: u64,
) -> Result<std::sync::Arc<dyn StateStore>, novarocks::state_store::StateStoreError> {
    open_state_store(
        runtime,
        store_config(database, cluster_id, deadline_ms),
        deployment(),
    )
    .await
}

fn expected_tables() -> Vec<MysqlSchemaTableSnapshot> {
    vec![
        MysqlSchemaTableSnapshot {
            name: "state_store_changes".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("revision", "bigint unsigned", false, 1),
                MysqlSchemaColumnSnapshot::new("sequence", "int unsigned", false, 2),
                MysqlSchemaColumnSnapshot::new("key_bytes", "varbinary(3072)", false, 0),
            ],
            primary_key: vec!["revision".to_owned(), "sequence".to_owned()],
            secondary_indexes: Vec::new(),
        },
        MysqlSchemaTableSnapshot {
            name: "state_store_commits".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("transaction_id", "binary(16)", false, 1),
                MysqlSchemaColumnSnapshot::new("state", "tinyint unsigned", false, 0),
                MysqlSchemaColumnSnapshot::new("reservation_token", "binary(16)", true, 0),
                MysqlSchemaColumnSnapshot::new("revision", "bigint unsigned", true, 0),
                MysqlSchemaColumnSnapshot::new("updated_at_ms", "bigint unsigned", false, 0),
            ],
            primary_key: vec!["transaction_id".to_owned()],
            secondary_indexes: Vec::new(),
        },
        MysqlSchemaTableSnapshot {
            name: "state_store_kv".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("key_bytes", "varbinary(3072)", false, 1),
                MysqlSchemaColumnSnapshot::new("value_bytes", "mediumblob", false, 0),
                MysqlSchemaColumnSnapshot::new("version_bytes", "binary(12)", false, 0),
            ],
            primary_key: vec!["key_bytes".to_owned()],
            secondary_indexes: Vec::new(),
        },
        MysqlSchemaTableSnapshot {
            name: "state_store_meta".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("meta_key", "varbinary(64)", false, 1),
                MysqlSchemaColumnSnapshot::new("meta_value", "varbinary(4096)", false, 0),
            ],
            primary_key: vec!["meta_key".to_owned()],
            secondary_indexes: Vec::new(),
        },
    ]
}

async fn assert_open_corruption(runtime: &StateStoreRuntime, database: &str, cluster_id: &str) {
    let error = match open_store(runtime, database, cluster_id, 4_000).await {
        Ok(_) => panic!("schema drift must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_bootstraps_exact_four_tables_and_meta() {
    let database = TestDatabase::provision(
        "mysql_schema_bootstraps_exact_four_tables_and_meta",
        "exact",
    );
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");

    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap MySQL state store");
    assert_eq!(store.provider_name(), "mysql");
    assert_eq!(store.limits().max_key_bytes, 3072);
    let identity = store.identity().await.expect("store identity");
    assert_eq!(identity.cluster_id, CLUSTER_ID);
    assert_eq!(identity.initial_incarnation, 1);
    assert_eq!(identity.store_id.get_version(), Some(Version::SortRand));

    let snapshot = schema_snapshot(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("schema snapshot");
    assert_eq!(snapshot.tables, expected_tables());
    assert!(snapshot.views.is_empty());
    assert!(snapshot.triggers.is_empty());
    assert_eq!(
        snapshot.meta_keys,
        vec![
            "change_retention_floor",
            "cluster_id",
            "current_revision",
            "initial_incarnation",
            "schema_digest",
            "schema_version",
            "store_id",
        ]
    );
    assert_eq!(snapshot.schema_version, 1);
    assert_eq!(snapshot.schema_digest, EXPECTED_SCHEMA_DIGEST);
    assert_eq!(snapshot.store_id, identity.store_id);
    assert_eq!(snapshot.cluster_id, CLUSTER_ID);
    assert_eq!(snapshot.initial_incarnation, 1);
    assert_eq!(snapshot.current_revision, 0);
    assert_eq!(snapshot.change_retention_floor, (0, u32::MAX));

    drop(store);
    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_schema_concurrent_open_converges_on_one_identity() {
    let database = TestDatabase::provision(
        "mysql_schema_concurrent_open_converges_on_one_identity",
        "concurrent",
    );
    let mut first =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct first runtime");
    let mut second =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct second runtime");

    let (first_store, second_store) = tokio::join!(
        open_store(&first, &database.name, CLUSTER_ID, 4_000),
        open_store(&second, &database.name, CLUSTER_ID, 4_000)
    );
    let first_store = first_store.expect("first concurrent open");
    let second_store = second_store.expect("second concurrent open");
    assert_eq!(
        first_store.identity().await.expect("first identity"),
        second_store.identity().await.expect("second identity")
    );

    drop(first_store);
    drop(second_store);
    first.shutdown().await.expect("shutdown first runtime");
    second.shutdown().await.expect("shutdown second runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_cluster_identity_mismatch() {
    let database =
        TestDatabase::provision("mysql_schema_rejects_cluster_identity_mismatch", "cluster");
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("initialize first cluster identity");
    drop(store);

    let error = match open_store(
        &runtime,
        &database.name,
        "different-sensitive-cluster",
        4_000,
    )
    .await
    {
        Ok(_) => panic!("cluster mismatch must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_partial_and_extra_objects() {
    let partial =
        TestDatabase::provision("mysql_schema_rejects_partial_and_extra_objects", "partial");
    let extra = TestDatabase::provision("mysql_schema_rejects_partial_and_extra_objects", "extra");
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");

    apply_schema_mutation(
        &runtime,
        &partial.name,
        MysqlSchemaMutation::CreatePartialMetaTable,
        Duration::from_secs(4),
    )
    .await
    .expect("create partial schema");
    assert_open_corruption(&runtime, &partial.name, CLUSTER_ID).await;

    let store = open_store(&runtime, &extra.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap extra-object database");
    drop(store);
    apply_schema_mutation(
        &runtime,
        &extra.name,
        MysqlSchemaMutation::CreateExtraTable,
        Duration::from_secs(4),
    )
    .await
    .expect("create extra object");
    assert_open_corruption(&runtime, &extra.name, CLUSTER_ID).await;

    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_engine_row_format_column_and_index_drift() {
    let cases = [
        ("engine", MysqlSchemaMutation::DriftEngine),
        ("row_format", MysqlSchemaMutation::DriftRowFormat),
        ("column", MysqlSchemaMutation::DriftColumn),
        ("index", MysqlSchemaMutation::DriftIndex),
    ];
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");
    let mut databases = Vec::new();

    for (suffix, mutation) in cases {
        let database = TestDatabase::provision(
            "mysql_schema_rejects_engine_row_format_column_and_index_drift",
            suffix,
        );
        let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
            .await
            .expect("bootstrap drift database");
        drop(store);
        apply_schema_mutation(&runtime, &database.name, mutation, Duration::from_secs(4))
            .await
            .expect("apply schema drift");
        assert_open_corruption(&runtime, &database.name, CLUSTER_ID).await;
        databases.push(database);
    }

    runtime.shutdown().await.expect("shutdown MySQL runtime");
    drop(databases);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_missing_malformed_older_and_newer_meta() {
    let cases = [
        ("missing", MysqlSchemaMutation::DeleteSchemaVersion),
        ("malformed", MysqlSchemaMutation::MalformedSchemaVersion),
        ("older", MysqlSchemaMutation::OlderSchemaVersion),
        ("newer", MysqlSchemaMutation::NewerSchemaVersion),
    ];
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");
    let mut databases = Vec::new();

    for (suffix, mutation) in cases {
        let database = TestDatabase::provision(
            "mysql_schema_rejects_missing_malformed_older_and_newer_meta",
            suffix,
        );
        let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
            .await
            .expect("bootstrap meta drift database");
        drop(store);
        apply_schema_mutation(&runtime, &database.name, mutation, Duration::from_secs(4))
            .await
            .expect("apply meta drift");
        assert_open_corruption(&runtime, &database.name, CLUSTER_ID).await;
        databases.push(database);
    }

    runtime.shutdown().await.expect("shutdown MySQL runtime");
    drop(databases);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_advisory_lock_name_is_hashed_and_at_most_64_bytes() {
    let database = TestDatabase::provision(
        "mysql_schema_advisory_lock_name_is_hashed_and_at_most_64_bytes",
        "lock_name",
    );
    let lock_name = advisory_lock_name(&database.name);
    let digest = Sha256::digest(database.name.as_bytes());

    assert_eq!(
        lock_name,
        format!("novarocks-ss3-{}", hex::encode(&digest[..24]))
    );
    assert_eq!(lock_name.len(), 62);
    assert!(lock_name.len() <= 64);
    assert!(!lock_name.contains(&database.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_schema_advisory_lock_timeout_and_release_are_deterministic() {
    let database = TestDatabase::provision(
        "mysql_schema_advisory_lock_timeout_and_release_are_deterministic",
        "lock",
    );
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");
    let held = acquire_schema_advisory_lock(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("hold schema advisory lock");

    let error = match open_store(&runtime, &database.name, CLUSTER_ID, 100).await {
        Ok(_) => panic!("held advisory lock must bound open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
    held.release(Duration::from_secs(4))
        .await
        .expect("release schema advisory lock");
    assert!(
        is_schema_advisory_lock_free(&runtime, &database.name, Duration::from_secs(4))
            .await
            .expect("lock free after explicit release")
    );

    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open after lock release");
    drop(store);
    let mismatch = match open_store(&runtime, &database.name, "different-cluster", 4_000).await {
        Ok(_) => panic!("identity validation failure must fail closed"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert!(
        is_schema_advisory_lock_free(&runtime, &database.name, Duration::from_secs(4))
            .await
            .expect("lock free after validation failure")
    );

    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_never_creates_database_alters_or_drops_objects() {
    let partial = TestDatabase::provision(
        "mysql_schema_never_creates_database_alters_or_drops_objects",
        "partial",
    );
    let drift = TestDatabase::provision(
        "mysql_schema_never_creates_database_alters_or_drops_objects",
        "drift",
    );
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");

    let missing_database = format!(
        "novarocks_ss3_missing_{}",
        hex::encode(&Sha256::digest(partial.name.as_bytes())[..8])
    );
    let missing = match open_store(&runtime, &missing_database, CLUSTER_ID, 1_000).await {
        Ok(_) => panic!("provider must not create a missing database"),
        Err(error) => error,
    };
    assert_eq!(missing.kind(), StateStoreErrorKind::InvalidConfiguration);

    apply_schema_mutation(
        &runtime,
        &partial.name,
        MysqlSchemaMutation::CreatePartialMetaTable,
        Duration::from_secs(4),
    )
    .await
    .expect("create partial schema");
    let partial_before = schema_snapshot(&runtime, &partial.name, Duration::from_secs(4))
        .await
        .expect("partial snapshot before open");
    assert_open_corruption(&runtime, &partial.name, CLUSTER_ID).await;
    let partial_after = schema_snapshot(&runtime, &partial.name, Duration::from_secs(4))
        .await
        .expect("partial snapshot after open");
    assert_eq!(partial_after, partial_before);

    let store = open_store(&runtime, &drift.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap drift database");
    drop(store);
    apply_schema_mutation(
        &runtime,
        &drift.name,
        MysqlSchemaMutation::DriftEngine,
        Duration::from_secs(4),
    )
    .await
    .expect("drift engine");
    let drift_before = schema_snapshot(&runtime, &drift.name, Duration::from_secs(4))
        .await
        .expect("drift snapshot before open");
    assert_open_corruption(&runtime, &drift.name, CLUSTER_ID).await;
    let drift_after = schema_snapshot(&runtime, &drift.name, Duration::from_secs(4))
        .await
        .expect("drift snapshot after open");
    assert_eq!(drift_after, drift_before);

    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_cluster_id_beyond_meta_limit_before_ddl() {
    let boundary = TestDatabase::provision(
        "mysql_schema_rejects_cluster_id_beyond_meta_limit_before_ddl",
        "boundary",
    );
    let oversized = TestDatabase::provision(
        "mysql_schema_rejects_cluster_id_beyond_meta_limit_before_ddl",
        "oversized",
    );
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");

    let boundary_cluster_id = "c".repeat(4096);
    let store = open_store(&runtime, &boundary.name, &boundary_cluster_id, 4_000)
        .await
        .expect("4096-byte cluster identity must fit meta value");
    drop(store);

    let oversized_cluster_id = "c".repeat(4097);
    let error = match open_store(&runtime, &oversized.name, &oversized_cluster_id, 4_000).await {
        Ok(_) => panic!("4097-byte cluster identity must fail before DDL"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert_eq!(
        error.to_string(),
        "InvalidConfiguration: MySQL state store configuration is invalid"
    );
    let snapshot = schema_snapshot(&runtime, &oversized.name, Duration::from_secs(4))
        .await
        .expect("oversized cluster inventory snapshot");
    assert!(snapshot.tables.is_empty());
    assert!(snapshot.views.is_empty());
    assert!(snapshot.triggers.is_empty());
    assert!(snapshot.meta_keys.is_empty());

    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_store_readiness_validates_inventory_identity_and_transactions() {
    let database = TestDatabase::provision(
        "mysql_store_readiness_validates_inventory_identity_and_transactions",
        "ready",
    );
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");

    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open ready store");
    assert_eq!(store.provider_name(), "mysql");
    assert_eq!(
        store.identity().await.expect("ready identity").cluster_id,
        CLUSTER_ID
    );
    let readiness =
        store_readiness_snapshot(&runtime, &database.name, CLUSTER_ID, Duration::from_secs(4))
            .await
            .expect("transaction readiness");
    assert!(readiness.read_only_started_and_rolled_back);
    assert!(readiness.write_started_and_rolled_back);
    assert!(
        schema_timeout_connection_is_destroyed(
            &runtime,
            &database.name,
            Duration::from_millis(100),
            Duration::from_secs(4),
        )
        .await
        .expect("schema timeout disposition")
    );

    drop(store);
    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_store_readiness_rejects_schema_or_identity_drift_after_pool_checkout() {
    let schema_database = TestDatabase::provision(
        "mysql_store_readiness_rejects_schema_or_identity_drift_after_pool_checkout",
        "schema",
    );
    let identity_database = TestDatabase::provision(
        "mysql_store_readiness_rejects_schema_or_identity_drift_after_pool_checkout",
        "identity",
    );
    let mut runtime =
        StateStoreRuntime::mysql(fixture_client_config()).expect("construct MySQL runtime");

    active_readiness(&runtime, &schema_database.name, Duration::from_secs(4))
        .await
        .expect("checkout schema pool before bootstrap");
    let schema_store = open_store(&runtime, &schema_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap schema database");
    drop(schema_store);
    apply_schema_mutation(
        &runtime,
        &schema_database.name,
        MysqlSchemaMutation::DriftIndex,
        Duration::from_secs(4),
    )
    .await
    .expect("drift schema after pool checkout");
    assert_open_corruption(&runtime, &schema_database.name, CLUSTER_ID).await;

    active_readiness(&runtime, &identity_database.name, Duration::from_secs(4))
        .await
        .expect("checkout identity pool before bootstrap");
    let identity_store = open_store(&runtime, &identity_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap identity database");
    drop(identity_store);
    let mismatch = match open_store(
        &runtime,
        &identity_database.name,
        "different-cluster",
        4_000,
    )
    .await
    {
        Ok(_) => panic!("identity drift after checkout must fail"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);

    runtime.shutdown().await.expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_cancellation_after_lock_is_safely_disposed() {
    let database = TestDatabase::provision(
        "mysql_schema_cancellation_after_lock_is_safely_disposed",
        "lock",
    );
    let mut client_config = fixture_client_config();
    client_config.pool_max = 1;
    let observer_client_config = client_config.clone();
    let runtime = std::sync::Arc::new(
        StateStoreRuntime::mysql(client_config).expect("construct MySQL runtime"),
    );
    let gate = arm_mysql_open_gate(&database.name, MysqlOpenGatePhase::AfterAdvisoryLock)
        .expect("arm advisory-lock cancellation gate");
    let waiter_runtime = std::sync::Arc::clone(&runtime);
    let waiter_database = database.name.clone();
    let waiter = tokio::spawn(async move {
        open_store(&waiter_runtime, &waiter_database, CLUSTER_ID, 4_000).await
    });

    tokio::time::timeout(Duration::from_secs(4), gate.wait_reached())
        .await
        .expect("advisory-lock gate reached");
    let original_connection_id = gate.connection_id();
    assert_ne!(original_connection_id, 0);
    waiter.abort();
    match waiter.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("open waiter must be cancelled"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), gate.wait_completed())
            .await
            .is_err(),
        "provider-owned open must remain alive after waiter cancellation"
    );
    let mut runtime = std::sync::Arc::try_unwrap(runtime).expect("sole runtime owner");
    let mut shutdown = tokio::spawn(async move { runtime.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for advisory-lock cancellation cleanup"
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(4), gate.wait_completed())
        .await
        .expect("advisory-lock disposition completed");
    shutdown
        .await
        .expect("join MySQL runtime shutdown")
        .expect("shutdown MySQL runtime");

    let mut observer =
        StateStoreRuntime::mysql(observer_client_config).expect("construct observer runtime");
    let replacement = active_readiness(&observer, &database.name, Duration::from_secs(4))
        .await
        .expect("readiness after cancelled advisory lock");
    assert_ne!(replacement.connection_id, original_connection_id);
    assert!(
        is_schema_advisory_lock_free(&observer, &database.name, Duration::from_secs(4))
            .await
            .expect("lock state after waiter cancellation")
    );
    observer
        .shutdown()
        .await
        .expect("shutdown observer runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_store_readiness_cancellation_after_start_is_safely_disposed() {
    let database = TestDatabase::provision(
        "mysql_store_readiness_cancellation_after_start_is_safely_disposed",
        "transaction",
    );
    let mut client_config = fixture_client_config();
    client_config.pool_max = 1;
    let observer_client_config = client_config.clone();
    let runtime = std::sync::Arc::new(
        StateStoreRuntime::mysql(client_config).expect("construct MySQL runtime"),
    );
    let gate = arm_mysql_open_gate(&database.name, MysqlOpenGatePhase::AfterReadOnlyStart)
        .expect("arm transaction cancellation gate");
    let waiter_runtime = std::sync::Arc::clone(&runtime);
    let waiter_database = database.name.clone();
    let waiter = tokio::spawn(async move {
        open_store(&waiter_runtime, &waiter_database, CLUSTER_ID, 4_000).await
    });

    tokio::time::timeout(Duration::from_secs(4), gate.wait_reached())
        .await
        .expect("transaction gate reached");
    let original_connection_id = gate.connection_id();
    assert_ne!(original_connection_id, 0);
    waiter.abort();
    match waiter.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("open waiter must be cancelled"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), gate.wait_completed())
            .await
            .is_err(),
        "provider-owned readiness must remain alive after waiter cancellation"
    );
    let mut runtime = std::sync::Arc::try_unwrap(runtime).expect("sole runtime owner");
    let mut shutdown = tokio::spawn(async move { runtime.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for transaction cancellation cleanup"
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(4), gate.wait_completed())
        .await
        .expect("transaction disposition completed");
    shutdown
        .await
        .expect("join MySQL runtime shutdown")
        .expect("shutdown MySQL runtime");

    let mut observer =
        StateStoreRuntime::mysql(observer_client_config).expect("construct observer runtime");
    let replacement = active_readiness(&observer, &database.name, Duration::from_secs(4))
        .await
        .expect("readiness after cancelled transaction");
    assert_ne!(replacement.connection_id, original_connection_id);
    observer
        .shutdown()
        .await
        .expect("shutdown observer runtime");
}
