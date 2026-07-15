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

mod common;

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use novarocks::state_store::{
    ChangeCursor, ChangePage, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution,
    ContinuationToken, Direction, FeDeploymentView, FoundationDbClientConfig, Key, KeyRange,
    OperationId, Precondition, RangePage, RangeRequest, ReadTransaction, RunFailure, StateStore,
    StateStoreConfig, StateStoreError, StateStoreErrorKind, StateStoreLimitOverrides,
    StateStoreLimits, StateStoreMetrics, StateStoreOperation, StateStoreOutcome,
    StateStoreProviderConfig, StoreIdentity, StoreRevision, TransactionId, Value, VersionToken,
    WriteTransaction, derive_transaction_id, open_state_store, run_side_effect_free,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use common::state_store_conformance::{FaultGate, FaultInjectingStateStore, ScriptedCommitResult};

#[test]
fn foundationdb_config_parses_exact_tagged_provider() -> anyhow::Result<()> {
    let config: StateStoreConfig = toml::from_str(
        r#"
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "/tmp/fdb.cluster"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
    )?;

    assert!(matches!(
        config.provider,
        StateStoreProviderConfig::Foundationdb { keyspace_id, .. }
            if keyspace_id == Uuid::parse_str("22db595e-3031-48eb-8212-f56d3626ee41")?
    ));
    Ok(())
}

#[test]
fn foundationdb_config_rejects_missing_provider_and_cross_provider_fields() {
    for input in [
        r#"
cluster_id = "cluster-a"
cluster_file = "/tmp/fdb.cluster"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
        r#"
provider = "sqlite"
cluster_id = "cluster-a"
path = "meta/state-store.sqlite"
deployment_owner = "fe-a"
cluster_file = "/tmp/fdb.cluster"
"#,
        r#"
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "/tmp/fdb.cluster"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
path = "meta/state-store.sqlite"
"#,
        r#"
provider = "sqlite"
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "/tmp/fdb.cluster"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
    ] {
        toml::from_str::<StateStoreConfig>(input)
            .expect_err("missing and cross-provider fields must fail closed");
    }
}

#[test]
fn foundationdb_config_rejects_empty_cluster_id_and_invalid_cluster_files() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let missing = temp.path().join("missing.cluster");
    let fixtures = [
        StateStoreConfig {
            cluster_id: " ".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: temp.path().join("fdb.cluster"),
                keyspace_id: Uuid::nil(),
            },
        },
        StateStoreConfig {
            cluster_id: "cluster-a".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: PathBuf::new(),
                keyspace_id: Uuid::nil(),
            },
        },
        StateStoreConfig {
            cluster_id: "cluster-a".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: missing,
                keyspace_id: Uuid::nil(),
            },
        },
        StateStoreConfig {
            cluster_id: "cluster-a".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: temp.path().to_path_buf(),
                keyspace_id: Uuid::nil(),
            },
        },
        StateStoreConfig {
            cluster_id: "cluster-a".to_owned(),
            limits: StateStoreLimitOverrides::default(),
            provider: StateStoreProviderConfig::Foundationdb {
                cluster_file: PathBuf::from("bad\0cluster-file"),
                keyspace_id: Uuid::nil(),
            },
        },
    ];

    for config in fixtures {
        config
            .validate()
            .expect_err("invalid FoundationDB configuration must fail closed");
    }
}

#[cfg(unix)]
#[test]
fn foundationdb_config_rejects_non_utf8_cluster_file_path() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let config = StateStoreConfig {
        cluster_id: "cluster-a".to_owned(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Foundationdb {
            cluster_file: PathBuf::from(OsString::from_vec(b"/tmp/fdb-\xff.cluster".to_vec())),
            keyspace_id: Uuid::nil(),
        },
    };

    let error = config
        .validate()
        .expect_err("non-UTF-8 FoundationDB cluster_file must fail closed");
    assert_eq!(
        error.to_string(),
        "InvalidStateStoreConfig: cluster_file must be valid UTF-8"
    );
}

#[test]
fn foundationdb_config_rejects_invalid_client_configuration() -> anyhow::Result<()> {
    let cluster_file = tempfile::NamedTempFile::new()?;
    let config_path = tempfile::NamedTempFile::new()?;
    let state_store = format!(
        r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
        cluster_file.path().display()
    );

    for client in [
        r#"
[foundationdb_client]
disable_multi_version_client = false
"#,
        r#"
[foundationdb_client]
"#,
        r#"
[foundationdb_client]
disable_multi_version_client = true
tls_cert_path = "/tmp/client.crt"
"#,
        r#"
[foundationdb_client]
disable_multi_version_client = true
tls_password = "plaintext-secret"
"#,
    ] {
        std::fs::write(config_path.path(), format!("{state_store}{client}"))?;
        if novarocks::common::app_config::NovaRocksConfig::load_from_file(config_path.path())
            .is_ok()
        {
            panic!("invalid FoundationDB client configuration must fail closed");
        }
    }
    Ok(())
}

#[test]
fn foundationdb_config_rejects_missing_or_orphaned_client_configuration() -> anyhow::Result<()> {
    let cluster_file = tempfile::NamedTempFile::new()?;
    let config_path = tempfile::NamedTempFile::new()?;
    let fixtures = [
        format!(
            r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"
"#,
            cluster_file.path().display()
        ),
        r#"
[state_store]
provider = "sqlite"
cluster_id = "cluster-a"
path = "meta/state-store.sqlite"
deployment_owner = "fe-a"

[foundationdb_client]
disable_multi_version_client = true
"#
        .to_owned(),
        r#"
[foundationdb_client]
disable_multi_version_client = true
"#
        .to_owned(),
    ];

    for fixture in fixtures {
        std::fs::write(config_path.path(), fixture)?;
        if novarocks::common::app_config::NovaRocksConfig::load_from_file(config_path.path())
            .is_ok()
        {
            panic!("missing or orphaned FoundationDB client config must fail closed");
        }
    }
    Ok(())
}

#[test]
fn foundationdb_config_client_debug_redacts_paths_and_password_environment_name() {
    let config = FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: Some(PathBuf::from("/secret/client.crt")),
        tls_key_path: Some(PathBuf::from("/secret/client.key")),
        tls_ca_path: Some(PathBuf::from("/secret/ca.crt")),
        tls_verify_peers: Some("Check.Valid=1".to_owned()),
        tls_password_env: Some("FDB_TLS_PASSWORD_SECRET".to_owned()),
    };

    let debug = format!("{config:?}");
    for secret in [
        "/secret/client.crt",
        "/secret/client.key",
        "/secret/ca.crt",
        "Check.Valid=1",
        "FDB_TLS_PASSWORD_SECRET",
    ] {
        assert!(!debug.contains(secret), "Debug leaked {secret}: {debug}");
    }
    assert!(debug.contains("tls_cert_path_configured: true"));
    assert!(debug.contains("tls_password_env_configured: true"));
}

#[test]
fn foundationdb_config_loads_complete_tls_client_configuration() -> anyhow::Result<()> {
    const FIXTURE_ENV: &str = "NOVAROCKS_TEST_FDB_TLS_CONFIG_FIXTURE";
    const PASSWORD_ENV: &str = "NOVAROCKS_TEST_FDB_TLS_PASSWORD";

    if let Some(config_path) = std::env::var_os(FIXTURE_ENV) {
        return assert_complete_tls_client_configuration(PathBuf::from(config_path), PASSWORD_ENV);
    }

    let fixture_dir = tempfile::tempdir()?;
    let cluster_file = fixture_dir.path().join("fdb.cluster");
    let tls_cert = fixture_dir.path().join("client.crt");
    let tls_key = fixture_dir.path().join("client.key");
    let tls_ca = fixture_dir.path().join("ca.crt");
    let config_path = fixture_dir.path().join("novarocks.toml");
    for path in [&cluster_file, &tls_cert, &tls_key, &tls_ca] {
        std::fs::write(path, b"")?;
    }
    let config_text = format!(
        r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"

[foundationdb_client]
disable_multi_version_client = true
tls_cert_path = "{}"
tls_key_path = "{}"
tls_ca_path = "{}"
tls_verify_peers = "Check.Valid=1"
tls_password_env = "{}"
"#,
        cluster_file.display(),
        tls_cert.display(),
        tls_key.display(),
        tls_ca.display(),
        PASSWORD_ENV,
    );
    std::fs::write(&config_path, config_text)?;

    let output = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("foundationdb_config_loads_complete_tls_client_configuration")
        .arg("--nocapture")
        .env(FIXTURE_ENV, &config_path)
        .env(PASSWORD_ENV, "test-password")
        .output()?;
    assert!(
        output.status.success(),
        "isolated TLS config test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn assert_complete_tls_client_configuration(
    config_path: PathBuf,
    password_env: &str,
) -> anyhow::Result<()> {
    let fixture_dir = config_path.parent().expect("fixture directory");
    let cluster_file = fixture_dir.join("fdb.cluster");
    let tls_cert = fixture_dir.join("client.crt");
    let tls_key = fixture_dir.join("client.key");
    let tls_ca = fixture_dir.join("ca.crt");
    let loaded = novarocks::common::app_config::NovaRocksConfig::load_from_file(&config_path)?;

    let state_store = loaded.state_store.expect("state store config");
    assert!(matches!(
        state_store.provider,
        StateStoreProviderConfig::Foundationdb { cluster_file: loaded, keyspace_id }
            if loaded == cluster_file
                && keyspace_id == Uuid::parse_str("22db595e-3031-48eb-8212-f56d3626ee41")?
    ));

    let client = loaded
        .foundationdb_client
        .expect("FoundationDB client config");
    assert_eq!(client.tls_cert_path.as_deref(), Some(tls_cert.as_path()));
    assert_eq!(client.tls_key_path.as_deref(), Some(tls_key.as_path()));
    assert_eq!(client.tls_ca_path.as_deref(), Some(tls_ca.as_path()));
    assert_eq!(client.tls_verify_peers.as_deref(), Some("Check.Valid=1"));
    assert_eq!(client.tls_password_env.as_deref(), Some(password_env));

    let debug = format!("{client:?}");
    for secret in [
        tls_cert.to_string_lossy().as_ref(),
        tls_key.to_string_lossy().as_ref(),
        tls_ca.to_string_lossy().as_ref(),
        "Check.Valid=1",
        password_env,
    ] {
        assert!(!debug.contains(secret), "Debug leaked {secret}: {debug}");
    }
    Ok(())
}

#[cfg(not(feature = "foundationdb-provider"))]
#[tokio::test]
async fn foundationdb_config_feature_off_open_fails_without_fallback() {
    let cluster_file = tempfile::NamedTempFile::new().expect("cluster file");
    let config_path = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config_path.path(),
        format!(
            r#"
[state_store]
provider = "foundationdb"
cluster_id = "cluster-a"
cluster_file = "{}"
keyspace_id = "22db595e-3031-48eb-8212-f56d3626ee41"

[foundationdb_client]
disable_multi_version_client = true
"#,
            cluster_file.path().display()
        ),
    )
    .expect("write config");
    let loaded = novarocks::common::app_config::NovaRocksConfig::load_from_file(config_path.path())
        .expect("load FoundationDB config from TOML");
    let error = match open_state_store(
        loaded.state_store.expect("state store config"),
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(3).expect("three FEs"),
            topology_revision: Bytes::from_static(b"topology-r1"),
        },
    )
    .await
    {
        Ok(_) => panic!("feature-off FoundationDB open must fail without SQLite fallback"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert_eq!(
        error.to_string(),
        "InvalidConfiguration: FoundationDB provider is not compiled in"
    );
}

fn key(bytes: &'static [u8]) -> Key {
    Key::try_from(Bytes::from_static(bytes)).expect("valid key")
}

fn assert_object_safe(_: Arc<dyn StateStore>) {}
fn assert_read_object_safe(_: Box<dyn ReadTransaction>) {}
fn assert_write_object_safe(_: Box<dyn WriteTransaction>) {}

#[derive(Clone, Copy)]
enum ScriptedCommit {
    Committed,
    Conflict,
    TransientBeforeCommit,
    DefiniteFailure,
    CommitUnknown,
}

struct ScriptedStore {
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    commits: Mutex<VecDeque<ScriptedCommit>>,
    transaction_ids: Mutex<Vec<TransactionId>>,
}

impl ScriptedStore {
    fn new(commits: impl IntoIterator<Item = ScriptedCommit>) -> Self {
        Self {
            limits: StateStoreLimits::default(),
            metrics: Arc::new(StateStoreMetrics::new("scripted")),
            commits: Mutex::new(commits.into_iter().collect()),
            transaction_ids: Mutex::new(Vec::new()),
        }
    }

    fn with_limits(
        limits: StateStoreLimits,
        commits: impl IntoIterator<Item = ScriptedCommit>,
    ) -> Self {
        Self {
            limits,
            metrics: Arc::new(StateStoreMetrics::new("scripted")),
            commits: Mutex::new(commits.into_iter().collect()),
            transaction_ids: Mutex::new(Vec::new()),
        }
    }

    fn transaction_ids(&self) -> Vec<TransactionId> {
        self.transaction_ids
            .lock()
            .expect("transaction ids")
            .clone()
    }
}

struct ScriptedWriteTransaction {
    transaction_id: TransactionId,
    commit: ScriptedCommit,
    metrics: Arc<StateStoreMetrics>,
}

#[async_trait]
impl ReadTransaction for ScriptedWriteTransaction {
    async fn get(
        &mut self,
        _key: &Key,
    ) -> Result<Option<novarocks::state_store::StateRecord>, StateStoreError> {
        unreachable!("runner tests do not read")
    }

    async fn range(&mut self, _request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        unreachable!("runner tests do not scan")
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        Ok(())
    }
}

#[async_trait]
impl WriteTransaction for ScriptedWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    async fn put(
        &mut self,
        _key: Key,
        _value: Value,
        _precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        unreachable!("runner tests do not write")
    }

    async fn delete(
        &mut self,
        _key: Key,
        _precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        unreachable!("runner tests do not delete")
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        let started = std::time::Instant::now();
        let error = || {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "scripted state store outcome",
            )
        };
        let outcome = match self.commit {
            ScriptedCommit::Committed => CommitOutcome::Committed(CommitReceipt {
                transaction_id: self.transaction_id,
                revision: StoreRevision::try_from(Bytes::from_static(b"revision"))
                    .expect("revision"),
            }),
            ScriptedCommit::Conflict => CommitOutcome::Conflict(error()),
            ScriptedCommit::TransientBeforeCommit => CommitOutcome::TransientBeforeCommit(error()),
            ScriptedCommit::DefiniteFailure => CommitOutcome::DefiniteFailure(error()),
            ScriptedCommit::CommitUnknown => CommitOutcome::CommitUnknown(error()),
        };
        let metric_outcome = match &outcome {
            CommitOutcome::Committed(_) => StateStoreOutcome::Success,
            CommitOutcome::Conflict(_) => StateStoreOutcome::Conflict,
            CommitOutcome::TransientBeforeCommit(_) => StateStoreOutcome::TransientBeforeCommit,
            CommitOutcome::DefiniteFailure(_) => StateStoreOutcome::DefiniteFailure,
            CommitOutcome::CommitUnknown(_) => StateStoreOutcome::CommitUnknown,
        };
        self.metrics.record_operation(
            StateStoreOperation::Commit,
            metric_outcome,
            started.elapsed(),
        );
        outcome
    }
}

#[async_trait]
impl StateStore for ScriptedStore {
    fn provider_name(&self) -> &'static str {
        "scripted"
    }

    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> novarocks::state_store::StateStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        unreachable!("runner tests do not begin reads")
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let started = std::time::Instant::now();
        self.transaction_ids
            .lock()
            .expect("transaction ids")
            .push(transaction_id);
        let commit = self
            .commits
            .lock()
            .expect("commit script")
            .pop_front()
            .expect("scripted commit outcome");
        let transaction = Box::new(ScriptedWriteTransaction {
            transaction_id,
            commit,
            metrics: Arc::clone(&self.metrics),
        });
        self.metrics.record_operation(
            StateStoreOperation::Begin,
            StateStoreOutcome::Success,
            started.elapsed(),
        );
        Ok(transaction)
    }

    async fn poll_changes(
        &self,
        _request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        unreachable!("runner tests do not poll changes")
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        unreachable!("runner tests do not load identity")
    }

    async fn resolve_commit(
        &self,
        _transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        unreachable!("runner tests do not resolve commits")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fault_injecting_state_store_scripts_all_contract_boundaries() {
    let scripted = Arc::new(ScriptedStore::new(std::iter::repeat_n(
        ScriptedCommit::Committed,
        8,
    )));
    let fault = FaultInjectingStateStore::new(scripted as Arc<dyn StateStore>);
    let injected_error = || {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected contract fault",
        )
    };

    fault.fail_next_begin(injected_error());
    let begin_error = match fault
        .begin_write(Uuid::now_v7().into(), "injected begin")
        .await
    {
        Ok(_) => panic!("begin fault must fail"),
        Err(error) => error,
    };
    assert_eq!(begin_error.kind(), StateStoreErrorKind::ProviderUnavailable);

    let mut operation = fault
        .begin_write(Uuid::now_v7().into(), "injected operation")
        .await
        .expect("begin operation fault transaction");
    fault.fail_next_operation(injected_error());
    assert_eq!(
        operation
            .put(
                key(b"fault"),
                Value::try_from(Bytes::from_static(b"value")).expect("value"),
                Precondition::Any
            )
            .await
            .expect_err("operation fault must fail")
            .kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
    operation
        .abort()
        .await
        .expect("abort operation fault transaction");

    for result in [
        ScriptedCommitResult::Committed,
        ScriptedCommitResult::Conflict,
        ScriptedCommitResult::TransientBeforeCommit,
        ScriptedCommitResult::DefiniteFailure,
    ] {
        let transaction_id = Uuid::now_v7().into();
        let transaction = fault
            .begin_write(transaction_id, "injected pre-commit")
            .await
            .expect("begin pre-commit fault transaction");
        fault.script_next_pre_commit(result);
        let outcome = transaction.commit().await;
        assert!(
            matches!(
                (result, outcome),
                (ScriptedCommitResult::Committed, CommitOutcome::Committed(_))
                    | (ScriptedCommitResult::Conflict, CommitOutcome::Conflict(_))
                    | (
                        ScriptedCommitResult::TransientBeforeCommit,
                        CommitOutcome::TransientBeforeCommit(_)
                    )
                    | (
                        ScriptedCommitResult::DefiniteFailure,
                        CommitOutcome::DefiniteFailure(_)
                    )
            ),
            "unexpected injected outcome"
        );
    }

    fault.fail_next_change_poll(injected_error());
    assert_eq!(
        fault
            .poll_changes(&ChangePollRequest {
                after: None,
                page_size: 1,
            })
            .await
            .expect_err("change poll fault must fail")
            .kind(),
        StateStoreErrorKind::ProviderUnavailable
    );

    let post_dispatch_id = Uuid::now_v7().into();
    let transaction = fault
        .begin_write(post_dispatch_id, "injected post-dispatch")
        .await
        .expect("begin post-dispatch fault transaction");
    let gate = FaultGate::new();
    fault.pause_next_post_dispatch(gate.clone());
    let commit = tokio::spawn(async move { transaction.commit().await });
    gate.wait_reached().await;
    gate.wait_armed().await;
    assert!(
        !commit.is_finished(),
        "post-dispatch reply must remain gated"
    );
    gate.release().await;
    assert!(matches!(
        commit.await.expect("post-dispatch commit task"),
        CommitOutcome::Committed(_)
    ));
}

#[test]
fn contract_accepts_binary_payloads_and_rejects_invalid_ranges() {
    let binary = key(&[0, 255]);
    assert_eq!(binary.as_bytes(), &[0, 255]);
    assert_eq!(
        Value::try_from(Bytes::from_static(&[255, 0]))
            .expect("binary value")
            .as_bytes(),
        &[255, 0]
    );
    assert_eq!(
        VersionToken::try_from(Bytes::from_static(&[0, 255]))
            .expect("binary version")
            .as_bytes(),
        &[0, 255]
    );

    for (start, end) in [(key(&[1]), key(&[1])), (key(&[2]), key(&[1]))] {
        let error = KeyRange::new(start, end).expect_err("range must be increasing");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest);
    }
}

#[test]
fn contract_enforces_common_binary_and_page_bounds() {
    let limits = StateStoreLimits::default();
    assert_eq!(
        Key::try_from(Bytes::from(vec![0; limits.max_key_bytes + 1]))
            .expect_err("oversized key")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_eq!(
        Value::try_from(Bytes::from(vec![0; limits.max_value_bytes + 1]))
            .expect_err("oversized value")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_eq!(
        StoreRevision::try_from(Bytes::new())
            .expect_err("empty revision")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    for page_size in [0, limits.max_page_size + 1] {
        let request = RangeRequest {
            range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
            direction: Direction::Forward,
            page_size,
            continuation: None,
        };
        assert_eq!(
            request
                .validate(&limits)
                .expect_err("invalid page size")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }
}

#[test]
fn contract_prefix_range_requires_a_finite_successor() {
    let range = KeyRange::for_prefix(key(&[0, 255])).expect("finite prefix successor");
    assert_eq!(range.start.as_bytes(), &[0, 255]);
    assert_eq!(range.end.as_bytes(), &[1]);

    let error = KeyRange::for_prefix(key(&[255, 255])).expect_err("all-ff has no successor");
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest);
}

#[test]
fn contract_continuation_binds_range_and_direction() {
    let forward = RangeRequest {
        range: KeyRange::new(key(&[0]), key(&[2])).expect("range"),
        direction: Direction::Forward,
        page_size: 10,
        continuation: None,
    };
    let reverse = RangeRequest {
        direction: Direction::Reverse,
        ..forward.clone()
    };
    let other_range = RangeRequest {
        range: KeyRange::new(key(&[0]), key(&[3])).expect("range"),
        ..forward.clone()
    };

    let token = forward.continuation_after(&key(&[1])).expect("token");
    assert_eq!(
        token.resume_after(&forward).expect("matching request"),
        key(&[1])
    );
    assert_eq!(
        token
            .resume_after(&reverse)
            .expect_err("direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(
        token
            .resume_after(&other_range)
            .expect_err("range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn contract_codecs_reject_malformed_and_mismatched_tokens() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[0, 255])).expect("token");
    let mut trailing = token.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        novarocks::state_store::ContinuationToken::try_from(Bytes::from(trailing))
            .expect("opaque token")
            .resume_after(&request)
            .expect_err("trailing bytes")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let store_id = Uuid::now_v7();
    let revision = StoreRevision::try_from(Bytes::from_static(&[255, 255])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision.clone(), 42).expect("cursor");
    let (decoded_revision, sequence) = cursor.decode(store_id).expect("matching store");
    assert_eq!(decoded_revision, revision);
    assert_eq!(sequence, 42);
    assert_eq!(
        cursor
            .decode(Uuid::now_v7())
            .expect_err("store mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let mut trailing = cursor.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        ChangeCursor::try_from(Bytes::from(trailing))
            .expect("opaque cursor")
            .decode(store_id)
            .expect_err("trailing bytes")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn contract_codecs_preserve_their_error_context() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[1])).expect("token");
    let mut trailing_token = token.as_bytes().to_vec();
    trailing_token.push(0);
    for malformed in [
        Bytes::copy_from_slice(&token.as_bytes()[..token.as_bytes().len() - 1]),
        Bytes::from(trailing_token),
    ] {
        let error = ContinuationToken::try_from(malformed)
            .expect("opaque token")
            .resume_after(&request)
            .expect_err("malformed token");
        assert_eq!(
            error.to_string(),
            "InvalidRequest: invalid continuation token"
        );
    }

    let store_id = Uuid::now_v7();
    let revision = StoreRevision::try_from(Bytes::from_static(&[1])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision, 1).expect("cursor");
    let mut trailing_cursor = cursor.as_bytes().to_vec();
    trailing_cursor.push(0);
    for malformed in [
        Bytes::copy_from_slice(&cursor.as_bytes()[..cursor.as_bytes().len() - 1]),
        Bytes::from(trailing_cursor),
    ] {
        let error = ChangeCursor::try_from(malformed)
            .expect("opaque cursor")
            .decode(store_id)
            .expect_err("malformed cursor");
        assert_eq!(error.to_string(), "InvalidRequest: invalid change cursor");
    }
}

#[test]
fn contract_continuation_codec_has_the_stable_v1_layout() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[0, 255]), key(&[2])).expect("range"),
        direction: Direction::Reverse,
        page_size: 7,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[1, 0])).expect("token");
    let encoded = token.as_bytes();

    let expected_fingerprint = Sha256::digest(
        [
            &[1, 1][..],
            &2_u32.to_be_bytes(),
            &[0, 255],
            &1_u32.to_be_bytes(),
            &[2],
        ]
        .concat(),
    );
    assert_eq!(&encoded[..2], &[1, 1]);
    assert_eq!(&encoded[2..34], expected_fingerprint.as_slice());
    assert_eq!(&encoded[34..38], &2_u32.to_be_bytes());
    assert_eq!(&encoded[38..], &[1, 0]);

    for malformed in [
        Bytes::new(),
        Bytes::from_static(&[2, 1]),
        Bytes::copy_from_slice(&encoded[..encoded.len() - 1]),
    ] {
        assert_eq!(
            ContinuationToken::try_from(malformed)
                .expect("opaque token")
                .resume_after(&request)
                .expect_err("malformed token")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }
}

#[test]
fn contract_change_cursor_has_the_stable_v1_layout() {
    let store_id = Uuid::from_bytes([7; 16]);
    let revision = StoreRevision::try_from(Bytes::from_static(&[0, 255])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision, 0x01020304).expect("cursor");
    let encoded = cursor.as_bytes();

    assert_eq!(encoded[0], 1);
    assert_eq!(&encoded[1..17], store_id.as_bytes());
    assert_eq!(&encoded[17..21], &2_u32.to_be_bytes());
    assert_eq!(&encoded[21..23], &[0, 255]);
    assert_eq!(&encoded[23..27], &0x01020304_u32.to_be_bytes());

    for malformed in [
        Bytes::new(),
        Bytes::from_static(&[2]),
        Bytes::copy_from_slice(&encoded[..encoded.len() - 1]),
    ] {
        assert_eq!(
            ChangeCursor::try_from(malformed)
                .expect("opaque cursor")
                .decode(store_id)
                .expect_err("malformed cursor")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }
}

#[test]
fn contract_error_surface_is_typed_and_provider_neutral() {
    let kinds = [
        StateStoreErrorKind::InvalidRequest,
        StateStoreErrorKind::InvalidConfiguration,
        StateStoreErrorKind::UnsupportedDeployment,
        StateStoreErrorKind::LimitExceeded,
        StateStoreErrorKind::DeadlineExceeded,
        StateStoreErrorKind::PreconditionFailed,
        StateStoreErrorKind::Conflict,
        StateStoreErrorKind::Transient,
        StateStoreErrorKind::Corruption,
        StateStoreErrorKind::ProviderUnavailable,
        StateStoreErrorKind::Cancelled,
        StateStoreErrorKind::Internal,
    ];
    for kind in kinds {
        let error = StateStoreError::new(kind, "state store operation failed");
        assert_eq!(error.kind(), kind);
        assert!(!error.to_string().contains("SELECT"));
        assert!(!error.to_string().contains("password"));
    }
}

#[test]
fn contract_traits_are_object_safe() {
    let _ = assert_object_safe as fn(Arc<dyn StateStore>);
    let _ = assert_read_object_safe as fn(Box<dyn ReadTransaction>);
    let _ = assert_write_object_safe as fn(Box<dyn WriteTransaction>);
}

#[tokio::test]
async fn runner_replays_the_whole_operation_for_retryable_commit_outcomes() {
    let store = ScriptedStore::new([
        ScriptedCommit::Conflict,
        ScriptedCommit::TransientBeforeCommit,
        ScriptedCommit::Committed,
    ]);
    let operation_id = OperationId::new_v7();
    let operation_runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let success = run_side_effect_free(
        &store,
        store.metrics.as_ref(),
        operation_id,
        "runner retry test",
        {
            let operation_runs = Arc::clone(&operation_runs);
            move |_transaction| -> BoxFuture<'_, Result<usize, StateStoreError>> {
                let run = operation_runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                Box::pin(async move { Ok(run) })
            }
        },
    )
    .await
    .expect("third attempt commits");

    assert_eq!(success.value, 3);
    assert_eq!(operation_runs.load(std::sync::atomic::Ordering::SeqCst), 3);
    let expected_ids = (1..=3)
        .map(|attempt| derive_transaction_id(operation_id, attempt))
        .collect::<Vec<_>>();
    assert_eq!(store.transaction_ids(), expected_ids);
    assert_eq!(success.receipt.transaction_id, expected_ids[2]);

    let snapshot = store.metrics_snapshot();
    assert_eq!(snapshot.begin_count, 3);
    assert_eq!(snapshot.commit_count, 3);
    assert_eq!(snapshot.retry_count, 2);
    assert_eq!(
        snapshot.operation_outcome_count(StateStoreOperation::Commit, StateStoreOutcome::Conflict),
        1
    );
    assert_eq!(
        snapshot.operation_outcome_count(
            StateStoreOperation::Commit,
            StateStoreOutcome::TransientBeforeCommit
        ),
        1
    );
}

#[tokio::test]
async fn runner_does_not_retry_operation_definite_or_unknown_failures() {
    let operation_error_store = ScriptedStore::new([ScriptedCommit::Committed]);
    let operation_error_runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failure = run_side_effect_free(
        &operation_error_store,
        operation_error_store.metrics.as_ref(),
        OperationId::new_v7(),
        "operation error test",
        {
            let operation_error_runs = Arc::clone(&operation_error_runs);
            move |_transaction| -> BoxFuture<'_, Result<(), StateStoreError>> {
                operation_error_runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async {
                    Err(StateStoreError::new(
                        StateStoreErrorKind::InvalidRequest,
                        "scripted operation failure",
                    ))
                })
            }
        },
    )
    .await
    .expect_err("operation failure is terminal");
    assert!(matches!(failure, RunFailure::Operation(_)));
    assert_eq!(
        operation_error_runs.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(operation_error_store.transaction_ids().len(), 1);

    for (outcome, expect_unknown) in [
        (ScriptedCommit::DefiniteFailure, false),
        (ScriptedCommit::CommitUnknown, true),
    ] {
        let store = ScriptedStore::new([outcome]);
        let operation_id = OperationId::new_v7();
        let failure = run_side_effect_free(
            &store,
            store.metrics.as_ref(),
            operation_id,
            "terminal commit outcome test",
            |_transaction| Box::pin(async { Ok(()) }),
        )
        .await
        .expect_err("commit outcome is terminal");
        if expect_unknown {
            match failure {
                RunFailure::CommitUnknown { transaction_id, .. } => {
                    assert_eq!(transaction_id, derive_transaction_id(operation_id, 1));
                }
                other => panic!("expected CommitUnknown, got {other:?}"),
            }
        } else {
            assert!(matches!(failure, RunFailure::DefiniteFailure(_)));
        }
        assert_eq!(store.transaction_ids().len(), 1);
    }
}

#[test]
fn runner_derives_stable_distinct_uuid_v7_attempt_ids() {
    let operation_id = OperationId::new_v7();
    let first = derive_transaction_id(operation_id, 1);
    let first_again = derive_transaction_id(operation_id, 1);
    let second = derive_transaction_id(operation_id, 2);

    assert_eq!(first, first_again);
    assert_ne!(first, second);
    assert_eq!(
        &first.as_uuid().as_bytes()[..6],
        &operation_id.as_uuid().as_bytes()[..6]
    );
    assert_eq!(first.as_uuid().get_version(), Some(uuid::Version::SortRand));
    assert_eq!(first.as_uuid().get_variant(), uuid::Variant::RFC4122);

    let digest = Sha256::digest(
        [
            operation_id.as_uuid().as_bytes().as_slice(),
            &1_u32.to_be_bytes(),
        ]
        .concat(),
    );
    let mut expected = [0_u8; 16];
    expected[..6].copy_from_slice(&operation_id.as_uuid().as_bytes()[..6]);
    expected[6..].copy_from_slice(&digest[..10]);
    expected[6] = (expected[6] & 0x0f) | 0x70;
    expected[8] = (expected[8] & 0x3f) | 0x80;
    assert_eq!(first.as_uuid().as_bytes(), &expected);

    assert!(std::panic::catch_unwind(|| derive_transaction_id(operation_id, 0)).is_err());
    assert!(std::panic::catch_unwind(|| derive_transaction_id(operation_id, 6)).is_err());
}

#[tokio::test]
async fn runner_stops_at_the_attempt_budget_without_a_sixth_id() {
    let store = ScriptedStore::new([ScriptedCommit::Conflict; 5]);
    let operation_id = OperationId::new_v7();
    let failure = run_side_effect_free(
        &store,
        store.metrics.as_ref(),
        operation_id,
        "retry budget test",
        |_transaction| Box::pin(async { Ok(()) }),
    )
    .await
    .expect_err("five conflicts exhaust retry budget");

    assert!(matches!(failure, RunFailure::RetryExhausted(_)));
    assert_eq!(store.transaction_ids().len(), 5);
    assert_eq!(
        store.transaction_ids().last().copied(),
        Some(derive_transaction_id(operation_id, 5))
    );
}

#[tokio::test]
async fn runner_enforces_one_total_deadline_without_retrying_a_slow_operation() {
    let limits = StateStoreLimits {
        transaction_deadline: Duration::from_millis(20),
        ..StateStoreLimits::default()
    };
    let store = ScriptedStore::with_limits(limits, [ScriptedCommit::Committed]);
    let failure = run_side_effect_free(
        &store,
        store.metrics.as_ref(),
        OperationId::new_v7(),
        "deadline test",
        |_transaction| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(())
            })
        },
    )
    .await
    .expect_err("operation exceeds total deadline");

    assert!(matches!(failure, RunFailure::DeadlineExceeded));
    assert_eq!(store.transaction_ids().len(), 1);
    assert_eq!(store.metrics_snapshot().deadline_count, 1);
}
