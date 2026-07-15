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

mod schema;

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};

use crate::state_store::{
    FeDeploymentView, StateStoreConfig, StateStoreError, StateStoreErrorKind, StateStoreLimits,
    StoreIdentity,
};

#[allow(dead_code)]
pub(super) struct SqliteStateStore {
    pub(super) path: PathBuf,
    pub(super) limits: StateStoreLimits,
    identity: StoreIdentity,
    _owner_lock: File,
}

#[allow(dead_code)]
impl SqliteStateStore {
    pub(super) async fn open(
        config: StateStoreConfig,
        deployment: FeDeploymentView,
    ) -> Result<Self, StateStoreError> {
        if deployment.active_fe_count.get() != 1 {
            return Err(StateStoreError::new(
                StateStoreErrorKind::UnsupportedDeployment,
                "sqlite state store requires exactly one active FE",
            ));
        }

        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store configuration is invalid",
            )
        })?;
        if is_memory_path(&config.path) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store requires a persistent file path",
            ));
        }
        let limits = StateStoreLimits::from_overrides(&config.limits).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store limits are invalid",
            )
        })?;

        tokio::task::spawn_blocking(move || open_blocking(config, limits))
            .await
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "SQLite state store open worker failed",
                )
            })?
    }

    pub(super) fn identity_snapshot(&self) -> &StoreIdentity {
        &self.identity
    }
}

fn open_blocking(
    config: StateStoreConfig,
    limits: StateStoreLimits,
) -> Result<SqliteStateStore, StateStoreError> {
    let path = canonicalize_database_path(&config.path)?;
    let owner_lock = acquire_owner_lock(&path)?;
    let mut connection = open_connection(&path)?;
    let identity = schema::initialize(
        &mut connection,
        config.cluster_id.as_bytes(),
        config.deployment_owner.as_bytes(),
    )?;

    Ok(SqliteStateStore {
        path,
        limits,
        identity,
        _owner_lock: owner_lock,
    })
}

pub(super) fn open_connection(path: &Path) -> Result<Connection, StateStoreError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let connection = Connection::open_with_flags(path, flags).map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "failed to open SQLite state store database",
        )
    })?;
    let journal_mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        .map_err(|_| connection_configuration_error())?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(connection_configuration_error());
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|_| connection_configuration_error())?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|_| connection_configuration_error())?;

    let synchronous = connection
        .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
        .map_err(|_| connection_configuration_error())?;
    let foreign_keys = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
        .map_err(|_| connection_configuration_error())?;
    if synchronous != 2 || foreign_keys != 1 {
        return Err(connection_configuration_error());
    }
    Ok(connection)
}

fn canonicalize_database_path(path: &Path) -> Result<PathBuf, StateStoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|_| path_error("failed to resolve SQLite state store working directory"))?
            .join(path)
    };
    let file_name = absolute.file_name().ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store path must name a database file",
        )
    })?;
    let parent = absolute.parent().ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store path must have a parent directory",
        )
    })?;
    fs::create_dir_all(parent)
        .map_err(|_| path_error("failed to create SQLite state store directory"))?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| path_error("failed to canonicalize SQLite state store directory"))?;
    let candidate = canonical_parent.join(file_name);

    match fs::canonicalize(&candidate) {
        Ok(canonical) => Ok(canonical),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            match fs::symlink_metadata(&candidate) {
                Err(metadata_error) if metadata_error.kind() == ErrorKind::NotFound => {
                    Ok(candidate)
                }
                _ => Err(path_error(
                    "SQLite state store path exists but cannot be canonicalized",
                )),
            }
        }
        Err(_) => Err(path_error(
            "failed to canonicalize SQLite state store database",
        )),
    }
}

fn acquire_owner_lock(path: &Path) -> Result<File, StateStoreError> {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".owner.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock_path))
        .map_err(|_| path_error("failed to open SQLite state store owner lock"))?;
    lock.try_lock_exclusive().map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "SQLite state store path is already owned by another provider",
        )
    })?;
    Ok(lock)
}

fn is_memory_path(path: &Path) -> bool {
    let Some(path) = path.to_str() else {
        return false;
    };
    let path = path.to_ascii_lowercase();
    path == ":memory:"
        || (path.starts_with("file:")
            && (path.contains(":memory:")
                || path
                    .split_once('?')
                    .is_some_and(|(_, query)| query.split('&').any(|part| part == "mode=memory"))))
}

const fn connection_configuration_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "failed to configure SQLite state store connection",
    )
}

const fn path_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::ProviderUnavailable, message)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};

    use bytes::Bytes;
    use rusqlite::{Connection, OptionalExtension, params};
    use tempfile::TempDir;
    use tokio::runtime::{Builder, Runtime};
    use uuid::Version;

    use super::*;
    use crate::state_store::{
        FeDeploymentView, StateStoreConfig, StateStoreErrorKind, StateStoreLimitOverrides,
        StateStoreProviderConfig,
    };

    fn runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime")
    }

    fn config(
        path: impl Into<PathBuf>,
        cluster_id: &str,
        deployment_owner: &str,
    ) -> StateStoreConfig {
        StateStoreConfig {
            provider: StateStoreProviderConfig::Sqlite,
            path: path.into(),
            cluster_id: cluster_id.to_owned(),
            deployment_owner: deployment_owner.to_owned(),
            limits: StateStoreLimitOverrides::default(),
        }
    }

    fn deployment(active_fe_count: usize) -> FeDeploymentView {
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(active_fe_count).expect("non-zero FE count"),
            topology_revision: Bytes::from_static(b"topology-r1"),
        }
    }

    fn lock_path(path: &Path) -> PathBuf {
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".owner.lock");
        PathBuf::from(lock_path)
    }

    fn open_error(
        runtime: &Runtime,
        config: StateStoreConfig,
        deployment: FeDeploymentView,
    ) -> crate::state_store::StateStoreError {
        runtime.block_on(async {
            match SqliteStateStore::open(config, deployment).await {
                Ok(_) => panic!("SQLite open unexpectedly succeeded"),
                Err(error) => error,
            }
        })
    }

    #[test]
    fn sqlite_open_rejects_multi_fe_before_path_io() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("missing-parent/state-store.sqlite");

        let error = open_error(
            &runtime(),
            config(&path, "cluster-a", "fe-a"),
            deployment(2),
        );

        assert_eq!(error.kind(), StateStoreErrorKind::UnsupportedDeployment);
        assert!(!path.exists(), "topology rejection must not create the DB");
        assert!(
            !lock_path(&path).exists(),
            "topology rejection must not create the owner lock"
        );
        assert!(
            !path.parent().expect("DB parent").exists(),
            "topology rejection must not create parent directories"
        );
    }

    #[test]
    fn sqlite_open_creates_isolated_schema_and_uuid_v7_identity() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let legacy = Connection::open(&path).expect("seed legacy table");
        legacy
            .execute_batch(
                "CREATE TABLE meta_records(marker TEXT NOT NULL);\n\
                 INSERT INTO meta_records(marker) VALUES ('legacy-sentinel');",
            )
            .expect("seed legacy row");
        drop(legacy);

        let store = runtime()
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("first SQLite open");
        let identity = store.identity_snapshot();

        assert_eq!(identity.store_id.get_version(), Some(Version::SortRand));
        assert_eq!(identity.cluster_id, "cluster-a");
        assert_eq!(identity.initial_incarnation, 1);

        let connection = open_connection(&path).expect("configured SQLite connection");
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .expect("journal mode"),
            "wal"
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                .expect("synchronous mode"),
            2
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
                .expect("foreign keys"),
            1
        );

        let mut statement = connection
            .prepare(
                "SELECT name FROM sqlite_schema \
                 WHERE type = 'table' AND name LIKE 'state_store_%' ORDER BY name",
            )
            .expect("prepare schema query");
        let tables = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query schema")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect schema");
        assert_eq!(
            tables,
            [
                "state_store_changes",
                "state_store_commits",
                "state_store_kv",
                "state_store_meta",
            ]
        );
        let legacy_marker: String = connection
            .query_row("SELECT marker FROM meta_records", [], |row| row.get(0))
            .expect("legacy row must remain untouched");
        assert_eq!(legacy_marker, "legacy-sentinel");
        let cluster: Vec<u8> = connection
            .query_row(
                "SELECT value FROM state_store_meta WHERE key = ?1",
                params![b"cluster_id".as_slice()],
                |row| row.get(0),
            )
            .expect("cluster identity row");
        let owner: Vec<u8> = connection
            .query_row(
                "SELECT value FROM state_store_meta WHERE key = ?1",
                params![b"deployment_owner".as_slice()],
                |row| row.get(0),
            )
            .expect("owner identity row");
        assert_eq!(cluster, b"cluster-a");
        assert_eq!(owner, b"fe-a");
    }

    #[test]
    fn sqlite_open_rejects_second_handle_for_same_path() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("first handle");

        let error = open_error(&runtime, config(&path, "cluster-a", "fe-a"), deployment(1));

        assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);
        drop(first);
    }

    #[test]
    fn sqlite_open_restart_preserves_identity_after_lock_release() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("first handle");
        let first_identity = first.identity_snapshot().clone();
        drop(first);

        let restarted = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("restart after lock release");

        assert_eq!(restarted.identity_snapshot(), &first_identity);
    }

    #[test]
    fn sqlite_open_rejects_owner_and_cluster_mismatch() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("first handle");
        drop(first);

        let owner_error = open_error(&runtime, config(&path, "cluster-a", "fe-b"), deployment(1));
        assert_eq!(
            owner_error.kind(),
            StateStoreErrorKind::InvalidConfiguration
        );

        let cluster_error = open_error(&runtime, config(&path, "cluster-b", "fe-a"), deployment(1));
        assert_eq!(
            cluster_error.kind(),
            StateStoreErrorKind::InvalidConfiguration
        );
    }

    #[test]
    fn sqlite_open_rejects_memory_paths() {
        let runtime = runtime();

        for path in [
            ":memory:",
            "file::memory:?cache=shared",
            "file:state?mode=memory",
        ] {
            let error = open_error(&runtime, config(path, "cluster-a", "fe-a"), deployment(1));
            assert_eq!(
                error.kind(),
                StateStoreErrorKind::InvalidConfiguration,
                "memory path {path} must be rejected"
            );
        }
    }

    #[test]
    fn sqlite_open_rejects_unknown_schema_version() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("first handle");
        drop(first);

        let connection = Connection::open(&path).expect("open seeded store");
        let updated = connection
            .execute(
                "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
                params![2_u32.to_be_bytes().as_slice(), b"schema_version".as_slice()],
            )
            .expect("write unsupported schema version");
        assert_eq!(updated, 1);
        let persisted: Option<Vec<u8>> = connection
            .query_row(
                "SELECT value FROM state_store_meta WHERE key = ?1",
                params![b"schema_version".as_slice()],
                |row| row.get(0),
            )
            .optional()
            .expect("read schema version");
        assert_eq!(persisted, Some(2_u32.to_be_bytes().to_vec()));
        drop(connection);

        let error = open_error(&runtime, config(&path, "cluster-a", "fe-a"), deployment(1));
        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
    }
}
