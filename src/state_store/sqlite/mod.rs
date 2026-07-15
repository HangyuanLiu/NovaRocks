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
use std::path::{Path, PathBuf};

use fs2::FileExt;
use rusqlite::ffi::ErrorCode as SqliteErrorCode;
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
    let database_path = database_path_bytes(&path)?;
    let owner_lock = acquire_owner_lock(&path)?;
    let mut connection = open_connection(&path)?;
    let identity = schema::initialize(
        &mut connection,
        config.cluster_id.as_bytes(),
        config.deployment_owner.as_bytes(),
        &database_path,
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
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let connection = Connection::open_with_flags(path, flags).map_err(|error| {
        sqlite_error(
            &error,
            StateStoreErrorKind::ProviderUnavailable,
            "failed to open SQLite state store database",
        )
    })?;
    let journal_mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store connection",
            )
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(connection_configuration_error());
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store connection",
            )
        })?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store connection",
            )
        })?;

    let synchronous = connection
        .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to inspect SQLite state store connection configuration",
            )
        })?;
    let foreign_keys = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to inspect SQLite state store connection configuration",
            )
        })?;
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
    Ok(canonical_parent.join(file_name))
}

fn database_path_bytes(path: &Path) -> Result<Vec<u8>, StateStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;

        let native_path = path.as_os_str().as_bytes();
        let mut encoded = Vec::with_capacity(b"unix\0".len() + native_path.len());
        encoded.extend_from_slice(b"unix\0");
        encoded.extend_from_slice(native_path);
        return Ok(encoded);
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        let mut encoded = b"windows-utf16le\0".to_vec();
        for code_unit in path.as_os_str().encode_wide() {
            encoded.extend_from_slice(&code_unit.to_le_bytes());
        }
        return Ok(encoded);
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store native path identity encoding is unsupported on this target",
        ))
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

fn sqlite_error(
    error: &rusqlite::Error,
    fallback: StateStoreErrorKind,
    message: &'static str,
) -> StateStoreError {
    StateStoreError::new(sqlite_error_kind(error, fallback), message)
}

fn sqlite_error_kind(
    error: &rusqlite::Error,
    fallback: StateStoreErrorKind,
) -> StateStoreErrorKind {
    match error.sqlite_error_code() {
        Some(SqliteErrorCode::DatabaseBusy | SqliteErrorCode::DatabaseLocked) => {
            StateStoreErrorKind::Transient
        }
        Some(
            SqliteErrorCode::CannotOpen
            | SqliteErrorCode::SystemIoFailure
            | SqliteErrorCode::ReadOnly
            | SqliteErrorCode::DiskFull
            | SqliteErrorCode::PermissionDenied
            | SqliteErrorCode::AuthorizationForStatementDenied,
        ) => StateStoreErrorKind::ProviderUnavailable,
        Some(
            SqliteErrorCode::DatabaseCorrupt
            | SqliteErrorCode::NotADatabase
            | SqliteErrorCode::SchemaChanged,
        ) => StateStoreErrorKind::Corruption,
        _ => fallback,
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io::ErrorKind;
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};

    #[cfg(unix)]
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    #[cfg(windows)]
    use std::os::windows::ffi::OsStringExt;

    use bytes::Bytes;
    use rusqlite::{Connection, OptionalExtension, ffi, params};
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

        let connection =
            open_connection(&store.path).expect("configured SQLite connection on canonical path");
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
        let database_path: Vec<u8> = connection
            .query_row(
                "SELECT value FROM state_store_meta WHERE key = ?1",
                params![b"database_path".as_slice()],
                |row| row.get(0),
            )
            .expect("database path identity row");
        assert_eq!(cluster, b"cluster-a");
        assert_eq!(owner, b"fe-a");
        assert_eq!(database_path, database_path_bytes(&store.path).unwrap());
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

    #[cfg(unix)]
    #[test]
    fn sqlite_open_preserves_tagged_non_utf8_native_path_identity() {
        let temp = TempDir::new().expect("temp dir");
        let file_name = OsString::from_vec(b"state-store-\xff.sqlite".to_vec());
        let path = temp.path().join(file_name);
        let canonical_path = canonicalize_database_path(&path).expect("canonical database path");
        let mut expected = b"unix\0".to_vec();
        expected.extend_from_slice(canonical_path.as_os_str().as_bytes());

        assert_eq!(database_path_bytes(&canonical_path).unwrap(), expected);

        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(probe) => {
                drop(probe);
                fs::remove_file(&path).expect("remove native-path filesystem probe");
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
                ) || error.raw_os_error() == Some(libc::EILSEQ) =>
            {
                eprintln!(
                    "skipping non-UTF-8 SQLite open roundtrip: filesystem rejected native path: {error}"
                );
                return;
            }
            Err(error) => panic!("probe non-UTF-8 native path support: {error}"),
        }

        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("open SQLite database with non-UTF-8 path");
        let first_identity = first.identity_snapshot().clone();
        let connection = open_connection(&first.path).expect("reopen non-UTF-8 database path");
        let stored_path: Vec<u8> = connection
            .query_row(
                "SELECT value FROM state_store_meta WHERE key = ?1",
                params![b"database_path".as_slice()],
                |row| row.get(0),
            )
            .expect("database path identity row");
        assert_eq!(stored_path, expected);
        drop(connection);
        drop(first);

        let restarted = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("restart SQLite database with non-UTF-8 path");
        assert_eq!(restarted.identity_snapshot(), &first_identity);
    }

    #[cfg(windows)]
    #[test]
    fn sqlite_path_identity_encodes_unpaired_surrogate_as_tagged_utf16le() {
        let path = PathBuf::from(OsString::from_wide(&[0x0061, 0xd800, 0x0062]));

        assert_eq!(
            database_path_bytes(&path).unwrap(),
            b"windows-utf16le\0a\0\0\xd8b\0"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_open_rejects_final_component_symlink_without_touching_target_schema() {
        let temp = TempDir::new().expect("temp dir");
        let target = temp.path().join("target.sqlite");
        let configured_path = temp.path().join("state-store.sqlite");
        let connection = Connection::open(&target).expect("create target database");
        connection
            .execute_batch("CREATE TABLE target_sentinel(value TEXT NOT NULL);")
            .expect("create target sentinel");
        drop(connection);
        symlink(&target, &configured_path).expect("create final-component symlink");

        let error = open_error(
            &runtime(),
            config(&configured_path, "cluster-a", "fe-a"),
            deployment(1),
        );

        assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);
        let connection = Connection::open(&target).expect("reopen target database");
        let state_store_tables: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name LIKE 'state_store_%'",
                [],
                |row| row.get(0),
            )
            .expect("count target state store tables");
        assert_eq!(
            state_store_tables, 0,
            "symlink target must remain untouched"
        );
    }

    #[test]
    fn sqlite_open_rejects_hardlink_alternate_path_for_initialized_database() {
        let temp = TempDir::new().expect("temp dir");
        let primary_path = temp.path().join("primary.sqlite");
        let alternate_path = temp.path().join("alternate.sqlite");
        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&primary_path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("initialize primary database path");
        drop(first);

        if let Err(error) = fs::hard_link(&primary_path, &alternate_path) {
            if error.kind() == ErrorKind::Unsupported {
                eprintln!("skipping hardlink identity test: platform does not support hardlinks");
                return;
            }
            panic!("create hardlink alternate path: {error}");
        }

        let error = open_error(
            &runtime,
            config(&alternate_path, "cluster-a", "fe-a"),
            deployment(1),
        );

        assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
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

    #[test]
    fn sqlite_open_rolls_back_schema_creation_for_unknown_initial_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let connection = Connection::open(&path).expect("create malformed database");
        connection
            .execute_batch(
                "CREATE TABLE state_store_meta (key BLOB PRIMARY KEY, value BLOB NOT NULL);",
            )
            .expect("create state store metadata table");
        connection
            .execute(
                "INSERT INTO state_store_meta(key, value) VALUES (?1, ?2)",
                params![b"unknown_key".as_slice(), b"unknown_value".as_slice()],
            )
            .expect("insert unknown metadata");
        drop(connection);

        let error = open_error(
            &runtime(),
            config(&path, "cluster-a", "fe-a"),
            deployment(1),
        );

        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
        let connection = Connection::open(&path).expect("reopen malformed database");
        let mut statement = connection
            .prepare(
                "SELECT name FROM sqlite_schema \
                 WHERE type = 'table' AND name LIKE 'state_store_%' ORDER BY name",
            )
            .expect("prepare table query");
        let tables = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query tables")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect tables");
        assert_eq!(tables, ["state_store_meta"]);
        let metadata: Vec<u8> = connection
            .query_row(
                "SELECT value FROM state_store_meta WHERE key = ?1",
                params![b"unknown_key".as_slice()],
                |row| row.get(0),
            )
            .expect("unknown metadata must remain unchanged");
        assert_eq!(metadata, b"unknown_value");
    }

    #[test]
    fn sqlite_open_classifies_immediate_transaction_contention_as_transient() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("state-store.sqlite");
        let runtime = runtime();
        let first = runtime
            .block_on(SqliteStateStore::open(
                config(&path, "cluster-a", "fe-a"),
                deployment(1),
            ))
            .expect("initialize state store");
        drop(first);

        let blocker = Connection::open(&path).expect("open external connection");
        blocker
            .execute_batch("BEGIN IMMEDIATE")
            .expect("hold immediate transaction");

        let error = open_error(&runtime, config(&path, "cluster-a", "fe-a"), deployment(1));

        assert_eq!(error.kind(), StateStoreErrorKind::Transient);
        blocker
            .execute_batch("ROLLBACK")
            .expect("release external transaction");
    }

    #[test]
    fn sqlite_open_classifies_sqlite_primary_error_codes() {
        let cases = [
            (ffi::SQLITE_BUSY, StateStoreErrorKind::Transient),
            (ffi::SQLITE_LOCKED, StateStoreErrorKind::Transient),
            (
                ffi::SQLITE_CANTOPEN,
                StateStoreErrorKind::ProviderUnavailable,
            ),
            (ffi::SQLITE_IOERR, StateStoreErrorKind::ProviderUnavailable),
            (
                ffi::SQLITE_READONLY,
                StateStoreErrorKind::ProviderUnavailable,
            ),
            (ffi::SQLITE_FULL, StateStoreErrorKind::ProviderUnavailable),
            (ffi::SQLITE_PERM, StateStoreErrorKind::ProviderUnavailable),
            (ffi::SQLITE_AUTH, StateStoreErrorKind::ProviderUnavailable),
            (ffi::SQLITE_CORRUPT, StateStoreErrorKind::Corruption),
            (ffi::SQLITE_NOTADB, StateStoreErrorKind::Corruption),
            (ffi::SQLITE_SCHEMA, StateStoreErrorKind::Corruption),
        ];

        for (code, expected) in cases {
            let error = rusqlite::Error::SqliteFailure(ffi::Error::new(code), None);
            assert_eq!(
                sqlite_error_kind(&error, StateStoreErrorKind::Internal),
                expected,
                "SQLite result code {code}"
            );
        }
    }
}
