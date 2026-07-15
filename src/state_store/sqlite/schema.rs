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

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::{Uuid, Version};

use crate::state_store::{StateStoreError, StateStoreErrorKind, StoreIdentity};

use super::sqlite_error;

pub(super) const CURRENT_SCHEMA_VERSION: u32 = 1;
pub(super) const SCHEMA_VERSION_KEY: &[u8] = b"schema_version";
pub(super) const CLUSTER_ID_KEY: &[u8] = b"cluster_id";
pub(super) const STORE_ID_KEY: &[u8] = b"store_id";
pub(super) const INITIAL_INCARNATION_KEY: &[u8] = b"initial_incarnation";
pub(super) const DEPLOYMENT_OWNER_KEY: &[u8] = b"deployment_owner";
pub(super) const DATABASE_PATH_KEY: &[u8] = b"database_path";
pub(super) const CURRENT_REVISION_KEY: &[u8] = b"current_revision";
pub(super) const CHANGE_RETENTION_FLOOR_KEY: &[u8] = b"change_retention_floor";

const INITIAL_INCARNATION: u64 = 1;
const INITIAL_REVISION: u64 = 0;
const INITIAL_CHANGE_RETENTION_FLOOR: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff];

pub(super) fn initialize(
    connection: &mut Connection,
    cluster_id: &[u8],
    deployment_owner: &[u8],
    database_path: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Internal,
                "failed to start SQLite initialization transaction",
            )
        })?;
    transaction
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS state_store_meta (
                key BLOB PRIMARY KEY,
                value BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS state_store_kv (
                key BLOB PRIMARY KEY,
                value BLOB NOT NULL,
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS state_store_changes (
                revision INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                key BLOB NOT NULL,
                PRIMARY KEY(revision, sequence)
            );

            CREATE TABLE IF NOT EXISTS state_store_commits (
                transaction_id BLOB PRIMARY KEY,
                revision INTEGER NOT NULL,
                committed_at_ms INTEGER NOT NULL
            );
            "#,
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Internal,
                "failed to create SQLite state store schema",
            )
        })?;

    let identity = match load_optional(&transaction, SCHEMA_VERSION_KEY)? {
        None => initialize_identity(&transaction, cluster_id, deployment_owner, database_path)?,
        Some(version) => {
            validate_schema_version(&version)?;
            load_identity(&transaction, cluster_id, deployment_owner, database_path)?
        }
    };
    transaction.commit().map_err(|error| {
        sqlite_error(
            &error,
            StateStoreErrorKind::Internal,
            "failed to commit SQLite initialization transaction",
        )
    })?;
    Ok(identity)
}

fn initialize_identity(
    transaction: &Transaction<'_>,
    cluster_id: &[u8],
    deployment_owner: &[u8],
    database_path: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let existing_rows: i64 = transaction
        .query_row("SELECT COUNT(*) FROM state_store_meta", [], |row| {
            row.get(0)
        })
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store identity",
            )
        })?;
    if existing_rows != 0 {
        return Err(schema_error(
            "SQLite state store identity is partially initialized",
        ));
    }

    let store_id = Uuid::now_v7();
    insert_meta(
        transaction,
        SCHEMA_VERSION_KEY,
        &CURRENT_SCHEMA_VERSION.to_be_bytes(),
    )?;
    insert_meta(transaction, CLUSTER_ID_KEY, cluster_id)?;
    insert_meta(transaction, STORE_ID_KEY, store_id.as_bytes())?;
    insert_meta(
        transaction,
        INITIAL_INCARNATION_KEY,
        &INITIAL_INCARNATION.to_be_bytes(),
    )?;
    insert_meta(transaction, DEPLOYMENT_OWNER_KEY, deployment_owner)?;
    insert_meta(transaction, DATABASE_PATH_KEY, database_path)?;
    insert_meta(
        transaction,
        CURRENT_REVISION_KEY,
        &INITIAL_REVISION.to_be_bytes(),
    )?;
    insert_meta(
        transaction,
        CHANGE_RETENTION_FLOOR_KEY,
        &INITIAL_CHANGE_RETENTION_FLOOR,
    )?;

    Ok(StoreIdentity {
        store_id,
        cluster_id: String::from_utf8(cluster_id.to_vec())
            .map_err(|_| schema_error("configured SQLite cluster id is not UTF-8"))?,
        initial_incarnation: INITIAL_INCARNATION,
    })
}

fn load_identity(
    transaction: &Transaction<'_>,
    cluster_id: &[u8],
    deployment_owner: &[u8],
    database_path: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let stored_cluster_id = load_required(transaction, CLUSTER_ID_KEY)?;
    if stored_cluster_id != cluster_id {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store cluster id does not match configuration",
        ));
    }

    let stored_owner = load_required(transaction, DEPLOYMENT_OWNER_KEY)?;
    if stored_owner != deployment_owner {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store deployment owner does not match configuration",
        ));
    }

    let stored_database_path = load_required(transaction, DATABASE_PATH_KEY)?;
    if stored_database_path != database_path {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store database path does not match initialized identity",
        ));
    }

    let store_id = Uuid::from_slice(&load_required(transaction, STORE_ID_KEY)?)
        .map_err(|_| schema_error("SQLite state store id is malformed"))?;
    if store_id.get_version() != Some(Version::SortRand) {
        return Err(schema_error("SQLite state store id is not UUIDv7"));
    }

    let initial_incarnation = decode_u64(
        &load_required(transaction, INITIAL_INCARNATION_KEY)?,
        "SQLite initial incarnation is malformed",
    )?;
    if initial_incarnation != INITIAL_INCARNATION {
        return Err(schema_error(
            "SQLite initial incarnation has an unsupported value",
        ));
    }
    let current_revision = decode_u64(
        &load_required(transaction, CURRENT_REVISION_KEY)?,
        "SQLite current revision is malformed",
    )?;
    let retention_floor =
        decode_change_retention_floor(&load_required(transaction, CHANGE_RETENTION_FLOOR_KEY)?)?;
    validate_change_retention_floor(retention_floor, current_revision)?;

    let cluster_id = String::from_utf8(stored_cluster_id)
        .map_err(|_| schema_error("SQLite cluster id is not UTF-8"))?;
    Ok(StoreIdentity {
        store_id,
        cluster_id,
        initial_incarnation,
    })
}

pub(super) fn load_change_retention_floor(
    connection: &Connection,
    current_revision: u64,
) -> Result<(u64, u32), StateStoreError> {
    let value = connection
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![CHANGE_RETENTION_FLOOR_KEY],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to read SQLite change retention floor",
            )
        })?
        .ok_or_else(|| schema_error("SQLite change retention floor is missing"))?;
    let retention_floor = decode_change_retention_floor(&value)?;
    validate_change_retention_floor(retention_floor, current_revision)?;
    Ok(retention_floor)
}

fn decode_change_retention_floor(value: &[u8]) -> Result<(u64, u32), StateStoreError> {
    let bytes: [u8; 12] = value
        .try_into()
        .map_err(|_| schema_error("SQLite change retention floor is malformed"))?;
    let revision = u64::from_be_bytes(bytes[..8].try_into().expect("fixed revision bytes"));
    if i64::try_from(revision).is_err() {
        return Err(schema_error(
            "SQLite change retention floor revision is out of range",
        ));
    }
    let sequence = u32::from_be_bytes(bytes[8..].try_into().expect("fixed sequence bytes"));
    Ok((revision, sequence))
}

fn validate_change_retention_floor(
    retention_floor: (u64, u32),
    current_revision: u64,
) -> Result<(), StateStoreError> {
    if retention_floor.0 > current_revision {
        return Err(schema_error(
            "SQLite change retention floor is ahead of current revision",
        ));
    }
    Ok(())
}

fn validate_schema_version(value: &[u8]) -> Result<(), StateStoreError> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| schema_error("SQLite state store schema version is malformed"))?;
    if u32::from_be_bytes(bytes) != CURRENT_SCHEMA_VERSION {
        return Err(schema_error(
            "SQLite state store schema version is unsupported",
        ));
    }
    Ok(())
}

fn decode_u64(value: &[u8], message: &'static str) -> Result<u64, StateStoreError> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| schema_error(message))?;
    Ok(u64::from_be_bytes(bytes))
}

fn insert_meta(
    transaction: &Transaction<'_>,
    key: &[u8],
    value: &[u8],
) -> Result<(), StateStoreError> {
    transaction
        .execute(
            "INSERT INTO state_store_meta(key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Internal,
                "failed to initialize SQLite state store identity",
            )
        })?;
    Ok(())
}

fn load_required(transaction: &Transaction<'_>, key: &[u8]) -> Result<Vec<u8>, StateStoreError> {
    load_optional(transaction, key)?
        .ok_or_else(|| schema_error("SQLite state store identity is missing required metadata"))
}

fn load_optional(
    transaction: &Transaction<'_>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StateStoreError> {
    transaction
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to read SQLite state store identity",
            )
        })
}

const fn schema_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Corruption, message)
}
