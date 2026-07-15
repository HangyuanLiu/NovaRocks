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

#![allow(dead_code)] // Adapter-private until Task 6 wires the public StateStore traits.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rusqlite::{Connection, InterruptHandle, OptionalExtension, ffi, params};

use crate::state_store::{
    CommitOutcome, CommitReceipt, CommitResolution, Key, Precondition, StateRecord,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StoreRevision, TransactionId, Value,
    VersionToken,
};

use super::{SqliteStateStore, open_connection, schema};

const MUTATION_ENVELOPE_BYTES: usize = 32;
const SQLITE_BUSY_SNAPSHOT: i32 = ffi::SQLITE_BUSY_SNAPSHOT;

pub(super) type CommitRegistry = Arc<Mutex<HashMap<TransactionId, CommitRegistryState>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CommitRegistryState {
    InFlight,
    Committed(CommitReceipt),
    NotCommitted,
}

pub(super) fn new_commit_registry() -> CommitRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

#[derive(Clone, Debug)]
pub(super) enum Mutation {
    Put {
        value: Value,
        precondition: Precondition,
    },
    Delete {
        precondition: Precondition,
    },
}

pub(super) struct SqliteTxnState {
    connection: Connection,
    pub(super) overlay: BTreeMap<Key, Mutation>,
    mutations: Vec<(Key, Mutation)>,
    operation_count: usize,
    accounted_bytes: usize,
    limits: StateStoreLimits,
    pub(super) deadline: Instant,
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) range_frozen: bool,
    pub(super) interrupt_handle: Arc<InterruptHandle>,
    snapshot_established: bool,
    active: bool,
}

#[derive(Clone)]
struct TxnOwner {
    state: Arc<Mutex<SqliteTxnState>>,
    limits: StateStoreLimits,
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    interrupt_handle: Arc<InterruptHandle>,
}

pub(super) struct SqliteReadTransaction {
    owner: Option<TxnOwner>,
}

pub(super) struct SqliteWriteTransaction {
    owner: Option<TxnOwner>,
    transaction_id: TransactionId,
    path: PathBuf,
    commit_registry: CommitRegistry,
}

impl SqliteStateStore {
    pub(super) async fn begin_read(&self) -> Result<SqliteReadTransaction, StateStoreError> {
        let owner = begin_transaction(self.path.clone(), self.limits.clone()).await?;
        Ok(SqliteReadTransaction { owner: Some(owner) })
    }

    pub(super) async fn begin_write(
        &self,
        transaction_id: TransactionId,
    ) -> Result<SqliteWriteTransaction, StateStoreError> {
        let owner = begin_transaction(self.path.clone(), self.limits.clone()).await?;
        Ok(SqliteWriteTransaction {
            owner: Some(owner),
            transaction_id,
            path: self.path.clone(),
            commit_registry: Arc::clone(&self.commit_registry),
        })
    }

    pub(super) async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        if let Some(state) = lock_registry(&self.commit_registry)?
            .get(transaction_id)
            .cloned()
        {
            return Ok(match state {
                CommitRegistryState::InFlight => CommitResolution::Unresolved,
                CommitRegistryState::Committed(receipt) => CommitResolution::Committed(receipt),
                CommitRegistryState::NotCommitted => CommitResolution::NotCommitted,
            });
        }

        let path = self.path.clone();
        let recovery_lock = Arc::clone(&self.recovery_lock);
        let transaction_id = *transaction_id;
        let resolution = tokio::task::spawn_blocking(move || {
            let _recovery_guard = recovery_lock.lock().map_err(|_| internal_error())?;
            match lookup_commit(&path, transaction_id)? {
                Some(receipt) => Ok(CommitResolution::Committed(receipt)),
                None => Ok(CommitResolution::NotCommitted),
            }
        })
        .await
        .map_err(|_| worker_error())??;

        let terminal = match &resolution {
            CommitResolution::Committed(receipt) => CommitRegistryState::Committed(receipt.clone()),
            CommitResolution::NotCommitted => CommitRegistryState::NotCommitted,
            CommitResolution::Unresolved => unreachable!("recovery lookup is terminal"),
        };
        lock_registry(&self.commit_registry)?.insert(transaction_id, terminal);
        Ok(resolution)
    }
}

impl SqliteReadTransaction {
    pub(super) async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        get(self.owner()?, key.clone()).await
    }

    pub(super) async fn abort(mut self) -> Result<(), StateStoreError> {
        let owner = self.take_owner()?;
        run_operation(&owner, |state| rollback(state)).await
    }

    fn owner(&self) -> Result<&TxnOwner, StateStoreError> {
        self.owner.as_ref().ok_or_else(transaction_finished)
    }

    fn take_owner(&mut self) -> Result<TxnOwner, StateStoreError> {
        self.owner.take().ok_or_else(transaction_finished)
    }
}

impl Drop for SqliteReadTransaction {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            schedule_rollback(owner);
        }
    }
}

impl SqliteWriteTransaction {
    pub(super) async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        get(self.owner()?, key.clone()).await
    }

    pub(super) async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        validate_key_value(&key, Some(&value), &self.owner()?.limits)?;
        let owner = self.owner()?.clone();
        run_operation(&owner, move |state| {
            stage_mutation(
                state,
                key,
                Mutation::Put {
                    value,
                    precondition,
                },
            )
        })
        .await
    }

    pub(super) async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        validate_key_value(&key, None, &self.owner()?.limits)?;
        let owner = self.owner()?.clone();
        run_operation(&owner, move |state| {
            stage_mutation(state, key, Mutation::Delete { precondition })
        })
        .await
    }

    pub(super) async fn abort(mut self) -> Result<(), StateStoreError> {
        let owner = self.take_owner()?;
        run_operation(&owner, |state| rollback(state)).await
    }

    pub(super) async fn commit(mut self) -> CommitOutcome {
        let owner = match self.take_owner() {
            Ok(owner) => owner,
            Err(error) => return CommitOutcome::DefiniteFailure(error),
        };

        match register_inflight(&self.commit_registry, self.transaction_id) {
            Ok(Some(receipt)) => {
                schedule_rollback(owner);
                return CommitOutcome::Committed(receipt);
            }
            Ok(None) => {}
            Err(error) => {
                schedule_rollback(owner);
                return CommitOutcome::CommitUnknown(error);
            }
        }

        let state = Arc::clone(&owner.state);
        let registry = Arc::clone(&self.commit_registry);
        let transaction_id = self.transaction_id;
        let path = self.path.clone();
        let mut cancel_guard = CancelOnDrop::new(&owner);
        let mut worker = tokio::task::spawn_blocking(move || {
            let outcome = match state.lock() {
                Ok(mut state) => commit_blocking(&mut state, transaction_id, &path),
                Err(_) => CommitOutcome::CommitUnknown(internal_error()),
            };
            finalize_registry(&registry, transaction_id, &outcome);
            outcome
        });

        let deadline = tokio::time::Instant::from_std(owner.deadline);
        let outcome = match tokio::time::timeout_at(deadline, &mut worker).await {
            Ok(joined) => joined.unwrap_or_else(|_| CommitOutcome::CommitUnknown(worker_error())),
            Err(_) => {
                cancel_guard.cancel();
                worker
                    .await
                    .unwrap_or_else(|_| CommitOutcome::CommitUnknown(worker_error()))
            }
        };
        cancel_guard.disarm();
        outcome
    }

    fn owner(&self) -> Result<&TxnOwner, StateStoreError> {
        self.owner.as_ref().ok_or_else(transaction_finished)
    }

    fn take_owner(&mut self) -> Result<TxnOwner, StateStoreError> {
        self.owner.take().ok_or_else(transaction_finished)
    }
}

impl Drop for SqliteWriteTransaction {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            schedule_rollback(owner);
        }
    }
}

async fn begin_transaction(
    path: PathBuf,
    limits: StateStoreLimits,
) -> Result<TxnOwner, StateStoreError> {
    let deadline = Instant::now() + limits.transaction_deadline;
    let cancelled = Arc::new(AtomicBool::new(false));
    let interrupt_slot = Arc::new(Mutex::new(None));
    let worker_cancelled = Arc::clone(&cancelled);
    let worker_interrupt_slot = Arc::clone(&interrupt_slot);
    let mut cancel_guard =
        BeginCancelOnDrop::new(Arc::clone(&cancelled), Arc::clone(&interrupt_slot));
    let mut worker = tokio::task::spawn_blocking(move || {
        let connection = open_connection(&path)?;
        let interrupt_handle = Arc::new(connection.get_interrupt_handle());
        *worker_interrupt_slot.lock().map_err(|_| internal_error())? =
            Some(Arc::clone(&interrupt_handle));
        if worker_cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(deadline_error());
        }
        connection
            .busy_timeout(remaining(deadline))
            .map_err(|error| operation_error(&error, "failed to configure SQLite transaction"))?;
        connection
            .execute_batch("BEGIN DEFERRED")
            .map_err(|error| {
                operation_error(&error, "failed to begin SQLite deferred transaction")
            })?;
        if worker_cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            connection.execute_batch("ROLLBACK").map_err(|error| {
                operation_error(&error, "failed to roll back timed out SQLite begin")
            })?;
            return Err(deadline_error());
        }
        Ok::<_, StateStoreError>(SqliteTxnState {
            connection,
            overlay: BTreeMap::new(),
            mutations: Vec::new(),
            operation_count: 0,
            accounted_bytes: 0,
            limits,
            deadline,
            cancelled: worker_cancelled,
            range_frozen: false,
            interrupt_handle,
            snapshot_established: false,
            active: true,
        })
    });
    let state = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), &mut worker)
        .await
    {
        Ok(joined) => joined.map_err(|_| worker_error())??,
        Err(_) => {
            cancel_guard.cancel();
            if let Ok(Ok(mut state)) = worker.await {
                tokio::task::spawn_blocking(move || rollback(&mut state))
                    .await
                    .map_err(|_| worker_error())??;
            }
            cancel_guard.disarm();
            return Err(deadline_error());
        }
    };
    cancel_guard.disarm();

    let limits = state.limits.clone();
    let cancelled = Arc::clone(&state.cancelled);
    let interrupt_handle = Arc::clone(&state.interrupt_handle);
    Ok(TxnOwner {
        state: Arc::new(Mutex::new(state)),
        limits,
        deadline,
        cancelled,
        interrupt_handle,
    })
}

struct BeginCancelOnDrop {
    cancelled: Arc<AtomicBool>,
    interrupt_slot: Arc<Mutex<Option<Arc<InterruptHandle>>>>,
    armed: bool,
}

impl BeginCancelOnDrop {
    fn new(
        cancelled: Arc<AtomicBool>,
        interrupt_slot: Arc<Mutex<Option<Arc<InterruptHandle>>>>,
    ) -> Self {
        Self {
            cancelled,
            interrupt_slot,
            armed: true,
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(interrupt_slot) = self.interrupt_slot.lock()
            && let Some(interrupt_handle) = interrupt_slot.as_ref()
        {
            interrupt_handle.interrupt();
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BeginCancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

async fn get(owner: &TxnOwner, key: Key) -> Result<Option<StateRecord>, StateStoreError> {
    let owner = owner.clone();
    run_operation(&owner, move |state| {
        if let Some(mutation) = state.overlay.get(&key).cloned() {
            return match mutation {
                Mutation::Delete { .. } => Ok(None),
                Mutation::Put { value, .. } => {
                    let version = load_record(&state.connection, &key)?
                        .map(|record| record.version)
                        .unwrap_or_else(zero_version);
                    state.snapshot_established = true;
                    Ok(Some(StateRecord {
                        key,
                        value,
                        version,
                    }))
                }
            };
        }
        let record = load_record(&state.connection, &key)?;
        state.snapshot_established = true;
        Ok(record)
    })
    .await
}

async fn run_operation<T, F>(owner: &TxnOwner, operation: F) -> Result<T, StateStoreError>
where
    T: Send + 'static,
    F: FnOnce(&mut SqliteTxnState) -> Result<T, StateStoreError> + Send + 'static,
{
    let state = Arc::clone(&owner.state);
    let timeout_state = Arc::clone(&owner.state);
    let cancelled = Arc::clone(&owner.cancelled);
    let deadline = owner.deadline;
    let mut cancel_guard = CancelOnDrop::new(owner);
    let mut worker = tokio::task::spawn_blocking(move || {
        let mut state = state.lock().map_err(|_| internal_error())?;
        if !state.active {
            return Err(transaction_finished());
        }
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            rollback(&mut state)?;
            return Err(deadline_error());
        }
        state
            .connection
            .busy_timeout(remaining(deadline))
            .map_err(|error| operation_error(&error, "failed to configure SQLite transaction"))?;
        let result = operation(&mut state);
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            rollback(&mut state)?;
            return Err(deadline_error());
        }
        result
    });

    let result = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        &mut worker,
    )
    .await
    {
        Ok(joined) => joined.map_err(|_| worker_error())?,
        Err(_) => {
            cancel_guard.cancel();
            let _ = worker.await.map_err(|_| worker_error())?;
            tokio::task::spawn_blocking(move || {
                let mut state = timeout_state.lock().map_err(|_| internal_error())?;
                rollback(&mut state)
            })
            .await
            .map_err(|_| worker_error())??;
            Err(deadline_error())
        }
    };
    cancel_guard.disarm();
    result
}

struct CancelOnDrop {
    cancelled: Arc<AtomicBool>,
    interrupt_handle: Arc<InterruptHandle>,
    armed: bool,
}

impl CancelOnDrop {
    fn new(owner: &TxnOwner) -> Self {
        Self {
            cancelled: Arc::clone(&owner.cancelled),
            interrupt_handle: Arc::clone(&owner.interrupt_handle),
            armed: true,
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.interrupt_handle.interrupt();
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

fn schedule_rollback(owner: TxnOwner) {
    owner.cancelled.store(true, Ordering::Release);
    owner.interrupt_handle.interrupt();
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn_blocking(move || {
            if let Ok(mut state) = owner.state.lock() {
                let _ = rollback(&mut state);
            }
        });
    }
}

fn validate_key_value(
    key: &Key,
    value: Option<&Value>,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    if key.as_bytes().len() > limits.max_key_bytes {
        return Err(limit_error("key exceeds the configured byte limit"));
    }
    if value.is_some_and(|value| value.as_bytes().len() > limits.max_value_bytes) {
        return Err(limit_error("value exceeds the configured byte limit"));
    }
    Ok(())
}

fn stage_mutation(
    state: &mut SqliteTxnState,
    key: Key,
    mutation: Mutation,
) -> Result<(), StateStoreError> {
    if state.range_frozen {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "writes are frozen after paginated range reads",
        ));
    }
    let next_operations = state
        .operation_count
        .checked_add(1)
        .ok_or_else(|| limit_error("transaction operation limit exceeded"))?;
    if next_operations > state.limits.max_transaction_operations {
        return Err(limit_error("transaction operation limit exceeded"));
    }
    let mutation_bytes = accounted_mutation_bytes(&key, &mutation)?;
    let next_bytes = state
        .accounted_bytes
        .checked_add(mutation_bytes)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))?;
    if next_bytes > state.limits.max_transaction_bytes {
        return Err(limit_error("transaction byte limit exceeded"));
    }

    state.operation_count = next_operations;
    state.accounted_bytes = next_bytes;
    state.overlay.insert(key.clone(), mutation.clone());
    state.mutations.push((key, mutation));
    Ok(())
}

fn accounted_mutation_bytes(key: &Key, mutation: &Mutation) -> Result<usize, StateStoreError> {
    let value_bytes = match mutation {
        Mutation::Put {
            value,
            precondition,
        } => value.as_bytes().len() + precondition_bytes(precondition),
        Mutation::Delete { precondition } => precondition_bytes(precondition),
    };
    key.as_bytes()
        .len()
        .checked_add(value_bytes)
        .and_then(|bytes| bytes.checked_add(MUTATION_ENVELOPE_BYTES))
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))
}

fn precondition_bytes(precondition: &Precondition) -> usize {
    match precondition {
        Precondition::Version(version) => version.as_bytes().len(),
        _ => 0,
    }
}

fn commit_blocking(
    state: &mut SqliteTxnState,
    transaction_id: TransactionId,
    path: &PathBuf,
) -> CommitOutcome {
    if !state.active {
        return CommitOutcome::DefiniteFailure(transaction_finished());
    }
    if state.cancelled.load(Ordering::Acquire) || Instant::now() >= state.deadline {
        return rollback_outcome(state, CommitOutcome::DefiniteFailure(deadline_error()));
    }

    if let Err(error) = state.connection.busy_timeout(remaining(state.deadline)) {
        return rollback_outcome(
            state,
            CommitOutcome::TransientBeforeCommit(operation_error(
                &error,
                "failed to configure SQLite commit",
            )),
        );
    }

    let current_revision = match load_current_revision(&state.connection) {
        Ok(revision) => revision,
        Err(error) => {
            return rollback_outcome(state, CommitOutcome::DefiniteFailure(error));
        }
    };
    state.snapshot_established = true;
    let revision = match current_revision.checked_add(1) {
        Some(revision) if i64::try_from(revision).is_ok() => revision,
        _ => {
            return rollback_outcome(
                state,
                CommitOutcome::DefiniteFailure(StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "SQLite state store revision is exhausted",
                )),
            );
        }
    };

    let mutations = state.mutations.clone();
    let mut changed_keys = Vec::new();
    let mut seen_changed_keys = HashSet::new();
    for (key, mutation) in mutations {
        if state.cancelled.load(Ordering::Acquire) || Instant::now() >= state.deadline {
            return rollback_outcome(state, CommitOutcome::DefiniteFailure(deadline_error()));
        }
        let existing_version = match load_version(&state.connection, &key) {
            Ok(version) => version,
            Err(error) => {
                return rollback_outcome(state, CommitOutcome::DefiniteFailure(error));
            }
        };
        if !precondition_matches(mutation_precondition(&mutation), existing_version) {
            return rollback_outcome(
                state,
                CommitOutcome::Conflict(StateStoreError::new(
                    StateStoreErrorKind::PreconditionFailed,
                    "SQLite transaction precondition failed",
                )),
            );
        }

        let apply_result = apply_mutation(&state.connection, &key, &mutation, revision);
        let changed = match apply_result {
            Ok(changed) => changed,
            Err(error) => {
                let outcome = classify_apply_error(&error, state.snapshot_established);
                return rollback_outcome(state, outcome);
            }
        };
        if changed && seen_changed_keys.insert(key.clone()) {
            changed_keys.push(key);
        }
    }

    let revision_i64 = i64::try_from(revision).expect("revision checked above");
    for (sequence, key) in changed_keys.iter().enumerate() {
        if let Err(error) = state.connection.execute(
            "INSERT INTO state_store_changes(revision, sequence, key) VALUES (?1, ?2, ?3)",
            params![revision_i64, sequence as i64, key.as_bytes()],
        ) {
            let outcome = classify_apply_error(&error, state.snapshot_established);
            return rollback_outcome(state, outcome);
        }
    }

    let committed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    if let Err(error) = state.connection.execute(
        "INSERT INTO state_store_commits(transaction_id, revision, committed_at_ms) VALUES (?1, ?2, ?3)",
        params![transaction_id.as_uuid().as_bytes(), revision_i64, committed_at_ms],
    ) {
        let outcome = classify_apply_error(&error, state.snapshot_established);
        return rollback_outcome(state, outcome);
    }
    match state.connection.execute(
        "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
        params![
            revision.to_be_bytes().as_slice(),
            schema::CURRENT_REVISION_KEY
        ],
    ) {
        Ok(1) => {}
        Ok(_) => {
            return rollback_outcome(
                state,
                CommitOutcome::DefiniteFailure(StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "SQLite current revision metadata is missing",
                )),
            );
        }
        Err(error) => {
            let outcome = classify_apply_error(&error, state.snapshot_established);
            return rollback_outcome(state, outcome);
        }
    }

    match state.connection.execute_batch("COMMIT") {
        Ok(()) => {
            state.active = false;
            CommitOutcome::Committed(CommitReceipt {
                transaction_id,
                revision: revision_token(revision),
            })
        }
        Err(error) => classify_commit_error(state, transaction_id, path, &error),
    }
}

fn apply_mutation(
    connection: &Connection,
    key: &Key,
    mutation: &Mutation,
    revision: u64,
) -> rusqlite::Result<bool> {
    let revision = i64::try_from(revision).expect("revision checked before apply");
    match mutation {
        Mutation::Put { value, .. } => {
            connection.execute(
                "INSERT INTO state_store_kv(key, value, version) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, version = excluded.version",
                params![key.as_bytes(), value.as_bytes(), revision],
            )?;
            Ok(true)
        }
        Mutation::Delete { .. } => Ok(connection.execute(
            "DELETE FROM state_store_kv WHERE key = ?1",
            params![key.as_bytes()],
        )? > 0),
    }
}

fn mutation_precondition(mutation: &Mutation) -> &Precondition {
    match mutation {
        Mutation::Put { precondition, .. } | Mutation::Delete { precondition } => precondition,
    }
}

fn precondition_matches(precondition: &Precondition, existing_version: Option<u64>) -> bool {
    match precondition {
        Precondition::Any => true,
        Precondition::Absent => existing_version.is_none(),
        Precondition::Present => existing_version.is_some(),
        Precondition::Version(expected) => existing_version
            .map(|version| expected.as_bytes() == version.to_be_bytes())
            .unwrap_or(false),
    }
}

fn load_record(connection: &Connection, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
    let row = connection
        .query_row(
            "SELECT value, version FROM state_store_kv WHERE key = ?1",
            params![key.as_bytes()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|error| operation_error(&error, "failed to read SQLite state record"))?;
    row.map(|(value, version)| {
        let version = u64::try_from(version).map_err(|_| corruption_error())?;
        Ok(StateRecord {
            key: key.clone(),
            value: Value::try_from(Bytes::from(value))?,
            version: revision_version(version),
        })
    })
    .transpose()
}

fn load_version(connection: &Connection, key: &Key) -> Result<Option<u64>, StateStoreError> {
    connection
        .query_row(
            "SELECT version FROM state_store_kv WHERE key = ?1",
            params![key.as_bytes()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| operation_error(&error, "failed to validate SQLite precondition"))?
        .map(|version| u64::try_from(version).map_err(|_| corruption_error()))
        .transpose()
}

fn load_current_revision(connection: &Connection) -> Result<u64, StateStoreError> {
    let value = connection
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![schema::CURRENT_REVISION_KEY],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(|error| operation_error(&error, "failed to read SQLite store revision"))?;
    let bytes: [u8; 8] = value.try_into().map_err(|_| corruption_error())?;
    Ok(u64::from_be_bytes(bytes))
}

fn classify_apply_error(error: &rusqlite::Error, snapshot_established: bool) -> CommitOutcome {
    if is_busy_snapshot(error)
        || (snapshot_established
            && matches!(
                error.sqlite_error_code(),
                Some(ffi::ErrorCode::DatabaseBusy | ffi::ErrorCode::DatabaseLocked)
            ))
    {
        return CommitOutcome::Conflict(StateStoreError::new(
            StateStoreErrorKind::Conflict,
            "SQLite transaction snapshot conflicted",
        ));
    }
    match error.sqlite_error_code() {
        Some(ffi::ErrorCode::DatabaseBusy | ffi::ErrorCode::DatabaseLocked) => {
            CommitOutcome::TransientBeforeCommit(operation_error(
                error,
                "SQLite transaction was busy before commit",
            ))
        }
        _ => CommitOutcome::DefiniteFailure(operation_error(
            error,
            "SQLite transaction failed before commit",
        )),
    }
}

fn classify_commit_error(
    state: &mut SqliteTxnState,
    transaction_id: TransactionId,
    path: &PathBuf,
    error: &rusqlite::Error,
) -> CommitOutcome {
    let mapped = operation_error(error, "SQLite transaction commit failed");
    if !state.connection.is_autocommit() {
        if rollback(state).is_ok() {
            return CommitOutcome::DefiniteFailure(mapped);
        }
        return CommitOutcome::CommitUnknown(mapped);
    }
    state.active = false;
    match lookup_commit(path, transaction_id) {
        Ok(Some(receipt)) => CommitOutcome::Committed(receipt),
        Ok(None) => CommitOutcome::DefiniteFailure(mapped),
        Err(_) => CommitOutcome::CommitUnknown(mapped),
    }
}

fn rollback(state: &mut SqliteTxnState) -> Result<(), StateStoreError> {
    if !state.active {
        return Ok(());
    }
    state
        .connection
        .execute_batch("ROLLBACK")
        .map_err(|error| operation_error(&error, "failed to roll back SQLite transaction"))?;
    state.active = false;
    Ok(())
}

fn rollback_outcome(state: &mut SqliteTxnState, outcome: CommitOutcome) -> CommitOutcome {
    match rollback(state) {
        Ok(()) => outcome,
        Err(error) => CommitOutcome::CommitUnknown(error),
    }
}

fn register_inflight(
    registry: &CommitRegistry,
    transaction_id: TransactionId,
) -> Result<Option<CommitReceipt>, StateStoreError> {
    let mut registry = lock_registry(registry)?;
    match registry.get(&transaction_id) {
        Some(CommitRegistryState::Committed(receipt)) => Ok(Some(receipt.clone())),
        Some(CommitRegistryState::InFlight) => Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "SQLite transaction id is already in flight",
        )),
        Some(CommitRegistryState::NotCommitted) | None => {
            registry.insert(transaction_id, CommitRegistryState::InFlight);
            Ok(None)
        }
    }
}

fn finalize_registry(
    registry: &CommitRegistry,
    transaction_id: TransactionId,
    outcome: &CommitOutcome,
) {
    if let Ok(mut registry) = registry.lock() {
        match outcome {
            CommitOutcome::Committed(receipt) => {
                registry.insert(
                    transaction_id,
                    CommitRegistryState::Committed(receipt.clone()),
                );
            }
            CommitOutcome::Conflict(_)
            | CommitOutcome::TransientBeforeCommit(_)
            | CommitOutcome::DefiniteFailure(_) => {
                registry.insert(transaction_id, CommitRegistryState::NotCommitted);
            }
            CommitOutcome::CommitUnknown(_) => {
                registry.remove(&transaction_id);
            }
        }
    }
}

fn lookup_commit(
    path: &PathBuf,
    transaction_id: TransactionId,
) -> Result<Option<CommitReceipt>, StateStoreError> {
    let connection = open_connection(path)?;
    let revision = connection
        .query_row(
            "SELECT revision FROM state_store_commits WHERE transaction_id = ?1",
            params![transaction_id.as_uuid().as_bytes()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| operation_error(&error, "failed to resolve SQLite commit"))?;
    revision
        .map(|revision| {
            let revision = u64::try_from(revision).map_err(|_| corruption_error())?;
            Ok(CommitReceipt {
                transaction_id,
                revision: revision_token(revision),
            })
        })
        .transpose()
}

fn lock_registry(
    registry: &CommitRegistry,
) -> Result<std::sync::MutexGuard<'_, HashMap<TransactionId, CommitRegistryState>>, StateStoreError>
{
    registry.lock().map_err(|_| internal_error())
}

fn is_busy_snapshot(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(error, _) if error.extended_code == SQLITE_BUSY_SNAPSHOT
    )
}

fn operation_error(error: &rusqlite::Error, message: &'static str) -> StateStoreError {
    let kind = match error.sqlite_error_code() {
        Some(ffi::ErrorCode::OperationInterrupted) => StateStoreErrorKind::Cancelled,
        Some(ffi::ErrorCode::DatabaseBusy | ffi::ErrorCode::DatabaseLocked) => {
            StateStoreErrorKind::Transient
        }
        Some(ffi::ErrorCode::DatabaseCorrupt | ffi::ErrorCode::NotADatabase) => {
            StateStoreErrorKind::Corruption
        }
        Some(
            ffi::ErrorCode::CannotOpen
            | ffi::ErrorCode::SystemIoFailure
            | ffi::ErrorCode::ReadOnly
            | ffi::ErrorCode::DiskFull
            | ffi::ErrorCode::PermissionDenied,
        ) => StateStoreErrorKind::ProviderUnavailable,
        _ => StateStoreErrorKind::Internal,
    };
    StateStoreError::new(kind, message)
}

fn revision_token(revision: u64) -> StoreRevision {
    StoreRevision::try_from(Bytes::copy_from_slice(&revision.to_be_bytes()))
        .expect("u64 revision is non-empty")
}

fn revision_version(revision: u64) -> VersionToken {
    VersionToken::try_from(Bytes::copy_from_slice(&revision.to_be_bytes()))
        .expect("u64 version is non-empty")
}

fn zero_version() -> VersionToken {
    revision_version(0)
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

const fn transaction_finished() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Cancelled,
        "SQLite transaction is no longer active",
    )
}

const fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "SQLite transaction deadline exceeded",
    )
}

const fn worker_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "SQLite transaction blocking worker failed",
    )
}

const fn internal_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "SQLite transaction state is unavailable",
    )
}

const fn corruption_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "SQLite state store revision is malformed",
    )
}

const fn limit_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::LimitExceeded, message)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::TempDir;
    use tokio::sync::Barrier;
    use uuid::Uuid;

    use super::*;
    use crate::state_store::sqlite::SqliteStateStore;
    use crate::state_store::{
        CommitOutcome, CommitReceipt, CommitResolution, FeDeploymentView, Key, Precondition,
        StateRecord, StateStoreConfig, StateStoreErrorKind, StateStoreLimitOverrides,
        StateStoreProviderConfig, TransactionId, Value, VersionToken,
    };

    fn key(value: &'static [u8]) -> Key {
        Key::try_from(Bytes::from_static(value)).expect("valid key")
    }

    fn value(value: &'static [u8]) -> Value {
        Value::try_from(Bytes::from_static(value)).expect("valid value")
    }

    fn transaction_id() -> TransactionId {
        Uuid::now_v7().into()
    }

    async fn store(temp: &TempDir) -> Arc<SqliteStateStore> {
        Arc::new(
            SqliteStateStore::open(
                StateStoreConfig {
                    provider: StateStoreProviderConfig::Sqlite,
                    path: temp.path().join("state-store.sqlite"),
                    cluster_id: "cluster-a".to_owned(),
                    deployment_owner: "fe-a".to_owned(),
                    limits: StateStoreLimitOverrides::default(),
                },
                FeDeploymentView {
                    active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                    topology_revision: Bytes::from_static(b"topology-r1"),
                },
            )
            .await
            .expect("open SQLite store"),
        )
    }

    fn committed(outcome: CommitOutcome) -> CommitReceipt {
        match outcome {
            CommitOutcome::Committed(receipt) => receipt,
            other => panic!("expected committed outcome, got {other:?}"),
        }
    }

    fn assert_conflict(outcome: CommitOutcome) {
        match outcome {
            CommitOutcome::Conflict(error) => assert!(matches!(
                error.kind(),
                StateStoreErrorKind::Conflict | StateStoreErrorKind::PreconditionFailed
            )),
            other => panic!("expected conflict outcome, got {other:?}"),
        }
    }

    async fn put_committed(store: &SqliteStateStore, key: Key, value: Value) -> CommitReceipt {
        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");
        transaction
            .put(key, value, Precondition::Any)
            .await
            .expect("stage put");
        committed(transaction.commit().await)
    }

    async fn read_value(store: &SqliteStateStore, key: &Key) -> Option<StateRecord> {
        let mut transaction = store.begin_read().await.expect("begin read");
        let value = transaction.get(key).await.expect("read key");
        transaction.abort().await.expect("abort read");
        value
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_repeatable_point_read() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"repeatable");
        put_committed(&store, item.clone(), value(b"v1")).await;

        let mut reader = store.begin_read().await.expect("begin read");
        let first = reader
            .get(&item)
            .await
            .expect("first read")
            .expect("record");

        put_committed(&store, item.clone(), value(b"v2")).await;

        let second = reader
            .get(&item)
            .await
            .expect("second read")
            .expect("record");
        assert_eq!(first, second);
        reader.abort().await.expect("abort read");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_reads_own_ordered_mutations() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"overlay");
        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");

        transaction
            .put(item.clone(), value(b"v1"), Precondition::Absent)
            .await
            .expect("stage first put");
        assert_eq!(
            transaction
                .get(&item)
                .await
                .expect("read overlay")
                .expect("overlay record")
                .value,
            value(b"v1")
        );
        transaction
            .delete(item.clone(), Precondition::Present)
            .await
            .expect("stage delete");
        assert_eq!(transaction.get(&item).await.expect("read delete"), None);
        transaction
            .put(item.clone(), value(b"v2"), Precondition::Absent)
            .await
            .expect("stage second put");

        let receipt = committed(transaction.commit().await);
        assert_eq!(
            read_value(&store, &item)
                .await
                .expect("committed record")
                .value,
            value(b"v2")
        );

        let revision = u64::from_be_bytes(
            receipt
                .revision
                .as_bytes()
                .try_into()
                .expect("SQLite revision encoding"),
        );
        let path = store.path.clone();
        let transaction_id = receipt.transaction_id;
        let item_bytes = item.as_bytes().to_vec();
        let (kv_version, ledger_revision, change_count, current_revision) =
            tokio::task::spawn_blocking(move || {
                let connection = open_connection(&path).expect("inspection connection");
                let kv_version = connection
                    .query_row(
                        "SELECT version FROM state_store_kv WHERE key = ?1",
                        params![item_bytes],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("KV version");
                let ledger_revision = connection
                    .query_row(
                        "SELECT revision FROM state_store_commits WHERE transaction_id = ?1",
                        params![transaction_id.as_uuid().as_bytes()],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("ledger revision");
                let change_count = connection
                    .query_row(
                        "SELECT COUNT(*) FROM state_store_changes WHERE revision = ?1",
                        params![revision as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("change rows");
                let current_revision = connection
                    .query_row(
                        "SELECT value FROM state_store_meta WHERE key = ?1",
                        params![schema::CURRENT_REVISION_KEY],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .expect("current revision");
                (kv_version, ledger_revision, change_count, current_revision)
            })
            .await
            .expect("inspection worker");
        assert_eq!(kv_version, revision as i64);
        assert_eq!(ledger_revision, revision as i64);
        assert_eq!(change_count, 1, "same-key changes must be deduplicated");
        assert_eq!(current_revision, revision.to_be_bytes());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_rolls_back_all_keys_on_precondition_failure() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let guarded = key(b"guarded");
        let partial = key(b"must-not-commit");
        put_committed(&store, guarded.clone(), value(b"original")).await;

        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");
        transaction
            .put(partial.clone(), value(b"partial"), Precondition::Any)
            .await
            .expect("stage unguarded put");
        transaction
            .put(guarded.clone(), value(b"wrong"), Precondition::Absent)
            .await
            .expect("stage failing put");
        assert_conflict(transaction.commit().await);

        assert_eq!(read_value(&store, &partial).await, None);
        assert_eq!(
            read_value(&store, &guarded)
                .await
                .expect("guarded record")
                .value,
            value(b"original")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_enforces_all_preconditions() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"preconditions");

        let mut absent = store
            .begin_write(transaction_id())
            .await
            .expect("begin absent");
        absent
            .put(item.clone(), value(b"v1"), Precondition::Absent)
            .await
            .expect("stage absent put");
        committed(absent.commit().await);

        let mut present = store
            .begin_write(transaction_id())
            .await
            .expect("begin present");
        present
            .put(item.clone(), value(b"v2"), Precondition::Present)
            .await
            .expect("stage present put");
        committed(present.commit().await);

        let record = read_value(&store, &item).await.expect("versioned record");
        let mut versioned = store
            .begin_write(transaction_id())
            .await
            .expect("begin versioned");
        versioned
            .put(
                item.clone(),
                value(b"v3"),
                Precondition::Version(record.version),
            )
            .await
            .expect("stage versioned put");
        committed(versioned.commit().await);

        let mut stale = store
            .begin_write(transaction_id())
            .await
            .expect("begin stale");
        stale
            .delete(
                item.clone(),
                Precondition::Version(
                    VersionToken::try_from(Bytes::from_static(b"wrong-version"))
                        .expect("non-empty version"),
                ),
            )
            .await
            .expect("stage stale delete");
        assert_conflict(stale.commit().await);

        let mut missing = store
            .begin_write(transaction_id())
            .await
            .expect("begin missing");
        missing
            .delete(key(b"missing"), Precondition::Present)
            .await
            .expect("stage missing delete");
        assert_conflict(missing.commit().await);

        let mut any = store
            .begin_write(transaction_id())
            .await
            .expect("begin any");
        any.delete(item.clone(), Precondition::Any)
            .await
            .expect("stage any delete");
        committed(any.commit().await);
        assert_eq!(read_value(&store, &item).await, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_same_key_snapshot_conflict_has_one_winner() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"same-key");
        put_committed(&store, item.clone(), value(b"initial")).await;
        let barrier = Arc::new(Barrier::new(2));

        let writers = [value(b"writer-a"), value(b"writer-b")]
            .into_iter()
            .map(|next| {
                let store = Arc::clone(&store);
                let item = item.clone();
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    let mut transaction = store
                        .begin_write(transaction_id())
                        .await
                        .expect("begin concurrent write");
                    transaction
                        .get(&item)
                        .await
                        .expect("establish snapshot")
                        .expect("initial record");
                    barrier.wait().await;
                    transaction
                        .put(item, next, Precondition::Any)
                        .await
                        .expect("stage concurrent put");
                    transaction.commit().await
                })
            })
            .collect::<Vec<_>>();

        let mut committed_count = 0;
        let mut conflict_count = 0;
        for writer in writers {
            match writer.await.expect("writer task") {
                CommitOutcome::Committed(_) => committed_count += 1,
                CommitOutcome::Conflict(_) => conflict_count += 1,
                other => panic!("unexpected concurrent outcome: {other:?}"),
            }
        }
        assert_eq!((committed_count, conflict_count), (1, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_write_skew_snapshot_conflict_has_one_winner() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let left = key(b"doctor-left");
        let right = key(b"doctor-right");
        put_committed(&store, left.clone(), value(b"on-call")).await;
        put_committed(&store, right.clone(), value(b"on-call")).await;
        let barrier = Arc::new(Barrier::new(2));

        let writers = [left.clone(), right.clone()]
            .into_iter()
            .map(|delete_key| {
                let store = Arc::clone(&store);
                let left = left.clone();
                let right = right.clone();
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    let mut transaction = store
                        .begin_write(transaction_id())
                        .await
                        .expect("begin skew write");
                    transaction
                        .get(&left)
                        .await
                        .expect("read left")
                        .expect("left present");
                    transaction
                        .get(&right)
                        .await
                        .expect("read right")
                        .expect("right present");
                    barrier.wait().await;
                    transaction
                        .delete(delete_key, Precondition::Any)
                        .await
                        .expect("stage skew delete");
                    transaction.commit().await
                })
            })
            .collect::<Vec<_>>();

        let mut committed_count = 0;
        let mut conflict_count = 0;
        for writer in writers {
            match writer.await.expect("writer task") {
                CommitOutcome::Committed(_) => committed_count += 1,
                CommitOutcome::Conflict(_) => conflict_count += 1,
                other => panic!("unexpected skew outcome: {other:?}"),
            }
        }
        assert_eq!((committed_count, conflict_count), (1, 1));
        let survivors = usize::from(read_value(&store, &left).await.is_some())
            + usize::from(read_value(&store, &right).await.is_some());
        assert_eq!(survivors, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_resolves_inflight_committed_and_not_committed_ids() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let committed_id = transaction_id();
        let mut transaction = store
            .begin_write(committed_id)
            .await
            .expect("begin committed transaction");
        transaction
            .put(key(b"ledger"), value(b"value"), Precondition::Any)
            .await
            .expect("stage ledger put");
        let receipt = committed(transaction.commit().await);
        assert_eq!(
            store
                .resolve_commit(&committed_id)
                .await
                .expect("resolve registry commit"),
            CommitResolution::Committed(receipt.clone())
        );

        store
            .commit_registry
            .lock()
            .expect("commit registry")
            .remove(&committed_id);
        assert_eq!(
            store
                .resolve_commit(&committed_id)
                .await
                .expect("resolve ledger commit"),
            CommitResolution::Committed(receipt)
        );

        let missing_id = transaction_id();
        assert_eq!(
            store
                .resolve_commit(&missing_id)
                .await
                .expect("resolve missing transaction"),
            CommitResolution::NotCommitted
        );
        assert!(matches!(
            store
                .commit_registry
                .lock()
                .expect("commit registry")
                .get(&missing_id),
            Some(CommitRegistryState::NotCommitted)
        ));

        let inflight_id = transaction_id();
        store
            .commit_registry
            .lock()
            .expect("commit registry")
            .insert(inflight_id, CommitRegistryState::InFlight);
        assert_eq!(
            store
                .resolve_commit(&inflight_id)
                .await
                .expect("resolve in-flight transaction"),
            CommitResolution::Unresolved
        );
    }
}
