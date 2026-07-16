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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant as StdInstant;

use async_trait::async_trait;
use futures::future::BoxFuture;
use mysql_async::{Conn, IsolationLevel, Transaction, TxOpts, prelude::Queryable};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

use super::budget::TransactionBudget;
use super::client::{
    MysqlPoolConnection, PoolLifecycle, checkout_hygienic_connection, execute_owned_with_deadline,
};
use super::codec::MysqlCodec;
use super::range::{decode_record, read_range_page};
use crate::state_store::runtime::MysqlRuntimeGuard;
use crate::state_store::{
    CommitOutcome, CommitReceipt, Direction, Key, Precondition, RangePage, RangeRequest,
    ReadTransaction, StateRecord, StateStoreError, StateStoreErrorKind, StateStoreLimits,
    StateStoreMetrics, StateStoreOperation, StateStoreOutcome, StoreRevision, TransactionId, Value,
    VersionToken, WriteTransaction,
};

const PROVISIONAL_VERSION_TAG: &[u8] = b"mysql-provisional-v1\0";
static EXPLICIT_ROLLBACKS: AtomicU64 = AtomicU64::new(0);

pub(super) struct OwnedMysqlTransaction {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlRuntimeGuard>,
    deadline: Instant,
    active: bool,
    statement_in_flight: bool,
}

pub(super) struct MysqlReadTransaction {
    commands: Option<mpsc::Sender<ReadCommand>>,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PointObservation {
    Absent,
    Present(VersionToken),
}

#[derive(Clone, Debug)]
enum Mutation {
    Put {
        value: Value,
        precondition: Precondition,
        provisional_version: VersionToken,
    },
    Delete {
        precondition: Precondition,
    },
}

impl Mutation {
    fn precondition(&self) -> &Precondition {
        match self {
            Self::Put { precondition, .. } | Self::Delete { precondition } => precondition,
        }
    }
}

pub(super) struct MysqlWriteTransaction {
    commands: Option<mpsc::Sender<WriteCommand>>,
    transaction_id: TransactionId,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
}

struct MysqlWriteActorState {
    transaction: Option<OwnedMysqlTransaction>,
    codec: MysqlCodec,
    limits: StateStoreLimits,
    transaction_id: TransactionId,
    base_revision: u64,
    point_observations: BTreeMap<Key, PointObservation>,
    range_observed: bool,
    mutations: Vec<(Key, Mutation)>,
    overlay: BTreeMap<Key, Mutation>,
    budget: TransactionBudget,
    range_frozen: bool,
}

enum WriteCommand {
    Get {
        key: Key,
        response: oneshot::Sender<Result<Option<StateRecord>, StateStoreError>>,
    },
    Range {
        request: RangeRequest,
        response: oneshot::Sender<Result<RangePage, StateStoreError>>,
    },
    Put {
        key: Key,
        value: Value,
        precondition: Precondition,
        response: oneshot::Sender<Result<usize, StateStoreError>>,
    },
    Delete {
        key: Key,
        precondition: Precondition,
        response: oneshot::Sender<Result<usize, StateStoreError>>,
    },
    Abort {
        response: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Commit {
        response: oneshot::Sender<CommitOutcome>,
    },
}

enum ReadCommand {
    Get {
        key: Key,
        response: oneshot::Sender<Result<Option<StateRecord>, StateStoreError>>,
    },
    Range {
        request: RangeRequest,
        response: oneshot::Sender<Result<RangePage, StateStoreError>>,
    },
    Abort {
        response: oneshot::Sender<Result<(), StateStoreError>>,
    },
}

enum ReadActorDisposition {
    Rollback,
    Destroy,
}

pub(super) async fn begin_read(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
) -> Result<MysqlReadTransaction, StateStoreError> {
    let started = StdInstant::now();
    let deadline = Instant::now() + limits.transaction_deadline;
    let codec = MysqlCodec::new(limits.max_key_bytes)?;
    let (commands, receiver) = mpsc::channel(1);
    let (ready_sender, ready_receiver) = oneshot::channel();
    let actor_limits = limits.clone();
    tokio::spawn(async move {
        read_actor(
            pool,
            operation,
            codec,
            actor_limits,
            deadline,
            receiver,
            ready_sender,
        )
        .await;
    });
    let result = ready_receiver.await.map_err(|_| owner_stopped())?;
    record_result(&metrics, StateStoreOperation::Begin, started, &result);
    result?;
    Ok(MysqlReadTransaction {
        commands: Some(commands),
        limits,
        metrics,
    })
}

pub(super) async fn begin_write(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    transaction_id: TransactionId,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
) -> Result<MysqlWriteTransaction, StateStoreError> {
    let started = StdInstant::now();
    let deadline = Instant::now() + limits.transaction_deadline;
    let (commands, receiver) = mpsc::channel(1);
    let (ready_sender, ready_receiver) = oneshot::channel();
    let actor_limits = limits.clone();
    tokio::spawn(async move {
        write_actor(
            pool,
            operation,
            transaction_id,
            actor_limits,
            deadline,
            receiver,
            ready_sender,
        )
        .await;
    });
    let result = ready_receiver.await.map_err(|_| owner_stopped())?;
    record_result(&metrics, StateStoreOperation::Begin, started, &result);
    result?;
    Ok(MysqlWriteTransaction {
        commands: Some(commands),
        transaction_id,
        limits,
        metrics,
    })
}

async fn write_actor(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    transaction_id: TransactionId,
    limits: StateStoreLimits,
    deadline: Instant,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: oneshot::Sender<Result<(), StateStoreError>>,
) {
    let result = initialize_write_actor(pool, operation, transaction_id, limits, deadline).await;
    let mut state = match result {
        Ok(state) => {
            if ready.send(Ok(())).is_err() {
                return;
            }
            state
        }
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    while let Some(command) = commands.recv().await {
        match command {
            WriteCommand::Get { key, response } => {
                let _ = response.send(state.get_inner(&key).await);
            }
            WriteCommand::Range { request, response } => {
                let _ = response.send(state.range_inner(&request).await);
            }
            WriteCommand::Put {
                key,
                value,
                precondition,
                response,
            } => {
                let _ = response.send(state.put_inner(key, value, precondition));
            }
            WriteCommand::Delete {
                key,
                precondition,
                response,
            } => {
                let _ = response.send(state.delete_inner(key, precondition));
            }
            WriteCommand::Abort { response } => {
                let result = match state.take_transaction() {
                    Ok(transaction) => transaction.rollback().await,
                    Err(error) => Err(error),
                };
                let _ = response.send(result);
                return;
            }
            WriteCommand::Commit { response } => {
                let outcome = state.commit_inner().await;
                let _ = response.send(outcome);
                return;
            }
        }
    }
}

async fn initialize_write_actor(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    transaction_id: TransactionId,
    limits: StateStoreLimits,
    deadline: Instant,
) -> Result<MysqlWriteActorState, StateStoreError> {
    let mut transaction = OwnedMysqlTransaction::begin(pool, operation, deadline, false).await?;
    let revision_bytes: Option<Vec<u8>> = transaction
        .run(|connection| {
            Box::pin(connection.exec_first(
                "SELECT meta_value FROM state_store_meta WHERE meta_key = ?",
                (b"current_revision".to_vec(),),
            ))
        })
        .await?;
    let codec = MysqlCodec::new(limits.max_key_bytes)?;
    let base_revision =
        codec.decode_revision(revision_bytes.as_deref().ok_or_else(persisted_corruption)?)?;
    Ok(MysqlWriteActorState {
        transaction: Some(transaction),
        codec,
        limits: limits.clone(),
        transaction_id,
        base_revision,
        point_observations: BTreeMap::new(),
        range_observed: false,
        mutations: Vec::new(),
        overlay: BTreeMap::new(),
        budget: TransactionBudget::new(limits),
        range_frozen: false,
    })
}

async fn read_actor(
    pool: Arc<dyn PoolLifecycle>,
    _operation: MysqlRuntimeGuard,
    codec: MysqlCodec,
    limits: StateStoreLimits,
    deadline: Instant,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: oneshot::Sender<Result<(), StateStoreError>>,
) {
    if ready.send(Ok(())).is_err() {
        return;
    }
    let Some(first_command) = commands.recv().await else {
        return;
    };
    let mut connection = match checkout_hygienic_connection(pool, deadline).await {
        Ok(connection) => connection,
        Err(error) => {
            respond_read_start_error(first_command, error);
            return;
        }
    };
    let disposition = run_read_actor(
        &mut connection,
        &codec,
        &limits,
        deadline,
        first_command,
        &mut commands,
    )
    .await;
    if matches!(disposition, ReadActorDisposition::Destroy) {
        connection.destroy().await;
    }
}

async fn run_read_actor(
    connection: &mut MysqlPoolConnection,
    codec: &MysqlCodec,
    limits: &StateStoreLimits,
    deadline: Instant,
    first_command: ReadCommand,
    commands: &mut mpsc::Receiver<ReadCommand>,
) -> ReadActorDisposition {
    let mut options = TxOpts::default();
    options
        .with_isolation_level(IsolationLevel::RepeatableRead)
        .with_consistent_snapshot(true)
        .with_readonly(true);
    super::client::record_statement();
    let mut transaction = match timeout_at(deadline, connection.start_transaction(options)).await {
        Ok(Ok(transaction)) => transaction,
        Ok(Err(error)) => {
            respond_read_start_error(
                first_command,
                super::error::MysqlNativeError::from(error).into_public(),
            );
            return ReadActorDisposition::Destroy;
        }
        Err(_) => {
            respond_read_start_error(first_command, deadline_error());
            return ReadActorDisposition::Destroy;
        }
    };

    let mut disposition = ReadActorDisposition::Rollback;
    let mut next_command = Some(first_command);
    while let Some(command) = match next_command.take() {
        Some(command) => Some(command),
        None => commands.recv().await,
    } {
        match command {
            ReadCommand::Get { key, response } => {
                let result = actor_get(&mut transaction, &codec, &limits, &key, deadline).await;
                let terminal = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.kind() == StateStoreErrorKind::DeadlineExceeded);
                let _ = response.send(result);
                if terminal {
                    disposition = ReadActorDisposition::Destroy;
                    break;
                }
            }
            ReadCommand::Range { request, response } => {
                let result = read_range_page(
                    &mut transaction,
                    &codec,
                    &request,
                    limits.max_value_bytes,
                    deadline,
                )
                .await;
                let terminal = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.kind() == StateStoreErrorKind::DeadlineExceeded);
                let _ = response.send(result);
                if terminal {
                    disposition = ReadActorDisposition::Destroy;
                    break;
                }
            }
            ReadCommand::Abort { response } => {
                let rollback = timeout_at(deadline, transaction.rollback()).await;
                match rollback {
                    Ok(Ok(())) => {
                        EXPLICIT_ROLLBACKS.fetch_add(1, Ordering::Relaxed);
                        let _ = response.send(Ok(()));
                        return ReadActorDisposition::Rollback;
                    }
                    Ok(Err(error)) => {
                        let _ = response.send(Err(
                            super::error::MysqlNativeError::from(error).into_public()
                        ));
                        return ReadActorDisposition::Destroy;
                    }
                    Err(_) => {
                        let _ = response.send(Err(deadline_error()));
                        return ReadActorDisposition::Destroy;
                    }
                }
            }
        }
    }

    match disposition {
        ReadActorDisposition::Rollback => {
            let rollback = timeout_at(
                Instant::now() + std::time::Duration::from_secs(1),
                transaction.rollback(),
            )
            .await;
            if matches!(rollback, Ok(Ok(()))) {
                ReadActorDisposition::Rollback
            } else {
                ReadActorDisposition::Destroy
            }
        }
        ReadActorDisposition::Destroy => {
            drop(transaction);
            ReadActorDisposition::Destroy
        }
    }
}

fn respond_read_start_error(command: ReadCommand, error: StateStoreError) {
    match command {
        ReadCommand::Get { response, .. } => {
            let _ = response.send(Err(error));
        }
        ReadCommand::Range { response, .. } => {
            let _ = response.send(Err(error));
        }
        ReadCommand::Abort { response } => {
            let _ = response.send(Err(error));
        }
    }
}

async fn actor_get(
    transaction: &mut Transaction<'_>,
    codec: &MysqlCodec,
    limits: &StateStoreLimits,
    key: &Key,
    deadline: Instant,
) -> Result<Option<StateRecord>, StateStoreError> {
    validate_key(key, limits)?;
    let key_bytes = key.as_bytes().to_vec();
    super::client::record_statement();
    let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = match timeout_at(
        deadline,
        transaction.exec_first(
            "SELECT key_bytes, value_bytes, version_bytes
             FROM state_store_kv WHERE key_bytes = ?",
            (key_bytes,),
        ),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(error)) => {
            return Err(super::error::MysqlNativeError::from(error).into_public());
        }
        Err(_) => return Err(deadline_error()),
    };
    row.map(|(key, value, version)| {
        decode_record(codec, key, value, version, limits.max_value_bytes)
    })
    .transpose()
}

impl OwnedMysqlTransaction {
    async fn begin(
        pool: Arc<dyn PoolLifecycle>,
        operation: MysqlRuntimeGuard,
        deadline: Instant,
        read_only: bool,
    ) -> Result<Self, StateStoreError> {
        let connection = checkout_hygienic_connection(pool, deadline).await?;
        let mut transaction = Self {
            connection: Some(connection),
            operation: Some(operation),
            deadline,
            active: false,
            statement_in_flight: false,
        };
        transaction
            .run(|connection| {
                Box::pin(connection.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            })
            .await?;
        let started = if read_only {
            transaction
                .run(|connection| {
                    Box::pin(
                        connection
                            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"),
                    )
                })
                .await
        } else {
            transaction
                .run(|connection| {
                    Box::pin(connection.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT"))
                })
                .await
        };
        if let Err(error) = started {
            if let Some(connection) = transaction.connection.take() {
                connection.destroy().await;
            }
            return Err(error);
        }
        transaction.active = true;
        Ok(transaction)
    }

    pub(super) async fn run<T>(
        &mut self,
        operation: impl for<'a> FnOnce(&'a mut Conn) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
    ) -> Result<T, StateStoreError> {
        let connection = self.connection.take().ok_or_else(transaction_finished)?;
        self.statement_in_flight = true;
        let result = execute_owned_with_deadline(connection, self.deadline, operation).await;
        self.statement_in_flight = false;
        match result {
            Ok((connection, Ok(value))) => {
                self.connection = Some(connection);
                Ok(value)
            }
            Ok((connection, Err(error))) => {
                self.connection = Some(connection);
                Err(error)
            }
            Err(error) => {
                self.active = false;
                Err(error)
            }
        }
    }

    pub(super) async fn rollback(mut self) -> Result<(), StateStoreError> {
        let result = self.rollback_inner().await;
        self.active = false;
        result
    }

    async fn commit_native(mut self) -> Result<(), StateStoreError> {
        if !self.active {
            return Err(transaction_finished());
        }
        let result = self
            .run(|connection| Box::pin(connection.query_drop("COMMIT")))
            .await;
        self.active = false;
        if result.is_err()
            && let Some(connection) = self.connection.take()
        {
            connection.destroy().await;
        }
        result
    }

    async fn rollback_inner(&mut self) -> Result<(), StateStoreError> {
        if !self.active {
            return Ok(());
        }
        let result = self
            .run(|connection| Box::pin(connection.query_drop("ROLLBACK")))
            .await;
        self.active = false;
        if result.is_err()
            && let Some(connection) = self.connection.take()
        {
            connection.destroy().await;
        }
        result
    }
}

impl Drop for OwnedMysqlTransaction {
    fn drop(&mut self) {
        let Some(mut connection) = self.connection.take() else {
            return;
        };
        let operation = self.operation.take();
        if self.statement_in_flight {
            tokio::spawn(async move {
                connection.destroy().await;
                drop(operation);
            });
            return;
        }
        if !self.active {
            drop(connection);
            drop(operation);
            return;
        }
        tokio::spawn(async move {
            let cleanup = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                connection.query_drop("ROLLBACK"),
            )
            .await;
            if !matches!(cleanup, Ok(Ok(()))) {
                connection.destroy().await;
            }
            drop(operation);
        });
    }
}

impl MysqlReadTransaction {
    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StateStoreError>>) -> ReadCommand,
    ) -> Result<T, StateStoreError> {
        let commands = self.commands.as_ref().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(build(response))
            .await
            .map_err(|_| owner_stopped())?;
        receiver.await.map_err(|_| owner_stopped())?
    }
}

#[async_trait]
impl ReadTransaction for MysqlReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key(key, &self.limits).and_then(|_| Ok(key.clone()));
        let result = match result {
            Ok(key) => {
                self.request(|response| ReadCommand::Get { key, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Get, started, &result);
        if let Ok(Some(record)) = &result {
            self.metrics.record_bytes_read(
                u64::try_from(record.key.as_bytes().len() + record.value.as_bytes().len())
                    .unwrap_or(u64::MAX),
            );
        }
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_range(request, &self.limits).and_then(|_| Ok(request.clone()));
        let result = match result {
            Ok(request) => {
                self.request(|response| ReadCommand::Range { request, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Range, started, &result);
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.records.len() as u64);
        }
        result
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        let commands = self.commands.take().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(ReadCommand::Abort { response })
            .await
            .map_err(|_| owner_stopped())?;
        drop(commands);
        receiver.await.map_err(|_| owner_stopped())?
    }
}

impl MysqlWriteActorState {
    fn transaction(&mut self) -> Result<&mut OwnedMysqlTransaction, StateStoreError> {
        self.transaction.as_mut().ok_or_else(transaction_finished)
    }

    fn take_transaction(&mut self) -> Result<OwnedMysqlTransaction, StateStoreError> {
        self.transaction.take().ok_or_else(transaction_finished)
    }

    async fn load_base_record(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreError> {
        let key_bytes = key.as_bytes().to_vec();
        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = self
            .transaction()?
            .run(move |connection| {
                Box::pin(connection.exec_first(
                    "SELECT key_bytes, value_bytes, version_bytes
                     FROM state_store_kv WHERE key_bytes = ?",
                    (key_bytes,),
                ))
            })
            .await?;
        row.map(|(key, value, version)| {
            decode_record(
                &self.codec,
                key,
                value,
                version,
                self.limits.max_value_bytes,
            )
        })
        .transpose()
    }

    async fn get_inner(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        validate_key(key, &self.limits)?;
        let base = self.load_base_record(key).await?;
        let observation = base.as_ref().map_or(PointObservation::Absent, |record| {
            PointObservation::Present(record.version.clone())
        });
        if let Some(previous) = self.point_observations.get(key) {
            if previous != &observation {
                return Err(persisted_corruption());
            }
        } else {
            self.point_observations.insert(key.clone(), observation);
        }
        Ok(replay_visible_record(
            key,
            base,
            self.mutations
                .iter()
                .filter(|(candidate, _)| candidate == key),
        ))
    }

    async fn range_inner(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        validate_range(request, &self.limits)?;
        self.range_observed = true;
        let resume = request
            .continuation
            .as_ref()
            .map(|token| token.resume_after(request))
            .transpose()?;
        let limit = request
            .page_size
            .checked_add(self.overlay.len())
            .and_then(|value| value.checked_add(1))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(invalid_range)?;
        let start = request.range.start.as_bytes().to_vec();
        let end = request.range.end.as_bytes().to_vec();
        let direction = request.direction;
        let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = match (direction, resume.as_ref()) {
            (Direction::Forward, None) => {
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ?
                             ORDER BY key_bytes ASC LIMIT ?",
                            (start, end, limit),
                        ))
                    })
                    .await?
            }
            (Direction::Forward, Some(resume)) => {
                let resume = resume.as_bytes().to_vec();
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ? AND key_bytes > ?
                             ORDER BY key_bytes ASC LIMIT ?",
                            (start, end, resume, limit),
                        ))
                    })
                    .await?
            }
            (Direction::Reverse, None) => {
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ?
                             ORDER BY key_bytes DESC LIMIT ?",
                            (start, end, limit),
                        ))
                    })
                    .await?
            }
            (Direction::Reverse, Some(resume)) => {
                let resume = resume.as_bytes().to_vec();
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ? AND key_bytes < ?
                             ORDER BY key_bytes DESC LIMIT ?",
                            (start, end, resume, limit),
                        ))
                    })
                    .await?
            }
        };
        let mut visible = rows
            .into_iter()
            .map(|(key, value, version)| {
                let record = decode_record(
                    &self.codec,
                    key,
                    value,
                    version,
                    self.limits.max_value_bytes,
                )?;
                Ok((record.key.clone(), record))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        for (key, mutation) in self
            .overlay
            .range(request.range.start.clone()..request.range.end.clone())
        {
            match mutation {
                Mutation::Put {
                    value,
                    provisional_version,
                    ..
                } => {
                    visible.insert(
                        key.clone(),
                        StateRecord {
                            key: key.clone(),
                            value: value.clone(),
                            version: provisional_version.clone(),
                        },
                    );
                }
                Mutation::Delete { .. } => {
                    visible.remove(key);
                }
            }
        }
        let mut records = visible.into_values().collect::<Vec<_>>();
        if direction == Direction::Reverse {
            records.reverse();
        }
        if let Some(resume) = resume {
            records.retain(|record| match direction {
                Direction::Forward => record.key > resume,
                Direction::Reverse => record.key < resume,
            });
        }
        let has_more = records.len() > request.page_size;
        records.truncate(request.page_size);
        let continuation = if has_more {
            records
                .last()
                .map(|record| request.continuation_after(&record.key))
                .transpose()?
        } else {
            None
        };
        if continuation.is_some() {
            self.range_frozen = true;
        }
        Ok(RangePage {
            records,
            continuation,
        })
    }

    fn put_inner(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<usize, StateStoreError> {
        validate_key_value(&key, Some(&value), &self.limits)?;
        if self.range_frozen {
            return Err(writes_frozen());
        }
        let operation = self
            .budget
            .stage_put(key.as_bytes(), value.as_bytes(), &precondition)?;
        let provisional_version = provisional_version(self.transaction_id, operation);
        let bytes = key.as_bytes().len().saturating_add(value.as_bytes().len());
        let mutation = Mutation::Put {
            value,
            precondition,
            provisional_version,
        };
        self.overlay.insert(key.clone(), mutation.clone());
        self.mutations.push((key, mutation));
        Ok(bytes)
    }

    fn delete_inner(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<usize, StateStoreError> {
        validate_key(&key, &self.limits)?;
        if self.range_frozen {
            return Err(writes_frozen());
        }
        self.budget.stage_delete(key.as_bytes(), &precondition)?;
        let bytes = key.as_bytes().len();
        let mutation = Mutation::Delete { precondition };
        self.overlay.insert(key.clone(), mutation.clone());
        self.mutations.push((key, mutation));
        Ok(bytes)
    }
}

#[async_trait]
impl ReadTransaction for MysqlWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key(key, &self.limits).and_then(|_| Ok(key.clone()));
        let result = match result {
            Ok(key) => {
                self.request(|response| WriteCommand::Get { key, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Get, started, &result);
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_range(request, &self.limits).and_then(|_| Ok(request.clone()));
        let result = match result {
            Ok(request) => {
                self.request(|response| WriteCommand::Range { request, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Range, started, &result);
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.records.len() as u64);
        }
        result
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        let commands = self.commands.take().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(WriteCommand::Abort { response })
            .await
            .map_err(|_| owner_stopped())?;
        drop(commands);
        receiver.await.map_err(|_| owner_stopped())?
    }
}

impl MysqlWriteTransaction {
    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StateStoreError>>) -> WriteCommand,
    ) -> Result<T, StateStoreError> {
        let commands = self.commands.as_ref().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(build(response))
            .await
            .map_err(|_| owner_stopped())?;
        receiver.await.map_err(|_| owner_stopped())?
    }
}

#[async_trait]
impl WriteTransaction for MysqlWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key_value(&key, Some(&value), &self.limits);
        let result = match result {
            Ok(()) => {
                self.request(|response| WriteCommand::Put {
                    key,
                    value,
                    precondition,
                    response,
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Put, started, &result);
        if let Ok(bytes) = result {
            self.metrics
                .record_bytes_written(u64::try_from(bytes).unwrap_or(u64::MAX));
            Ok(())
        } else {
            result.map(|_| ())
        }
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key(&key, &self.limits);
        let result = match result {
            Ok(()) => {
                self.request(|response| WriteCommand::Delete {
                    key,
                    precondition,
                    response,
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Delete, started, &result);
        if let Ok(bytes) = result {
            self.metrics
                .record_bytes_written(u64::try_from(bytes).unwrap_or(u64::MAX));
            Ok(())
        } else {
            result.map(|_| ())
        }
    }

    async fn commit(mut self: Box<Self>) -> CommitOutcome {
        let started = StdInstant::now();
        let metrics = Arc::clone(&self.metrics);
        let outcome = match self.commands.take() {
            Some(commands) => {
                let (response, receiver) = oneshot::channel();
                if commands
                    .send(WriteCommand::Commit { response })
                    .await
                    .is_err()
                {
                    CommitOutcome::DefiniteFailure(owner_stopped())
                } else {
                    drop(commands);
                    receiver
                        .await
                        .unwrap_or_else(|_| CommitOutcome::DefiniteFailure(owner_stopped()))
                }
            }
            None => CommitOutcome::DefiniteFailure(transaction_finished()),
        };
        record_commit(&metrics, started, &outcome);
        outcome
    }
}

impl MysqlWriteActorState {
    async fn commit_inner(&mut self) -> CommitOutcome {
        let result = self.prepare_and_apply_commit().await;
        match result {
            Ok(revision) => {
                let transaction = match self.take_transaction() {
                    Ok(transaction) => transaction,
                    Err(error) => return CommitOutcome::DefiniteFailure(error),
                };
                match transaction.commit_native().await {
                    Ok(()) => CommitOutcome::Committed(CommitReceipt {
                        transaction_id: self.transaction_id,
                        revision,
                    }),
                    Err(error) => CommitOutcome::CommitUnknown(error),
                }
            }
            Err(error) => {
                let rollback = match self.take_transaction() {
                    Ok(transaction) => transaction.rollback().await,
                    Err(rollback_error) => return CommitOutcome::DefiniteFailure(rollback_error),
                };
                if let Err(rollback_error) = rollback {
                    return CommitOutcome::DefiniteFailure(rollback_error);
                }
                classify_prepare_error(error)
            }
        }
    }

    async fn prepare_and_apply_commit(&mut self) -> Result<StoreRevision, StateStoreError> {
        let revision_bytes: Option<Vec<u8>> = self
            .transaction()?
            .run(|connection| {
                Box::pin(connection.exec_first(
                    "SELECT meta_value FROM state_store_meta
                     WHERE meta_key = ? FOR UPDATE",
                    (b"current_revision".to_vec(),),
                ))
            })
            .await?;
        let commit_base_revision = self
            .codec
            .decode_revision(revision_bytes.as_deref().ok_or_else(persisted_corruption)?)?;

        let observations = self
            .point_observations
            .iter()
            .map(|(key, observation)| (key.clone(), observation.clone()))
            .collect::<Vec<_>>();
        for (key, expected) in observations {
            let current = self.load_current_locking(&key).await?;
            let actual = current.as_ref().map_or(PointObservation::Absent, |record| {
                PointObservation::Present(record.version.clone())
            });
            if actual != expected {
                return Err(conflict("MySQL point observation changed before commit"));
            }
        }
        if self.range_observed && commit_base_revision != self.base_revision {
            return Err(conflict(
                "MySQL range observation revision changed before commit",
            ));
        }

        let touched = self
            .mutations
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>();
        let mut state = BTreeMap::new();
        for key in &touched {
            state.insert(key.clone(), self.load_current_locking(key).await?);
        }
        let original = state.clone();
        for (key, mutation) in &self.mutations {
            let current = state.get(key).cloned().flatten();
            if !precondition_matches(mutation.precondition(), current.as_ref()) {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::PreconditionFailed,
                    "MySQL transaction precondition failed",
                ));
            }
            let next = match mutation {
                Mutation::Put {
                    value,
                    provisional_version,
                    ..
                } => Some(StateRecord {
                    key: key.clone(),
                    value: value.clone(),
                    version: provisional_version.clone(),
                }),
                Mutation::Delete { .. } => None,
            };
            state.insert(key.clone(), next);
        }
        let changed = touched
            .into_iter()
            .filter(|key| logical_value(original.get(key)) != logical_value(state.get(key)))
            .collect::<Vec<_>>();
        let next_revision = self.codec.checked_next_revision(commit_base_revision)?;
        for (sequence, key) in changed.iter().enumerate() {
            let sequence = u32::try_from(sequence).map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::LimitExceeded,
                    "MySQL transaction change sequence exceeds the supported range",
                )
            })?;
            match state.get(key).cloned().flatten() {
                Some(record) => {
                    let key_bytes = key.as_bytes().to_vec();
                    let value_bytes = record.value.as_bytes().to_vec();
                    let version = self.codec.encode_version(next_revision, sequence).to_vec();
                    let update_value = value_bytes.clone();
                    let update_version = version.clone();
                    self.transaction()?
                        .run(move |connection| {
                            Box::pin(connection.exec_drop(
                                "INSERT INTO state_store_kv
                                    (key_bytes, value_bytes, version_bytes)
                                 VALUES (?, ?, ?)
                                 ON DUPLICATE KEY UPDATE
                                    value_bytes = ?, version_bytes = ?",
                                (
                                    key_bytes,
                                    value_bytes,
                                    version,
                                    update_value,
                                    update_version,
                                ),
                            ))
                        })
                        .await?;
                }
                None => {
                    let key_bytes = key.as_bytes().to_vec();
                    self.transaction()?
                        .run(move |connection| {
                            Box::pin(connection.exec_drop(
                                "DELETE FROM state_store_kv WHERE key_bytes = ?",
                                (key_bytes,),
                            ))
                        })
                        .await?;
                }
            }
        }
        self.transaction()?
            .run(move |connection| {
                Box::pin(connection.exec_drop(
                    "UPDATE state_store_meta SET meta_value = ?
                     WHERE meta_key = ?",
                    (
                        next_revision.to_be_bytes().to_vec(),
                        b"current_revision".to_vec(),
                    ),
                ))
            })
            .await?;
        StoreRevision::try_from(bytes::Bytes::copy_from_slice(
            &self.codec.encode_revision(next_revision),
        ))
    }

    async fn load_current_locking(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreError> {
        let key_bytes = key.as_bytes().to_vec();
        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = self
            .transaction()?
            .run(move |connection| {
                Box::pin(connection.exec_first(
                    "SELECT key_bytes, value_bytes, version_bytes
                     FROM state_store_kv WHERE key_bytes = ? FOR UPDATE",
                    (key_bytes,),
                ))
            })
            .await?;
        row.map(|(key, value, version)| {
            decode_record(
                &self.codec,
                key,
                value,
                version,
                self.limits.max_value_bytes,
            )
        })
        .transpose()
    }
}

fn replay_visible_record<'a>(
    key: &Key,
    mut state: Option<StateRecord>,
    mutations: impl Iterator<Item = &'a (Key, Mutation)>,
) -> Option<StateRecord> {
    for (_, mutation) in mutations {
        state = match mutation {
            Mutation::Put {
                value,
                provisional_version,
                ..
            } => Some(StateRecord {
                key: key.clone(),
                value: value.clone(),
                version: provisional_version.clone(),
            }),
            Mutation::Delete { .. } => None,
        };
    }
    state
}

fn precondition_matches(precondition: &Precondition, current: Option<&StateRecord>) -> bool {
    match precondition {
        Precondition::Any => true,
        Precondition::Absent => current.is_none(),
        Precondition::Present => current.is_some(),
        Precondition::Version(expected) => {
            current.is_some_and(|record| &record.version == expected)
        }
    }
}

fn logical_value(record: Option<&Option<StateRecord>>) -> Option<&[u8]> {
    record
        .and_then(Option::as_ref)
        .map(|record| record.value.as_bytes())
}

fn provisional_version(transaction_id: TransactionId, operation: u64) -> VersionToken {
    VersionToken::try_from(bytes::Bytes::from(
        [
            PROVISIONAL_VERSION_TAG,
            transaction_id.as_uuid().as_bytes(),
            &operation.to_be_bytes(),
        ]
        .concat(),
    ))
    .expect("provisional version is non-empty")
}

fn validate_key(key: &Key, limits: &StateStoreLimits) -> Result<(), StateStoreError> {
    if key.as_bytes().len() > limits.max_key_bytes {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            "key exceeds the configured byte limit",
        ));
    }
    Ok(())
}

fn validate_key_value(
    key: &Key,
    value: Option<&Value>,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    validate_key(key, limits)?;
    if value.is_some_and(|value| value.as_bytes().len() > limits.max_value_bytes) {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            "value exceeds the configured byte limit",
        ));
    }
    Ok(())
}

fn validate_range(
    request: &RangeRequest,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    request.validate(limits)?;
    validate_key(&request.range.start, limits)?;
    validate_key(&request.range.end, limits)?;
    if let Some(continuation) = &request.continuation {
        validate_key(&continuation.resume_after(request)?, limits)?;
    }
    Ok(())
}

fn record_result<T>(
    metrics: &StateStoreMetrics,
    operation: StateStoreOperation,
    started: StdInstant,
    result: &Result<T, StateStoreError>,
) {
    metrics.record_operation(
        operation,
        if result.is_ok() {
            StateStoreOutcome::Success
        } else {
            StateStoreOutcome::Error
        },
        started.elapsed(),
    );
}

fn record_commit(metrics: &StateStoreMetrics, started: StdInstant, outcome: &CommitOutcome) {
    let metric = match outcome {
        CommitOutcome::Committed(_) => StateStoreOutcome::Success,
        CommitOutcome::Conflict(_) => StateStoreOutcome::Conflict,
        CommitOutcome::TransientBeforeCommit(_) => StateStoreOutcome::TransientBeforeCommit,
        CommitOutcome::DefiniteFailure(_) => StateStoreOutcome::DefiniteFailure,
        CommitOutcome::CommitUnknown(_) => StateStoreOutcome::CommitUnknown,
    };
    metrics.record_operation(StateStoreOperation::Commit, metric, started.elapsed());
}

fn classify_prepare_error(error: StateStoreError) -> CommitOutcome {
    match error.kind() {
        StateStoreErrorKind::Conflict | StateStoreErrorKind::PreconditionFailed => {
            CommitOutcome::Conflict(error)
        }
        StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable => {
            CommitOutcome::TransientBeforeCommit(error)
        }
        _ => CommitOutcome::DefiniteFailure(error),
    }
}

const fn conflict(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Conflict, message)
}

const fn transaction_finished() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "MySQL state transaction is already finished",
    )
}

const fn owner_stopped() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL state transaction owner stopped unexpectedly",
    )
}

const fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL state transaction deadline exceeded",
    )
}

const fn persisted_corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL state store persisted state is malformed",
    )
}

const fn invalid_range() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "MySQL range request is invalid",
    )
}

const fn writes_frozen() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "writes are frozen after paginated range reads",
    )
}

pub(crate) fn explicit_rollback_count_for_test() -> u64 {
    EXPLICIT_ROLLBACKS.load(Ordering::Relaxed)
}

pub(crate) async fn insert_malformed_kv_row_for_test(
    pool: Arc<dyn PoolLifecycle>,
    key: &[u8],
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut transaction = OwnedMysqlTransaction {
        connection: Some(connection),
        operation: None,
        deadline,
        active: false,
        statement_in_flight: false,
    };
    let key = key.to_vec();
    let oversized_value = vec![0x5a; crate::state_store::limits::MAX_VALUE_BYTES + 1];
    transaction
        .run(move |connection| {
            Box::pin(connection.exec_drop(
                "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                 VALUES (?, ?, ?)",
                (key, oversized_value, vec![0; 12]),
            ))
        })
        .await
}

pub(in crate::state_store) async fn deadlock_1213_maps_to_conflict_for_test(
    pool: Arc<dyn PoolLifecycle>,
    first_operation: MysqlRuntimeGuard,
    second_operation: MysqlRuntimeGuard,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut first =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), first_operation, deadline, false).await?;
    let mut second = OwnedMysqlTransaction::begin(pool, second_operation, deadline, false).await?;
    let first_key = b"task5-deadlock-a".to_vec();
    let second_key = b"task5-deadlock-b".to_vec();
    first
        .run({
            let first_key = first_key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (first_key, vec![1], vec![0; 12]),
                ))
            }
        })
        .await?;
    second
        .run({
            let second_key = second_key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (second_key, vec![2], vec![0; 12]),
                ))
            }
        })
        .await?;

    let first_wait = first.run({
        let second_key = second_key.clone();
        move |connection| {
            Box::pin(connection.exec_drop(
                "UPDATE state_store_kv SET value_bytes = ? WHERE key_bytes = ?",
                (vec![3], second_key),
            ))
        }
    });
    let second_wait = second.run({
        let first_key = first_key.clone();
        move |connection| {
            Box::pin(connection.exec_drop(
                "UPDATE state_store_kv SET value_bytes = ? WHERE key_bytes = ?",
                (vec![4], first_key),
            ))
        }
    });
    let (first_result, second_result) = tokio::join!(first_wait, second_wait);
    let conflicts = [&first_result, &second_result]
        .into_iter()
        .filter(|result| {
            result
                .as_ref()
                .err()
                .is_some_and(|error| error.kind() == StateStoreErrorKind::Conflict)
        })
        .count();
    let successes = [&first_result, &second_result]
        .into_iter()
        .filter(|result| result.is_ok())
        .count();
    let first_rollback = first.rollback().await;
    let second_rollback = second.rollback().await;
    if conflicts != 1 || successes != 1 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "MySQL deadlock probe did not produce one 1213 conflict and one survivor",
        ));
    }
    first_rollback?;
    second_rollback?;
    Ok(())
}

pub(in crate::state_store) async fn lock_timeout_1205_rolls_back_before_conflict_for_test(
    pool: Arc<dyn PoolLifecycle>,
    first_operation: MysqlRuntimeGuard,
    second_operation: MysqlRuntimeGuard,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut holder =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), first_operation, deadline, false).await?;
    let mut waiter =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), second_operation, deadline, false).await?;
    waiter
        .run(|connection| {
            Box::pin(connection.query_drop("SET SESSION innodb_lock_wait_timeout = 1"))
        })
        .await?;
    let key = b"task5-lock-timeout".to_vec();
    holder
        .run({
            let key = key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (key, vec![1], vec![0; 12]),
                ))
            }
        })
        .await?;
    let error = waiter
        .run({
            let key = key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (key, vec![2], vec![0; 12]),
                ))
            }
        })
        .await
        .expect_err("lock waiter must time out");
    if error.kind() != StateStoreErrorKind::Conflict {
        return Err(error);
    }
    waiter.rollback().await?;
    holder.rollback().await?;

    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let (connection, persisted) = execute_owned_with_deadline(connection, deadline, {
        let key = key.clone();
        move |connection| {
            Box::pin(
                connection.exec_first("SELECT 1 FROM state_store_kv WHERE key_bytes = ?", (key,)),
            )
        }
    })
    .await?;
    let persisted: Option<u8> = persisted?;
    drop(connection);
    if persisted.is_some() {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "timed out MySQL transaction left a durable row",
        ));
    }
    Ok(())
}
