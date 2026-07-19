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

use std::fmt;

use super::{StateStoreError, StateStoreErrorKind};

#[cfg(all(
    feature = "mysql-state-store-provider",
    feature = "state-store-test-hooks"
))]
use super::mysql::client::delayed_active_readiness as mysql_delayed_active_readiness;
#[cfg(feature = "mysql-state-store-provider")]
use {
    super::limits::MYSQL_MAX_KEY_BYTES,
    super::mysql::client::{
        PoolLifecycle, ResolvedMysqlClient, active_readiness as mysql_active_readiness,
        checkout_hygienic_connection, pollute_session as mysql_pollute_session,
    },
    super::mysql::test_support::{
        MysqlHeldAdvisoryLock, MysqlHeldConnection, MysqlReadinessSnapshot, MysqlRuntimeOwner,
        MysqlSchemaMutation, MysqlSchemaSnapshot, MysqlStoreReadinessSnapshot, MysqlTestHandle,
    },
    super::mysql::{MysqlOpenCancellation, MysqlStateStore},
    super::{FeDeploymentView, MySqlClientConfig, StateStore, StateStoreConfig, StateStoreLimits},
    std::collections::HashMap,
    std::sync::Arc,
    std::sync::Mutex,
    std::sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    std::time::Duration,
    tokio::sync::{Notify, oneshot},
    tokio::time::{Instant, timeout_at},
};

#[cfg(feature = "foundationdb-provider")]
use {
    super::foundationdb::FoundationDbStateStore,
    super::{FoundationDbClientConfig, StateStore, StateStoreConfig, StateStoreLimits},
    foundationdb::Database,
    foundationdb::api::{FdbApiBuilder, NetworkRunner, NetworkStop},
    foundationdb::options::NetworkOption,
    std::collections::HashMap,
    std::panic::{AssertUnwindSafe, catch_unwind},
    std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    std::sync::{Arc, Mutex},
    std::thread::JoinHandle,
    std::time::Duration,
    tokio::sync::Notify,
    tokio::time::{Instant, timeout_at},
};

const RUNTIME_PID_ERROR: &str = "state store runtime belongs to a different process";
#[cfg(feature = "foundationdb-provider")]
const FOUNDATIONDB_API_VERSION: i32 = 730;

#[cfg(feature = "foundationdb-provider")]
struct ShutdownDeferredLogFields {
    lifecycle: &'static str,
    reason: &'static str,
}

#[cfg(feature = "foundationdb-provider")]
const fn shutdown_deferred_log_fields() -> ShutdownDeferredLogFields {
    ShutdownDeferredLogFields {
        lifecycle: "shutdown_deferred",
        reason: "handles_not_drained",
    }
}

pub struct StateStoreRuntime {
    inner: RuntimeInner,
}

enum RuntimeInner {
    Local(LocalRuntime),
    #[cfg(feature = "foundationdb-provider")]
    FoundationDb(FoundationDbRuntime),
    #[cfg(feature = "mysql-state-store-provider")]
    Mysql(MysqlRuntime),
}

struct LocalRuntime {
    pid: u32,
    accepting: bool,
}

impl StateStoreRuntime {
    pub fn local() -> Result<Self, StateStoreError> {
        Ok(Self {
            inner: RuntimeInner::Local(LocalRuntime {
                pid: std::process::id(),
                accepting: true,
            }),
        })
    }

    #[cfg(feature = "foundationdb-provider")]
    pub fn foundationdb(config: FoundationDbClientConfig) -> Result<Self, StateStoreError> {
        FoundationDbRuntime::boot(config).map(|runtime| Self {
            inner: RuntimeInner::FoundationDb(runtime),
        })
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub fn mysql(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        MysqlRuntime::boot(config).map(|runtime| Self {
            inner: RuntimeInner::Mysql(runtime),
        })
    }

    pub async fn shutdown(&mut self) -> Result<(), StateStoreError> {
        match &mut self.inner {
            RuntimeInner::Local(runtime) => runtime.shutdown(),
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(runtime) => runtime.shutdown().await,
            #[cfg(feature = "mysql-state-store-provider")]
            RuntimeInner::Mysql(runtime) => runtime.shutdown(Duration::from_secs(5)).await,
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub async fn shutdown_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), StateStoreError> {
        match &mut self.inner {
            RuntimeInner::Mysql(runtime) => runtime.shutdown(timeout).await,
            _ => self.shutdown().await,
        }
    }

    pub(crate) fn accepts_local(&self) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Local(runtime) => runtime.validate(),
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(_) => Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store requires a local runtime",
            )),
            #[cfg(feature = "mysql-state-store-provider")]
            RuntimeInner::Mysql(_) => Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "SQLite state store requires a local runtime",
            )),
        }
    }

    #[cfg(feature = "foundationdb-provider")]
    pub(crate) async fn open_foundationdb_store(
        &self,
        config: &StateStoreConfig,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        match &self.inner {
            RuntimeInner::Local(_) => Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store requires a FoundationDB runtime",
            )),
            RuntimeInner::FoundationDb(runtime) => runtime.open_store(config).await,
            #[cfg(feature = "mysql-state-store-provider")]
            RuntimeInner::Mysql(_) => Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store requires a FoundationDB runtime",
            )),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn open_mysql_store(
        &self,
        config: &StateStoreConfig,
        deployment: FeDeploymentView,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.open_store(config, deployment).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_owner(&self) -> Result<MysqlRuntimeOwner, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => Ok(runtime.shared.owner),
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_validate_owner(&self, pid: u32) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.validate_pid_owner(pid),
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_prepare_pool(
        &self,
        database: &str,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.prepare_pool(database).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(all(
        feature = "mysql-state-store-provider",
        feature = "state-store-test-hooks"
    ))]
    pub(crate) fn mysql_test_pool(
        &self,
        database: &str,
    ) -> Result<Arc<dyn PoolLifecycle>, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.get_or_create_pool(database),
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_pool_count(&self) -> Result<usize, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.pool_count(),
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_acquire_provider_handle(
        &self,
    ) -> Result<MysqlTestHandle, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                let guard = runtime.acquire_provider_handle()?;
                Ok(MysqlTestHandle::new(move || drop(guard)))
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_acquire_operation(&self) -> Result<MysqlTestHandle, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                let guard = runtime.acquire_operation()?;
                Ok(MysqlTestHandle::new(move || drop(guard)))
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_is_accepting(&self) -> Result<bool, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => Ok(runtime.shared.accepting.load(Ordering::Acquire)),
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) fn mysql_test_begin_shutdown(&self) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.begin_shutdown(),
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_active_readiness(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.active_readiness(database, deadline).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(all(
        feature = "mysql-state-store-provider",
        feature = "state-store-test-hooks"
    ))]
    pub(crate) async fn mysql_test_delayed_active_readiness(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime.delayed_active_readiness(database, deadline).await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_pollute_session(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.pollute_session(database, deadline).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_hold_connection(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlHeldConnection, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.hold_connection(database, deadline).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_schema_snapshot(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlSchemaSnapshot, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.schema_snapshot(database, deadline).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_apply_schema_mutation(
        &self,
        database: &str,
        mutation: MysqlSchemaMutation,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .apply_schema_mutation(database, mutation, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_acquire_schema_advisory_lock(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlHeldAdvisoryLock, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .acquire_schema_advisory_lock(database, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_is_schema_advisory_lock_free(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .is_schema_advisory_lock_free(database, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_store_readiness_snapshot(
        &self,
        database: &str,
        cluster_id: &str,
        deadline: Duration,
    ) -> Result<MysqlStoreReadinessSnapshot, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .store_readiness_snapshot(database, cluster_id, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_schema_timeout_connection_is_destroyed(
        &self,
        database: &str,
        timeout_deadline: Duration,
        checkout_deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .schema_timeout_connection_is_destroyed(
                        database,
                        timeout_deadline,
                        checkout_deadline,
                    )
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_insert_malformed_kv_row(
        &self,
        database: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .insert_malformed_kv_row(database, key, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_deadlock_1213_maps_to_conflict(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .deadlock_1213_maps_to_conflict(database, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(crate) async fn mysql_test_lock_timeout_1205_rolls_back_before_conflict(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .lock_timeout_1205_rolls_back_before_conflict(database, deadline)
                    .await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub(in crate::state_store) async fn mysql_test_hold_kv_lock(
        &self,
        database: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<super::mysql::txn::MysqlHeldKvLock, StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => runtime.hold_kv_lock(database, key, deadline).await,
            _ => Err(mysql_runtime_mismatch()),
        }
    }

    #[cfg(all(
        feature = "mysql-state-store-provider",
        feature = "state-store-test-hooks"
    ))]
    pub(crate) async fn mysql_test_run_sleep_until_deadline(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        match &self.inner {
            RuntimeInner::Mysql(runtime) => {
                runtime.run_sleep_until_deadline(database, deadline).await
            }
            _ => Err(mysql_runtime_mismatch()),
        }
    }
}

impl fmt::Debug for StateStoreRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            RuntimeInner::Local(_) => formatter.write_str("StateStoreRuntime::Local"),
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(_) => formatter.write_str("StateStoreRuntime::FoundationDb"),
            #[cfg(feature = "mysql-state-store-provider")]
            RuntimeInner::Mysql(_) => formatter.write_str("StateStoreRuntime::Mysql"),
        }
    }
}

#[cfg(feature = "mysql-state-store-provider")]
struct MysqlRuntime {
    shared: Arc<MysqlRuntimeShared>,
    lifecycle: Mutex<MysqlRuntimeLifecycle>,
}

#[cfg(feature = "mysql-state-store-provider")]
struct MysqlOpenWaiterGuard {
    cancellation: MysqlOpenCancellation,
    armed: bool,
}

#[cfg(feature = "mysql-state-store-provider")]
struct MysqlRuntimeShared {
    owner: MysqlRuntimeOwner,
    accepting: AtomicBool,
    in_flight: AtomicUsize,
    provider_handles: AtomicUsize,
    client: ResolvedMysqlClient,
    pools_by_database: Mutex<HashMap<String, Arc<dyn PoolLifecycle>>>,
    drained: Notify,
}

#[cfg(feature = "mysql-state-store-provider")]
enum MysqlRuntimeLifecycle {
    Running,
    Failed(StateStoreError),
    Stopped,
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlRuntime {
    fn boot(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store runtime requires an active Tokio runtime",
            )
        })?;
        let owner = MysqlRuntimeOwner {
            pid: std::process::id(),
            tokio_runtime_id: handle.id(),
        };
        Ok(Self {
            shared: Arc::new(MysqlRuntimeShared {
                owner,
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                provider_handles: AtomicUsize::new(0),
                client: ResolvedMysqlClient::resolve(config)?,
                pools_by_database: Mutex::new(HashMap::new()),
                drained: Notify::new(),
            }),
            lifecycle: Mutex::new(MysqlRuntimeLifecycle::Running),
        })
    }

    fn validate_pid_owner(&self, pid: u32) -> Result<(), StateStoreError> {
        if self.shared.owner.pid != pid {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store runtime owner does not match",
            ));
        }
        Ok(())
    }

    fn validate_process_and_context(&self) -> Result<(), StateStoreError> {
        if self.shared.owner.pid != std::process::id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                RUNTIME_PID_ERROR,
            ));
        }
        let current = tokio::runtime::Handle::try_current().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store operation requires an active Tokio runtime",
            )
        })?;
        if self.shared.owner.tokio_runtime_id != current.id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store runtime belongs to a different Tokio runtime",
            ));
        }
        Ok(())
    }

    async fn open_store(
        &self,
        config: &StateStoreConfig,
        _deployment: FeDeploymentView,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        self.validate_process_and_context()?;
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store configuration is invalid",
            )
        })?;
        let database = match &config.provider {
            super::StateStoreProviderConfig::Mysql { database } => database.clone(),
            _ => return Err(mysql_runtime_mismatch()),
        };
        let limits =
            StateStoreLimits::from_overrides_with_max_key(&config.limits, MYSQL_MAX_KEY_BYTES)
                .map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::InvalidConfiguration,
                        "MySQL state store limits are invalid",
                    )
                })?;
        let opening = self.acquire_operation()?;
        let pool = self.get_or_create_pool(&database)?;
        let deadline = Instant::now() + limits.transaction_deadline;
        let cancellation = MysqlOpenCancellation::new();
        let waiter = MysqlOpenWaiterGuard::new(cancellation.clone());
        let shared = Arc::clone(&self.shared);
        let cluster_id = config.cluster_id.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = Self::open_store_owned(
                shared,
                pool,
                database,
                cluster_id,
                limits,
                deadline,
                cancellation,
                opening,
            )
            .await;
            let _ = sender.send(result);
        });
        let result = receiver.await.map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store open task stopped unexpectedly",
            )
        });
        waiter.complete();
        result?
    }

    async fn open_store_owned(
        shared: Arc<MysqlRuntimeShared>,
        pool: Arc<dyn PoolLifecycle>,
        database: String,
        cluster_id: String,
        limits: StateStoreLimits,
        deadline: Instant,
        cancellation: MysqlOpenCancellation,
        _opening: MysqlRuntimeGuard,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        cancellation.check()?;
        mysql_active_readiness(Arc::clone(&pool), deadline).await?;
        cancellation.check()?;
        let lease = MysqlProviderHandle::new(shared, pool)?;
        let store =
            MysqlStateStore::open(lease, database, cluster_id, limits, deadline, cancellation)
                .await?;
        Ok(Arc::new(store))
    }

    async fn prepare_pool(&self, database: &str) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        self.get_or_create_pool(database)?;
        Ok(())
    }

    fn get_or_create_pool(
        &self,
        database: &str,
    ) -> Result<Arc<dyn PoolLifecycle>, StateStoreError> {
        let mut pools = self.shared.pools_by_database.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL pool registry is poisoned",
            )
        })?;
        if !pools.contains_key(database) {
            let pool = self.shared.client.build_pool(database)?;
            pools.insert(database.to_owned(), pool);
        }
        Ok(Arc::clone(
            pools
                .get(database)
                .expect("pool exists immediately after insertion"),
        ))
    }

    fn pool_count(&self) -> Result<usize, StateStoreError> {
        self.shared
            .pools_by_database
            .lock()
            .map(|pools| pools.len())
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL pool registry is poisoned",
                )
            })
    }

    fn acquire_operation(&self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        self.validate_process_and_context()?;
        MysqlRuntimeGuard::acquire(Arc::clone(&self.shared), MysqlGuardKind::Operation)
    }

    fn acquire_provider_handle(&self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        self.validate_process_and_context()?;
        MysqlRuntimeGuard::acquire(Arc::clone(&self.shared), MysqlGuardKind::Provider)
    }

    fn begin_shutdown(&self) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        self.shared.accepting.store(false, Ordering::Release);
        Ok(())
    }

    async fn active_readiness(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        let snapshot = mysql_active_readiness(pool, deadline).await?;
        Ok(MysqlReadinessSnapshot {
            server_version: snapshot.server_version,
            innodb_page_size: snapshot.innodb_page_size,
            innodb_available: snapshot.innodb_available,
            default_storage_engine: snapshot.default_storage_engine,
            sql_mode: snapshot.sql_mode,
            time_zone: snapshot.time_zone,
            character_set: snapshot.character_set,
            connection_id: snapshot.connection_id,
        })
    }

    async fn pollute_session(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        mysql_pollute_session(pool, deadline).await
    }

    #[cfg(feature = "state-store-test-hooks")]
    async fn delayed_active_readiness(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        let snapshot = mysql_delayed_active_readiness(pool, deadline).await?;
        Ok(MysqlReadinessSnapshot {
            server_version: snapshot.server_version,
            innodb_page_size: snapshot.innodb_page_size,
            innodb_available: snapshot.innodb_available,
            default_storage_engine: snapshot.default_storage_engine,
            sql_mode: snapshot.sql_mode,
            time_zone: snapshot.time_zone,
            character_set: snapshot.character_set,
            connection_id: snapshot.connection_id,
        })
    }

    async fn hold_connection(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlHeldConnection, StateStoreError> {
        self.validate_process_and_context()?;
        let operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        let connection = checkout_hygienic_connection(pool, deadline).await?;
        Ok(MysqlHeldConnection::new(
            connection,
            MysqlTestHandle::new(move || drop(operation)),
        ))
    }

    async fn schema_snapshot(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlSchemaSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::schema::snapshot_for_test(pool, Instant::now() + total_deadline).await
    }

    async fn apply_schema_mutation(
        &self,
        database: &str,
        mutation: MysqlSchemaMutation,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::schema::apply_mutation_for_test(
            pool,
            mutation,
            Instant::now() + total_deadline,
        )
        .await
    }

    async fn acquire_schema_advisory_lock(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlHeldAdvisoryLock, StateStoreError> {
        self.validate_process_and_context()?;
        let operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let (connection, lock_name) = super::mysql::schema::acquire_lock_for_test(
            pool,
            database,
            Instant::now() + total_deadline,
        )
        .await?;
        Ok(MysqlHeldAdvisoryLock::new(
            connection,
            MysqlTestHandle::new(move || drop(operation)),
            lock_name,
        ))
    }

    async fn is_schema_advisory_lock_free(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::schema::is_lock_free_for_test(pool, database, Instant::now() + total_deadline)
            .await
    }

    async fn store_readiness_snapshot(
        &self,
        database: &str,
        cluster_id: &str,
        total_deadline: Duration,
    ) -> Result<MysqlStoreReadinessSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let (_, readiness) = super::mysql::schema::validate_store_readiness(
            pool,
            database,
            cluster_id,
            MYSQL_MAX_KEY_BYTES,
            Instant::now() + total_deadline,
            &MysqlOpenCancellation::new(),
        )
        .await?;
        Ok(readiness)
    }

    async fn schema_timeout_connection_is_destroyed(
        &self,
        database: &str,
        timeout_deadline: Duration,
        checkout_deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::schema::timeout_connection_is_destroyed_for_test(
            pool,
            Instant::now() + timeout_deadline,
            Instant::now() + checkout_deadline,
        )
        .await
    }

    async fn insert_malformed_kv_row(
        &self,
        database: &str,
        key: &[u8],
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::txn::insert_malformed_kv_row_for_test(
            pool,
            key,
            Instant::now() + total_deadline,
        )
        .await
    }

    async fn deadlock_1213_maps_to_conflict(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let first_operation = self.acquire_operation()?;
        let second_operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::txn::deadlock_1213_maps_to_conflict_for_test(
            pool,
            first_operation,
            second_operation,
            Instant::now() + total_deadline,
        )
        .await
    }

    async fn lock_timeout_1205_rolls_back_before_conflict(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let first_operation = self.acquire_operation()?;
        let second_operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::txn::lock_timeout_1205_rolls_back_before_conflict_for_test(
            pool,
            first_operation,
            second_operation,
            Instant::now() + total_deadline,
        )
        .await
    }

    async fn hold_kv_lock(
        &self,
        database: &str,
        key: &[u8],
        total_deadline: Duration,
    ) -> Result<super::mysql::txn::MysqlHeldKvLock, StateStoreError> {
        self.validate_process_and_context()?;
        let operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::mysql::txn::hold_kv_lock_for_test(
            pool,
            operation,
            key,
            Instant::now() + total_deadline,
        )
        .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    async fn run_sleep_until_deadline(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        super::mysql::client::run_sleep_until_deadline(pool, deadline).await
    }

    async fn shutdown(&mut self, timeout: Duration) -> Result<(), StateStoreError> {
        self.shutdown_with_drain_hook(timeout, || {}).await
    }

    #[cfg(test)]
    async fn shutdown_with_drain_registration_hook(
        &mut self,
        timeout: Duration,
        hook: impl FnMut(),
    ) -> Result<(), StateStoreError> {
        self.shutdown_with_drain_hook(timeout, hook).await
    }

    async fn shutdown_with_drain_hook(
        &mut self,
        timeout: Duration,
        mut after_registration: impl FnMut(),
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        {
            let lifecycle = self.lifecycle.lock().map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL runtime lifecycle is poisoned",
                )
            })?;
            match &*lifecycle {
                MysqlRuntimeLifecycle::Failed(error) => return Err(error.clone()),
                MysqlRuntimeLifecycle::Stopped => return Ok(()),
                MysqlRuntimeLifecycle::Running => {}
            }
        }
        self.shared.accepting.store(false, Ordering::Release);
        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.shared.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            after_registration();
            if self.shared.is_drained() {
                break;
            }
            if timeout_at(deadline, notified).await.is_err() {
                self.shared.accepting.store(true, Ordering::Release);
                return Err(StateStoreError::new(
                    StateStoreErrorKind::DeadlineExceeded,
                    "MySQL runtime handles did not drain before shutdown deadline",
                ));
            }
        }

        let pools = self
            .shared
            .pools_by_database
            .lock()
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL pool registry is poisoned",
                )
            })?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for pool in pools {
            if let Err(native) = pool.disconnect().await {
                let error = native.into_public();
                let mut lifecycle = self.lifecycle.lock().map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::Internal,
                        "MySQL runtime lifecycle is poisoned",
                    )
                })?;
                *lifecycle = MysqlRuntimeLifecycle::Failed(error.clone());
                return Err(error);
            }
        }
        self.shared
            .pools_by_database
            .lock()
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL pool registry is poisoned",
                )
            })?
            .clear();
        let mut lifecycle = self.lifecycle.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL runtime lifecycle is poisoned",
            )
        })?;
        *lifecycle = MysqlRuntimeLifecycle::Stopped;
        Ok(())
    }
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlOpenWaiterGuard {
    fn new(cancellation: MysqlOpenCancellation) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    fn complete(mut self) {
        self.armed = false;
    }
}

#[cfg(feature = "mysql-state-store-provider")]
impl Drop for MysqlOpenWaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

#[cfg(feature = "mysql-state-store-provider")]
enum MysqlGuardKind {
    Operation,
    Provider,
}

#[cfg(feature = "mysql-state-store-provider")]
pub(super) struct MysqlProviderHandle {
    _provider: MysqlRuntimeGuard,
    pool: Arc<dyn PoolLifecycle>,
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlProviderHandle {
    fn new(
        shared: Arc<MysqlRuntimeShared>,
        pool: Arc<dyn PoolLifecycle>,
    ) -> Result<Self, StateStoreError> {
        Ok(Self {
            _provider: MysqlRuntimeGuard::acquire(shared, MysqlGuardKind::Provider)?,
            pool,
        })
    }

    pub(super) fn pool(&self) -> Arc<dyn PoolLifecycle> {
        Arc::clone(&self.pool)
    }

    pub(super) fn acquire_operation(&self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        MysqlRuntimeGuard::acquire(
            Arc::clone(&self._provider.shared),
            MysqlGuardKind::Operation,
        )
    }
}

#[cfg(feature = "mysql-state-store-provider")]
pub(super) struct MysqlRuntimeGuard {
    shared: Arc<MysqlRuntimeShared>,
    kind: MysqlGuardKind,
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlRuntimeGuard {
    fn acquire(
        shared: Arc<MysqlRuntimeShared>,
        kind: MysqlGuardKind,
    ) -> Result<Self, StateStoreError> {
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store runtime is stopping",
            ));
        }
        let counter = match kind {
            MysqlGuardKind::Operation => &shared.in_flight,
            MysqlGuardKind::Provider => &shared.provider_handles,
        };
        counter.fetch_add(1, Ordering::AcqRel);
        if !shared.accepting.load(Ordering::Acquire) {
            counter.fetch_sub(1, Ordering::AcqRel);
            shared.notify_if_drained();
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store runtime is stopping",
            ));
        }
        Ok(Self { shared, kind })
    }
}

#[cfg(feature = "mysql-state-store-provider")]
impl Drop for MysqlRuntimeGuard {
    fn drop(&mut self) {
        let counter = match self.kind {
            MysqlGuardKind::Operation => &self.shared.in_flight,
            MysqlGuardKind::Provider => &self.shared.provider_handles,
        };
        counter.fetch_sub(1, Ordering::AcqRel);
        self.shared.notify_if_drained();
    }
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlRuntimeShared {
    fn is_drained(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) == 0
            && self.provider_handles.load(Ordering::Acquire) == 0
    }

    fn notify_if_drained(&self) {
        if self.is_drained() {
            self.drained.notify_waiters();
        }
    }
}

#[cfg(all(test, feature = "mysql-state-store-provider"))]
fn test_mysql_runtime_with_pool(pool: Arc<dyn PoolLifecycle>) -> MysqlRuntime {
    const PASSWORD_ENV: &str = "NOVAROCKS_SS3_DISCONNECT_TEST_PASSWORD";
    unsafe {
        std::env::set_var(PASSWORD_ENV, "test-secret");
    }
    let client = ResolvedMysqlClient::resolve(MySqlClientConfig {
        host: "localhost".to_owned(),
        port: 3306,
        username: "runtime-test".to_owned(),
        password_env: PASSWORD_ENV.to_owned(),
        tls_mode: super::MySqlTlsMode::Disabled,
        tls_ca_path: None,
        tls_cert_path: None,
        tls_key_path: None,
        connect_timeout_ms: 100,
        pool_min: 1,
        pool_max: 1,
        inactive_connection_ttl_ms: 1_000,
    })
    .expect("test MySQL client");
    let mut pools = HashMap::new();
    pools.insert("test".to_owned(), pool);
    MysqlRuntime {
        shared: Arc::new(MysqlRuntimeShared {
            owner: MysqlRuntimeOwner {
                pid: std::process::id(),
                tokio_runtime_id: tokio::runtime::Handle::current().id(),
            },
            accepting: AtomicBool::new(true),
            in_flight: AtomicUsize::new(0),
            provider_handles: AtomicUsize::new(0),
            client,
            pools_by_database: Mutex::new(pools),
            drained: Notify::new(),
        }),
        lifecycle: Mutex::new(MysqlRuntimeLifecycle::Running),
    }
}

#[cfg(feature = "mysql-state-store-provider")]
fn mysql_runtime_mismatch() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidConfiguration,
        "operation requires a MySQL state store runtime",
    )
}

#[cfg(all(test, feature = "mysql-state-store-provider"))]
mod mysql_tests {
    use super::*;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailingPool {
        disconnects: Arc<AtomicUsize>,
    }

    impl PoolLifecycle for FailingPool {
        fn get_conn<'a>(
            &'a self,
            _deadline: Instant,
        ) -> BoxFuture<
            'a,
            Result<
                super::super::mysql::client::MysqlPoolConnection,
                super::super::mysql::error::MysqlNativeError,
            >,
        > {
            Box::pin(async {
                Err(super::super::mysql::error::MysqlNativeError::provider_unavailable())
            })
        }

        fn disconnect(
            self: Arc<Self>,
        ) -> BoxFuture<'static, Result<(), super::super::mysql::error::MysqlNativeError>> {
            Box::pin(async move {
                self.disconnects.fetch_add(1, Ordering::AcqRel);
                Err(super::super::mysql::error::MysqlNativeError::provider_unavailable())
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mysql_disconnect_failure_is_stable_and_not_stopped() {
        let disconnects = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailingPool {
            disconnects: Arc::clone(&disconnects),
        });
        let mut runtime = test_mysql_runtime_with_pool(pool);

        let first = runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect_err("disconnect failure must surface");
        let repeated = runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect_err("disconnect failure must remain stable");

        assert_eq!(first, repeated);
        assert_eq!(disconnects.load(Ordering::Acquire), 1);
        assert_eq!(runtime.pool_count().expect("pool ownership"), 1);
        assert!(matches!(
            *runtime.lifecycle.lock().expect("lifecycle"),
            MysqlRuntimeLifecycle::Failed(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mysql_shutdown_does_not_lose_final_guard_notification() {
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailingPool {
            disconnects: Arc::new(AtomicUsize::new(0)),
        });
        let mut runtime = test_mysql_runtime_with_pool(pool);
        let guard = runtime
            .acquire_provider_handle()
            .expect("acquire final provider handle");
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let shutdown = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                let result = runtime
                    .shutdown_with_drain_registration_hook(Duration::from_millis(250), move || {
                        entered.wait();
                        release.wait();
                    })
                    .await;
                (runtime, result)
            })
        };
        entered.wait();
        drop(guard);
        release.wait();

        let (runtime, result) = shutdown.await.expect("join shutdown");
        let error = result.expect_err("the injected pool disconnect still fails");
        assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);
        assert!(
            !runtime.shared.accepting.load(Ordering::Acquire),
            "a completed drain must not reopen the runtime"
        );
    }
}

impl LocalRuntime {
    fn validate(&self) -> Result<(), StateStoreError> {
        if self.pid != std::process::id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                RUNTIME_PID_ERROR,
            ));
        }
        if !self.accepting {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "state store runtime is stopped",
            ));
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), StateStoreError> {
        if self.pid != std::process::id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                RUNTIME_PID_ERROR,
            ));
        }
        self.accepting = false;
        Ok(())
    }
}

#[cfg(feature = "foundationdb-provider")]
#[derive(Clone)]
enum ProcessNetworkState {
    Never,
    Starting {
        pid: u32,
        config: FoundationDbClientConfig,
    },
    Running {
        pid: u32,
        config: FoundationDbClientConfig,
    },
    Stopped {
        pid: u32,
    },
    Failed {
        pid: u32,
        error: StateStoreError,
    },
}

#[cfg(feature = "foundationdb-provider")]
static PROCESS_NETWORK: Mutex<ProcessNetworkState> = Mutex::new(ProcessNetworkState::Never);

#[cfg(feature = "foundationdb-provider")]
struct FoundationDbRuntime {
    shared: Arc<FoundationDbRuntimeShared>,
    network: FoundationDbNetworkLifecycle,
}

#[cfg(feature = "foundationdb-provider")]
struct FoundationDbRuntimeShared {
    pid: u32,
    accepting: AtomicBool,
    in_flight: AtomicUsize,
    provider_handles: AtomicUsize,
    next_database_id: AtomicU64,
    databases: Mutex<HashMap<u64, Arc<Database>>>,
    drained: Notify,
}

#[cfg(feature = "foundationdb-provider")]
struct FoundationDbNetworkOwner {
    stop: Option<Box<dyn NetworkStopAction>>,
    thread: Option<JoinHandle<Result<(), StateStoreError>>>,
}

#[cfg(feature = "foundationdb-provider")]
enum FoundationDbNetworkLifecycle {
    Running(FoundationDbNetworkOwner),
    Failed {
        error: StateStoreError,
        thread: Option<JoinHandle<Result<(), StateStoreError>>>,
    },
    Stopped,
}

#[cfg(feature = "foundationdb-provider")]
trait NetworkStopAction: Send {
    fn stop(self: Box<Self>) -> Result<(), StateStoreError>;
}

#[cfg(feature = "foundationdb-provider")]
struct NativeNetworkStop(NetworkStop);

#[cfg(feature = "foundationdb-provider")]
impl NetworkStopAction for NativeNetworkStop {
    fn stop(self: Box<Self>) -> Result<(), StateStoreError> {
        self.0.stop().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network stop failed",
            )
        })
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbRuntime {
    fn boot(config: FoundationDbClientConfig) -> Result<Self, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB client configuration is invalid",
            )
        })?;
        let pid = std::process::id();
        {
            let mut process = process_network_state()?;
            match &*process {
                ProcessNetworkState::Never => {
                    *process = ProcessNetworkState::Starting {
                        pid,
                        config: config.clone(),
                    };
                }
                ProcessNetworkState::Starting {
                    pid: owner_pid,
                    config: owner_config,
                }
                | ProcessNetworkState::Running {
                    pid: owner_pid,
                    config: owner_config,
                } => {
                    if *owner_pid != pid {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "FoundationDB network was initialized in a different process",
                        ));
                    }
                    if owner_config != &config {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "a different FoundationDB client configuration is already active",
                        ));
                    }
                    return Err(StateStoreError::new(
                        StateStoreErrorKind::InvalidConfiguration,
                        "FoundationDB network is already running in this process",
                    ));
                }
                ProcessNetworkState::Stopped { pid: owner_pid } => {
                    if *owner_pid != pid {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "FoundationDB network was stopped in a different process",
                        ));
                    }
                    return Err(StateStoreError::new(
                        StateStoreErrorKind::InvalidConfiguration,
                        "FoundationDB network is stopped and cannot restart in this process",
                    ));
                }
                ProcessNetworkState::Failed {
                    pid: owner_pid,
                    error,
                } => {
                    if *owner_pid != pid {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "FoundationDB network failed in a different process",
                        ));
                    }
                    return Err(error.clone());
                }
            }
        }

        let owner = match start_foundationdb_network(&config) {
            Ok(owner) => owner,
            Err(error) => {
                mark_process_network_failed(pid, error.clone());
                return Err(error);
            }
        };
        {
            let mut process = process_network_state()?;
            *process = ProcessNetworkState::Running {
                pid,
                config: config.clone(),
            };
        }
        tracing::info!(
            provider = "foundationdb",
            lifecycle = "started",
            process_id = pid,
            "FoundationDB state store runtime started"
        );

        Ok(Self {
            shared: Arc::new(FoundationDbRuntimeShared {
                pid,
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                provider_handles: AtomicUsize::new(0),
                next_database_id: AtomicU64::new(1),
                databases: Mutex::new(HashMap::new()),
                drained: Notify::new(),
            }),
            network: FoundationDbNetworkLifecycle::Running(owner),
        })
    }

    async fn open_store(
        &self,
        config: &StateStoreConfig,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store configuration is invalid",
            )
        })?;
        let opening = self.shared.acquire_operation()?;
        let (cluster_file, keyspace_id) = match &config.provider {
            super::StateStoreProviderConfig::Foundationdb {
                cluster_file,
                keyspace_id,
            } => (cluster_file, *keyspace_id),
            super::StateStoreProviderConfig::Sqlite { .. } => {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "FoundationDB runtime cannot open a SQLite state store",
                ));
            }
            super::StateStoreProviderConfig::Mysql { .. } => {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "FoundationDB runtime cannot open a MySQL state store",
                ));
            }
        };
        let limits = StateStoreLimits::from_overrides(&config.limits).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "state store limits are invalid",
            )
        })?;
        let path = cluster_file.to_str().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB cluster file path must be valid UTF-8",
            )
        })?;
        let database = Database::from_path(path).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB database creation failed",
            )
        })?;
        let lease = ProviderHandle::new(Arc::clone(&self.shared), Arc::new(database))?;
        let store =
            FoundationDbStateStore::open(lease, limits, config.cluster_id.clone(), keyspace_id)
                .await?;
        drop(opening);
        Ok(Arc::new(store))
    }

    async fn shutdown(&mut self) -> Result<(), StateStoreError> {
        self.shared.validate_pid()?;
        if let Some(result) = self.network.terminal_result() {
            return result;
        }
        if self
            .shared
            .accepting
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime shutdown is already in progress",
            ));
        }
        tracing::info!(
            provider = "foundationdb",
            lifecycle = "stopping",
            process_id = self.shared.pid,
            "FoundationDB state store runtime stopping"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.shared.is_drained() {
            if timeout_at(deadline, self.shared.drained.notified())
                .await
                .is_err()
            {
                self.shared.accepting.store(true, Ordering::Release);
                let fields = shutdown_deferred_log_fields();
                tracing::warn!(
                    provider = "foundationdb",
                    lifecycle = fields.lifecycle,
                    reason = fields.reason,
                    process_id = self.shared.pid,
                    in_flight = self.shared.in_flight.load(Ordering::Acquire),
                    provider_handles = self.shared.provider_handles.load(Ordering::Acquire),
                    "FoundationDB state store runtime shutdown deferred"
                );
                return Err(StateStoreError::new(
                    StateStoreErrorKind::DeadlineExceeded,
                    "FoundationDB runtime handles did not drain within five seconds",
                ));
            }
        }

        self.shared.drop_database_registry();
        match self.network.stop_and_join() {
            Ok(()) => {
                mark_process_network_stopped(self.shared.pid);
                tracing::info!(
                    provider = "foundationdb",
                    lifecycle = "stopped",
                    process_id = self.shared.pid,
                    "FoundationDB state store runtime stopped"
                );
                Ok(())
            }
            Err(error) => {
                mark_process_network_failed(self.shared.pid, error.clone());
                tracing::warn!(
                    provider = "foundationdb",
                    lifecycle = "stop_failed",
                    process_id = self.shared.pid,
                    error_kind = ?error.kind(),
                    "FoundationDB state store runtime stop failed"
                );
                Err(error)
            }
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
impl Drop for FoundationDbRuntime {
    fn drop(&mut self) {
        let shutdown_required_reason = match &mut self.network {
            FoundationDbNetworkLifecycle::Running(_) => Some("runtime_still_running"),
            FoundationDbNetworkLifecycle::Failed { thread, .. }
                if thread.as_ref().is_some_and(|thread| !thread.is_finished()) =>
            {
                Some("network_thread_still_running")
            }
            FoundationDbNetworkLifecycle::Failed { thread, .. } => {
                FoundationDbNetworkLifecycle::reap_finished_thread(thread);
                None
            }
            FoundationDbNetworkLifecycle::Stopped => None,
        };
        if let Some(reason) = shutdown_required_reason {
            tracing::error!(
                provider = "foundationdb",
                lifecycle = "shutdown_required",
                reason,
                process_id = self.shared.pid,
                "FoundationDB state store runtime dropped before shutdown"
            );
            std::process::abort();
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbNetworkLifecycle {
    fn terminal_result(&mut self) -> Option<Result<(), StateStoreError>> {
        match self {
            Self::Running(_) => None,
            Self::Stopped => Some(Ok(())),
            Self::Failed { error, thread } => {
                Self::reap_finished_thread(thread);
                Some(Err(error.clone()))
            }
        }
    }

    fn stop_and_join(&mut self) -> Result<(), StateStoreError> {
        let owner = match self {
            Self::Running(owner) => owner,
            Self::Stopped => return Ok(()),
            Self::Failed { error, thread } => {
                Self::reap_finished_thread(thread);
                return Err(error.clone());
            }
        };
        let stop = match owner.stop.take() {
            Some(stop) => stop,
            None => {
                let error = StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "FoundationDB runtime lost its stop handle",
                );
                let thread = owner.thread.take();
                *self = Self::Failed {
                    error: error.clone(),
                    thread,
                };
                return Err(error);
            }
        };
        let stop_result = catch_unwind(AssertUnwindSafe(|| stop.stop())).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network stop panicked",
            )
        });
        let stop_result = match stop_result {
            Ok(result) => result,
            Err(error) => Err(error),
        };
        if let Err(error) = stop_result {
            let thread = owner.thread.take();
            *self = Self::Failed {
                error: error.clone(),
                thread,
            };
            return Err(error);
        }

        let thread = match owner.thread.take() {
            Some(thread) => thread,
            None => {
                let error = StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "FoundationDB runtime lost its network thread",
                );
                *self = Self::Failed {
                    error: error.clone(),
                    thread: None,
                };
                return Err(error);
            }
        };
        let joined = catch_unwind(AssertUnwindSafe(|| thread.join()));
        let result = match joined {
            Err(_) => Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network join panicked",
            )),
            Ok(Err(_)) => Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network thread panicked",
            )),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Ok(Ok(()))) => Ok(()),
        };
        match result {
            Ok(()) => {
                *self = Self::Stopped;
                Ok(())
            }
            Err(error) => {
                *self = Self::Failed {
                    error: error.clone(),
                    thread: None,
                };
                Err(error)
            }
        }
    }

    fn reap_finished_thread(thread: &mut Option<JoinHandle<Result<(), StateStoreError>>>) {
        if thread.as_ref().is_some_and(JoinHandle::is_finished) {
            if let Some(handle) = thread.take() {
                let _ = handle.join();
            }
        }
    }

    #[cfg(test)]
    fn retains_join_ownership(&self) -> bool {
        matches!(
            self,
            Self::Running(FoundationDbNetworkOwner {
                thread: Some(_),
                ..
            })
        ) || matches!(
            self,
            Self::Failed {
                thread: Some(_),
                ..
            }
        )
    }

    #[cfg(test)]
    fn failed_thread_is_finished(&self) -> bool {
        matches!(self, Self::Failed { thread: Some(thread), .. } if thread.is_finished())
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbRuntimeShared {
    fn validate_pid(&self) -> Result<(), StateStoreError> {
        if self.pid != std::process::id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                RUNTIME_PID_ERROR,
            ));
        }
        Ok(())
    }

    fn validate_accepting(&self) -> Result<(), StateStoreError> {
        self.validate_pid()?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime is not accepting new handles",
            ));
        }
        Ok(())
    }

    fn is_drained(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) == 0
            && self.provider_handles.load(Ordering::Acquire) == 0
    }

    fn drop_database_registry(&self) {
        match self.databases.lock() {
            Ok(mut databases) => databases.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }

    #[allow(dead_code)]
    fn acquire_operation(self: &Arc<Self>) -> Result<OperationHandle, StateStoreError> {
        self.validate_accepting()?;
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) || self.pid != std::process::id() {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.drained.notify_one();
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime stopped accepting operations",
            ));
        }
        Ok(OperationHandle {
            shared: Arc::clone(self),
        })
    }
}

#[cfg(feature = "foundationdb-provider")]
pub(super) struct ProviderHandle {
    shared: Arc<FoundationDbRuntimeShared>,
    database_id: u64,
}

#[cfg(feature = "foundationdb-provider")]
impl ProviderHandle {
    fn new(
        shared: Arc<FoundationDbRuntimeShared>,
        database: Arc<Database>,
    ) -> Result<Self, StateStoreError> {
        shared.validate_accepting()?;
        let database_id = shared.next_database_id.fetch_add(1, Ordering::Relaxed);
        shared.provider_handles.fetch_add(1, Ordering::AcqRel);
        match shared.databases.lock() {
            Ok(mut databases) => {
                databases.insert(database_id, database);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(database_id, database);
            }
        }
        if !shared.accepting.load(Ordering::Acquire) || shared.pid != std::process::id() {
            match shared.databases.lock() {
                Ok(mut databases) => {
                    databases.remove(&database_id);
                }
                Err(poisoned) => {
                    poisoned.into_inner().remove(&database_id);
                }
            }
            shared.provider_handles.fetch_sub(1, Ordering::AcqRel);
            shared.drained.notify_one();
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime stopped accepting provider handles",
            ));
        }
        Ok(Self {
            shared,
            database_id,
        })
    }

    #[allow(dead_code)]
    pub(super) fn database(&self) -> Result<Arc<Database>, StateStoreError> {
        let databases = self.shared.databases.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "FoundationDB database registry is poisoned",
            )
        })?;
        databases.get(&self.database_id).cloned().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB database handle is closed",
            )
        })
    }

    #[allow(dead_code)]
    pub(super) fn acquire_operation(&self) -> Result<OperationHandle, StateStoreError> {
        self.shared.acquire_operation()
    }
}

#[cfg(feature = "foundationdb-provider")]
impl Drop for ProviderHandle {
    fn drop(&mut self) {
        let database = match self.shared.databases.lock() {
            Ok(mut databases) => databases.remove(&self.database_id),
            Err(poisoned) => poisoned.into_inner().remove(&self.database_id),
        };
        drop(database);
        self.shared.provider_handles.fetch_sub(1, Ordering::AcqRel);
        self.shared.drained.notify_one();
    }
}

#[cfg(feature = "foundationdb-provider")]
#[allow(dead_code)]
pub(super) struct OperationHandle {
    shared: Arc<FoundationDbRuntimeShared>,
}

#[cfg(feature = "foundationdb-provider")]
impl Drop for OperationHandle {
    fn drop(&mut self) {
        self.shared.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.shared.drained.notify_one();
    }
}

#[cfg(feature = "foundationdb-provider")]
fn start_foundationdb_network(
    config: &FoundationDbClientConfig,
) -> Result<FoundationDbNetworkOwner, StateStoreError> {
    let (max_api_version, selected_api_version) =
        foundationdb_api_versions(foundationdb::api::get_max_api_version())?;
    tracing::info!(
        provider = "foundationdb",
        api_max_version = max_api_version,
        api_selected_version = selected_api_version,
        "FoundationDB API version selected"
    );

    let initialized = catch_unwind(AssertUnwindSafe(|| {
        let mut network = FdbApiBuilder::default()
            .set_runtime_version(selected_api_version)
            .build()
            .map_err(|_| ())?;
        network = network
            .set_option(NetworkOption::DisableMultiVersionClientApi)
            .map_err(|_| ())?;
        if let Some(password_env) = config.tls_password_env.as_deref() {
            let password = std::env::var(password_env).map_err(|_| ())?;
            network = network
                .set_option(NetworkOption::TLSPassword(password))
                .map_err(|_| ())?;
        }
        if let Some(path) = config.tls_key_path.as_deref() {
            network = network
                .set_option(NetworkOption::TLSKeyPath(
                    path.to_str().ok_or(())?.to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(path) = config.tls_cert_path.as_deref() {
            network = network
                .set_option(NetworkOption::TLSCertPath(
                    path.to_str().ok_or(())?.to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(path) = config.tls_ca_path.as_deref() {
            network = network
                .set_option(NetworkOption::TLSCaPath(
                    path.to_str().ok_or(())?.to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(peers) = config.tls_verify_peers.as_deref() {
            network = network
                .set_option(NetworkOption::TLSVerifyPeers(peers.as_bytes().to_vec()))
                .map_err(|_| ())?;
        }
        network.build().map_err(|_| ())
    }))
    .map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB network initialization panicked",
        )
    })?
    .map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB network initialization failed",
        )
    })?;
    let (runner, wait) = initialized;
    let thread = std::thread::Builder::new()
        .name("novarocks-foundationdb-network".to_owned())
        .spawn(move || {
            catch_unwind(AssertUnwindSafe(|| unsafe { NetworkRunner::run(runner) }))
                .map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::ProviderUnavailable,
                        "FoundationDB network runner panicked",
                    )
                })?
                .map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::ProviderUnavailable,
                        "FoundationDB network runner failed",
                    )
                })
        })
        .map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network thread could not be started",
            )
        })?;
    finish_foundationdb_network_start(thread, || {
        catch_unwind(AssertUnwindSafe(|| wait.wait()))
            .map(|stop| Box::new(NativeNetworkStop(stop)) as Box<dyn NetworkStopAction>)
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB network startup wait panicked",
                )
            })
    })
}

#[cfg(feature = "foundationdb-provider")]
fn foundationdb_api_versions(max_api_version: i32) -> Result<(i32, i32), StateStoreError> {
    if max_api_version < FOUNDATIONDB_API_VERSION {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "FoundationDB client does not support API version 730",
        ));
    }
    Ok((max_api_version, FOUNDATIONDB_API_VERSION))
}

#[cfg(feature = "foundationdb-provider")]
fn finish_foundationdb_network_start(
    thread: JoinHandle<Result<(), StateStoreError>>,
    wait: impl FnOnce() -> Result<Box<dyn NetworkStopAction>, StateStoreError>,
) -> Result<FoundationDbNetworkOwner, StateStoreError> {
    match wait() {
        Ok(stop) => Ok(FoundationDbNetworkOwner {
            stop: Some(stop),
            thread: Some(thread),
        }),
        Err(error) => {
            // NetworkWait only panics when its shared mutex is poisoned. The runner
            // uses that same mutex before entering the native loop, so it has already
            // terminated and can be joined without a stop handle.
            let _ = catch_unwind(AssertUnwindSafe(|| thread.join()));
            Err(error)
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
fn process_network_state()
-> Result<std::sync::MutexGuard<'static, ProcessNetworkState>, StateStoreError> {
    PROCESS_NETWORK.lock().map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB process runtime state is poisoned",
        )
    })
}

#[cfg(feature = "foundationdb-provider")]
fn mark_process_network_stopped(pid: u32) {
    match PROCESS_NETWORK.lock() {
        Ok(mut process) => *process = ProcessNetworkState::Stopped { pid },
        Err(poisoned) => *poisoned.into_inner() = ProcessNetworkState::Stopped { pid },
    }
}

#[cfg(feature = "foundationdb-provider")]
fn mark_process_network_failed(pid: u32, error: StateStoreError) {
    match PROCESS_NETWORK.lock() {
        Ok(mut process) => *process = ProcessNetworkState::Failed { pid, error },
        Err(poisoned) => *poisoned.into_inner() = ProcessNetworkState::Failed { pid, error },
    }
}

#[cfg(all(test, feature = "foundationdb-provider"))]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::mpsc;
    use std::time::Instant as StdInstant;

    static TEST_PROCESS_STATE: Mutex<()> = Mutex::new(());

    struct TestNetworkStop {
        action: Box<dyn FnOnce() -> Result<(), StateStoreError> + Send>,
    }

    impl NetworkStopAction for TestNetworkStop {
        fn stop(self: Box<Self>) -> Result<(), StateStoreError> {
            (self.action)()
        }
    }

    fn test_stop(
        action: impl FnOnce() -> Result<(), StateStoreError> + Send + 'static,
    ) -> Box<dyn NetworkStopAction> {
        Box::new(TestNetworkStop {
            action: Box::new(action),
        })
    }

    fn test_runtime(
        stop: Box<dyn NetworkStopAction>,
        thread: JoinHandle<Result<(), StateStoreError>>,
    ) -> FoundationDbRuntime {
        FoundationDbRuntime {
            shared: Arc::new(FoundationDbRuntimeShared {
                pid: std::process::id(),
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                provider_handles: AtomicUsize::new(0),
                next_database_id: AtomicU64::new(1),
                databases: Mutex::new(HashMap::new()),
                drained: Notify::new(),
            }),
            network: FoundationDbNetworkLifecycle::Running(FoundationDbNetworkOwner {
                stop: Some(stop),
                thread: Some(thread),
            }),
        }
    }

    fn reset_process_state() {
        *PROCESS_NETWORK
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = ProcessNetworkState::Never;
    }

    #[test]
    fn foundationdb_api_selection_reports_max_and_fixed_selected_version() {
        assert_eq!(
            foundationdb_api_versions(740).expect("supported API"),
            (740, 730)
        );
        let error = foundationdb_api_versions(729).expect_err("old client must fail closed");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_stop_failure_is_stable_and_retains_join_ownership() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            Ok(())
        });
        let expected = StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected network stop failure",
        );
        let injected = expected.clone();
        let mut runtime = test_runtime(test_stop(move || Err(injected)), thread);

        let first = runtime
            .shutdown()
            .await
            .expect_err("stop failure must surface");
        assert_eq!(first, expected);
        assert!(runtime.network.retains_join_ownership());
        let second = runtime
            .shutdown()
            .await
            .expect_err("repeated shutdown must return the stable failure");
        assert_eq!(second, expected);
        assert!(runtime.network.retains_join_ownership());

        release_tx.send(()).expect("release failed network thread");
        while !runtime.network.failed_thread_is_finished() {
            std::thread::yield_now();
        }
        let third = runtime
            .shutdown()
            .await
            .expect_err("reaping must preserve the stable failure");
        assert_eq!(third, expected);
        assert!(!runtime.network.retains_join_ownership());
        reset_process_state();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_join_failure_is_stable_instead_of_becoming_success() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let expected = StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected network runner failure",
        );
        let injected = expected.clone();
        let thread = std::thread::spawn(move || Err(injected));
        let mut runtime = test_runtime(test_stop(|| Ok(())), thread);

        let first = runtime
            .shutdown()
            .await
            .expect_err("join failure must surface");
        assert_eq!(first, expected);
        let second = runtime
            .shutdown()
            .await
            .expect_err("consumed join failure must remain terminal");
        assert_eq!(second, expected);
        reset_process_state();
    }

    #[test]
    fn startup_wait_failure_joins_the_spawned_thread_and_poison_is_stable() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let completed = Arc::new(AtomicBool::new(false));
        let thread_completed = Arc::clone(&completed);
        let thread = std::thread::spawn(move || {
            thread_completed.store(true, Ordering::Release);
            Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected startup runner failure",
            ))
        });
        let wait_error = StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected startup wait failure",
        );

        let result = finish_foundationdb_network_start(thread, || Err(wait_error.clone()));
        let result = match result {
            Ok(_) => panic!("startup wait failure must surface"),
            Err(error) => error,
        };
        assert_eq!(result, wait_error);
        assert!(completed.load(Ordering::Acquire));
        mark_process_network_failed(std::process::id(), wait_error.clone());
        let repeated = FoundationDbRuntime::boot(FoundationDbClientConfig {
            disable_multi_version_client: true,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_verify_peers: None,
            tls_password_env: None,
        });
        let repeated = match repeated {
            Ok(_) => panic!("poisoned process runtime must reject later boot"),
            Err(error) => error,
        };
        assert_eq!(repeated, wait_error);
        reset_process_state();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operation_handle_blocks_five_second_shutdown_then_allows_retry() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            stop_rx.recv().expect("network stop signal");
            Ok(())
        });
        let mut runtime = test_runtime(
            test_stop(move || {
                stop_tx.send(()).expect("signal network stop");
                Ok(())
            }),
            thread,
        );
        let operation = runtime
            .shared
            .acquire_operation()
            .expect("acquire real operation handle");

        let started = StdInstant::now();
        let timeout = runtime
            .shutdown()
            .await
            .expect_err("live operation must block shutdown");
        assert_eq!(timeout.kind(), StateStoreErrorKind::DeadlineExceeded);
        assert!(started.elapsed() >= Duration::from_millis(4_900));
        drop(operation);
        runtime
            .shutdown()
            .await
            .expect("shutdown must retry after operation drain");
        reset_process_state();
    }

    #[test]
    fn failed_runtime_with_running_thread_drop_fails_fast() {
        let child = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                "state_store::runtime::tests::failed_runtime_with_running_thread_drop_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .status()
            .expect("exec failed runtime drop child");
        assert!(
            !child.success(),
            "dropping a failed runtime with a live network thread must fail fast"
        );
    }

    #[test]
    fn running_runtime_drop_fails_fast() {
        let child = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                "state_store::runtime::tests::running_runtime_drop_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .status()
            .expect("exec running runtime drop child");
        assert!(
            !child.success(),
            "dropping a running runtime must fail fast instead of detaching the network"
        );
    }

    #[test]
    #[ignore = "exec helper used by running_runtime_drop_fails_fast"]
    fn running_runtime_drop_child() {
        let runtime = StateStoreRuntime {
            inner: RuntimeInner::FoundationDb(test_runtime(
                test_stop(|| Ok(())),
                std::thread::spawn(|| Ok(())),
            )),
        };
        drop(runtime);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopped_runtime_drop_is_safe() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (stop_tx, stop_rx) = mpsc::channel();
        let runtime = test_runtime(
            test_stop(move || {
                stop_tx.send(()).expect("signal network stop");
                Ok(())
            }),
            std::thread::spawn(move || {
                stop_rx.recv().expect("network stop signal");
                Ok(())
            }),
        );
        let mut runtime = StateStoreRuntime {
            inner: RuntimeInner::FoundationDb(runtime),
        };

        runtime.shutdown().await.expect("stop test runtime");
        drop(runtime);
        reset_process_state();
    }

    #[test]
    #[ignore = "exec helper used by failed_runtime_with_running_thread_drop_fails_fast"]
    fn failed_runtime_with_running_thread_drop_child() {
        let stop_calls = Arc::new(AtomicUsize::new(0));
        let observed_stop_calls = Arc::clone(&stop_calls);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            Ok(())
        });
        let mut runtime = test_runtime(
            test_stop(move || {
                observed_stop_calls.fetch_add(1, Ordering::AcqRel);
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected network stop failure",
                ))
            }),
            thread,
        );
        runtime
            .network
            .stop_and_join()
            .expect_err("injected stop failure must enter Failed");
        assert_eq!(stop_calls.load(Ordering::Acquire), 1);
        let runtime = StateStoreRuntime {
            inner: RuntimeInner::FoundationDb(runtime),
        };

        std::mem::forget(release_tx);
        drop(runtime);
    }

    #[test]
    fn failed_runtime_with_finished_thread_drop_is_safe() {
        let stop_calls = Arc::new(AtomicUsize::new(0));
        let observed_stop_calls = Arc::clone(&stop_calls);
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            Ok(())
        });
        let mut runtime = test_runtime(
            test_stop(move || {
                observed_stop_calls.fetch_add(1, Ordering::AcqRel);
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected network stop failure",
                ))
            }),
            thread,
        );
        runtime
            .network
            .stop_and_join()
            .expect_err("injected stop failure must enter Failed");
        release_tx.send(()).expect("release failed network thread");
        while !runtime.network.failed_thread_is_finished() {
            std::thread::yield_now();
        }
        let runtime = StateStoreRuntime {
            inner: RuntimeInner::FoundationDb(runtime),
        };

        drop(runtime);
        assert_eq!(stop_calls.load(Ordering::Acquire), 1);
    }
}
