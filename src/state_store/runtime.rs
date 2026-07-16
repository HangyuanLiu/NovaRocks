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

#[cfg(feature = "foundationdb-provider")]
use {
    super::{
        ChangePage, ChangePollRequest, CommitResolution, FoundationDbClientConfig, ReadTransaction,
        StateStore, StateStoreConfig, StateStoreLimits, StoreIdentity, TransactionId,
        WriteTransaction,
    },
    async_trait::async_trait,
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

pub struct StateStoreRuntime {
    inner: RuntimeInner,
}

enum RuntimeInner {
    Local(LocalRuntime),
    #[cfg(feature = "foundationdb-provider")]
    FoundationDb(FoundationDbRuntime),
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

    pub async fn shutdown(&mut self) -> Result<(), StateStoreError> {
        match &mut self.inner {
            RuntimeInner::Local(runtime) => runtime.shutdown(),
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(runtime) => runtime.shutdown().await,
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
        }
    }

    #[cfg(feature = "foundationdb-provider")]
    pub(crate) fn open_foundationdb_store(
        &self,
        config: &StateStoreConfig,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        match &self.inner {
            RuntimeInner::Local(_) => Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store requires a FoundationDB runtime",
            )),
            RuntimeInner::FoundationDb(runtime) => runtime.open_store(config),
        }
    }
}

impl fmt::Debug for StateStoreRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            RuntimeInner::Local(_) => formatter.write_str("StateStoreRuntime::Local"),
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(_) => formatter.write_str("StateStoreRuntime::FoundationDb"),
        }
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
}

#[cfg(feature = "foundationdb-provider")]
static PROCESS_NETWORK: Mutex<ProcessNetworkState> = Mutex::new(ProcessNetworkState::Never);

#[cfg(feature = "foundationdb-provider")]
struct FoundationDbRuntime {
    shared: Arc<FoundationDbRuntimeShared>,
    owner: Option<FoundationDbNetworkOwner>,
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
    stop: Option<NetworkStop>,
    thread: Option<JoinHandle<Result<(), StateStoreError>>>,
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
            }
        }

        let owner = match start_foundationdb_network(&config) {
            Ok(owner) => owner,
            Err(error) => {
                mark_process_network_stopped(pid);
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
            owner: Some(owner),
        })
    }

    fn open_store(
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
        let cluster_file = match &config.provider {
            super::StateStoreProviderConfig::Foundationdb { cluster_file, .. } => cluster_file,
            super::StateStoreProviderConfig::Sqlite { .. } => {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "FoundationDB runtime cannot open a SQLite state store",
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
        drop(opening);
        Ok(Arc::new(FoundationDbRuntimeStore { lease, limits }))
    }

    async fn shutdown(&mut self) -> Result<(), StateStoreError> {
        self.shared.validate_pid()?;
        if self.owner.is_none() {
            return Ok(());
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

        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.shared.is_drained() {
            if timeout_at(deadline, self.shared.drained.notified())
                .await
                .is_err()
            {
                self.shared.accepting.store(true, Ordering::Release);
                return Err(StateStoreError::new(
                    StateStoreErrorKind::DeadlineExceeded,
                    "FoundationDB runtime handles did not drain within five seconds",
                ));
            }
        }

        self.shared.drop_database_registry();
        let mut owner = self.owner.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "FoundationDB runtime lost network ownership",
            )
        })?;
        let stop = owner.stop.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "FoundationDB runtime lost its stop handle",
            )
        })?;
        catch_unwind(AssertUnwindSafe(|| stop.stop()))
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB network stop panicked",
                )
            })?
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB network stop failed",
                )
            })?;
        let thread = owner.thread.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "FoundationDB runtime lost its network thread",
            )
        })?;
        let thread_result = catch_unwind(AssertUnwindSafe(|| thread.join())).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network join panicked",
            )
        })?;
        thread_result.map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network thread panicked",
            )
        })??;
        mark_process_network_stopped(self.shared.pid);
        Ok(())
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
struct ProviderHandle {
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
    fn database(&self) -> Result<Arc<Database>, StateStoreError> {
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
    fn acquire_operation(&self) -> Result<OperationHandle, StateStoreError> {
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
struct OperationHandle {
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
struct FoundationDbRuntimeStore {
    lease: ProviderHandle,
    limits: StateStoreLimits,
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbRuntimeStore {
    fn unavailable() -> StateStoreError {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB state store operations are not initialized",
        )
    }
}

#[cfg(feature = "foundationdb-provider")]
#[async_trait]
impl StateStore for FoundationDbRuntimeStore {
    fn provider_name(&self) -> &'static str {
        "foundationdb"
    }

    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> super::StateStoreMetricsSnapshot {
        super::StateStoreMetrics::new("foundationdb").snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let _ = &self.lease;
        Err(Self::unavailable())
    }

    async fn begin_write(
        &self,
        _transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        Err(Self::unavailable())
    }

    async fn poll_changes(
        &self,
        _request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        Err(Self::unavailable())
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        Err(Self::unavailable())
    }

    async fn resolve_commit(
        &self,
        _transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        Err(Self::unavailable())
    }
}

#[cfg(feature = "foundationdb-provider")]
fn start_foundationdb_network(
    config: &FoundationDbClientConfig,
) -> Result<FoundationDbNetworkOwner, StateStoreError> {
    if foundationdb::api::get_max_api_version() < 730 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "FoundationDB client does not support API version 730",
        ));
    }

    let initialized = catch_unwind(AssertUnwindSafe(|| {
        let mut network = FdbApiBuilder::default()
            .set_runtime_version(730)
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
    let stop = catch_unwind(AssertUnwindSafe(|| wait.wait())).map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB network startup wait panicked",
        )
    })?;
    Ok(FoundationDbNetworkOwner {
        stop: Some(stop),
        thread: Some(thread),
    })
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
