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
        match self.network.stop_and_join() {
            Ok(()) => {
                mark_process_network_stopped(self.shared.pid);
                Ok(())
            }
            Err(error) => {
                mark_process_network_failed(self.shared.pid, error.clone());
                Err(error)
            }
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
    fn failed_lifecycle_drop_is_inert() {
        let child = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                "state_store::runtime::tests::failed_lifecycle_drop_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .status()
            .expect("exec failed lifecycle drop child");
        assert!(
            child.success(),
            "dropping a failed lifecycle must not abort, stop, or join"
        );
    }

    #[test]
    #[ignore = "exec helper used by failed_lifecycle_drop_is_inert"]
    fn failed_lifecycle_drop_child() {
        let stop_calls = Arc::new(AtomicUsize::new(0));
        let observed_stop_calls = Arc::clone(&stop_calls);
        let (release_tx, release_rx) = mpsc::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let thread_completed = Arc::clone(&completed);
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            thread_completed.store(true, Ordering::Release);
            Ok(())
        });
        let mut lifecycle = FoundationDbNetworkLifecycle::Running(FoundationDbNetworkOwner {
            stop: Some(test_stop(move || {
                observed_stop_calls.fetch_add(1, Ordering::AcqRel);
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected network stop failure",
                ))
            })),
            thread: Some(thread),
        });
        lifecycle
            .stop_and_join()
            .expect_err("injected stop failure must enter Failed");
        assert_eq!(stop_calls.load(Ordering::Acquire), 1);

        drop(lifecycle);

        assert_eq!(stop_calls.load(Ordering::Acquire), 1);
        assert!(!completed.load(Ordering::Acquire));
        release_tx.send(()).expect("release detached test thread");
    }
}
