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

use novarocks_spi::state_store::{StateStoreError, StateStoreErrorKind};

#[cfg(any(
    feature = "mysql-state-store-provider",
    feature = "foundationdb-provider"
))]
use novarocks_spi::state_store::StateStore;
#[cfg(any(
    feature = "mysql-state-store-provider",
    feature = "foundationdb-provider"
))]
use {super::StateStoreConfig, std::sync::Arc, std::time::Duration};

#[cfg(feature = "foundationdb-provider")]
use {super::FoundationDbClientConfig, super::foundationdb::LegacyFoundationDbRuntime};

#[cfg(feature = "mysql-state-store-provider")]
use {
    super::limits::{MYSQL_MAX_KEY_BYTES, resolve_state_store_limits},
    super::mysql::LegacyMysqlRuntime,
    super::{FeDeploymentView, MySqlClientConfig},
    novarocks_spi::state_store::{FeDeploymentView as SpiFeDeploymentView, StateStoreOpenRequest},
};

const RUNTIME_PID_ERROR: &str = "state store runtime belongs to a different process";

pub struct StateStoreRuntime {
    inner: RuntimeInner,
}

enum RuntimeInner {
    Local(LocalRuntime),
    #[cfg(feature = "foundationdb-provider")]
    FoundationDb(LegacyFoundationDbRuntime),
    #[cfg(feature = "mysql-state-store-provider")]
    Mysql(LegacyMysqlRuntime),
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
        LegacyFoundationDbRuntime::boot(config).map(|runtime| Self {
            inner: RuntimeInner::FoundationDb(runtime),
        })
    }

    #[cfg(feature = "mysql-state-store-provider")]
    pub fn mysql(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        LegacyMysqlRuntime::boot(config).map(|runtime| Self {
            inner: RuntimeInner::Mysql(runtime),
        })
    }

    pub async fn shutdown(&mut self) -> Result<(), StateStoreError> {
        match &mut self.inner {
            RuntimeInner::Local(runtime) => runtime.shutdown(),
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(runtime) => {
                runtime
                    .shutdown_until(std::time::Instant::now() + Duration::from_secs(5))
                    .await
            }
            #[cfg(feature = "mysql-state-store-provider")]
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .shutdown_until(std::time::Instant::now() + Duration::from_secs(5))
                    .await
            }
        }
    }

    #[cfg(any(
        feature = "mysql-state-store-provider",
        feature = "foundationdb-provider"
    ))]
    pub async fn shutdown_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), StateStoreError> {
        match &mut self.inner {
            #[cfg(feature = "foundationdb-provider")]
            RuntimeInner::FoundationDb(runtime) => {
                runtime
                    .shutdown_until(std::time::Instant::now() + timeout)
                    .await
            }
            #[cfg(feature = "mysql-state-store-provider")]
            RuntimeInner::Mysql(runtime) => {
                runtime
                    .shutdown_until(std::time::Instant::now() + timeout)
                    .await
            }
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
        deployment: super::FeDeploymentView,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        match &self.inner {
            RuntimeInner::Local(_) => Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store requires a FoundationDB runtime",
            )),
            RuntimeInner::FoundationDb(runtime) => runtime.open_store(config, deployment).await,
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
            RuntimeInner::Mysql(runtime) => {
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
                let limits = resolve_state_store_limits(&config.limits, MYSQL_MAX_KEY_BYTES)
                    .map_err(|_| {
                        StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "MySQL state store limits are invalid",
                        )
                    })?;
                runtime
                    .open_store(
                        database,
                        StateStoreOpenRequest {
                            cluster_id: config.cluster_id.clone(),
                            limits: limits.clone(),
                            deployment: SpiFeDeploymentView {
                                active_fe_count: deployment.active_fe_count,
                                topology_revision: deployment.topology_revision,
                            },
                            deadline: std::time::Instant::now() + limits.transaction_deadline,
                        },
                    )
                    .await
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
fn mysql_runtime_mismatch() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidConfiguration,
        "operation requires a MySQL state store runtime",
    )
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
