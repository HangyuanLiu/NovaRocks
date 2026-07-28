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

use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod config;
pub mod coordination;
mod deployment;
mod host;
pub mod host_error;
pub mod limits;
pub mod metrics;
pub mod provider;
pub mod runner;
mod runtime;

use novarocks_spi::state_store::{
    FeDeploymentView as SpiFeDeploymentView, StateStore, StateStoreError, StateStoreErrorKind,
    StateStoreOpenRequest, StateStoreProviderFactory,
};

mod sqlite;

#[cfg(feature = "foundationdb-provider")]
mod foundationdb;

#[cfg(feature = "mysql-state-store-provider")]
pub mod mysql;

#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[doc(hidden)]
pub use foundationdb::test_support::{FoundationDbCommitGateControl, arm_next_foundationdb_commit};

pub use config::{
    FoundationDbClientConfig, MySqlClientConfig, MySqlTlsMode, StateStoreAppConfig,
    StateStoreConfig, StateStoreHostConfig, StateStoreProviderConfig,
};
pub use deployment::FeDeploymentView;
pub use host::{StateStoreHost, StateStoreHostLifecycle};
pub use host_error::{StateStoreHostError, StateStoreHostErrorKind};
pub use limits::StateStoreLimitOverrides;
pub use provider::{
    FOUNDATIONDB_STATE_STORE_PROVIDER_ID, MYSQL_STATE_STORE_PROVIDER_ID,
    SQLITE_STATE_STORE_PROVIDER_ID, StateStoreProviderRegistration, StateStoreProviderRegistry,
    builtin_state_store_provider_registry,
};
pub use runner::{
    OperationId, RunFailure, RunSuccess, derive_transaction_id, run_side_effect_free,
};
pub use runtime::StateStoreRuntime;

pub async fn open_state_store(
    runtime: &StateStoreRuntime,
    config: StateStoreConfig,
    deployment: FeDeploymentView,
) -> Result<Arc<dyn StateStore>, StateStoreError> {
    match &config.provider {
        StateStoreProviderConfig::Sqlite { .. } => {
            runtime.accepts_local()?;
            config.validate().map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "SQLite state store configuration is invalid",
                )
            })?;
            let StateStoreProviderConfig::Sqlite {
                path,
                deployment_owner,
            } = config.provider
            else {
                unreachable!()
            };
            let limits = limits::resolve_state_store_limits(
                &config.limits,
                novarocks_spi::state_store::MAX_KEY_BYTES,
            )
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "SQLite state store limits are invalid",
                )
            })?;
            let factory = sqlite::SqliteStateStoreProviderFactory::new(path, deployment_owner);
            let instance = Box::new(factory)
                .open(StateStoreOpenRequest {
                    cluster_id: config.cluster_id,
                    limits,
                    deployment: SpiFeDeploymentView {
                        active_fe_count: deployment.active_fe_count,
                        topology_revision: deployment.topology_revision,
                    },
                    deadline: Instant::now() + Duration::from_secs(30),
                })
                .await?;
            instance.state_store().ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "SQLite state store provider opened without a store",
                )
            })
        }
        StateStoreProviderConfig::Foundationdb { .. } => {
            #[cfg(not(feature = "foundationdb-provider"))]
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB provider is not compiled in",
            ));
            #[cfg(feature = "foundationdb-provider")]
            return runtime.open_foundationdb_store(&config).await;
        }
        StateStoreProviderConfig::Mysql { .. } => {
            #[cfg(not(feature = "mysql-state-store-provider"))]
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL provider is not compiled in",
            ));
            #[cfg(feature = "mysql-state-store-provider")]
            return runtime.open_mysql_store(&config, deployment).await;
        }
    }
}
