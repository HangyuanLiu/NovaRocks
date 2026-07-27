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

pub mod config;
pub mod coordination;
mod deployment;
pub mod limits;
pub mod metrics;
pub mod runner;
mod runtime;

use novarocks_spi::state_store::{StateStore, StateStoreError, StateStoreErrorKind};

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
    StateStoreConfig, StateStoreProviderConfig,
};
pub use deployment::FeDeploymentView;
pub use limits::StateStoreLimitOverrides;
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
            Ok(Arc::new(
                sqlite::SqliteStateStore::open(config, deployment).await?,
            ))
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
