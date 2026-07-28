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

pub mod config;
pub mod coordination;
mod host;
pub mod host_error;
pub mod limits;
pub mod metrics;
pub mod provider;
pub mod runner;

mod sqlite;

#[cfg(feature = "foundationdb-provider")]
mod foundationdb;

#[cfg(feature = "mysql-state-store-provider")]
pub mod mysql;

#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[doc(hidden)]
pub use foundationdb::provider::FoundationDbProviderTestHarness;
#[cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]
#[doc(hidden)]
pub use foundationdb::test_support::{FoundationDbCommitGateControl, arm_next_foundationdb_commit};

pub use config::{
    FoundationDbClientConfig, MySqlClientConfig, MySqlTlsMode, StateStoreAppConfig,
    StateStoreConfig, StateStoreHostConfig, StateStoreProviderConfig,
};
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
