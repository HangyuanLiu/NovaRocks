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

use super::super::{StateStoreError, StateStoreErrorKind, StateStoreRuntime};
use super::client::MysqlPoolConnection;
use std::time::Duration;

#[cfg(feature = "state-store-test-hooks")]
pub use super::open_test_hooks::{MysqlOpenGateControl, MysqlOpenGatePhase, arm_mysql_open_gate};
pub use super::schema::{
    SchemaColumnSnapshot as MysqlSchemaColumnSnapshot, SchemaMutation as MysqlSchemaMutation,
    SchemaSnapshot as MysqlSchemaSnapshot, SchemaTableSnapshot as MysqlSchemaTableSnapshot,
    StoreReadinessSnapshot as MysqlStoreReadinessSnapshot,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlRuntimeOwner {
    pub pid: u32,
    pub tokio_runtime_id: tokio::runtime::Id,
}

pub struct MysqlTestHandle {
    dropper: Option<Box<dyn FnOnce() + Send>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MysqlReadinessSnapshot {
    pub server_version: String,
    pub innodb_page_size: u64,
    pub innodb_available: bool,
    pub default_storage_engine: String,
    pub sql_mode: String,
    pub time_zone: String,
    pub character_set: String,
    pub connection_id: u64,
}

pub struct MysqlHeldConnection {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlTestHandle>,
}

pub struct MysqlHeldAdvisoryLock {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlTestHandle>,
    lock_name: String,
}

pub fn runtime_owner(runtime: &StateStoreRuntime) -> Result<MysqlRuntimeOwner, StateStoreError> {
    runtime.mysql_test_owner()
}

pub fn validate_owner(runtime: &StateStoreRuntime, pid: u32) -> Result<(), StateStoreError> {
    runtime.mysql_test_validate_owner(pid)
}

pub async fn prepare_pool(
    runtime: &StateStoreRuntime,
    database: &str,
) -> Result<(), StateStoreError> {
    runtime.mysql_test_prepare_pool(database).await
}

pub fn pool_count(runtime: &StateStoreRuntime) -> Result<usize, StateStoreError> {
    runtime.mysql_test_pool_count()
}

pub fn acquire_provider_handle(
    runtime: &StateStoreRuntime,
) -> Result<MysqlTestHandle, StateStoreError> {
    runtime.mysql_test_acquire_provider_handle()
}

pub fn acquire_operation(runtime: &StateStoreRuntime) -> Result<MysqlTestHandle, StateStoreError> {
    runtime.mysql_test_acquire_operation()
}

pub fn is_accepting(runtime: &StateStoreRuntime) -> Result<bool, StateStoreError> {
    runtime.mysql_test_is_accepting()
}

pub fn begin_shutdown(runtime: &StateStoreRuntime) -> Result<(), StateStoreError> {
    runtime.mysql_test_begin_shutdown()
}

pub async fn active_readiness(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<MysqlReadinessSnapshot, StateStoreError> {
    runtime
        .mysql_test_active_readiness(database, deadline)
        .await
}

#[cfg(feature = "state-store-test-hooks")]
pub async fn delayed_active_readiness(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<MysqlReadinessSnapshot, StateStoreError> {
    runtime
        .mysql_test_delayed_active_readiness(database, deadline)
        .await
}

pub async fn pollute_session(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<(), StateStoreError> {
    runtime.mysql_test_pollute_session(database, deadline).await
}

pub async fn hold_connection(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<MysqlHeldConnection, StateStoreError> {
    runtime.mysql_test_hold_connection(database, deadline).await
}

pub async fn restart_mysql_fixture() -> Result<(), StateStoreError> {
    let compose_project = std::env::var_os("NOVA_MYSQL_COMPOSE_PROJECT").ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL fixture compose project is missing",
        )
    })?;
    let compose_file = std::env::var_os("NOVA_MYSQL_COMPOSE_FILE").ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL fixture compose file is missing",
        )
    })?;
    let compose_env = std::env::var_os("NOVA_MYSQL_COMPOSE_ENV").ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL fixture compose environment is missing",
        )
    })?;
    tokio::task::spawn_blocking(move || {
        let restarted = std::process::Command::new("docker")
            .args(["compose", "--env-file"])
            .arg(compose_env)
            .arg("-p")
            .arg(compose_project)
            .arg("-f")
            .arg(compose_file)
            .args(["restart", "mysql"])
            .status()
            .map_err(|_| fixture_control_error())?;
        if !restarted.success() {
            return Err(fixture_control_error());
        }

        let status_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("docker/mysql-state-store/status.sh");
        for _ in 0..120 {
            if std::process::Command::new(&status_script)
                .status()
                .is_ok_and(|status| status.success())
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        Err(StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "MySQL fixture did not become ready after restart",
        ))
    })
    .await
    .map_err(|_| fixture_control_error())?
}

pub fn advisory_lock_name(database: &str) -> String {
    super::identity::advisory_lock_name(database)
}

pub async fn schema_snapshot(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<MysqlSchemaSnapshot, StateStoreError> {
    runtime.mysql_test_schema_snapshot(database, deadline).await
}

pub async fn apply_schema_mutation(
    runtime: &StateStoreRuntime,
    database: &str,
    mutation: MysqlSchemaMutation,
    deadline: Duration,
) -> Result<(), StateStoreError> {
    runtime
        .mysql_test_apply_schema_mutation(database, mutation, deadline)
        .await
}

pub async fn acquire_schema_advisory_lock(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<MysqlHeldAdvisoryLock, StateStoreError> {
    runtime
        .mysql_test_acquire_schema_advisory_lock(database, deadline)
        .await
}

pub async fn is_schema_advisory_lock_free(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<bool, StateStoreError> {
    runtime
        .mysql_test_is_schema_advisory_lock_free(database, deadline)
        .await
}

pub async fn store_readiness_snapshot(
    runtime: &StateStoreRuntime,
    database: &str,
    cluster_id: &str,
    deadline: Duration,
) -> Result<MysqlStoreReadinessSnapshot, StateStoreError> {
    runtime
        .mysql_test_store_readiness_snapshot(database, cluster_id, deadline)
        .await
}

pub async fn schema_timeout_connection_is_destroyed(
    runtime: &StateStoreRuntime,
    database: &str,
    timeout_deadline: Duration,
    checkout_deadline: Duration,
) -> Result<bool, StateStoreError> {
    runtime
        .mysql_test_schema_timeout_connection_is_destroyed(
            database,
            timeout_deadline,
            checkout_deadline,
        )
        .await
}

#[cfg(feature = "state-store-test-hooks")]
pub async fn run_sleep_until_deadline(
    runtime: &StateStoreRuntime,
    database: &str,
    deadline: Duration,
) -> Result<(), StateStoreError> {
    runtime
        .mysql_test_run_sleep_until_deadline(database, deadline)
        .await
}

fn fixture_control_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL fixture control command failed",
    )
}

impl MysqlTestHandle {
    pub(crate) fn new(dropper: impl FnOnce() + Send + 'static) -> Self {
        Self {
            dropper: Some(Box::new(dropper)),
        }
    }
}

impl std::fmt::Debug for MysqlTestHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlTestHandle")
    }
}

impl Drop for MysqlTestHandle {
    fn drop(&mut self) {
        if let Some(dropper) = self.dropper.take() {
            dropper();
        }
    }
}

impl std::fmt::Debug for MysqlHeldConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlHeldConnection")
    }
}

impl MysqlHeldConnection {
    pub(crate) fn new(connection: MysqlPoolConnection, operation: MysqlTestHandle) -> Self {
        Self {
            connection: Some(connection),
            operation: Some(operation),
        }
    }
}

impl Drop for MysqlHeldConnection {
    fn drop(&mut self) {
        drop(self.connection.take());
        drop(self.operation.take());
    }
}

impl MysqlHeldAdvisoryLock {
    pub(crate) fn new(
        connection: MysqlPoolConnection,
        operation: MysqlTestHandle,
        lock_name: String,
    ) -> Self {
        Self {
            connection: Some(connection),
            operation: Some(operation),
            lock_name,
        }
    }

    pub async fn release(mut self, deadline: Duration) -> Result<(), StateStoreError> {
        let connection = self.connection.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL advisory lock connection is missing",
            )
        })?;
        let result = super::schema::release_lock_for_test(
            connection,
            &self.lock_name,
            tokio::time::Instant::now() + deadline,
        )
        .await;
        drop(self.operation.take());
        result
    }
}

impl std::fmt::Debug for MysqlHeldAdvisoryLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlHeldAdvisoryLock")
    }
}

impl Drop for MysqlHeldAdvisoryLock {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            tokio::spawn(connection.destroy());
        }
        drop(self.operation.take());
    }
}
