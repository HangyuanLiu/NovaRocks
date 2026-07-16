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
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mysql_async::{
    ClientIdentity, Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts, prelude::Queryable,
};
use tokio::time::{Instant, timeout_at};

use super::super::{MySqlClientConfig, MySqlTlsMode, StateStoreError, StateStoreErrorKind};
use super::error::MysqlNativeError;

pub(crate) trait PoolLifecycle: Send + Sync {
    fn get_conn<'a>(&'a self) -> BoxFuture<'a, Result<mysql_async::Conn, MysqlNativeError>>;

    fn disconnect(self: Arc<Self>) -> BoxFuture<'static, Result<(), MysqlNativeError>>;
}

pub(crate) struct MysqlAsyncPoolLifecycle {
    pool: Pool,
    connect_timeout: Duration,
}

pub(crate) struct ResolvedMysqlClient {
    config: MySqlClientConfig,
    password: MysqlPassword,
}

pub(crate) struct MysqlClientReadiness {
    pub(crate) server_version: String,
    pub(crate) innodb_page_size: u64,
    pub(crate) innodb_available: bool,
    pub(crate) sql_mode: String,
    pub(crate) time_zone: String,
    pub(crate) character_set: String,
    pub(crate) connection_id: u64,
}

struct MysqlPassword(String);

pub(crate) async fn active_readiness(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<MysqlClientReadiness, StateStoreError> {
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    let row: Option<(String, u64, u8, String, String, String, u64)> = timeout_at(
        deadline,
        connection.query_first(
            "SELECT VERSION(), @@innodb_page_size, \
             EXISTS(SELECT 1 FROM information_schema.ENGINES \
             WHERE ENGINE = 'InnoDB' AND SUPPORT IN ('YES', 'DEFAULT')), \
             @@SESSION.sql_mode, @@SESSION.time_zone, \
             @@SESSION.character_set_connection, CONNECTION_ID()",
        ),
    )
    .await
    .map_err(|_| mysql_deadline_error())?
    .map_err(|_| mysql_provider_error())?;
    let (
        server_version,
        innodb_page_size,
        innodb_available,
        sql_mode,
        time_zone,
        character_set,
        connection_id,
    ) = row.ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "MySQL readiness query returned no row",
        )
    })?;
    if server_version != "8.4.10" {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL server version is not the supported 8.4.10 release",
        ));
    }
    if innodb_page_size != 16_384 || innodb_available == 0 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL InnoDB readiness contract is not satisfied",
        ));
    }
    if !sql_mode
        .split(',')
        .any(|mode| mode.trim().starts_with("STRICT_"))
        || time_zone != "+00:00"
        || character_set != "utf8mb4"
    {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL session readiness contract is not satisfied",
        ));
    }
    Ok(MysqlClientReadiness {
        server_version,
        innodb_page_size,
        innodb_available: true,
        sql_mode,
        time_zone,
        character_set,
        connection_id,
    })
}

pub(crate) async fn pollute_session(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut connection = checkout_connection(pool, deadline).await?;
    timeout_at(
        deadline,
        connection.query_drop(
            "SET SESSION time_zone = '+05:00', SESSION sql_mode = '', \
             SESSION character_set_connection = 'latin1'",
        ),
    )
    .await
    .map_err(|_| mysql_deadline_error())?
    .map_err(|_| mysql_provider_error())
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) async fn run_sleep_until_deadline(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    match timeout_at(deadline, connection.query_drop("SELECT SLEEP(10)")).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(mysql_provider_error()),
        Err(_) => {
            let _ = tokio::time::timeout(Duration::from_secs(1), connection.disconnect()).await;
            Err(mysql_deadline_error())
        }
    }
}

pub(crate) async fn checkout_hygienic_connection(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<Conn, StateStoreError> {
    let mut connection = checkout_connection(pool, deadline).await?;
    timeout_at(deadline, apply_session_hygiene(&mut connection))
        .await
        .map_err(|_| mysql_deadline_error())??;
    Ok(connection)
}

async fn checkout_connection(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<Conn, StateStoreError> {
    timeout_at(deadline, pool.get_conn())
        .await
        .map_err(|_| mysql_deadline_error())?
        .map_err(MysqlNativeError::into_public)
}

async fn apply_session_hygiene(connection: &mut Conn) -> Result<(), StateStoreError> {
    connection
        .query_drop(
            "SET SESSION time_zone = '+00:00', \
             SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION', \
             SESSION character_set_client = 'utf8mb4', \
             SESSION character_set_connection = 'utf8mb4', \
             SESSION character_set_results = 'utf8mb4'",
        )
        .await
        .map_err(|_| mysql_provider_error())
}

fn mysql_provider_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL provider operation failed",
    )
}

fn mysql_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL provider operation exceeded its deadline",
    )
}

impl ResolvedMysqlClient {
    pub(crate) fn resolve(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL client configuration is invalid",
            )
        })?;
        let password = std::env::var_os(&config.password_env).ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL password environment value is missing",
            )
        })?;
        let password = password.into_string().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL password environment value is not valid UTF-8",
            )
        })?;
        if password.is_empty() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL password environment value is empty",
            ));
        }
        Ok(Self {
            config,
            password: MysqlPassword(password),
        })
    }

    pub(crate) fn build_pool(
        &self,
        database: &str,
    ) -> Result<Arc<dyn PoolLifecycle>, StateStoreError> {
        let constraints = PoolConstraints::new(self.config.pool_min, self.config.pool_max)
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "MySQL pool constraints are invalid",
                )
            })?;
        let pool_opts = PoolOpts::new()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(Duration::from_millis(
                self.config.inactive_connection_ttl_ms,
            ))
            .with_reset_connection(true);
        let ssl_opts = self.ssl_opts();
        let opts = OptsBuilder::default()
            .ip_or_hostname(self.config.host.clone())
            .tcp_port(self.config.port)
            .user(Some(self.config.username.clone()))
            .pass(Some(self.password.0.clone()))
            .db_name(Some(database.to_owned()))
            .prefer_socket(false)
            .pool_opts(pool_opts)
            .ssl_opts(ssl_opts);
        Ok(Arc::new(MysqlAsyncPoolLifecycle {
            pool: Pool::new(opts),
            connect_timeout: Duration::from_millis(self.config.connect_timeout_ms),
        }))
    }

    fn ssl_opts(&self) -> Option<SslOpts> {
        match self.config.tls_mode {
            MySqlTlsMode::Disabled => None,
            MySqlTlsMode::Required => Some(
                SslOpts::default()
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true),
            ),
            MySqlTlsMode::VerifyIdentity => {
                let mut ssl = SslOpts::default().with_disable_built_in_roots(true);
                if let Some(path) = self.config.tls_ca_path.clone() {
                    ssl = ssl.with_root_certs(vec![path.into()]);
                }
                if let (Some(cert), Some(key)) = (
                    self.config.tls_cert_path.clone(),
                    self.config.tls_key_path.clone(),
                ) {
                    ssl = ssl
                        .with_client_identity(Some(ClientIdentity::new(cert.into(), key.into())));
                }
                Some(ssl)
            }
        }
    }
}

impl PoolLifecycle for MysqlAsyncPoolLifecycle {
    fn get_conn<'a>(&'a self) -> BoxFuture<'a, Result<mysql_async::Conn, MysqlNativeError>> {
        Box::pin(async move {
            tokio::time::timeout(self.connect_timeout, self.pool.get_conn())
                .await
                .map_err(|_| MysqlNativeError::deadline())?
                .map_err(MysqlNativeError::from)
        })
    }

    fn disconnect(self: Arc<Self>) -> BoxFuture<'static, Result<(), MysqlNativeError>> {
        Box::pin(async move {
            self.pool
                .clone()
                .disconnect()
                .await
                .map_err(MysqlNativeError::from)
        })
    }
}

impl fmt::Debug for ResolvedMysqlClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedMysqlClient")
            .field("client_configured", &true)
            .field(
                "tls_enabled",
                &(self.config.tls_mode != MySqlTlsMode::Disabled),
            )
            .field("password_resolved", &true)
            .finish()
    }
}

impl fmt::Debug for MysqlPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MysqlPassword([REDACTED])")
    }
}
