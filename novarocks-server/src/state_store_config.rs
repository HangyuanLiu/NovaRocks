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
use std::fs::File;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use novarocks_secret::SecretValue;
use uuid::Uuid;

use crate::state_store_limits::{
    MYSQL_MAX_KEY_BYTES, MYSQL_MAX_META_VALUE_BYTES, StateStoreLimitOverrides,
    resolve_state_store_limits,
};
use novarocks_spi::state_store::MAX_KEY_BYTES;
use novarocks_spi::state_store::StateStoreProviderId;

const MYSQL_MAX_CONNECT_TIMEOUT_MS: u64 = 60_000;
const MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS: u64 = 86_400_000;
pub const SQLITE_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("sqlite");
pub const MYSQL_STATE_STORE_PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("mysql");
pub const FOUNDATIONDB_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("foundationdb");

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateStoreProviderConfig {
    Sqlite {
        path: PathBuf,
        deployment_owner: String,
    },
    Foundationdb {
        cluster_file: PathBuf,
        keyspace_id: Uuid,
    },
    Mysql {
        database: String,
    },
}

impl StateStoreProviderConfig {
    pub const fn provider_id(&self) -> StateStoreProviderId {
        match self {
            Self::Sqlite { .. } => SQLITE_STATE_STORE_PROVIDER_ID,
            Self::Foundationdb { .. } => FOUNDATIONDB_STATE_STORE_PROVIDER_ID,
            Self::Mysql { .. } => MYSQL_STATE_STORE_PROVIDER_ID,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreConfig {
    pub cluster_id: String,
    pub limits: StateStoreLimitOverrides,
    pub provider: StateStoreProviderConfig,
}

impl StateStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id.trim().is_empty() {
            bail!("InvalidStateStoreConfig: cluster_id must not be empty");
        }
        match &self.provider {
            StateStoreProviderConfig::Sqlite {
                path,
                deployment_owner,
            } => {
                if path.as_os_str().is_empty() {
                    bail!("InvalidStateStoreConfig: path must not be empty");
                }
                if deployment_owner.trim().is_empty() {
                    bail!("InvalidStateStoreConfig: deployment_owner must not be empty");
                }
                resolve_state_store_limits(&self.limits, MAX_KEY_BYTES)?;
            }
            StateStoreProviderConfig::Foundationdb { cluster_file, .. } => {
                validate_readable_file(cluster_file, "cluster_file")?;
                resolve_state_store_limits(&self.limits, MAX_KEY_BYTES)?;
            }
            StateStoreProviderConfig::Mysql { database } => {
                if self.cluster_id.len() > MYSQL_MAX_META_VALUE_BYTES {
                    bail!(
                        "InvalidStateStoreConfig: MySQL cluster_id exceeds the physical meta value limit"
                    );
                }
                if database.is_empty()
                    || database.len() > 64
                    || !database
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                {
                    bail!(
                        "InvalidStateStoreConfig: database must match ASCII [A-Za-z0-9_]{{1,64}}"
                    );
                }
                resolve_state_store_limits(&self.limits, MYSQL_MAX_KEY_BYTES)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MySqlTlsMode {
    Disabled,
    Required,
    VerifyIdentity,
}

#[derive(Clone, Eq, PartialEq)]
pub struct MySqlClientConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: SecretValue,
    pub tls_mode: MySqlTlsMode,
    pub tls_ca_path: Option<PathBuf>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub connect_timeout_ms: u64,
    pub pool_min: usize,
    pub pool_max: usize,
    pub inactive_connection_ttl_ms: u64,
}

impl MySqlClientConfig {
    pub fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            bail!("InvalidStateStoreConfig: mysql_client.host must not be empty");
        }
        if self.port == 0 {
            bail!("InvalidStateStoreConfig: mysql_client.port must be non-zero");
        }
        if self.username.trim().is_empty() {
            bail!("InvalidStateStoreConfig: mysql_client.username must not be empty");
        }
        if self.password.is_empty() {
            bail!("InvalidStateStoreConfig: mysql_client.password must not be empty");
        }
        if self.connect_timeout_ms == 0 || self.connect_timeout_ms > MYSQL_MAX_CONNECT_TIMEOUT_MS {
            bail!(
                "InvalidStateStoreConfig: mysql_client.connect_timeout_ms must be between 1 and {MYSQL_MAX_CONNECT_TIMEOUT_MS}"
            );
        }
        if self.pool_min == 0 || self.pool_min > self.pool_max {
            bail!("InvalidStateStoreConfig: mysql_client.pool_min must be between 1 and pool_max");
        }
        if self.inactive_connection_ttl_ms == 0
            || self.inactive_connection_ttl_ms > MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS
        {
            bail!(
                "InvalidStateStoreConfig: mysql_client.inactive_connection_ttl_ms must be between 1 and {MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS}"
            );
        }

        if self.tls_cert_path.is_some() != self.tls_key_path.is_some() {
            let missing = if self.tls_cert_path.is_none() {
                "tls_cert_path"
            } else {
                "tls_key_path"
            };
            bail!(
                "InvalidStateStoreConfig: mysql_client.{missing} is required when the matching client TLS path is configured"
            );
        }
        if self.tls_mode == MySqlTlsMode::VerifyIdentity {
            if self.tls_ca_path.is_none() {
                bail!(
                    "InvalidStateStoreConfig: mysql_client.tls_ca_path is required for verify_identity"
                );
            }
            if mysql_host_is_ip_address(&self.host) {
                bail!(
                    "InvalidStateStoreConfig: mysql_client.host must be a DNS hostname for verify_identity"
                );
            }
        }
        for (name, path) in [
            ("tls_ca_path", self.tls_ca_path.as_deref()),
            ("tls_cert_path", self.tls_cert_path.as_deref()),
            ("tls_key_path", self.tls_key_path.as_deref()),
        ] {
            if let Some(path) = path {
                validate_readable_file(path, &format!("mysql_client.{name}"))?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for MySqlClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MySqlClientConfig")
            .field("host_configured", &!self.host.trim().is_empty())
            .field("port_configured", &(self.port != 0))
            .field("username_configured", &!self.username.trim().is_empty())
            .field("password_configured", &!self.password.is_empty())
            .field("tls_enabled", &(self.tls_mode != MySqlTlsMode::Disabled))
            .field(
                "tls_verify_identity",
                &(self.tls_mode == MySqlTlsMode::VerifyIdentity),
            )
            .field("tls_ca_path_configured", &self.tls_ca_path.is_some())
            .field("tls_cert_path_configured", &self.tls_cert_path.is_some())
            .field("tls_key_path_configured", &self.tls_key_path.is_some())
            .field(
                "connect_timeout_configured",
                &(self.connect_timeout_ms != 0),
            )
            .field(
                "pool_bounds_configured",
                &(self.pool_min != 0 && self.pool_max != 0),
            )
            .field(
                "inactive_connection_ttl_configured",
                &(self.inactive_connection_ttl_ms != 0),
            )
            .finish()
    }
}

fn mysql_host_is_ip_address(host: &str) -> bool {
    let normalized = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    normalized.parse::<IpAddr>().is_ok()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreAppConfig {
    pub store: StateStoreConfig,
    pub mysql_client: Option<MySqlClientConfig>,
}

impl StateStoreAppConfig {
    pub fn validate(&self) -> Result<()> {
        self.store.validate()?;
        match (&self.store.provider, &self.mysql_client) {
            (StateStoreProviderConfig::Mysql { .. }, Some(client)) => client.validate(),
            (StateStoreProviderConfig::Mysql { .. }, None) => {
                bail!("InvalidStateStoreConfig: mysql provider requires [state_store.mysql_client]")
            }
            (_, None) => Ok(()),
            (_, Some(_)) => bail!(
                "InvalidStateStoreConfig: [state_store.mysql_client] requires the mysql state store provider"
            ),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct FoundationDbClientConfig {
    pub disable_multi_version_client: bool,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub tls_ca_path: Option<PathBuf>,
    pub tls_verify_peers: Option<String>,
    pub tls_password: Option<SecretValue>,
}

impl FoundationDbClientConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.disable_multi_version_client {
            bail!(
                "InvalidStateStoreConfig: foundationdb_client.disable_multi_version_client must be true"
            );
        }

        let tls_configured = self.tls_cert_path.is_some()
            || self.tls_key_path.is_some()
            || self.tls_ca_path.is_some()
            || self.tls_verify_peers.is_some()
            || self.tls_password.is_some();
        if tls_configured
            && (self.tls_cert_path.is_none()
                || self.tls_key_path.is_none()
                || self.tls_ca_path.is_none()
                || self.tls_verify_peers.is_none())
        {
            bail!(
                "InvalidStateStoreConfig: FoundationDB TLS cert, key, CA, and verify peers must be configured together"
            );
        }

        for (name, path) in [
            ("tls_cert_path", self.tls_cert_path.as_deref()),
            ("tls_key_path", self.tls_key_path.as_deref()),
            ("tls_ca_path", self.tls_ca_path.as_deref()),
        ] {
            if let Some(path) = path {
                validate_readable_file(path, name)?;
            }
        }
        if self
            .tls_verify_peers
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            bail!("InvalidStateStoreConfig: tls_verify_peers must not be empty");
        }
        if self
            .tls_password
            .as_ref()
            .is_some_and(SecretValue::is_empty)
        {
            bail!("InvalidStateStoreConfig: tls_password must not be empty");
        }

        Ok(())
    }
}

impl fmt::Debug for FoundationDbClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FoundationDbClientConfig")
            .field(
                "disable_multi_version_client",
                &self.disable_multi_version_client,
            )
            .field("tls_cert_path_configured", &self.tls_cert_path.is_some())
            .field("tls_key_path_configured", &self.tls_key_path.is_some())
            .field("tls_ca_path_configured", &self.tls_ca_path.is_some())
            .field(
                "tls_verify_peers_configured",
                &self.tls_verify_peers.is_some(),
            )
            .field("tls_password_configured", &self.tls_password.is_some())
            .finish()
    }
}

fn validate_readable_file(path: &Path, name: &str) -> Result<()> {
    if path.as_os_str().is_empty() {
        bail!("InvalidStateStoreConfig: {name} must not be empty");
    }
    let path_text = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("InvalidStateStoreConfig: {name} must be valid UTF-8"))?;
    if path_text.contains('\0') {
        bail!("InvalidStateStoreConfig: {name} must not contain NUL");
    }
    let metadata = std::fs::metadata(path)
        .map_err(|_| anyhow::anyhow!("InvalidStateStoreConfig: {name} must exist"))?;
    if !metadata.is_file() {
        bail!("InvalidStateStoreConfig: {name} must be a regular file");
    }
    File::open(path)
        .map_err(|_| anyhow::anyhow!("InvalidStateStoreConfig: {name} must be readable"))?;
    Ok(())
}
