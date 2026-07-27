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
use serde::{Deserialize, Deserializer};
use uuid::Uuid;

use super::limits::{
    MYSQL_MAX_KEY_BYTES, MYSQL_MAX_META_VALUE_BYTES, StateStoreLimitOverrides,
    resolve_state_store_limits,
};
use novarocks_spi::state_store::MAX_KEY_BYTES;

const MYSQL_MAX_CONNECT_TIMEOUT_MS: u64 = 60_000;
const MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS: u64 = 86_400_000;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreConfig {
    pub cluster_id: String,
    pub limits: StateStoreLimitOverrides,
    pub provider: StateStoreProviderConfig,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StateStoreProviderKind {
    Sqlite,
    Foundationdb,
    Mysql,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreConfigWire {
    provider: StateStoreProviderKind,
    cluster_id: String,
    path: Option<PathBuf>,
    deployment_owner: Option<String>,
    cluster_file: Option<PathBuf>,
    keyspace_id: Option<Uuid>,
    database: Option<String>,
    #[serde(default)]
    limits: StateStoreLimitOverrides,
}

fn state_store_config_from_wire<E>(
    wire: StateStoreConfigWire,
) -> std::result::Result<StateStoreConfig, E>
where
    E: serde::de::Error,
{
    let provider = match wire.provider {
        StateStoreProviderKind::Sqlite => {
            if wire.cluster_file.is_some() || wire.keyspace_id.is_some() || wire.database.is_some()
            {
                return Err(E::custom(
                    "non-SQLite fields are not valid for the sqlite state store provider",
                ));
            }
            StateStoreProviderConfig::Sqlite {
                path: wire.path.ok_or_else(|| E::missing_field("path"))?,
                deployment_owner: wire
                    .deployment_owner
                    .ok_or_else(|| E::missing_field("deployment_owner"))?,
            }
        }
        StateStoreProviderKind::Foundationdb => {
            if wire.path.is_some() || wire.deployment_owner.is_some() || wire.database.is_some() {
                return Err(E::custom(
                    "non-FoundationDB fields are not valid for the foundationdb state store provider",
                ));
            }
            StateStoreProviderConfig::Foundationdb {
                cluster_file: wire
                    .cluster_file
                    .ok_or_else(|| E::missing_field("cluster_file"))?,
                keyspace_id: wire
                    .keyspace_id
                    .ok_or_else(|| E::missing_field("keyspace_id"))?,
            }
        }
        StateStoreProviderKind::Mysql => {
            if wire.path.is_some()
                || wire.deployment_owner.is_some()
                || wire.cluster_file.is_some()
                || wire.keyspace_id.is_some()
            {
                return Err(E::custom(
                    "non-MySQL fields are not valid for the mysql state store provider",
                ));
            }
            StateStoreProviderConfig::Mysql {
                database: wire.database.ok_or_else(|| E::missing_field("database"))?,
            }
        }
    };

    Ok(StateStoreConfig {
        cluster_id: wire.cluster_id,
        limits: wire.limits,
        provider,
    })
}

impl<'de> Deserialize<'de> for StateStoreConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        state_store_config_from_wire(StateStoreConfigWire::deserialize(deserializer)?)
    }
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MySqlTlsMode {
    Disabled,
    Required,
    VerifyIdentity,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MySqlClientConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password_env: String,
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
        if !valid_environment_variable_name(&self.password_env) {
            bail!(
                "InvalidStateStoreConfig: mysql_client.password_env must be a non-empty environment variable name"
            );
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
            .field(
                "password_env_configured",
                &valid_environment_variable_name(&self.password_env),
            )
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

fn valid_environment_variable_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreAppConfig {
    pub store: StateStoreConfig,
    pub mysql_client: Option<MySqlClientConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreAppConfigWire {
    provider: StateStoreProviderKind,
    cluster_id: String,
    path: Option<PathBuf>,
    deployment_owner: Option<String>,
    cluster_file: Option<PathBuf>,
    keyspace_id: Option<Uuid>,
    database: Option<String>,
    #[serde(default)]
    limits: StateStoreLimitOverrides,
    mysql_client: Option<MySqlClientConfig>,
}

impl<'de> Deserialize<'de> for StateStoreAppConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StateStoreAppConfigWire::deserialize(deserializer)?;
        let mysql_client = wire.mysql_client;
        let store = state_store_config_from_wire(StateStoreConfigWire {
            provider: wire.provider,
            cluster_id: wire.cluster_id,
            path: wire.path,
            deployment_owner: wire.deployment_owner,
            cluster_file: wire.cluster_file,
            keyspace_id: wire.keyspace_id,
            database: wire.database,
            limits: wire.limits,
        })?;
        Ok(Self {
            store,
            mysql_client,
        })
    }
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

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FoundationDbClientConfig {
    pub disable_multi_version_client: bool,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub tls_ca_path: Option<PathBuf>,
    pub tls_verify_peers: Option<String>,
    pub tls_password_env: Option<String>,
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
            || self.tls_password_env.is_some();
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
        if let Some(variable) = self.tls_password_env.as_deref() {
            if variable.trim().is_empty() {
                bail!("InvalidStateStoreConfig: tls_password_env must not be empty");
            }
            let value = std::env::var_os(variable).ok_or_else(|| {
                anyhow::anyhow!("InvalidStateStoreConfig: tls_password_env variable is not defined")
            })?;
            if value.is_empty() {
                bail!("InvalidStateStoreConfig: tls_password_env variable must not be empty");
            }
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
            .field(
                "tls_password_env_configured",
                &self.tls_password_env.is_some(),
            )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_store_config_rejects_unknown_fields() {
        let error = toml::from_str::<StateStoreConfig>(
            r#"
provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
deployment_owner = "fe-a"
fallback_to_metadata = true
"#,
        )
        .expect_err("unknown state store config keys must fail closed");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn state_store_config_rejects_empty_identity_fields() {
        for (field, input) in [
            (
                "path",
                r#"provider = "sqlite"
path = ""
cluster_id = "cluster-a"
deployment_owner = "fe-a""#,
            ),
            (
                "cluster_id",
                r#"provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = " "
deployment_owner = "fe-a""#,
            ),
            (
                "deployment_owner",
                r#"provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
deployment_owner = " ""#,
            ),
        ] {
            let config: StateStoreConfig = toml::from_str(input).expect("parse fixture");
            let error = config
                .validate()
                .expect_err("empty fields must fail closed");
            assert!(
                error.to_string().contains(field),
                "wrong error for {field}: {error}"
            );
        }
    }
}
