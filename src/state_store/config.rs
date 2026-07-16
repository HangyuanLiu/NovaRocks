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
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::{Deserialize, Deserializer, de::Error as _};
use uuid::Uuid;

use super::limits::{StateStoreLimitOverrides, StateStoreLimits};

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
    #[serde(default)]
    limits: StateStoreLimitOverrides,
}

impl<'de> Deserialize<'de> for StateStoreConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StateStoreConfigWire::deserialize(deserializer)?;
        let provider = match wire.provider {
            StateStoreProviderKind::Sqlite => {
                if wire.cluster_file.is_some() || wire.keyspace_id.is_some() {
                    return Err(D::Error::custom(
                        "FoundationDB fields are not valid for the sqlite state store provider",
                    ));
                }
                StateStoreProviderConfig::Sqlite {
                    path: wire.path.ok_or_else(|| D::Error::missing_field("path"))?,
                    deployment_owner: wire
                        .deployment_owner
                        .ok_or_else(|| D::Error::missing_field("deployment_owner"))?,
                }
            }
            StateStoreProviderKind::Foundationdb => {
                if wire.path.is_some() || wire.deployment_owner.is_some() {
                    return Err(D::Error::custom(
                        "SQLite fields are not valid for the foundationdb state store provider",
                    ));
                }
                StateStoreProviderConfig::Foundationdb {
                    cluster_file: wire
                        .cluster_file
                        .ok_or_else(|| D::Error::missing_field("cluster_file"))?,
                    keyspace_id: wire
                        .keyspace_id
                        .ok_or_else(|| D::Error::missing_field("keyspace_id"))?,
                }
            }
        };

        Ok(Self {
            cluster_id: wire.cluster_id,
            limits: wire.limits,
            provider,
        })
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
            }
            StateStoreProviderConfig::Foundationdb { cluster_file, .. } => {
                validate_readable_file(cluster_file, "cluster_file")?;
            }
        }
        StateStoreLimits::from_overrides(&self.limits)?;
        Ok(())
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
