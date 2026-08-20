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
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoundationDbProviderConfig {
    pub cluster_file: PathBuf,
    pub keyspace_id: Uuid,
}

impl FoundationDbProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_readable_file(&self.cluster_file, "cluster_file")
    }
}

#[derive(Clone, Eq, PartialEq)]
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
