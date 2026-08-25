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
use novarocks_secret::SecretValue;
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

#[cfg(test)]
mod tests {
    use super::FoundationDbClientConfig;
    use novarocks_secret::SecretValue;
    use tempfile::TempDir;

    fn configured_tls_client(password: Option<SecretValue>) -> (FoundationDbClientConfig, TempDir) {
        let tls = TempDir::new().expect("TLS config temp dir");
        let cert = tls.path().join("client.crt");
        let key = tls.path().join("client.key");
        let ca = tls.path().join("ca.crt");
        std::fs::write(&cert, b"cert").expect("write cert fixture");
        std::fs::write(&key, b"key").expect("write key fixture");
        std::fs::write(&ca, b"ca").expect("write CA fixture");

        (
            FoundationDbClientConfig {
                disable_multi_version_client: true,
                tls_cert_path: Some(cert),
                tls_key_path: Some(key),
                tls_ca_path: Some(ca),
                tls_verify_peers: Some("Check.Valid=1".to_owned()),
                tls_password: password,
            },
            tls,
        )
    }

    #[test]
    fn direct_empty_tls_password_fails_closed_without_disclosing_it() {
        let (config, _tls) = configured_tls_client(Some(SecretValue::new("")));

        let error = config.validate().expect_err("empty TLS password must fail");

        assert_eq!(
            error.to_string(),
            "InvalidStateStoreConfig: tls_password must not be empty"
        );
    }

    #[test]
    fn client_config_debug_redacts_direct_tls_password() {
        let canary = "nwt-1-fdb-tls-password-canary";
        let (config, _tls) = configured_tls_client(Some(SecretValue::new(canary)));

        let debug = format!("{config:?}");

        assert!(debug.contains("tls_password_configured: true"));
        assert!(!debug.contains(canary));
    }

    #[test]
    fn invalid_tls_configuration_error_redacts_direct_tls_password() {
        let canary = "nwt-1-fdb-tls-password-canary";
        let config = FoundationDbClientConfig {
            disable_multi_version_client: true,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_verify_peers: None,
            tls_password: Some(SecretValue::new(canary)),
        };

        let error = config
            .validate()
            .expect_err("incomplete TLS configuration must fail");

        assert!(!error.to_string().contains(canary));
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
