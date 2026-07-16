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

use std::path::PathBuf;

use anyhow::{Result, bail};
use serde::Deserialize;

use super::limits::{StateStoreLimitOverrides, StateStoreLimits};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StateStoreProviderConfig {
    Sqlite,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StateStoreConfig {
    pub provider: StateStoreProviderConfig,
    pub path: PathBuf,
    pub cluster_id: String,
    pub deployment_owner: String,
    #[serde(default)]
    pub limits: StateStoreLimitOverrides,
}

impl StateStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.path.as_os_str().is_empty() {
            bail!("InvalidStateStoreConfig: path must not be empty");
        }
        if self.cluster_id.trim().is_empty() {
            bail!("InvalidStateStoreConfig: cluster_id must not be empty");
        }
        if self.deployment_owner.trim().is_empty() {
            bail!("InvalidStateStoreConfig: deployment_owner must not be empty");
        }
        StateStoreLimits::from_overrides(&self.limits)?;
        Ok(())
    }
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
