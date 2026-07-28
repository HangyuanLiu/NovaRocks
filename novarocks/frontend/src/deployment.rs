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
use std::num::NonZeroUsize;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_state_store::{StateStoreConfig, StateStoreProviderConfig};
use sha2::{Digest, Sha256};

const REVISION_DOMAIN: &[u8] = b"novarocks/fe-deployment-view/v1";
const SQLITE_SINGLE_FE_COUNT: NonZeroUsize = NonZeroUsize::MIN;

#[async_trait]
pub trait FeDeploymentViewSource: Send + Sync {
    async fn snapshot(&self) -> Result<FeDeploymentView, FeDeploymentViewSourceError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeDeploymentViewSourceErrorKind {
    UnsupportedProvider,
    InvalidConfiguration,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeDeploymentViewSourceError {
    kind: FeDeploymentViewSourceErrorKind,
    message: &'static str,
}

impl FeDeploymentViewSourceError {
    const fn new(kind: FeDeploymentViewSourceErrorKind, message: &'static str) -> Self {
        Self { kind, message }
    }

    pub const fn kind(&self) -> FeDeploymentViewSourceErrorKind {
        self.kind
    }
}

impl fmt::Display for FeDeploymentViewSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for FeDeploymentViewSourceError {}

pub struct SqliteSingleFeDeploymentViewSource {
    snapshot: FeDeploymentView,
}

impl SqliteSingleFeDeploymentViewSource {
    pub fn try_from_state_store_config(
        config: &StateStoreConfig,
    ) -> Result<Self, FeDeploymentViewSourceError> {
        let StateStoreProviderConfig::Sqlite {
            deployment_owner, ..
        } = &config.provider
        else {
            return Err(FeDeploymentViewSourceError::new(
                FeDeploymentViewSourceErrorKind::UnsupportedProvider,
                "SQLite single-FE deployment source requires the SQLite state store provider",
            ));
        };

        config.validate().map_err(|_| {
            FeDeploymentViewSourceError::new(
                FeDeploymentViewSourceErrorKind::InvalidConfiguration,
                "SQLite state store configuration is invalid",
            )
        })?;

        let topology_revision = derive_topology_revision(&config.cluster_id, deployment_owner)?;
        Ok(Self {
            snapshot: FeDeploymentView {
                active_fe_count: SQLITE_SINGLE_FE_COUNT,
                topology_revision,
            },
        })
    }
}

#[async_trait]
impl FeDeploymentViewSource for SqliteSingleFeDeploymentViewSource {
    async fn snapshot(&self) -> Result<FeDeploymentView, FeDeploymentViewSourceError> {
        Ok(self.snapshot.clone())
    }
}

fn derive_topology_revision(
    cluster_id: &str,
    deployment_owner: &str,
) -> Result<Bytes, FeDeploymentViewSourceError> {
    let mut hasher = Sha256::new();
    hasher.update(REVISION_DOMAIN);
    update_length_framed(&mut hasher, cluster_id)?;
    update_length_framed(&mut hasher, deployment_owner)?;
    Ok(Bytes::copy_from_slice(&hasher.finalize()))
}

fn update_length_framed(
    hasher: &mut Sha256,
    value: &str,
) -> Result<(), FeDeploymentViewSourceError> {
    let length = u32::try_from(value.len()).map_err(|_| {
        FeDeploymentViewSourceError::new(
            FeDeploymentViewSourceErrorKind::InvalidConfiguration,
            "SQLite deployment identity field is too long",
        )
    })?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}
