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
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use novarocks_spi::state_store::{
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderInstance,
    StateStoreProviderLifecycle,
};
use uuid::Uuid;

use super::runtime::FoundationDbRuntime;
use crate::state_store::FoundationDbClientConfig;
use crate::state_store::provider::FOUNDATIONDB_STATE_STORE_PROVIDER_ID;

pub(crate) struct FoundationDbStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    cluster_file: PathBuf,
    keyspace_id: Uuid,
    client: FoundationDbClientConfig,
}

impl FoundationDbStateStoreProviderFactory {
    pub(crate) fn new(
        cluster_file: PathBuf,
        keyspace_id: Uuid,
        client: FoundationDbClientConfig,
    ) -> Self {
        Self {
            descriptor: StateStoreProviderDescriptor::new(FOUNDATIONDB_STATE_STORE_PROVIDER_ID),
            cluster_file,
            keyspace_id,
            client,
        }
    }
}

#[async_trait]
impl StateStoreProviderFactory for FoundationDbStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(provider_deadline_error());
        }
        let mut runtime = FoundationDbRuntime::boot(self.client)?;
        let deadline = request.deadline;
        let state_store = match runtime
            .open_store(&self.cluster_file, self.keyspace_id, request)
            .await
        {
            Ok(store) => store,
            Err(open) => {
                return match runtime.shutdown_until(deadline).await {
                    Ok(()) => Err(open),
                    Err(cleanup) => Err(open.with_cleanup_context(cleanup)),
                };
            }
        };
        Ok(Box::new(FoundationDbStateStoreProviderInstance {
            descriptor: self.descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(state_store),
            runtime: Some(runtime),
        }))
    }
}

fn provider_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB state store provider deadline exceeded",
    )
}

pub(super) struct FoundationDbStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    runtime: Option<FoundationDbRuntime>,
}

#[async_trait]
impl StateStoreProviderInstance for FoundationDbStateStoreProviderInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        self.lifecycle
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        if self.lifecycle == StateStoreProviderLifecycle::Ready {
            self.state_store.clone()
        } else {
            None
        }
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        if self.lifecycle == StateStoreProviderLifecycle::Stopped {
            return Ok(());
        }
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        self.state_store.take();
        let Some(runtime) = self.runtime.as_mut() else {
            self.lifecycle = StateStoreProviderLifecycle::Stopped;
            return Ok(());
        };
        runtime.shutdown_until(deadline).await?;
        self.runtime.take();
        self.lifecycle = StateStoreProviderLifecycle::Stopped;
        Ok(())
    }
}

#[cfg(feature = "state-store-test-hooks")]
#[doc(hidden)]
pub struct FoundationDbProviderTestHarness {
    runtime: Option<FoundationDbRuntime>,
}

#[cfg(feature = "state-store-test-hooks")]
impl FoundationDbProviderTestHarness {
    pub fn boot(config: FoundationDbClientConfig) -> Result<Self, StateStoreError> {
        Ok(Self {
            runtime: Some(FoundationDbRuntime::boot(config)?),
        })
    }

    pub async fn open_store(
        &self,
        config: crate::state_store::StateStoreConfig,
        deployment: crate::state_store::FeDeploymentView,
        deadline: Instant,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store configuration is invalid",
            )
        })?;
        let (cluster_file, keyspace_id) = match config.provider {
            crate::state_store::StateStoreProviderConfig::Foundationdb {
                cluster_file,
                keyspace_id,
            } => (cluster_file, keyspace_id),
            _ => {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "FoundationDB test harness requires FoundationDB provider configuration",
                ));
            }
        };
        let limits = crate::state_store::limits::resolve_state_store_limits(
            &config.limits,
            novarocks_spi::state_store::MAX_KEY_BYTES,
        )
        .map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store limits are invalid",
            )
        })?;
        self.runtime
            .as_ref()
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB test harness is stopped",
                )
            })?
            .open_store(
                &cluster_file,
                keyspace_id,
                StateStoreOpenRequest {
                    cluster_id: config.cluster_id,
                    limits,
                    deployment: novarocks_spi::state_store::FeDeploymentView {
                        active_fe_count: deployment.active_fe_count,
                        topology_revision: deployment.topology_revision,
                    },
                    deadline,
                },
            )
            .await
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        let Some(runtime) = self.runtime.as_mut() else {
            return Ok(());
        };
        runtime.shutdown_until(deadline).await?;
        self.runtime.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use uuid::Uuid;

    use novarocks_spi::state_store::StateStoreProviderInstance;

    use crate::state_store::{
        FOUNDATIONDB_STATE_STORE_PROVIDER_ID, FoundationDbClientConfig, StateStoreAppConfig,
        StateStoreConfig, StateStoreHostConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
        builtin_state_store_provider_registry,
    };

    fn foundationdb_host_config(cluster_file: std::path::PathBuf) -> StateStoreHostConfig {
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "cluster-a".to_owned(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Foundationdb {
                        cluster_file,
                        keyspace_id: Uuid::nil(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: Some(FoundationDbClientConfig {
                disable_multi_version_client: true,
                tls_cert_path: None,
                tls_key_path: None,
                tls_ca_path: None,
                tls_verify_peers: None,
                tls_password_env: None,
            }),
        }
    }

    #[test]
    fn foundationdb_registration_binds_the_typed_factory_without_network_start() {
        let temp = TempDir::new().expect("FoundationDB bind temp dir");
        let cluster_file = temp.path().join("fdb.cluster");
        std::fs::write(&cluster_file, b"test:test@127.0.0.1:4500")
            .expect("write FoundationDB cluster file");
        let registry = builtin_state_store_provider_registry().unwrap();
        let bound = registry
            .bind(
                FOUNDATIONDB_STATE_STORE_PROVIDER_ID,
                &foundationdb_host_config(cluster_file),
            )
            .unwrap();
        assert_eq!(
            bound.factory.descriptor().id,
            FOUNDATIONDB_STATE_STORE_PROVIDER_ID
        );
        assert_foundationdb_instance_contract::<super::FoundationDbStateStoreProviderInstance>();
    }

    fn assert_foundationdb_instance_contract<T: StateStoreProviderInstance>() {}
}
