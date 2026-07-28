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

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::state_store::{
    MAX_KEY_BYTES, StateStoreLimits, StateStoreProviderFactory, StateStoreProviderId,
};

use super::config::{StateStoreHostConfig, StateStoreProviderConfig};
use super::host_error::{StateStoreHostError, StateStoreHostErrorKind};
use super::limits::{MYSQL_MAX_KEY_BYTES, resolve_state_store_limits};
use super::sqlite::SqliteStateStoreProviderFactory;

pub const SQLITE_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("sqlite");
pub const MYSQL_STATE_STORE_PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("mysql");
pub const FOUNDATIONDB_STATE_STORE_PROVIDER_ID: StateStoreProviderId =
    StateStoreProviderId::new("foundationdb");

pub type StateStoreProviderBinder = Arc<
    dyn Fn(&StateStoreHostConfig) -> Result<Box<dyn StateStoreProviderFactory>, StateStoreHostError>
        + Send
        + Sync,
>;

enum StateStoreProviderAvailability {
    Available(StateStoreProviderBinder),
    Unavailable(&'static str),
}

pub struct StateStoreProviderRegistration {
    id: StateStoreProviderId,
    provider_max_key_bytes: usize,
    availability: StateStoreProviderAvailability,
}

impl StateStoreProviderRegistration {
    pub fn available<F>(id: StateStoreProviderId, provider_max_key_bytes: usize, binder: F) -> Self
    where
        F: Fn(
                &StateStoreHostConfig,
            ) -> Result<Box<dyn StateStoreProviderFactory>, StateStoreHostError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            id,
            provider_max_key_bytes,
            availability: StateStoreProviderAvailability::Available(Arc::new(binder)),
        }
    }

    pub const fn unavailable(
        id: StateStoreProviderId,
        reason: &'static str,
        provider_max_key_bytes: usize,
    ) -> Self {
        Self {
            id,
            provider_max_key_bytes,
            availability: StateStoreProviderAvailability::Unavailable(reason),
        }
    }
}

pub struct StateStoreProviderRegistry {
    registrations: BTreeMap<StateStoreProviderId, StateStoreProviderRegistration>,
}

#[allow(dead_code)] // Consumed by StateStoreHost when it is introduced in the next task.
pub(crate) struct BoundStateStoreProvider {
    pub factory: Box<dyn StateStoreProviderFactory>,
    pub limits: StateStoreLimits,
}

impl StateStoreProviderRegistry {
    pub fn new() -> Self {
        Self {
            registrations: BTreeMap::new(),
        }
    }

    pub fn register(
        &mut self,
        registration: StateStoreProviderRegistration,
    ) -> Result<(), StateStoreHostError> {
        let id = registration.id;
        if self.registrations.contains_key(&id) {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::DuplicateProvider,
                Some(id),
                "state store provider is already registered",
            ));
        }
        self.registrations.insert(id, registration);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn bind(
        &self,
        id: StateStoreProviderId,
        config: &StateStoreHostConfig,
    ) -> Result<BoundStateStoreProvider, StateStoreHostError> {
        let Some(registration) = self.registrations.get(&id) else {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::ProviderNotRegistered,
                Some(id),
                "state store provider is not registered",
            ));
        };
        let binder = match &registration.availability {
            StateStoreProviderAvailability::Available(binder) => binder,
            StateStoreProviderAvailability::Unavailable(reason) => {
                return Err(StateStoreHostError::new(
                    StateStoreHostErrorKind::ProviderNotCompiled,
                    Some(id),
                    *reason,
                ));
            }
        };
        let limits = resolve_state_store_limits(
            &config.state_store.store.limits,
            registration.provider_max_key_bytes,
        )
        .map_err(|error| StateStoreHostError::invalid_configuration(id, error))?;
        let factory = binder(config)?;
        if factory.descriptor().id != id {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::DescriptorMismatch,
                Some(id),
                "state store provider factory descriptor does not match registration",
            ));
        }
        Ok(BoundStateStoreProvider { factory, limits })
    }
}

pub fn builtin_state_store_provider_registry()
-> Result<StateStoreProviderRegistry, StateStoreHostError> {
    let mut registry = StateStoreProviderRegistry::new();
    registry.register(StateStoreProviderRegistration::available(
        SQLITE_STATE_STORE_PROVIDER_ID,
        MAX_KEY_BYTES,
        |config| {
            let StateStoreProviderConfig::Sqlite {
                path,
                deployment_owner,
            } = &config.state_store.store.provider
            else {
                return Err(StateStoreHostError::new(
                    StateStoreHostErrorKind::Bind,
                    Some(SQLITE_STATE_STORE_PROVIDER_ID),
                    "SQLite provider binder requires SQLite provider configuration",
                ));
            };
            Ok(Box::new(SqliteStateStoreProviderFactory::new(
                path.clone(),
                deployment_owner.clone(),
            )))
        },
    ))?;
    registry.register(StateStoreProviderRegistration::unavailable(
        MYSQL_STATE_STORE_PROVIDER_ID,
        "MySQL provider is not compiled in",
        MYSQL_MAX_KEY_BYTES,
    ))?;
    registry.register(StateStoreProviderRegistration::unavailable(
        FOUNDATIONDB_STATE_STORE_PROVIDER_ID,
        "FoundationDB provider is not compiled in",
        MAX_KEY_BYTES,
    ))?;
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::config::{
        MySqlClientConfig, MySqlTlsMode, StateStoreAppConfig, StateStoreConfig,
        StateStoreProviderConfig,
    };
    use crate::state_store::limits::StateStoreLimitOverrides;
    use async_trait::async_trait;
    use novarocks_spi::state_store::{
        StateStoreError, StateStoreOpenRequest, StateStoreProviderDescriptor,
        StateStoreProviderInstance,
    };

    struct TestFactory {
        descriptor: StateStoreProviderDescriptor,
    }

    #[async_trait]
    impl StateStoreProviderFactory for TestFactory {
        fn descriptor(&self) -> &StateStoreProviderDescriptor {
            &self.descriptor
        }

        async fn open(
            self: Box<Self>,
            _request: StateStoreOpenRequest,
        ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
            panic!("registry binding must not open a provider instance")
        }
    }

    fn mysql_host_config() -> StateStoreHostConfig {
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "cluster-a".to_owned(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Mysql {
                        database: "novarocks_control_plane".to_owned(),
                    },
                },
                mysql_client: Some(MySqlClientConfig {
                    host: "mysql.internal.example".to_owned(),
                    port: 3306,
                    username: "novarocks_state_store".to_owned(),
                    password_env: "NOVAROCKS_STATE_STORE_MYSQL_PASSWORD".to_owned(),
                    tls_mode: MySqlTlsMode::Required,
                    tls_ca_path: None,
                    tls_cert_path: None,
                    tls_key_path: None,
                    connect_timeout_ms: 1_000,
                    pool_min: 1,
                    pool_max: 16,
                    inactive_connection_ttl_ms: 30_000,
                }),
            },
            foundationdb_client: None,
        }
    }

    #[test]
    fn registry_distinguishes_unknown_feature_off_and_duplicate_provider() {
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::unavailable(
                MYSQL_STATE_STORE_PROVIDER_ID,
                "MySQL provider is not compiled in",
                3_072,
            ))
            .unwrap();

        let duplicate = registry
            .register(StateStoreProviderRegistration::unavailable(
                MYSQL_STATE_STORE_PROVIDER_ID,
                "duplicate",
                3_072,
            ))
            .unwrap_err();
        assert_eq!(duplicate.kind(), StateStoreHostErrorKind::DuplicateProvider);

        let missing = match registry.bind(
            StateStoreProviderId::new("unknown-provider"),
            &mysql_host_config(),
        ) {
            Ok(_) => panic!("unknown provider must not bind"),
            Err(error) => error,
        };
        assert_eq!(
            missing.kind(),
            StateStoreHostErrorKind::ProviderNotRegistered
        );

        let unavailable = match registry.bind(MYSQL_STATE_STORE_PROVIDER_ID, &mysql_host_config()) {
            Ok(_) => panic!("unavailable provider must not bind"),
            Err(error) => error,
        };
        assert_eq!(
            unavailable.kind(),
            StateStoreHostErrorKind::ProviderNotCompiled
        );
    }

    #[test]
    fn registry_binds_matching_factory_with_provider_specific_limits() {
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::available(
                MYSQL_STATE_STORE_PROVIDER_ID,
                3_072,
                |_| {
                    Ok(Box::new(TestFactory {
                        descriptor: StateStoreProviderDescriptor::new(
                            MYSQL_STATE_STORE_PROVIDER_ID,
                        ),
                    }))
                },
            ))
            .expect("register available MySQL provider");

        let bound = registry
            .bind(MYSQL_STATE_STORE_PROVIDER_ID, &mysql_host_config())
            .expect("matching provider factory must bind without opening I/O");

        assert_eq!(bound.factory.descriptor().id, MYSQL_STATE_STORE_PROVIDER_ID);
        assert_eq!(bound.limits.max_key_bytes, 3_072);
    }

    #[test]
    fn registry_rejects_factory_descriptor_mismatch() {
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::available(
                MYSQL_STATE_STORE_PROVIDER_ID,
                3_072,
                |_| {
                    Ok(Box::new(TestFactory {
                        descriptor: StateStoreProviderDescriptor::new(
                            SQLITE_STATE_STORE_PROVIDER_ID,
                        ),
                    }))
                },
            ))
            .expect("register mismatched factory for validation");

        let error = match registry.bind(MYSQL_STATE_STORE_PROVIDER_ID, &mysql_host_config()) {
            Ok(_) => panic!("mismatched factory must not bind"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StateStoreHostErrorKind::DescriptorMismatch);
    }
}
