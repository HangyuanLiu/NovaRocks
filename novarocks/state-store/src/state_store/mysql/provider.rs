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

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use novarocks_spi::state_store::{
    StateStore, StateStoreError, StateStoreOpenRequest, StateStoreProviderDescriptor,
    StateStoreProviderFactory, StateStoreProviderInstance, StateStoreProviderLifecycle,
};

use super::runtime::MysqlRuntime;
use crate::state_store::MySqlClientConfig;
use crate::state_store::provider::MYSQL_STATE_STORE_PROVIDER_ID;

pub(crate) struct MysqlStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    database: String,
    client: MySqlClientConfig,
}

impl MysqlStateStoreProviderFactory {
    pub(crate) fn new(database: String, client: MySqlClientConfig) -> Self {
        Self {
            descriptor: StateStoreProviderDescriptor::new(MYSQL_STATE_STORE_PROVIDER_ID),
            database,
            client,
        }
    }
}

#[async_trait]
impl StateStoreProviderFactory for MysqlStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        let mut runtime = MysqlRuntime::boot(self.client)?;
        let deadline = request.deadline;
        let store = match runtime.open_store(self.database, request).await {
            Ok(store) => store,
            Err(open) => {
                return match runtime.shutdown_until(deadline).await {
                    Ok(()) => Err(open),
                    Err(cleanup) => Err(open.with_cleanup_context(cleanup)),
                };
            }
        };
        Ok(Box::new(MysqlStateStoreProviderInstance {
            descriptor: self.descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(store),
            runtime: Some(runtime),
        }))
    }
}

pub(super) struct MysqlStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    runtime: Option<MysqlRuntime>,
}

#[async_trait]
impl StateStoreProviderInstance for MysqlStateStoreProviderInstance {
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "state-store-test-hooks")]
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(feature = "state-store-test-hooks")]
    use std::time::Duration;

    #[cfg(feature = "state-store-test-hooks")]
    use futures::future::BoxFuture;
    use novarocks_spi::state_store::StateStoreProviderInstance;
    #[cfg(feature = "state-store-test-hooks")]
    use novarocks_spi::state_store::{
        ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStoreErrorKind,
        StateStoreLimits, StateStoreMetricsSnapshot, StoreIdentity, TransactionId,
        WriteTransaction,
    };

    #[cfg(feature = "state-store-test-hooks")]
    use super::*;
    #[cfg(feature = "state-store-test-hooks")]
    use crate::state_store::mysql::client::{MysqlPoolConnection, PoolLifecycle};
    #[cfg(feature = "state-store-test-hooks")]
    use crate::state_store::mysql::error::MysqlNativeError;
    #[cfg(feature = "state-store-test-hooks")]
    use crate::state_store::mysql::runtime::test_mysql_runtime_with_pool;

    use crate::state_store::{
        MYSQL_STATE_STORE_PROVIDER_ID, MySqlClientConfig, MySqlTlsMode, StateStoreAppConfig,
        StateStoreConfig, StateStoreHostConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
        builtin_state_store_provider_registry,
    };

    const PASSWORD_ENV: &str = "NOVAROCKS_SPI2_MYSQL_BIND_TEST_PASSWORD";

    #[cfg(feature = "state-store-test-hooks")]
    struct FailOncePool {
        disconnects: Arc<AtomicUsize>,
    }

    #[cfg(feature = "state-store-test-hooks")]
    impl PoolLifecycle for FailOncePool {
        fn get_conn<'a>(
            &'a self,
            _deadline: tokio::time::Instant,
        ) -> BoxFuture<'a, Result<MysqlPoolConnection, MysqlNativeError>> {
            Box::pin(async { Err(MysqlNativeError::provider_unavailable()) })
        }

        fn disconnect(self: Arc<Self>) -> BoxFuture<'static, Result<(), MysqlNativeError>> {
            Box::pin(async move {
                let attempt = self.disconnects.fetch_add(1, Ordering::AcqRel);
                if attempt == 0 {
                    Err(MysqlNativeError::provider_unavailable())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    struct FakeStore;

    #[cfg(feature = "state-store-test-hooks")]
    #[async_trait]
    impl StateStore for FakeStore {
        fn limits(&self) -> &StateStoreLimits {
            static LIMITS: std::sync::LazyLock<StateStoreLimits> =
                std::sync::LazyLock::new(StateStoreLimits::default);
            &LIMITS
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            panic!("unused fake store operation")
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn begin_write(
            &self,
            _transaction_id: TransactionId,
            _purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn resolve_commit(
            &self,
            _transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            panic!("unused fake store operation")
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
                    password_env: PASSWORD_ENV.to_owned(),
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
    fn mysql_registration_binds_the_typed_factory_without_connecting() {
        unsafe {
            std::env::set_var(PASSWORD_ENV, "test-secret");
        }
        let registry = builtin_state_store_provider_registry().unwrap();
        let bound = registry
            .bind(MYSQL_STATE_STORE_PROVIDER_ID, &mysql_host_config())
            .unwrap();
        assert_eq!(bound.factory.descriptor().id, MYSQL_STATE_STORE_PROVIDER_ID);
        assert_mysql_instance_contract::<super::MysqlStateStoreProviderInstance>();
    }

    #[cfg(feature = "state-store-test-hooks")]
    #[tokio::test(flavor = "current_thread")]
    async fn disconnect_failure_keeps_instance_draining_and_retryable() {
        let disconnects = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailOncePool {
            disconnects: Arc::clone(&disconnects),
        });
        let runtime = test_mysql_runtime_with_pool(pool);
        let mut instance = MysqlStateStoreProviderInstance {
            descriptor: StateStoreProviderDescriptor::new(MYSQL_STATE_STORE_PROVIDER_ID),
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(Arc::new(FakeStore)),
            runtime: Some(runtime),
        };
        assert!(instance.state_store().is_some());

        let first = instance
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("first disconnect must surface its native error");

        assert_eq!(first.kind(), StateStoreErrorKind::ProviderUnavailable);
        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
        assert!(instance.state_store().is_none());
        let runtime = instance
            .runtime
            .as_ref()
            .expect("runtime retained for retry");
        assert_eq!(runtime.pool_count().expect("pool ownership"), 1);
        assert!(!runtime.is_accepting());
        let blocked = match runtime.acquire_operation() {
            Ok(_) => panic!("draining instance must not admit new work"),
            Err(error) => error,
        };
        assert_eq!(blocked.kind(), StateStoreErrorKind::ProviderUnavailable);

        instance
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect("second disconnect must finish shutdown");

        assert_eq!(disconnects.load(Ordering::Acquire), 2);
        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
        assert!(instance.state_store().is_none());
        assert!(instance.runtime.is_none());
    }

    fn assert_mysql_instance_contract<T: StateStoreProviderInstance>() {}
}
