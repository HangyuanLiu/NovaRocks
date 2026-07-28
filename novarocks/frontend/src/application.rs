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
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_spi::state_store::{StateStore, StateStoreProviderId};
use novarocks_state_store::{
    StateStoreHost, StateStoreHostConfig, builtin_state_store_provider_registry,
};

use crate::deployment::{FeDeploymentViewSource, SqliteSingleFeDeploymentViewSource};
use crate::mv::{FrontendMvService, repository::StateStoreMvRepository};
use crate::statistics::FrontendStatisticsService;
use crate::table_maintenance::FrontendTableMaintenanceService;
use crate::view::FrontendViewService;

const STATE_STORE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendApplicationErrorKind {
    DeploymentSource,
    StateStoreHost,
    ViewServiceOpen,
    TableMaintenanceServiceOpen,
    MvServiceOpen,
    Server,
    Shutdown,
}

#[derive(Debug)]
pub struct FrontendApplicationError {
    kind: FrontendApplicationErrorKind,
    message: String,
}

impl FrontendApplicationError {
    pub(crate) fn new(kind: FrontendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    pub(crate) fn server(error: impl fmt::Display) -> Self {
        Self::new(FrontendApplicationErrorKind::Server, error)
    }

    pub(crate) fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
        self.message
            .push_str(&format!("; cleanup failed: {cleanup_error}"));
        self
    }

    pub const fn kind(&self) -> FrontendApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for FrontendApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for FrontendApplicationError {}

pub struct FrontendApplicationHost {
    statistics_service: Option<Arc<FrontendStatisticsService>>,
    view_service: Option<Arc<dyn novarocks::engine::view::ViewService>>,
    table_maintenance_service:
        Option<Arc<dyn novarocks::engine::table_maintenance::TableMaintenanceService>>,
    mv_repository: Option<Arc<dyn novarocks::mv::repository::MvRepository>>,
    mv_application_service: Option<Arc<dyn novarocks::mv::application::MvApplicationService>>,
    state_store_host: Option<StateStoreHost>,
}

impl FrontendApplicationHost {
    pub async fn open(
        config: Option<StateStoreHostConfig>,
    ) -> Result<Self, FrontendApplicationError> {
        let mut host = Self {
            statistics_service: None,
            view_service: None,
            table_maintenance_service: None,
            mv_repository: None,
            mv_application_service: None,
            state_store_host: None,
        };

        if let Some(config) = config {
            if let Err(error) = host.open_configured(config).await {
                return Err(host.cleanup_open_error(error).await);
            }
        }
        host.statistics_service = Some(Arc::new(FrontendStatisticsService::new()));
        match FrontendViewService::open(host.state_store(), tokio::runtime::Handle::current()).await
        {
            Ok(view_service) => host.view_service = Some(Arc::new(view_service)),
            Err(error) => {
                let error = FrontendApplicationError::new(
                    FrontendApplicationErrorKind::ViewServiceOpen,
                    error,
                );
                return Err(host.cleanup_open_error(error).await);
            }
        }
        match FrontendTableMaintenanceService::open(
            host.state_store(),
            tokio::runtime::Handle::current(),
        )
        .await
        {
            Ok(service) => host.table_maintenance_service = Some(Arc::new(service)),
            Err(error) => {
                let error = FrontendApplicationError::new(
                    FrontendApplicationErrorKind::TableMaintenanceServiceOpen,
                    error,
                );
                return Err(host.cleanup_open_error(error).await);
            }
        }
        match host.state_store() {
            Some(store) => {
                match StateStoreMvRepository::open(store, tokio::runtime::Handle::current()).await {
                    Ok(repository) => {
                        let repository: Arc<dyn novarocks::mv::repository::MvRepository> =
                            repository;
                        host.mv_application_service =
                            Some(Arc::new(FrontendMvService::new(Arc::clone(&repository))));
                        host.mv_repository = Some(repository);
                    }
                    Err(error) => {
                        return Err(host
                            .cleanup_open_error(FrontendApplicationError::new(
                                FrontendApplicationErrorKind::MvServiceOpen,
                                error,
                            ))
                            .await);
                    }
                }
            }
            None => {
                host.mv_repository =
                    Some(Arc::new(novarocks::mv::repository::UnavailableMvRepository));
                host.mv_application_service = Some(Arc::new(
                    novarocks::mv::application::UnavailableMvApplicationService,
                ));
            }
        }

        Ok(host)
    }

    pub fn view_service(&self) -> Arc<dyn novarocks::engine::view::ViewService> {
        Arc::clone(
            self.view_service
                .as_ref()
                .expect("frontend view service is installed before host open returns"),
        )
    }

    pub fn statistics_service(&self) -> Arc<dyn novarocks::engine::statistics::StatisticsService> {
        self.statistics_service
            .as_ref()
            .expect("frontend statistics service is installed before host open returns")
            .clone()
    }

    pub fn table_maintenance_service(
        &self,
    ) -> Arc<dyn novarocks::engine::table_maintenance::TableMaintenanceService> {
        Arc::clone(
            self.table_maintenance_service
                .as_ref()
                .expect("frontend table-maintenance service is installed before host open returns"),
        )
    }

    pub fn mv_repository(&self) -> Arc<dyn novarocks::mv::repository::MvRepository> {
        Arc::clone(
            self.mv_repository
                .as_ref()
                .expect("frontend MV repository is installed before host open returns"),
        )
    }

    pub fn mv_application_service(
        &self,
    ) -> Arc<dyn novarocks::mv::application::MvApplicationService> {
        Arc::clone(
            self.mv_application_service
                .as_ref()
                .expect("frontend MV application service is installed before host open returns"),
        )
    }

    pub fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store_host
            .as_ref()
            .and_then(StateStoreHost::state_store)
    }

    pub fn state_store_provider_id(&self) -> Option<StateStoreProviderId> {
        self.state_store_host
            .as_ref()
            .map(StateStoreHost::provider_id)
    }

    pub async fn shutdown(mut self) -> Result<(), FrontendApplicationError> {
        self.release_resources().await.map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::Shutdown, error)
        })
    }

    async fn open_configured(
        &mut self,
        config: StateStoreHostConfig,
    ) -> Result<(), FrontendApplicationError> {
        let source = SqliteSingleFeDeploymentViewSource::try_from_state_store_config(
            &config.state_store.store,
        )
        .map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::DeploymentSource, error)
        })?;
        let deployment = source.snapshot().await.map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::DeploymentSource, error)
        })?;
        let registry = builtin_state_store_provider_registry().map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, error)
        })?;
        self.state_store_host = Some(
            StateStoreHost::open(
                &registry,
                config,
                deployment,
                Instant::now() + STATE_STORE_OPEN_TIMEOUT,
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, error)
            })?,
        );

        Ok(())
    }

    async fn cleanup_open_error(
        &mut self,
        primary: FrontendApplicationError,
    ) -> FrontendApplicationError {
        match self.release_resources().await {
            Ok(()) => primary,
            Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
        }
    }

    async fn release_resources(&mut self) -> Result<(), String> {
        let mut primary_error = self
            .table_maintenance_service
            .as_ref()
            .and_then(|service| service.shutdown().err())
            .map(|error| format!("shutdown frontend table-maintenance service failed: {error}"));
        self.table_maintenance_service.take();
        self.statistics_service.take();
        self.view_service.take();
        self.mv_application_service.take();
        self.mv_repository.take();
        if let Some(host) = self.state_store_host.as_mut() {
            if let Err(error) = host
                .shutdown(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
                .await
            {
                let host_error = format!("shutdown frontend StateStore host failed: {error}");
                if let Some(primary) = primary_error.as_mut() {
                    primary.push_str(&format!("; cleanup failed: {host_error}"));
                } else {
                    primary_error = Some(host_error);
                }
            }
            self.state_store_host.take();
        }
        primary_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use novarocks_spi::state_store::{
        FeDeploymentView, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
        StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderInstance,
    };
    use novarocks_state_store::{
        SQLITE_STATE_STORE_PROVIDER_ID, StateStoreAppConfig, StateStoreConfig, StateStoreHost,
        StateStoreHostConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
        StateStoreProviderRegistration, StateStoreProviderRegistry,
    };

    use super::{FrontendApplicationError, FrontendApplicationErrorKind};

    const SECRET_CONFIG_VALUE: &str = "client-secret-must-not-leak";
    const DESCRIPTOR: StateStoreProviderDescriptor =
        StateStoreProviderDescriptor::new(SQLITE_STATE_STORE_PROVIDER_ID);

    struct FailingFactory;

    #[async_trait]
    impl StateStoreProviderFactory for FailingFactory {
        fn descriptor(&self) -> &StateStoreProviderDescriptor {
            &DESCRIPTOR
        }

        async fn open(
            self: Box<Self>,
            _request: StateStoreOpenRequest,
        ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
            Err(StateStoreError::new(
                StateStoreErrorKind::Corruption,
                "injected provider primary failure",
            )
            .with_cleanup_context(StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "injected provider cleanup failure",
            )))
        }
    }

    #[tokio::test]
    async fn frontend_stringification_preserves_host_primary_and_cleanup_context() {
        let temp = tempfile::tempdir().expect("temporary host diagnostics directory");
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::available(
                SQLITE_STATE_STORE_PROVIDER_ID,
                novarocks_spi::state_store::MAX_KEY_BYTES,
                |_| Ok(Box::new(FailingFactory)),
            ))
            .expect("register diagnostic provider");
        let host_error = match StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "diagnostic-cluster".to_owned(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: SECRET_CONFIG_VALUE.to_owned(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).unwrap(),
                topology_revision: Bytes::from_static(b"topology-r1"),
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await
        {
            Ok(_) => panic!("injected provider failure must reject host open"),
            Err(error) => error,
        };

        assert_eq!(
            host_error.primary().map(StateStoreError::kind),
            Some(StateStoreErrorKind::Corruption)
        );
        let frontend_error =
            FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, host_error);
        let diagnostic = frontend_error.to_string();

        assert!(diagnostic.contains("StateStoreHost"));
        assert!(diagnostic.contains("Open (sqlite)"));
        assert!(diagnostic.contains("injected provider primary failure"));
        assert!(diagnostic.contains("injected provider cleanup failure"));
        assert!(!diagnostic.contains(SECRET_CONFIG_VALUE));
    }
}
