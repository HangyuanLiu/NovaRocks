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
    StateStoreHost, StateStoreHostConfig, StateStoreHostError,
    builtin_state_store_provider_registry,
};

use crate::deployment::{FeDeploymentViewSource, SqliteSingleFeDeploymentViewSource};
use crate::view::FrontendViewService;

const STATE_STORE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendApplicationErrorKind {
    DeploymentSource,
    StateStoreHost,
    ViewServiceOpen,
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
    view_service: Option<Arc<dyn novarocks::engine::view::ViewService>>,
    state_store_host: Option<StateStoreHost>,
}

impl FrontendApplicationHost {
    pub async fn open(
        config: Option<StateStoreHostConfig>,
    ) -> Result<Self, FrontendApplicationError> {
        let mut host = Self {
            view_service: None,
            state_store_host: None,
        };

        if let Some(config) = config {
            if let Err(error) = host.open_configured(config).await {
                return Err(host.cleanup_open_error(error).await);
            }
        }
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

        Ok(host)
    }

    pub fn view_service(&self) -> Arc<dyn novarocks::engine::view::ViewService> {
        Arc::clone(
            self.view_service
                .as_ref()
                .expect("frontend view service is installed before host open returns"),
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

    async fn release_resources(&mut self) -> Result<(), StateStoreHostError> {
        self.view_service.take();
        if let Some(host) = self.state_store_host.as_mut() {
            host.shutdown(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
                .await?;
            self.state_store_host.take();
            Ok(())
        } else {
            Ok(())
        }
    }
}
