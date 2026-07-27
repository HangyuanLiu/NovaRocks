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

use novarocks_spi::state_store::{StateStore, StateStoreError};
use novarocks_state_store::{StateStoreAppConfig, StateStoreRuntime, open_state_store};

use crate::deployment::{FeDeploymentViewSource, SqliteSingleFeDeploymentViewSource};
use crate::view::FrontendViewService;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendApplicationErrorKind {
    DeploymentSource,
    RuntimeOpen,
    StoreOpen,
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
    state_store: Option<Arc<dyn StateStore>>,
    runtime: Option<StateStoreRuntime>,
}

impl FrontendApplicationHost {
    pub async fn open(
        config: Option<StateStoreAppConfig>,
    ) -> Result<Self, FrontendApplicationError> {
        let mut host = Self {
            view_service: None,
            state_store: None,
            runtime: None,
        };

        if let Some(config) = config {
            if let Err(error) = host.open_configured(config).await {
                return Err(host.cleanup_open_error(error).await);
            }
        }
        match FrontendViewService::open(host.state_store.clone(), tokio::runtime::Handle::current())
            .await
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
        self.state_store.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), FrontendApplicationError> {
        self.release_resources().await.map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::Shutdown, error)
        })
    }

    async fn open_configured(
        &mut self,
        config: StateStoreAppConfig,
    ) -> Result<(), FrontendApplicationError> {
        let source = SqliteSingleFeDeploymentViewSource::try_from_state_store_config(&config.store)
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::DeploymentSource, error)
            })?;
        let deployment = source.snapshot().await.map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::DeploymentSource, error)
        })?;

        self.runtime = Some(StateStoreRuntime::local().map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::RuntimeOpen, error)
        })?);
        let runtime = self
            .runtime
            .as_ref()
            .expect("runtime is installed before store open");
        self.state_store = Some(
            open_state_store(runtime, config.store, deployment)
                .await
                .map_err(|error| {
                    FrontendApplicationError::new(FrontendApplicationErrorKind::StoreOpen, error)
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

    async fn release_resources(&mut self) -> Result<(), StateStoreError> {
        self.view_service.take();
        self.state_store.take();
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.shutdown().await
        } else {
            Ok(())
        }
    }
}
