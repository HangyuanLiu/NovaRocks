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

use super::SqliteStateStore;
use crate::state_store::provider::SQLITE_STATE_STORE_PROVIDER_ID;

pub(crate) struct SqliteStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    path: PathBuf,
    deployment_owner: String,
}

impl SqliteStateStoreProviderFactory {
    pub(crate) fn new(path: PathBuf, deployment_owner: String) -> Self {
        Self {
            descriptor: StateStoreProviderDescriptor::new(SQLITE_STATE_STORE_PROVIDER_ID),
            path,
            deployment_owner,
        }
    }
}

#[async_trait]
impl StateStoreProviderFactory for SqliteStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(deadline_error());
        }
        let store =
            SqliteStateStore::open(self.path, self.deployment_owner, request.clone()).await?;
        if Instant::now() >= request.deadline {
            drop(store);
            return Err(deadline_error());
        }
        let state_store: Arc<dyn StateStore> = Arc::new(store);
        Ok(Box::new(SqliteStateStoreProviderInstance {
            descriptor: self.descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(state_store),
        }))
    }
}

struct SqliteStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
}

#[async_trait]
impl StateStoreProviderInstance for SqliteStateStoreProviderInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        self.lifecycle
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store.clone()
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        if self.lifecycle == StateStoreProviderLifecycle::Stopped {
            return Ok(());
        }
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        let Some(store) = self.state_store.take() else {
            self.lifecycle = StateStoreProviderLifecycle::Stopped;
            return Ok(());
        };
        loop {
            if Arc::strong_count(&store) == 1 {
                drop(store);
                self.lifecycle = StateStoreProviderLifecycle::Stopped;
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.state_store = Some(store);
                return Err(deadline_error());
            }
            tokio::task::yield_now().await;
        }
    }
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "SQLite state store provider deadline exceeded",
    )
}
