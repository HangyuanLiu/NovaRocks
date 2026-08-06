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

//! Provider-owned process-local filesystem access binding.
//!
//! The binding carries only startup-composed credentials, access resolution,
//! and file-I/O runtime services. It is intentionally independent of Core's
//! execution operators and SQL/application lifecycle.

use std::sync::{Arc, OnceLock};

use novarocks_fs::{
    FileError, FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner, FsAccessHandle,
    FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

const DEFAULT_ACCESS_BINDING: &str = "default";

#[derive(Clone)]
pub struct IcebergReadBinding {
    access_binding: String,
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    access_resolver: FsAccessResolver,
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
}

impl std::fmt::Debug for IcebergReadBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergReadBinding")
            .field("access_binding", &self.access_binding)
            .field(
                "object_store_config",
                &self.object_store_config.as_ref().map(|_| "<redacted>"),
            )
            .finish_non_exhaustive()
    }
}

impl IcebergReadBinding {
    pub fn default_binding(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    ) -> Result<Self, ConnectorError> {
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            static FALLBACK_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
            FALLBACK_RUNTIME
                .get_or_init(|| {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("Iceberg fallback Tokio runtime must initialize")
                })
                .handle()
                .clone()
        });
        Ok(Self::new(
            object_store_config,
            Arc::new(TokioFileIoRuntime::new(handle.clone())),
            Arc::new(TokioFileTaskSpawner::new(handle)),
        ))
    }

    pub fn new(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self {
            access_binding: DEFAULT_ACCESS_BINDING.to_string(),
            object_store_config,
            access_resolver: FsAccessResolver::new(),
            file_runtime,
            file_task_spawner,
        }
    }

    pub fn access_binding(&self) -> &str {
        &self.access_binding
    }

    pub fn object_store_config(&self) -> Option<&novarocks_fs::ObjectStoreConfig> {
        self.object_store_config.as_ref()
    }

    pub fn resolve_access(&self, location: &str) -> Result<FsAccessHandle, ConnectorError> {
        self.access_resolver
            .resolve_location(location, self.object_store_config.as_ref())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
    }

    pub fn resolve_access_for_locations<I, S>(
        &self,
        locations: I,
    ) -> Result<FsAccessHandle, ConnectorError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.access_resolver
            .resolve_locations(locations, self.object_store_config.as_ref())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
    }

    pub fn file_read_context(
        &self,
        cancellation: novarocks_fs::FileCancellation,
        deadline: std::time::Instant,
    ) -> Result<FileReadContext, ConnectorError> {
        Ok(FileReadContext {
            cancellation,
            deadline: Some(deadline),
            runtime: Arc::clone(&self.file_runtime),
            task_spawner: Arc::clone(&self.file_task_spawner),
        })
    }

    pub fn file_size(
        &self,
        path: &str,
        access: &FsAccessHandle,
        context: &FileReadContext,
    ) -> Result<u64, ConnectorError> {
        let file = access
            .bind_location(path, FileIdentity::new(path, 0, None))
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let cancellation = context.cancellation.clone();
        context
            .runtime
            .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))
            .map_err(|error: FileError| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
                    .with_retryable_before_progress()
            })
    }
}
