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

use std::sync::Arc;

use novarocks_fs::{
    FileError, FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner, FsAccessHandle,
    FsAccessResolver, FsAccessResources,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

const DEFAULT_ACCESS_BINDING: &str = "default";

#[derive(Clone)]
pub struct IcebergReadBinding {
    access_binding: String,
    resources: FsAccessResources,
}

/// Provider-local credentials selected for one Iceberg object-store location.
/// This is process-local construction state, never a connector handle or
/// durable catalog property.
#[derive(Clone, Debug)]
pub struct IcebergObjectStoreBinding {
    bucket: String,
    config: novarocks_fs::ObjectStoreConfig,
}

impl IcebergObjectStoreBinding {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn config(&self) -> &novarocks_fs::ObjectStoreConfig {
        &self.config
    }
}

impl std::fmt::Debug for IcebergReadBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergReadBinding")
            .field("access_binding", &self.access_binding)
            .field(
                "object_store_config",
                &self.resources.object_store_config().map(|_| "<redacted>"),
            )
            .finish_non_exhaustive()
    }
}

impl IcebergReadBinding {
    /// Builds an Iceberg filesystem binding from resources supplied by the
    /// process composition root.
    pub fn from_resources(resources: FsAccessResources) -> Self {
        Self {
            access_binding: DEFAULT_ACCESS_BINDING.to_string(),
            resources,
        }
    }

    /// Explicit convenience constructor for composition roots that do not
    /// retain a reusable [`FsAccessResources`] bundle.
    pub fn new(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self::from_resources(FsAccessResources::new(
            object_store_config,
            access_resolver,
            file_runtime,
            file_task_spawner,
        ))
    }

    pub fn with_access_binding(
        access_binding: impl Into<String>,
        resources: FsAccessResources,
    ) -> Self {
        Self {
            access_binding: access_binding.into(),
            resources,
        }
    }

    pub fn access_binding(&self) -> &str {
        &self.access_binding
    }

    pub fn object_store_config(&self) -> Option<&novarocks_fs::ObjectStoreConfig> {
        self.resources.object_store_config()
    }

    /// Resolve the startup-composed object-store credentials for an Iceberg
    /// output location. Local/HDFS paths intentionally return no object-store
    /// binding; object-store paths must name a bucket and have explicit BE
    /// credentials.
    pub fn object_store_binding_for_location(
        &self,
        location: &str,
    ) -> Result<Option<IcebergObjectStoreBinding>, String> {
        let location = self
            .resources
            .access_resolver()
            .parse_location(location)
            .map_err(|error| format!("parse Iceberg output location: {error}"))?;
        if location.scheme() != novarocks_fs::FsScheme::ObjectStore {
            return Ok(None);
        }
        let bucket = location.authority().ok_or_else(|| {
            format!(
                "Iceberg object-store output location is missing a bucket: {}",
                location.original()
            )
        })?;
        let config = self.object_store_config().cloned().ok_or_else(|| {
            format!(
                "Iceberg connector writer needs a startup object-store binding for bucket {bucket}"
            )
        })?;
        Ok(Some(IcebergObjectStoreBinding {
            bucket: bucket.to_string(),
            config,
        }))
    }

    pub fn resolve_access(&self, location: &str) -> Result<FsAccessHandle, ConnectorError> {
        self.resources
            .access_resolver()
            .resolve_location(location, self.object_store_config())
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
        self.resources
            .access_resolver()
            .resolve_locations(locations, self.object_store_config())
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
            runtime: Arc::clone(self.resources.file_runtime()),
            task_spawner: Arc::clone(self.resources.file_task_spawner()),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_fs::{FileCancellation, TokioFileIoRuntime, TokioFileTaskSpawner};

    #[test]
    fn requires_a_composition_owned_runtime() {
        let runtime = tokio::runtime::Runtime::new().expect("build explicit Tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );

        let context = binding
            .file_read_context(
                FileCancellation::new(),
                Instant::now() + Duration::from_secs(1),
            )
            .expect("build file read context");

        assert!(Arc::ptr_eq(&context.runtime, &file_runtime));
        assert!(Arc::ptr_eq(&context.task_spawner, &task_spawner));
    }

    #[test]
    fn object_store_writer_binding_never_discovers_credentials() {
        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let config = novarocks_fs::ObjectStoreConfig {
            endpoint: "http://minio:9000".to_string(),
            access_key_id: "test".to_string(),
            access_key_secret: "test".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        let binding = IcebergReadBinding::new(
            Some(config),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );

        let selected = binding
            .object_store_binding_for_location("s3://warehouse/staging/data.parquet")
            .expect("select object store")
            .expect("object store binding");
        assert_eq!(selected.bucket(), "warehouse");
        assert_eq!(selected.config().endpoint, "http://minio:9000");
        assert!(
            binding
                .object_store_binding_for_location("file:///tmp/data.parquet")
                .expect("local location")
                .is_none()
        );
    }
}
