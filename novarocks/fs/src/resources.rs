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

//! Explicit process-local resources used by connector filesystem bindings.
//!
//! A composition root supplies all asynchronous and credential-bearing state.
//! This crate never discovers a Tokio runtime or creates a process-global
//! fallback on behalf of a connector.

use std::sync::Arc;

use crate::{FileIoRuntime, FileTaskSpawner, FsAccessResolver, ObjectStoreConfig};

/// Filesystem resources bound by a connector instance or execution binding.
///
/// `ObjectStoreConfig` is intentionally process-local. Callers must not
/// serialize this value into connector handles, durable state, or wire payloads.
#[derive(Clone)]
pub struct FsAccessResources {
    object_store_config: Option<ObjectStoreConfig>,
    access_resolver: FsAccessResolver,
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
}

impl std::fmt::Debug for FsAccessResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsAccessResources")
            .field(
                "object_store_config",
                &self.object_store_config.as_ref().map(|_| "<redacted>"),
            )
            .field("access_resolver", &self.access_resolver)
            .finish_non_exhaustive()
    }
}

impl FsAccessResources {
    /// Constructs a binding from composition-owned resources.
    ///
    /// All services are mandatory arguments so connectors cannot silently
    /// discover a current runtime, construct a fallback runtime, or use a
    /// process-global filesystem service.
    pub fn new(
        object_store_config: Option<ObjectStoreConfig>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self {
            object_store_config,
            access_resolver,
            file_runtime,
            file_task_spawner,
        }
    }

    pub fn object_store_config(&self) -> Option<&ObjectStoreConfig> {
        self.object_store_config.as_ref()
    }

    pub fn access_resolver(&self) -> FsAccessResolver {
        self.access_resolver
    }

    pub fn file_runtime(&self) -> &Arc<dyn FileIoRuntime> {
        &self.file_runtime
    }

    pub fn file_task_spawner(&self) -> &Arc<dyn FileTaskSpawner> {
        &self.file_task_spawner
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{TokioFileIoRuntime, TokioFileTaskSpawner};

    #[test]
    fn retains_the_explicitly_composed_runtime_services() {
        let runtime = tokio::runtime::Runtime::new().expect("build explicit Tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let resources = FsAccessResources::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );

        assert!(Arc::ptr_eq(resources.file_runtime(), &file_runtime));
        assert!(Arc::ptr_eq(resources.file_task_spawner(), &task_spawner));
        assert!(resources.object_store_config().is_none());
    }
}
