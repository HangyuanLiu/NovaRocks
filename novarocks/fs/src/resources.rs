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

use crate::{
    FileIoRuntime, FileTaskSpawner, FsAccessResolver, ObjectStoreProviderPool, RefreshExecutor,
    RefreshPolicy, StorageAuthorityRegistry,
};

/// Bridges the registry's refresh executor onto the composed task spawner, so a
/// refresh runs wherever that spawner puts detached blocking work rather than on
/// the thread that asked for material.
struct SpawnerRefreshExecutor {
    spawner: Arc<dyn FileTaskSpawner>,
}

impl RefreshExecutor for SpawnerRefreshExecutor {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.spawner.spawn_detached_blocking(job);
    }
}

/// Filesystem resources bound by a connector instance or execution binding.
///
/// Endpoint configuration and secret material are intentionally absent. Each
/// acquire operation supplies those short-lived values explicitly while this
/// resource owns only shared, bounded, process-lived state: the object-store
/// provider pool and the storage authority registry.
///
/// Those two belong together. The pool key now names an authority rather than
/// a query-scoped lease, so a resident operator signs with whatever authority
/// it captured at construction. If each resolution minted its own authority,
/// material installed by a later resolution would never reach that operator and
/// it would stop working the moment its first material expired. The registry is
/// what makes the two agree, which is why it is composed alongside the pool
/// rather than left to each caller (CAD-1 D0 and D10 together).
#[derive(Clone)]
pub struct FsAccessResources {
    object_store_provider_pool: Arc<ObjectStoreProviderPool>,
    storage_authority_registry: Arc<StorageAuthorityRegistry>,
    access_resolver: FsAccessResolver,
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
}

impl std::fmt::Debug for FsAccessResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsAccessResources")
            .field(
                "object_store_provider_pool",
                &self.object_store_provider_pool,
            )
            .field(
                "storage_authority_registry",
                &self.storage_authority_registry,
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
        object_store_provider_pool: Arc<ObjectStoreProviderPool>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self::new_with_refresh_spawner(
            object_store_provider_pool,
            access_resolver,
            file_runtime,
            Arc::clone(&file_task_spawner),
            file_task_spawner,
        )
    }

    /// Keep credential acquisition on its composed owner when scan I/O uses
    /// a dedicated runtime. The scan spawner remains available to file reads;
    /// the refresh spawner is private to the storage-authority registry.
    pub fn new_with_refresh_spawner(
        object_store_provider_pool: Arc<ObjectStoreProviderPool>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
        refresh_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        // The registry is built here rather than passed in because it must be
        // exactly as shared as the pool beside it. Its refreshes run on the
        // explicitly composed owner, which may differ from scan I/O. Letting
        // callers supply the registry itself would allow two authorities with
        // one identity, which the pool key cannot survive.
        let storage_authority_registry = Arc::new(StorageAuthorityRegistry::with_default_options(
            Arc::new(SpawnerRefreshExecutor {
                spawner: refresh_spawner,
            }),
            RefreshPolicy::default(),
        ));
        Self {
            object_store_provider_pool,
            storage_authority_registry,
            access_resolver,
            file_runtime,
            file_task_spawner,
        }
    }

    pub fn object_store_provider_pool(&self) -> &Arc<ObjectStoreProviderPool> {
        &self.object_store_provider_pool
    }

    /// The process-lived home of storage authorities. A binding must look its
    /// authority up here rather than minting one per resolution: the operator
    /// pool keys on the authority identity, so two authorities with the same
    /// identity would leave the resident operator holding the stale one.
    pub fn storage_authority_registry(&self) -> &Arc<StorageAuthorityRegistry> {
        &self.storage_authority_registry
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
            Arc::new(
                ObjectStoreProviderPool::new(crate::ObjectStoreProviderPoolOptions::default())
                    .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );

        assert!(Arc::ptr_eq(resources.file_runtime(), &file_runtime));
        assert!(Arc::ptr_eq(resources.file_task_spawner(), &task_spawner));
        assert_eq!(
            resources.object_store_provider_pool().options(),
            crate::ObjectStoreProviderPoolOptions::default()
        );
    }
}
