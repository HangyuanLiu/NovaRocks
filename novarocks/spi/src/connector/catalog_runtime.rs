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

//! Provider-local catalog runtimes materialized by a backend.
//!
//! `CatalogProperties` is the complete, immutable frontend contribution.  A
//! materializer is selected from the backend's startup-sealed provider set and
//! may combine that contribution with local credentials, clients, and resource
//! limits.  Neither those local facts nor a process-local runtime identity
//! travel over the native query wire.

use std::sync::Arc;

use super::{
    CatalogHandle, CatalogProperties, CatalogProviderKind, ConnectorBatchWriter, ConnectorError,
    ConnectorOpenWriterRequest,
};

/// One exact backend-local catalog materialization.
///
/// The trait intentionally exposes only immutable identity.  Provider-owned
/// execution capabilities remain behind the runtime until their native
/// read/write callers have resolved this exact handle through the query lease.
pub trait CatalogRuntime: Send + Sync {
    fn handle(&self) -> &CatalogHandle;

    fn provider_kind(&self) -> CatalogProviderKind;
}

/// Startup-composed provider materializer for one catalog kind.
///
/// Implementations must reject a mismatched provider kind and must not obtain
/// catalog properties from any source other than the supplied immutable value.
pub trait CatalogRuntimeMaterializer: Send + Sync {
    fn provider_kind(&self) -> CatalogProviderKind;

    fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError>;
}

/// One exact backend-local catalog-scoped writer capability.
///
/// The capability is created only after the backend has materialized and
/// identity-checked its `CatalogHandle`. Fragment decode must first resolve it
/// through the admitted query lease; it must not reconstruct a writer from a
/// retained catalog or an effect-generation registry. The request retains the
/// existing opaque writer facts because those identify the operation and its
/// report cohort, not this catalog runtime.
pub trait CatalogWriteExecution: Send + Sync {
    fn catalog_handle(&self) -> &CatalogHandle;

    fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError>;
}

/// Complete catalog-scoped writer unit for one exact immutable handle.
///
/// Keeping the execution behind a bundle mirrors the typed-read installation
/// boundary: provider composition creates it once while catalog materializes,
/// and the backend lifecycle owns every later lease and retirement decision.
#[derive(Clone)]
pub struct CatalogWriteExecutionBundle {
    execution: Arc<dyn CatalogWriteExecution>,
}

impl CatalogWriteExecutionBundle {
    pub fn new(execution: Arc<dyn CatalogWriteExecution>) -> Self {
        Self { execution }
    }

    pub fn execution(&self) -> Arc<dyn CatalogWriteExecution> {
        Arc::clone(&self.execution)
    }
}

impl std::fmt::Debug for CatalogWriteExecutionBundle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogWriteExecutionBundle")
            .finish_non_exhaustive()
    }
}

/// Provider-owned constructor for a catalog-scoped writer unit.
///
/// Server composition seals one factory per catalog provider kind. The
/// `CatalogManager` invokes the factory only for immutable properties that its
/// matching materializer has already accepted.
pub trait CatalogWriteExecutionBundleFactory: Send + Sync {
    fn build(
        &self,
        properties: &CatalogProperties,
    ) -> Result<CatalogWriteExecutionBundle, ConnectorError>;
}
