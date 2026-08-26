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

//! The worker-side provider factory the backend registry installs.
//!
//! A page-source provider owns a footer cache and a delete manager and needs
//! the request's deadline and cancellation, so it is built per fragment
//! instance rather than shared process-wide. This factory is the
//! generation-scoped, stateless thing the backend can hold instead.

use std::sync::Arc;

use novarocks_proto::connector_read::{
    TypedConnectorPageSourceProvider, TypedConnectorProviderFactory,
    TypedConnectorSystemTableProvider,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorRequestContext};

use crate::access_binding::IcebergReadBinding;
use crate::typed_read::page_source_provider::{
    IcebergPageSourceProvider, IcebergPageSourceProviderOptions,
};

/// Builds Iceberg worker readers for one installed execution binding.
#[derive(Clone)]
pub struct IcebergTypedProviderFactory {
    binding: IcebergReadBinding,
    options: IcebergPageSourceProviderOptions,
}

impl IcebergTypedProviderFactory {
    /// The composition-root entry point. `binding` is the same process-local
    /// access binding the existing execution installer holds, so both paths
    /// resolve object storage through one owner.
    pub fn new(binding: IcebergReadBinding, options: IcebergPageSourceProviderOptions) -> Self {
        Self { binding, options }
    }
}

impl std::fmt::Debug for IcebergTypedProviderFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergTypedProviderFactory")
            .finish_non_exhaustive()
    }
}

impl TypedConnectorProviderFactory for IcebergTypedProviderFactory {
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorPageSourceProvider>, ConnectorError> {
        let context = self
            .binding
            .file_read_context(novarocks_fs::FileCancellation::new(), request.deadline())?;
        Ok(Arc::new(IcebergPageSourceProvider::new(
            self.binding.clone(),
            context,
            self.options.clone(),
        )))
    }

    fn create_system_table_provider(
        &self,
        _request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorSystemTableProvider>, ConnectorError> {
        // MIGRATION: the selected-backend system page source lands with the
        // exact metadata-relation schemas. Refusing here keeps the boundary
        // fail-closed rather than returning an empty relation that would look
        // like a table with no rows.
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "iceberg system relations are not served through the typed page source yet",
        ))
    }
}
