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

use novarocks_proto_codec::connector_read::{
    ConnectorReadExecutionBundle, ConnectorReadExecutionBundleFactory,
    TypedConnectorPageSourceProvider, TypedConnectorProviderFactory,
    TypedConnectorSystemTableProvider,
};
use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadFactory, ProviderReadFactoryAdapter, ProviderReadPageSourceProvider,
    ProviderReadRuntime, ProviderReadSystemTableProvider, ReadRuntimeAdapter,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorProviderId,
    ConnectorRequestContext,
};

use crate::access_binding::IcebergReadBinding;
use crate::typed_read::page_source_provider::{
    IcebergPageSourceProvider, IcebergPageSourceProviderOptions,
};
use crate::typed_read::system_page_source::IcebergSystemTableProvider;
use crate::typed_read::{
    HiveTransactionHandle, IcebergColumnHandle, IcebergConnectorReadCodec,
    IcebergExecutionReadRuntime, IcebergReadSplit, IcebergRuntimeRelation,
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

    /// Pair this request-scoped execution factory with one exact provider
    /// runtime. This is connector-internal composition; roles only receive
    /// the erased SPI factory created by the execution bundle.
    pub fn into_read_runtime_factory<P>(
        self,
        adapter: ReadRuntimeAdapter<P>,
    ) -> ProviderReadFactoryAdapter<P, Self>
    where
        P: ProviderReadRuntime<
                Table = IcebergRuntimeRelation,
                Column = IcebergColumnHandle,
                Transaction = HiveTransactionHandle,
                Split = IcebergReadSplit,
            >,
        Self: ProviderReadFactory<P>,
    {
        ProviderReadFactoryAdapter::new(adapter, Arc::new(self))
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
            self.options,
        )))
    }

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorSystemTableProvider>, ConnectorError> {
        let context = self
            .binding
            .file_read_context(novarocks_fs::FileCancellation::new(), request.deadline())?;
        // A system relation shares the fragment's page-row budget but neither
        // its footer cache nor its delete manager: it opens metadata files, not
        // data files, so there is nothing for those two to hold.
        Ok(Arc::new(IcebergSystemTableProvider::new(
            self.binding.clone(),
            context,
            self.options.budget.max_rows,
        )))
    }
}

impl<P> ProviderReadFactory<P> for IcebergTypedProviderFactory
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ProviderReadPageSourceProvider<P>>, ConnectorError> {
        let context = self
            .binding
            .file_read_context(novarocks_fs::FileCancellation::new(), request.deadline())?;
        Ok(Arc::new(IcebergPageSourceProvider::new(
            self.binding.clone(),
            context,
            self.options,
        )))
    }

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ProviderReadSystemTableProvider<P>>, ConnectorError> {
        let context = self
            .binding
            .file_read_context(novarocks_fs::FileCancellation::new(), request.deadline())?;
        Ok(Arc::new(IcebergSystemTableProvider::new(
            self.binding.clone(),
            context,
            self.options.budget.max_rows,
        )))
    }
}

impl ConnectorReadExecutionBundleFactory for IcebergTypedProviderFactory {
    fn build(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorReadExecutionBundle, ConnectorError> {
        let runtime = IcebergExecutionReadRuntime::new(
            iceberg_descriptor(key),
            key.incarnation,
            HiveTransactionHandle::new(true, key.incarnation.to_bytes()),
        );

        let adapter = ReadRuntimeAdapter::new(Arc::new(runtime));
        let codec = Arc::new(IcebergConnectorReadCodec::new(adapter.clone()));
        let provider_factory = Arc::new(ProviderReadFactoryAdapter::new(
            adapter,
            Arc::new(self.clone()),
        ));
        Ok(ConnectorReadExecutionBundle::new(provider_factory, codec))
    }
}

/// Rebuild the descriptor from the Host-admitted exact binding key. The
/// provider is static because this factory is installed only in Iceberg's
/// sealed provider-kind slot; the key supplies the per-generation facts.
fn iceberg_descriptor(key: &ConnectorExecutionBindingKey) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID)
            .expect("static Iceberg provider ID is valid"),
        instance_id: key.instance_id.clone(),
    }
}
